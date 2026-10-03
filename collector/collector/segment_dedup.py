"""P0-4: segment-granularity exact trade dedup (bounded RAM, exact, crash-safe).

Why this exists (docs/P0_4_BOUNDED_TRADE_DEDUP.md sections 11-14): the
canonical writer (``ParquetWriter``) is durable per *segment*, not per
trade -- a row is crash-durable only after ``_close_segment``'s
fsync+``os.replace``. A per-trade durable dedup insert therefore cannot be
ordered correctly against "the canonical write". This module instead makes
the *published segment* the recovery anchor:

* **Open-segment authority (RAM)**: identities admitted/written into a
  segment that is not yet published are held in memory and suppress
  duplicates immediately.
* **Historical authority (disk)**: once a segment is durably published, its
  identities are read *back out of that published file* and committed to a
  SQLite index in one atomic transaction together with a "this segment is
  reconciled" marker. Only after that commit are the segment's identities
  released from RAM.
* **Startup reconciliation**: any published segment without a marker (crash
  between publication and index commit) is indexed before ingestion resumes.
  The index is therefore a *derived, rebuildable* view of published
  segments -- losing it costs re-reading segments, never correctness.

An identity from a segment that was never published (a crashed ``.tmp``,
deleted by ``ParquetWriter._recover_orphans`` with a DATA_DROP) is never in
the index, so a redelivery after restart is accepted again (Invariant 8).

Every index failure raises :class:`DedupStateError`. Nothing here ever maps
an error to "new" or to "duplicate" (Invariants 9/J/K).
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Set, Tuple

import pyarrow.parquet as pq

UNIDENTIFIED = "<unidentified>"
SegmentToken = Tuple[str, int]


class DedupStateError(RuntimeError):
    """The dedup state cannot be established. Callers must stop ingestion for
    the affected stream -- never continue as if the identity were unseen or
    already seen."""


def dedup_identity_key(exchange: str, market_type: str, instrument_key: str,
                       stream: str, trade_id: str) -> str:
    """Length-prefixed (structurally unambiguous) encoding of the production
    five-part identity ``(exchange, market_type, instrument, stream,
    trade_id)`` -- identical semantics to ``ExchangeAdapter._dedupe_trades``."""
    return "".join(f"{len(p)}:{p}" for p in (exchange, market_type, instrument_key, stream, trade_id))


class SegmentDedupIndex:
    """Exact, non-evicting, segment-transactional membership index."""

    def __init__(self, path: str) -> None:
        self.path = path
        try:
            self._conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=5.0)
            self._conn.execute("PRAGMA journal_mode=WAL")
            # One commit per *segment*, so FULL costs one fsync per ~30s/5000 rows.
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("CREATE TABLE IF NOT EXISTS seen (identity_key TEXT PRIMARY KEY) WITHOUT ROWID")
            self._conn.execute("CREATE TABLE IF NOT EXISTS reconciled_segments "
                               "(segment_key TEXT PRIMARY KEY, identity_count INTEGER NOT NULL) WITHOUT ROWID")
        except sqlite3.Error as exc:
            raise DedupStateError(f"cannot open dedup index {path!r}: {exc}") from exc

    def contains(self, identity_key: str) -> bool:
        try:
            return self._conn.execute("SELECT 1 FROM seen WHERE identity_key=? LIMIT 1",
                                      (identity_key,)).fetchone() is not None
        except sqlite3.Error as exc:
            raise DedupStateError(f"dedup lookup failed: {exc}") from exc

    def is_segment_reconciled(self, segment_key: str) -> bool:
        try:
            return self._conn.execute("SELECT 1 FROM reconciled_segments WHERE segment_key=?",
                                      (segment_key,)).fetchone() is not None
        except sqlite3.Error as exc:
            raise DedupStateError(f"reconciliation lookup failed: {exc}") from exc

    def commit_segment(self, segment_key: str, identity_keys: Iterable[str]) -> bool:
        """Atomically record a published segment's identities AND its
        reconciled marker (one transaction). Idempotent: an already-marked
        segment is a no-op returning False. Any failure rolls back fully."""
        keys = list(identity_keys)
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if self._conn.execute("SELECT 1 FROM reconciled_segments WHERE segment_key=?",
                                      (segment_key,)).fetchone() is not None:
                    self._conn.execute("COMMIT")
                    return False
                self._conn.executemany("INSERT OR IGNORE INTO seen (identity_key) VALUES (?)",
                                       ((k,) for k in keys))
                self._conn.execute("INSERT INTO reconciled_segments (segment_key, identity_count) VALUES (?,?)",
                                   (segment_key, len(keys)))
                self._conn.execute("COMMIT")
                return True
            except BaseException:
                try:
                    self._conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
        except sqlite3.Error as exc:
            raise DedupStateError(f"segment commit failed for {segment_key!r}: {exc}") from exc

    def identity_count(self) -> int:
        try:
            return self._conn.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
        except sqlite3.Error as exc:
            raise DedupStateError(f"count failed: {exc}") from exc

    def disk_size_bytes(self) -> int:
        return sum(os.path.getsize(self.path + s) for s in ("", "-wal", "-shm") if os.path.exists(self.path + s))

    def close(self) -> None:
        self._conn.close()


class SegmentDedupCoordinator:
    """Per-stream coordinator: open-segment RAM authority + persistent index.

    ``row_identity(row) -> Optional[str]`` maps a stored canonical row to the
    same identity key the live path uses (None for ``trade_id is None`` rows,
    which are never deduplicated).
    """

    def __init__(self, index: SegmentDedupIndex, row_identity: Callable[[Dict[str, Any]], Optional[str]]) -> None:
        self.index = index
        self._row_identity = row_identity
        self._admitted: Set[str] = set()                 # admitted, not yet attributed to a segment
        self._pending_by_token: Dict[SegmentToken, Set[str]] = {}
        self._pending_index: Dict[str, SegmentToken] = {}

    # -- live path -----------------------------------------------------
    def check_and_admit(self, key: str) -> bool:
        """True if ``key`` is new (caller proceeds); False if duplicate.
        Raises DedupStateError if the durable state cannot be consulted."""
        if key in self._admitted or key in self._pending_index:
            return False
        if self.index.contains(key):
            return False
        self._admitted.add(key)
        return True

    def note_written(self, key: str, token: SegmentToken) -> None:
        """Attribute an admitted identity to the (unpublished) segment that
        receives its row. Must be called by the writer's ``bind`` callback,
        i.e. after hour-rollover handling and before the append."""
        self._admitted.discard(key)
        self._pending_by_token.setdefault(token, set()).add(key)
        self._pending_index[key] = token

    def end_message(self) -> None:
        """Forget admitted-but-never-written identities (e.g. rejected by a
        validator): they are not durable, so a redelivery must be accepted."""
        self._admitted.clear()

    def on_segment_published(self, token: SegmentToken, path: Path) -> None:
        """Writer publication hook: index the published file, then (and only
        then) release its identities from RAM."""
        keys = self._identities_of(path)
        self.index.commit_segment(self._segment_key(path), keys)
        for k in self._pending_by_token.pop(token, ()):
            self._pending_index.pop(k, None)

    # -- recovery ------------------------------------------------------
    def startup_reconcile(self, stream_dir: Path) -> int:
        """Index every published, un-marked segment. Fail closed if any
        published segment cannot be read. Returns segments newly reconciled."""
        done = 0
        for path in sorted(Path(stream_dir).glob("*.seg")):
            if self.index.is_segment_reconciled(self._segment_key(path)):
                continue
            self.index.commit_segment(self._segment_key(path), self._identities_of(path))
            done += 1
        return done

    # -- introspection -------------------------------------------------
    @property
    def ram_identity_count(self) -> int:
        # Count EVERY structure that holds identities (a leak in any of them
        # must be visible): the by-token sets are the authoritative storage.
        return (len(self._admitted) + len(self._pending_index)
                + sum(len(v) for v in self._pending_by_token.values()))

    @property
    def ram_segment_token_count(self) -> int:
        return len(self._pending_by_token)

    # -- helpers -------------------------------------------------------
    @staticmethod
    def _segment_key(path: Path) -> str:
        return f"{Path(path).parent.name}/{Path(path).name}"

    def _identities_of(self, path: Path) -> list:
        try:
            rows = pq.read_table(str(path)).to_pylist()
        except Exception as exc:  # noqa: BLE001 - unreadable published segment => cannot establish state
            raise DedupStateError(f"cannot read published segment {str(path)!r}: {exc!r}") from exc
        keys = []
        for row in rows:
            key = self._row_identity(row)
            if key is not None:
                keys.append(key)
        return keys


# ---------------------------------------------------------------------------
# Runner wiring (P0-4 production integration)
# ---------------------------------------------------------------------------


def event_identity_key(event: Any) -> str:
    """The identity key of a ``CanonicalTradeEvent`` -- exactly the tuple
    ``ExchangeAdapter._dedupe_trades`` uses, so writer-side attribution and
    adapter-side admission can never name the same trade differently."""
    return dedup_identity_key(
        event.exchange, event.market_type,
        event.instrument.key if event.instrument is not None else UNIDENTIFIED,
        event.stream, event.trade_id)


class StreamSpec:
    """One durable trade stream: which adapter event stream it is, which
    writer's *published segments* are its recovery anchor, and how to read
    the identity back out of a stored row."""

    def __init__(self, event_stream: str, writer: Any, exchange: str, market_type: str,
                 trade_id_field: str = "trade_id") -> None:
        self.event_stream, self.writer = event_stream, writer
        self.exchange, self.market_type, self.trade_id_field = exchange, market_type, trade_id_field

    def row_identity(self, row: Dict[str, Any]) -> Optional[str]:
        trade_id = row.get(self.trade_id_field)
        if trade_id is None:
            return None                      # never deduplicated -- same exemption as production
        return dedup_identity_key(self.exchange, self.market_type,
                                  row.get("instrument_key") or UNIDENTIFIED,
                                  self.event_stream, trade_id)


class SegmentDedupHandle:
    """What a runner keeps: one coordinator + one index file per stream."""

    def __init__(self) -> None:
        self.coordinators: Dict[str, SegmentDedupCoordinator] = {}
        self.indexes: list = []

    def bind_for(self, event: Any) -> Callable[[SegmentToken], None]:
        coordinator = self.coordinators[event.stream]      # KeyError = unwired stream -> loud
        key = event_identity_key(event)
        return lambda token: coordinator.note_written(key, token)

    def end_message(self) -> None:
        for c in self.coordinators.values():
            c.end_message()

    def close(self) -> None:
        for i in self.indexes:
            try:
                i.close()
            except Exception:  # noqa: BLE001 - shutdown must not raise
                pass


def attach_segment_dedup(adapter: Any, specs: Iterable[StreamSpec]) -> SegmentDedupHandle:
    """Wire the segment-granularity backend into a live runner. Order is the
    contract: index open -> publication hook installed -> startup reconcile
    (raises DedupStateError => the runner's constructor fails => the collector
    never starts ingestion) -> adapter installed last. Nothing here is
    reached by ``ReplayEngine``, which builds its own adapter."""
    handle = SegmentDedupHandle()
    backends: Dict[str, SegmentDedupCoordinator] = {}
    for spec in specs:
        state_dir = Path(spec.writer.base_dir) / "dedup_state"
        try:
            state_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise DedupStateError(f"cannot create dedup state dir {str(state_dir)!r}: {exc}") from exc
        index = SegmentDedupIndex(str(state_dir / f"{spec.writer.stream_name}.sqlite3"))
        handle.indexes.append(index)
        coordinator = SegmentDedupCoordinator(index, spec.row_identity)
        spec.writer.on_segment_published = coordinator.on_segment_published
        coordinator.startup_reconcile(spec.writer.stream_dir)
        backends[spec.event_stream] = coordinator
        handle.coordinators[spec.event_stream] = coordinator
    adapter.set_trade_dedup(backends)
    return handle


def bind_arg(handle: Optional[SegmentDedupHandle], event: Any) -> Optional[Callable[[SegmentToken], None]]:
    """``bind=`` value for ``writer.write`` -- None when no backend is wired
    (legacy/test construction), which leaves the writer behaviour unchanged."""
    return None if handle is None else handle.bind_for(event)
