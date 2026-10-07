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
* **Startup reconciliation**: a segment becomes dedup authority only with a
  valid publication marker (``<seg>.meta.json`` v2: sha256 + size of its exact
  bytes, written after the rename's directory fsync -- see
  ``docs/F1_DURABLE_PUBLICATION.md``). A visible ``.seg`` without one is first
  confirmed by the startup refsync protocol (or startup fails closed); only
  then is it indexed, before ingestion resumes. The index is a *derived,
  rebuildable* view of confirmed segments and every row carries the marker's
  evidence -- losing or distrusting it costs re-reading segments, never
  correctness.

An identity from a segment that was never published (a crashed ``.tmp``,
deleted by ``ParquetWriter._recover_orphans`` with a DATA_DROP) is never in
the index, so a redelivery after restart is accepted again (Invariant 8).

Every index failure raises :class:`DedupStateError`. Nothing here ever maps
an error to "new" or to "duplicate" (Invariants 9/J/K).
"""
from __future__ import annotations

import io
import os
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

import pyarrow.parquet as pq

from .publication import (
    DEEP_VERIFY_ENV, MarkerResult, MarkerStatus, PublicationError, PublicationState, UnmarkedSegment,
    classify_segment, confirm_unmarked, fs_guard, fsync_dir, marker_path, orphan_marker, preserve_invalid,
    read_marker, read_segment_table, sha256_bytes,
)
from .utils import logger

#: ``PRAGMA user_version`` of an index whose rows carry evidence (F1). Anything
#: lower has no provenance and is rebuilt once from confirmed segments.
INDEX_SCHEMA_VERSION = 2

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
    """Exact, non-evicting, segment-transactional membership index.

    F1: every ``reconciled_segments`` row carries the evidence (sha256, size,
    confirmed_by) of the publication marker it was derived from. A row can never
    be silently reused for different bytes (``EVIDENCE_CONFLICT``)."""

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
            existing = {r[1] for r in self._conn.execute("PRAGMA table_info(reconciled_segments)")}
            fresh = self._conn.execute("SELECT 1 FROM reconciled_segments LIMIT 1").fetchone() is None \
                and self._conn.execute("SELECT 1 FROM seen LIMIT 1").fetchone() is None
            for column, ddl in (("evidence_sha256", "TEXT"), ("evidence_size", "INTEGER"), ("confirmed_by", "TEXT")):
                if column not in existing:
                    self._conn.execute(f"ALTER TABLE reconciled_segments ADD COLUMN {column} {ddl}")
            if fresh and self.user_version() < INDEX_SCHEMA_VERSION:
                self._conn.execute(f"PRAGMA user_version={INDEX_SCHEMA_VERSION}")   # nothing to rebuild
        except sqlite3.Error as exc:
            raise DedupStateError(f"cannot open dedup index {path!r}: {exc}") from exc

    def user_version(self) -> int:
        try:
            return self._conn.execute("PRAGMA user_version").fetchone()[0]
        except sqlite3.Error as exc:
            raise DedupStateError(f"user_version read failed: {exc}") from exc

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

    def evidence_of(self, segment_key: str) -> Optional[Tuple[Optional[str], Optional[int], Optional[str]]]:
        try:
            return self._conn.execute(
                "SELECT evidence_sha256, evidence_size, confirmed_by FROM reconciled_segments WHERE segment_key=?",
                (segment_key,)).fetchone()
        except sqlite3.Error as exc:
            raise DedupStateError(f"evidence lookup failed: {exc}") from exc

    def all_reconciled(self) -> Dict[str, Tuple[Optional[str], Optional[int]]]:
        try:
            return {k: (h, n) for k, h, n in self._conn.execute(
                "SELECT segment_key, evidence_sha256, evidence_size FROM reconciled_segments")}
        except sqlite3.Error as exc:
            raise DedupStateError(f"reconciled listing failed: {exc}") from exc

    def commit_segment(self, segment_key: str, identity_keys: Iterable[str], evidence_sha256: str,
                       evidence_size: int, confirmed_by: str) -> bool:
        """Atomically record a published segment's identities AND its reconciled
        row (one transaction), bound to the marker evidence.

        * no row                      -> insert, return True
        * row with EQUAL evidence     -> idempotent no-op, return False
        * row with DIFFERENT evidence -> ``DedupStateError("EVIDENCE_CONFLICT ...")``
          (never a silent no-op: this is what closes sequence/name reuse).
        Any failure rolls back fully."""
        keys = list(identity_keys)
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT evidence_sha256, evidence_size FROM reconciled_segments WHERE segment_key=?",
                    (segment_key,)).fetchone()
                if row is not None:
                    if row[0] == evidence_sha256 and row[1] == evidence_size:
                        self._conn.execute("COMMIT")
                        return False
                    self._conn.execute("ROLLBACK")
                    raise DedupStateError(
                        f"EVIDENCE_CONFLICT for {segment_key!r}: index has sha256={row[0]!r} size={row[1]!r}, "
                        f"marker says sha256={evidence_sha256!r} size={evidence_size!r}")
                self._conn.executemany("INSERT OR IGNORE INTO seen (identity_key) VALUES (?)",
                                       ((k,) for k in keys))
                self._conn.execute(
                    "INSERT INTO reconciled_segments (segment_key, identity_count, evidence_sha256, "
                    "evidence_size, confirmed_by) VALUES (?,?,?,?,?)",
                    (segment_key, len(keys), evidence_sha256, evidence_size, confirmed_by))
                self._conn.execute("COMMIT")
                return True
            except DedupStateError:
                raise
            except BaseException:
                try:
                    self._conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
        except sqlite3.Error as exc:
            raise DedupStateError(f"segment commit failed for {segment_key!r}: {exc}") from exc

    def rebuild_reset(self) -> None:
        """ONE transaction: drop every identity and reconciled row and set
        ``user_version`` to the evidence schema. Crash-safe: either all of it
        happened or none of it did; the caller then re-indexes from segments."""
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute("DELETE FROM seen")
                self._conn.execute("DELETE FROM reconciled_segments")
                self._conn.execute(f"PRAGMA user_version={INDEX_SCHEMA_VERSION}")
                self._conn.execute("COMMIT")
            except BaseException:
                try:
                    self._conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
        except sqlite3.Error as exc:
            raise DedupStateError(f"index rebuild reset failed: {exc}") from exc

    def identity_count(self) -> int:
        try:
            return self._conn.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
        except sqlite3.Error as exc:
            raise DedupStateError(f"count failed: {exc}") from exc

    def disk_size_bytes(self) -> int:
        return sum(os.path.getsize(self.path + s) for s in ("", "-wal", "-shm") if os.path.exists(self.path + s))

    def close(self) -> None:
        self._conn.close()


@dataclass
class ReconcileReport:
    confirmed_by_refsync: int = 0
    indexed: int = 0
    rebuilt: bool = False
    rebuild_reasons: List[str] = field(default_factory=list)
    dangling: int = 0
    invalid_markers: int = 0


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
        self.last_report: ReconcileReport = ReconcileReport()

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
        """Writer publication hook: re-verify the marker against the exact
        segment bytes, index THOSE bytes, then (and only then) release the
        identities from RAM. Any violation raises ``DedupStateError`` and
        commits nothing."""
        path = Path(path)
        marker = read_marker(path)
        keys, sha, size, by = self._verified_identities(path, marker)
        self.index.commit_segment(self._segment_key(path), keys, sha, size, by)
        for k in self._pending_by_token.pop(token, ()):
            self._pending_index.pop(k, None)

    # -- recovery ------------------------------------------------------
    def startup_reconcile(self, stream_dir: Path) -> int:
        """Deterministic, idempotent startup reconciliation (runs before
        ingestion). A visible ``.seg`` is never authority by itself:

          0 filesystem policy   1 scan segments + markers   2 classify
          3 confirm UNMARKED (refsync + marker v2)          4 dangling markers
          5 audit the index against marker evidence; rebuild on any divergence
          6 index confirmed-but-unreconciled segments

        Returns the number of segments newly indexed; the full account is in
        ``self.last_report``. Raises ``DedupStateError`` on any uncertainty."""
        stream_dir = Path(stream_dir)
        report = ReconcileReport()
        self.last_report = report
        verdict = fs_guard(stream_dir)
        segs = sorted(stream_dir.glob("*.seg"))
        markers: Dict[Path, MarkerResult] = {p: read_marker(p) for p in segs}

        confirmed: List[Path] = []
        unmarked: List[UnmarkedSegment] = []
        renamed = False
        for path in segs:
            marker = markers[path]
            if marker.status is MarkerStatus.INVALID:
                report.invalid_markers += 1
                logger.error("publication_marker_invalid", file=str(marker_path(path)), reason=marker.reason)
                try:
                    preserve_invalid(marker_path(path))
                except OSError as exc:
                    raise DedupStateError(f"cannot preserve invalid marker of {path.name}: {exc!r}") from exc
                renamed = True
            state = classify_segment(path, marker)
            if state is PublicationState.CORRUPT:
                raise DedupStateError(
                    f"segment {path.name} disagrees with its publication marker (size {path.stat().st_size} "
                    f"!= marker {marker.publication['size_bytes']}); evidence left untouched")
            if state is PublicationState.PUBLICATION_CONFIRMED:
                confirmed.append(path)
            else:
                unmarked.append(UnmarkedSegment(path, marker))

        present = {p.name for p in segs}
        diverged: List[str] = []
        for marker_file in sorted(stream_dir.glob("*.seg.meta.json")):
            if marker_file.name[: -len(".meta.json")] not in present:
                report.dangling += 1
                logger.error("publication_marker_dangling", file=str(marker_file))
                try:
                    orphan_marker(marker_file)
                except OSError as exc:
                    raise DedupStateError(f"cannot preserve dangling marker {marker_file.name}: {exc!r}") from exc
                renamed = True
                if self.index.is_segment_reconciled(self._segment_key(Path(marker_file.name[: -len(".meta.json")]),
                                                                     stream_dir)):
                    diverged.append(f"dangling marker with indexed segment {marker_file.name}")
        if renamed:
            self._fsync_dir(stream_dir)

        try:
            promoted = confirm_unmarked(unmarked, stream_dir, verdict=verdict)
        except PublicationError as exc:
            raise DedupStateError(str(exc)) from exc
        report.confirmed_by_refsync = len(promoted)
        confirmed = sorted(confirmed + [c.path for c in promoted])

        diverged += self._audit_index(stream_dir, confirmed)
        if diverged:
            report.rebuilt, report.rebuild_reasons = True, diverged
            logger.error("DEDUP_INDEX_REBUILT", stream_dir=str(stream_dir), reasons=diverged[:20],
                         count=len(diverged))
            self.index.rebuild_reset()

        deep = os.environ.get(DEEP_VERIFY_ENV) == "1"
        for path in confirmed:
            key = self._segment_key(path)
            if self.index.is_segment_reconciled(key):
                if deep:
                    self._verified_identities(path, read_marker(path))      # re-hash; raises on mismatch
                continue
            keys, sha, size, by = self._verified_identities(path, read_marker(path))
            self.index.commit_segment(key, keys, sha, size, by)
            report.indexed += 1
        return report.indexed

    def _audit_index(self, stream_dir: Path, confirmed: List[Path]) -> List[str]:
        """Reasons the index is not a subset of confirmed-segment evidence."""
        reasons: List[str] = []
        if self.index.user_version() < INDEX_SCHEMA_VERSION:
            reasons.append(f"index schema user_version={self.index.user_version()} < {INDEX_SCHEMA_VERSION} "
                           f"(no evidence provenance)")
        evidence = {self._segment_key(p): read_marker(p).publication for p in confirmed}
        rows = self.index.all_reconciled()
        for key, (sha, size) in sorted(rows.items()):
            pub = evidence.get(key)
            if pub is None:
                reasons.append(f"indexed segment {key} has no confirmed segment on disk")
            elif sha != pub["sha256"] or size != pub["size_bytes"]:
                reasons.append(f"indexed evidence for {key} differs from marker")
        if not rows and self.index.identity_count() > 0:
            reasons.append("identities present without any reconciled segment (no provenance)")
        return reasons

    @staticmethod
    def _fsync_dir(stream_dir: Path) -> None:
        try:
            fsync_dir(stream_dir)
        except OSError as exc:
            raise DedupStateError(f"directory fsync of {stream_dir} failed: {exc!r}") from exc

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
    def _segment_key(path: Path, stream_dir: Optional[Path] = None) -> str:
        parent = Path(path).parent.name if stream_dir is None else Path(stream_dir).name
        return f"{parent}/{Path(path).name}"

    def _verified_identities(self, path: Path, marker: MarkerResult) -> Tuple[list, str, int, str]:
        """Read the segment ONCE and prove it is the one the marker describes
        (valid marker, size, sha256, footer rows); return identities extracted
        from those same bytes plus the evidence to bind them to."""
        if not marker.valid:
            raise DedupStateError(
                f"no valid publication marker for {Path(path).name} ({marker.status.value}: {marker.reason}); "
                f"refusing to create dedup authority")
        pub = marker.publication
        try:
            data = Path(path).read_bytes()
        except OSError as exc:
            raise DedupStateError(f"cannot read published segment {str(path)!r}: {exc!r}") from exc
        if len(data) != pub["size_bytes"]:
            raise DedupStateError(f"{Path(path).name}: size {len(data)} != marker size_bytes {pub['size_bytes']}")
        digest = sha256_bytes(data)
        if digest != pub["sha256"]:
            raise DedupStateError(f"{Path(path).name}: sha256 {digest} != marker sha256 {pub['sha256']}")
        try:
            table = read_segment_table(data, Path(path).name)
        except PublicationError as exc:
            raise DedupStateError(str(exc)) from exc
        rows = table.num_rows
        if rows != marker.data["record_count"]:
            raise DedupStateError(f"{Path(path).name}: {rows} rows != marker record_count {marker.data['record_count']}")
        return self._identities_of_table(table, path), digest, len(data), pub["confirmed_by"]

    def _identities_of_table(self, table: Any, path: Path) -> list:
        try:
            rows = table.to_pylist()
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

    def bind_for(self, event: Any) -> Optional[Callable[[SegmentToken], None]]:
        if event.trade_id is None:
            # The adapter never admits (and StreamSpec.row_identity never indexes)
            # an unidentified trade, so there is no identity to attribute. Without
            # this, event_identity_key() raised TypeError on len(None) and the
            # write -- a trade the adapter deliberately keeps -- was lost.
            return None
        coordinator = self.coordinators[event.stream]      # KeyError = unwired stream -> loud
        key = event_identity_key(event)
        return lambda token: coordinator.note_written(key, token)

    def end_message(self) -> None:
        """Message boundary. Runners MUST call this from a ``finally`` that
        encloses ``adapter.normalize()`` and every write for the message, so an
        early exit / exception cannot leave admitted-but-never-written
        identities in RAM to suppress a legitimate redelivery."""
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
