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
  reconciled" row. Only after that commit are the segment's identities
  released from RAM.
* **Startup reconciliation**: a segment becomes dedup authority only with a
  valid publication marker (``<seg>.meta.json`` v2: sha256 + size of its exact
  bytes, written after the rename's directory fsync -- see
  ``docs/F1_DURABLE_PUBLICATION.md``). A visible ``.seg`` without one is first
  confirmed by the startup refsync protocol (or startup fails closed); only
  then is it indexed, before ingestion resumes.

C1 (index content audit). The index is a *derived, rebuildable* view, and every
fact in it is auditable against evidence that is stored outside SQLite (a separate file per
segment, derived from the same bytes by the same identity code: storage independence, NOT
independent derivation):

    segment bytes (authority)  ->  marker (sha256, size)
        ->  identity evidence file ``<index stem>.identity_evidence/<segment>.ids.json``
            (count + order-independent digest of the segment's identity SET,
            bound to the marker's sha256/size and to the identity-key encoding)
        ->  ``seen(segment_id, identity_key)`` membership + the ``reconciled_segments``
            row (count, digest, evidence sha256/size)

Startup recomputes each segment's membership digest from ``seen`` and compares
it with the evidence file, never with a value that only SQLite holds. Missing,
extra, swapped or mis-attributed identities, wrong counts, an identity owned by
two segments, stale evidence after sequence reuse and identity-key encoding
drift are all divergence: the index is rebuilt from the segment bytes (or
startup raises). ``DEDUP_DEEP_VERIFY=1`` additionally re-derives every
segment's identity set from its bytes and checks the evidence file itself.
Losing or distrusting the index or an evidence file costs re-reading segments,
never correctness.

An identity from a segment that was never published (a crashed ``.tmp``,
deleted by ``ParquetWriter._recover_orphans`` with a DATA_DROP) is never in
the index, so a redelivery after restart is accepted again (Invariant 8).

Every index failure raises :class:`DedupStateError`. Nothing here ever maps
an error to "new" or to "duplicate" (Invariants 9/J/K).
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, NamedTuple, Optional, Set, Tuple

import pyarrow.parquet as pq

from .publication import (
    DEEP_VERIFY_ENV, MarkerResult, MarkerStatus, PublicationError, PublicationState, UnmarkedSegment,
    classify_segment, confirm_unmarked, fs_guard, fsync_dir, marker_path, orphan_marker, preserve_invalid,
    read_marker, read_segment_table, sha256_bytes,
)
from .storage_errors import FatalStorageError
from .utils import logger

#: ``PRAGMA user_version`` of the current index layout. History: 0/1 = no
#: provenance, 2 = F1 (rows carry marker evidence, ``seen`` has no segment
#: ownership), 3 = C1 (``seen`` rows are owned by a segment; every segment row
#: carries an identity count + digest + encoding). Anything lower is rebuilt
#: once from confirmed segments -- old ``seen`` rows are never trusted.
INDEX_SCHEMA_VERSION = 3

#: Bump when ``dedup_identity_key`` (or what a stored row maps to) changes
#: meaning. Also guarded by a content canary (see ``identity_encoding_fingerprint``)
#: so a forgotten bump is still caught.
IDENTITY_ENCODING_VERSION = 1
IDENTITY_DIGEST_ALGO = "sha256-sorted-framed/1"
EVIDENCE_SCHEMA = 1
EVIDENCE_SUFFIX = ".ids.json"

UNIDENTIFIED = "<unidentified>"
SegmentToken = Tuple[str, int]


class DedupStateError(FatalStorageError):
    """The dedup state cannot be established. Callers must stop ingestion for
    the affected stream -- never continue as if the identity were unseen or
    already seen.

    F5: participates in the typed fatal-storage hierarchy (``isinstance(exc,
    FatalStorageError)``) while keeping its own name, its message-only
    constructor and its ``RuntimeError`` ancestry. Its failure domain is the
    trades route: the dedup index only ever guards the trade stream, so the
    defaults below name that stream and the ``dedup`` component."""

    def __init__(self, *args: object, stream: Optional[str] = "trades",
                 component: Optional[str] = "dedup", stage: Optional[str] = "dedup_state",
                 durability: Optional[str] = None) -> None:
        super().__init__(*args, stream=stream, component=component, stage=stage,
                         durability=durability)


def dedup_identity_key(exchange: str, market_type: str, instrument_key: str,
                       stream: str, trade_id: str) -> str:
    """Length-prefixed (structurally unambiguous) encoding of the production
    five-part identity ``(exchange, market_type, instrument, stream,
    trade_id)`` -- identical semantics to ``ExchangeAdapter._dedupe_trades``."""
    return "".join(f"{len(p)}:{p}" for p in (exchange, market_type, instrument_key, stream, trade_id))


# ---------------------------------------------------------------------------
# C1: deterministic, order-independent identity-set digest
# ---------------------------------------------------------------------------

_DIGEST_DOMAIN = b"btc-collector/dedup-identity-set/1\x00"


def identity_encoding_fingerprint() -> str:
    """Names the identity-key encoding AND digest algorithm. It embeds a canary
    computed through the live ``dedup_identity_key``, so changing the encoding
    without bumping ``IDENTITY_ENCODING_VERSION`` still changes the fingerprint."""
    probe = dedup_identity_key("\x00ex", "mk:t", "ins|k", "str\u00e9am", "1:2")
    return (f"v{IDENTITY_ENCODING_VERSION}:{IDENTITY_DIGEST_ALGO}:"
            f"{hashlib.sha256(probe.encode('utf-8')).hexdigest()[:16]}")


_LEN = struct.Struct(">Q").pack


def _new_digest() -> Any:
    h = hashlib.sha256()
    h.update(_DIGEST_DOMAIN)
    return h


def _feed(h: Any, key_bytes: bytes) -> None:
    h.update(_LEN(len(key_bytes)) + key_bytes)


def identity_set_digest(keys: Iterable[str]) -> Tuple[int, str]:
    """``(distinct count, hex digest)`` of an identity SET. Order- and
    duplicate-independent: sha256 over the UTF-8 byte-sorted, length-framed,
    distinct keys. ``seen`` is clustered on ``(identity_key, segment_id)``, so a single pass in identity
    order hands every segment its own keys in this same (byte) order -- SQLite BINARY collation == UTF-8
    byte order -- and membership can be streamed without sorting."""
    unique = sorted({k.encode("utf-8") for k in keys})
    h = _new_digest()
    for kb in unique:
        _feed(h, kb)
    return len(unique), h.hexdigest()


def future_schema_message(version: int) -> Optional[str]:
    """``None`` unless ``version`` is NEWER than this code's ``INDEX_SCHEMA_VERSION``. A newer layout is never
    rebuilt, downgraded or reinterpreted (an older one is rebuilt once, from the segment bytes)."""
    if version > INDEX_SCHEMA_VERSION:
        return (f"UNSUPPORTED_FUTURE_SCHEMA: index user_version={version} is newer than this code supports "
                f"({INDEX_SCHEMA_VERSION}); refusing to read, rebuild or downgrade it -- run a collector "
                f"version that understands it, or move the index aside deliberately")
    return None


class IndexedSegment(NamedTuple):
    segment_id: int
    sha256: Optional[str]
    size: Optional[int]
    identity_count: int
    identity_digest: str
    identity_encoding: str
    confirmed_by: str


_SCHEMA_DDL = (
    "CREATE TABLE reconciled_segments (segment_id INTEGER PRIMARY KEY, segment_key TEXT NOT NULL UNIQUE, "
    "identity_count INTEGER NOT NULL, identity_digest TEXT NOT NULL, identity_encoding TEXT NOT NULL, "
    "evidence_sha256 TEXT NOT NULL, evidence_size INTEGER NOT NULL, confirmed_by TEXT NOT NULL)",
    # One clustered b-tree, keyed by identity first: ``contains`` is a prefix lookup and a single pass in key
    # order yields every segment's keys sorted (see identity_set_digest) and every duplicate owner adjacent.
    "CREATE TABLE seen (identity_key TEXT NOT NULL, segment_id INTEGER NOT NULL, "
    "PRIMARY KEY (identity_key, segment_id)) WITHOUT ROWID",
)


def _evidence_conflict(segment_key: str, held_sha: Any, held_size: Any, sha: str, size: int) -> DedupStateError:
    return DedupStateError(
        f"EVIDENCE_CONFLICT for {segment_key!r}: index has sha256={held_sha!r} size={held_size!r}, "
        f"marker says sha256={sha!r} size={size!r}")


class SegmentDedupIndex:
    """Exact, non-evicting, segment-transactional membership index.

    ``reconciled_segments`` has one row per confirmed segment: marker evidence
    (sha256, size, confirmed_by) plus the segment's identity count, identity
    digest and identity-key encoding. ``seen`` holds ``(segment_id,
    identity_key)``: every identity is owned by the segment it was read from, so
    membership can be audited per segment. A row can never be silently reused
    for different bytes (``EVIDENCE_CONFLICT``)."""

    def __init__(self, path: str) -> None:
        self.path = path
        try:
            self._conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=5.0)
            self._conn.execute("PRAGMA journal_mode=WAL")
            # One commit per *segment*, so FULL costs one fsync per ~30s/5000 rows.
            self._conn.execute("PRAGMA synchronous=FULL")
            present = {r[0] for r in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('seen','reconciled_segments')")}
            if not present:                              # brand new file: nothing to migrate or rebuild
                self._reset_schema(drop=False)
        except sqlite3.Error as exc:
            raise DedupStateError(f"cannot open dedup index {path!r}: {exc}") from exc

    # -- schema --------------------------------------------------------
    def _reset_schema(self, *, drop: bool) -> None:
        """ONE transaction: (optionally drop and) create the current layout and set
        ``user_version``. DDL is transactional in SQLite, so a failure anywhere
        leaves the previous schema and rows exactly as they were."""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            future = self._future_schema_problem_in_txn()
            if future is not None:                       # never drop/downgrade a layout this code does not know
                raise DedupStateError(future)
            if drop:
                self._conn.execute("DROP TABLE IF EXISTS seen")
                self._conn.execute("DROP TABLE IF EXISTS reconciled_segments")
            for ddl in _SCHEMA_DDL:
                self._conn.execute(ddl)
            self._conn.execute(f"PRAGMA user_version={INDEX_SCHEMA_VERSION}")
            self._conn.execute("COMMIT")
        except BaseException:
            try:
                self._conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    def user_version(self) -> int:
        try:
            return self._conn.execute("PRAGMA user_version").fetchone()[0]
        except sqlite3.Error as exc:
            raise DedupStateError(f"user_version read failed: {exc}") from exc

    def _future_schema_problem_in_txn(self) -> Optional[str]:
        return future_schema_message(self._conn.execute("PRAGMA user_version").fetchone()[0])

    def future_schema_problem(self) -> Optional[str]:
        """Why this file must NOT be touched: its ``user_version`` is newer than this code understands.
        Unlike an older layout (rebuilt once, from the segment bytes) a newer one is never dropped,
        downgraded or reinterpreted -- the caller stops."""
        return future_schema_message(self.user_version())

    def schema_problem(self) -> Optional[str]:
        """None when the file has exactly the current layout; otherwise why it must be rebuilt
        (older/newer ``user_version``, or tables/indexes that do not have the expected shape)."""
        version = self.user_version()
        future = future_schema_message(version)
        if future is not None:
            return future
        if version != INDEX_SCHEMA_VERSION:
            return (f"index schema user_version={version} != {INDEX_SCHEMA_VERSION} "
                    f"(no per-segment identity ownership/evidence)")
        try:
            have = dict(self._conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE name IN ('seen','reconciled_segments') AND type='table'"))
            extra = [r[0] for r in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE tbl_name IN ('seen','reconciled_segments') AND type='index' "
                "AND name NOT LIKE 'sqlite_autoindex_%'")]
        except sqlite3.Error as exc:
            raise DedupStateError(f"schema inspection failed: {exc}") from exc
        want = {"reconciled_segments": _SCHEMA_DDL[0], "seen": _SCHEMA_DDL[1]}
        if have != want or extra:
            return f"index tables do not have the user_version={INDEX_SCHEMA_VERSION} shape"
        return None

    # -- queries -------------------------------------------------------
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

    def reconciled_rows(self) -> Dict[str, IndexedSegment]:
        try:
            return {r[0]: IndexedSegment(*r[1:]) for r in self._conn.execute(
                "SELECT segment_key, segment_id, evidence_sha256, evidence_size, identity_count, "
                "identity_digest, identity_encoding, confirmed_by FROM reconciled_segments")}
        except sqlite3.Error as exc:
            raise DedupStateError(f"reconciled listing failed: {exc}") from exc

    def membership_digests(self, duplicate_limit: int = 5) -> Tuple[Dict[int, Tuple[int, str]], List[Tuple[str, List[int]]]]:
        """``({segment_id: (identity rows, digest)}, duplicates)`` recomputed from the ``seen`` rows themselves
        in ONE pass over the clustered primary key (nothing recorded elsewhere is consulted).

        Rows arrive in identity order, so each segment's keys arrive sorted and feed its own running digest.
        ``duplicates`` lists identities owned by more than one segment (adjacent rows): ``[(identity_key,
        [segment_id, ...])]``, at most ``duplicate_limit`` of them."""
        state: Dict[int, list] = {}
        owners: Dict[bytes, List[int]] = {}
        prev, prev_sid = None, None
        try:
            for sid, kb, kind in self._conn.execute(
                    "SELECT segment_id, CAST(identity_key AS BLOB), typeof(identity_key) FROM seen "
                    "ORDER BY identity_key, segment_id"):
                entry = state.get(sid)
                if entry is None:
                    entry = state[sid] = [0, _new_digest()]
                if kind != "text":
                    # CAST(... AS BLOB) would give a BLOB key the very bytes of the TEXT key it imitates, yet
                    # ``contains()`` (a TEXT comparison) can never match it. Fold the storage type into the
                    # digest so the segment diverges from its evidence instead of silently normalising.
                    kb = b"\xff<non-text:" + str(kind).encode("ascii", "replace") + b">" + kb
                entry[0] += 1
                entry[1].update(_LEN(len(kb)) + kb)
                if kb == prev:
                    if kb in owners:
                        owners[kb].append(sid)
                    elif len(owners) < duplicate_limit:
                        owners[kb] = [prev_sid, sid]
                prev, prev_sid = kb, sid
        except sqlite3.Error as exc:
            raise DedupStateError(f"membership scan failed: {exc}") from exc
        return ({sid: (n, h.hexdigest()) for sid, (n, h) in state.items()},
                [(kb.decode("utf-8", "replace"), sids) for kb, sids in owners.items()])

    # -- writes --------------------------------------------------------
    def commit_segment(self, segment_key: str, identity_keys: Iterable[str], evidence_sha256: str,
                       evidence_size: int, confirmed_by: str) -> bool:
        """Atomically record a published segment's identities AND its reconciled
        row (one transaction), bound to the marker evidence.

        * no row                              -> insert, return True
        * row with EQUAL evidence and identity set -> idempotent no-op, return False
        * row with DIFFERENT evidence         -> ``DedupStateError("EVIDENCE_CONFLICT ...")``
          (never a silent no-op: this is what closes sequence/name reuse)
        * row with equal evidence but a different identity set -> ``IDENTITY_CONFLICT``
        * any identity already owned by ANOTHER segment -> ``CROSS_SEGMENT_DUPLICATE`` (live publication and
          startup indexing both fail closed; the operator quarantines the superseded segment)
        Any failure rolls back fully."""
        keys = list(identity_keys)
        count, digest = identity_set_digest(keys)
        encoding = identity_encoding_fingerprint()
        distinct = sorted(set(keys))
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT evidence_sha256, evidence_size, identity_count, identity_digest, identity_encoding "
                    "FROM reconciled_segments WHERE segment_key=?", (segment_key,)).fetchone()
                if row is not None:
                    if (row[0], row[1]) != (evidence_sha256, evidence_size):
                        self._conn.execute("ROLLBACK")
                        raise _evidence_conflict(segment_key, row[0], row[1], evidence_sha256, evidence_size)
                    if (row[2], row[3], row[4]) == (count, digest, encoding):
                        self._conn.execute("COMMIT")
                        return False
                    self._conn.execute("ROLLBACK")
                    raise DedupStateError(
                        f"IDENTITY_CONFLICT for {segment_key!r}: index has {row[2]} identities digest={row[3]!r} "
                        f"encoding={row[4]!r}, the segment yields {count} digest={digest!r} encoding={encoding!r}")
                for start in range(0, len(distinct), 500):
                    batch = distinct[start:start + 500]
                    owned = self._conn.execute(
                        "SELECT e.identity_key, s.segment_key FROM seen e LEFT JOIN reconciled_segments s "
                        f"USING(segment_id) WHERE e.identity_key IN ({','.join('?' * len(batch))}) LIMIT 1",
                        batch).fetchone()
                    if owned is not None:
                        self._conn.execute("ROLLBACK")
                        raise DedupStateError(
                            f"CROSS_SEGMENT_DUPLICATE: identity {owned[0]!r} in {segment_key!r} is already "
                            f"owned by segment {owned[1]!r}")
                cur = self._conn.execute(
                    "INSERT INTO reconciled_segments (segment_key, identity_count, identity_digest, "
                    "identity_encoding, evidence_sha256, evidence_size, confirmed_by) VALUES (?,?,?,?,?,?,?)",
                    (segment_key, count, digest, encoding, evidence_sha256, evidence_size, confirmed_by))
                segment_id = cur.lastrowid
                self._conn.executemany("INSERT INTO seen (segment_id, identity_key) VALUES (?,?)",
                                       ((segment_id, k) for k in distinct))
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
        """ONE transaction: drop every identity and reconciled row (and any old-layout table) and
        recreate the current layout. Crash-safe: either all of it happened or none of it did; the
        caller then re-indexes from segments. This is also the one-time migration of an older index."""
        try:
            self._reset_schema(drop=True)
        except sqlite3.Error as exc:
            raise DedupStateError(f"index rebuild reset failed: {exc}") from exc

    def identity_count(self) -> int:
        try:
            return self._conn.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
        except sqlite3.Error as exc:
            raise DedupStateError(f"count failed: {exc}") from exc

    def non_text_identity_rows(self) -> int:
        """Persisted ``identity_key`` values whose SQLite storage class is not TEXT (corruption: ``contains()``
        compares TEXT and would never match them). Read-only; used by audits/tools."""
        try:
            return self._conn.execute("SELECT COUNT(*) FROM seen WHERE typeof(identity_key) != 'text'").fetchone()[0]
        except sqlite3.Error as exc:
            raise DedupStateError(f"type scan failed: {exc}") from exc

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
    #: identity-evidence files (re)derived from segment bytes (missing / stale / unreadable / forged)
    evidence_regenerated: int = 0
    #: segments whose identity set was re-derived from bytes by DEDUP_DEEP_VERIFY=1
    deep_verified: int = 0


class SegmentDedupCoordinator:
    """Per-stream coordinator: open-segment RAM authority + persistent index.

    ``row_identity(row) -> Optional[str]`` maps a stored canonical row to the
    same identity key the live path uses (None for ``trade_id is None`` rows,
    which are never deduplicated).

    ``evidence_dir`` holds one derived identity-evidence file per segment
    (default: ``<index stem>.identity_evidence`` beside the index). Each is a pure function of the
    segment bytes, bound to the marker's sha256/size; it is the index's
    separately stored audit reference (stored apart from SQLite, but derived by the same
    ``row_identity`` code from the same bytes -- it catches SQLite loss/corruption/tampering,
    not a bug in identity derivation that both would share) and is regenerated from the bytes
    whenever it is missing, stale, unreadable or (under deep verify) wrong.
    """

    def __init__(self, index: SegmentDedupIndex, row_identity: Callable[[Dict[str, Any]], Optional[str]],
                 evidence_dir: Optional[Path] = None) -> None:
        self.index = index
        self._row_identity = row_identity
        self.evidence_dir = Path(evidence_dir) if evidence_dir is not None else self.default_evidence_dir(index)
        self._admitted: Set[str] = set()                 # admitted, not yet attributed to a segment
        self._pending_by_token: Dict[SegmentToken, Set[str]] = {}
        self._pending_index: Dict[str, SegmentToken] = {}
        self.last_report: ReconcileReport = ReconcileReport()

    @staticmethod
    def default_evidence_dir(index: SegmentDedupIndex) -> Path:
        """``<dir>/<stem>.identity_evidence`` next to ``<dir>/<stem>.sqlite3`` -- deliberately NOT prefixed by
        the index file name, so deleting ``<index>*`` (a lost or distrusted index) does not delete the
        separately stored evidence with it."""
        idx = Path(index.path)
        return idx.with_name(idx.stem + ".identity_evidence")

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
        self._index_segment(path, marker)
        for k in self._pending_by_token.pop(token, ()):
            self._pending_index.pop(k, None)

    def _index_segment(self, path: Path, marker: MarkerResult) -> None:
        """Derive the identity set from the verified segment bytes, record its evidence file, then commit
        the membership + reconciled row in one transaction (evidence first: a crash in between leaves
        an evidence file that the next startup finds, verifies and indexes)."""
        keys, sha, size, by = self._verified_identities(path, marker)
        segment_key = self._segment_key(path)
        held = self.index.evidence_of(segment_key)
        if held is not None and (held[0], held[1]) != (sha, size):
            raise _evidence_conflict(segment_key, held[0], held[1], sha, size)     # before touching any file
        count, digest = identity_set_digest(keys)
        self._ensure_evidence(segment_key, sha, size, count, digest)
        self.index.commit_segment(segment_key, keys, sha, size, by)

    # -- identity evidence files ----------------------------------------
    def _evidence_path(self, segment_key: str) -> Path:
        return self.evidence_dir / (segment_key.replace("/", "%2F") + EVIDENCE_SUFFIX)

    def _load_evidence(self, segment_key: str, sha: str, size: int) -> Tuple[str, Optional[Tuple[int, str]]]:
        """``(state, (count, digest))``; state is ``ok`` | ``missing`` | ``invalid`` (unreadable, malformed,
        bound to other bytes -- stale after sequence reuse -- or written under another identity encoding)."""
        path = self._evidence_path(segment_key)
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            return "missing", None
        except OSError:
            return "invalid", None
        try:
            obj = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return "invalid", None
        if not isinstance(obj, dict):
            return "invalid", None
        count, digest = obj.get("identity_count"), obj.get("identity_digest")
        good = (obj.get("schema") == EVIDENCE_SCHEMA and obj.get("segment_key") == segment_key
                and obj.get("segment_sha256") == sha and obj.get("segment_size") == size
                and obj.get("identity_encoding") == identity_encoding_fingerprint()
                and isinstance(count, int) and not isinstance(count, bool) and count >= 0
                and isinstance(digest, str) and len(digest) == 64)
        return ("ok", (count, digest)) if good else ("invalid", None)

    def _ensure_evidence(self, segment_key: str, sha: str, size: int, count: int, digest: str) -> bool:
        """Make the evidence file equal what the segment bytes yield. An existing file that disagrees is
        preserved (``.invalid.N``), never deleted. Returns True when a file was (re)written.

        Deliberately NOT fsynced: the file is a pure function of the segment bytes, so a lost or torn
        copy is detected (missing / unparsable / unbound) and re-derived by the next startup at the cost
        of one segment read. It adds no fsync to the publication path and is not a durability boundary."""
        state, held = self._load_evidence(segment_key, sha, size)
        if state == "ok" and held == (count, digest):
            return False
        path = self._evidence_path(segment_key)
        payload = (json.dumps({
            "digest_algo": IDENTITY_DIGEST_ALGO, "identity_count": count, "identity_digest": digest,
            "identity_encoding": identity_encoding_fingerprint(), "schema": EVIDENCE_SCHEMA,
            "segment_key": segment_key, "segment_sha256": sha, "segment_size": size,
        }, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        tmp = Path(str(path) + ".tmp")
        try:
            self.evidence_dir.mkdir(parents=True, exist_ok=True)
            if state != "missing":
                preserve_invalid(path)
            tmp.write_bytes(payload)
            os.replace(tmp, path)
        except OSError as exc:
            raise DedupStateError(f"cannot write identity evidence for {segment_key!r}: {exc!r}") from exc
        return True

    # -- recovery ------------------------------------------------------
    def startup_reconcile(self, stream_dir: Path) -> int:
        """Deterministic, idempotent startup reconciliation (runs before
        ingestion). A visible ``.seg`` is never authority by itself:

          0 filesystem policy   1 scan segments + markers   2 classify
          3 confirm UNMARKED (refsync + marker v2)          4 dangling markers
          5 audit the index against marker + identity evidence; rebuild on any divergence
          6 index confirmed-but-unreconciled segments (an identity already owned by another
            segment raises CROSS_SEGMENT_DUPLICATE: the bytes themselves hold the duplicate)

        Returns the number of segments newly indexed; the full account is in
        ``self.last_report``. Raises ``DedupStateError`` on any uncertainty."""
        stream_dir = Path(stream_dir)
        report = ReconcileReport()
        self.last_report = report
        verdict = fs_guard(stream_dir)
        future = self.index.future_schema_problem()
        if future is not None:        # before ANY marker is renamed or written: an unknown layout is never rebuilt
            raise DedupStateError(future)
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

        schema_problem = self.index.schema_problem()
        present = {p.name for p in segs}
        diverged: List[str] = [schema_problem] if schema_problem else []
        for marker_file in sorted(stream_dir.glob("*.seg.meta.json")):
            if marker_file.name[: -len(".meta.json")] not in present:
                report.dangling += 1
                logger.error("publication_marker_dangling", file=str(marker_file))
                try:
                    orphan_marker(marker_file)
                except OSError as exc:
                    raise DedupStateError(f"cannot preserve dangling marker {marker_file.name}: {exc!r}") from exc
                renamed = True
                if not schema_problem and self.index.is_segment_reconciled(
                        self._segment_key(Path(marker_file.name[: -len(".meta.json")]), stream_dir)):
                    diverged.append(f"dangling marker with indexed segment {marker_file.name}")
        if renamed:
            self._fsync_dir(stream_dir)

        try:
            promoted = confirm_unmarked(unmarked, stream_dir, verdict=verdict)
        except PublicationError as exc:
            raise DedupStateError(str(exc)) from exc
        report.confirmed_by_refsync = len(promoted)
        confirmed = sorted(confirmed + [c.path for c in promoted])

        if not schema_problem:
            diverged += self._audit_index(confirmed, report)
        if diverged:
            report.rebuilt, report.rebuild_reasons = True, diverged
            logger.error("DEDUP_INDEX_REBUILT", stream_dir=str(stream_dir), reasons=diverged[:20],
                         count=len(diverged))
            self.index.rebuild_reset()

        for path in confirmed:
            if self.index.is_segment_reconciled(self._segment_key(path)):
                continue
            self._index_segment(path, read_marker(path))
            report.indexed += 1

        return report.indexed

    def _audit_index(self, confirmed: List[Path], report: ReconcileReport) -> List[str]:
        """Reasons the index is not an exact, evidence-backed view of the confirmed segments.

        Reference values come from the marker and the identity-evidence file -- never from SQLite alone;
        a missing/stale evidence file is re-derived from the segment bytes first. Membership is recomputed
        from the ``seen`` rows themselves, so a deleted, fake, swapped or mis-attributed identity changes
        the digest even when the row count (or a recorded digest) is untouched."""
        reasons: List[str] = []
        deep = os.environ.get(DEEP_VERIFY_ENV) == "1"
        fingerprint = identity_encoding_fingerprint()
        on_disk: Dict[str, Tuple[Path, MarkerResult]] = {}
        for p in confirmed:
            on_disk[self._segment_key(p)] = (p, read_marker(p))
        rows = self.index.reconciled_rows()
        expected: Dict[int, Tuple[int, str]] = {}
        for key, row in sorted(rows.items()):
            if key not in on_disk:
                reasons.append(f"indexed segment {key} has no confirmed segment on disk")
                continue
            path, marker = on_disk[key]
            pub = marker.publication
            if row.sha256 != pub["sha256"] or row.size != pub["size_bytes"]:
                reasons.append(f"indexed evidence for {key} differs from marker")
                continue
            if row.identity_encoding != fingerprint:
                reasons.append(f"identity-key encoding of {key} ({row.identity_encoding}) != current ({fingerprint})")
                continue
            state, held = self._load_evidence(key, pub["sha256"], pub["size_bytes"])
            if state != "ok" or deep:
                derived = identity_set_digest(self._verified_identities(path, marker)[0])   # the bytes
                if state == "ok" and held != derived:
                    reasons.append(f"identity evidence of {key} differs from the segment bytes")
                if state != "ok" or held != derived:
                    self._ensure_evidence(key, pub["sha256"], pub["size_bytes"], *derived)
                    report.evidence_regenerated += 1
                if deep:
                    report.deep_verified += 1
                held = derived
            if (row.identity_count, row.identity_digest) != held:
                reasons.append(f"recorded identity set of {key} (count {row.identity_count}) differs from "
                               f"segment evidence (count {held[0]})")
            expected[row.segment_id] = held
        by_id = {row.segment_id: key for key, row in rows.items()}
        membership, duplicates = self.index.membership_digests()
        for segment_id, (n, digest) in sorted(membership.items()):
            if segment_id not in by_id:
                reasons.append(f"{n} identity row(s) owned by unknown segment_id {segment_id}")
            elif segment_id in expected and (n, digest) != expected[segment_id]:
                reasons.append(f"index membership of {by_id[segment_id]} ({n} identities) differs from the "
                               f"segment's identity evidence ({expected[segment_id][0]} identities)")
        for segment_id, (count, _digest) in sorted(expected.items()):
            if segment_id not in membership and count != 0:
                reasons.append(f"index has no identities for {by_id[segment_id]} (evidence says {count})")
        for identity, segment_ids in duplicates:
            reasons.append(f"identity {identity!r} owned by several segments {[by_id.get(i, i) for i in segment_ids]}")
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
