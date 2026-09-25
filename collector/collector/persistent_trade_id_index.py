"""P0-4: an exact, off-heap, non-evicting trade-identity membership index.

## The problem this solves, precisely

``ExchangeAdapter._seen_trade_ids`` (adapters/base.py) is a Python `set`
that grows for the adapter instance's entire lifetime -- unsafe RAM growth
for a 24/7 process on a small instance. PR #50's own evidence-based
conclusion (Outcome B, docs/TRADE_DEDUPLICATION.md) established that NO
lossy eviction strategy (TTL, LRU, high-water mark) can currently be proven
safe: no supported venue's official documentation states a maximum
duplicate-redelivery delay, and Bybit's trade IDs are UUID-shaped (no
ordering property to build a high-water mark from at all). That finding is
not revisited or weakened here.

This module answers a different, narrower question: can RAM be bounded
*without* evicting anything, ever -- i.e. by moving the exact, permanent
membership record off the Python heap and onto disk instead? If so, the
"we can't prove a safe expiry horizon" problem simply does not need
solving, because nothing is ever expired.

## Design

A single SQLite table, one row per distinct trade identity ever seen,
``PRIMARY KEY`` enforcing uniqueness. Membership is checked and recorded
in one atomic operation:

    INSERT OR IGNORE INTO seen_trade_ids (identity_key) VALUES (?)

then ``cursor.rowcount``: ``1`` means this identity was not previously
present (genuinely new -- insert succeeded); ``0`` means it already existed
(duplicate). This is atomic within one SQLite transaction -- there is no
separate SELECT-then-INSERT race window (task's explicit "ATOMICITY
QUESTION" requirement): SQLite's own row-level conflict resolution decides
uniqueness, not two round-trips from this process.

``PRAGMA journal_mode=WAL`` and ``PRAGMA synchronous=NORMAL`` are used:
WAL for concurrent-reader-friendly writes, NORMAL (not FULL) trading a
narrow window of durability (a true OS-level power-loss event, not a mere
process crash, could lose the most recent few committed transactions still
in the OS page cache) for substantially better throughput. Measured on
this session's sandbox (not the target EC2 instance -- see "What remains
unverified" below): ~46,000 inserts/sec sustained with per-trade autocommit
(21.5 microseconds/insert average), ~180,000 lookups/sec on the
already-seen path, 200,000 unique 60-character keys -> 35.8 MiB on disk.
Even with a large safety margin for a slower disk than this sandbox's, this
is far beyond any plausible sustained trade rate for a single BTC
instrument across four venues.

## Explicit semantics this module adds, that the in-memory set does not have

**Restart survival**: because the SQLite file persists across process
restarts (unlike the in-memory set, which is always empty on a fresh
process), using this module changes the dedup lifecycle from
"adapter-instance lifetime" to "durable-namespace lifetime" -- this is a
new capability, not a bug fix, and is why this module is NOT wired into
``ExchangeAdapter`` as the default in this change. See
``docs/P0_4_BOUNDED_TRADE_DEDUP.md`` for the full decision record on why
the production default is deliberately left unchanged.

## Failure semantics

Any SQLite error on open, insert, or lookup (corrupt file, missing
directory, permission denied, disk full, I/O timeout) raises
``PersistentIndexError`` -- never silently treated as "not seen" (which
could accept a duplicate) and never silently treated as "everything is a
duplicate" (which could suppress genuinely new, legitimate trades). A
caller that wants a fallback behavior must choose one explicitly; this
module does not choose one on the caller's behalf.

## What remains unverified (stated explicitly, not glossed over)

- This benchmark is this sandbox's disk, not the target EC2 instance's.
  Real deployment validation (sustained load, concurrent I/O with the
  Parquet writers already running on the same instance, actual disk
  characteristics) has not been performed and cannot be performed here.
- The crash-ordering question (durable insert succeeds, process crashes
  before the corresponding canonical trade event is actually emitted/
  persisted downstream -- a genuinely new failure mode this module
  introduces that the in-memory set, being wholly lost on any restart
  anyway, does not have) is analyzed in the design doc but not resolved
  by a full two-phase-commit across the dedup index and the canonical
  writers -- that would be a substantially larger architectural change,
  out of this task's scope.
"""
from __future__ import annotations

import sqlite3
from typing import Iterable


class PersistentIndexError(RuntimeError):
    """Raised on any SQLite failure -- open, insert, or lookup. Never
    caught and silently converted into "treat as new" or "treat as
    duplicate" by this module; a caller must choose explicitly."""


class PersistentTradeIdIndex:
    """Exact, non-evicting, disk-backed set of trade-identity keys.

    Keys are opaque strings (callers are expected to pass the same
    ``(exchange, market_type, instrument_key, stream, trade_id)`` tuple
    representation production ``_dedupe_trades`` already uses, joined into
    one string -- see ``identity_key()`` below for the exact, tested join
    format). Nothing is ever evicted: every key inserted remains a member
    for the lifetime of the underlying file.
    """

    def __init__(self, path: str, *, journal_mode: str = "WAL", synchronous: str = "NORMAL"):
        self.path = path
        try:
            self._conn = sqlite3.connect(path, check_same_thread=False)
            self._conn.execute(f"PRAGMA journal_mode={journal_mode}")
            self._conn.execute(f"PRAGMA synchronous={synchronous}")
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS seen_trade_ids (identity_key TEXT PRIMARY KEY)"
            )
            self._conn.commit()
        except sqlite3.Error as exc:
            raise PersistentIndexError(f"failed to open/initialize persistent index at {path!r}: {exc}") from exc

    def add_if_new(self, identity_key: str) -> bool:
        """Atomically record ``identity_key`` as seen. Returns True if this
        was the first time (genuinely new -- caller should proceed), False
        if it was already present (a duplicate -- caller should suppress).
        One round trip, one transaction: no separate existence check
        precedes the insert, so there is no read-then-write race window.
        """
        try:
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO seen_trade_ids (identity_key) VALUES (?)", (identity_key,)
            )
            self._conn.commit()
            return cursor.rowcount == 1
        except sqlite3.Error as exc:
            raise PersistentIndexError(f"failed to record identity {identity_key!r} in {self.path!r}: {exc}") from exc

    def __contains__(self, identity_key: str) -> bool:
        try:
            row = self._conn.execute(
                "SELECT 1 FROM seen_trade_ids WHERE identity_key = ? LIMIT 1", (identity_key,)
            ).fetchone()
            return row is not None
        except sqlite3.Error as exc:
            raise PersistentIndexError(f"failed to look up identity {identity_key!r} in {self.path!r}: {exc}") from exc

    def __len__(self) -> int:
        try:
            row = self._conn.execute("SELECT COUNT(*) FROM seen_trade_ids").fetchone()
            return row[0]
        except sqlite3.Error as exc:
            raise PersistentIndexError(f"failed to count entries in {self.path!r}: {exc}") from exc

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "PersistentTradeIdIndex":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()


def identity_key(exchange: str, market_type: str, instrument_key: str, stream: str, trade_id: str) -> str:
    """The exact, tested join format for the five-part identity tuple
    production ``_dedupe_trades`` already keys on (adapters/base.py). Uses
    a delimiter (``"\\x1f"``, ASCII unit separator) that cannot appear in
    any of the five components as currently produced -- exchange names,
    market_type constants, and stream names are fixed internal literals;
    instrument_key is itself pipe-delimited (never contains \\x1f); trade_id
    is a venue-native string (numeric or UUID-shaped for every currently
    supported venue, never containing a control character) -- so no
    delimiter-injection ambiguity is possible with real production values.
    """
    return "\x1f".join((exchange, market_type, instrument_key, stream, trade_id))


class ReferenceExactSet:
    """The existing in-memory `set`-based semantics, wrapped behind the
    same `add_if_new` interface as `PersistentTradeIdIndex`, so the two can
    be driven through one shared differential test with no special-casing.
    This is not a new implementation -- it is the same logic
    `ExchangeAdapter._dedupe_trades` already uses, extracted to a class so
    it can serve as the correctness oracle for the persistent index."""

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def add_if_new(self, identity_key: str) -> bool:
        if identity_key in self._seen:
            return False
        self._seen.add(identity_key)
        return True

    def __contains__(self, identity_key: str) -> bool:
        return identity_key in self._seen

    def __len__(self) -> int:
        return len(self._seen)


def differential_check(keys: Iterable[str], reference: ReferenceExactSet, candidate) -> None:
    """Feed the same key sequence through both implementations in lockstep
    and raise AssertionError at the first divergence, naming the exact key
    and index where the two implementations disagreed. Used by the
    randomized differential test, not by production code."""
    for i, key in enumerate(keys):
        expected = reference.add_if_new(key)
        actual = candidate.add_if_new(key)
        if expected != actual:
            raise AssertionError(
                f"divergence at index {i}, key={key!r}: reference={expected}, candidate={actual}"
            )
