"""Deterministic replay over recorded raw wire data.

Drives Binance, Bybit, and OKX through their own adapters (venue selected
at construction). Order-book events go through LocalBook; every other
canonical event an adapter produces -- trades, funding, liquidations, and
(for Binance) REST-polled open interest via binance_oi.py -- is preserved
in `ReplayResult.non_book_events` rather than being silently dropped or
re-shaped into a book-like record.

What was here before
--------------------

``scripts/replay_test.py`` loaded a derived Parquet file and asserted bounds
on its columns (no NaNs, OBI within [-1, 1], spread positive). That is a
dataset sanity check. It never replayed anything: there was no recorded
clock, no raw event source, no reconstruction, no determinism check, and it
could not have detected a reconstruction bug because it never ran the
reconstruction.

What replay means here
----------------------

Replay drives the **same objects** the live collector drives::

    recorded frame -> BinanceAdapter.normalize() -> LocalBook.apply() -> quality state

Only the clock and the source differ. There is no replay-only reconstruction
path, because a second implementation would be free to diverge from
production and would mask exactly the bugs replay exists to catch.

Causality
---------

Recorded websocket frames and recorded REST snapshot responses are merged
into **one** time-ordered stream:

* a websocket frame becomes available at its ``local_receive_ts``
* a REST snapshot becomes available at its ``response_receive_ts``

This mirrors live, where a snapshot requested during a gap arrives
asynchronously and can only bridge the book once it has actually landed. A
snapshot is therefore never visible to the engine before the moment it
arrived in the recorded run, so replay cannot repair a gap using information
from the future.

What the evidence does NOT establish: REST rows are stamped in whole
milliseconds, so within a millisecond shared by a WIRE frame and a REST row
the true order is unobservable. ``order_key`` places the WIRE frame first --
a deterministic convention, not a measurement. Replay reports each place it
relied on that convention in ``ReplayResult.unresolved_order_ties`` rather
than letting the result look fully evidenced.

Determinism
-----------

Frames are ordered by a total key --
``(timestamp_ms, kind_rank, ns_tiebreak, source_index)`` -- so ties never
depend on filesystem iteration order, dict ordering or sort instability.
Running the same input twice produces the same :attr:`ReplayResult.digest`.

Determinism is not causal truth. The key is deterministic everywhere; it is
evidence-based only where the recorded stamps distinguish the frames.

Three separate questions, three separate answers on :class:`ReplayResult`:

* what was produced -- ``digest`` (output state only);
* which replay frames produced it, plus how many rows were excluded and why
  -- ``input_fingerprint`` (a fingerprint of the frames replayed and the
  exclusion ACCOUNTING; not of the content of excluded rows);
* whether the result is fully evidenced -- ``integrity_issues()`` /
  ``is_pristine`` (skipped foreign rows, dropped REST rows, truncated,
  undecodable or unhandled frames, rejected snapshots, unresolved ties).
  ``is_pristine=False`` means NOT FULLY EVIDENCED; it is not a verdict that
  the replay is unusable.

No network
----------

This module imports no HTTP client and performs no I/O beyond reading
recorded segments. Replay that contacts the live exchange to fill missing
history is not replay, and the absence is enforced by test.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any, Iterable, Iterator, Optional, Sequence

from .adapters.binance import BinanceAdapter
from .adapters.binance_spot import BinanceSpotAdapter
from .binance_oi import BinanceOIParseError, normalize_binance_oi
from .adapters.bybit import BybitAdapter
from .adapters.okx import OKXAdapter
from .book_engine import LocalBook
from .canonical import CanonicalOrderBookEvent
from .clock import ms_from_ns, require_epoch_ns
from .quality_events import BookQuality, QualityEventType
from .storage_layout import StorageCollisionError, iter_segments, read_streams
from .utils import logger

__all__ = [
    "ReplayFrame",
    "ReplaySource",
    "ReplayEngine",
    "ReplayResult",
    "BookUpdate",
    "FrameKind",
    "replay_directory",
]


class FrameKind:
    WIRE = "wire"
    REST_SNAPSHOT = "rest_snapshot"
    #: A REST open-interest observation (currently Binance only -- see
    #: binance_oi.py). Kept distinct from REST_SNAPSHOT because it drives
    #: no order-book state and never bridges anything; it is a plain
    #: point-in-time reading routed straight into non_book_events.
    REST_OI = "rest_oi"


#: Tie-break rank. A snapshot that landed in the same millisecond as a diff is
#: ordered after it, matching live, where the diff was already in the socket
#: buffer when the HTTP response completed.
_KIND_RANK = {FrameKind.WIRE: 0, FrameKind.REST_SNAPSHOT: 1, FrameKind.REST_OI: 1}


@dataclass(frozen=True)
class ReplayFrame:
    """One recorded input, with the total order key used to sequence it."""

    timestamp_ms: int
    kind: str
    source_index: int
    payload: str
    connection_id: Optional[str] = None
    decode_ok: bool = True
    http_ok: bool = True
    endpoint: Optional[str] = None
    #: P0-11: the RECORDED historical local receive time in epoch ns (never
    #: the replay-time clock, never file-read time). None for legacy rows,
    #: which only ever had ms precision.
    receive_ns: Optional[int] = None
    #: Recorded monotonic ns; meaningful only relative to other frames of the
    #: same connection lineage in the same process run. Never an epoch value.
    receive_mono_ns: Optional[int] = None
    #: Recorded ``truncated`` flag: the stored payload is a clipped prefix of
    #: what the collector actually received (raw_capture bounds frame size).
    #: Live saw the whole frame; replay can only see the prefix.
    truncated: bool = False

    @property
    def order_key(self) -> tuple[int, int, int, int]:
        """``(timestamp_ms, kind_rank, ns_tiebreak, source_index)``.

        P0-11 follow-up: ``kind_rank`` must be compared BEFORE any
        nanosecond tiebreak, not after. The earlier ``(effective_ns(...),
        kind_rank, source_index)`` form synthesized a REST snapshot's
        missing ``receive_ns`` as the start of its millisecond
        (``timestamp_ms * 1_000_000``), which can sort *below* a WIRE
        frame's real, later-in-the-millisecond ``receive_ns`` -- moving a
        REST snapshot ahead of a websocket frame the collector actually
        received first, purely because the snapshot has no finer evidence
        to offer. That is a causality-relevant reordering: it can change
        which event an order-book recovery sees as available first.

        Putting ``timestamp_ms`` and ``kind_rank`` first restores the
        documented event-kind ordering for same-millisecond frames
        regardless of which ones happen to carry nanosecond evidence. The
        ns tiebreak then refines order only *within* one (ms, kind) group
        -- exactly where finer evidence is actually comparable. A frame
        with no ``receive_ns`` (a REST row, or a legacy WIRE row) uses a
        sentinel below any real epoch-ns value, so it is never confused
        with a genuine zero-ns reading; which side of other such frames it
        falls on is then settled by ``source_index``, the final,
        deterministic tiebreaker -- never a synthesized offset.
        """
        ns_tiebreak = self.receive_ns if self.receive_ns is not None else -1
        return (self.timestamp_ms, _KIND_RANK.get(self.kind, 9), ns_tiebreak, self.source_index)


@dataclass(frozen=True)
class BookUpdate:
    """One reconstructed book state, recorded for comparison."""

    timestamp_ms: int
    update_id: Optional[int]
    first_update_id: Optional[int]
    previous_update_id: Optional[int]
    best_bid: Optional[str]
    best_ask: Optional[str]
    quality_state: str
    event_kind: str
    recovery_generation: int
    #: Recorded time at which this state first became AVAILABLE to the
    #: collector: the ``timestamp_ms`` of the replay frame being handled when
    #: the update was committed. ``timestamp_ms`` is the diff's own receive
    #: time; for ``RECOVERY_BRIDGE`` / ``RECOVERY_INCREMENTAL`` updates the
    #: book only became VALID when the bridging snapshot landed, which can be
    #: later. Consumers joining on availability must use this, not
    #: ``timestamp_ms``. Not part of ``digest_tuple`` (digest unchanged).
    available_ts_ms: Optional[int] = None

    def digest_tuple(self) -> tuple:
        return (
            self.timestamp_ms, self.update_id, self.first_update_id,
            self.previous_update_id, self.best_bid, self.best_ask,
            self.quality_state, self.event_kind, self.recovery_generation,
        )


@dataclass
class ReplayResult:
    """Everything a replay produced, plus a digest for parity comparison."""

    book_updates: list[BookUpdate] = field(default_factory=list)
    #: Canonical events from every non-order-book stream a venue adapter
    #: produces (trades, mark/index/funding, OI, liquidations, ...), stored
    #: as the actual frozen dataclass instances `adapter.normalize()`
    #: yielded -- not re-shaped into a book-like record, and not routed
    #: through LocalBook, which only ever meant order-book reconstruction.
    non_book_events: list[Any] = field(default_factory=list)
    quality_events: list[dict[str, Any]] = field(default_factory=list)
    frames_total: int = 0
    frames_wire: int = 0
    frames_snapshot: int = 0
    frames_undecodable: int = 0
    frames_unhandled: int = 0
    snapshots_applied: int = 0
    snapshots_rejected: int = 0
    oi_observations: int = 0
    oi_rejected: int = 0
    final_state: str = BookQuality.VALID.value
    #: --- input provenance / integrity (NOT part of ``digest``) -----------
    #: Recorded frames whose stored payload is a truncated prefix.
    frames_truncated: int = 0
    #: Rows a directory read refused because they named another venue.
    skipped_rows: dict[str, int] = field(default_factory=dict)
    #: REST rows excluded before becoming frames, by reason (request never
    #: returned; purpose replay does not drive).
    dropped_rest_rows: dict[str, int] = field(default_factory=dict)
    #: Same-millisecond frame pairs whose true relative order the recorded
    #: evidence cannot establish (see ``ReplaySource.unresolved_order_ties``).
    unresolved_order_ties: dict[str, int] = field(default_factory=dict)
    #: SHA-256 over (1) the replay frames the engine was given -- kind,
    #: recorded stamps, lineage, success flags, truncated flag and a hash of
    #: each payload, in replay order -- and (2) the exclusion ACCOUNTING:
    #: counts of skipped foreign-venue rows and dropped REST rows by key.
    #: It does NOT fingerprint the content of excluded rows: inputs that differ
    #: only in what an excluded row contained (same counts) share a value.
    input_fingerprint: Optional[str] = None

    def integrity_issues(self) -> dict[str, Any]:
        """Every recorded condition under which this replay is NOT fully
        evidenced. Empty means pristine.

        Deliberately separate from :attr:`digest`: the digest says *what was
        produced*; this says what evidence was excluded, refused, rejected or
        order-ambiguous on the way. It is a disclosure, not a validity
        verdict: a non-empty result does not mean the retained evidence is
        wrong or that the replay is unusable.
        """
        issues: dict[str, Any] = {}
        for name in ("frames_undecodable", "frames_unhandled", "frames_truncated",
                     "snapshots_rejected", "oi_rejected"):
            if getattr(self, name):
                issues[name] = getattr(self, name)
        for name in ("skipped_rows", "dropped_rest_rows", "unresolved_order_ties"):
            if getattr(self, name):
                issues[name] = dict(getattr(self, name))
        if self.frames_total == 0:
            issues["empty_replay"] = True
        return issues

    @property
    def is_pristine(self) -> bool:
        """``True`` only when nothing was excluded, refused, rejected or
        order-ambiguous (:meth:`integrity_issues` is empty).

        ``False`` means the result is NOT FULLY EVIDENCED. It does not mean
        the replay is historically unusable, invalid for every research use,
        or that the evidence it did retain is wrong: an ordinary replay whose
        only issue is a same-millisecond WIRE/REST tie still reaches the same
        book it would without the tie. Callers decide fitness for their use
        from :meth:`integrity_issues`; this flag is not a gate.
        """
        return not self.integrity_issues()

    @property
    def digest(self) -> str:
        """Stable hash of the replay's OUTPUT state sequence.

        Covers book states, non-book canonical events, *and* the
        ``(event_type, reason, quality_state)`` of each quality event, so a
        replay that produced the same prices via a different quality path --
        or the same book but a changed trade, funding rate, OI reading, or
        liquidation -- does not compare equal.

        It is an output digest only. It does NOT cover the input evidence
        (use :attr:`input_fingerprint`), the frame counters, skipped or
        dropped source rows, unresolved ordering ties
        (:meth:`integrity_issues`), or quality-event ``previous_state`` /
        ``new_state`` / lineage keys. Equal digests therefore prove equal
        output, not equal or clean inputs.
        """
        hasher = hashlib.sha256()
        for update in self.book_updates:
            hasher.update(repr(update.digest_tuple()).encode())
        for event in self.non_book_events:
            # asdict() on a frozen dataclass of only str/int/float/bool/
            # tuple/Enum/None fields (see canonical.py) is deterministic;
            # default=str turns the one non-JSON-native field (OISource,
            # an Enum) into its repr rather than raising. The type name is
            # included so a trade and a liquidation with coincidentally
            # identical field values never hash the same.
            payload = {"__type__": type(event).__name__, **asdict(event)}
            hasher.update(json.dumps(payload, sort_keys=True, default=str).encode())
        for event in self.quality_events:
            hasher.update(
                repr((event.get("event_type"), event.get("reason"),
                      event.get("quality_state"))).encode()
            )
        return hasher.hexdigest()

    def summary(self) -> dict[str, Any]:
        return {
            "frames_total": self.frames_total,
            "frames_wire": self.frames_wire,
            "frames_snapshot": self.frames_snapshot,
            "frames_undecodable": self.frames_undecodable,
            "frames_unhandled": self.frames_unhandled,
            "book_updates": len(self.book_updates),
            "non_book_events": len(self.non_book_events),
            "quality_events": len(self.quality_events),
            "snapshots_applied": self.snapshots_applied,
            "snapshots_rejected": self.snapshots_rejected,
            "oi_observations": self.oi_observations,
            "oi_rejected": self.oi_rejected,
            "frames_truncated": self.frames_truncated,
            "final_state": self.final_state,
            "digest": self.digest,
            "input_fingerprint": self.input_fingerprint,
            "pristine": self.is_pristine,
            "integrity_issues": self.integrity_issues(),
        }


class ReplaySource:
    """A totally ordered sequence of recorded frames.

    Construct from memory (tests, fixtures) or from recorded segments on
    disk. Ordering is established once, here, so the engine never has to
    care where frames came from.
    """

    def __init__(self, frames: Iterable[ReplayFrame]) -> None:
        self._frames = sorted(frames, key=lambda frame: frame.order_key)
        #: Rows a directory read refused because their ``venue`` column named
        #: a different venue (or none). Empty for a clean read; never silent.
        self.skipped_rows: dict[str, int] = {}
        #: REST rows excluded before becoming frames, keyed by reason. Empty
        #: for a source built from frames directly.
        self.dropped_rest_rows: dict[str, int] = {}

    def unresolved_order_ties(self) -> dict[str, int]:
        """Count frames whose order against a same-millisecond neighbour is
        a deterministic convention, not something the evidence establishes.

        * ``wire_vs_rest_snapshot_same_ms`` / ``wire_vs_rest_oi_same_ms``:
          REST rows are stamped in whole milliseconds, so a REST response
          that shares a millisecond with a websocket frame may have landed
          before or after it. ``order_key`` places the REST row second; that
          is a tie-break, not an observation.
        * ``wire_ns_vs_legacy_same_ms``: a WIRE frame with no ``receive_ns``
          shares a millisecond with WIRE frames that have one; its position
          among them is likewise unobservable.

        Only non-zero keys are returned.
        """
        wire_ms: set[int] = set()
        wire_with_ns_ms: set[int] = set()
        for frame in self._frames:
            if frame.kind == FrameKind.WIRE:
                wire_ms.add(frame.timestamp_ms)
                if frame.receive_ns is not None:
                    wire_with_ns_ms.add(frame.timestamp_ms)
        ties = {
            "wire_vs_rest_snapshot_same_ms": sum(
                1 for f in self._frames
                if f.kind == FrameKind.REST_SNAPSHOT and f.timestamp_ms in wire_ms),
            "wire_vs_rest_oi_same_ms": sum(
                1 for f in self._frames
                if f.kind == FrameKind.REST_OI and f.timestamp_ms in wire_ms),
            "wire_ns_vs_legacy_same_ms": sum(
                1 for f in self._frames
                if f.kind == FrameKind.WIRE and f.receive_ns is None
                and f.timestamp_ms in wire_with_ns_ms),
        }
        return {key: count for key, count in ties.items() if count}

    def input_fingerprint(self) -> str:
        """SHA-256 over the replay frames this source presents to the engine
        (kind, recorded timestamps, lineage, success flags, truncated flag and
        a hash of each payload, in replay order) plus the exclusion
        ACCOUNTING (counts of skipped foreign-venue rows and dropped REST rows
        by key).

        What it identifies: the exact frame sequence replayed, so two sources
        with the same fingerprint present the engine identical frames -- which
        the output digest cannot say, because an ignored or undecodable frame
        changes nothing in it. What it does NOT identify: the content of
        excluded rows. Only their counts enter the hash, so inputs differing
        solely in what a skipped or dropped row contained share a fingerprint.
        It is therefore not a unique content fingerprint of everything that
        was on disk.
        """
        hasher = hashlib.sha256()
        for frame in self._frames:
            payload_hash = hashlib.sha256(
                str(frame.payload).encode("utf-8", errors="surrogatepass")).hexdigest()
            hasher.update(repr((
                frame.kind, frame.timestamp_ms, frame.receive_ns,
                frame.receive_mono_ns, frame.source_index, frame.connection_id,
                frame.decode_ok, frame.http_ok, frame.endpoint, frame.truncated,
                payload_hash,
            )).encode())
        hasher.update(repr(("skipped_rows", sorted(self.skipped_rows.items()))).encode())
        hasher.update(repr(("dropped_rest_rows", sorted(self.dropped_rest_rows.items()))).encode())
        return hasher.hexdigest()

    def __iter__(self) -> Iterator[ReplayFrame]:
        return iter(self._frames)

    def __len__(self) -> int:
        return len(self._frames)

    @property
    def frames(self) -> list[ReplayFrame]:
        return list(self._frames)

    @classmethod
    def from_records(
        cls,
        wire_rows: Sequence[dict] = (),
        rest_rows: Sequence[dict] = (),
    ) -> "ReplaySource":
        frames: list[ReplayFrame] = []
        dropped: dict[str, int] = {}
        index = 0
        for row in wire_rows:
            recv_ms = _as_ms(row.get("local_receive_ts") or row.get("timestamp"))
            recv_ns = _optional_ns(row.get("local_receive_ns"), "local_receive_ns")
            if recv_ns is not None and ms_from_ns(recv_ns) != recv_ms:
                # Two clock reads that disagree mean domains were mixed at
                # capture; refuse rather than silently pick one.
                raise ValueError(
                    f"recorded local_receive_ts ({recv_ms}) disagrees with "
                    f"local_receive_ns ({recv_ns}) at wire row {index}")
            frames.append(ReplayFrame(
                timestamp_ms=recv_ms,
                kind=FrameKind.WIRE, source_index=index,
                payload=row.get("payload") or "",
                connection_id=row.get("connection_id"),
                decode_ok=bool(row.get("decode_ok", True)),
                receive_ns=recv_ns,
                receive_mono_ns=_optional_ns(row.get("receive_mono_ns"), "receive_mono_ns", epoch=False),
                truncated=_recorded_flag(row.get("truncated")),
            ))
            index += 1
        for row in rest_rows:
            purpose = row.get("purpose")
            landed = row.get("response_receive_ts")
            if _is_missing(landed):
                # A request that never returned was never available live,
                # so it cannot become available in replay either. A stored
                # null comes back from parquet as NaT/NaN, not None; both are
                # "missing", and the exclusion is counted, never silent.
                dropped["rest_request_never_returned"] = dropped.get("rest_request_never_returned", 0) + 1
                continue
            if purpose == "orderbook_snapshot":
                kind = FrameKind.REST_SNAPSHOT
            elif purpose == "open_interest":
                # G2: previously excluded entirely -- every recorded OI
                # observation vanished before becoming a ReplayFrame, so
                # replay could not reproduce OI at all.
                kind = FrameKind.REST_OI
            else:
                # Recorded lineage for a purpose replay does not yet drive
                # (e.g. a future REST stream). Kept out of the frame stream
                # deliberately, not silently: nothing currently claims to
                # replay it, so nothing should quietly start doing so. It is
                # counted in ``dropped_rest_rows`` so the exclusion is visible.
                key = f"rest_unsupported_purpose:{purpose}"
                dropped[key] = dropped.get(key, 0) + 1
                continue
            frames.append(ReplayFrame(
                timestamp_ms=_as_ms(landed), kind=kind,
                source_index=index, payload=row.get("payload") or "",
                http_ok=bool(row.get("ok", False)), endpoint=row.get("endpoint"),
                truncated=_recorded_flag(row.get("truncated")),
            ))
            index += 1
        source = cls(frames)
        source.dropped_rest_rows = dropped
        return source

    @classmethod
    def from_directory(
        cls, data_dir: str, date: str | None = None, venue: str = "BINANCE"
    ) -> "ReplaySource":
        """Read ``venue``'s recorded ``raw_wire`` and ``raw_rest`` segments.

        Each venue records into its own stream directory (``venue_stream``),
        so replaying Bybit or OKX never reads Binance's frames. OKX also
        reads the legacy unprefixed ``raw_wire``, where its pre-namespace
        captures live alongside Binance's; that directory is shared history,
        so every row is kept only if its own ``venue`` column matches. Rows
        naming another venue are excluded and counted in ``skipped_rows``
        rather than replayed through the wrong adapter or silently dropped.
        """
        import pandas as pd

        venue_key = venue.upper()
        skipped: dict[str, int] = {}

        def read(stream: str) -> list[dict]:
            rows: list[dict] = []
            for name in read_streams(venue_key, stream):
                try:
                    paths = list(iter_segments(data_dir, name, date=date))
                except StorageCollisionError as exc:
                    raise StorageCollisionError(
                        f"cannot replay {name}: ambiguous storage ({exc})"
                    ) from exc
                for path in sorted(paths):
                    frame = pd.read_parquet(path)
                    records = frame.to_dict("records")
                    _restore_exact_ns_columns(path, records)
                    for row in records:
                        row_venue = _row_venue(row)
                        if row_venue != venue_key:
                            key = row_venue or "<unattributed>"
                            skipped[key] = skipped.get(key, 0) + 1
                            continue
                        rows.append(row)
            return rows

        source = cls.from_records(read("raw_wire"), read("raw_rest"))
        source.skipped_rows = skipped
        if skipped:
            logger.warning("replay_skipped_foreign_venue_rows", venue=venue_key, skipped=skipped)
        return source


_EXACT_INT_COLUMNS = ("local_receive_ns", "receive_mono_ns")


def _restore_exact_ns_columns(path: str, records: list[dict]) -> None:
    """Overwrite the ns columns with exact Python ints read straight from
    Arrow. pandas turns an int64 column containing any null into float64,
    and a float64 cannot hold an epoch-nanosecond (~1.7e18 > 2**53) exactly:
    without this the sub-millisecond distinctions P0-11 preserves would be
    silently rounded away on the way back in. Legacy files lack the columns
    and are left untouched.
    """
    import pyarrow.parquet as pq

    names = pq.read_schema(path).names
    present = [c for c in _EXACT_INT_COLUMNS if c in names]
    if not present:
        return
    table = pq.read_table(path, columns=present)
    for column in present:
        exact = table.column(column).to_pylist()
        if len(exact) != len(records):
            raise ValueError(f"row count mismatch reading {column} from {path}")
        for record, value in zip(records, exact):
            record[column] = value


def _optional_ns(value, name: str, *, epoch: bool = True):
    """None/NaN/NA -> None (a missing stamp stays missing); otherwise an
    exact int. bool and float are rejected: a float cannot carry an
    epoch-ns exactly, so accepting one would hide precision loss."""
    if value is None:
        return None
    try:
        if value != value:  # NaN
            return None
    except (TypeError, ValueError):
        pass
    if type(value).__name__ == "NAType":
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an exact int or missing, got {type(value).__name__}: {value!r}")
    if epoch:
        return require_epoch_ns(value, name)
    return value


def _is_missing(value: Any) -> bool:
    """None, NaN, or pandas NaT/NA: a stored null in any of the shapes a
    parquet read can produce. ``value is None`` alone misses NaT."""
    if value is None:
        return True
    if type(value).__name__ in ("NaTType", "NAType"):
        return True
    return isinstance(value, float) and value != value


def _recorded_flag(value: Any) -> bool:
    """A recorded boolean flag; a missing value is False (not flagged)."""
    return False if _is_missing(value) else bool(value)


def _row_venue(row: dict) -> str:
    """A stored row's venue, upper-cased; ``""`` when it names none.

    pandas hands back ``NaN`` (a truthy float) for a null string cell, so a
    plain ``str(value or "")`` would file an unattributed row under ``"NAN"``.
    """
    value = row.get("venue")
    if value is None or (isinstance(value, float) and value != value):
        return ""
    return str(value).strip().upper()


def _as_ms(value: Any) -> int:
    """Coerce a stored timestamp to epoch ms without inventing one."""
    if value is None:
        raise ValueError("replay frame has no timestamp")
    if isinstance(value, (int,)) and not isinstance(value, bool):
        return int(value)
    if isinstance(value, float):
        return int(value)
    for attr in ("timestamp", "to_pydatetime"):
        converter = getattr(value, attr, None)
        if callable(converter):
            converted = converter()
            if attr == "timestamp":
                return int(converted * 1000)
            return int(converted.timestamp() * 1000)
    raise ValueError(f"unsupported replay timestamp: {value!r}")


#: Which adapter replays which venue's wire frames. ``LocalBook`` already
#: has its own venue-keyed comparator dict (see ``book_engine.LocalBook``);
#: this is that same idea one layer up, so the constructor argument actually
#: determines what runs instead of being accepted and ignored.
_ADAPTER_CLASSES: dict[str, type] = {
    "BINANCE": BinanceAdapter,
    "BYBIT": BybitAdapter,
    "OKX": OKXAdapter,
    # P5: a distinct storage/book-engine venue from "BINANCE" -- see
    # adapters/binance_spot.py's module docstring for the identity split
    # (canonical events carry exchange="BINANCE", market_type="spot";
    # this key is the replay/storage-internal one, matching LocalBook and
    # storage_layout.VENUE_STREAM_PREFIX).
    "BINANCE_SPOT": BinanceSpotAdapter,
}


class ReplayEngine:
    """Drive recorded frames through the production reconstruction path."""

    def __init__(self, venue: str = "BINANCE", max_buffer_events: int = 10_000) -> None:
        self.venue = venue.upper()
        try:
            adapter_cls = _ADAPTER_CLASSES[self.venue]
        except KeyError:
            raise ValueError(
                f"replay does not support venue {venue!r}; supported venues: "
                f"{sorted(_ADAPTER_CLASSES)}"
            ) from None
        self.adapter = adapter_cls()
        self.book = LocalBook(self.venue, max_buffer_events=max_buffer_events)
        self.result = ReplayResult()
        #: Recorded time of the frame currently being handled.
        self._frame_ts_ms: Optional[int] = None
        self.adapter.set_unhandled_sink(self._on_unhandled)

    # -- recording helpers ------------------------------------------------

    def _on_unhandled(self, message) -> None:
        self.result.frames_unhandled += 1
        self.result.quality_events.append(message.to_quality_event())

    def _drain_book_quality(self) -> None:
        for event in self.book.drain_quality_events():
            record = event.record() if hasattr(event, "record") else dict(event)
            self.result.quality_events.append(record)

    def _record_transition(self, reason: str, kind: str, before: BookQuality, after: BookQuality) -> None:
        """Record a quality-state transition using the states actually
        observed by the caller, not ``LocalBook.last_transition``.

        ``last_transition`` is not updated by every state-changing path --
        notably ``LocalBook.snapshot()`` (the plain websocket-snapshot bridge
        Bybit's wire uses) changes ``state`` via ``recovered()`` without
        touching it, while Binance's ``_attempt_bridge`` does set it. Reading
        it here would make correctness depend on every current and future
        state-changing path in ``LocalBook`` remembering to set it too. Live
        never has this problem because it compares its own ``before``/
        ``after`` locals directly (see
        ``run_bybit_collector.BybitCollectorApp._apply_orderbook``); passing
        them in here is the same fix, not a workaround.
        """
        self.result.quality_events.append({
            "event_type": kind,
            "reason": reason,
            "quality_state": after.value,
            "previous_state": before.value,
            "new_state": after.value,
        })

    def _record_book(self, applied, event_kind: str, generation: int) -> None:
        self.result.book_updates.append(BookUpdate(
            timestamp_ms=applied.local_receive_ts,
            update_id=applied.update_id,
            first_update_id=applied.first_update_id,
            previous_update_id=applied.previous_update_id,
            best_bid=str(applied.bids[0][0]) if applied.bids else None,
            best_ask=str(applied.asks[0][0]) if applied.asks else None,
            quality_state=applied.quality_state,
            event_kind=event_kind,
            recovery_generation=generation,
            available_ts_ms=self._frame_ts_ms,
        ))

    # -- frame handling ---------------------------------------------------

    def _handle_wire(self, frame: ReplayFrame) -> None:
        self.result.frames_wire += 1
        if not frame.decode_ok:
            # The frame did not parse when it was received. It must not parse
            # now either, or replay would be processing data live never saw.
            self.result.frames_undecodable += 1
            self.result.quality_events.append({
                "event_type": QualityEventType.ERROR.value,
                "reason": "replay_undecodable_frame",
                "quality_state": self.book.state.state.value,
            })
            return
        if frame.truncated:
            # Capture clipped this payload to bound its size. Live handled the
            # complete frame; replay holds only a prefix and must not guess
            # the rest. Refused under its own reason so a clipped recording
            # is never mistaken for a frame that was undecodable on arrival.
            self.result.frames_undecodable += 1
            self.result.quality_events.append({
                "event_type": QualityEventType.ERROR.value,
                "reason": "replay_truncated_frame",
                "quality_state": self.book.state.state.value,
            })
            return
        try:
            message = json.loads(frame.payload)
        except (json.JSONDecodeError, TypeError, ValueError):
            self.result.frames_undecodable += 1
            self.result.quality_events.append({
                "event_type": QualityEventType.ERROR.value,
                "reason": "replay_undecodable_frame",
                "quality_state": self.book.state.state.value,
            })
            return

        for event in self.adapter.normalize(
            message, local_receive_ts=frame.timestamp_ms
        ):
            if not isinstance(event, CanonicalOrderBookEvent):
                # Trades, mark/index/funding, OI, liquidations, ... -- every
                # non-order-book canonical event this adapter's normalize()
                # produces. These never touch LocalBook (that machinery is
                # order-book reconstruction only); they are preserved
                # directly, exactly as live would hand them to its own
                # downstream writers, so a changed trade or funding value
                # changes the replay digest instead of vanishing here.
                self.result.non_book_events.append(event)
                continue
            if event.book_source != "DIFF_DEPTH_RECONSTRUCTED":
                # depth10 partials are not authoritative; live refuses them
                # and so must replay.
                self.result.quality_events.append({
                    "event_type": QualityEventType.BOOK_INVALID.value,
                    "reason": "partial_depth_not_authoritative",
                    "quality_state": self.book.state.state.value,
                })
                continue
            before = self.book.state.state
            applied = self.book.apply(event)
            after = self.book.state.state
            if applied is None and self.book.previous is None:
                # This diff was buffered while unbridged (or the book is
                # still RECOVERING). A retained "ahead of buffer" snapshot
                # (see book_engine.LocalBook.binance_snapshot's docstring)
                # deserves a chance against the now-larger buffer -- mirrors
                # run_binance_spot_collector._apply_orderbook's live
                # behaviour exactly, so live and replay treat a pending
                # snapshot identically rather than replay silently rejecting
                # what live would have bridged on the next diff.
                if self.book.retry_pending_snapshot():
                    after = self.book.state.state
                    self.result.snapshots_applied += 1
                    for pending_applied, event_kind, generation in self.book.committed_recovery_events:
                        self._record_book(pending_applied, event_kind, generation)
                    self.book.committed_recovery_events = []
            if before is not after:
                # Match live exactly (see run_bybit_collector._apply_orderbook):
                # any state change is recorded, not only a transition that
                # happens to land on SEQUENCE_GAP. A resync signal can drive
                # the book straight to RECOVERING without ever passing
                # through SEQUENCE_GAP (see book_engine.LocalBook.apply,
                # ``result.is_resync_signal`` -> ``state.resync()``), and a
                # narrower check here would silently omit exactly the
                # transition live records for that path.
                kind = (
                    QualityEventType.SEQUENCE_GAP.value
                    if after in (BookQuality.SEQUENCE_GAP, BookQuality.RECOVERING)
                    else QualityEventType.RECOVERY.value
                )
                self._record_transition(self.book.last_reason, kind, before, after)
            if applied is None and after in (BookQuality.SEQUENCE_GAP, BookQuality.RECOVERING):
                # Parity with live: the runner retries a retained snapshot
                # here, so replay must too or the same recorded bytes produce
                # a different book.
                retry_before = after
                if self.book.retry_pending_snapshot():
                    self.result.snapshots_applied += 1
                    for committed, event_kind, generation in self.book.committed_recovery_events:
                        self._record_book(committed, event_kind, generation)
                    self.book.committed_recovery_events = []
                    after = self.book.state.state
                    self._record_transition("pending_snapshot_bridge_completed",
                                            QualityEventType.RECOVERY.value, retry_before, after)
            if self.book.duplicate_count:
                self.result.quality_events.append({
                    "event_type": QualityEventType.DUPLICATE.value,
                    "reason": "binance_duplicate_update",
                    "quality_state": after.value,
                })
                self.book.duplicate_count = 0
            self._drain_book_quality()
            if applied is not None:
                self._record_book(applied, "NORMAL_INCREMENTAL", self.book.recovery_generation)

    def _handle_rest_oi(self, frame: ReplayFrame) -> None:
        """Route a recorded Binance OI response through the shared normalizer.

        This is the replay half of G2: the exact same function
        (`normalize_binance_oi`) that live's poll loop calls. A failed
        request (``http_ok=False``) produced no observation live, so it
        produces none here either.
        """
        if self.venue != "BINANCE":
            self.result.oi_rejected += 1
            self.result.quality_events.append({
                "event_type": QualityEventType.DATA_DROP.value,
                "reason": f"replay_oi_frame_for_unsupported_venue:{self.venue}",
                "quality_state": self.book.state.state.value,
            })
            return
        if not frame.http_ok:
            self.result.oi_rejected += 1
            self.result.quality_events.append({
                "event_type": QualityEventType.ERROR.value,
                "reason": "replay_oi_request_failed",
                "quality_state": self.book.state.state.value,
            })
            return
        try:
            # `symbol` is what live passes; omitting it left replayed OI events
            # unidentified while live's were identified (live != replay).
            event = normalize_binance_oi(
                frame.payload, response_receive_ts=frame.timestamp_ms,
                symbol=self.adapter.instrument.native_symbol)
        except BinanceOIParseError as exc:
            self.result.oi_rejected += 1
            self.result.quality_events.append({
                "event_type": QualityEventType.ERROR.value,
                "reason": f"replay_oi_malformed:{exc}",
                "quality_state": self.book.state.state.value,
            })
            return
        self.result.oi_observations += 1
        self.result.non_book_events.append(event)

    def _handle_snapshot(self, frame: ReplayFrame) -> None:
        self.result.frames_snapshot += 1
        if self.venue not in ("BINANCE", "BINANCE_SPOT"):
            # Only Binance (USD-M futures and Spot) bridges a gap with a REST
            # snapshot; Bybit's snapshot arrives on the wire itself
            # (``type": "snapshot"``, see BybitAdapter.normalize) and is
            # handled by _handle_wire via LocalBook.snapshot(), never here. A
            # REST_SNAPSHOT frame for any other venue means the recorded data
            # does not match this venue's protocol -- treating it as
            # Binance's ``lastUpdateId`` format would either crash on the
            # wrong shape or, worse, parse by coincidence and silently
            # corrupt the book.
            self.result.frames_unhandled += 1
            self.result.quality_events.append({
                "event_type": QualityEventType.ERROR.value,
                "reason": f"replay_rest_snapshot_not_supported_for_venue:{self.venue}",
                "quality_state": self.book.state.state.value,
            })
            return
        if not frame.http_ok:
            # A failed snapshot bridged nothing live. Record the attempt.
            self.result.snapshots_rejected += 1
            self.result.quality_events.append({
                "event_type": QualityEventType.ERROR.value,
                "reason": "replay_snapshot_request_failed",
                "quality_state": self.book.state.state.value,
            })
            return
        try:
            payload = json.loads(frame.payload)
            last_update_id = int(payload["lastUpdateId"])
            bids = tuple((Decimal(p), Decimal(q)) for p, q in payload["bids"])
            asks = tuple((Decimal(p), Decimal(q)) for p, q in payload["asks"])
        except (json.JSONDecodeError, KeyError, TypeError, ValueError, ArithmeticError):
            self.result.snapshots_rejected += 1
            self.result.quality_events.append({
                "event_type": QualityEventType.ERROR.value,
                "reason": "replay_snapshot_malformed",
                "quality_state": self.book.state.state.value,
            })
            return
        if not bids or not asks:
            self.result.snapshots_rejected += 1
            self.result.quality_events.append({
                "event_type": QualityEventType.ERROR.value,
                "reason": "replay_snapshot_empty",
                "quality_state": self.book.state.state.value,
            })
            return

        # Canonical identity: exchange is always "BINANCE" (Spot is not a
        # separate exchange -- see adapters/binance_spot.py's module
        # docstring); self.venue ("BINANCE" vs "BINANCE_SPOT") is the
        # storage/book-engine key, not the canonical exchange field, so it
        # must never be written into CanonicalOrderBookEvent.exchange
        # directly or a Spot snapshot would compare unequal to every diff
        # BinanceSpotAdapter itself produces for the exact same book.
        is_spot = self.venue == "BINANCE_SPOT"
        # Built by the same adapter method the live runners call, so replay's
        # snapshot event has exactly live's stream, market_type and instrument.
        snapshot_event = self.adapter.snapshot_event(
            last_update_id, bids, asks,
            local_receive_ts=frame.timestamp_ms, local_process_ts=frame.timestamp_ms)
        before = self.book.state.state
        if not self.book.binance_snapshot(last_update_id, snapshot_event):
            after = self.book.state.state
            if self.book.last_reason == "snapshot_ahead_of_buffer":
                # Not a rejection: every buffered diff has u < lastUpdateId,
                # so a later diff will straddle it and retry_pending_snapshot
                # (called from _handle_wire above) will bridge it then. This
                # mirrors the live runner's identical distinction -- see
                # run_binance_spot_collector._recover_book.
                self._drain_book_quality()
                return
            self.result.snapshots_rejected += 1
            self._record_transition(self.book.last_reason or "snapshot_rejected",
                                    QualityEventType.ERROR.value, before, after)
            self._drain_book_quality()
            return

        after = self.book.state.state
        self.result.snapshots_applied += 1
        for applied, event_kind, generation in self.book.committed_recovery_events:
            self._record_book(applied, event_kind, generation)
        self.book.committed_recovery_events = []
        self._record_transition("snapshot_bridge_completed", QualityEventType.RECOVERY.value, before, after)
        self._drain_book_quality()

    # -- driver -----------------------------------------------------------

    def _stamp_lineage(self, first_new: int, frame: ReplayFrame) -> None:
        """Attach source lineage to quality events raised while handling
        ``frame``. Additive keys only (``setdefault``): an event's own
        fields are never overwritten, and ``digest`` reads none of these.

        ``replay_ts_ms`` is the frame's recorded availability time -- what
        the collector could have known when the event fired -- never the
        time replay ran.
        """
        for event in self.result.quality_events[first_new:]:
            if not isinstance(event, dict):
                continue
            event.setdefault("replay_ts_ms", frame.timestamp_ms)
            event.setdefault("replay_source_index", frame.source_index)
            event.setdefault("replay_frame_kind", frame.kind)
            if frame.connection_id is not None:
                event.setdefault("replay_connection_id", frame.connection_id)

    def run(self, source: ReplaySource | Iterable[ReplayFrame]) -> ReplayResult:
        frames = source if isinstance(source, ReplaySource) else ReplaySource(source)
        # Input provenance travels with the result. Without this, a replay
        # that skipped foreign-venue rows or dropped REST rows was
        # indistinguishable from a pristine one once ``from_directory``'s
        # source object went out of scope (see ``replay_directory``).
        self.result.skipped_rows = dict(frames.skipped_rows)
        self.result.dropped_rest_rows = dict(frames.dropped_rest_rows)
        self.result.unresolved_order_ties = frames.unresolved_order_ties()
        self.result.input_fingerprint = frames.input_fingerprint()
        for frame in frames:
            self.result.frames_total += 1
            if frame.truncated:
                self.result.frames_truncated += 1
            first_new_event = len(self.result.quality_events)
            self._dispatch(frame)
            self._stamp_lineage(first_new_event, frame)
        self._drain_book_quality()
        self.result.final_state = self.book.state.state.value
        return self.result

    def _dispatch(self, frame: ReplayFrame) -> None:
        self._frame_ts_ms = frame.timestamp_ms
        if frame.kind == FrameKind.WIRE:
            self._handle_wire(frame)
        elif frame.kind == FrameKind.REST_SNAPSHOT:
            self._handle_snapshot(frame)
        elif frame.kind == FrameKind.REST_OI:
            self._handle_rest_oi(frame)
        else:
            self.result.quality_events.append({
                "event_type": QualityEventType.DATA_DROP.value,
                "reason": f"replay_unknown_frame_kind:{frame.kind}",
                "quality_state": self.book.state.state.value,
            })


def replay_directory(
    data_dir: str, date: str | None = None, venue: str = "BINANCE"
) -> ReplayResult:
    """Convenience: replay ``venue``'s recorded segments from disk.

    The venue selects both the stream directories read and the adapter that
    replays them, so a venue's frames are never replayed by another's adapter.
    """
    return ReplayEngine(venue=venue).run(
        ReplaySource.from_directory(data_dir, date=date, venue=venue))
