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

This mirrors live exactly, where a snapshot requested during a gap arrives
asynchronously and can only bridge the book once it has actually landed. A
snapshot is therefore never visible to the engine before the moment it
arrived in the recorded run, so replay cannot repair a gap using information
from the future.

Determinism
-----------

Frames are ordered by a total key -- ``(timestamp, kind_rank, source_index)``
-- so ties never depend on filesystem iteration order, dict ordering or
sort instability. Running the same input twice produces the same
:attr:`ReplayResult.digest`.

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

    @property
    def order_key(self) -> tuple[int, int, int]:
        return (self.timestamp_ms, _KIND_RANK.get(self.kind, 9), self.source_index)


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

    @property
    def digest(self) -> str:
        """Stable hash of the full output sequence.

        Covers book states, non-book canonical events, *and* quality
        transitions, so a replay that produced the same prices via a
        different quality path -- or the same book but a changed trade,
        funding rate, OI reading, or liquidation -- does not compare equal.
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
            "final_state": self.final_state,
            "digest": self.digest,
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
        index = 0
        for row in wire_rows:
            frames.append(ReplayFrame(
                timestamp_ms=_as_ms(row.get("local_receive_ts") or row.get("timestamp")),
                kind=FrameKind.WIRE, source_index=index,
                payload=row.get("payload") or "",
                connection_id=row.get("connection_id"),
                decode_ok=bool(row.get("decode_ok", True)),
            ))
            index += 1
        for row in rest_rows:
            purpose = row.get("purpose")
            landed = row.get("response_receive_ts")
            if landed is None:
                # A request that never returned was never available live,
                # so it cannot become available in replay either.
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
                # replay it, so nothing should quietly start doing so.
                continue
            frames.append(ReplayFrame(
                timestamp_ms=_as_ms(landed), kind=kind,
                source_index=index, payload=row.get("payload") or "",
                http_ok=bool(row.get("ok", False)), endpoint=row.get("endpoint"),
            ))
            index += 1
        return cls(frames)

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
                    for row in frame.to_dict("records"):
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
            event = normalize_binance_oi(
                frame.payload, response_receive_ts=frame.timestamp_ms)
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
        snapshot_event = CanonicalOrderBookEvent(
            "BINANCE", "spot_orderbook" if is_spot else "orderbook", None, None,
            frame.timestamp_ms, market_type="spot" if is_spot else "linear_perpetual",
            local_process_ts=frame.timestamp_ms, bids=bids, asks=asks,
            update_id=last_update_id, is_snapshot=True,
            book_source="DIFF_DEPTH_RECONSTRUCTED",
        )
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

    def run(self, source: ReplaySource | Iterable[ReplayFrame]) -> ReplayResult:
        frames = source if isinstance(source, ReplaySource) else ReplaySource(source)
        for frame in frames:
            self.result.frames_total += 1
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
        self._drain_book_quality()
        self.result.final_state = self.book.state.state.value
        return self.result


def replay_directory(
    data_dir: str, date: str | None = None, venue: str = "BINANCE"
) -> ReplayResult:
    """Convenience: replay ``venue``'s recorded segments from disk.

    The venue selects both the stream directories read and the adapter that
    replays them, so a venue's frames are never replayed by another's adapter.
    """
    return ReplayEngine(venue=venue).run(
        ReplaySource.from_directory(data_dir, date=date, venue=venue))
