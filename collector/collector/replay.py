"""Deterministic replay over recorded raw wire data.

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
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable, Iterator, Optional, Sequence

from .adapters.binance import BinanceAdapter
from .adapters.bybit import BybitAdapter
from .book_engine import LocalBook
from .canonical import CanonicalOrderBookEvent
from .quality_events import BookQuality, QualityEventType
from .storage_layout import StorageCollisionError, iter_segments

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


#: Tie-break rank. A snapshot that landed in the same millisecond as a diff is
#: ordered after it, matching live, where the diff was already in the socket
#: buffer when the HTTP response completed.
_KIND_RANK = {FrameKind.WIRE: 0, FrameKind.REST_SNAPSHOT: 1}


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
    quality_events: list[dict[str, Any]] = field(default_factory=list)
    frames_total: int = 0
    frames_wire: int = 0
    frames_snapshot: int = 0
    frames_undecodable: int = 0
    frames_unhandled: int = 0
    snapshots_applied: int = 0
    snapshots_rejected: int = 0
    final_state: str = BookQuality.VALID.value

    @property
    def digest(self) -> str:
        """Stable hash of the full output sequence.

        Covers book states *and* quality transitions, so a replay that
        produced the same prices via a different quality path does not
        compare equal.
        """
        hasher = hashlib.sha256()
        for update in self.book_updates:
            hasher.update(repr(update.digest_tuple()).encode())
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
            "quality_events": len(self.quality_events),
            "snapshots_applied": self.snapshots_applied,
            "snapshots_rejected": self.snapshots_rejected,
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
            # Only snapshot responses drive the book. Other REST purposes are
            # recorded lineage but are not inputs to reconstruction.
            if row.get("purpose") != "orderbook_snapshot":
                continue
            landed = row.get("response_receive_ts")
            if landed is None:
                # A request that never returned never bridged anything live,
                # so it must not bridge anything in replay either.
                continue
            frames.append(ReplayFrame(
                timestamp_ms=_as_ms(landed), kind=FrameKind.REST_SNAPSHOT,
                source_index=index, payload=row.get("payload") or "",
                http_ok=bool(row.get("ok", False)), endpoint=row.get("endpoint"),
            ))
            index += 1
        return cls(frames)

    @classmethod
    def from_directory(
        cls, data_dir: str, date: str | None = None
    ) -> "ReplaySource":
        """Read recorded ``raw_wire`` and ``raw_rest`` segments."""
        import pandas as pd

        def read(stream: str) -> list[dict]:
            rows: list[dict] = []
            try:
                paths = list(iter_segments(data_dir, stream, date=date))
            except StorageCollisionError as exc:
                raise StorageCollisionError(
                    f"cannot replay {stream}: ambiguous storage ({exc})"
                ) from exc
            for path in sorted(paths):
                frame = pd.read_parquet(path)
                rows.extend(frame.to_dict("records"))
            return rows

        return cls.from_records(read("raw_wire"), read("raw_rest"))


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

    def _handle_snapshot(self, frame: ReplayFrame) -> None:
        self.result.frames_snapshot += 1
        if self.venue != "BINANCE":
            # Only Binance bridges a gap with a REST snapshot; Bybit's
            # snapshot arrives on the wire itself (``type": "snapshot"``,
            # see BybitAdapter.normalize) and is handled by _handle_wire via
            # LocalBook.snapshot(), never here. A REST_SNAPSHOT frame for
            # any other venue means the recorded data does not match this
            # venue's protocol -- treating it as Binance's ``lastUpdateId``
            # format would either crash on the wrong shape or, worse, parse
            # by coincidence and silently corrupt the book.
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

        snapshot_event = CanonicalOrderBookEvent(
            self.venue, "orderbook", None, None, frame.timestamp_ms,
            local_process_ts=frame.timestamp_ms, bids=bids, asks=asks,
            update_id=last_update_id, is_snapshot=True,
            book_source="DIFF_DEPTH_RECONSTRUCTED",
        )
        before = self.book.state.state
        if not self.book.binance_snapshot(last_update_id, snapshot_event):
            after = self.book.state.state
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
            else:
                self.result.quality_events.append({
                    "event_type": QualityEventType.DATA_DROP.value,
                    "reason": f"replay_unknown_frame_kind:{frame.kind}",
                    "quality_state": self.book.state.state.value,
                })
        self._drain_book_quality()
        self.result.final_state = self.book.state.state.value
        return self.result


def replay_directory(data_dir: str, date: str | None = None) -> ReplayResult:
    """Convenience: replay recorded segments from disk."""
    return ReplayEngine().run(ReplaySource.from_directory(data_dir, date=date))
