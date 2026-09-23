"""Exchange adapter contract.

The important rule enforced here is that an adapter may not drop a message
in silence. Every adapter previously ended ``normalize()`` in a bare
``return []``, so an unroutable frame, an unimplemented channel or a
structurally malformed payload produced exactly the same result as a frame
carrying no events: nothing, with no counter, no quality event and no log.
That is silent data loss, and it made "this channel is supported" and "this
channel is silently discarded" indistinguishable from the outside.

Adapters now route every non-event outcome through :meth:`unhandled`, which
records an :class:`UnhandledMessage` and forwards it to an optional sink so
the collector can persist it as a durable quality event. ``normalize()``
still returns ``list``, so existing callers are unaffected.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
import functools
from dataclasses import dataclass, replace as _replace
from enum import Enum
from typing import Any, Callable, Iterable, Optional

from ..canonical import CanonicalEvent, CanonicalTradeEvent
from ..instrument import InstrumentId, InstrumentIdError

#: Bounded so a misbehaving venue cannot grow this without limit.
UNHANDLED_BUFFER_MAX = 256

#: Matches pipeline.cross_exchange_alignment.UNIDENTIFIED's value, but
#: defined independently here rather than imported: adapters/ is lower in
#: the dependency graph than pipeline/ (pipeline already imports from
#: collector/, not the reverse), and this module has no other reason to
#: depend on the alignment module. Same sentinel value, no coupling.
_UNIDENTIFIED = "<unidentified>"


class UnhandledReason(str, Enum):
    """Why a message produced no canonical events."""

    #: The frame did not match any channel this adapter routes.
    NO_ROUTE = "no_route"
    #: The channel is declared in ``channel_event_types`` but not implemented.
    CHANNEL_NOT_IMPLEMENTED = "channel_not_implemented"
    #: Routed correctly, but the payload was structurally unusable.
    MALFORMED_PAYLOAD = "malformed_payload"
    #: A venue control frame (subscribe ack, pong, error envelope).
    CONTROL_FRAME = "control_frame"
    #: Routed and well formed, but carried an empty data array.
    EMPTY_DATA = "empty_data"
    #: A well-formed trade event whose (exchange, market_type, instrument,
    #: stream, trade_id) tuple was already seen by this adapter instance.
    #: Distinct from every reason above: normalize() DID produce a valid
    #: event here -- nothing was malformed or unroutable -- it was
    #: suppressed because it is redundant, not because it was unusable.
    DUPLICATE_TRADE = "duplicate_trade"


@dataclass(frozen=True)
class UnhandledMessage:
    """A message an adapter could not turn into canonical events."""

    venue: str
    reason: UnhandledReason
    channel: Optional[str] = None
    detail: Optional[str] = None
    local_receive_ts: Optional[int] = None
    #: Top-level keys only. The full payload belongs in raw capture, not here.
    payload_keys: tuple[str, ...] = ()

    def to_quality_event(self) -> dict[str, Any]:
        # A duplicate trade is categorically different from every other
        # reason here: normalize() DID produce a valid event, so nothing was
        # actually lost -- the first occurrence already captured this trade.
        # DATA_DROP would misrepresent it as a loss; the existing DUPLICATE
        # quality-event type (already used for book-level duplicate updates,
        # see run_collector.py/replay.py) is the correct, already-existing
        # taxonomy value, reused rather than inventing a new one.
        event_type = "DUPLICATE" if self.reason is UnhandledReason.DUPLICATE_TRADE else "DATA_DROP"
        rows_lost = 0 if self.reason is UnhandledReason.DUPLICATE_TRADE else 1
        return {
            "exchange": self.venue,
            "stream": self.channel or "unrouted",
            "event_type": event_type,
            "reason": f"adapter_unhandled:{self.reason.value}"
            + (f":{self.detail}" if self.detail else ""),
            "rows_lost": rows_lost,
            "local_receive_ts": self.local_receive_ts,
            "local_ts": self.local_receive_ts,
        }


class ExchangeAdapter(ABC):
    venue: str = "UNKNOWN"
    channel_event_types: dict[str, tuple[str, ...]] = {}
    #: Channels declared above but not yet implemented by ``normalize()``.
    #: Declaring them here makes the gap explicit in code and in tests
    #: instead of leaving it to be discovered by reading an early return.
    unimplemented_channels: frozenset[str] = frozenset()
    #: The instrument this adapter's events are about; ``None`` when the adapter
    #: is configured for an instrument outside the registry (never a fake one).
    instrument: Optional[InstrumentId] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Make every concrete ``normalize()`` stamp its events with the adapter's
        instrument and suppress duplicate trade messages, so no adapter
        (present or future) can forget either. Order matters: instrument
        stamping runs first, so the duplicate key below can use the
        already-resolved ``event.instrument`` rather than re-deriving it."""
        super().__init_subclass__(**kwargs)
        impl = cls.__dict__.get("normalize")
        if impl is None or getattr(impl, "_stamps_instrument", False):
            return

        @functools.wraps(impl)
        def normalize(self, raw, *, local_receive_ts=None):
            events = impl(self, raw, local_receive_ts=local_receive_ts)
            return self._dedupe_trades(self._stamp_instrument(events))

        normalize._stamps_instrument = True  # type: ignore[attr-defined]
        cls.normalize = normalize  # type: ignore[method-assign]

    def _instrument_scoped(self, event: Any) -> bool:
        """Whether ``event`` is about this adapter's instrument. Override where a
        channel is not scoped to it (see the OKX adapter)."""
        return True

    def _stamp_instrument(self, events: Any) -> Any:
        instrument = self.instrument
        if instrument is None or not isinstance(events, list):
            return events
        stamped = []
        for event in events:
            if isinstance(event, CanonicalEvent) and event.instrument is None and self._instrument_scoped(event):
                if (event.exchange, event.market_type) != (instrument.exchange, instrument.market_type):
                    raise InstrumentIdError(
                        f"{type(self).__name__} produced an event for ({event.exchange!r}, "
                        f"{event.market_type!r}) but is bound to {instrument.key}")
                event = _replace(event, instrument=instrument)
            stamped.append(event)
        return stamped

    def _dedupe_trades(self, events: Any) -> Any:
        """Suppress a trade whose (exchange, market_type, instrument, stream,
        trade_id) tuple this adapter instance has already produced.

        Deliberately narrow, per this task's own conservatism requirements:

        * Only ``CanonicalTradeEvent`` is touched. Books already have their
          own, unrelated duplicate mechanism (``LocalBook.duplicate_count``,
          an update_id-range check, not identity-based); every other event
          type is untouched here.
        * A ``trade_id is None`` event is NEVER deduplicated against
          anything, including other ``None`` trades -- it is always kept.
          Collapsing all unidentified trades together would be exactly the
          catastrophic "None in seen_ids => duplicate" bug this must avoid.
        * The key includes ``stream`` as well as identity, so two distinct
          trade *representations* of what might be the same underlying
          venue activity (Binance USD-M's aggTrade-based ``trades`` vs
          Binance Spot's ordinary-trade ``spot_trades``; OKX's ``trades``
          vs ``trades-all``, whose overlap is explicitly an open question
          per adapters/okx.py's own docstring) are never merged into one
          identity space. Two representations are conflated only if this
          method is never asked to -- it isn't, by construction.
        * State lives exactly as long as this adapter instance does: one
          runner process (surviving reconnects, since the runner does not
          recreate its adapter on reconnect) or one ReplayEngine.run() call
          (spanning every frame/segment passed to it) -- this is a property
          of adapter lifetime, not something this method manages, so
          reconnect-overlap and segment/hour-boundary duplicates are both
          caught for free as long as the caller does not construct a new
          adapter mid-stream (none currently do).
        * Known, documented cost: ``_seen_trade_ids`` grows without bound
          for the adapter's lifetime -- unsafe for an unbounded live
          process over very long runs. Deliberately not optimized here
          (this task's own instruction: keep the simple, correct reference
          implementation over an unproven bounded structure); flagged as
          the natural next bounded task.
        """
        if not isinstance(events, list):
            return events
        kept = []
        for event in events:
            if not isinstance(event, CanonicalTradeEvent) or event.trade_id is None:
                kept.append(event)
                continue
            key = (event.exchange, event.market_type,
                   event.instrument.key if event.instrument is not None else _UNIDENTIFIED,
                   event.stream, event.trade_id)
            if key in self._seen_trade_ids:
                self.unhandled(UnhandledReason.DUPLICATE_TRADE, detail=event.trade_id,
                                channel=event.stream, local_receive_ts=event.local_receive_ts)
                continue
            self._seen_trade_ids.add(key)
            kept.append(event)
        return kept

    def __init__(self) -> None:
        self._unhandled: deque[UnhandledMessage] = deque(maxlen=UNHANDLED_BUFFER_MAX)
        self._unhandled_sink: Optional[Callable[[UnhandledMessage], None]] = None
        self.unhandled_count = 0
        self.unhandled_dropped = 0
        #: See _dedupe_trades's docstring for the identity key, lifetime,
        #: and unbounded-growth caveat.
        self._seen_trade_ids: set[tuple] = set()

    # -- unhandled plumbing ----------------------------------------------

    def set_unhandled_sink(self, sink: Optional[Callable[[UnhandledMessage], None]]) -> None:
        self._unhandled_sink = sink

    def unhandled(
        self,
        reason: UnhandledReason,
        raw: Any = None,
        *,
        channel: Optional[str] = None,
        detail: Optional[str] = None,
        local_receive_ts: Optional[int] = None,
    ) -> list[Any]:
        """Record a non-event outcome and return an empty event list.

        Returns ``[]`` so call sites read as ``return self.unhandled(...)``,
        preserving the existing signature while making the drop observable.
        """
        keys: tuple[str, ...] = ()
        if isinstance(raw, dict):
            keys = tuple(sorted(str(key) for key in raw.keys()))
        message = UnhandledMessage(
            venue=self.venue,
            reason=reason,
            channel=channel,
            detail=detail,
            local_receive_ts=local_receive_ts,
            payload_keys=keys,
        )
        if len(self._unhandled) == self._unhandled.maxlen:
            # The deque is about to evict its oldest entry. Count it so the
            # loss of an unhandled record is itself not silent.
            self.unhandled_dropped += 1
        self._unhandled.append(message)
        self.unhandled_count += 1
        if self._unhandled_sink is not None:
            try:
                self._unhandled_sink(message)
            except Exception:  # noqa: BLE001 - sink must never break ingest
                pass
        return []

    def drain_unhandled(self) -> list[UnhandledMessage]:
        drained = list(self._unhandled)
        self._unhandled.clear()
        return drained

    def declared_channels(self) -> frozenset[str]:
        return frozenset(self.channel_event_types)

    def implemented_channels(self) -> frozenset[str]:
        return self.declared_channels() - self.unimplemented_channels

    # -- contract ---------------------------------------------------------

    @abstractmethod
    def connect(self): ...

    @abstractmethod
    def subscribe_message(self, streams: Iterable[str]): ...

    @abstractmethod
    def route_message(self, raw: dict) -> Optional[str]: ...

    @abstractmethod
    def normalize(self, raw: dict, *, local_receive_ts: Optional[int] = None) -> list[Any]: ...
