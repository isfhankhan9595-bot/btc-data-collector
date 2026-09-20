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

from ..canonical import CanonicalEvent
from ..instrument import InstrumentId, InstrumentIdError

#: Bounded so a misbehaving venue cannot grow this without limit.
UNHANDLED_BUFFER_MAX = 256


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
        return {
            "exchange": self.venue,
            "stream": self.channel or "unrouted",
            "event_type": "DATA_DROP",
            "reason": f"adapter_unhandled:{self.reason.value}"
            + (f":{self.detail}" if self.detail else ""),
            "rows_lost": 1,
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
        instrument, so no adapter (present or future) can forget to."""
        super().__init_subclass__(**kwargs)
        impl = cls.__dict__.get("normalize")
        if impl is None or getattr(impl, "_stamps_instrument", False):
            return

        @functools.wraps(impl)
        def normalize(self, raw, *, local_receive_ts=None):
            return self._stamp_instrument(impl(self, raw, local_receive_ts=local_receive_ts))

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

    def __init__(self) -> None:
        self._unhandled: deque[UnhandledMessage] = deque(maxlen=UNHANDLED_BUFFER_MAX)
        self._unhandled_sink: Optional[Callable[[UnhandledMessage], None]] = None
        self.unhandled_count = 0
        self.unhandled_dropped = 0

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
