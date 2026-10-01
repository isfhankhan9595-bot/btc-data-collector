"""Clock domains and timestamp precision (P0-11).

Three clock domains exist in this collector and they are never
interchangeable:

* **Exchange clock** -- whatever the venue stamped on a message. Source
  evidence. Every venue this collector consumes stamps in *milliseconds*
  (see ``EXCHANGE_TS_PRECISION``); converting such a value to nanoseconds
  (``ns_from_ms``) changes its representation, never its precision. A value
  stored in a nanosecond column is NOT thereby a nanosecond measurement.
* **Local wall clock** (``time.time_ns()``) -- epoch-referenced, can step
  (NTP, manual set). Used for the *local receive* stamp, the only local
  time that is persisted and compared across processes.
* **Local monotonic clock** (``time.monotonic_ns()``) -- meaningful only as
  a difference within one process run. Its zero point is arbitrary and is
  not preserved across restarts. It is stored only as ``receive_mono_ns``
  for intra-run delay measurement and must never be written to, compared
  with, or converted into an epoch column.

The receive stamp is taken once, at the receive boundary, before decode,
capture, or queueing (see websocket_client). Later stages never re-stamp:
a queue delay must not move the receive time.

No wall<->monotonic offset is computed anywhere. ``wall - monotonic`` is not
stable (the wall clock steps), and no consumer in this repository needs it;
adding one would invite exactly the clock mixing this module exists to
prevent.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Optional

__all__ = [
    "EXCHANGE_TS_PRECISION",
    "LOCAL_RECEIVE_PRECISION_NS",
    "LOCAL_RECEIVE_PRECISION_MS",
    "ReceiveStamp",
    "capture_receive_stamp",
    "ns_from_ms",
    "ms_from_ns",
    "require_epoch_ns",
    "effective_ns",
    "local_receive_precision_for",
]

#: All supported venues stamp exchange event times in milliseconds. This is
#: a property of the *source*; it is recorded next to any stored exchange
#: timestamp so nanosecond-typed storage can never be mistaken for a
#: nanosecond-precision measurement.
EXCHANGE_TS_PRECISION = "ms"

#: Local receive stamp taken from the OS wall clock in integer nanoseconds.
#: Actual resolution is whatever the OS clock provides (never coarser than
#: the recorded value's granularity); it is a local observation, not an
#: exchange one.
LOCAL_RECEIVE_PRECISION_NS = "ns_local_clock"

#: Legacy rows (and REST rows not yet upgraded) only ever had milliseconds.
LOCAL_RECEIVE_PRECISION_MS = "ms"

_NS_PER_MS = 1_000_000


def require_epoch_ns(value: object, name: str = "epoch_ns") -> int:
    """Return ``value`` if it is a valid epoch-nanosecond integer.

    Rejects ``bool`` (an ``int`` subclass that is never a timestamp),
    non-integers (including floats: a float cannot hold an epoch-ns exactly
    and silently rounding it is precisely the loss this module prevents),
    and negatives.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an int, got {type(value).__name__}: {value!r}")
    if value < 0:
        raise ValueError(f"{name} must be non-negative, got {value}")
    return value


def ns_from_ms(ms: object, name: str = "epoch_ms") -> int:
    """Exact representation change ms -> ns. Adds NO precision.

    ``1750000000123`` ms becomes ``1750000000123000000`` ns -- still a
    millisecond-precision value; callers that persist the result must also
    persist ``EXCHANGE_TS_PRECISION`` (or ``LOCAL_RECEIVE_PRECISION_MS``).
    """
    if isinstance(ms, bool) or not isinstance(ms, int):
        raise ValueError(f"{name} must be an int, got {type(ms).__name__}: {ms!r}")
    if ms < 0:
        raise ValueError(f"{name} must be non-negative, got {ms}")
    return ms * _NS_PER_MS


def ms_from_ns(ns: int) -> int:
    """Floor conversion ns -> ms, consistent with the legacy ms column."""
    return require_epoch_ns(ns) // _NS_PER_MS


@dataclass(frozen=True)
class ReceiveStamp:
    """One receive-boundary observation in two distinct clock domains."""

    wall_ns: int   # epoch nanoseconds, local wall clock
    mono_ns: int   # monotonic nanoseconds, this process run only

    def __post_init__(self) -> None:
        require_epoch_ns(self.wall_ns, "wall_ns")
        if isinstance(self.mono_ns, bool) or not isinstance(self.mono_ns, int):
            raise ValueError(f"mono_ns must be an int, got {self.mono_ns!r}")

    @property
    def wall_ms(self) -> int:
        """Legacy millisecond receive time, derived from the same clock read
        as ``wall_ns`` so the two can never disagree."""
        return self.wall_ns // _NS_PER_MS


def capture_receive_stamp(
    wall_ns_fn: Callable[[], int] = time.time_ns,
    mono_ns_fn: Callable[[], int] = time.monotonic_ns,
) -> ReceiveStamp:
    """Read both clocks back-to-back. Direct clock calls only: this runs
    once per received frame, so no datetime/pandas/format work belongs here.
    """
    wall = wall_ns_fn()
    mono = mono_ns_fn()
    return ReceiveStamp(wall_ns=wall, mono_ns=mono)


def effective_ns(ms: int, ns: Optional[int]) -> int:
    """Ordering key in ns: the recorded ns when present, else the ms value
    expressed in ns. A legacy ms-only row therefore sorts at the *start* of
    its millisecond -- a representation choice, not a claim about where in
    the millisecond it arrived (see ``local_receive_precision_for``).
    """
    if ns is not None:
        return require_epoch_ns(ns)
    return ns_from_ms(ms)


def local_receive_precision_for(ns: Optional[int]) -> str:
    return LOCAL_RECEIVE_PRECISION_NS if ns is not None else LOCAL_RECEIVE_PRECISION_MS
