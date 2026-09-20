"""Causal alignment of canonical events by what the collector had *received*.

The question this module answers is: "which observations had this collector
actually received by time T?" -- never "which exchange timestamps look closest
to T". Exchanges are not synchronised with each other or with us, so nothing
here treats two venues as observed simultaneously.

Rules
-----
* **Availability** is ``event.local_receive_ts <= observation_ts`` (inclusive).
  ``exchange_event_ts`` and ``exchange_transaction_ts`` never affect
  eligibility: an event stamped an hour ago but received after T is invisible
  at T, and one stamped in the future but received by T is visible. Both
  exchange timestamps are preserved verbatim on the original event.
* **Identity** is ``(exchange, market_type, stream)``, so a venue's perpetual
  and spot feeds of the same stream name cannot collide.
* **Latest wins**, per key, among eligible events. There is no nearest-
  timestamp matching and no interpolation.
* **Staleness is explicit.** The caller must pass ``staleness_ms``; a stale
  observation is returned as STALE, never dropped and never relabelled fresh.
* **Missingness is explicit.** A key the caller listed in ``expected_keys``
  with no eligible event is NEVER_OBSERVED (event ``None``). A key nobody asked
  for and nobody observed is simply absent: missing is not zero, not stale, and
  never a synthetic value.
* **Availability is not quality.** The original event is returned untouched, so
  its ``quality_state`` (SEQUENCE_GAP, RECOVERING, ...) stays inspectable; an
  AVAILABLE observation may still be degraded.

Ties: two eligible events with the same key *and* the same ``local_receive_ts``
resolve by input order (the later one in the input wins). Everything else is
independent of input order.

Limitation (deliberate): canonical events carry no instrument/symbol field. The
collector is configured for a single instrument, so ``(exchange, market_type,
stream)`` is sufficient *today*. Once a second instrument is collected,
canonical identity must gain an instrument before cross-instrument alignment is
safe; this module does not guess one.

Pure: no clock, no network, no randomness, no global state.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Optional

__all__ = ["AlignmentKey", "AlignmentStatus", "AlignedObservation", "alignment_key", "causally_align"]

#: (exchange, market_type, stream)
AlignmentKey = tuple[str, str, str]


class AlignmentStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    STALE = "STALE"
    NEVER_OBSERVED = "NEVER_OBSERVED"


@dataclass(frozen=True)
class AlignedObservation:
    """A canonical event plus how old it was at the observation time.

    ``event`` is the original object, unmodified; ``age_ms`` is
    ``observation_ts - event.local_receive_ts`` (``None`` when nothing was
    observed). There is deliberately no "synchronized" or "current" status.
    """

    key: AlignmentKey
    event: Optional[Any]
    age_ms: Optional[int]
    status: AlignmentStatus


def alignment_key(event: Any) -> AlignmentKey:
    return (event.exchange, event.market_type, event.stream)


def _require_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int (epoch ms), got {value!r}")
    return value


def causally_align(
    events: Iterable[Any],
    observation_ts: int,
    *,
    staleness_ms: int,
    expected_keys: Optional[Iterable[AlignmentKey]] = None,
) -> dict[AlignmentKey, AlignedObservation]:
    """Latest eligible event per ``(exchange, market_type, stream)`` at ``observation_ts``.

    ``staleness_ms`` is required (no default): whether an old observation is
    still usable is the caller's decision and must not be made silently here.
    The result is ordered by key, independent of input order.
    """
    _require_int("observation_ts", observation_ts)
    _require_int("staleness_ms", staleness_ms)
    if staleness_ms < 0:
        raise ValueError("staleness_ms must be >= 0")

    latest: dict[AlignmentKey, Any] = {}
    # sorted() is stable: equal local_receive_ts keep input order (documented tie rule).
    for event in sorted(events, key=lambda item: _require_int("local_receive_ts", item.local_receive_ts)):
        if event.local_receive_ts > observation_ts:
            break
        latest[alignment_key(event)] = event

    result: dict[AlignmentKey, AlignedObservation] = {}
    for key, event in latest.items():
        age = observation_ts - event.local_receive_ts
        status = AlignmentStatus.AVAILABLE if age <= staleness_ms else AlignmentStatus.STALE
        result[key] = AlignedObservation(key, event, age, status)
    for key in (expected_keys or ()):
        key = tuple(key)
        if key not in result:
            result[key] = AlignedObservation(key, None, None, AlignmentStatus.NEVER_OBSERVED)
    return dict(sorted(result.items()))
