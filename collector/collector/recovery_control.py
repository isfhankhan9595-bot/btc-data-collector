"""Bounded, deduplicated, observable recovery scheduling.

The defect this closes (D13): a sequence gap scheduled a REST snapshot, and
the guard was only "is the previous task still running". A burst of gaps
arriving faster than a snapshot completes, or a flapping connection, could
therefore drive repeated snapshot requests with no minimum spacing, no
ceiling per minute, and no response to a venue rate limit. One gap must not
become hundreds of REST calls.

Four independent limits
-----------------------

1. **Deduplication** -- at most one recovery in flight. Further requests
   while one is running are suppressed, not queued, because a second
   snapshot taken during the first adds nothing.
2. **Cooldown** -- a minimum interval between the end of one attempt and
   the start of the next.
3. **Window ceiling** -- a hard cap on attempts per rolling window, so even
   perfectly spaced attempts cannot exceed a known request rate.
4. **Backoff** -- after consecutive failures, wait longer, with jitter, up
   to an attempt budget.

A venue rate-limit response (429/418) overrides all of the above with the
penalty the venue asked for.

Observability without a second storm
------------------------------------

Every suppression is counted. Quality events are emitted on *transitions*
-- the first suppression of an episode, and the resumption -- rather than
one per suppressed request. Emitting an event per suppressed gap would
replace a REST storm with a quality-event storm, which is the same bug
wearing a different hat. The counters carry the full magnitude and are
flushed into a single summary event when the episode ends.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Deque, Optional

from .backoff import BackoffExhausted, ExponentialBackoff, RateLimitPenalty

__all__ = ["RecoveryDecision", "RecoveryVerdict", "RecoveryController"]


class RecoveryDecision(str, Enum):
    ALLOW = "allow"
    #: Another recovery is already running.
    IN_FLIGHT = "in_flight"
    #: Too soon after the previous attempt.
    COOLDOWN = "cooldown"
    #: The venue told us to back off.
    RATE_LIMITED = "rate_limited"
    #: Attempt ceiling for the rolling window reached.
    WINDOW_EXHAUSTED = "window_exhausted"
    #: The consecutive-failure budget is spent.
    ATTEMPTS_EXHAUSTED = "attempts_exhausted"


@dataclass(frozen=True)
class RecoveryVerdict:
    decision: RecoveryDecision
    reason: str = ""
    wait_seconds: float = 0.0

    @property
    def allowed(self) -> bool:
        return self.decision is RecoveryDecision.ALLOW


@dataclass
class _Counters:
    requested: int = 0
    allowed: int = 0
    suppressed_in_flight: int = 0
    suppressed_cooldown: int = 0
    suppressed_rate_limited: int = 0
    suppressed_window: int = 0
    suppressed_exhausted: int = 0
    succeeded: int = 0
    failed: int = 0
    rate_limits_seen: int = 0

    @property
    def suppressed_total(self) -> int:
        return (
            self.suppressed_in_flight + self.suppressed_cooldown
            + self.suppressed_rate_limited + self.suppressed_window
            + self.suppressed_exhausted
        )


class RecoveryController:
    """Decides whether a recovery attempt may start, and records why not."""

    def __init__(
        self,
        *,
        name: str = "binance_orderbook",
        min_interval_s: float = 1.0,
        max_per_window: int = 5,
        window_s: float = 60.0,
        backoff: Optional[ExponentialBackoff] = None,
        clock: Callable[[], float] = time.monotonic,
        quality_sink: Optional[Callable[[dict], None]] = None,
    ) -> None:
        if min_interval_s < 0:
            raise ValueError("min_interval_s must be >= 0")
        if max_per_window <= 0:
            raise ValueError("max_per_window must be positive")
        if window_s <= 0:
            raise ValueError("window_s must be positive")
        self.name = name
        self.min_interval_s = min_interval_s
        self.max_per_window = max_per_window
        self.window_s = window_s
        self.backoff = backoff or ExponentialBackoff(
            base_delay=1.0, max_delay=60.0, max_attempts=10
        )
        self._clock = clock
        self._quality_sink = quality_sink

        self._in_flight = False
        self._last_attempt_end: Optional[float] = None
        self._attempt_times: Deque[float] = deque()
        self._blocked_until: Optional[float] = None
        self._consecutive_failures = 0
        self._suppressing = False
        self.counters = _Counters()

    # -- internals --------------------------------------------------------

    def _emit(self, event_type: str, reason: str, **extra) -> None:
        if self._quality_sink is None:
            return
        payload = {
            "stream": "orderbook",
            "event_type": event_type,
            "reason": reason,
            "local_ts": int(time.time() * 1000),
        }
        payload.update(extra)
        try:
            self._quality_sink(payload)
        except Exception:  # noqa: BLE001 - observability must not break recovery
            pass

    def _prune_window(self, now: float) -> None:
        cutoff = now - self.window_s
        while self._attempt_times and self._attempt_times[0] < cutoff:
            self._attempt_times.popleft()

    def _suppress(self, decision: RecoveryDecision, reason: str, wait: float) -> RecoveryVerdict:
        counter = {
            RecoveryDecision.IN_FLIGHT: "suppressed_in_flight",
            RecoveryDecision.COOLDOWN: "suppressed_cooldown",
            RecoveryDecision.RATE_LIMITED: "suppressed_rate_limited",
            RecoveryDecision.WINDOW_EXHAUSTED: "suppressed_window",
            RecoveryDecision.ATTEMPTS_EXHAUSTED: "suppressed_exhausted",
        }[decision]
        setattr(self.counters, counter, getattr(self.counters, counter) + 1)
        # Emit only on entering suppression, never per suppressed request:
        # one event per gap would be a quality-event storm.
        if not self._suppressing:
            self._suppressing = True
            self._emit("RATE_LIMIT" if decision is RecoveryDecision.RATE_LIMITED else "RECOVERY",
                       f"recovery_suppressed:{decision.value}:{reason}")
        return RecoveryVerdict(decision, reason, wait)

    def _resume(self) -> None:
        if self._suppressing:
            self._suppressing = False
            self._emit("RECOVERY", "recovery_resumed",
                       rows_lost=self.counters.suppressed_total)

    # -- public API -------------------------------------------------------

    def request(self, reason: str = "") -> RecoveryVerdict:
        """Ask whether a recovery attempt may start now."""
        now = self._clock()
        self.counters.requested += 1
        self._prune_window(now)

        if self._in_flight:
            return self._suppress(RecoveryDecision.IN_FLIGHT, reason, 0.0)

        if self._blocked_until is not None:
            if now < self._blocked_until:
                return self._suppress(RecoveryDecision.RATE_LIMITED, reason,
                                      self._blocked_until - now)
            self._blocked_until = None

        if self.backoff.exhausted:
            return self._suppress(RecoveryDecision.ATTEMPTS_EXHAUSTED, reason, 0.0)

        if len(self._attempt_times) >= self.max_per_window:
            wait = self._attempt_times[0] + self.window_s - now
            return self._suppress(RecoveryDecision.WINDOW_EXHAUSTED, reason, max(wait, 0.0))

        if self._last_attempt_end is not None:
            elapsed = now - self._last_attempt_end
            if elapsed < self.min_interval_s:
                return self._suppress(RecoveryDecision.COOLDOWN, reason,
                                      self.min_interval_s - elapsed)

        self._resume()
        self.counters.allowed += 1
        return RecoveryVerdict(RecoveryDecision.ALLOW, reason, 0.0)

    def begin(self) -> None:
        """Mark an attempt as started. Only call after an ALLOW verdict."""
        if self._in_flight:
            raise RuntimeError("recovery already in flight")
        self._in_flight = True
        self._attempt_times.append(self._clock())

    def succeed(self) -> None:
        self._in_flight = False
        self._last_attempt_end = self._clock()
        self._consecutive_failures = 0
        self.counters.succeeded += 1
        self.backoff.reset()
        self._resume()

    def fail(self, reason: str = "") -> float:
        """Record a failed attempt; returns the delay before the next one."""
        self._in_flight = False
        self._last_attempt_end = self._clock()
        self._consecutive_failures += 1
        self.counters.failed += 1
        try:
            delay = self.backoff.next_delay()
        except BackoffExhausted:
            self._emit("ERROR", f"recovery_attempts_exhausted:{reason}")
            return 0.0
        # A failure's backoff is enforced through the cooldown gate, so a
        # caller that ignores the returned delay is still rate-limited.
        self._last_attempt_end = self._clock() + max(delay - self.min_interval_s, 0.0)
        return delay

    def note_rate_limit(self, penalty: RateLimitPenalty) -> None:
        """Honour a venue rate limit, overriding local pacing."""
        self.counters.rate_limits_seen += 1
        self._in_flight = False
        until = self._clock() + max(penalty.seconds, 0.0)
        self._blocked_until = max(self._blocked_until or 0.0, until)
        self._emit(
            "RATE_LIMIT",
            f"venue_rate_limit:{penalty.status}:{penalty.source}",
            gap_size_ms=int(penalty.seconds * 1000),
        )

    @property
    def in_flight(self) -> bool:
        return self._in_flight

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def stats(self) -> dict:
        data = vars(self.counters).copy()
        data["suppressed_total"] = self.counters.suppressed_total
        data["in_flight"] = self._in_flight
        data["consecutive_failures"] = self._consecutive_failures
        data["backoff_attempts"] = self.backoff.attempts
        return data
