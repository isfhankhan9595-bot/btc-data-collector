"""Bounded retry timing: exponential backoff with jitter, and rate-limit penalties.

Two failure modes this exists to prevent:

**Reconnect storms.** The previous websocket loop doubled a delay with no
jitter and no attempt cap. Every client that lost a connection at the same
moment retried at the same moment, forever. Jitter decorrelates them; an
attempt cap means a permanently broken endpoint eventually stops being
hammered and says so.

**REST hammering.** One sequence gap could previously schedule a snapshot
request, and the next gap another, with no minimum spacing. See
:mod:`collector.collector.recovery_control` for the deduplication side;
this module supplies the timing.

Jitter strategy
---------------

Full jitter (``delay = uniform(0, cap)``) is used rather than the
"multiply the delay itself" variant, because only the *cap* should grow.
Jittering the delay in place lets one unlucky short sleep permanently
depress the series. A consequence worth stating: successive delays are
**not** monotonically increasing. That is the point -- a monotonic sequence
is exactly what synchronises clients.

The RNG is injectable so tests are deterministic without patching globals.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any, Mapping, Optional

__all__ = [
    "BackoffExhausted",
    "ExponentialBackoff",
    "RateLimitPenalty",
    "parse_retry_after",
    "rate_limit_penalty",
    "DEFAULT_429_PENALTY_S",
    "DEFAULT_418_PENALTY_S",
]

#: Conservative defaults used only when the venue sends no ``Retry-After``.
#: Binance USD-M returns 429 when a rate limit is being approached/exceeded
#: and 418 when an IP has been auto-banned for continuing past 429, so the
#: 418 penalty is deliberately much larger. When the header is present it
#: always wins -- these are fallbacks, not assumed venue semantics.
DEFAULT_429_PENALTY_S = 30.0
DEFAULT_418_PENALTY_S = 300.0


class BackoffExhausted(RuntimeError):
    """Raised when the configured attempt budget is spent."""


class ExponentialBackoff:
    """Attempt-capped exponential backoff with full jitter."""

    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
        max_attempts: Optional[int] = None,
        jitter: bool = True,
        rng: Optional[random.Random] = None,
    ) -> None:
        if base_delay <= 0:
            raise ValueError("base_delay must be positive")
        if max_delay < base_delay:
            raise ValueError("max_delay must be >= base_delay")
        if multiplier <= 1.0:
            raise ValueError("multiplier must be > 1")
        if max_attempts is not None and max_attempts <= 0:
            raise ValueError("max_attempts must be positive or None")
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.max_attempts = max_attempts
        self.jitter = jitter
        self._rng = rng or random.Random()
        self._attempts = 0

    @property
    def attempts(self) -> int:
        return self._attempts

    @property
    def exhausted(self) -> bool:
        return self.max_attempts is not None and self._attempts >= self.max_attempts

    def cap_for(self, attempt: int) -> float:
        """Uniformly-bounded ceiling for a zero-based attempt index."""
        return min(self.max_delay, self.base_delay * (self.multiplier ** attempt))

    def peek_cap(self) -> float:
        return self.cap_for(self._attempts)

    def next_delay(self) -> float:
        if self.exhausted:
            raise BackoffExhausted(
                f"retry budget of {self.max_attempts} attempts is spent"
            )
        cap = self.cap_for(self._attempts)
        self._attempts += 1
        if not self.jitter:
            return cap
        return self._rng.uniform(0.0, cap)

    def reset(self) -> None:
        self._attempts = 0


@dataclass(frozen=True)
class RateLimitPenalty:
    """How long the venue says (or we assume) we must wait."""

    seconds: float
    status: int
    source: str  # "retry_after" | "default"

    @property
    def is_ban(self) -> bool:
        return self.status == 418


def parse_retry_after(value: Any) -> Optional[float]:
    """Parse a ``Retry-After`` value expressed in seconds.

    Only the delta-seconds form is honoured. The HTTP-date form is not
    guessed at: returning ``None`` makes the caller fall back to a documented
    default rather than mis-parsing a date into a wrong delay.
    """
    if value is None:
        return None
    try:
        seconds = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    if seconds < 0 or seconds != seconds or seconds in (float("inf"), float("-inf")):
        return None
    return seconds


def rate_limit_penalty(
    status: Optional[int],
    headers: Optional[Mapping[str, Any]] = None,
    *,
    default_429: float = DEFAULT_429_PENALTY_S,
    default_418: float = DEFAULT_418_PENALTY_S,
) -> Optional[RateLimitPenalty]:
    """Return a penalty for a rate-limited response, or ``None``.

    A venue-supplied ``Retry-After`` always wins over the local default.
    """
    if status not in (429, 418):
        return None
    header_value = None
    if headers:
        for key in ("Retry-After", "retry-after", "RETRY-AFTER"):
            if key in headers:
                header_value = headers[key]
                break
    seconds = parse_retry_after(header_value)
    if seconds is not None:
        return RateLimitPenalty(seconds=seconds, status=status, source="retry_after")
    default = default_418 if status == 418 else default_429
    return RateLimitPenalty(seconds=default, status=status, source="default")
