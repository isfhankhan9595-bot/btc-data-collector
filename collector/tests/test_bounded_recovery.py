"""Adversarial tests for bounded recovery, jittered backoff and rate limiting.

Invariants:

1. One sequence gap cannot become many REST snapshot requests.
2. Reconnect delays are jittered and attempt-capped, so clients that drop
   together do not retry together forever.
3. A venue rate limit (429/418) overrides local pacing.
4. Suppression is counted and observable, but does not itself become a
   storm of quality events.
"""
from __future__ import annotations

import random

import pytest

from collector.collector.backoff import (
    DEFAULT_418_PENALTY_S,
    DEFAULT_429_PENALTY_S,
    BackoffExhausted,
    ExponentialBackoff,
    RateLimitPenalty,
    parse_retry_after,
    rate_limit_penalty,
)
from collector.collector.recovery_control import (
    RecoveryController,
    RecoveryDecision,
)


class _Clock:
    """Controllable monotonic clock; no sleeping in tests."""

    def __init__(self, start: float = 1000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _controller(**kwargs):
    clock = _Clock()
    events: list[dict] = []
    defaults = dict(
        min_interval_s=1.0, max_per_window=5, window_s=60.0,
        clock=clock, quality_sink=events.append,
    )
    defaults.update(kwargs)
    return RecoveryController(**defaults), clock, events


# ---------------------------------------------------------------------------
# Backoff
# ---------------------------------------------------------------------------


def test_cap_grows_exponentially_and_saturates():
    backoff = ExponentialBackoff(base_delay=1.0, max_delay=8.0, multiplier=2.0)
    assert [backoff.cap_for(i) for i in range(6)] == [1.0, 2.0, 4.0, 8.0, 8.0, 8.0]


def test_full_jitter_keeps_every_delay_within_its_cap():
    backoff = ExponentialBackoff(base_delay=1.0, max_delay=32.0,
                                 rng=random.Random(7))
    for attempt in range(12):
        cap = backoff.peek_cap()
        delay = backoff.next_delay()
        assert 0.0 <= delay <= cap


def test_jittered_delays_are_not_monotonic():
    """Monotonic delays are exactly what synchronises clients."""
    backoff = ExponentialBackoff(base_delay=1.0, max_delay=64.0,
                                 rng=random.Random(3))
    delays = [backoff.next_delay() for _ in range(40)]
    assert any(b < a for a, b in zip(delays, delays[1:])), (
        "full jitter must be able to produce a shorter delay than its predecessor"
    )


def test_two_clients_with_different_seeds_do_not_retry_in_lockstep():
    a = ExponentialBackoff(rng=random.Random(1))
    b = ExponentialBackoff(rng=random.Random(2))
    assert [a.next_delay() for _ in range(8)] != [b.next_delay() for _ in range(8)]


def test_same_seed_is_deterministic():
    a = ExponentialBackoff(rng=random.Random(11))
    b = ExponentialBackoff(rng=random.Random(11))
    assert [a.next_delay() for _ in range(6)] == [b.next_delay() for _ in range(6)]


def test_attempt_budget_is_enforced():
    backoff = ExponentialBackoff(max_attempts=3, rng=random.Random(0))
    for _ in range(3):
        backoff.next_delay()
    assert backoff.exhausted
    with pytest.raises(BackoffExhausted):
        backoff.next_delay()


def test_reset_restores_the_budget():
    backoff = ExponentialBackoff(max_attempts=2, rng=random.Random(0))
    backoff.next_delay()
    backoff.next_delay()
    backoff.reset()
    assert not backoff.exhausted
    assert backoff.attempts == 0


def test_jitter_can_be_disabled_for_deterministic_callers():
    backoff = ExponentialBackoff(base_delay=1.0, max_delay=4.0, jitter=False)
    assert [backoff.next_delay() for _ in range(4)] == [1.0, 2.0, 4.0, 4.0]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"base_delay": 0}, {"base_delay": -1}, {"max_delay": 0.5},
        {"multiplier": 1.0}, {"multiplier": 0.5}, {"max_attempts": 0},
    ],
)
def test_invalid_backoff_configuration_is_rejected(kwargs):
    with pytest.raises(ValueError):
        ExponentialBackoff(**kwargs)


# ---------------------------------------------------------------------------
# Rate limit parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value,expected", [
    ("30", 30.0), (30, 30.0), ("0", 0.0), (" 12 ", 12.0),
    (None, None), ("", None), ("soon", None), ("-5", None),
    ("Wed, 21 Oct 2026 07:28:00 GMT", None),  # HTTP-date form is not guessed
    ("nan", None), ("inf", None),
])
def test_retry_after_parsing(value, expected):
    assert parse_retry_after(value) == expected


def test_non_rate_limited_status_has_no_penalty():
    for status in (200, 400, 404, 500, None):
        assert rate_limit_penalty(status, {}) is None


def test_429_and_418_get_documented_defaults_without_a_header():
    assert rate_limit_penalty(429, {}).seconds == DEFAULT_429_PENALTY_S
    ban = rate_limit_penalty(418, {})
    assert ban.seconds == DEFAULT_418_PENALTY_S
    assert ban.is_ban is True
    # A ban must be treated more seriously than a warning.
    assert DEFAULT_418_PENALTY_S > DEFAULT_429_PENALTY_S


def test_venue_retry_after_overrides_the_local_default():
    penalty = rate_limit_penalty(429, {"Retry-After": "7"})
    assert penalty.seconds == 7.0
    assert penalty.source == "retry_after"


def test_retry_after_header_is_matched_case_insensitively():
    assert rate_limit_penalty(429, {"retry-after": "5"}).seconds == 5.0


def test_unparseable_retry_after_falls_back_rather_than_guessing():
    penalty = rate_limit_penalty(418, {"Retry-After": "tomorrow"})
    assert penalty.source == "default"
    assert penalty.seconds == DEFAULT_418_PENALTY_S


# ---------------------------------------------------------------------------
# Recovery deduplication: the headline defect
# ---------------------------------------------------------------------------


def test_one_gap_does_not_become_hundreds_of_snapshot_requests():
    """The D13 headline: a burst of gaps yields exactly one attempt."""
    controller, clock, _ = _controller()
    assert controller.request("gap").allowed
    controller.begin()

    for _ in range(500):
        verdict = controller.request("gap")
        assert verdict.decision is RecoveryDecision.IN_FLIGHT

    assert controller.counters.allowed == 1
    assert controller.counters.suppressed_in_flight == 500


def test_cooldown_spaces_consecutive_attempts():
    controller, clock, _ = _controller(min_interval_s=5.0)
    assert controller.request().allowed
    controller.begin()
    controller.succeed()

    assert controller.request().decision is RecoveryDecision.COOLDOWN
    clock.advance(4.9)
    assert controller.request().decision is RecoveryDecision.COOLDOWN
    clock.advance(0.2)
    assert controller.request().allowed


def test_window_ceiling_caps_the_request_rate():
    controller, clock, _ = _controller(min_interval_s=0.0, max_per_window=3, window_s=60.0)
    for _ in range(3):
        assert controller.request().allowed
        controller.begin()
        controller.succeed()
        clock.advance(1.0)

    assert controller.request().decision is RecoveryDecision.WINDOW_EXHAUSTED
    # The window is rolling, not fixed.
    clock.advance(60.0)
    assert controller.request().allowed


def test_window_verdict_reports_how_long_to_wait():
    controller, clock, _ = _controller(min_interval_s=0.0, max_per_window=1, window_s=30.0)
    controller.request(); controller.begin(); controller.succeed()
    verdict = controller.request()
    assert verdict.decision is RecoveryDecision.WINDOW_EXHAUSTED
    assert 0 < verdict.wait_seconds <= 30.0


def test_failures_extend_the_gate_via_backoff():
    controller, clock, _ = _controller(
        min_interval_s=1.0,
        backoff=ExponentialBackoff(base_delay=10.0, max_delay=10.0, jitter=False),
    )
    controller.request(); controller.begin()
    delay = controller.fail("snapshot_timeout")
    assert delay == 10.0
    # The backoff is enforced through the gate, so ignoring the returned
    # delay does not let a caller retry immediately.
    assert controller.request().decision is RecoveryDecision.COOLDOWN
    clock.advance(10.1)
    assert controller.request().allowed


def test_success_resets_the_failure_budget():
    controller, clock, _ = _controller(min_interval_s=0.0)
    controller.request(); controller.begin(); controller.fail("x")
    clock.advance(100)
    assert controller.consecutive_failures == 1
    controller.request(); controller.begin(); controller.succeed()
    assert controller.consecutive_failures == 0
    assert controller.backoff.attempts == 0


def test_attempt_budget_exhaustion_stops_retrying_and_is_recorded():
    controller, clock, events = _controller(
        min_interval_s=0.0,
        backoff=ExponentialBackoff(base_delay=1.0, max_delay=1.0,
                                   max_attempts=3, jitter=False),
    )
    for _ in range(3):
        controller.request(); controller.begin(); controller.fail("boom")
        clock.advance(100)
    assert controller.request().decision is RecoveryDecision.ATTEMPTS_EXHAUSTED
    assert any("exhausted" in e["reason"] for e in events)


# ---------------------------------------------------------------------------
# Venue rate limits override local pacing
# ---------------------------------------------------------------------------


def test_venue_rate_limit_blocks_until_the_penalty_expires():
    controller, clock, events = _controller(min_interval_s=0.0)
    controller.request(); controller.begin()
    controller.note_rate_limit(RateLimitPenalty(seconds=30.0, status=429, source="retry_after"))

    assert controller.in_flight is False
    verdict = controller.request()
    assert verdict.decision is RecoveryDecision.RATE_LIMITED
    assert verdict.wait_seconds == pytest.approx(30.0, abs=0.01)

    clock.advance(30.1)
    assert controller.request().allowed
    assert any("venue_rate_limit:429" in e["reason"] for e in events)


def test_a_ban_penalty_is_not_shortened_by_a_later_warning():
    controller, clock, _ = _controller(min_interval_s=0.0)
    controller.note_rate_limit(RateLimitPenalty(300.0, 418, "default"))
    controller.note_rate_limit(RateLimitPenalty(5.0, 429, "default"))
    clock.advance(10.0)
    # The longer ban still applies.
    assert controller.request().decision is RecoveryDecision.RATE_LIMITED


def test_rate_limit_is_counted():
    controller, _, _ = _controller()
    controller.note_rate_limit(RateLimitPenalty(1.0, 429, "default"))
    assert controller.stats()["rate_limits_seen"] == 1


# ---------------------------------------------------------------------------
# Observability without a second storm
# ---------------------------------------------------------------------------


def test_suppression_does_not_emit_one_event_per_suppressed_request():
    """Replacing a REST storm with a quality-event storm is the same bug."""
    controller, _, events = _controller()
    controller.request(); controller.begin()
    for _ in range(1000):
        controller.request()

    assert controller.counters.suppressed_in_flight == 1000
    # One transition event, not one thousand.
    assert len(events) == 1


def test_resumption_emits_one_event_carrying_the_magnitude():
    controller, clock, events = _controller(min_interval_s=0.0)
    controller.request(); controller.begin()
    for _ in range(50):
        controller.request()
    controller.succeed()

    resumed = [e for e in events if e["reason"] == "recovery_resumed"]
    assert len(resumed) == 1
    assert resumed[0]["rows_lost"] == 50


def test_every_suppression_reason_is_counted_separately():
    controller, clock, _ = _controller(min_interval_s=5.0, max_per_window=2, window_s=60.0)
    controller.request(); controller.begin()
    controller.request()                      # in flight
    controller.succeed()
    controller.request()                      # cooldown
    clock.advance(10)
    controller.request(); controller.begin(); controller.succeed()
    clock.advance(10)
    controller.request()                      # window exhausted
    stats = controller.stats()
    assert stats["suppressed_in_flight"] >= 1
    assert stats["suppressed_cooldown"] >= 1
    assert stats["suppressed_window"] >= 1
    assert stats["suppressed_total"] == (
        stats["suppressed_in_flight"] + stats["suppressed_cooldown"]
        + stats["suppressed_rate_limited"] + stats["suppressed_window"]
        + stats["suppressed_exhausted"]
    )


def test_a_raising_quality_sink_cannot_break_recovery():
    def bad(_):
        raise RuntimeError("sink down")

    controller = RecoveryController(clock=_Clock(), quality_sink=bad)
    controller.request()
    controller.begin()
    for _ in range(5):
        controller.request()  # must not raise


def test_begin_without_allow_is_a_programming_error():
    controller, _, _ = _controller()
    controller.request(); controller.begin()
    with pytest.raises(RuntimeError):
        controller.begin()


@pytest.mark.parametrize("kwargs", [
    {"min_interval_s": -1}, {"max_per_window": 0}, {"window_s": 0},
])
def test_invalid_controller_configuration_is_rejected(kwargs):
    with pytest.raises(ValueError):
        RecoveryController(**kwargs)


# ---------------------------------------------------------------------------
# Integration: websocket reconnect + collector recovery scheduling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_websocket_reconnect_is_jittered_and_budget_capped(monkeypatch):
    """Replaces an older test that required strictly increasing delays.

    That contract is incompatible with jitter, and jitter is the point:
    a monotonic delay series synchronises every client that dropped at the
    same moment. The correct contract is bounded-by-cap, budget-capped, and
    observable on exhaustion.
    """
    from collector.collector.websocket_client import WebSocketClient
    from unittest.mock import AsyncMock

    attempts = 0

    async def always_fail(url, **kwargs):
        nonlocal attempts
        attempts += 1
        raise ConnectionRefusedError("down")

    quality: list[tuple] = []
    client = WebSocketClient(
        "ws://localhost:9999", AsyncMock(),
        on_quality_event=lambda *a: quality.append(a),
        backoff=ExponentialBackoff(base_delay=1.0, max_delay=8.0,
                                   max_attempts=5, rng=random.Random(42)),
    )
    client.running = True

    sleeps: list[float] = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(
        "collector.collector.websocket_client.websockets.connect", always_fail)
    monkeypatch.setattr(
        "collector.collector.websocket_client.asyncio.sleep", fake_sleep)
    await client.start()

    # The budget stops the loop rather than retrying forever.
    assert client.reconnect_budget_exhausted is True
    assert client.running is False
    assert len(sleeps) == 5
    for index, delay in enumerate(sleeps):
        assert 0.0 <= delay <= min(8.0, 1.0 * (2 ** index))
    assert any("reconnect_budget_exhausted" in str(event) for event in quality)


@pytest.mark.asyncio
async def test_a_burst_of_gaps_schedules_at_most_one_recovery_task():
    """End-to-end on the collector: gap burst -> one snapshot attempt."""
    import asyncio

    from collector import run_collector as _rc

    app = _rc.CollectorApp.__new__(_rc.CollectorApp)
    app._recovery_task = None
    app.quality_events = []
    app._persist_quality_event = app.quality_events.append
    clock = _Clock()
    app.recovery_controller = RecoveryController(
        min_interval_s=1.0, clock=clock, quality_sink=app._persist_quality_event)

    started = 0

    async def fake_recover(reason):
        nonlocal started
        started += 1
        await asyncio.sleep(0)
        return True

    app._recover_binance_book = fake_recover

    scheduled = [app._schedule_recovery("gap") for _ in range(200)]
    await asyncio.sleep(0)

    assert sum(scheduled) == 1, "a gap burst must schedule exactly one recovery"
    assert started == 1
    assert app.recovery_controller.counters.suppressed_in_flight >= 1


def test_collector_falls_back_safely_without_a_controller():
    """Legacy construction paths must not crash on the new gate."""
    import asyncio

    from collector import run_collector as _rc

    app = _rc.CollectorApp.__new__(_rc.CollectorApp)
    app._recovery_task = None
    app.recovery_controller = None

    async def fake_recover(reason):
        return True

    app._recover_binance_book = fake_recover

    async def drive():
        assert app._schedule_recovery("gap") is True
        assert app._schedule_recovery("gap") is False  # still running
        await asyncio.gather(app._recovery_task, return_exceptions=True)

    asyncio.run(drive())
