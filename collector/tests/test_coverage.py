"""Adversarial tests for the truthful coverage model.

The governing invariant, which every test here exists to defend:

    A coverage report may read 100% only when every millisecond of the
    requested window is backed by an observation within tolerance.

The historical failure this replaces reported 100% for three observations
one second apart in a 24-hour day.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

import pytest

from collector.collector.coverage import (
    CoverageStatus,
    GapKind,
    compute_coverage,
    day_window_ms,
    normalize_observations,
    window_from_dates,
)

DAY_MS = 24 * 60 * 60 * 1000
DAY = "2026-06-03"
START, END = day_window_ms(DAY)
TOL = 5000  # trades-like tolerance


def cov(observations, *, start=START, end=END, tolerance=TOL, **kwargs):
    return compute_coverage(
        observations,
        window_start_ms=start,
        window_end_ms=end,
        tolerance_ms=tolerance,
        stream="trades",
        **kwargs,
    )


# ---------------------------------------------------------------------------
# 1-3: the headline false-clean scenarios
# ---------------------------------------------------------------------------


def test_completely_absent_day_reports_zero_not_hundred():
    report = cov([], source_count=0)
    assert report.coverage_pct == 0.0
    assert report.is_complete is False
    assert report.status is CoverageStatus.NO_SOURCES
    assert report.gap_count == 1
    assert report.gaps[0].kind is GapKind.ABSENT
    assert report.gaps[0].duration_ms == DAY_MS
    assert report.missing_ms == DAY_MS


def test_sources_exist_but_no_observations_is_absent_not_clean():
    report = cov([], source_count=3)
    assert report.status is CoverageStatus.ABSENT
    assert report.coverage_pct == 0.0
    assert report.is_trustworthy is False


def test_one_hour_present_23_hours_missing_cannot_report_complete():
    hour_ms = 60 * 60 * 1000
    # Dense observations for exactly one hour, then nothing.
    stamps = list(range(START, START + hour_ms, 1000))
    report = cov(stamps, source_count=1)

    assert report.is_complete is False
    # One hour of evidence plus one tolerance of trailing freshness.
    assert report.covered_ms == pytest.approx(hour_ms - 1000 + TOL, abs=1)
    assert report.coverage_pct < 4.3
    assert report.coverage_pct > 4.1
    assert report.trailing_gap_ms > 22 * hour_ms


def test_three_observations_over_two_seconds_is_not_full_coverage():
    """The exact scenario the old formula reported as 100%."""
    stamps = [START, START + 1000, START + 2000]
    report = cov(stamps, source_count=1)

    assert report.is_complete is False
    assert report.coverage_pct < 0.01
    assert report.covered_ms == 2000 + TOL
    assert report.status is CoverageStatus.PARTIAL


def test_three_observations_one_millisecond_apart_is_not_full_coverage():
    """Old formula: zero inter-observation gaps, therefore 100%."""
    stamps = [START, START + 1, START + 2]
    report = cov(stamps, source_count=1)
    assert report.coverage_pct < 0.01
    assert report.is_complete is False


# ---------------------------------------------------------------------------
# 4-7: gap position and classification
# ---------------------------------------------------------------------------


def test_leading_gap_is_counted_and_classified():
    offset = 3 * 60 * 60 * 1000  # data starts 3h into the day
    stamps = list(range(START + offset, END, 1000))
    report = cov(stamps)

    leading = report.gaps_of_kind(GapKind.LEADING)
    assert len(leading) == 1
    assert leading[0].start_ms == START
    assert leading[0].duration_ms == offset
    assert report.is_complete is False


def test_trailing_gap_is_counted_and_classified():
    stop = END - 2 * 60 * 60 * 1000  # data stops 2h before day end
    stamps = list(range(START, stop, 1000))
    report = cov(stamps)

    trailing = report.gaps_of_kind(GapKind.TRAILING)
    assert len(trailing) == 1
    assert trailing[0].end_ms == END
    assert report.is_complete is False


def test_interior_gap_is_counted_and_classified():
    first = list(range(START, START + 60_000, 1000))
    second = list(range(START + 600_000, END, 1000))
    report = cov(first + second)

    interior = report.gaps_of_kind(GapKind.INTERIOR)
    assert len(interior) == 1
    assert interior[0].duration_ms == 600_000 - 60_000 + 1000 - TOL
    assert report.leading_gap_ms == 0


def test_multiple_gaps_are_each_reported():
    stamps = []
    for block in range(4):
        base = START + block * 3 * 60 * 60 * 1000
        stamps.extend(range(base, base + 60_000, 1000))
    report = cov(stamps)

    assert report.gap_count >= 4
    assert report.interior_gap_ms > 0
    assert report.trailing_gap_ms > 0
    # Gaps plus coverage must exactly reconstruct the window.
    assert report.total_gap_ms + report.covered_ms == DAY_MS


# ---------------------------------------------------------------------------
# 8-10: sparsity and threshold boundaries
# ---------------------------------------------------------------------------


def test_sparse_observations_cover_only_their_tolerance_windows():
    stamps = [START + i * 60_000 for i in range(10)]  # one per minute
    report = cov(stamps)
    assert report.covered_ms == 10 * TOL
    assert report.coverage_pct < 0.06


def test_observation_exactly_on_tolerance_boundary_is_continuous():
    stamps = [START, START + TOL]
    report = cov(stamps)
    # Intervals [t, t+TOL) and [t+TOL, t+2TOL) abut exactly: no interior gap.
    assert report.interior_gap_ms == 0
    assert report.covered_ms == 2 * TOL


def test_observation_just_outside_tolerance_creates_a_one_ms_gap():
    stamps = [START, START + TOL + 1]
    report = cov(stamps)
    interior = report.gaps_of_kind(GapKind.INTERIOR)
    assert len(interior) == 1
    assert interior[0].duration_ms == 1


# ---------------------------------------------------------------------------
# 11-15: input hygiene, never silent
# ---------------------------------------------------------------------------


def test_duplicate_timestamps_are_counted_not_silently_collapsed():
    stamps = [START, START, START, START + 1000]
    report = cov(stamps)
    assert report.duplicate_observation_count == 2
    assert report.observation_count == 2


def test_out_of_order_timestamps_are_counted_and_sorted():
    """out_of_order counts descents: arrivals earlier than their predecessor."""
    stamps = [START + 3000, START + 1000, START + 2000]
    report = cov(stamps)
    assert report.out_of_order_count == 1  # only 3000 -> 1000 descends
    assert report.first_observation_ms == START + 1000
    assert report.last_observation_ms == START + 3000


def test_every_descent_is_counted():
    stamps = [START + 5000, START + 1000, START + 6000, START + 2000]
    report = cov(stamps)
    assert report.out_of_order_count == 2


def test_timezone_aware_datetimes_normalize_to_utc():
    tokyo = timezone(timedelta(hours=9))
    base = datetime(2026, 6, 3, 9, 0, 0, tzinfo=tokyo)  # 00:00 UTC
    report = cov([base, base + timedelta(seconds=1)])
    assert report.first_observation_ms == START


def test_naive_datetimes_are_interpreted_as_utc():
    naive = datetime(2026, 6, 3, 0, 0, 0)
    report = cov([naive])
    assert report.first_observation_ms == START


def test_invalid_timestamps_are_counted_and_excluded_never_silent():
    stamps = [START, float("nan"), None, float("inf"), "not-a-time", START + 1000]
    report = cov(stamps)
    assert report.invalid_observation_count == 4
    assert report.observation_count == 2
    assert report.covered_ms == 1000 + TOL


def test_booleans_are_not_timestamps():
    result = normalize_observations([True, False, START])
    assert result.invalid_count == 2
    assert result.timestamps == (START,)


# ---------------------------------------------------------------------------
# 16-17: requested window is authoritative
# ---------------------------------------------------------------------------


def test_requested_window_smaller_than_dataset_clips_and_does_not_expand():
    stamps = list(range(START, END, 1000))
    narrow_start = START + 3_600_000
    narrow_end = narrow_start + 3_600_000
    report = cov(stamps, start=narrow_start, end=narrow_end)

    assert report.window_ms == 3_600_000
    assert report.is_complete is True
    assert report.covered_ms == 3_600_000
    assert report.observation_count == 3600


def test_requested_window_larger_than_dataset_reports_the_shortfall():
    stamps = list(range(START, START + 3_600_000, 1000))
    wide_end = END + DAY_MS
    report = cov(stamps, end=wide_end)

    assert report.window_ms == 2 * DAY_MS
    assert report.coverage_pct < 2.2
    assert report.trailing_gap_ms > DAY_MS


def test_window_is_never_silently_adjusted_to_flatter_bounds():
    stamps = [START + 12 * 3_600_000]
    report = cov(stamps)
    assert report.window_start_ms == START
    assert report.window_end_ms == END


def test_inclusive_end_date_spans_whole_final_day():
    start_ms, end_ms = window_from_dates("2026-06-01", "2026-06-03")
    assert end_ms - start_ms == 3 * DAY_MS


# ---------------------------------------------------------------------------
# Causality: pre-window carry-in
# ---------------------------------------------------------------------------


def test_observation_just_before_window_covers_the_window_opening():
    """Past data legitimately covers the present. This is causal."""
    stamps = [START - 1000]
    report = cov(stamps)
    assert report.carried_in_observation_ms == START - 1000
    assert report.covered_ms == TOL - 1000
    assert report.observation_count == 0  # not inside the window


def test_stale_pre_window_observation_does_not_cover_the_window():
    stamps = [START - TOL - 1]
    report = cov(stamps, source_count=1)
    assert report.carried_in_observation_ms is None
    assert report.covered_ms == 0
    assert report.status is CoverageStatus.ABSENT


def test_future_observation_cannot_cover_earlier_window_time():
    """An observation after the window must not retroactively cover it."""
    stamps = [END + 1000]
    report = cov(stamps)
    assert report.covered_ms == 0
    assert report.coverage_pct == 0.0


# ---------------------------------------------------------------------------
# 18-20: streams, emptiness, partial days
# ---------------------------------------------------------------------------


def test_each_stream_is_measured_independently():
    full = list(range(START, END, 100))
    reports = {
        "orderbook": compute_coverage(
            full, window_start_ms=START, window_end_ms=END,
            tolerance_ms=500, stream="orderbook", source_count=1,
        ),
        "trades": compute_coverage(
            [START], window_start_ms=START, window_end_ms=END,
            tolerance_ms=5000, stream="trades", source_count=1,
        ),
    }
    assert reports["orderbook"].is_complete is True
    assert reports["trades"].is_complete is False


def test_empty_stream_never_inherits_a_healthy_sibling_verdict():
    report = cov([], source_count=0)
    assert report.coverage_pct == 0.0


def test_partial_day_reports_the_real_fraction():
    half = START + DAY_MS // 2
    stamps = list(range(START, half, 1000))
    report = cov(stamps)
    assert 49.9 < report.coverage_pct < 50.1


# ---------------------------------------------------------------------------
# Unreadable sources must poison the verdict
# ---------------------------------------------------------------------------


def test_unreadable_source_makes_a_complete_report_untrustworthy():
    stamps = list(range(START, END, 1000))
    report = cov(stamps, source_count=24, unreadable_sources=("x.seg: corrupt",))
    assert report.is_complete is True
    assert report.is_trustworthy is False


# ---------------------------------------------------------------------------
# Structural invariants
# ---------------------------------------------------------------------------


def test_gaps_and_coverage_always_partition_the_window():
    random.seed(20260918)
    for _ in range(200):
        n = random.randint(0, 60)
        stamps = [random.randrange(START - TOL * 2, END + TOL * 2) for _ in range(n)]
        report = cov(stamps)
        assert report.covered_ms + report.total_gap_ms == DAY_MS
        assert 0 <= report.covered_ms <= DAY_MS


def test_no_random_input_can_produce_a_false_hundred_percent():
    """The central property: 100% implies genuinely full evidence."""
    random.seed(1)
    for _ in range(500):
        n = random.randint(0, 40)
        stamps = [random.randrange(START, END) for _ in range(n)]
        report = cov(stamps)
        if report.coverage_pct == 100.0:
            assert report.total_gap_ms == 0
            assert report.covered_ms == DAY_MS
        # Sparse data can never be complete.
        if n * TOL < DAY_MS:
            assert report.is_complete is False


def test_gaps_are_ordered_non_overlapping_and_inside_the_window():
    stamps = [START + i * 900_000 for i in range(12)]
    report = cov(stamps)
    previous_end = START
    for gap in report.gaps:
        assert gap.start_ms >= previous_end
        assert gap.end_ms <= END
        assert gap.duration_ms > 0
        previous_end = gap.end_ms


def test_zero_duration_gap_is_rejected_at_construction():
    from collector.collector.coverage import Gap

    with pytest.raises(ValueError):
        Gap(START, START, GapKind.INTERIOR)


def test_invalid_window_is_rejected():
    with pytest.raises(ValueError):
        cov([], start=END, end=START)


def test_non_positive_tolerance_is_rejected():
    with pytest.raises(ValueError):
        cov([START], tolerance=0)


def test_to_dict_round_trips_the_headline_numbers():
    stamps = [START, START + 1000]
    report = cov(stamps, source_count=1)
    payload = report.to_dict()
    assert payload["is_complete"] is False
    assert payload["covered_ms"] == report.covered_ms
    assert payload["gap_count"] == report.gap_count
    assert payload["status"] == "partial"
    assert len(payload["gaps"]) == report.gap_count
