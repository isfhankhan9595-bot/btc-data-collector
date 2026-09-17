"""End-to-end tests for the gap/coverage report tool.

These replace two earlier tests that asserted the old false-coverage
contract -- one of them required that three observations spanning five
seconds of a 24-hour day report ``100.00%``. That contract was the defect,
so the tests encoding it were rewritten rather than preserved.
"""
from __future__ import annotations

import json
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.collector.coverage import CoverageStatus
from collector.scripts.gap_report import (
    build_reports,
    generate_gap_report,
    main,
)

DATE = "2026-06-03"
DAY_MS = 24 * 60 * 60 * 1000
START_TS = int(pd.Timestamp(f"{DATE} 00:00:00", tz="UTC").timestamp() * 1000)


def _write_stream(data_dir, stream, date_str, timestamps, hour=0, suffix="parquet"):
    stream_dir = os.path.join(data_dir, "raw", stream)
    os.makedirs(stream_dir, exist_ok=True)
    frame = pd.DataFrame(
        {"timestamp": pd.to_datetime(list(timestamps), unit="ms", utc=True)}
    )
    table = pa.Table.from_pandas(frame, preserve_index=False)
    schema = pa.schema([pa.field("timestamp", pa.timestamp("ms", tz="UTC"))])
    if suffix == "parquet":
        name = f"{date_str}-{hour:02d}.parquet"
    else:
        name = f"{date_str}-{hour:02d}-000000.seg"
    pq.write_table(table.cast(schema), os.path.join(stream_dir, name))


def _report_for(data_dir, stream, date_str=DATE, **kwargs):
    reports = build_reports(date_str, date_str, data_dir=str(data_dir), **kwargs)
    return next(report for label, report in reports if report.stream == stream)


# ---------------------------------------------------------------------------
# The replaced contract
# ---------------------------------------------------------------------------


def test_three_observations_in_a_day_no_longer_report_full_coverage(tmp_path):
    """Previously asserted 100.00%. It is five seconds of a 24-hour day."""
    _write_stream(tmp_path, "trades", DATE, [START_TS, START_TS + 1000, START_TS + 5000])

    report = _report_for(tmp_path, "trades")

    assert report.is_complete is False
    assert report.coverage_pct < 0.02
    assert report.status is CoverageStatus.PARTIAL
    assert report.missing_ms > DAY_MS - 60_000


def test_gap_above_threshold_is_still_detected_as_an_interior_gap(tmp_path):
    _write_stream(tmp_path, "trades", DATE, [START_TS, START_TS + 1000, START_TS + 7000])

    report = _report_for(tmp_path, "trades")

    # 1000 -> 7000 is a 6s hop against a 5s tolerance: 1s uncovered interior.
    assert report.interior_gap_ms == 1000
    assert report.observation_count == 3
    assert report.is_complete is False


# ---------------------------------------------------------------------------
# Absence
# ---------------------------------------------------------------------------


def test_missing_stream_reports_no_sources_and_fails(tmp_path, capsys):
    _write_stream(tmp_path, "trades", DATE, [START_TS])

    ok = generate_gap_report(DATE, DATE, data_dir=str(tmp_path))
    output = capsys.readouterr().out

    assert ok is False
    assert "no published source segments" in output
    orderbook = _report_for(tmp_path, "orderbook")
    assert orderbook.status is CoverageStatus.NO_SOURCES
    assert orderbook.coverage_pct == 0.0


def test_entirely_absent_day_fails_the_report(tmp_path, capsys):
    ok = generate_gap_report(DATE, DATE, data_dir=str(tmp_path))
    output = capsys.readouterr().out
    assert ok is False
    assert output.count("no published source segments") == 3


def test_requested_day_outside_the_data_is_not_borrowed_from_a_neighbour(tmp_path):
    _write_stream(tmp_path, "trades", DATE, list(range(START_TS, START_TS + DAY_MS, 1000)))

    report = _report_for(tmp_path, "trades", date_str="2026-06-04")
    assert report.status is CoverageStatus.NO_SOURCES
    assert report.coverage_pct == 0.0


# ---------------------------------------------------------------------------
# Genuinely complete data must still be reported as complete
# ---------------------------------------------------------------------------


def test_dense_full_day_reports_complete(tmp_path):
    stamps = range(START_TS, START_TS + DAY_MS, 1000)
    _write_stream(tmp_path, "trades", DATE, stamps)

    report = _report_for(tmp_path, "trades")
    assert report.is_complete is True
    assert report.coverage_pct == 100.0
    assert report.status is CoverageStatus.COMPLETE


def test_clean_run_returns_true_only_when_every_stream_is_complete(tmp_path, capsys):
    stamps = list(range(START_TS, START_TS + DAY_MS, 400))
    for stream in ("orderbook", "trades", "markprice"):
        _write_stream(tmp_path, stream, DATE, stamps)

    ok = generate_gap_report(DATE, DATE, data_dir=str(tmp_path))
    capsys.readouterr()
    assert ok is True


# ---------------------------------------------------------------------------
# Multi-segment and multi-day behaviour
# ---------------------------------------------------------------------------


def test_observations_are_pooled_across_hourly_segments(tmp_path):
    for hour in range(24):
        base = START_TS + hour * 3_600_000
        _write_stream(
            tmp_path, "trades", DATE,
            range(base, base + 3_600_000, 1000), hour=hour,
        )

    report = _report_for(tmp_path, "trades")
    assert report.is_complete is True
    assert report.observation_count == 86_400


def test_missing_middle_hour_is_visible_as_an_interior_gap(tmp_path):
    for hour in range(24):
        if hour == 12:
            continue
        base = START_TS + hour * 3_600_000
        _write_stream(
            tmp_path, "trades", DATE,
            range(base, base + 3_600_000, 1000), hour=hour,
        )

    report = _report_for(tmp_path, "trades")
    assert report.is_complete is False
    assert report.interior_gap_ms == pytest.approx(3_600_000, abs=10_000)
    assert 95.5 < report.coverage_pct < 96.2


def test_multi_day_range_reports_each_day_separately(tmp_path):
    _write_stream(tmp_path, "trades", "2026-06-03",
                  range(START_TS, START_TS + DAY_MS, 1000))

    reports = build_reports("2026-06-03", "2026-06-04", data_dir=str(tmp_path))
    trades = [r for _, r in reports if r.stream == "trades"]
    assert len(trades) == 2
    assert trades[0].is_complete is True
    assert trades[1].status is CoverageStatus.NO_SOURCES


def test_whole_range_mode_measures_one_continuous_window(tmp_path):
    _write_stream(tmp_path, "trades", "2026-06-03",
                  range(START_TS, START_TS + DAY_MS, 1000))

    reports = build_reports(
        "2026-06-03", "2026-06-04", data_dir=str(tmp_path), per_day=False
    )
    trades = [r for _, r in reports if r.stream == "trades"]
    assert len(trades) == 1
    assert trades[0].window_ms == 2 * DAY_MS
    assert 49.5 < trades[0].coverage_pct < 50.5


# ---------------------------------------------------------------------------
# Unreadable and colliding sources
# ---------------------------------------------------------------------------


def test_corrupt_segment_is_surfaced_and_poisons_the_verdict(tmp_path, capsys):
    stamps = list(range(START_TS, START_TS + DAY_MS, 1000))
    _write_stream(tmp_path, "trades", DATE, stamps, hour=0)
    corrupt = tmp_path / "raw" / "trades" / f"{DATE}-05.parquet"
    corrupt.write_bytes(b"this is not parquet")

    report = _report_for(tmp_path, "trades")
    assert report.has_unreadable_sources is True
    assert report.is_trustworthy is False

    ok = generate_gap_report(DATE, DATE, data_dir=str(tmp_path))
    assert ok is False
    assert "unreadable" in capsys.readouterr().out


def test_storage_collision_is_reported_not_silently_double_counted(tmp_path):
    stamps = list(range(START_TS, START_TS + 3_600_000, 1000))
    _write_stream(tmp_path, "trades", DATE, stamps, hour=0, suffix="parquet")
    _write_stream(tmp_path, "trades", DATE, stamps, hour=0, suffix="seg")

    report = _report_for(tmp_path, "trades")
    assert report.has_unreadable_sources is True
    assert any("collision" in problem for problem in report.unreadable_sources)
    assert report.coverage_pct == 0.0


# ---------------------------------------------------------------------------
# Machine-readable output and CLI
# ---------------------------------------------------------------------------


def test_json_output_is_parseable_and_truthful(tmp_path, capsys):
    _write_stream(tmp_path, "trades", DATE, [START_TS, START_TS + 1000])

    generate_gap_report(DATE, DATE, data_dir=str(tmp_path), as_json=True)
    payload = json.loads(capsys.readouterr().out)

    trades = next(row for row in payload if row["stream"] == "trades")
    assert trades["is_complete"] is False
    assert trades["coverage_pct"] < 0.01
    assert trades["missing_ms"] > 0
    assert trades["covered_ms"] + trades["missing_ms"] == DAY_MS


def test_cli_exit_code_is_nonzero_when_data_is_incomplete(tmp_path, capsys):
    _write_stream(tmp_path, "trades", DATE, [START_TS])
    code = main([DATE, "--data-dir", str(tmp_path)])
    capsys.readouterr()
    assert code == 1


def test_cli_exit_code_is_zero_on_genuinely_complete_data(tmp_path, capsys):
    stamps = list(range(START_TS, START_TS + DAY_MS, 400))
    for stream in ("orderbook", "trades", "markprice"):
        _write_stream(tmp_path, stream, DATE, stamps)

    code = main([DATE, "--data-dir", str(tmp_path)])
    capsys.readouterr()
    assert code == 0


def test_unknown_stream_without_a_tolerance_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="tolerance"):
        build_reports(DATE, DATE, data_dir=str(tmp_path), streams=["mystery"])


# ---------------------------------------------------------------------------
# Regression guards pinned to the exact historical defect
# ---------------------------------------------------------------------------


def test_int64_epoch_ms_timestamps_are_measured_identically(tmp_path):
    """Legacy segments may store epoch ms as int64 rather than timestamp[ms]."""
    stream_dir = tmp_path / "raw" / "trades"
    stream_dir.mkdir(parents=True)
    stamps = list(range(START_TS, START_TS + DAY_MS, 1000))
    table = pa.Table.from_pydict(
        {"timestamp": stamps}, schema=pa.schema([pa.field("timestamp", pa.int64())])
    )
    pq.write_table(table, str(stream_dir / f"{DATE}-00.parquet"))

    report = _report_for(tmp_path, "trades")
    assert report.is_complete is True
    assert report.observation_count == 86_400


def test_old_false_coverage_formula_is_not_reachable(tmp_path):
    """Pins the defect: (window - detected_gaps)/window must not be the answer.

    Under the old formula this dataset scored 100% because the two 1s hops
    fell under the 5s trades tolerance and nothing else was subtracted.
    """
    _write_stream(tmp_path, "trades", DATE, [START_TS, START_TS + 1000, START_TS + 2000])

    report = _report_for(tmp_path, "trades")
    old_formula_pct = ((DAY_MS - report.interior_gap_ms) / DAY_MS) * 100

    assert old_formula_pct == 100.0          # what the defect produced
    assert report.coverage_pct < 0.01        # what is actually true
    assert report.is_complete is False
