"""Truthful gap and coverage reporting for published raw segments.

This tool answers "how much of the window I asked about is actually backed
by data", and it is built so that the answer cannot be flattering by
accident. In particular:

* Coverage is measured against the *requested* window, not against the span
  of whatever data happens to exist.
* A stream with no segments at all reports 0%, not "no gaps detected".
* Periods before the first observation and after the last one count as
  missing.
* Segments that cannot be read make the report untrustworthy, and that is
  stated rather than silently skipped.

See :mod:`collector.collector.coverage` for the coverage model.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Iterable, Sequence

import pandas as pd

from collector.collector.coverage import (
    CoverageReport,
    CoverageStatus,
    compute_coverage,
    day_window_ms,
    window_from_dates,
)
from collector.collector.storage_layout import StorageCollisionError, iter_segments

#: Staleness budget per stream, in milliseconds. These mirror the live
#: ``GapDetector`` thresholds so that "never tripped a gap alert" and "reports
#: full interior coverage" mean the same thing.
DEFAULT_TOLERANCES_MS: dict[str, int] = {
    "orderbook": 500,
    "trades": 5000,
    "markprice": 5000,
    "openinterest": 300_000,
}

DEFAULT_STREAMS: tuple[str, ...] = ("orderbook", "trades", "markprice")


def _read_stream_timestamps(
    data_dir: str, stream: str, date: str | None
) -> tuple[list[Any], int, list[str]]:
    """Return ``(timestamps, source_count, unreadable)`` for one stream/day.

    Unreadable sources are returned rather than raised so that one corrupt
    segment does not hide the state of every other segment -- but they are
    never silently dropped either.
    """
    unreadable: list[str] = []
    try:
        files = list(iter_segments(data_dir, stream, date=date))
    except StorageCollisionError as exc:
        return [], 0, [f"storage collision: {exc}"]

    timestamps: list[Any] = []
    for path in files:
        try:
            frame = pd.read_parquet(path, columns=["timestamp"])
        except (OSError, ValueError, KeyError) as exc:
            unreadable.append(f"{path}: {exc}")
            continue
        timestamps.extend(frame["timestamp"].tolist())

    return timestamps, len(files), unreadable


def build_reports(
    start_date: str,
    end_date: str,
    data_dir: str = "data",
    streams: Sequence[str] = DEFAULT_STREAMS,
    tolerances_ms: dict[str, int] | None = None,
    per_day: bool = True,
) -> list[tuple[str, CoverageReport]]:
    """Compute a truthful coverage report per stream (and per day if asked).

    ``per_day=False`` measures the whole requested range as a single window,
    which is the right shape for "was this month usable" questions.
    """
    tolerances = dict(DEFAULT_TOLERANCES_MS)
    if tolerances_ms:
        tolerances.update(tolerances_ms)

    reports: list[tuple[str, CoverageReport]] = []

    if per_day:
        dates = pd.date_range(start_date, end_date).strftime("%Y-%m-%d").tolist()
        windows: list[tuple[str | None, str, tuple[int, int]]] = [
            (day, day, day_window_ms(day)) for day in dates
        ]
    else:
        windows = [(None, f"{start_date}..{end_date}", window_from_dates(start_date, end_date))]

    for date_filter, label, (window_start, window_end) in windows:
        for stream in streams:
            tolerance = tolerances.get(stream)
            if tolerance is None:
                raise ValueError(f"No coverage tolerance configured for stream: {stream}")

            timestamps, source_count, unreadable = _read_stream_timestamps(
                data_dir, stream, date_filter
            )
            report = compute_coverage(
                timestamps,
                window_start_ms=window_start,
                window_end_ms=window_end,
                tolerance_ms=tolerance,
                stream=stream,
                source_count=source_count,
                unreadable_sources=unreadable,
            )
            # Carry the human label for printing without polluting the model.
            reports.append((label, report))

    return reports


def _format_ms(value: int) -> str:
    if value == 0:
        return "0"
    if value < 1000:
        return f"{value}ms"
    seconds = value / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"
    return f"{minutes / 60:.2f}h"


def print_reports(labelled: Iterable[tuple[str, CoverageReport]]) -> bool:
    """Print a human-readable table. Returns True when everything is clean."""
    header = (
        f"{'Stream':<14} {'Window':<22} {'Status':<10} {'Coverage':>9} "
        f"{'Obs':>8} {'Gaps':>5} {'Missing':>10} {'Longest':>10}"
    )
    print(header)
    print("-" * len(header))

    clean = True
    footnotes: list[str] = []

    for label, report in labelled:
        if not report.is_trustworthy:
            clean = False
        print(
            f"{report.stream:<14} {label:<22} {report.status.value:<10} "
            f"{report.coverage_pct:>8.4f}% {report.observation_count:>8} "
            f"{report.gap_count:>5} {_format_ms(report.missing_ms):>10} "
            f"{_format_ms(report.longest_gap_ms):>10}"
        )

        if report.status is CoverageStatus.NO_SOURCES:
            footnotes.append(
                f"[FAIL] {report.stream} {label}: no published source segments"
            )
        elif report.status is CoverageStatus.ABSENT:
            footnotes.append(
                f"[FAIL] {report.stream} {label}: sources exist but contain no "
                f"usable observation inside the requested window"
            )
        if report.unreadable_sources:
            for problem in report.unreadable_sources:
                footnotes.append(f"[FAIL] {report.stream} {label}: unreadable -> {problem}")
        if report.invalid_observation_count:
            footnotes.append(
                f"[WARN] {report.stream} {label}: "
                f"{report.invalid_observation_count} unusable timestamp(s) excluded"
            )
        if report.out_of_order_count:
            footnotes.append(
                f"[WARN] {report.stream} {label}: "
                f"{report.out_of_order_count} out-of-order timestamp(s)"
            )
        if report.leading_gap_ms:
            footnotes.append(
                f"[WARN] {report.stream} {label}: leading gap "
                f"{_format_ms(report.leading_gap_ms)} before first observation"
            )
        if report.trailing_gap_ms:
            footnotes.append(
                f"[WARN] {report.stream} {label}: trailing gap "
                f"{_format_ms(report.trailing_gap_ms)} after last observation"
            )

    if footnotes:
        print()
        for note in footnotes:
            print(note)

    return clean


def generate_gap_report(
    start_date: str,
    end_date: str,
    data_dir: str = "data",
    streams: Sequence[str] = DEFAULT_STREAMS,
    tolerances_ms: dict[str, int] | None = None,
    per_day: bool = True,
    as_json: bool = False,
) -> bool:
    """Generate and print a coverage report. Returns True only if clean.

    "Clean" means every requested stream/window is fully covered by readable
    sources. Partial coverage is a failure, because the entire point of this
    tool is that partial data must not look complete.
    """
    labelled = build_reports(
        start_date, end_date, data_dir, streams, tolerances_ms, per_day
    )

    if as_json:
        payload = [
            {"window_label": label, **report.to_dict()} for label, report in labelled
        ]
        print(json.dumps(payload, indent=2))
        return all(report.is_trustworthy for _, report in labelled)

    return print_reports(labelled)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Truthful coverage/gap report for published raw segments."
    )
    parser.add_argument("start_date", help="UTC start date, YYYY-MM-DD")
    parser.add_argument(
        "end_date", nargs="?", default=None, help="UTC end date (inclusive)"
    )
    parser.add_argument("--data-dir", default="data")
    parser.add_argument(
        "--streams", nargs="+", default=list(DEFAULT_STREAMS),
        help="Streams to measure.",
    )
    parser.add_argument(
        "--whole-range", action="store_true",
        help="Measure the full range as one window instead of per day.",
    )
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    ok = generate_gap_report(
        args.start_date,
        args.end_date or args.start_date,
        data_dir=args.data_dir,
        streams=args.streams,
        per_day=not args.whole_range,
        as_json=args.as_json,
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
