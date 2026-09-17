"""Truthful temporal coverage measurement.

The question this module answers is deliberately narrow and causal:

    For what fraction of an explicitly requested window was fresh data
    actually available to a researcher reading this dataset?

That is *not* the same question as "how much time did we spend inside
detected gaps", which is what the previous implementation measured. The
difference matters, because the old formula::

    coverage = (window_ms - sum(detected_gap_ms)) / window_ms

reports 100% for a stream containing three observations one millisecond
apart in a 24-hour day: there are no inter-observation gaps, so nothing is
subtracted. It also never accounts for the period before the first
observation or after the last one, so a day with 23 missing hours reports
as clean. A researcher trusting that number would believe they had a full
day of market data when they had five seconds of it.

Model
-----

Coverage is built from *evidence*, not from the absence of counter-evidence.

An observation at time ``t`` is direct evidence that the stream was alive at
``t``. It is also, up to a declared staleness tolerance, evidence that data
was *available* for a short period afterwards: a researcher standing at
``t + 200ms`` with a 500ms tolerance still holds data that is fresh enough
to use. So each observation contributes the half-open interval::

    [t, t + tolerance_ms)

Covered time is the union of those intervals, intersected with the requested
window. Everything else in the window is a gap. This yields the properties
we actually want:

* An isolated observation covers ``tolerance_ms``, not a whole day.
* Time before the first observation is uncovered (a leading gap).
* Time after the last observation goes stale and becomes uncovered
  (a trailing gap).
* A completely absent stream reports 0%, not 100%.
* ``coverage == 100%`` is reachable *only* when the union of evidence
  intervals covers every millisecond of the requested window.

Causality
---------

Coverage at time ``t`` is determined solely by observations at or before
``t``. An observation is never allowed to justify coverage of a period that
precedes it. One consequence is deliberate: an observation that lands just
before ``window_start`` *does* legitimately cover the beginning of the
window, because that data was genuinely in hand when the window opened, so
the most recent pre-window observation is carried in when it is within
tolerance. Observations at or after ``window_end`` contribute nothing to the
window, which falls out of the model rather than being special-cased.

Nothing here silently repairs input. Unparseable timestamps are counted and
reported, never dropped in silence; duplicates and out-of-order arrivals are
counted and reported; the requested window is never widened or narrowed to
make a number look better.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date as _date, datetime, timezone
from enum import Enum
from typing import Any, Iterable, Sequence

__all__ = [
    "GapKind",
    "CoverageStatus",
    "Gap",
    "CoverageReport",
    "NormalizedObservations",
    "normalize_observations",
    "compute_coverage",
    "day_window_ms",
    "window_from_dates",
]

_MS_PER_DAY = 24 * 60 * 60 * 1000


class GapKind(str, Enum):
    """Where an uncovered interval sits relative to the observed evidence."""

    #: No usable observation exists anywhere in or before the window.
    ABSENT = "absent"
    #: Window opened before any data was available.
    LEADING = "leading"
    #: Coverage was established, lost, and re-established.
    INTERIOR = "interior"
    #: Data went stale before the window closed and never resumed.
    TRAILING = "trailing"


class CoverageStatus(str, Enum):
    """Coarse verdict for a stream over a window."""

    #: Every millisecond of the requested window is covered.
    COMPLETE = "complete"
    #: Some but not all of the window is covered.
    PARTIAL = "partial"
    #: Sources existed but yielded no usable observation for the window.
    ABSENT = "absent"
    #: No source files were found at all.
    NO_SOURCES = "no_sources"


@dataclass(frozen=True)
class Gap:
    """A half-open ``[start_ms, end_ms)`` interval with no available data."""

    start_ms: int
    end_ms: int
    kind: GapKind

    def __post_init__(self) -> None:
        if self.end_ms <= self.start_ms:
            raise ValueError(
                f"gap must have positive duration, got [{self.start_ms}, {self.end_ms})"
            )

    @property
    def duration_ms(self) -> int:
        return self.end_ms - self.start_ms


@dataclass(frozen=True)
class NormalizedObservations:
    """Result of coercing raw timestamps into sorted UTC epoch milliseconds.

    Every rejection and every anomaly is counted. Nothing is discarded
    without being represented here.
    """

    timestamps: tuple[int, ...]
    invalid_count: int = 0
    duplicate_count: int = 0
    out_of_order_count: int = 0

    @property
    def total_input(self) -> int:
        return (
            len(self.timestamps)
            + self.invalid_count
            + self.duplicate_count
        )


@dataclass(frozen=True)
class CoverageReport:
    """Truthful coverage of one stream over one explicitly requested window."""

    stream: str
    window_start_ms: int
    window_end_ms: int
    tolerance_ms: int
    covered_ms: int
    gaps: tuple[Gap, ...]
    observation_count: int
    invalid_observation_count: int = 0
    duplicate_observation_count: int = 0
    out_of_order_count: int = 0
    first_observation_ms: int | None = None
    last_observation_ms: int | None = None
    carried_in_observation_ms: int | None = None
    source_count: int = 0
    unreadable_sources: tuple[str, ...] = field(default_factory=tuple)

    # -- window -----------------------------------------------------------

    @property
    def window_ms(self) -> int:
        return self.window_end_ms - self.window_start_ms

    @property
    def missing_ms(self) -> int:
        return self.window_ms - self.covered_ms

    # -- headline numbers -------------------------------------------------

    @property
    def coverage_ratio(self) -> float:
        if self.window_ms <= 0:
            return 0.0
        return self.covered_ms / self.window_ms

    @property
    def coverage_pct(self) -> float:
        return self.coverage_ratio * 100.0

    @property
    def is_complete(self) -> bool:
        """True only when literally every millisecond is accounted for."""
        return self.window_ms > 0 and self.covered_ms == self.window_ms

    # -- gap breakdown ----------------------------------------------------

    @property
    def gap_count(self) -> int:
        return len(self.gaps)

    @property
    def total_gap_ms(self) -> int:
        return sum(gap.duration_ms for gap in self.gaps)

    @property
    def longest_gap_ms(self) -> int:
        return max((gap.duration_ms for gap in self.gaps), default=0)

    def gaps_of_kind(self, kind: GapKind) -> tuple[Gap, ...]:
        return tuple(gap for gap in self.gaps if gap.kind is kind)

    @property
    def leading_gap_ms(self) -> int:
        return sum(g.duration_ms for g in self.gaps_of_kind(GapKind.LEADING))

    @property
    def trailing_gap_ms(self) -> int:
        return sum(g.duration_ms for g in self.gaps_of_kind(GapKind.TRAILING))

    @property
    def interior_gap_ms(self) -> int:
        return sum(g.duration_ms for g in self.gaps_of_kind(GapKind.INTERIOR))

    # -- verdict ----------------------------------------------------------

    @property
    def status(self) -> CoverageStatus:
        if self.source_count == 0:
            return CoverageStatus.NO_SOURCES
        if self.covered_ms == 0:
            return CoverageStatus.ABSENT
        if self.is_complete:
            return CoverageStatus.COMPLETE
        return CoverageStatus.PARTIAL

    @property
    def has_unreadable_sources(self) -> bool:
        return bool(self.unreadable_sources)

    @property
    def is_trustworthy(self) -> bool:
        """Whether this report may be relied on without qualification.

        A report built over sources that could not be read is not a clean
        bill of health even if the readable part looks complete.
        """
        return self.is_complete and not self.unreadable_sources

    def to_dict(self) -> dict[str, Any]:
        """Machine-readable form for validation tooling and CI."""
        return {
            "stream": self.stream,
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
            "window_ms": self.window_ms,
            "tolerance_ms": self.tolerance_ms,
            "status": self.status.value,
            "coverage_pct": self.coverage_pct,
            "covered_ms": self.covered_ms,
            "missing_ms": self.missing_ms,
            "is_complete": self.is_complete,
            "is_trustworthy": self.is_trustworthy,
            "observation_count": self.observation_count,
            "invalid_observation_count": self.invalid_observation_count,
            "duplicate_observation_count": self.duplicate_observation_count,
            "out_of_order_count": self.out_of_order_count,
            "first_observation_ms": self.first_observation_ms,
            "last_observation_ms": self.last_observation_ms,
            "carried_in_observation_ms": self.carried_in_observation_ms,
            "source_count": self.source_count,
            "unreadable_sources": list(self.unreadable_sources),
            "gap_count": self.gap_count,
            "total_gap_ms": self.total_gap_ms,
            "longest_gap_ms": self.longest_gap_ms,
            "leading_gap_ms": self.leading_gap_ms,
            "interior_gap_ms": self.interior_gap_ms,
            "trailing_gap_ms": self.trailing_gap_ms,
            "gaps": [
                {
                    "start_ms": gap.start_ms,
                    "end_ms": gap.end_ms,
                    "duration_ms": gap.duration_ms,
                    "kind": gap.kind.value,
                }
                for gap in self.gaps
            ],
        }


# ---------------------------------------------------------------------------
# Timestamp normalisation
# ---------------------------------------------------------------------------


def _coerce_one(value: Any) -> int | None:
    """Coerce a single timestamp to UTC epoch milliseconds, or None.

    ``None`` means "this input is not a usable timestamp". Callers must
    count those rather than ignore them.
    """
    if value is None:
        return None

    # bool is an int subclass; a boolean is never a timestamp.
    if isinstance(value, bool):
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            # A naive timestamp is interpreted as UTC. The collector writes
            # UTC everywhere; documenting the assumption is better than
            # rejecting historical data that predates tz-aware writes.
            value = value.replace(tzinfo=timezone.utc)
        return int(value.timestamp() * 1000)

    if isinstance(value, _date):
        return int(
            datetime(value.year, value.month, value.day, tzinfo=timezone.utc).timestamp()
            * 1000
        )

    if isinstance(value, (int,)):
        return int(value)

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return int(value)

    # numpy / pandas scalars and anything else that quacks like a number or
    # a timestamp. Tried last so that the explicit branches above win.
    for attr in ("to_pydatetime", "item"):
        converter = getattr(value, attr, None)
        if callable(converter):
            try:
                converted = converter()
            except (ValueError, TypeError, OverflowError):
                return None
            if converted is value:  # avoid infinite recursion
                break
            return _coerce_one(converted)

    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return _coerce_one(parsed)

    return None


def normalize_observations(values: Iterable[Any]) -> NormalizedObservations:
    """Sort, de-duplicate and UTC-normalise raw observation timestamps.

    Anomalies are counted, not hidden:

    * ``invalid_count`` — values that are not usable timestamps (NaN, NaT,
      ``None``, infinities, unparseable strings).
    * ``duplicate_count`` — repeated timestamps collapsed into one.
    * ``out_of_order_count`` — values that arrived earlier than their
      predecessor in the input sequence.
    """
    raw: list[int] = []
    invalid = 0
    out_of_order = 0
    previous: int | None = None

    # pandas Series and numpy arrays iterate element-wise, which is what we
    # want; this stays dependency-free on purpose.
    for value in values:
        coerced = _coerce_one(value)
        if coerced is None:
            invalid += 1
            continue
        if previous is not None and coerced < previous:
            out_of_order += 1
        previous = coerced
        raw.append(coerced)

    unique = sorted(set(raw))
    duplicates = len(raw) - len(unique)

    return NormalizedObservations(
        timestamps=tuple(unique),
        invalid_count=invalid,
        duplicate_count=duplicates,
        out_of_order_count=out_of_order,
    )


# ---------------------------------------------------------------------------
# Window helpers
# ---------------------------------------------------------------------------


def day_window_ms(date_str: str) -> tuple[int, int]:
    """Return the half-open ``[start, end)`` epoch-ms bounds of a UTC day."""
    day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start = int(day.timestamp() * 1000)
    return start, start + _MS_PER_DAY


def window_from_dates(start_date: str, end_date: str) -> tuple[int, int]:
    """Return ``[start, end)`` spanning whole UTC days, inclusive of both.

    ``end_date`` is inclusive as a *day*: the window closes at the end of
    that day, not at its start. Silently treating an inclusive end date as
    an exclusive bound would understate the requested window and inflate
    coverage.
    """
    start_ms, _ = day_window_ms(start_date)
    _, end_ms = day_window_ms(end_date)
    if end_ms <= start_ms:
        raise ValueError(f"end_date {end_date} precedes start_date {start_date}")
    return start_ms, end_ms


# ---------------------------------------------------------------------------
# Coverage computation
# ---------------------------------------------------------------------------


def compute_coverage(
    observations: Iterable[Any] | NormalizedObservations,
    *,
    window_start_ms: int,
    window_end_ms: int,
    tolerance_ms: int,
    stream: str = "",
    source_count: int = 0,
    unreadable_sources: Sequence[str] = (),
) -> CoverageReport:
    """Measure coverage of ``[window_start_ms, window_end_ms)`` from evidence.

    ``tolerance_ms`` is the declared staleness budget: how long after an
    observation the data is still considered available. It is the same
    quantity the live ``GapDetector`` uses as its gap threshold, so a
    stream that never trips a live gap alert is exactly a stream that
    reports full interior coverage here.

    The requested window is used verbatim. It is never adjusted to fit the
    data.
    """
    if window_end_ms <= window_start_ms:
        raise ValueError(
            f"window must have positive duration, got "
            f"[{window_start_ms}, {window_end_ms})"
        )
    if tolerance_ms <= 0:
        raise ValueError(f"tolerance_ms must be positive, got {tolerance_ms}")

    if isinstance(observations, NormalizedObservations):
        normalized = observations
    else:
        normalized = normalize_observations(observations)

    stamps = normalized.timestamps

    # Observations inside the window are the reportable population.
    in_window = [ts for ts in stamps if window_start_ms <= ts < window_end_ms]

    # The most recent observation strictly before the window may still be
    # providing fresh data as the window opens. Using it is causal: it
    # happened before the period it covers.
    carried_in: int | None = None
    before = [ts for ts in stamps if ts < window_start_ms]
    if before:
        candidate = before[-1]
        if candidate + tolerance_ms > window_start_ms:
            carried_in = candidate

    # Build evidence intervals and merge them.
    contributing: list[int] = list(in_window)
    if carried_in is not None:
        contributing.append(carried_in)
    contributing.sort()

    merged: list[list[int]] = []
    for ts in contributing:
        start = max(ts, window_start_ms)
        end = min(ts + tolerance_ms, window_end_ms)
        if end <= start:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])

    covered_ms = sum(end - start for start, end in merged)

    # Complement of the merged evidence inside the window == the gaps.
    gaps: list[Gap] = []
    if not merged:
        gaps.append(
            Gap(window_start_ms, window_end_ms, GapKind.ABSENT)
        )
    else:
        cursor = window_start_ms
        for start, end in merged:
            if start > cursor:
                kind = GapKind.LEADING if cursor == window_start_ms else GapKind.INTERIOR
                gaps.append(Gap(cursor, start, kind))
            cursor = end
        if cursor < window_end_ms:
            gaps.append(Gap(cursor, window_end_ms, GapKind.TRAILING))

    return CoverageReport(
        stream=stream,
        window_start_ms=window_start_ms,
        window_end_ms=window_end_ms,
        tolerance_ms=tolerance_ms,
        covered_ms=covered_ms,
        gaps=tuple(gaps),
        observation_count=len(in_window),
        invalid_observation_count=normalized.invalid_count,
        duplicate_observation_count=normalized.duplicate_count,
        out_of_order_count=normalized.out_of_order_count,
        first_observation_ms=in_window[0] if in_window else None,
        last_observation_ms=in_window[-1] if in_window else None,
        carried_in_observation_ms=carried_in,
        source_count=source_count,
        unreadable_sources=tuple(unreadable_sources),
    )
