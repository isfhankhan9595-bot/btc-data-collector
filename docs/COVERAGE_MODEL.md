# Coverage model

How this repository measures "do we actually have the data we think we have".

Implemented in `collector/collector/coverage.py`; consumed by
`collector/scripts/gap_report.py`.

## The question

> For what fraction of an explicitly requested window was fresh data
> actually available to a researcher reading this dataset?

This is deliberately not the question the previous implementation answered.

## The defect this replaces

`gap_report.py` used to compute:

```
coverage = (window_ms - sum(detected_gap_ms)) / window_ms
```

with `window_ms` hardcoded to 24 hours and `detected_gap_ms` summing only the
inter-observation hops that exceeded a per-stream threshold.

Three ways that lies:

| Dataset | Old result | Truth |
|---|---|---|
| 3 observations 1s apart in a 24h day | **100.00%** | 0.008% |
| 1 hour of dense data, 23 hours absent | **100.00%** | ~4.2% |
| Data starting 6h into the day | **100.00%** | ~75% |

The period before the first observation and after the last one was never
counted, so absence was structurally invisible. A researcher trusting the
number would believe they held a full day of market data.

Verified live against the pre-fix code on a synthetic three-observation day:
old implementation reported `100.00%` for all three streams and exited `0`.

## The model

An observation at time `t` is direct evidence the stream was alive at `t`. Up
to a declared staleness tolerance it is also evidence that data was
*available* shortly afterwards. So each observation contributes the half-open
interval:

```
[t, t + tolerance_ms)
```

Covered time is the **union** of those intervals, intersected with the
requested window. Everything else in the window is a gap.

### Properties this buys

- An isolated observation covers `tolerance_ms`, not a day.
- Time before the first observation is a **leading** gap.
- Time after the last observation goes stale and becomes a **trailing** gap.
- A completely absent stream reports 0%.
- `coverage == 100%` is reachable **only** when the union of evidence covers
  every millisecond of the requested window.

That last property is enforced by a property test over random inputs
(`test_no_random_input_can_produce_a_false_hundred_percent`), and
`covered_ms + total_gap_ms == window_ms` is asserted as a partition invariant.

## Tolerance

`tolerance_ms` is the declared staleness budget per stream. It mirrors the
live `GapDetector` thresholds so that "never tripped a live gap alert" and
"reports full interior coverage" mean the same thing.

| Stream | Tolerance |
|---|---|
| `orderbook` | 500 ms |
| `trades` | 5 000 ms |
| `markprice` | 5 000 ms |
| `openinterest` | 300 000 ms |

A stream with no configured tolerance raises rather than defaulting, because
a silently-chosen tolerance is a silently-chosen coverage number.

## Causality

Coverage at `t` is determined only by observations at or before `t`.

One consequence is deliberate: the most recent observation *before*
`window_start` is carried in when it is within tolerance, because that data
was genuinely in hand when the window opened. Observations at or after
`window_end` contribute nothing to the window — this falls out of the
half-open interval model rather than being special-cased, so there is no
code path where future data can back-fill earlier coverage.

## Nothing is silent

| Input problem | Behaviour |
|---|---|
| NaN / NaT / None / Inf / unparseable string | counted in `invalid_observation_count`, excluded from evidence |
| Duplicate timestamps | counted in `duplicate_observation_count`, collapsed once |
| Out-of-order arrival | counted in `out_of_order_count` (descents), sorted |
| Naive datetime | interpreted as UTC (documented assumption, not a rejection) |
| Unreadable segment | listed in `unreadable_sources`; `is_trustworthy` becomes False |
| Legacy/`.seg` storage collision | surfaced via `unreadable_sources`; coverage is **not** computed from ambiguous sources |

The requested window is used verbatim and is never widened or narrowed.

## `is_complete` vs `is_trustworthy`

- `is_complete` — every millisecond of the window is covered.
- `is_trustworthy` — complete **and** no source was unreadable.

A day whose readable segments look perfect but which contains one corrupt
file is complete-but-not-trustworthy. `gap_report` keys its exit code off
`is_trustworthy`, so a corrupt segment fails the report.

## Usage

```bash
# Per-day table, exit 1 unless every stream is fully covered and trustworthy
python -m collector.scripts.gap_report 2026-06-03 --data-dir data

# Inclusive date range
python -m collector.scripts.gap_report 2026-06-01 2026-06-07 --data-dir data

# One continuous window instead of per-day
python -m collector.scripts.gap_report 2026-06-01 2026-06-07 --whole-range

# Machine-readable, for CI and validation tooling
python -m collector.scripts.gap_report 2026-06-03 --json
```

## Known limitations

- Coverage measures *availability of observations*, not correctness of their
  content. A stream emitting well-formed but wrong prices reports full
  coverage. Content validation is a separate concern.
- Tolerances are fixed per stream rather than adaptive to observed cadence.
  A venue that legitimately slows down at low volume will show interior gaps.
  This is the conservative direction: it over-reports missingness rather
  than under-reporting it.
- Sequence-gap state and recovery state are not yet folded into the coverage
  verdict. A period that was covered by observations but whose order book was
  in `SEQUENCE_GAP` still counts as covered here. Wiring quality state into
  coverage is tracked as later-phase work.
