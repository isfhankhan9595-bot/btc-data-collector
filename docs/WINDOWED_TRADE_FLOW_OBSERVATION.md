# Causal Windowed CVD

Built after an independent audit of the merged cumulative CVD (PR #46) —
verified via GitHub API and a real local test run (main at `fc1f7e5`, 1154
tests genuinely passing, `compileall`/`git diff --check` clean), not
trusted from the handoff doc. The audit found no defect blocking this
work; two things worth recording explicitly rather than silently accepting
or silently fixing:

## Audit finding 1: unknown trade side (confirmed by test, not assumed)

A trade whose `side` is missing, empty, or any spelling other than a
BUY/SELL variant contributes to **neither** `buy_volume` nor `sell_volume`
— it is never guessed into a direction. It still counts in `trade_count`,
so `buy_volume + sell_volume < trade_count` is the caller's signal that
some trade's direction went unrecorded, rather than a silent loss with no
trace. Confirmed with `tests/test_windowed_trade_flow_observation.py::test_unknown_side_contributes_to_neither_buy_nor_sell_but_is_still_counted`.

## Audit finding 2: no duplicate-trade-message protection (pre-existing, not fixed here)

The trade pipeline has no dedup logic anywhere in the replay/adapter layer
— unlike order-book diffs, which `sequence.py` validates for continuity
and duplication, a resent trade wire message (a real reconnect/replay
scenario on every venue) would be counted twice by both the cumulative and
windowed CVD. This is a property of `non_book_events` generally, not
something windowed CVD introduces. Per this phase's own instruction
("report a pipeline correctness hole rather than silently compensate for
it inside a feature module"), this is recorded here and in
`trade_flow_observation.py`'s docstring, not patched around. Fixing it
belongs in the adapter/replay layer.

**Resolved in a later phase:** `ExchangeAdapter._dedupe_trades`
(`adapters/base.py`) now suppresses a duplicate trade message before it
ever reaches `non_book_events`, at the same shared layer every runner and
`ReplayEngine` already funnel through — see `test_trade_deduplication.py`
for the venue-by-venue audit and the acceptance tests proving this fix
required no change to either CVD function. This finding is left as
written above rather than rewritten, as the accurate record of what was
true when this doc was written.

## What windowed CVD answers

"What was net aggressive flow during the last W as of observation time T?"
— `collector/collector/trade_flow_observation.py:observe_windowed_trade_flow_at(frames, observation_ts, window_ms, *, venue, staleness_ms=5000)`
→ `WindowedTradeFlowObservation`.

## Temporal contract: `(window_start, window_end]`

`window_start = observation_ts - window_ms` (**excluded**),
`window_end = observation_ts` (**included**). A trade exactly at
`window_start` is outside the window; a trade exactly at `observation_ts`
is inside it. Tested at all four boundary points
(`T-W`, `T-W+1`, `T`, `T+1`).

## Causality

Two layers, both required, proven independently not to be redundant:

1. **Pre-replay frame filter** (`_causal_trades`, shared with cumulative
   CVD): frames with `timestamp_ms > observation_ts` never enter
   `ReplaySource` at all.
2. **Post-replay window filter**: of the causally-known trades, only
   those with `window_start < local_receive_ts <= observation_ts` are
   tallied.

**Architectural finding from writing the mutation tests**: for trades
specifically (stateless-cumulative), removing layer 1 alone does **not**
leak anything, because layer 2's own `<= observation_ts` check catches it
independently — confirmed by
`test_removing_only_the_pre_replay_filter_does_not_leak_for_stateless_trades`.
This is *not* true for order-book replay (`reconstruct_book_at` excludes
late frames before replay for a stateful reason — a late frame could
corrupt sequence/gap state even if its resulting book state were filtered
out afterward). The genuine leakage-producing mutation for trades has to
remove the upper bound from *both* places at once
(`test_mutation_1_removing_all_causal_bounds_would_leak_a_future_trade`).

Exchange timestamps never establish eligibility at either layer — proven
adversarially (a payload claiming an exchange timestamp inside the window
while its causal `timestamp_ms` is after `observation_ts`).

## Missingness — corrected to a genuine three-way split (this phase)

An earlier version of this module treated every empty window as
`NEVER_OBSERVED`, whether or not causal evidence for the venue existed
elsewhere. That conflates "we know nothing about this venue" with "we
know this venue was quiet" — wrong for research use, since it hides a
genuinely confirmable zero as an unknown. Corrected here to:

- **Zero causal trade evidence for the venue at all** (`frames_considered=0`,
  or frames existed but were malformed/produced no trade): `NEVER_OBSERVED`,
  `cvd=None`, `buy_volume=None`, `sell_volume=None`. The only case this
  status is reserved for.
- **Evidence exists, none of it falls in this window, and the nearest
  evidence is within `staleness_ms` of `observation_ts`**: `AVAILABLE`
  with `cvd == buy_volume == sell_volume == 0.0`, `trade_count == 0`. A
  genuine, confirmed zero — real information, not an unknown.
- **Evidence exists, none of it falls in this window, and the nearest
  evidence is older than `staleness_ms`**: `STALE`, same numeric zero as
  above but flagged — nothing heard from this venue recently enough to
  distinguish "quiet market" from "silent reception gap", mirroring how
  the cumulative observation retains a numeric CVD under `STALE` rather
  than discarding it.
- **Last trade *inside* a non-empty window is older than `staleness_ms`**:
  `STALE`, `cvd` retained from the real trades in the window (not
  discarded, not reset to `0.0`).

`instrument`/`exchange` are populated from the nearest evidence in the
first three cases below `NEVER_OBSERVED` (never fabricated, never left
as the bare venue string when a real instrument is actually known).

See `trade_flow_observation.py`'s `WindowedTradeFlowObservation`
docstring for the full reasoning, and
`test_genuinely_no_evidence_at_all_is_never_observed_with_none_fields`,
`test_empty_window_with_stale_older_evidence_is_stale_not_never_observed`,
and `test_empty_window_with_fresh_nearby_evidence_is_available_with_genuine_zero`
in the test file for the three cases exercised directly.

## Side normalization

Reuses `_side_volumes` from the cumulative CVD module — the same
`.upper()` normalization, same Binance/Bybit/OKX casing finding. A
regression test confirms windowed CVD does not bypass it
(`test_windowed_cvd_normalizes_bybit_title_case_and_okx_lowercase_sides`).

## Identity, replay, determinism, corruption resistance

- Correct per-venue `InstrumentId`; Binance/Bybit/OKX proven not to collide.
- Deterministic across repeated calls and reversed input-list order
  (frame *timestamp* order governs, via `ReplaySource`'s own ordering,
  unchanged).
- A corrupted canonical `instrument_key` sitting on disk proven not to
  rewrite the identity raw replay produces — same pattern as
  `tests/test_storage_replay_corruption_resistance.py`, applied to the
  windowed primitive.

## Performance

`observe_trade_flow_at` / `observe_windowed_trade_flow_at` are the reference
implementation. Each call is stateless and replays its whole causal prefix
from scratch: filter all frames (`O(N)`), sort the causal frames, run them
through `ReplayEngine` (JSON decode, adapter normalize, instrument stamp,
duplicate check: ~92% of the time), then tally. One call costs about
**17 us per causal frame** and scales with total frames, not with the window
size; nothing is shared between calls.

Measured (synthetic Binance aggTrade frames at 10 frames/s, 1 CPU, 4 GB,
Python 3.12.3, one fresh process per row; cumulative + 5m + 15m = 3 calls per
observation point):

| Workload | Reference | `TradeFlowObserver` |
|---|---|---|
| 10k frames, 1 call | 0.17 s | build 0.23 s |
| 100k frames, 1 call | 1.7 s | build 2.3 s |
| 500k frames, 1 call | 10.3 s (peak RSS 717 MB) | build 11.0 s (684 MB) |
| 100k frames, 20 points x 3 queries | 61 s | 2.5 s total |
| 100k frames, 100 points x 3 queries | 318 s | 5.0 s total |
| 100k frames, 1000 points x 3 queries | not run (extrapolates to about 45 min) | 26.8 s total |

K observation points over N frames cost `O(K*N)` adapter operations in the
reference (60 calls over 10k frames made 315,000 `normalize` calls against
10,000 frames). `TradeFlowObserver(frames, venue=...)` replays once and
answers every later query by bisecting the trade list: construction is one
replay, a windowed query is `O(log T + T_window)`, and a cumulative query is
`O(T_prefix)` for the unchanged `_side_volumes` sums (kept non-incremental on
purpose so floating-point results stay bit-identical). That remaining term is
what dominates the 1000-point row.

Equivalence rests on two properties that the observer checks while building
and otherwise falls back to the reference path (`observer.mode`,
`observer.fallback_reason`): `timestamp_ms` non-decreasing along replay order
(so the causal frames are a prefix of the replay and a later frame cannot
influence an earlier trade) and every trade stamped with its own frame's
`timestamp_ms`. `tests/test_trade_flow_observer.py` compares every output
field with the reference across all four replay venues, many timestamps and
windows, boundaries, resends and unknown sides, and counts adapter calls so
the single-replay property is protected without wall-clock thresholds.

The numbers are for synthetic frames; real depth/ticker frames in a recorded
session add replay work to both paths equally. Not benchmarked against a
production event rate.

## Explicitly not built here

- Materializing every required window (30s/1m/3m/5m/15m) into the
  production collector — this PR is the causal observation API only.
- OBI, microprice, spread, OFI, liquidation/OI/funding features.
- Cross-venue CVD magnitude comparability — not independently audited
  this session (trade quantity is base-asset volume for all three venues
  here, which is less fraught than the OI unit problem, but this was not
  re-verified).
- Any signal, label, or predictive logic. This is an observation feature.
