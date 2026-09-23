# Hostile audit of PR #47 (windowed CVD) + spread/microprice primitive

## Audit of PR #47

Verified independently against the real repository before any code was
written: PR #47 merged (`merge_commit_sha` `21fcab4`, matching `main` HEAD
exactly), baseline **1184 passed**, compileall clean, diff-check clean.

**Causal boundary, cumulative-vs-windowed distinction, unknown-side
handling, staleness-vs-window-membership, missingness (cases A-D),
determinism, identity separation, and corruption resistance**: all
independently reviewed against real fixture-driven tests (not simulated),
found correct. The `(window_start, window_end]` contract is exactly
`(observation_ts - window_ms, observation_ts]` as claimed, verified by the
existing boundary tests, all of which call the real function.

**Duplicate-trade protection**: verified independently (not trusted from
the module's own docstring) — grepped every adapter, `sequence.py`, and
`replay.py` for dedup/cache logic; none exists. The docstring's claim
("the trade pipeline has no duplicate-trade-message protection anywhere")
is accurate. Not fixed here, per this task's own no-silent-compensation
rule: it is a pipeline-layer gap, not a trade-flow-module defect, and
fixing it would touch shared adapter/replay infrastructure well beyond
this bounded task. Recommended as the next task below.

**Mutation-testing rigor gap found and closed.** PR #46/#47's four
"mutation" tests each hand-write a second, inline reimplementation of the
function and assert its output differs from the real one — a
reimplementation-comparison, not proof that this project's actual
regression suite would catch a real regression, and weaker than the
real-source-mutation convention this project used in Phase D/H. Redone
here with real source mutation, in-session (edited, tested, restored,
`cmp` confirmed byte-identical each time):

| Mutation | Location | Real failures |
|---|---|---|
| Remove `_causal_trades`'s pre-replay filter only | `trade_flow_observation.py` | 4 (all in `test_trade_flow_observation.py` — cumulative CVD's only protection) |
| Remove `observe_windowed_trade_flow_at`'s own upper bound only | `trade_flow_observation.py` | **0** |
| Both removed together | `trade_flow_observation.py` | 9 (4 + 5 in `test_windowed_trade_flow_observation.py`) |

**Finding, not previously known:** the windowed function's own `<=
window_end` check is currently fully redundant — `window_end` is always
exactly `observation_ts`, so the shared `_causal_trades` call already
enforces that exact bound before the window filter runs. PR #47's own
inline simulation could not have found this: its hand-copied "mutated"
function always removed both filters at once. This is not a defect —
defense-in-depth is a reasonable, deliberate choice — but the claim that
the window's own bound is independently protective was untested and, in
isolation, false today. `tests/test_trade_flow_mutation_audit.py` pins
the invariant (`window_end == observation_ts`, always) that currently
makes the redundancy safe, so a future change decoupling them would have
to consciously touch a failing test rather than silently lose protection.

**Verdict: Case 2** (PR #47 is causally sound; evidence-rigor gap closed
with real mutation testing and one pinning test — no behavior changed).

## New primitive: spread and microprice (`book_metrics.py`)

Chosen per the task's feature hierarchy (foundational correctness >
causal observation > state reconstruction > data-quality observability >
microstructure primitives): the smallest composable primitive that adds
zero new causal/replay surface, because it is a pure function of an
already-reconstructed, already-audited `BookObservation` — no frames, no
`observation_ts` of its own, no replay. `test_book_metrics_is_a_pure_function_no_new_causal_surface`
confirms by source inspection that the module never imports
`ReplayEngine`/`ReplaySource`/`ReplayFrame` at all.

- `best_bid_price`/`best_bid_qty`/`best_ask_price`/`best_ask_qty`: top of
  book, independently reported per side.
- `spread`, `spread_bps`, `mid_price`: `None` unless both sides are
  present — never fabricated from one side.
- `microprice`: standard Stoikov formula, each side's price weighted by
  the *opposite* side's quantity. Direction verified with genuinely
  asymmetric sizes (the shared fixture builders can't express asymmetry):
  more resting quantity at one level is support/resistance there, pulling
  the fair price toward the *other*, thinner side.
- Relies on, but does not re-check, `LocalBook._validated_maps`'s
  `max(bids) >= min(asks)` rejection (Phase H, already audited): whenever
  both sides are present, `spread` is always strictly positive. Pinned by
  `test_spread_is_always_strictly_positive_when_both_sides_present`
  against real reconstructed output.
- One-sided books (reachable via a qty-0 deletion of the only level on
  one side, confirmed — a lone one-sided snapshot is rejected outright by
  Binance's bridging requirement) report that side alone; all three
  derived quantities are `None`, not fabricated.

7 new tests, all built on the real, unmodified `reconstruct_book_at` and
real fixture-derived sessions (bridging diff + snapshot, deletion via
qty=0) — no hand-built book state.

## Verification

Full suite: **1193 passed** (1184 + 2 audit + 7 book-metrics). compileall
clean. `git diff --check` clean.

## Recommended next bounded task

**Trade-message deduplication.** The highest-priority item per this
task's own hierarchy ("foundational data correctness" ranks first), a
real (not hypothetical) gap affecting every consumer of
`non_book_events` today, already identified and documented by PR #46/#47
but explicitly deferred as out of scope for a feature module. Requires
its own audit: determine each venue's actual trade-identity field
(Binance's `a` aggTrade id, Bybit's and OKX's own trade-id fields — none
independently confirmed here), decide the correct architectural layer
(most likely alongside `sequence.py`'s existing per-venue continuity
model, or in `replay.py`'s dispatch, not inside a feature module), and
scope the blast radius (touches shared adapter/replay infrastructure used
by every current and future trade-flow consumer, not an isolated
feature) — deliberately not attempted in this session as its own bounded
task.
