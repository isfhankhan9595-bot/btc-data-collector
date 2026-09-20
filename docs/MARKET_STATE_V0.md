# Market State Engine V0

## Purpose

Describes **what market conditions currently exist** for one venue, from
validated canonical events, using only what the collector had actually
received by a given moment.

## Non-goals

- **Not a trading signal.** No BUY/SELL output, no confidence score, no
  prediction, no regime-descriptor labels in V0 (deferred; see "Deferred
  from V0" below).
- **Not cross-exchange.** One `MarketStateEngine` instance describes one
  exchange (`update()` raises if given an event from another venue).
  `pipeline/cross_exchange_alignment.py` exists but has no dedicated test
  file, and building derived cross-venue state on top of an unverified
  alignment primitive risks silently collapsing two venues' semantics into
  one — deliberately out of scope until that primitive is itself tested.
- **Not spot-aware.** No real BTC spot feed exists in this collector yet, so
  no spot/perp basis is computed or implied anywhere in this module.
- **Not derived from `feature_computer.py` or `dataset_assembler.py`.**
  `feature_computer.py` reads raw Binance-message keys directly (e.g.
  `msg.get("E", timestamp)`, silently substituting wall-clock time when a
  field is absent) and has no persistent-book state — reusing it would
  silently produce wrong values on non-Binance payload shapes. This module
  is a new, independent, causal implementation.

## Input events

`CanonicalTradeEvent`, `CanonicalOrderBookEvent`, `CanonicalLiquidationEvent`,
`CanonicalMarkPriceEvent`, `CanonicalOIEvent` (all from `canonical.py`,
unmodified). An event type this engine does not model is silently ignored by
`update()`, not an error.

## The causal rule

**`local_receive_ts`, never `exchange_event_ts`, decides availability.**

`MarketStateEngine.snapshot(observation_ts)` includes only events with
`event.local_receive_ts <= observation_ts`. A slow REST response's exchange
timestamp can predate when the collector actually received it; using it
would let replay (or live) claim information earlier than it was actually
known. See `test_market_state_v0.py::test_snapshot_uses_local_receive_ts_not_exchange_event_ts_for_availability`
and `::test_future_exchange_timestamp_cannot_pull_an_event_into_an_earlier_snapshot`.

## Why `snapshot()` is a pure function, not an incremental update

`snapshot(observation_ts)` recomputes state from scratch from the full
stored event list each call, rather than mutating one running "current
state". This is a deliberate simplification, made because it turns several
required guarantees into structural properties rather than merely tested-for
ones:

- A late-arriving event cannot rewrite an earlier snapshot — there is no
  mutable state for it to overwrite; a previously-returned `MarketState` is
  a frozen dataclass, unaffected by any later `update()` call.
- Replaying the same events in a different insertion order produces an
  identical result — `snapshot()` sorts by `local_receive_ts` internally
  before computing anything; call order of `update()` never matters.
- No wall-clock reads, no network access, no randomness are structurally
  possible inside a pure function of its stored inputs (enforced by an AST-based
  test, `test_market_state_module_has_no_wall_clock_or_network_imports`, not
  just a docstring claim).

The tradeoff: this recomputes from the full history on every call rather
than maintaining O(1) incremental state. Acceptable for V0's purpose
(describing state, not a hot low-latency path); revisit if profiling ever
shows it matters.

## Field provenance and staleness

Every state dimension carries an explicit freshness signal rather than
silently going stale-looking-fresh:

| Dimension | Freshness field | Default threshold |
|---|---|---|
| Book | `book_stale` | 5,000 ms |
| Mark/index price | `mark_stale` | 10,000 ms |
| Open interest | `oi_stale` | 120,000 ms |
| Funding | `funding_stale` | 3,600,000 ms |

`price_vs_mark` is `None` whenever the mark price is stale — a derived
comparison is never computed from data already flagged untrustworthy.

**Missing is not zero.** `trades_observed`, `book_available`,
`liquidations_observed` are separate boolean fields from the numeric
totals they gate. Zero liquidations *observed* (`liquidations_observed=True,
liquidation_count=0`) and a liquidation stream that has *never produced an
event* (`liquidations_observed=False`) are different facts and are never
collapsed into the same `0`.

**Known limitation, stated plainly:** there is no explicit staleness flag
for the trade-flow or liquidation dimensions themselves (only book/mark/OI/
funding have one). If a trade stream stops pushing mid-session,
`trades_observed` stays `True` forever with an aging `last_trade_ts` that a
caller must check manually — there is no `trade_flow_stale` field yet. Not
fixed in V0; a real design decision (event-driven streams don't have a
single obvious staleness threshold the way a periodic poll does) deferred
rather than guessed at.

## Open-interest unit safety

Uses the existing `OIUnit`/`assert_comparable_oi` contract from
`canonical.py` without weakening it. `oi_change` is computed only between
two consecutive observations **from the same engine instance** (therefore
same exchange, same unit by construction) — this is exactly the one case
that contract calls safe even when the unit itself is `UNKNOWN` ("one
venue's stream is one physical quantity regardless of whether its name is
documented"). This module performs no cross-venue OI comparison anywhere;
`assert_comparable_oi` is exercised directly in tests, not by this engine's
own logic, since venue isolation makes it structurally unreachable here.

## Liquidation direction — deliberately not labelled long/short

A forced BUY liquidation order generally closes a short position and a
forced SELL generally closes a long, but this mapping has not been
individually re-verified against each adapter's actual `side` semantics in
this pass. `LiquidationState` tracks `buy_side_quantity`/`sell_side_quantity`
using the raw venue-reported side only — never relabelled `long_liquidated`/
`short_liquidated`.

## Order-book authority

`update()` refuses any `CanonicalOrderBookEvent` whose `book_source` is in
`book_engine.NON_AUTHORITATIVE_BOOK_SOURCES` (currently `PARTIAL_DEPTH`).
`run_collector.py` and `run_bybit_collector.py` already filter this before
persisting, but this is defense in depth against a future caller that
doesn't remember to.

## Digest and replay parity

`MarketState.digest()` hashes an explicit, fixed tuple of every field (not
Python's `hash()`, which is per-process-salted for strings and therefore not
stable across runs — exactly the nondeterminism this module must not
exhibit). `test_market_state_replay_parity.py` drives real recorded frames
through the real `ReplayEngine`, then the real `MarketStateEngine`, and
proves: replaying the same session twice gives an identical digest;
mutating a trade price, a liquidation quantity, funding, or OI each changes
the digest; an event never fed to the engine has zero effect (mutation-tests
the causal cutoff itself, not just the happy path).

**Scope, stated plainly:** `ReplayResult.book_updates` records only
`best_bid`/`best_ask` as strings (see `replay.py`'s `BookUpdate`), not full
bid/ask arrays, so it cannot be turned back into a `CanonicalOrderBookEvent`.
Replay parity is proven here for trade flow, liquidations, mark/funding, and
OI — **not** for book state. No live exchange session was used; parity is
proven against recorded/synthetic frames through the real code path, as
recorded throughout this project's other replay tests.

## Deferred from V0

- **Regime descriptor** (TREND/RANGE/HIGH_VOLATILITY/etc.) — the document
  that scoped this phase marks it optional for V0 and it adds a full
  classification-logic surface; left out to keep V0 "small enough to fully
  audit."
- Cross-exchange state, spot/perp basis, trade-flow/liquidation staleness
  flags — all stated above as explicitly out of scope or a known
  limitation, not silently absent.
