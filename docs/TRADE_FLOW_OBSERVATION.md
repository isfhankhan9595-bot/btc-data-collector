# Causal Trade-Flow Observation (CVD)

First feature in the research/perception layer, built after an independent
audit of Phase H's final state (verified, not trusted: PR #45 merged into
main at `3f66d9a`, 1136 tests passing, `compileall`/`git diff --check`
clean, OKX's documented production limitations confirmed by reading
`run_okx_collector.py` directly). One bounded objective per this phase's
own "no overengineering" instruction — cumulative volume delta (CVD) only,
not the full trade-flow/order-book/OI/funding taxonomy.

## What it answers

"What has aggressive order flow done by local receive time T?" — the
trade-flow analogue of `book_observation.py`'s "what did the book look
like at T?". Same architecture, same reason: a thin, causal, read-only
wrapper around the existing, unmodified `ReplayEngine`. Frames are filtered
to `timestamp_ms <= observation_ts` *before* a `ReplaySource` is
constructed — exclusion happens before a frame can influence anything, not
after.

`collector/collector/trade_flow_observation.py`:
`observe_trade_flow_at(frames, observation_ts, *, venue, staleness_ms=5000)`
→ `TradeFlowObservation(exchange, instrument, observation_ts, status, cvd,
buy_volume, sell_volume, trade_count, last_trade_local_receive_ts, age_ms,
frames_considered)`.

## Real finding: cross-venue side-casing is not consistent

The three adapters do not agree on `CanonicalTradeEvent.side` casing —
confirmed by reading each adapter's `_parse_trades`, not assumed:

- **Binance**: the adapter constructs `"BUY"`/`"SELL"` itself (uppercase),
  never taken from the raw payload directly (derived from the `m` flag).
- **Bybit**: passes its raw `"S"` field through verbatim — `"Buy"`/`"Sell"`.
- **OKX**: passes its raw `"side"` field through verbatim — lowercase
  `"buy"`/`"sell"`.

This module normalizes with `.upper()` before comparing. Nothing upstream
was changed — each adapter's choice to preserve the raw casing (or not) is
itself intentional and not this module's business to alter. Any future
feature that reads `.side` directly, without going through this module,
needs to apply the same normalization or it will misclassify Bybit/OKX
sides.

## Quality gates satisfied (tests, `tests/test_trade_flow_observation.py`, 18)

Following this phase's own gate taxonomy:

- **Gate A (causality)**: exact causal boundary; a future trade proven not
  to pollute a past observation's CVD (constructed adversarially — an
  early buy and a much later, much larger sell — not just a boundary-index
  check).
- **Gate B (identity)**: correct per-venue `InstrumentId`; Bybit and OKX
  proven not to collide.
- **Gate F (missingness)**: no causal frames, or frames-but-no-trade-
  produced, both resolve to `NEVER_OBSERVED` with `cvd=None` —
  **never a fabricated `0.0`**, which would misread as "flow was flat"
  rather than "nothing is known yet".
- **Gate E (staleness)**: a stale observation retains its last-known CVD,
  never discarded or reset to zero.
- **Gate G/H (replay/determinism)**: identical result across two runs and
  across reversed input-list order (frame *timestamp* order, not list
  order, governs — `ReplaySource`'s own ordering, unchanged).
- **Mutation test**: the task's own "Mutation B, remove the causal filter"
  performed against a hand-written copy of the function (not the real one —
  the real one is never mutated in a test run) and proven to produce a
  different, leaked result versus the real function's output on the same
  adversarial input.

## Explicitly not built here

- Rolling/windowed CVD (30s/1m/5m/15m) — this is the raw cumulative
  primitive; windowing needs its own explicit inclusion-boundary and
  late-data-behavior contract per this phase's Section 10, not bolted on
  here.
- Order-book-derived features (OBI, microprice, spread, depth-weighted
  imbalance) — separate feature family, separate module, separate audit.
- OFI (order-flow imbalance) — the task explicitly warns against computing
  it from snapshots as if they were incremental updates; deferred until
  that distinction is deliberately designed for, not assumed compatible
  with this primitive.
- Liquidations, OI, funding features — not started.
- Cross-venue CVD comparison/aggregation — `buy_volume`/`sell_volume` are
  in each trade's own `quantity` unit; nothing here claims Binance's BTC
  quantity is directly comparable to Bybit's or OKX's without further
  audit (unlike OI, trade quantity is base-asset volume for all three
  venues here, so this is *less* fraught than the OI unit problem, but it
  was not independently re-verified in this session and should not be
  assumed).
