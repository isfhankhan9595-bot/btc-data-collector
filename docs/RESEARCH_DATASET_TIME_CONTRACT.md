# Research dataset time contract (P0-6 / P0-7)

`collector/pipeline/dataset_assembler.py`. Governs which clock decides whether
a row of information was available to the collector at a given research
observation time. This is the flattened-parquet-feature-layer counterpart to
`CROSS_EXCHANGE_ALIGNMENT.md`'s canonical-event-layer rule; both encode the
same principle, `local_receive_ts <= observation_ts`, at different layers of
the pipeline.

## The three clocks

Every feature row the assembler reads (from `ORDERBOOK_SCHEMA`,
`TRADES_SCHEMA`, `MARKPRICE_SCHEMA`) can carry up to three distinct
timestamps:

| Column | Meaning | Producer | Safe for causal alignment? |
|---|---|---|---|
| `exchange_timestamp` | The venue's own clock: when the exchange says the event happened. | Adapters, parsed from the raw payload. | **No.** Descriptive only. Exchanges are not synchronised with each other or with the collector. |
| `local_timestamp` | The collector's *receive* time: the local instant the information actually became available. | Captured once, at frame arrival (`handle_message` entry / raw-wire receipt), carried through unmodified via `local_receive_ts` on `CanonicalEvent`. | **Yes.** This is the availability/eligibility clock. |
| `timestamp` | Historically ambiguous. In the schemas this module reads, it is the local *processing* timestamp: when application code got around to handling the event (after book reconstruction, lock acquisition, etc.), which can lag receive time by an arbitrary amount. | Set at the point application code happens to run. | **No.** Descriptive/diagnostic only. |

## The rule

For every causal join the assembler performs (orderbook and markprice
`merge_asof`, trade-to-grid binning):

```
local_timestamp (availability) <= observation_ts
```

Never `exchange_timestamp <= observation_ts`, and never
`timestamp (processing) <= observation_ts`. A future-received row can never
appear early because its exchange timestamp is old; a processing delay can
never delay a row's availability past its actual receive time.

## Known historical hazard (fixed by P0-6)

Before this fix, the live Binance USD-M orderbook/trades handlers
(`_handle_binance_orderbook`, `_handle_binance_trade` in `run_collector.py`)
wrote:

```python
features["timestamp"] = applied.local_process_ts   # processing time
features["local_timestamp"] = applied.local_receive_ts  # true receive time
```

...while `dataset_assembler.py` aligned on `frame["timestamp"]` — i.e. on
processing time, not receive time. The markprice handler had an even more
basic version of the same defect: it never captured a receive time at all;
`local_timestamp` was simply set to a second copy of the processing
timestamp. Both are fixed: the assembler now aligns on `local_timestamp`
(`<prefix>_ts` internally), and markprice's handler now stamps
`local_timestamp` from the real `local_receive_ts` captured at frame
arrival, mirroring the existing orderbook/trades pattern.

## Legacy / missing receive-time data

Some older segments (and minimal test fixtures) carry only `timestamp`, with
no `local_timestamp` column. Nothing fabricates a receive time for these:
the assembler falls back to `timestamp` for that stream's rows only when
`local_timestamp` is genuinely absent, and every such row is flagged in the
aligned output via `<stream>_time_unknown = True` (`orderbook_time_unknown`,
`markprice_time_unknown`, `trades_time_unknown`). Ambiguous legacy timestamp
semantics are never presented as causally safe.

## Ties

Rows are ordered with a stable sort on the availability clock (`kind="stable"`
in `sort_values`), so two rows sharing one `<prefix>_ts` keep their original
input order — the same tie rule `causally_align()` documents for the
canonical-event layer. Determinism does not depend on filesystem iteration
order.

Tests: `tests/test_dataset_assembler.py`,
`tests/test_dataset_assembler_causal_contract.py` (adversarial leakage,
future-exchange-timestamp, processing-delay, cross-stream consistency,
legacy-fallback flagging, determinism), `tests/test_run_collector_routing.py`
(`test_markprice_local_timestamp_is_receive_time_not_processing_time`).

## P0-7: open interest and liquidations

Before P0-7, `dataset_assembler.py` contained **zero references** to
`openinterest` or `liquidation` at all — both were genuinely collected (their
own `ParquetWriter`s, `OPENINTEREST_SCHEMA`/`LIQUIDATION_SCHEMA`) but never
read into the research dataset. Raw data existed; research data did not
represent it. P0-7 fixes this while preserving the P0-6 contract above.

**Dependency found and fixed:** `_handle_liquidation` in `run_collector.py`
had the exact same defect class markprice had before P0-6: `compute_liquidation_features`'s
`"timestamp"` is a fresh `time.time()` call (processing time), and
`"local_timestamp"` silently duplicated it — no genuine receive time existed
for liquidation at all. This also broke live/replay parity: `BinanceAdapter.normalize()`
already threads a real `local_receive_ts` into `CanonicalLiquidationEvent`
for the replay path. Fixed the same way markprice was: `local_receive_ts`
(captured once at `handle_message` entry) is now threaded through and stamps
`local_timestamp`.

Binance OI's write path (`_poll_openinterest`) was already correct before
P0-7: both `"timestamp"` and `"local_timestamp"` are `event.local_receive_ts`.

### Open interest

- Joined with `merge_asof` exactly like markprice: backward, receive-time
  keyed, with a staleness tolerance (`OI_STALE_MS = 5000`, comfortably above
  the ~3s `OI_POLL_INTERVAL_S` REST-poll cadence in `run_collector.py`).
- A miss or stale reading is `NaN` + `openinterest_gap = True` — **never**
  `open_interest = 0`. A poll outage and a genuine zero-OI reading are not
  interchangeable, and none is fabricated to stand in for the other.
- No unit conversion is performed anywhere in the assembler: Binance's
  `/fapi/v1/openInterest` response is already in native contract units
  (`binance_oi.normalize_binance_oi` passes it through unchanged), so there
  is nothing to silently rescale.
- **Replayable at the canonical-event layer, but not reconstructed into this
  assembler's input.** Correcting an earlier version of this document:
  Binance OI **is** replayable. `ReplaySource.from_records` routes a
  recorded `purpose="open_interest"` REST row into a `FrameKind.REST_OI`
  frame ordered by `response_receive_ts` (never the exchange-reported
  time), and `ReplayEngine._handle_rest_oi()` sends it through
  `binance_oi.normalize_binance_oi()` — **the exact same normalizer**
  `run_collector.py`'s live poll loop calls — producing an identical
  `CanonicalOIEvent` with `local_receive_ts = response_receive_ts`. This is
  proven by the repository's existing
  `tests/test_binance_oi_replayability.py` (predates P0-7; in particular
  `test_live_and_replay_produce_identical_canonical_events_from_the_same_body`
  and `test_availability_is_the_response_receive_time_not_the_exchange_time`),
  which P0-7 re-ran and confirmed still passing rather than duplicating.
  What replay does *not* currently do is write its `CanonicalOIEvent`
  output back into the flattened `OPENINTEREST_SCHEMA` parquet this
  assembler reads — no such pipeline stage exists. So in practice this
  assembler only aligns whichever OI segments `_poll_openinterest` actually
  wrote live; reconstructing a day's OI from raw REST records via replay,
  into this assembler's input format, would need a new (currently
  nonexistent) stage and is out of P0-7's scope. See `docs/REPLAY.md` for
  the full replay-support table.

### Liquidations

- Aggregated per grid bin the same way trades are: binned by the causal
  availability clock (never processing time), producing
  `liquidation_count`, `liquidation_buy_volume`, `liquidation_sell_volume`
  (side: `+1`=BUY/short-liquidated, `-1`=SELL/long-liquidated, per
  `feature_computer.compute_liquidation_features`), `liquidation_net_volume`,
  `liquidation_notional`.
- **No venue-assigned unique event id — dedup here is a heuristic, not
  exact deduplication.** Unlike trades (`trade_id`), Binance's `forceOrder`
  liquidation stream carries no id field, and no repository evidence (in
  `canonical.py`, `binance_oi.py`, or the raw payload shape) establishes
  that the exchange guarantees any tuple of fields is a unique identifier.
  Rows sharing an identical `(exchange_timestamp, side, price, quantity)`
  tuple are dropped before aggregation, keeping the first, on the
  assumption that this is a WS-redelivered copy of one event. **This is not
  proven identity**: two genuinely distinct liquidations could in principle
  share all four fields (same millisecond, same side, same price, same
  size) and would then be incorrectly collapsed into one. No fabricated ID
  is invented and no raw evidence is discarded beyond this specific,
  explicitly-labeled collision risk — the alternative (not deduplicating at
  all) trades a rare false negative for a comparatively more likely false
  positive from genuine WS redelivery, which is the more common failure
  mode this heuristic targets. Do not describe this elsewhere as "exact" or
  "guaranteed" deduplication.
- **Empty-interval semantics are explicit**, per the requirement that "no row
  = zero" must not be silently assumed: `liquidation_stream_available`
  distinguishes a day with **no liquidation segment collected at all**
  (`False` — the zero columns carry no evidentiary weight, we simply don't
  know) from a day where the stream **was** collected and a bin genuinely saw
  no events (`True`). **Superseded for per-bin claims**: that flag is
  day-level, and a `True` does NOT make an empty bin a confidently observed zero
  (a mid-day market-socket outage leaves it `True`). Per-bin outage detection is
  now `liquidation_bin_observed` (see "Availability truth" below). The earlier
  assumption that this needed a join against the quality-event stream was
  unnecessary: the market socket's own receive-time frames already bound an
  outage.

### Instrument/venue isolation

Both OI and liquidation frames pass through `_enforce_single_instrument`
(applied uniformly to all five streams this assembler reads) as
defense-in-depth: any row whose `instrument_key` contradicts the single
expected instrument (`BINANCE_USDM_BTCUSDT`) is dropped and reported, never
silently blended in. This is a second layer behind the write-time validation
(`_binance_usdm_instrument_key`) that already rejects mismatched symbols
before they reach a writer.

Tests: `tests/test_dataset_assembler_oi_liquidation.py` (OI/liquidation
happy path, missing-vs-stale-vs-unavailable semantics, future-receive
leakage, exchange/processing-timestamp substitution, instrument isolation,
no silent unit conversion, liquidation dedup, OI tie-resolution),
`tests/test_run_collector_routing.py`
(`test_liquidation_local_timestamp_is_receive_time_not_processing_time`).

## Timestamp field inventory (P0-10)

Traced from repository source, not from names.

| Field | Meaning | Actual source | Used for | Causal? |
|---|---|---|---|---|
| `exchange_timestamp` / `exchange_event_ts` | venue clock | parsed from payload by adapters | descriptive | No |
| `local_receive_ts` / `local_timestamp` | instant the frame became available to the collector | `WebSocketClient._consume`, first statement after the frame arrives (before decode, raw capture, queue) | **the** causal availability clock; replay's frame clock; alignment/assembler eligibility | **Yes** |
| raw wire `timestamp` | equals `local_receive_ts` | `RawWireRecord.to_row` | segment ordering | Yes (same value) |
| `local_process_ts` / canonical `timestamp` | when application code handled the event | `time.time()` in the runner handlers | diagnostics, processing latency | No |
| `local_capture_ts` | **legacy**: when the raw row was built | `time.time()` in `run_collector._capture_raw_frame` or `RawWireRecord.to_row` fallback | nothing (write-only provenance) | **No** |
| persistence time | when Parquet was written | not recorded per row | n/a | No |

Queue delay never touches `local_receive_ts`: the value is stamped before
the frame is enqueued and travels inside the `IngestItem`; the worker's
later `time.time()` calls only feed processing-time fields.

Known, documented, not changed by P0-10: adapters and
`run_collector.handle_message` fall back to `time.time()` when a direct
caller passes no `local_receive_ts`. The live path (`WebSocketClient`) and
replay always supply one, so the fallback only affects ad-hoc direct
calls; it is a fabrication risk for such callers and is recorded as a
remaining issue rather than widened into this change.

## P1 assembler integrity audit

### Day boundary (fixed)

Trades and liquidations are binned to `(T - grid_ms, T]` with `T = start_ts +
ceil((t - start_ts) / grid_ms) * grid_ms`, on the availability clock. Segment
files are partitioned by the **wall-clock hour of the write call**
(`ParquetWriter._get_current_hour_str`), not by row receive time, so a row
received just before midnight but written just after it sits in the new day's
segment with `local_timestamp < start_ts`. Previously `ceil` clamped any event
in `(start_ts - grid_ms, start_ts)` into the **first bin of the day**, and an
in-day event in `(last_grid, end_ts)` mapped to a bin label `>= end_ts` that has
no row and was dropped without a trace.

Now only events with `start_ts <= t <= last_grid` are binned. Everything else is
excluded **and counted** in a `WARNING` line: before `start_ts`, in the trailing
partial bin (no grid row exists), or at/after `end_ts`. A consequence worth
stating: the final `< grid_ms` of each day cannot be represented in that day's
dataset, and the previous day's tail is no longer carried into the next day's
first row. Carrying it would require attributing out-of-day evidence to the
requested day, which this contract forbids.

`merge_asof` streams (orderbook, markprice, OI) are unaffected: a backward join
on receive time within a tolerance only ever carries earlier-available state
forward, which is causal.

### Stream availability

`trades_stream_available` mirrors `liquidation_stream_available`: `False` means
no trades segment was collected for the day, so the zero trade columns carry no
evidentiary weight. **`True` is a day-level fact only ("a segment exists") and is
NOT proof that an empty bin was observed**: a mid-day disconnect leaves it `True`
while every bin in the outage reads `trade_count == 0`, bit-for-bit identical to a
bin where the stream was alive and nothing traded (demonstrated, see "Availability
truth" below). The per-bin proof is `trades_bin_observed`. `trade_flow_imbalance`
is `0.0` for an empty bin (unchanged); use `trade_count` to tell "balanced" from
"no trades", and `trades_bin_observed` to tell "no trades" from "no evidence".

### Liquidation dedup (heuristic, now measurable)

The `(exchange_timestamp, side, price, quantity)` key cannot prove identity.
Changes: rows are ordered by availability time **before** `keep="first"` (so the
earliest-received copy survives regardless of segment file order); rows with a
null `exchange_timestamp` are never collapsed (NaN compared equal to NaN);
`liquidation_dup_dropped` (new, int32) counts rows the heuristic collapsed, in
the bin of the collapsed row's own receive time. A nonzero value is a lower bound
on possible over-collapse, not an error count.

### Null availability time

A row whose `local_timestamp` is null (or null `timestamp` on the legacy
fallback path) is dropped with a `WARNING` count. Previously `NaT.astype("int64")`
produced a hugely negative sentinel that silently fell out of every join.

### Audited, no change

* Legacy fallback to `timestamp`: in every writer in this repository `timestamp`
  is processing time (`time.time()` at handling), which is never earlier than
  receive time, so the fallback can only delay availability, never advance it.
  It cannot create false historical causality; rows are retained and flagged
  `*_time_unknown`. **No downstream consumer (`label_generator`,
  `split_generator`, `stats_computer`) reads these flags**, so they document
  uncertainty without enforcing anything. Rows written before P0-6/P0-7 may carry
  `local_timestamp == timestamp` (processing time) and are *not* flagged.
* Orderbook validity: `ORDERBOOK_SCHEMA` carries no `quality_state`. The live
  handler only persists rows after the book applied a diff in a valid state
  (`run_collector._handle_binance_orderbook` returns before writing on a gap or
  recovery), so invalid books are absent rather than marked. The assembler cannot
  verify this from the parquet and invents no signal; staleness is bounded only by
  the 500 ms tolerance. `bids_*`/`asks_*` are dropped, `bid_depth`/`ask_depth` kept.
* Unknown trade side: a null `is_buyer_maker` raises; it is never guessed.
  Upstream (`validator.validate_trade`) rejects `quantity <= 0`, so the assembler
  has no non-positive-quantity guard of its own (a negative quantity would subtract
  from `buy_volume`).
* Equal availability timestamps: the stable sort keeps input order and
  `merge_asof` takes the last row, so the last-written row wins; earlier states at
  the same millisecond are overwritten without a trace. Deterministic, not lossless.
* Upstream, not changed here: `compute_liquidation_features` maps any `S` other
  than `"BUY"` (including empty/unknown) to side `-1`.

Tests: `tests/test_dataset_assembler_integrity_audit.py`.

## Availability truth: observed zero vs no evidence (Agent 10)

Rule: *absence of an event is not evidence of zero activity unless the stream was
demonstrably delivering for that interval.* Until this section the assembler could
not state that per bin. Reproduced against the real assembler (1 s grid): a trades
stream with a 05:00-05:10 collector outage and one alive-but-quiet second at
12:00:10 produced, for 05:05:00 and 12:00:10, **identical values in every trade
column including `trades_stream_available == True`**. Same for liquidations over a
20-minute market-socket outage.

### What is proven, and from what

Per-bin proof reuses the repository's one evidence-interval model,
`collector/collector/coverage.py::compute_coverage` (an observation at `t` covers
`[t, t + tolerance)`; leading, interior, trailing and total-absence gaps). The
assembler only maps that model's gaps onto the grid; it defines no second
availability system. Evidence is always the receive-time availability clock
(`<stream>_ts`), never exchange or processing time.

A grid row `T` owns the receive-time bin `(T - grid_ms, T]`, clipped to the
requested day (the bin `_grid_bin` assigns events to). It is **observed only if
every millisecond of that bin is covered**. A millisecond `x` is covered only by
receipts at or before `x`, so a row's flag depends only on frames received at or
before `T`; a later frame can never back-fill an earlier bin.

### New fields

| | `trades_bin_observed` | `liquidation_bin_observed` |
|---|---|---|
| meaning | the trades stream demonstrably delivered throughout the bin | the shared market WebSocket demonstrably delivered throughout the bin |
| input evidence | receive time of every trade row in the day's trades segments | receive time of every trade, markPrice and liquidation row (duplicates included) |
| availability clock | `local_timestamp` (receive time); legacy fallback to `timestamp` is flagged by `trades_time_unknown` | same |
| aggregation | `compute_coverage` gaps over the day, mapped to bins; bin observed iff no gap overlaps it | same, over the union of the three streams' receipts |
| tolerance (staleness budget) | `config.TRADES_STALE_MS` = 5000 ms | `config.MARKPRICE_STALE_MS` = 5000 ms |
| missing behavior | no trades segment, or no usable receipt: `False` for every bin | no liquidation segment, or no usable receipt: `False` for every bin |
| stale behavior | more than the tolerance after the last receipt: `False` | same |
| invalid behavior | null/non-numeric receive time is dropped (counted, existing WARNING) and is not evidence | same |
| units / type | bool (never null) | bool (never null) |
| causal guarantee | depends only on receipts `<= T`; future receipts, exchange time and processing time never contribute | same |

Why the market socket: `aggTrade`, `markPrice@1s` and `forceOrder` are subscribed
on ONE WebSocket (`BINANCE_MARKET_WS_URL`, `stream_group="market"`;
`run_collector.py` builds one `WebSocketClient` for it and `_route_stream` fans the
frames out). Liquidations are event-sparse (`LIQUIDATION_STALE_MS` is 30 min), so
the stream's own frames cannot bound an outage; markPrice's 1 s cadence can. The
order book rides a **separate** socket (`BINANCE_PUBLIC_WS_URL`) and is never
evidence for these two streams.

### Semantic matrix

| Condition | Value columns (`trade_count`, `*_volume`, `liquidation_*`) | Flag that carries the truth |
|---|---|---|
| stream observed, genuinely zero | `0` | `*_bin_observed == True` |
| stream/connection down inside the bin | `0` (**not evidence**) | `*_bin_observed == False` |
| no segment for the day | `0` (**not evidence**) | `*_stream_available == False`, `*_bin_observed == False` |
| bin only partly covered, events present | real events, **lower bound** | `*_bin_observed == False`, `trade_count > 0` possible |
| receive time unknown (legacy column absent) | value present | `*_time_unknown == True` |
| receive time null | row dropped, counted | n/a |
| stale beyond tolerance | `0` (**not evidence**) | `*_bin_observed == False` |
| future receipt | never aggregated into an earlier row, never evidence for it | n/a |
| unreadable source file | assembly raises; no dataset is written | n/a |

`*_bin_observed == True` implies `*_stream_available == True`; never the reverse.
Count and volume columns are deliberately **not** changed to NaN: an unobserved bin
still stores `0`, and consumers must gate on `*_bin_observed`. A zero with
`*_bin_observed == False` is "no evidence", never "observed none".

### Known limits (not hidden)

* **Tolerance is a declared budget, not a measurement.** A bin up to 5 s after a
  real drop can still read `observed` with a zero count, because the last receipt's
  evidence interval has not yet expired. Bounded, not eliminated.
* **Liquidation proof is connection liveness.** It proves the shared socket was
  delivering, not that the `forceOrder` subscription alone was healthy; Binance
  sends no `forceOrder` heartbeat, so a per-subscription stall is undetectable here.
* **No liquidation segment for a whole day is never observed**, even if the market
  was quiet (fail-closed; BTCUSDT normally has liquidations daily).
* **Order-book validity is not provable from the parquet.** `ORDERBOOK_SCHEMA` has
  no `quality_state`. The guarantee is producer-side and executable:
  `run_collector._handle_binance_orderbook` returns before writing when
  `book_source != "DIFF_DEPTH_RECONSTRUCTED"` and when `LocalBook.apply()` (the
  `binance_book`) returns `None` (gap/recovery/invalid), so invalid books are
  *absent*, not marked.
  `orderbook_gap == False` therefore means "a row was received within 500 ms", never
  "validated". Rows written before that guard existed (the legacy
  `_handle_orderbook` partial-depth path has no production caller now) cannot be
  identified from the data.
* **`*_time_unknown` is generated, not enforced.** No code outside tests reads these
  flags. `orderbook_/markprice_/openinterest_time_unknown` are nullable (`None` in a
  gap row, where there is no observation) while `trades_`/`liquidation_` are
  coerced to bool. In a day mixing legacy segments (no `local_timestamp`) with
  modern ones, the legacy rows are dropped (WARNING) rather than flagged, so their
  bins read `orderbook_gap == True`; fail-closed, but the loss is only in the log.
* **`scripts/gap_report.py` measures coverage on `timestamp` (processing time)**,
  while this assembler uses `local_timestamp`. Its OI tolerance (300 000 ms) is also
  60x the assembler's (`OI_STALE_MS = 5000`). Not changed here.

Tests: `tests/test_dataset_assembler_availability_truth.py`.
