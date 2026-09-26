# Research dataset time contract (P0-6)

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
