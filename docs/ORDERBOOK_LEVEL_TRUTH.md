# Order-book level truth (P0-8)

`feature_computer.compute_orderbook_features()`, used by the live Binance
USD-M orderbook path (`run_collector.py`'s `_handle_binance_orderbook`).

## The defect (fixed)

Before this fix, when fewer than 10 real price levels were available on a
side, the function padded the array up to length 10 by repeating the last
real price with a fabricated `0.0` quantity:

```python
while len(bids_price) < 10:
    bids_price.append(bids_price[-1])
    bids_qty.append(0.0)
```

This made a nonexistent price level indistinguishable from a real level at
the same price with genuinely zero size, and let `obi_level_3`/`obi_level_5`
be computed as though 3 or 5 real levels always existed, even when they
didn't.

## The contract now

- `bids_price`/`bids_qty`/`asks_price`/`asks_qty` contain **only real,
  observed levels** — truncated to the top 10 when more are available,
  **never padded** when fewer exist. A missing level is represented by the
  array simply being shorter; it is never a repeated price with a
  synthetic quantity.
- `bid_depth`/`ask_depth` (new in `ORDERBOOK_SCHEMA` v1.2, nullable) give
  the count of real levels explicitly, so a consumer doesn't have to infer
  truncation-vs-padding from array length alone.
- `obi_level_3` is `null` unless `bid_depth >= 3 and ask_depth >= 3`.
  `obi_level_5` is `null` unless `bid_depth >= 5 and ask_depth >= 5`. A
  level-N metric is only ever written under that name when N real levels
  were actually observed on **both** sides — never computed over whatever
  partial real depth happens to exist and presented as if it were the full
  N levels.
- `obi` (the aggregate, all-levels version) and `obi_level_1` are unaffected
  by depth beyond the pre-existing requirement of at least one real level
  per side (unchanged from before this fix): `obi` is a valid aggregate over
  however many real levels are present (up to the top-10 truncation), and
  `obi_level_1` only ever needs one real level per side, which is guaranteed
  by the time any non-empty dict is returned.
- `best_bid`/`best_ask`/`mid_price`/`micro_price`/`spread`/`spread_bps` were
  already derived only from the top real level and are unaffected.

## Why this was safe to fix without a breaking schema change

`bids_price`/`bids_qty`/`asks_price`/`asks_qty` were always
`pa.list_(pa.float64())` — a variable-length list type, not a fixed-size
array. The "always exactly 10" assumption was purely an application-level
artifact of the removed padding loops, never a genuine storage constraint.
No existing schema migration was required for those four columns.

`bid_depth`/`ask_depth` are additive, nullable `int32` columns (schema
v1.2). Rows written before this fix do not have them (`null`, never
fabricated for them) — those older rows' `bids_price`/`asks_price` may
still contain the pre-fix padding artifact, which is a known, accepted
historical data-quality limitation for data collected before this fix,
not something this migration retroactively repairs.

## Production path verified

`compute_orderbook_features` has exactly two callers in `run_collector.py`:

- `_handle_binance_orderbook` (line ~507) — the **live, production-active**
  path, reachable from `handle_message`'s routing. Its input is the
  **authoritative reconstructed local book** (`applied.bids`/`applied.asks`
  from `LocalBook.apply()`, an unbounded, fully-reconstructed dict of every
  price level currently tracked — not a single incremental diff-depth
  update). Fewer than 10 real levels here means the true reconstructed book
  genuinely has fewer than 10 levels (e.g. shortly after initialization),
  not that one partial update was mistaken for the whole book.
- `_handle_orderbook` (line ~601) — **dead code**, reachable only from a
  direct test call (`test_run_collector_routing.py`), never from live
  `handle_message` routing (confirmed by a repository-wide call-site
  search). Also fixed as a side effect of fixing the shared function, but
  this was not the active production defect.

Downstream: `dataset_assembler.py` drops the raw
`bids_price`/`bids_qty`/`asks_price`/`asks_qty` arrays entirely and never
required exactly-10 semantics; it passes `obi`/`obi_level_1`/`obi_level_3`/
`obi_level_5` through unchanged (a `null` `obi_level_3`/`obi_level_5`
propagates as `NaN` through its Fisher-transform step, same as any other
missing float, with no special handling needed). `validator.validate_orderbook`
only ever required `len(bids_price) >= 1`, never exactly 10.

Bybit's independent order-book feature path (`run_bybit_collector.py`) was
checked and does not have this padding pattern — it builds its arrays
directly from its own reconstructed book with no fabrication loop.
`bid_depth`/`ask_depth` are Binance-USD-M-schema-specific for now; extending
the same explicit-depth pattern to Bybit is not required by this P0 since
Bybit was never fabricating levels, and is left for a future P0 if the
absence of an equivalent depth field there is ever found to matter.

## Not addressed here (separate P0s)

- Float precision for prices/quantities is unchanged — that is P0-9's scope.
- Timestamp semantics are unchanged — that is P0-10/P0-11's scope.

## Tests

`tests/test_feature_computer.py`: exact-10-levels unchanged semantics,
fewer-than-10 bid/ask levels never fabricated, single-real-level-per-side,
missing level-3/level-5 (including asymmetric bid/ask depth) yield `None`
rather than a partial computation, duplicate real prices passed through
faithfully, a real zero-quantity level distinguished from a missing level,
empty asks, malformed input, best-bid/ask from real evidence only, and
more-than-10 real levels truncating (not fabricating) down to 10.

`tests/test_run_collector_routing.py` (live path): a 1-level diff applied to
an authoritative 10-level book still yields a 10-real-level feature row (a
partial update is never mistaken for the whole book), and a genuinely
3-level authoritative book yields exactly 3 real levels with `obi_level_5`
null.

`tests/test_orderbook_depth_persistence.py`: real `ParquetWriter` round trip
of a short-array row (variable-length lists, true nulls, `bid_depth`/
`ask_depth`), schema v1.2 nullability, the research assembler carrying a
null `obi_level_5` through as `NaN` (Fisher transform included) rather than
`0.0`, and statelessness (a thin update after a deep one inherits nothing).

## Replay / causality

- Replay works from raw capture and the reconstructed-book raw rows
  (`_persist_reconstructed_books`), which write `applied.bids`/`applied.asks`
  as-is and never passed through the padding; the padding lived only in the
  derived feature layer, so live and replay agree on the real book.
- `compute_orderbook_features` is a pure function of its input message: no
  cached book, no previous-call state, no fallback to any other record. A
  feature row can therefore only contain levels present in the book state
  it was handed at that moment — no future or stale levels are introduced.
