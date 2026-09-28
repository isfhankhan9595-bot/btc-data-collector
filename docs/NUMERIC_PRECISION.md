# Numeric precision contract (P0-9)

Exchange prices, quantities, rates and open interest are **decimal text**
(all four supported venues send them as JSON strings). Before P0-9 the
adapters and feature layer parsed them with `float(...)`, so the value the
system stored was the nearest binary64, not what the venue sent.

## The contract

| Layer | Representation | Exact? |
|---|---|---|
| Raw wire / raw REST capture (`payload`) | the venue's text, verbatim (`pa.string()`) | **yes** (unchanged) |
| Canonical events (`canonical.py`) | `Decimal` for every venue-decimal field | **yes** |
| Local book (`book_engine.py`) | `Decimal` price keys / quantities | **yes** (was already so) |
| Feature dicts (`feature_computer.py`) | `Decimal` evidence; `float` only for explicitly derived values | evidence yes |
| Parquet, `<field>_exact` columns (new, nullable `string`, or `list<string>`) | the venue's decimal text | **yes** |
| Parquet, existing float64 columns | `float(exact value)` — a **derived, approximate** copy | **no, by design** |
| Research dataset (`dataset_assembler.py`) | float64 columns (derived) + scalar `_exact` companions carried through | derived + exact evidence |
| Derived analytics (`market_state`, imbalance, VWAP, `funding_rate_bps`, `signed_qty`) | float64 | approximate, allowed |

**One conversion boundary.** `collector/numeric.py` defines it:
`dec()` parses a venue value exactly (never via `float`; malformed input
raises, a missing value is `None`), `exact_text()` renders it without exponent
and preserves every digit including trailing zeros, `to_float()` is the
*explicit* exact→approximate crossing, and `column_value()` is what
`ParquetWriter.flush` uses to turn a record into columns.

**Exactness is never reconstructed from a float.** `<field>_exact` is filled
only from a `Decimal` (or list of `Decimal`s). If a caller hands the writer a
float, the companion is `null`, not text minted from the float — a float has
already lost the information the companion exists to keep.

**Integer identifiers are not decimals.** Trade ids, update ids and timestamps
stay `int`/`str` and never pass through `float` (a `2**53 + 1` id would be
corrupted by it). Regression-tested.

**Missing is not zero.** `p`/`q`/`r`/`openInterest` absent or malformed now
yields no feature row; previously a missing field was replaced by `0.0`.

## Which fields have exact companions

Every venue-decimal float64 column in the Binance USD-M, Binance Spot, Bybit
and OKX schemas: order-book arrays (`bids_price`, `bids_qty`, `asks_price`,
`asks_qty`, as `list<string>`), trade `price`/`quantity`, `mark_price`,
`index_price`, all OKX funding fields, `open_interest` (and OKX `oi_ccy`,
`oi_usd`), liquidation `price`/`quantity`/`bk_loss`. Schema versions were bumped
(minor) and each migration note says so. Open interest's **unit is untouched**:
Binance OI stays `OIUnit.UNKNOWN`; nothing here converts or guesses it.

## Historical data limitation

Rows written before P0-9 hold only the float64 value, which **may already be
lossy**. Their `_exact` columns are `null`. Nothing here repairs or
reinterprets them; the honest statement is "no exact evidence was recorded in
this table". Exact text is only recoverable for those rows from the raw wire /
REST capture (`payload`), which was never lossy — no such backfill is
implemented here. `compact_daily` treats `*_exact` (and `bid_depth`/`ask_depth`)
as legitimately absent in older hourly segments and fills nulls; any other
missing column is still a hard error (tested).

## Where float64 is still used, and why that is acceptable

Everywhere a value is *derived* (ratios, sums over windows, bps, signed
volume, mid/micro-price, the `MarketState` view, `book_metrics`). Those
contracts are approximate by nature. The float64 storage columns are also kept
for compatibility and fast analytics, but they are no longer the *only*
record: the exact text sits beside them. Note that the float64 column can
collapse distinct venue decimals (two prices closer than ~1e-16 relative);
`_exact` is what disambiguates them.

## Cost (measured, not estimated)

Micro-benchmark against `origin/main`, best of 5, identical inputs:
order-book feature computation (20 levels) 8.6 µs → 38.2 µs per call (4.5×);
trade features 0.85 µs → 1.79 µs (2.1×). Trade rows end-to-end through
`ParquetWriter` (30k rows, warmed, alternating order, 5 rounds, median):
133,778 → 78,432 rows/s (**0.59×**). That is a real regression in per-row
cost; it remains far above the live message rate (order of tens of book
updates/s, hundreds of trades/s at peak), but it is not free. Replay reuses
the same adapters, so it pays the same parse cost.

## Not in scope here

Timestamp semantics/resolution (P0-10/P0-11); persistent trade dedup (P0-4);
quality-event WAL/batching (P0-2/P0-3); OI unit semantics; backfilling exact
text for pre-P0-9 rows from raw capture.
