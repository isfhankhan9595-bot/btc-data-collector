# Instrument identity

`collector/collector/instrument.py`. Identity, metadata and observation are kept
apart: **identity** says which instrument; **metadata** (tick size, contract
value, margin) is deliberately absent because it changes over time and does not
belong in a key; an **observation** (a trade, a book update) carries an identity
and is not part of it.

## Why it exists

`(exchange, market_type, stream)` could not tell two instruments of one stream
apart, and `BINANCE` alone covers both Binance Spot and USD-M: before this
change one `MarketStateEngine("BINANCE")` would have accepted spot and
perpetual events into a single state.

## The identity

`InstrumentId(exchange, market_type, instrument, native_symbol)`: frozen,
hashable, ordered, all four fields part of equality.

| Field | Meaning | Example |
|---|---|---|
| `exchange` | canonical venue (never a storage namespace) | `BINANCE` |
| `market_type` | the existing vocabulary: `spot`, `linear_perpetual` | `spot` |
| `instrument` | canonical `BASE-QUOTE`, shared across venues | `BTC-USDT` |
| `native_symbol` | the venue's own symbol, verbatim, case-sensitive | `BTCUSDT`, `BTC-USDT-SWAP` |

Supported today: Binance Spot BTCUSDT, Binance USD-M BTCUSDT, Bybit linear
BTCUSDT, OKX BTC-USDT-SWAP. Construction is strict: nothing is case-folded or
repaired (`binance`, `Spot`, `btc-usdt`, `BINANCE_SPOT` all raise), because a
silent fix is how two instruments become one. `key` (`EXCHANGE|market|BASE-QUOTE|native`)
and `to_dict()` round-trip losslessly and deterministically.

## Unknown is `None`, never a default

`CanonicalEvent.instrument` is `Optional[InstrumentId]`. `None` means
**unidentified**: legacy data, or an event not scoped to one registered
instrument. There is no UNKNOWN identity, and no reader may treat `None` as a
default instrument.

## Where identity is set

Every adapter is bound to its instrument (`ExchangeAdapter.instrument`) and a
hook in the adapter base class stamps it on every event its `normalize()`
returns, so no adapter can forget. It refuses an event whose `exchange` or
`market_type` contradicts the adapter. Deliberate exceptions, left `None`:

- **OKX `index-tickers`**: keyed by the index pair, not the swap (OKX open
  question #3).
- **OKX `liquidation-orders`** rows whose own `instId` is not the adapter's
  instrument: the stream is instType-scoped (one stream, many instruments);
  BTC is never assumed. The row's raw `inst_id` is preserved.
- An adapter configured for an unregistered instrument (e.g. OKX `ETH-USDT-SWAP`).

## Storage and replay

- **Raw layer (explicit):** `raw_wire` / `raw_rest` rows already carry `venue`,
  `market_type` and `symbol` (native, verbatim). `resolve_raw_record()` rebuilds
  the identity from them, and returns `None` for legacy rows missing a column,
  unregistered combinations, and contradictory ones (venue `BINANCE_SPOT` with a
  perpetual `market_type` does **not** resolve to the perpetual).
- **Replay:** replay re-runs the same adapters, so identity is stamped exactly as
  live. Tests record all four instruments through the real writers, read the
  rows back, replay them, and assert live == replayed identity. The replay
  digest covers `instrument`.
- **Legacy rows** are never reinterpreted from the row: replay identifies them by
  the adapter bound to the venue namespace (single-instrument by configuration).

## Alignment and MarketState

- `causally_align` key: `(exchange, market_type, instrument_key, stream)`.
  Unidentified events use `UNIDENTIFIED` and never collide with identified ones.
  Causal semantics are unchanged (receive-time availability, stale kept, nothing
  fabricated).
- `MarketStateEngine(exchange, instrument=...)` refuses events for any other
  instrument (including spot vs perpetual on one exchange, and unidentified
  events); `MarketState.instrument` and its `digest()` carry the identity. An
  unbound engine behaves as before.

## Intentionally not part of identity

Tick/lot size, contract value, margin currency, listing status, index
membership, display symbol, and the storage namespace (`BINANCE_SPOT`).

## Limitations

- **Canonical derived streams** (`spot_trades`, `okx_trades`, ...) carry no
  explicit instrument column; their identity is implied by the venue namespace
  plus schema metadata. Add an explicit `instrument_key` column before any
  aggregation or cross-venue dataset is built on them.
- Namespaces are single-instrument. A second instrument on one venue needs its
  own registry entry, adapter binding and namespace; replay does not yet
  cross-check each row's `symbol` against the adapter's instrument.
- Bybit spot, Binance COIN-M, and inverse contracts are not registered.
- No live verification of any identity path; tests use documented frame shapes.
