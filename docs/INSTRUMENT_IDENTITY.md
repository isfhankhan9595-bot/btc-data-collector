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

## Storage: canonical Parquet schemas

`instrument_key` (`InstrumentId.key`, nullable string) added to Bybit's five
canonical schemas (`bybit_orderbook`, `bybit_trades`, `bybit_markprice`,
`bybit_openinterest`, `bybit_liquidation` — versions bumped, migration notes
in each schema's metadata), OKX's six non-book schemas (`okx_trades`,
`okx_trades_all`, `okx_markprice`, `okx_indextickers`, `okx_fundingrate`,
`okx_openinterest`, `okx_liquidation`), Binance USD-M's seven schemas
(`orderbook`, `trades`, `binance_orderbook_raw`, `binance_trades_raw`,
`markprice`, `openinterest`, `liquidation`), and Binance Spot's two
(`spot_orderbook_raw`, `spot_trades`). All 24 pyarrow schemas in the codebase
have been classified; the three deliberately excluded (`quality_events`,
`raw_wire`, `raw_rest`) are covered under "Storage and replay" above.

Bybit stamps a single validated constant (`BYBIT_LINEAR_BTCUSDT.key`) in the
runner, not a per-event resolution: `run_bybit_collector.py`'s five topics
are all built from one hardcoded `SYMBOL` constant, so the process cannot
receive any other instrument — there is no per-event field to disagree with.
`BybitAdapter` **does** set `.instrument` (`instrument = BYBIT_LINEAR_BTCUSDT`
class attribute, same as OKX and Binance below, picked up by the generic
`ExchangeAdapter.__init_subclass__` hook) — verified at runtime:
`BybitAdapter().normalize(...)` returns events carrying
`instrument=BYBIT_LINEAR_BTCUSDT` for all five topics. The runner's constant
is therefore not a substitute for the event's own identity: `_persist_event`
cross-checks `event.instrument` against the constant and raises
`InstrumentIdError` on a contradiction, rather than trusting the constant
blind.

OKX stamps `event.instrument.key` where `event.instrument` is already set by
the generic base-class mechanism described above — the runner does no
resolution of its own, just reads what the adapter already decided. This
means `okx_indextickers` rows and non-BTC `okx_liquidation` rows correctly
persist `instrument_key = NULL`, not because the runner special-cased them,
but because `_instrument_scoped` already said so at the adapter layer.

Binance USD-M's 7 instrument-scoped canonical schemas (`orderbook`, `trades`,
`binance_orderbook_raw`, `binance_trades_raw`, `markprice`, `openinterest`,
`liquidation`) and Binance Spot's 2 (`spot_orderbook_raw`, `spot_trades`) are
wired. `BinanceAdapter` and `BinanceSpotAdapter` both declare `instrument`
class attributes exactly like Bybit and OKX; orderbook/trades and their raw
siblings on both venues read `event.instrument.key` (or the value carried
through `LocalBook.apply()`'s `dataclasses.replace`, which preserves fields
it does not touch), the same adapter-stamped path as OKX. Binance USD-M's
`markprice` and `liquidation` are the one exception across all four venues:
legacy raw-dict handlers that bypass `BinanceAdapter.normalize()` entirely,
so they stamp the validated `BINANCE_USDM_BTCUSDT` constant directly — but
only after checking the payload's own symbol field against the configured
symbol; a mismatch is rejected (quality event, row dropped), never silently
stamped. `openinterest` resolves identity generically via
`resolve_instrument()` from the REST response's own `symbol` field, checked
against what was requested. Binance Spot has no such legacy-handler
exception: both its schemas are adapter-routed like OKX and Bybit's OKX-style
path.

**Reading these rows back:** `resolve_canonical_instrument_key(row, expected=...)`
distinguishes four states a naive reader would conflate: column absent
(legacy row) and explicit null both resolve to `None`; a value that parses
but disagrees with the row's own stream identity raises
`InstrumentIdError` rather than being silently accepted (a Binance-Spot key
sitting in a Bybit row is corruption, not an unusual-but-valid identity).

## Replay identity: what is the source of truth?

**Raw venue frame + the venue's adapter.** Replay reads only `raw_wire` / `raw_rest`
and recomputes identity by running the same adapter (and the same shared
constructors) that live runs. The `instrument_key` persisted on canonical Parquet
streams is a *derived assertion*: replay never reads it, so a corrupted persisted
key cannot become truth in replay. Canonical readers (compaction, Phase D) validate
a persisted key against the stream's expected identity and reject malformed or
contradictory values rather than trusting them or collapsing them to unidentified.

Two live/replay divergences were closed by making live and replay share one
constructor: REST depth snapshots are built only by `BinanceAdapter.snapshot_event`
/ `BinanceSpotAdapter.snapshot_event` (stamped), and replay's Binance OI call passes
the adapter's native symbol exactly as live does. `BookUpdate` carries no identity by
design: a replay run is bound to one venue/instrument through its adapter, and the
identity is asserted on the events that feed the book engine (tests capture them).

## Intentionally not part of identity

Tick/lot size, contract value, margin currency, listing status, index
membership, display symbol, and the storage namespace (`BINANCE_SPOT`).

## Limitations

- Namespaces are single-instrument. A second instrument on one venue needs its
  own registry entry, adapter binding and namespace; replay does not yet
  cross-check each row's `symbol` against the adapter's instrument.
- Bybit spot, Binance COIN-M, and inverse contracts are not registered.
- No live verification of any identity path; tests use documented frame shapes.
