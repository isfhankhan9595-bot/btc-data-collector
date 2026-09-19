# Data sufficiency matrix

What the event taxonomy needs versus what this repository can actually
collect. The rule this table enforces: **do not implement an event whose
required underlying data does not exist.**

Re-audited against source at `main` @ `e2c3c8a` (591 tests passing
when run as CI runs them, `working-directory: collector`). This replaces an
earlier version that predated the Bybit collector (PR #16), storage
namespacing (PR #19), the OKX channel implementation (PR #22) and non-book
replay (PR #23).

Status terms (defined in `REPLAY.md`): IMPLEMENTED, TESTED, REPLAY-VERIFIED,
LIVE-VERIFIED. **Implementation is not live verification, and this document
never converts one into the other.**

## Ingestion reality

| Venue | Adapter | Collector (runner) | Streams collected | Tested | Replay-verified | LIVE-VERIFIED |
|---|---|---|---|---|---|---|
| Binance USD-M | complete; sequence semantics documented (D14 closed) | `run_collector.py` | book (diff + REST snapshot), aggTrade, markPrice (funding), forceOrder, OI **REST poll** | yes | book, trades, mark price, liquidation. **OI: no** (see `REPLAY.md`) | **not established by this repo** |
| Bybit v5 linear | 4/4 declared channels | `run_bybit_collector.py` | `orderbook.<depth>`, `publicTrade`, `tickers` (mark/index/funding/OI), `allLiquidation` | yes (fake socket, official-doc fixtures) | book, trades, ticker -> mark + OI, liquidation | **no** |
| OKX v5 swap | all 7 D11 channels | `run_okx_collector.py` (storage-wired); `run_okx_capture.py` (raw frames only) | `trades`, `trades-all`, `mark-price`, `index-tickers`, `funding-rate`, `open-interest`, `liquidation-orders`. **`books` is parsed by the adapter but deliberately not collected.** | yes (fake socket, official-doc fixtures) | the 7 D11 channels. Book: no committed test | **no** |
| Any BTC spot | **none** | **none** | none | n/a | n/a | n/a |

**Live evidence.** The repository contains no recorded live-session artifact for
any venue. The execution container has no route to exchange hosts (measured
from that container: Binance, Bybit and OKX REST and WebSocket hosts all return
`403 x-deny-reason: host_not_allowed` or no response). The previous version of
this table said Binance data was "flowing: yes"; nothing in the repository
records that, so it is not repeated here. A Binance collection history may
exist on the operator's host, but it is outside what this repository can show.

Every venue's live status is therefore **LIVE-UNVERIFIED — ENVIRONMENT
BLOCKED**. In particular, nobody has confirmed against a real venue that the
subscribe requests are accepted, that heartbeats satisfy the servers, that
timestamp and sequence fields are populated as documented, or that reconnect
behaves as tested against fake sockets.

## Event feasibility

"Data collection implemented" means the collector code for the required
streams exists and is tested. It is not live-verified, and the feature layer
that computes the event does not exist for any row.

| Event | Required data | Data collection implemented? | Missing dependency |
|---|---|---|---|
| Liquidation cascade | liquidation stream + price + OI | **YES** (Binance, Bybit, OKX) | live verification; feature layer; sustained run for sample size |
| Liquidation exhaustion | as above, plus intensity over time | **YES** | feature layer |
| Absorption | aggressive trade flow + book + price response | **YES** (Binance, Bybit books; OKX book not collected) | feature layer |
| Liquidity sweep | book depth history + penetration + reclaim | **YES** (Binance, Bybit) | feature layer |
| Failed sweep / failed breakout | range definition + flow + OI | **YES** | feature layer |
| CVD / CVD divergence | trade flow + price | **YES** (all three) | feature layer |
| OI / price divergence | OI + price | **PARTIAL** | Binance OI is a REST poll, not event-time, and is not replayable; **OI units are not comparable across venues (below)** |
| Funding extreme | funding fields | **YES** (Binance markPrice, Bybit ticker, OKX `funding-rate`) | feature layer; per-venue funding-interval differences not analysed |
| Order-book imbalance / microprice | reconstructed book | **YES** (Binance, Bybit) | feature layer |
| Liquidity depletion / replenishment / vacuum | depth history per level | **YES** (Binance, Bybit) | feature layer |
| Abnormal price impact | trade size + depth + resulting move | **YES** (Binance, Bybit) | feature layer |
| VWAP deviation | trade price/volume | **YES** | feature layer |
| Volatility expansion / compression | price history | **YES** | feature layer |
| Regime transition | volatility + liquidity + flow history | **YES** | feature layer |
| **Spot / perp basis** | perp price + **real spot price** | **NO** | no spot feed exists anywhere in the repo |
| **Spot / perp divergence** | as above | **NO** | no spot feed |
| **Cross-exchange dislocation** | same instrument, 2+ venues, causally aligned | **PARTIAL** | live verification of Bybit/OKX; a causal alignment layer (`pipeline/cross_exchange_alignment.py` is a first-pass module that has not been audited or tested against these rules); explicit BTC instrument selection for OKX |
| **Cross-exchange lead / lag** | as above, sub-second aligned | **PARTIAL** | as above, plus evidence of real receive-latency and clock behaviour, which only a live run can give |

## Cross-venue hazards that block specific rows

**Open-interest units are not comparable, and nothing enforces it.**
`CanonicalOIEvent.open_interest` is one untyped float. OKX stores its `oi`
(contracts) there. `canonical.py`'s own comment describes Bybit's
`openInterest` as base-currency, while also declaring the canonical unit to be
"contracts": by the code's own account, one column holds different physical
quantities. Binance's OI unit is not documented anywhere in this repository
(`BINANCE_USDM_SEMANTICS.md` has no OI section). There is **no unit tag and no
comparability guard** in the code; comparing `open_interest` across venues
would silently compare different quantities. The OKX schema document lists the
canonical OI unit as an open question. This must be resolved (a unit tag on the
event, and an explicit refusal to compare across unknown units) before any
cross-venue OI row is built.

**OKX `liquidation-orders` is subscription-scoped by instrument type.** One
stream carries many instruments. Replay preserves every row and does not
filter by instrument (tested). Any BTC dataset must select the BTC instrument
explicitly downstream; no consumer may assume a row is BTC-USDT-SWAP.

## Honest reading

The collectors for all three venues exist and are tested against fake sockets
and documentation-derived fixtures. That is a necessary, not a sufficient,
condition: **no venue is live-verified by anything this repository can show.**

Most of the taxonomy is computable from Binance and Bybit data once the
market-state and causal feature layers exist. Those layers are **not started**.
Basis and spot/perp divergence are blocked on data that has never been
collected and must not be synthesised from perp-only data. Cross-exchange rows
are blocked on live verification, a causal alignment layer and the OI unit
contract.

## Timestamp quality caveats

| Stream | Event-time source | Caveat |
|---|---|---|
| Binance book | `E` / `T` + `U`/`u`/`pu` | authoritative; reconstruction doc-verified |
| Binance aggTrade | `E` / `T` | trade-id semantics differ across aggTrade vs trades vs REST — do not reseed across them |
| Binance markPrice | `E` | funding fields ride this stream |
| Binance OI | REST request/response ts | **not** an exchange event time; must never be presented as one |
| Binance liquidation | `E` + order `T` | `side` semantics are the venue's, not an inferred aggressor |
| Bybit / OKX | venue `ts` fields per `OKX_D11_CHANNEL_SCHEMAS.md` and the Bybit adapter | populated as documented is **unverified against a live session** |

## Rule

Any event added to the detection layer must cite its row here. If the row says
NO, the detector is not written: the data is collected first. If the row says
PARTIAL, the missing dependency is resolved first.
