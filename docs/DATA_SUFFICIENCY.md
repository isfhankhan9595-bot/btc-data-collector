# Data sufficiency matrix

What the event taxonomy needs versus what this repository can actually
collect today. The rule this table enforces: **do not implement an event
whose required underlying data does not exist.**

Audited against source at `main` (see `EXECUTION_STATUS.md` for the commit).

## Ingestion reality

| Venue | Adapter | Live client | Data flowing |
|---|---|---|---|
| Binance USD-M | COMPLETE, doc-verified (D14 closed) | `run_collector.py` | **yes** — book, trades, markPrice, forceOrder, OI poll |
| Bybit v5 linear | PARTIAL (4/4 declared channels parsed) | **none** | **no** |
| OKX v5 swap | PARTIAL (1/7 channels parsed, D11) | raw capture only, never run live | **no** |
| Any BTC spot | **none** | **none** | **no** |

Binance is the only venue that produces data. That single fact decides most
of the table below.

## Event feasibility

| Event | Required data | Feasible today? | Missing dependency |
|---|---|---|---|
| Liquidation cascade | liquidation stream + price + OI | **YES** (Binance) | needs sustained live run for sample size |
| Liquidation exhaustion | as above, plus intensity over time | **YES** (Binance) | feature layer |
| Absorption | aggressive trade flow + book + price response | **YES** (Binance) | feature layer |
| Liquidity sweep | book depth history + penetration + reclaim | **YES** (Binance) | feature layer |
| Failed sweep / failed breakout | range definition + flow + OI | **YES** (Binance) | feature layer |
| CVD / CVD divergence | aggTrade flow + price | **YES** (Binance) | feature layer |
| OI / price divergence | OI + price | **PARTIAL** (Binance) | OI is a 3s REST poll, not event-time; resolution limits short-horizon use |
| Funding extreme | markPrice stream funding fields | **YES** (Binance) | feature layer |
| Order-book imbalance / microprice | reconstructed book | **YES** (Binance, authoritative) | feature layer |
| Liquidity depletion / replenishment / vacuum | depth history per level | **YES** (Binance) | feature layer |
| Abnormal price impact | trade size + depth + resulting move | **YES** (Binance) | feature layer |
| VWAP deviation | trade price/volume | **YES** (Binance) | feature layer |
| Volatility expansion / compression | price history | **YES** (Binance) | feature layer |
| Regime transition | volatility + liquidity + flow history | **YES** (Binance) | feature layer |
| **Spot / perp basis** | perp price + **real spot price** | **NO** | no spot feed exists anywhere in the repo |
| **Spot / perp divergence** | as above | **NO** | no spot feed |
| **Cross-exchange dislocation** | same instrument, 2+ live venues | **NO** | Bybit/OKX not live |
| **Cross-exchange lead / lag** | as above, sub-second aligned | **NO** | Bybit/OKX not live |

## Honest reading

Roughly two-thirds of the taxonomy is buildable **today on Binance alone**,
once the market-state and causal feature layers exist. Those layers are the
actual next engineering dependency, and they are **not started**.

The remaining third is not a coding problem. Basis, spot/perp divergence,
cross-exchange dislocation and lead/lag are blocked on **data that has never
been collected**. No amount of code produces them, and per master rules 13
and 14 they must not be synthesised from perp-only data.

## Timestamp quality caveats

| Stream | Event-time source | Caveat |
|---|---|---|
| Binance book | `E` / `T` + `U`/`u`/`pu` | authoritative; reconstruction doc-verified |
| Binance aggTrade | `E` / `T` | trade-id semantics differ across aggTrade vs trades vs REST — do not reseed across them |
| Binance markPrice | `E` | funding fields ride this stream |
| Binance OI | REST request/response ts | **not** an exchange event time; must never be presented as one |
| Binance liquidation | `E` + order `T` | `side` semantics are the venue's, not an inferred aggressor |

## Rule

Any event added to the detection layer must cite its row here. If the row
says NO, the detector is not written — the data is collected first.
