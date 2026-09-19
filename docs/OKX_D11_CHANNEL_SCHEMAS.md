# OKX D11 — verified public-channel schemas (research phase)

**Status: schemas verified against official documentation. Parsers, canonical
mapping, storage wiring, replay support, and tests are NOT part of this
document and are not yet implemented.** This closes the "field discovery"
half of D11 (docs/EXECUTION_STATUS.md, D11 blocker) for six channels; the
implementation half (adapter code + tests + adversarial review, per the
project's phase discipline) is the next PR, done separately so a schema
citation error and an implementation defect are never entangled in the same
diff.

**Sources.** OKX's official docs (`www.okx.com/docs-v5/en`) render as a
single very large client-side page; individual channel sections could not be
fetched in one pass. Every schema below is cross-checked against at least
one regional mirror of the *same* official documentation (`app.okx.com`,
`my.okx.com`, `aws.okx.com` — these serve the identical `docs-v5` content
under OKX's region-routing scheme, not third-party sites) and, where noted,
against a real captured production frame. No field below is taken from
inferred variable names or unverified third-party tutorials. Fetched
2026-09-19.

---

## A. `trades` vs `trades-all` — NOT the same channel

These are two distinct official channels. Conflating them was the risk
flagged in the task brief, and the two are still confused in some
third-party SDKs.

### A1. `trades` channel (aggregated)
- **Endpoint:** `wss://ws.okx.com:8443/ws/v5/public`
- **Subscribe:** `{"channel": "trades", "instId": "BTC-USDT-SWAP"}`
- **Fields (per docs.rs/barter-data, itself sourced from
  `#websocket-api-public-channel-trades-channel`, cross-checked against the
  `app.okx.com` mirror):** `instId`, `tradeId`, `px`, `sz`, `side`
  (`buy`/`sell`), `ts` (ms).
- **`seqId` addition:** OKX's official changelog
  (`www.okx.com/docs-v5/log_en/`, entry dated 2025-07-08) states: *"Trades
  channel adds seqId field"* — `seqId` is `Integer`, described as
  *"Sequence ID of the current message"*, with the explicit caveat that
  **the same `seqId` can appear on different trade updates that occur at the
  same time**. This means `seqId` is an ordering hint, not a unique
  per-trade identifier, and a repeated `seqId` must not be treated as a gap.
  This addition was to `trades`, not confirmed for `trades-all` in any
  source found — must be re-verified against a live captured `trades-all`
  frame before the parser assumes its presence either way.
- **Aggregation semantics:** third-party captures (tardis.dev's OKX capture
  documentation) describe `trades` as the standard public trade-execution
  stream and separately list `trades-all` as *"All trades stream including
  non-aggregated trade messages"* (available since 2023-10-19) — implying
  `trades` **can** aggregate multiple fills into one push. Neither official
  doc mirror found spells out the aggregation rule in prose; this must be
  confirmed against captured frames (count of trades sharing one `tradeId`
  vs several `tradeId`s in one `data` push) before treating `trades` as a
  1:1 trade tape.

### A2. `trades-all` channel — "All trades channel"
- **Endpoint:** same, `wss://ws.okx.com:8443/ws/v5/public`
- **Official section title confirmed via mirror:** "WS / All trades
  channel" (under Order Book Trading → Market Data).
- **Push data parameters (from the `www.okx.com/docs-v5/en/` mirror
  directly):**

  | Parameter | Type | Description |
  |---|---|---|
  | `instId` | String | Instrument ID, e.g. BTC-USDT |
  | `tradeId` | String | Trade ID |
  | `px` | String | Trade price |
  | `sz` | String | Trade quantity — base currency for SPOT, contracts for FUTURES/SWAP/OPTION |
  | `side` | String | `buy` / `sell` |
  | `source` | String | Order source: `0` normal, `1` Enhanced Liquidity Program (ELP) order |
  | `ts` | String | Fill time, ms epoch |

  No `count` or `seqId` field is documented for `trades-all` in any source
  found. `source` is unique to `trades-all` among the two channels (per
  sources found) and should be preserved, not discarded, since it flags ELP
  liquidity — relevant to microstructure research.

### A3. Recommendation for canonical treatment (not yet implemented)
Per docs/DATA_SUFFICIENCY-style reasoning and the task brief's instruction
not to discard venue-native information: capture **both** channels as
separate raw streams (`okx_trades`, `okx_trades_all` — no historical
collision risk since neither has ever been captured for OKX yet). Do not
merge them into one canonical stream implicitly. Whether `trades-all`
becomes *the* canonical individual-trade source for OKX depends on
confirming, from real captured frames, whether `trades` really is
aggregated relative to `trades-all` (open question above) — defer that
decision to the implementation PR, backed by frame evidence, not by this
document's inference.

---

## B. `mark-price` channel
- **Subscribe:** `{"channel": "mark-price", "instId": "BTC-USDT-SWAP"}`
- **Push fields (`app.okx.com` mirror, `#websocket-api-public-channels-mark-price-channel`):**

  | Parameter | Type | Description |
  |---|---|---|
  | `instType` | String | Instrument type |
  | `instId` | String | Instrument ID |
  | `markPx` | String | Mark price |
  | `ts` | String | Price update time, ms epoch |

  Mark-price pushes carry **no** index price, funding rate, or OI — those
  are separate channels (§C, §D). This matches `canonical.py`'s existing
  `CanonicalMarkPriceEvent`, which already keeps `mark_price`,
  `index_price`, `funding_rate` as independently-nullable fields exactly so
  one channel's push doesn't fabricate values for another's.

---

## C. `index-tickers` channel
- **Subscribe:** `{"channel": "index-tickers", "instId": "BTC-USDT"}`
  (note: index instId is the spot-style pair, e.g. `BTC-USDT`, not the swap
  instId — confirm against the actual OKX index-instrument naming before
  wiring the subscription for `BTC-USDT-SWAP`'s underlying index).
- **Push fields (`app.okx.com`/`my.okx.com` mirrors agree):**

  | Parameter | Type | Description |
  |---|---|---|
  | `instId` | String | Index ID |
  | `idxPx` | String | Index price |
  | `high24h` / `low24h` / `open24h` | String | 24h stats |
  | `sodUtc0` / `sodUtc8` | String | Start-of-day price, UTC0 / UTC+8 |
  | `ts` | String | ms epoch |

---

## D. `funding-rate` channel
- **Subscribe:** `{"channel": "funding-rate", "instId": "BTC-USDT-SWAP"}`
- **Push fields, confirmed via a real production JSON example embedded in
  the `app.okx.com` mirror:**

  ```json
  {
    "arg": {"channel": "funding-rate", "instId": "BTC-USD-SWAP"},
    "data": [{
      "formulaType": "noRate", "fundingRate": "0.0001875391284828",
      "fundingTime": "1700726400000", "impactValue": "",
      "instId": "BTC-USD-SWAP", "instType": "SWAP", "interestRate": "",
      "method": "current_period", "maxFundingRate": "0.00375",
      "minFundingRate": "-0.00375", "nextFundingRate": "",
      "nextFundingTime": "1700755200000",
      "premium": "0.0001233824646391",
      "settFundingRate": "0.0001699799259033", "settState": "settled",
      "ts": "1700724675402"
    }]
  }
  ```

- **Current/next-period semantics (task brief explicitly required this):**
  present. `fundingRate` + `fundingTime` describe the **current** period
  (the rate that will settle at `fundingTime`); `nextFundingRate` +
  `nextFundingTime` describe the **next** period — `nextFundingRate` is
  documented as often empty (predicted rate not always published ahead of
  time). `settFundingRate` / `settState` describe the **last settled**
  rate, a third, distinct value from either current or next. A canonical
  mapping that only kept `fundingRate`/`nextFundingTime` (matching
  `CanonicalMarkPriceEvent`'s existing two fields) would silently discard
  `settFundingRate`, `settState`, `premium`, `interestRate`, and the
  min/max funding rate band — all native fields with no existing canonical
  slot. Per the raw-lineage requirement, these must ride along as raw
  lineage even where canonical has no dedicated field yet, not be dropped
  at parse time.
- **Interval:** OKX does not push a `fundingIntervalHour` field in this
  channel (unlike Bybit's ticker). NautilusTrader's OKX integration notes
  computing the interval as `nextFundingTime - fundingTime` rather than
  trusting a static 8h assumption, since OKX explicitly documents that the
  funding collection frequency **can change** for volatile alts (confirmed
  in the same mirror: default 8h, but "may be adjusted to higher
  frequencies such as 6 hours, 4 hours, 2 hours, or 1 hour" when needed).
  BTC-USDT-SWAP is not expected to be affected today, but the parser must
  not hardcode 8h.

---

## E. `open-interest` channel
- **Subscribe:** `{"channel": "open-interest", "instId": "BTC-USDT-SWAP"}`
- **Push fields (confirmed identically across a Go client's typed struct
  and the `app.okx.com` mirror's subscribe example):**

  | Parameter | Type | Description |
  |---|---|---|
  | `instType` | String | Instrument type |
  | `instId` | String | Instrument ID |
  | `oi` | String | Open interest, in contracts |
  | `oiCcy` | String | Open interest, in base currency |
  | `oiUsd` | String | Open interest, in USD |
  | `ts` | String | ms epoch |

  Three units are pushed simultaneously (contracts / coin / USD) — all
  three should be preserved as raw lineage; `canonical.py`'s
  `CanonicalOIEvent.open_interest` is a single float, so the mapping must
  pick one canonical unit (contracts, matching Binance's existing OI
  convention in this codebase, is the natural choice — to be decided in the
  implementation PR, not assumed here) while keeping the other two
  un-discarded.

---

## F. `liquidation-orders` channel
- **Subscribe:** `{"channel": "liquidation-orders", "instType": "SWAP"}` —
  **note the subscription is scoped by `instType`, not `instId`** (also
  accepts `FUTURES`; confirmed in two independent mirrors). A single
  subscription can therefore deliver liquidations for instruments beyond
  BTC-USDT-SWAP; the adapter/runner must filter by `instId` after
  ingestion, the subscription itself cannot narrow it to one instrument.
- **Envelope + fields — confirmed against a real captured production
  message** (from a ccxt GitHub issue reporting actual wire output,
  2024-08-17, not a documentation example):

  ```json
  {
    "arg": {"channel": "liquidation-orders", "instType": "SWAP"},
    "data": [{
      "details": [{
        "bkLoss": "0", "bkPx": "1.057", "ccy": "", "posSide": "long",
        "side": "sell", "sz": "768", "ts": "1723892524781"
      }],
      "instFamily": "DYDX-USDT", "instId": "DYDX-USDT-SWAP",
      "instType": "SWAP", "uly": "DYDX-USDT"
    }]
  }
  ```

  Outer object: `instId`, `instType`, `instFamily`, `uly` — one outer
  object per instrument, per push. Inner `details[]`: `bkPx` (bankruptcy
  price — the liquidation execution price), `bkLoss` (bankruptcy loss),
  `sz` (contracts), `side`, `posSide` (`long`/`short`), `ts`, `ccy`
  (observed empty for a USDT-margined instrument in this capture — needs
  confirming whether it's ever populated for BTC-USDT-SWAP or is
  consistently empty for linear contracts).
- **Liquidation-rule compliance (task brief §"LIQUIDATION RULE"):** this is
  OKX's public market-data liquidation channel — exchange-published,
  post-hoc records of liquidations that occurred, not private position-risk
  warnings. OKX's *separate* `liquidation-warning` channel (private,
  requires login, delivers `mgnRatio`/`markPx` pre-liquidation risk state)
  is a different channel entirely and out of scope for this public
  collector; noted here only so the distinction is explicit and the two are
  never merged.

---

## Open questions for the implementation PR (not resolved by documentation alone)

1. **`trades` aggregation vs `trades-all`** — needs a real captured
   frame-pair comparison (§A1/A3), not inferable from docs prose.
2. **`seqId` presence on `trades-all`** — the 2025-07-08 changelog entry
   names `trades`, not `trades-all`; must be checked against a live frame,
   not assumed either way.
3. **`index-tickers` instId convention for the BTC perp's underlying
   index** — confirm exact instId string before subscribing.
4. **Canonical unit for open interest** (contracts vs coin vs USD) — a
   product decision, not a documentation fact.
5. **Whether `ccy` in liquidation `details[]` is ever non-empty for
   BTC-USDT-SWAP** — one data point (DYDX-USDT) is not enough to
   generalize.

None of these block writing the six channels' *raw capture + storage*
(which only needs the envelope shape, already confirmed above); they block
finalizing the *canonical* mapping and must be resolved with real frames
before that mapping is called complete, per the project's "field discovery
!= protocol semantic verification" rule.
