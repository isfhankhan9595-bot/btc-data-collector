# Trade-dedup evidence: official documentation audit + analyzer hostile audit

No live capture occurred in this session. This sandboxed environment's
network allowlist does not include any exchange API domain
(`api.binance.com`, `api.bybit.com`, `www.okx.com`, or their websocket
hosts) — live capture is not merely unauthorized here, it is
**unreachable**, independent of any authorization decision. Consistent
with the task's own "if live capture is not available" fallback: this
session finished official-documentation verification, hostile-audited the
existing analyzer, fixed two real gaps found in it, and left
`_seen_trade_ids` in `adapters/base.py` completely untouched.

## Hostile audit of PR #52's `dedup_evidence_analysis.py`

Read the full implementation, design doc, and test file before trusting
any of it. Checked every item on the task's own audit checklist
(lookahead, input-order assumptions, timestamp assumptions, duplicate
grouping, identity collisions, cross-stream contamination, reconnect
causality, percentile calculation, negative delay, duplicate-of-duplicate,
missing-ID behavior, numeric-ID coercion, UUID handling, same-timestamp
handling, instrument identity handling).

**Two real gaps found and fixed** (both in `dedup_evidence_analysis.py`,
same file PR #52 introduced — not a new module):

1. **`missing_id_count` did not exist anywhere in the report.** Records
   with `trade_id=None` were correctly excluded from duplicate detection
   (matching production's own exemption), but the count of how many such
   records existed was never surfaced — this task's own Definition of
   Done explicitly lists "missing IDs" as a required statistic. Added
   `DedupEvidenceReport.missing_id_count`, computed in the same loop that
   already walked every record, with a test
   (`test_missing_trade_id_is_counted_not_silently_dropped`) proving it is
   populated, not silently dropped.
2. **Tie-breaking on identical `local_receive_ts` is input-order-dependent
   for which record is labeled "first" vs. "duplicate".** Python's stable
   sort picks whichever record appeared first in the input list when two
   share an exact timestamp; there is no secondary key in the record
   schema to break the tie deterministically. Investigated whether this
   affects any reported number: it does not — `delay_ms` for such a pair
   is always exactly `0` regardless of which record is picked as "first",
   proven by `test_same_local_receive_ts_duplicate_has_zero_delay_regardless_of_input_order`
   (asserts identical results for both orderings of the same two records).
   Documented in the function's own docstring as a known, low-impact
   limitation rather than adding a synthetic secondary sort key that would
   only be meaningful to construct once a real capture schema defines a
   genuine one (see the design doc's own "not required if the venue
   genuinely does not provide it" principle).

Everything else audited clean: no lookahead (the tool groups records
retrospectively for forensic measurement only, exactly as Section 17
requires — it does not simulate a live dedup algorithm); no input-order
dependence anywhere else (`test_analyze_does_not_mutate_or_reorder_the_input_list`
already covered this and still passes); OKX `trades`/`trades-all` kept as
separate identity domains via the `stream` component (never merged);
Bybit's UUID exemption is by explicit `exchange == "BYBIT"` branch, not
silently defaulted; percentile calculation is nearest-rank, honestly
documented as "simple and sufficient for this tool's purpose", not a
correctness bug.

One residual note, not fixed (out of scope — would need a real capture
schema to matter): the identity tuple collapses `instrument_key=None` to
`""`, so two different genuinely-unidentified-instrument trades on the
same exchange/market_type/stream with a coincidentally-equal `trade_id`
would be misclassified as duplicates of each other. Currently
low-probability (this project tracks one instrument per venue today) —
recorded here so it is not forgotten if that changes.

## Official documentation audit

Live capture being unreachable makes this section the actual deliverable
of this session. Every claim below is labeled exactly as the task
requires: **OFFICIAL-DOC-VERIFIED**, **EMPIRICALLY OBSERVED** (third-party
production libraries' real example payloads — not this project's own
capture), or **INFERRED**.

### Binance Spot (`t`, trade stream)
- **OFFICIAL-DOC-VERIFIED** (binance/binance-spot-api-docs,
  `web-socket-streams.md`): the Trade Stream's `t` field is documented as
  "Trade ID"; confirmed field shape matches this project's adapter usage
  exactly.
- **OFFICIAL-DOC-VERIFIED**: a websocket connection is force-disconnected
  at the 24-hour mark (explicit in the same official docs) — a concrete
  upper bound on *connection lifetime*, not on duplicate-redelivery delay.
- **NOT VERIFIED**: no websocket monotonicity guarantee for `t` found
  anywhere in official docs. The docs' own order-book bootstrap procedure
  (buffer, snapshot, discard `u <= lastUpdateId`) is a *depth-stream*
  procedure using `U`/`u`, unrelated to the trade stream's `t` — confirmed
  by reading it directly, not assumed transferable (matching the task's
  own explicit warning against conflating REST/other-stream guarantees
  with this one).
- **NOT VERIFIED**: no documented maximum duplicate-redelivery horizon
  found, including across reconnect.

### Binance USD-M (`a`, aggTrade stream)
- **OFFICIAL-DOC-VERIFIED** (developers.binance.com, Aggregate Trade
  Streams, USD-M futures): `a` is "Aggregate trade ID"; field shape
  confirmed, matches this project's adapter usage.
- **NOT VERIFIED**: no websocket monotonicity guarantee for `a` found.
- **NOT VERIFIED**: no documented maximum duplicate-redelivery horizon.

### Bybit Linear (`i`, publicTrade)
- **OFFICIAL-DOC-VERIFIED** (bybit-exchange.github.io, Public Trade):
  `i` ("Trade id") is documented only as type `string` — the official
  reference does **not** assert a UUID format.
- **EMPIRICALLY OBSERVED** (not this project's own capture — third-party
  production trading libraries' documented real example payloads,
  `barter-data` crate): actual `i` values observed are UUID-shaped, e.g.
  `"20f43950-d8dd-5b31-9112-a178eb6023af"`. This is consistent with, but
  does not upgrade, the prior session's finding — **UUID-shape remains
  EMPIRICALLY OBSERVED, not OFFICIAL-DOC-VERIFIED.** The existing decision
  (numeric high-water-mark inapplicable to Bybit; ordering analysis marked
  `applicable=False`) is correct either way — a documented string type
  with observed non-numeric values is sufficient reason on its own, independent
  of whether "UUID" is the exact official format name.
- **NOT VERIFIED**: no documented duplicate-redelivery horizon.

### OKX Swap (`tradeId`, `trades` / `trades-all`)
- **OFFICIAL-DOC-VERIFIED** (multiple independent sources mirroring
  OKX's own schema: nautilustrader, barter-data, both citing okx.com
  docs-v5 directly): `tradeId` field shape confirmed for the `trades`
  channel, matches this project's adapter usage.
- **OFFICIAL-DOC-VERIFIED** (okx.com/docs-v5, fetched directly this
  session): the official documentation's own navigation lists "Trades
  channel" and "All trades channel" as two distinct, separately-documented
  entries under Market Data — not variants or aliases of one channel. This
  confirms OKX itself treats them as separate, supporting the existing
  decision to keep them as separate identity domains.
- **INFERRED, not OFFICIAL-DOC-VERIFIED** (source: Tardis.dev, a
  professional market-data vendor's own channel catalog, not the body text
  of OKX's own docs page, which renders as a client-side app this
  session's `web_fetch` returned only the navigation shell of, not
  section-specific content — confirmed directly this session, not assumed
  from a prior one): `trades-all`, available since 2023-10-19, is
  described as carrying "non-aggregated trade messages", implying
  `trades` itself may be aggregated while `trades-all` is not. If
  accurate, this would mean the two channels are not just
  administratively separate but semantically different (different
  granularity, not substitutable IDs) — which would *strengthen*, not
  weaken, this project's existing decision to keep them as separate
  identity domains.
- **NOT VERIFIED**: no documented duplicate-redelivery horizon for either
  channel.

## Memory benchmark

Not re-run. The task's own instruction (Step 20) is to assess
representativeness, not repeat the benchmark by default; PR #50's
~160-195 bytes/identity figure was not challenged by anything found this
session, and no new information changes the operational-risk picture.

## Decision

**Outcome C, unchanged: bounded deduplication is not currently provable
safe for any venue.** Nothing this session found changes PR #50's
conclusion — if anything, the `trades-all` INFERRED finding above
reinforces the existing OKX stream-separation decision rather than
challenging it. `_seen_trade_ids` remains the exact, unbounded reference
set. No production source was touched.

## What would resolve this

Real observational capture (Step 5 onward in the task's own experiment
design) is the only path to OFFICIAL-DOC-VERIFIED-grade evidence for the
two open questions (redelivery horizon, monotonicity) that official
documentation does not answer, since neither venue's docs address them.
That requires live exchange websocket access this sandboxed environment
categorically does not have — not a matter of authorization, a matter of
network reachability. This is the same conclusion as PR #52's own design
doc, unchanged.

## Verification

Full suite: **1234 passed** (1232 + 2 new). compileall clean. `git diff
--check` clean. `collector/collector/adapters/base.py` — the production
dedup implementation — untouched, confirmed via `git status`.

## Next task

**Real observational capture**, exactly as PR #52's design doc already
specifies, gated entirely on live exchange access becoming reachable from
wherever the next session runs (not on further offline preparation — that
preparation is now complete: schema, analyzer, and this documentation
audit). No further offline-only work is recommended as the next step;
repeating it would not produce new evidence.
