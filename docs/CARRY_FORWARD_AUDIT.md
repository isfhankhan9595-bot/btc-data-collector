# BTC DATA COLLECTOR — MASTER CARRY-FORWARD AUDIT REPORT

Audited 2026-09-20 against the actual GitHub repository and CI, not against
any prior handoff document or chat summary. Where a claim below was
independently re-run this session (git ancestry check, CI check-run status,
`pytest` execution), it is marked **VERIFIED THIS SESSION**. Where it is
carried from `docs/EXECUTION_STATUS.md`'s own ledger (written by earlier
sessions with their own stated evidence) but not independently re-run today,
it is marked **PER LEDGER, NOT RE-VERIFIED**. Treat the second category as
lower-confidence than the first.

## 1. Executive status

The collector ingests live Binance USD-M (book, trades, markPrice, OI poll,
liquidations) and has live-capable Bybit and OKX collector scripts, though
neither has been run against a live exchange from this environment
(`ws.okx.com` is unreachable here; Bybit's live-verified status is per
ledger, not re-tested this session). Deterministic replay now covers all
three venues' adapters end-to-end, including non-order-book events, as of
today. Everything above raw ingestion — market state, microstructure
features, atomic events, opportunity evidence — is **NOT STARTED**. No
spot feed exists anywhere in the repo, which blocks basis/dislocation
research regardless of how much else is built.

## 2. Verified git state — VERIFIED THIS SESSION

```
main HEAD: e2c3c8ab1e5f2b5bd14045124b287af2ea853212
           "Merge pull request #23 from isfhankhan9595-bot/okx-and-non-book-replay"
```

Confirmed via `git fetch` + `git reset --hard origin/main` (not assumed from
a prior session's report) and `git merge-base --is-ancestor` for the PR #23
commit against `origin/main` — passed.

Working tree: clean. No stash. No uncommitted changes.

Stale branches present on origin (none contain unmerged work beyond what's
already on `main`, based on the commit graph — not individually diffed this
session): `claude-test-pr` (a throwaway test-PR branch from this session,
harmless), `docs/okx-d11-channel-schemas`, `docs/storage-namespace-phase-
complete`, `docs-replay-gap-note`, `hotfix-raw-capture-regression`,
`okx-d11-implementation`, `okx-and-non-book-replay`, `phase-00` through
`phase-08` variants, `review-phase9-leakage-splits`, `storage-namespace-
venue-isolation`. All correspond to already-merged PRs (#4 through #23) per
the PR list below; none are "unfinished WIP branches" in the sense earlier
handoff documents worried about.

**One stray open item:** PR #3, "Hi from Claude 👋 (test PR)" — a leftover
demonstration PR from early in this session, unrelated to real work, still
open. Should be closed as unmergeable noise; not blocking anything.

## 3. Verified test state — VERIFIED THIS SESSION

```
591 passed, 0 failed, 0 skipped
```

Run via `python3 -m pytest -q` against a clean `origin/main` checkout,
today, not carried from a prior report. Per-file breakdown collected
separately (46 test files); largest concentrations: `test_bounded_recovery.py`
(51), `test_okx_capture.py` (40), `test_daily_compaction.py` (42),
`test_leakage_safe_splits.py` (34), `test_okx_d11_channels.py` (31).

## 4. Merged PR history — VERIFIED THIS SESSION

All 23 numbered PRs are merged, in strict numeric/chronological order, per
`GET /repos/.../pulls?state=all`:

| PR | Title |
|---|---|
| 4–10 | Phases 0–3: baseline integrity, truthful coverage, raw capture, deterministic replay |
| 8 | Phase 4: bounded recovery, jittered backoff, rate limiting |
| 9, 11 | Phase 5: Bybit ticker staleness provenance (D12) |
| 12 | Phase 6: Binance USD-M semantics verified (D14) |
| 13 | Phase 7: OKX public raw capture and schema observation |
| 14 | P0 hotfix: raw-wire capture regression |
| 15 | Phase 9: leakage-safe label horizons and chronological splits |
| 16 | Phase 8: live Bybit v5 linear collector |
| 17 | Docs: ReplayEngine venue-parameter gap recorded |
| 18 | Fix: ReplayEngine venue-hardcoding + replay/live quality-transition parity |
| 19 | Storage: venue namespace isolation + single-writer lock |
| 20–21 | Docs: storage-namespace complete; OKX D11 channel schemas verified |
| 22 | OKX D11: all seven channels implemented |
| 23 | ReplayEngine: OKX registered; non-order-book events replayed |

No gaps, no orphaned merge commits, no force-pushes evident in the log.

## 5. Current architecture

```
RAW WIRE (websocket/REST) -> ParquetWriter (venue-namespaced stream)
    -> adapter.normalize() -> Canonical{OrderBook,Trade,MarkPrice,OI,Liquidation}Event
    -> [order book] LocalBook -> quality state -> derived writers
    -> [everything else] durable storage; now also ReplayResult.non_book_events in replay
```

Three adapters (`BinanceAdapter`, `BybitAdapter`, `OKXAdapter`), one
`ReplayEngine` dispatching on venue, one `LocalBook`/quality-state machine
shared by every venue's order-book path, one `ParquetWriter` with per-stream
flock-based single-writer enforcement (PR #19).

## 6. Completed phases

- Phase 0 — repository baseline integrity
- Phase 1 — truthful coverage measurement
- Phase 2 — raw wire capture, no silent discard
- Phase 3 — deterministic order-book replay, live/replay parity (Binance)
- Phase 4 — bounded recovery, backoff, rate limiting
- Phase 5 — Bybit ticker staleness provenance (D12)
- Phase 6 — Binance USD-M semantics verified (D14)
- Phase 7 — OKX public raw capture
- Phase 8 — live Bybit v5 collector
- Phase 8 addendum — Bybit venue-aware replay + quality-transition parity (this session, PR #18)
- Phase 9 — leakage-safe label horizons and chronological splits
- Storage-namespace phase — venue isolation, single-writer locks (PR #19)
- OKX D11 — all seven channels implemented (PR #22)
- Non-order-book replay, OKX registered in ReplayEngine (this session, PR #23)

## 7. Partial phases

- **Bybit live ingestion**: collector exists and is exercised by tests
  (`run_bybit_collector.py`, `test_bybit_collector.py`); live-exchange
  verification status is **PER LEDGER, NOT RE-VERIFIED** this session — no
  live connection was attempted today.
- **OKX**: D11 parsing/storage/replay complete; **order-book** replay for
  OKX is not — there is no live OKX book collector (`books` channel is
  raw-capture-only, per Phase 7), so nothing exists yet to replay through
  `LocalBook` for OKX specifically. Live verification: **NOT ATTEMPTED**
  this session (`ws.okx.com` unreachable from this environment, confirmed
  by the Phase 7 ledger entry and not re-tested since nothing changed about
  network access).
- **Leakage-safe research (Phase 9)**: per ledger, "PARTIAL, VERIFIED" —
  not independently re-audited this session.

## 8. Missing phases

Cross-exchange timestamp alignment, BTC spot ingestion, market-state
engine, microstructure feature layer, atomic event detection, event
clustering, opportunity evidence, historical conditional outcomes,
walk-forward/purge/embargo validation beyond Phase 9, fault injection,
performance testing, observability, deployment hardening, security review.
**None of these have any code in the repository.**

## 9. Broken / risky areas

Nothing found broken this session (591/591 passing, no known failing test).
Risk areas, not defects:

- Two independently-constructed `ParquetWriter`s sharing a stream name
  would still race on segment sequence before either publishes (documented,
  guarded by `FileExistsError`, not eliminated — this is why PR #19 gave
  every venue its own stream name rather than relying on the guard alone).
- OKX/Bybit live-exchange behavior is untested against the real exchange
  from this environment; fixture-based tests cannot catch a real protocol
  surprise (unannounced field, rate-limit behavior, reconnect edge cases).

## 10. Documentation drift — VERIFIED THIS SESSION (found by direct reading, not assumed)

Two real, material drift findings:

**`docs/REPLAY.md` is stale**, predating this session's work (PRs #18, #23):
- Says "Binance only. Bybit and OKX have no live client feeding raw
  capture" — false; Bybit has had a live collector since PR #16, OKX has
  full D11 storage wiring since PR #22.
- Says "Replay reconstructs the order book. Trades, mark price, OI and
  liquidations are captured in raw_wire but are not yet driven through
  their feature paths during replay" — false as of PR #23;
  `ReplayResult.non_book_events` now carries exactly these.
- **Not fixed in this audit pass** (audit-only per the task's own
  instruction not to silently fix docs before reporting); flagged for the
  next session.

**`docs/DATA_SUFFICIENCY.md` is stale**, predating PRs #16, #19, #22, #23:
- Table says Bybit "Live client: **none**", "Data flowing: **no**" — false;
  `run_bybit_collector.py` exists and is tested.
- Says OKX "PARTIAL (1/7 channels parsed, D11)", "raw capture only, never
  run live" — false; all 7 D11 channels are implemented and storage-wired
  (PR #22).
- The event-feasibility table's underlying "Binance is the only venue that
  produces data" premise is now false at the *implementation* level (Bybit/
  OKX collectors exist), though it may still be true at the *live-verified*
  level, which this audit did not test. **Not fixed in this pass** — the
  next session should re-derive this table against actually-running
  collectors, not just adapter/storage existence.

`docs/EXECUTION_STATUS.md` (944 lines) is, by contrast, internally
consistent with the verified git/test state above — its own running ledger
matches what `git log` and `pytest` show today. It is the most trustworthy
document in the repo and should remain the primary source future sessions
read first.

## 11. Replay status

**COMPLETE for what currently has live/raw data to replay**, per this
session's own PR #18 and #23 work (hands-on, not carried from elsewhere):

| Venue | Order book | Non-book events |
|---|---|---|
| Binance | COMPLETE (Phase 3) | COMPLETE (PR #23): trades, mark price, liquidations |
| Bybit | COMPLETE (PR #18 fixed venue-hardcoding + quality-transition parity) | COMPLETE (PR #23): trades, ticker→mark/OI, liquidations |
| OKX | **NOT APPLICABLE** — no live OKX book collector exists to produce book frames to replay | COMPLETE (PR #23): trades, trades-all, mark-price, index-tickers, funding-rate, open-interest, liquidation-orders |

Verified this session, not asserted: no replay-only parser exists (every
adapter method replay calls is the same one live calls — confirmed by
reading `replay.py`'s imports and dispatch, not by trusting a comment); a
malformed/missing-required-field frame produces `frames_unhandled`, never a
fabricated event (tested directly for OKX funding-rate); digest changes on
a changed/removed trade, changed OI, changed funding rate, changed
liquidation quantity (four separate tests, not one representative case);
Bybit's carried-forward ticker provenance survives replay unaltered
(tested directly, not inferred from "same adapter instance" architecture
alone); frame ordering is `(timestamp, kind_rank, source_index)`,
list-input-order-independent (tested).

**Not claimed:** OKX's D11 semantic open questions (trades vs trades-all
aggregation, OI's canonical unit claim, index-tickers instId convention,
liquidation `ccy` semantics) are unchanged by any replay work and remain
exactly as open as PR #22's own documentation states — this audit did not
re-derive them from OKX's official docs today.

## 12. Storage status — PER LEDGER, NOT RE-VERIFIED this session for the lock/race mechanics specifically

PR #19 (per its own merge-time ledger entry, cross-checked by me reading
`run_okx_capture.py`, `config.py`, and `parquet_writer.py` directly during
this session's OKX work, though not by re-running its own dedicated
adversarial test suite today beyond the full-suite pass): venue-prefixed
stream names (`bybit_raw_wire`, `okx_raw_wire`, etc.), `exchange=` writer
attribution, a `flock`-based single-live-writer-per-stream lock, and
venue-filtered replay readers (`ReplaySource.from_directory` keeps a row
only if its own `venue` column matches the requested venue; legacy
unprefixed `raw_wire` rows are read but filtered per-row, not excluded
wholesale). `tests/test_storage_namespace_collision.py` (18 tests) covers
this and passed in today's full-suite run. The specific adversarial
questions in the original PR #19 ask (rollover lock release/reacquire
races, orphan/crash recovery, compaction cross-venue mixing) were answered
by that PR's own ledger entry, not independently re-derived by this audit.

## 13. Binance status

Live, doc-verified (D14, Phase 6), full order-book + non-book replay
(this session). Highest-confidence venue in the repository by a wide
margin — the only one with sustained real-world exercise implied by its
maturity in the ledger.

## 14. Bybit status

Live collector exists and is tested (Phase 8, PR #16); ticker
carried-forward provenance verified in both live-code-reading and replay
(this session). Order-book replay fixed this session (PR #18). Live
exchange connectivity **not tested today** — treat as
IMPLEMENTED-BUT-LIVE-UNVERIFIED-THIS-SESSION, not "production verified."

## 15. OKX status

All 7 D11 non-book channels implemented, storage-wired, and now replay-
capable (PR #22, #23, all this session's own hands-on work for the replay
half). No live order-book collector. `ws.okx.com` unreachable from this
environment — live verification remains blocked, not attempted, not
claimed.

## 16. Cross-exchange status

**NOT STARTED.** No alignment layer, no shared clock reconciliation beyond
each venue's own `local_receive_ts`. Blocked on at least two of the three
venues having live, sustained, overlapping data — which itself is blocked
on live verification above.

## 17. Spot status

**Does not exist.** No spot adapter, no spot collector, no spot schema.
This is the single largest blocker for basis/dislocation research and is
independent of every other item on this list — no amount of perp-side work
unblocks it.

## 18. Canonical schema status

Five frozen dataclasses (`CanonicalEvent` base, `OrderBook`, `Trade`, `OI`,
`MarkPrice`, `Liquidation`), venue-neutral fields with venue-specific
extensions kept as distinctly-named optional fields rather than conflated
(e.g., OKX's three funding observations — current/next/settled — are three
separate fields, never merged; OI keeps `open_interest`/`oi_ccy`/`oi_usd`
separately, never coerced to one unit). This convention held up under this
session's own test-writing — no field had to be repurposed or guessed to
build the non-book replay tests.

## 19. Timestamp / causality status

`exchange_event_ts`, `exchange_transaction_ts`, `local_receive_ts`,
`local_process_ts` are all present on `CanonicalEvent` and populated
per-venue as each adapter's own comments describe (e.g., Binance OI's REST
poll timestamp is documented as explicitly *not* an exchange event time —
`DATA_SUFFICIENCY.md`'s timestamp-quality table, itself not otherwise
stale). Replay orders exclusively by recorded local-receive availability
(`ReplayFrame.order_key`), never by exchange timestamp — verified this
session by reading the ordering code during the digest/determinism test
work, not merely assumed.

## 20. Data quality status

Quality-state machine (`VALID`/`SEQUENCE_GAP`/`RECOVERING`) is shared
across venues via `LocalBook`; this session's PR #18 fixed a real
live/replay divergence in exactly this mechanism (Bybit's resync signal
driving `RECOVERING` without passing through `SEQUENCE_GAP`, which replay
silently dropped before the fix). No other quality-state defect surfaced
this session.

## 21. Data sufficiency matrix

See `docs/DATA_SUFFICIENCY.md` for the full per-event table — **flagged
stale above (section 10)** on the ingestion-reality row for Bybit/OKX. The
event-feasibility conclusions (roughly two-thirds of the taxonomy buildable
on Binance alone once market-state/feature layers exist; basis/cross-
exchange blocked on data that doesn't exist yet) are still directionally
correct and not contradicted by anything found this session, but the table
itself needs regeneration against current adapter/collector state, not a
patch.

## 22. Research features available now

Nothing yet — no market-state or feature layer exists to build any
detector on top of, regardless of which venues have raw data. Raw data
alone does not constitute a "research feature available."

## 23. Research features blocked

Everything in `docs/DATA_SUFFICIENCY.md`'s event table is blocked on the
market-state/feature layer (section 8, "Missing phases"). Basis, spot/perp
divergence, cross-exchange dislocation and lead/lag are additionally
blocked on data that doesn't exist (no spot feed; no sustained live multi-
venue run).

## 24. Lookahead / leakage risks

None newly found this session. The replay work added this session was
specifically designed and tested against lookahead (frame ordering by
local-receive availability only, non-book events appended in the same
single ordered pass as book events, no cross-frame future-peeking in any
new code path). Phase 9's leakage-safe splits work is per-ledger PARTIAL,
not re-audited today.

## 25. Test coverage

591 tests across 46 files, 0 failures, verified today. No coverage
percentage tool was run this session (only pass/fail count) — a coverage
gap could exist in code paths no test currently exercises; this audit
did not measure that.

## 26. Performance risks

Not assessed this session. No load testing, no throughput measurement, no
profiling was performed. `ParquetWriter`'s per-instance in-memory sequence
counter (documented risk, section 9) is the only performance-adjacent
concern surfaced, and it's a correctness risk more than a performance one.

## 27. Deployment risks

Not assessed. No deployment configuration, containerization, or process
supervision exists in the repository as far as this audit found (no
Dockerfile, no systemd unit, no orchestration config encountered while
reading the tree this session — not exhaustively searched for this
report).

## 28. Security risks

Not assessed beyond the credential-handling concern already surfaced
earlier in this conversation (a GitHub PAT was pasted in plaintext across
multiple chat turns this session and should be rotated by the repository
owner once active work concludes — this is a conversation-hygiene issue,
not a repository code issue).

## 29. Next 5 engineering priorities

In dependency order, reasoned from the actual current state above, not
from a fixed template:

1. **Fix documentation drift found in section 10** (`REPLAY.md`,
   `DATA_SUFFICIENCY.md`) — cheap, high-value, prevents a future session
   from re-discovering "Bybit/OKX have no live client" as if it were still
   true.
2. **Attempt live Bybit verification** from an environment that can reach
   Bybit's public endpoint (this one couldn't reach OKX; Bybit's
   reachability was not tested this session either — worth checking before
   assuming it's blocked too). This is the cheapest way to convert
   "IMPLEMENTED" to "LIVE-VERIFIED" for one full venue.
3. **Market-state engine** (trend/range, volatility, liquidity, order-flow
   regime) — the actual next architectural layer per the target pipeline;
   everything in the feature/event/evidence layers is blocked on it, and it
   needs no new data (Binance alone is sufficient per section 21).
4. **BTC spot ingestion** — independent of the market-state work, blocks
   an entire category (basis, dislocation) that nothing else unblocks.
   Can proceed in parallel with #3.
5. **OKX/Bybit live ingestion, sustained** — once network access allows,
   running both collectors for real accumulates the sample size the ledger
   already flags as needed for liquidation-cascade-style event work, and is
   a prerequisite for cross-exchange alignment.

## 30. Long-term roadmap

Unchanged from the target architecture already stated in
`docs/EXECUTION_STATUS.md` and this session's handoff documents:

```
RAW DATA -> DATA QUALITY -> MARKET STATE -> MICROSTRUCTURE FEATURES
  -> ATOMIC EVENTS -> EVENT CLUSTERS -> OPPORTUNITY EVIDENCE
  -> HISTORICAL CONDITIONAL OUTCOMES -> HUMAN TRADER
```

Cross-exchange alignment and spot ingestion sit alongside "market state" as
prerequisites for the features layer, not after it — they can and should
proceed in parallel with feature-layer work, not block it.

## 31. Explicitly do not build yet

- Any BUY/SELL signal, confidence score, or autonomous trading logic — not
  the system's purpose, per every carried-forward instruction this session
  received and per the repository's own stated design.
- Cross-exchange features before at least two venues have live, sustained,
  overlapping recorded data — building the alignment *code* is fine;
  claiming *results* from it before real data exists is not.
- Basis/spot-perp features before a spot feed exists — no exception.
- Any OI-unit comparison across venues before `assert_comparable_oi`-style
  unit checking is verified to actually enforce this (not independently
  re-checked this session — verify before relying on it).

## 32. Open questions / unverified semantics

Carried unchanged from PR #22's own documentation (this audit did not
attempt to resolve them):

- OKX `trades` vs `trades-all` — same underlying fills, or different
  aggregation? Unresolved.
- OKX `trades-all.seqId` — presence/semantics unconfirmed against real
  wire captures.
- OKX `index-tickers` instId convention — unconfirmed.
- OKX open-interest canonical unit (contracts, assumed by convention with
  Bybit/OKX's own `oi` field name, not independently re-verified against
  official docs this session).
- OKX `liquidation-orders.ccy` semantics for the BTC use case — unresolved.

New from this session: none. This session's work (PRs #18, #23) touched
replay plumbing and quality-transition logic, not any OKX parsing
semantics, so it neither resolved nor introduced any semantic question.

## 33. Exact next session instruction

---

### NEXT CLAUDE SESSION — START HERE

**Verified main commit:** `e2c3c8ab1e5f2b5bd14045124b287af2ea853212`
**Verified test count:** 591 passed, 0 failed (`python3 -m pytest -q` from
a clean checkout of the commit above)
**Merged PRs:** #4 through #23, all merged, linear history, no gaps
**Open, non-blocking:** PR #3 ("Hi from Claude 👋 (test PR)") — safe to
close, unrelated to real work

**Already complete — do not redo:**
Baseline integrity, truthful coverage, raw wire/REST capture, no-silent-
discard, Binance order-book replay + Binance/Bybit/OKX non-book replay,
Bybit venue-aware replay + quality-transition parity, OKX D11 all 7
channels (parser/canonical/storage), storage venue-namespace isolation +
single-writer locks, Bybit live collector, leakage-safe label/split
scaffolding (Phase 9, though marked PARTIAL — re-verify before extending).

**What must NOT be repeated:** re-implementing OKX D11 parsing, re-fixing
`ReplayEngine`'s venue dispatch or non-book event handling, re-doing the
storage namespace/lock architecture, re-deriving OKX channel schemas from
scratch (they're in `docs/OKX_D11_CHANNEL_SCHEMAS.md`).

**What must NOT be assumed:** that Bybit or OKX live ingestion has ever
actually run against the real exchange (neither was tested this session);
that `docs/REPLAY.md` or `docs/DATA_SUFFICIENCY.md` are current (both are
stale — see section 10); that any OKX semantic open question (section 32)
has been resolved.

**Single highest-priority next task:** fix the two stale docs (section
10), then attempt live Bybit verification if network access allows — it's
the cheapest path to converting one full venue from IMPLEMENTED to
LIVE-VERIFIED. If Bybit is also unreachable from your environment, move to
the market-state engine (needs no new data, unblocks the entire feature
layer).

**Files that matter:** `collector/collector/replay.py` (now feature-
complete for all three venues' current adapters), `collector/collector/
book_engine.py`, `collector/collector/adapters/{binance,bybit,okx}.py`,
`collector/collector/canonical.py`, `docs/EXECUTION_STATUS.md` (the
trustworthy running ledger — read this first, always).

**Tests that matter:** `tests/test_replay*.py` (57 tests across three
files, all replay), `tests/test_storage_namespace_collision.py`,
`tests/test_okx_d11_channels.py`.

**Documentation that must be updated:** `docs/REPLAY.md`,
`docs/DATA_SUFFICIENCY.md` — both stale as of this audit, not yet fixed
(audit-only pass, per instruction not to silently patch docs before
reporting).

**Remains blocked:** OKX/Bybit live-exchange verification (network access
dependent), cross-exchange work (needs live multi-venue data first), spot/
basis work (no spot feed exists), everything in the feature/event/evidence
layers (needs the market-state layer first, which doesn't exist).

**What constitutes completion for the next task:** whichever task you
pick from the priority list, completion means: implemented, tested (real
tests against real behavior, not tests that pass trivially), full suite
run once and green, adversarial review performed and documented, commit
made, pushed, PR opened, CI verified green, ancestry verified against
`origin/main` after merge — not "I made the change." The habit this
session's own two PRs established (verify against the real repo before
touching anything, run the full suite once at the end, document what is
*not* claimed alongside what is) should continue.

---
