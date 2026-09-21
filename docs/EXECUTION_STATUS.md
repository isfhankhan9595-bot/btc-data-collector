# Execution Status

Engineering ledger for the BTC market-data collector. This file records what
has actually been verified against source, not what was intended.

**Rule for this file:** a phase is only marked COMPLETE when implementation,
tests, adversarial tests and documentation all exist and the suite passes.
Documentation is never a substitute for implementation.

---

## Repository baseline (audited 2026-09-18)

| Item | Finding |
|---|---|
| Authoritative branch | `main` @ `fd5341c` (merge of PR #2) |
| Merged PRs | #1 (Phase 2 storage contract), #2 (Phase 3 runtime integrity) |
| Open PRs | #3 (`claude-test-pr`) — unrelated test PR, not project work |
| Stale branches | `phase-3-fixes` (superseded by `phase-3-runtime-integrity`) |
| Test baseline on `main` | **3 failing**, 170 passing (191 passing after Phase 0) |
| CI | None. Only a destructive one-shot importer workflow. |

### Baseline defects found on `main`

| # | Defect | Severity | Status |
|---|---|---|---|
| D1 | `datetime` timestamp (schema-legal) raised `TypeError` in `_close_segment`, killing the ingest task after publication | **High** — data/process loss | Fixed (Phase 0) |
| D2 | `_next_sequence` rescanned the stream directory on every hour rollover | Medium — hot-path O(files) | Fixed (Phase 0) |
| D3 | `import-collector.yml` could clone the upstream trading-bot repo and force-push over `main`, destroying Phase 2/3 work | **Critical** — repo loss | Removed (Phase 0) |
| D4 | No CI: nothing ran tests on push or PR | High | Fixed (Phase 0) |
| D5 | `collector.collector.utils` hard-imported repo-root `telegram_bot`, making the package unimportable standalone | **High** — architecture | Fixed (Phase 0) |
| D6 | `GapDetector.check_gap` notified *before* committing `last_seen`; a raising notifier permanently corrupted gap tracking | Medium — silent corruption | Fixed (Phase 0) |
| D7 | `datetime.utcnow()` (deprecated, naive) used for hour bucketing | Low | Fixed (Phase 0) |

### Known outstanding defects (carried forward, not yet fixed)

| # | Defect | Phase |
|---|---|---|
| D10 | No raw wire capture layer. `binance_orderbook_raw` stores normalised levels, not exact payloads; there is no connection id, no REST request/response lineage. Deterministic replay is not currently possible from stored data. | 6 |
| ~~D11~~ | *Closed in this session (PR #22).* OKX adapter now implements all seven D11 channel names (`trades`, `trades-all`, `mark-price`, `index-tickers`, `open-interest`, `funding-rate`, `liquidation-orders`) plus `books`; none silently return `[]` for a well-formed frame. See "Implementation pass (PR #22)" below. Non-orderbook replay and live verification remain separately tracked, not reopened as D11. | 14 |
| D12 | Bybit adapter merges ticker deltas into shared `_ticker_state` and emits merged values without marking which fields were carried forward. Staleness is not observable. | 13 |
| ~~D14~~ | *Closed in Phase 6.* Verified against official USD-M documentation 2026-09-19. The rules were correct; five defects were found around them (D18–D22). See `docs/BINANCE_USDM_SEMANTICS.md`. | — |

---

## Phase ledger

### Phase 0 — Repository baseline and integrity — **COMPLETE**

- **Branch:** `phase-00-baseline-integrity`
- **Objective:** make the repository buildable, testable, standalone and safe
  to work in before any feature work.
- **Changes:**
  - Segment metadata timestamps normalised to epoch ms (`_epoch_ms`), so a
    schema-legal `datetime` no longer destroys the ingest task.
  - Post-publication metadata failure is now a durable
    `STORAGE_METADATA_FAILED` quality event instead of a fatal exception.
  - `_next_sequence` performs one directory scan and caches every logical
    hour; collision semantics preserved and kept hour-local.
  - Removed the destructive `import-collector.yml` workflow.
  - Added a read-only `ci.yml` running compile, import and test checks.
  - New `collector/collector/notifications.py`: optional, injectable,
    fail-open notification backend. Collector no longer hard-imports
    `telegram_bot`.
  - `GapDetector` commits state before notifying and guards the notifier.
- **Tests:** `tests/test_phase0_storage_integrity.py` (13),
  `tests/test_phase0_standalone.py` (5).
- **Suite:** 191 passed, 0 failed.

### Phase 1 — Truthful coverage — **COMPLETE**

- **Branch:** `phase-01-truthful-coverage` (base: `main` @ `0bfbe74`)
- **Objective:** make it structurally impossible for a coverage report to
  claim completeness it cannot evidence. Closes **D8**.
- **Changes:**
  - New `collector/collector/coverage.py`: evidence-interval coverage model.
    Each observation covers `[t, t + tolerance)`; covered time is the union
    of those intervals intersected with the *requested* window. Leading,
    interior and trailing gaps are classified; complete absence is a
    first-class `ABSENT` state.
  - `scripts/gap_report.py` rewritten onto the model. Adds `--json`,
    `--whole-range`, `--streams`, `--data-dir`, and an exit code keyed off
    `is_trustworthy`.
  - Invalid / duplicate / out-of-order timestamps are counted and reported,
    never silently dropped. Unreadable segments and legacy/`.seg` storage
    collisions poison the verdict instead of being skipped.
  - New `docs/COVERAGE_MODEL.md`.
- **Tests:** `tests/test_coverage.py` (37), `tests/test_gap_report.py` (19,
  rewritten from 2). Includes the 20 required adversarial scenarios, a
  partition invariant (`covered + gaps == window`) and a property test over
  500 random inputs proving 100% is unreachable without full evidence.
- **Old tests:** the two prior `test_gap_report.py` tests asserted the false
  contract (one required `100.00%` for three observations spanning five
  seconds of a day). They were rewritten to the correct contract, not
  weakened or deleted.
- **Suite:** 262 passed, 0 failed.

#### Phase 1 also closes a Phase 0 regression (D15)

`main` was merged with **red CI**. The ledger recorded Phase 0 as complete on
the strength of a local `pytest` run; the workflow itself was failing at the
step *Assert collector does not import the trading bot* (run
`35271857845`, job `test`, step 8). Steps 1-7 passed.

Cause: Phase 0 added `notifications.py`, which probes for an optional Telegram
backend behind a guarded, function-local import — the correct standalone
design — and in the same change added an inline AST scan that flagged the
name `telegram_bot` anywhere. The two contradicted each other, so CI could
never be green.

Fix: the scan is replaced by `collector/scripts/check_standalone.py`, which
enforces *reachability* rather than naming. An import is a violation unless it
sits inside a `try` with at least one handler. Module-level bare imports,
unguarded function-local imports, and imports in `else:`/`finally:` are
violations; guarded imports are not. The checker has 17 of its own tests,
including a test that the real package passes.

This is in scope for Phase 1 because a red CI makes the verification step of
every subsequent phase meaningless.
- **Live verification:** on a synthetic 3-observation day the pre-fix code
  printed `100.00%` for all three streams and exited `0`; the new code prints
  `0.0017%`–`0.0081%` and exits `1`.

### Phase 2 — Raw wire capture and no silent discard — **COMPLETE**

- **Branch:** `phase-02-raw-wire-capture` (base: `main` @ `c7b7646`)
- **Objective:** make deterministic replay *possible* by persisting the wire,
  and close every path that turned received data into nothing. Closes **D16**,
  substantially addresses **D10**, and makes **D11** explicit in code.
- **Changes:**
  - New `collector/collector/raw_capture.py`: `raw_wire` and `raw_rest`
    schemas, records and a fail-open `RawCapture` sink. Capture precedes
    parsing; absent fields stay null; payloads are bounded and truncation is
    itself a quality event.
  - `websocket_client.py` captures the frame text before `json.loads`, so a
    malformed frame is preserved rather than logged and dropped. Connection
    id is now passed to handlers via a one-time arity probe (legacy
    two-argument handlers keep working).
  - `adapters/base.py`: `ExchangeAdapter.unhandled()` + `UnhandledReason`.
    No adapter ends `normalize()` in a bare `return []` any more; a
    structural test enforces this.
  - `run_collector.py`: raw wire + REST writers wired; non-envelope frames,
    unrouted streams, adapter drops, snapshot failures and OI poll failures
    all produce durable quality events; REST snapshot and OI bodies are
    recorded verbatim with request/response timestamps kept distinct.
  - `OKXAdapter.unimplemented_channels` names the six unbuilt channels.
- **Tests:** `tests/test_raw_capture.py` (33), `tests/test_no_silent_discard.py`
  (16). One existing fake in `test_run_collector_routing.py` was modernised to
  model the aiohttp surface actually used and now asserts REST lineage.
- **Suite:** 311 passed, 0 failed.
- **Live verification:** raw wire frames (including an undecodable one) and a
  REST snapshot body were written to parquet and read back; the snapshot's
  `lastUpdateId` round-trips off disk, which is the property replay needs.

### Phase 3 — Deterministic replay and live/replay parity — **COMPLETE**

- **Branch:** `phase-03-deterministic-replay` (base: `phase-02-raw-wire-capture`, **stacked** — Phase 2 is unmerged)
- **Objective:** replace the fake replay with a real one. Closes **D9**.
  Also closes **D17**, a false-VALID found while building it.
- **Changes:**
  - New `collector/collector/replay.py`. Recorded websocket frames and
    recorded REST snapshot responses merge into one time-ordered stream;
    a frame is available at `local_receive_ts`, a snapshot at
    `response_receive_ts`, so a snapshot can never bridge a gap that
    predates its arrival. Total ordering key `(timestamp, kind_rank,
    source_index)` makes ties independent of filesystem order.
  - Replay drives `BinanceAdapter` and `LocalBook` — **the same objects as
    live**. There is no replay-only reconstruction path.
  - `ReplayResult.digest` covers book states *and* quality transitions, so
    the same prices reached via a different quality path do not compare equal.
  - New `collector/scripts/replay.py` CLI with `--verify-determinism`.
  - **Deleted `collector/scripts/replay_test.py`** — it loaded a derived
    Parquet file and asserted column bounds. No clock, no raw source, no
    reconstruction; it could not have caught a reconstruction bug.
- **Tests:** `tests/test_replay.py` (23).
- **Suite:** 335 passed, 0 failed.
- **Live verification:** replayed recorded segments from disk —
  `snapshots_applied=1`, `final_state=VALID`, `deterministic=True`, and the
  recorded corrupt frame surfaced as `frames_undecodable=1`.

#### D17 — false VALID on an unbridged book (found in Phase 3)

`BookQualityStateMachine` initialised to `VALID`. A `LocalBook` that had
never been bridged by a snapshot therefore reported `VALID`, and that value
was written into quality events and raw records via
`self.binance_book.state.state.value`. This violates master acceptance
invariant #2 ("No false VALID state").

Fixed: the initial state is now `RECOVERING` — awaiting a bridge. Two
existing tests depended on the old behaviour; both fed diff-depth messages
to an unbridged book and expected them applied, which is the bug itself.
They now establish a snapshot bridge first, and a regression test pins that
an unbridged book never reports `VALID` and buffers rather than applies.

### Phases 4+ — NOT STARTED

Remaining scope is tracked in the defect table above (D9–D14), plus D16 below.

#### Adapter layer: verified state (audited 2026-09-18, read from source)

The multi-exchange layer is **declared but not built**. Measured sizes:
`binance.py` 32 lines, `bybit.py` 26, `okx.py` 16. These are stubs written as
minified one-liners, not implementations.

| Adapter | Channels declared | Channels implemented |
|---|---|---|
| Binance | 5 | 5 (unverified against official docs — D14) |
| Bybit | 4 | 4 (ticker staleness unobservable — D12) |
| OKX | 7 | **1** (`books` only — D11 confirmed) |

`OKXAdapter.normalize()` early-returns `[]` for any channel that is not
`books`, so `trades`, `mark-price`, `index-tickers`, `open-interest`,
`funding-rate` and `liquidation-orders` are advertised as supported and
silently produce nothing.

> **Update, this session (PR #22):** D11 is closed — see "Implementation
> pass (PR #22)" above. This table and the dependency graph below are left
> as the historical record of Phase 2's finding, not edited to match current
> state, the same convention used for `~~D14~~` below.

| # | Defect | Severity | Phase |
|---|---|---|---|
| ~~D16~~ | *Closed in Phase 2.* Adapters classify every non-event outcome via `unhandled()`. | — | — |

#### Verified dependency order for remaining work

```
D16 silent-discard  ──┐  (independent, small, unblocks honest measurement)
D13 recovery bounds ──┤  (independent)
D14 Binance docs    ──┤  (independent; needs current official Binance USD-M docs)
                      │
D10 raw wire capture ─┴──> D9 replay engine ──> live/replay parity
                                                     │
D11 OKX, D12 Bybit ─────────────────────────────────┴──> cross-exchange alignment
                                                              │
                                                              └──> features ──> labels ──> splits
```

`D10` is the architectural keystone: deterministic replay is impossible from
what is currently stored, because `binance_orderbook_raw` persists normalised
levels rather than exact payloads, with no connection id and no REST
request/response lineage. Nothing downstream of replay can be validated until
that layer exists.

**Exchange work requires current official documentation.** D11, D12 and D14
must not be implemented from memory; the sequence, timestamp, trade-side and
liquidation-side semantics have to be read from Binance USD-M, Bybit v5 and
OKX v5 docs at implementation time.


### Correction — PR #9 restored to main

PR #9 (Phase 5, Bybit ticker staleness / D12) merged into
`phase-04-bounded-recovery`, but that intermediate branch was never itself
merged into `main` — only an earlier commit on it was, via PR #8. The fix
was verified, tested and merged, yet absent from `main`. Diagnosed via
`git merge-base --is-ancestor`, confirmed via the GitHub API
(`merged: true`, `merge_commit_sha` present, but unreachable from
`origin/main`). Reapplied directly from the orphaned merge commit
(`41cd7f9`) onto current `main`: `bybit.py`, `canonical.py` provenance
fields, `test_bybit_ticker_staleness.py`, and its doc. Clean apply, no
conflicts. Suite: 401 passed, 0 failed.


### Phase 6 — Binance USD-M semantics verified (D14) — **COMPLETE**

- **Branch:** `phase-06-binance-semantics-d14` (base: `main` @ `f5463de`)
- **Objective:** close **D14** by verifying the USD-M diff-depth rules against
  current official Binance documentation, and fix whatever the verification
  exposes. This is P0: every downstream layer trusts book correctness.
- **Verification:** "How to manage a local order book correctly" (USD-M
  futures), read 2026-09-19. Steps 1–9 mapped to enforcement sites; see
  `docs/BINANCE_USDM_SEMANTICS.md` for the table.
- **Result:** the documented rules were **already correct**, including both
  places USD-M diverges from Spot (step 4's strict `<`, step 5's absent `+1`).
  Both are now pinned by tests that fail if changed to the Spot form.
- **Five defects found around them, all fixed:**

| # | Defect | Severity | Effect |
|---|---|---|---|
| D18 | A re-delivered diff failed the `pu` check and was classified as a sequence gap | **High** — false data quality | Wrote a `SEQUENCE_GAP` into the durable record for a hole the venue never created, and spent a bounded REST recovery slot. Step 7's absolute quantities make a non-advancing event redundant, not a break. |
| D19 | `depth10` partial depth was reachable by the diff path (`"@depth" in stream` also matches `@depth10`) | Medium — latent | A top-N snapshot applied as a diff freezes stale depth below the top N while the book still reports `VALID`. Guarded at both runners but not in `LocalBook`, which owns book authority. |
| D20 | A frame with no `pu` reported `pu_mismatch` | Low — misattribution | Unprovable continuity was indistinguishable from violated continuity. |
| D21 | `binance_snapshot_bridge` raised `TypeError` on `None` ids | Medium | Reachable via public `BinanceAdapter.bridge_accepts`; a raising predicate turns a data problem into a crashed ingest task. |
| D22 | A valid snapshot arriving *ahead* of the buffer was discarded, identically to one behind a hole | **High** — recovery | The first case resolves itself on the next 100ms diff; discarding it looped REST snapshots through the bounded budget and held the book un-bridged for up to a minute. Now retained and re-bridged from recorded data. |

- **Self-review finding.** The first D22 implementation left `run_collector`
  booking `snapshot_ahead_of_buffer` as `controller.fail()`. On a cold start
  the buffer is empty until the first diff lands, so that outcome is common
  and backoff would have escalated toward `attempts_exhausted` during normal
  operation. Now treated as a deferred success.
- **Live/replay parity preserved:** `replay.py` retries a retained snapshot at
  the same point as the runner, so the same recorded bytes produce the same
  book.
- **Tests:** `tests/test_binance_usdm_semantics.py` (24), including
  step-by-step conformance, one test per defect, a stale-storm adversarial
  test, and a 400-case property test proving an unbridged book never reports
  `VALID`.
- **Rewritten, not weakened:**
  `test_replay.py::test_replayed_repeat_of_an_applied_diff_is_not_silently_accepted`
  asserted the pre-D18 contract explicitly in its docstring. Replaced with
  `..._is_recorded_but_not_a_gap`, which still requires the duplicate to be
  observable but asserts idempotence instead of a false gap.
- **Suite:** 422 passed, 0 failed (was 401).
- **Not claimed:** no live Binance session was run; conformance is against the
  documented procedure and recorded fixtures.

### Phase 7 — OKX public raw capture — **COMPLETE**; D11 parsers still **BLOCKED**

- **Branch:** `phase-07-okx-raw-capture` (base: `main` @ `fa0d8d2`)
- **Objective:** stop being blocked *by documentation* on D11. Capture the real
  OKX frames so the schemas can be read off the wire instead of guessed.
- **Verified first:** Phase 6 / PR #12 is merged and its content is genuinely
  reachable from `main` (`git merge-base --is-ancestor` on both commits, plus a
  content check that `stale_update`/`pu_missing` and `retry_pending_snapshot`
  are present in `origin/main`'s blobs, not just that the commits exist).
  Suite re-measured on `main`: **422 passed**.

#### What was built

| Component | Role |
|---|---|
| `websocket_client.py` | three optional venue hooks: `on_open`, `keepalive`, `control_frames` |
| `collector/okx_capture.py` | `OKXPublicCapture`, `SubscriptionLedger`, envelope classification |
| `run_okx_capture.py` | standalone bounded entrypoint; writes `raw_wire` + `quality_events` only |
| `scripts/okx_schema_report.py` | reads captured frames back, reports observed field structure |

Connection protocol implemented **only** from official OKX v5 documentation
(read 2026-09-19): endpoint, subscribe/unsubscribe envelopes, 64 KB args
limit, `event: subscribe|error|notice` responses, notice code 64008, the 30s
idle disconnect and the literal `ping`/`pong` heartbeat, 3 connects/sec, 480
subscribe requests per connection per hour. Full table in
`docs/OKX_RAW_CAPTURE.md`.

The only payload fields read anywhere are `arg.channel` and `arg.instId` —
documented **envelope** fields. Nothing inside `data[]` is touched.

#### Defects prevented by design

- **False malformed-frame storm.** OKX's `pong` is the bare string `pong`, not
  JSON. Without an explicit control-frame allowlist it reaches `json.loads`,
  fails, and writes a durable `ERROR` quality event *once per heartbeat,
  forever, on a healthy connection* — a false data-quality signal of the same
  class as D18. Control frames are now captured, counted, and never reported
  as malformed; a genuinely corrupt frame still is (both pinned by tests).
- **Silent dead subscriptions.** A rejected channel produces nothing for the
  life of the connection. Logged-only, that is indistinguishable from a quiet
  market. `SubscriptionLedger` makes "not subscribed" observable, and
  acknowledgements are cleared on reconnect because an ack belongs to a
  connection, not to the process.
- **Guessed parsers.** Capture subscribes to all six unimplemented channels
  while `OKXAdapter.normalize` continues to refuse them. A test asserts the
  adapter still raises `CHANNEL_NOT_IMPLEMENTED` for every one, so this phase
  cannot have quietly enabled a guess.

#### Self-review fixes applied before commit

- Cancelled keepalive tasks are now awaited, so a pending task cannot outlive
  its connection across reconnects.
- `_keepalive_loop`'s `assert` replaced with a real guard (asserts are
  stripped under `python -O`).
- `okx_schema_report` catches `StorageCollisionError` and poisons the report
  instead of raising: ambiguous storage means the observed frame set is not
  provably the captured frame set.

- **Tests:** `tests/test_okx_capture.py` (40) — documented subscribe format,
  envelope classification for all seven event shapes, control-frame handling,
  malformed-frame regression, subscription lifecycle, reconnect ack clearing,
  capture-before-classification for all envelope kinds, keepalive ping/pong/
  timeout with a controlled clock, Binance-unchanged regression, and schema
  report behaviour including numeric-string typing and the semantics caveat.
- **Suite:** **461 passed**, 0 failed (was 422).

#### What this does and does not unblock

| | Status |
|---|---|
| Field **names** in `data[]` | obtainable from capture — this is the unblock |
| Field **types** (incl. numeric-strings) | obtainable from capture |
| Field **meanings** — units, sign conventions, period vs annualised funding, liquidation `side` encoding | **still blocked**; observation cannot establish semantics |

So D11 is **not** closed. Capture removes the *name* half of the blocker; the
*semantic* half still requires official documentation or an authoritative SDK.

#### Official-documentation semantic pass (this session)

`docs/OKX_D11_CHANNEL_SCHEMAS.md` records verified field-level schemas for
all six channels (`trades`, `trades-all`, `mark-price`, `index-tickers`,
`funding-rate`, `open-interest`, `liquidation-orders`), sourced from OKX's
official `docs-v5` documentation (cross-checked across regional mirrors of
the same content) and, for `liquidation-orders`, a real captured production
frame found in a public bug report. This substantially closes the semantic
half for envelope shape, field names/types, and funding
current/next/settled-period semantics. It explicitly does **not** resolve:
whether `trades` aggregates multiple fills per push relative to
`trades-all`, whether `seqId` (added to `trades` per the 2025-07-08
changelog) is present on `trades-all`, and one open question on
`liquidation-orders`' `ccy` field — those need comparison against real
captured frames for BTC-USDT-SWAP specifically, which this environment
cannot do (see "Environment blocker" below): capture-only verification is
still needed before the canonical mapping, not just documentation.
Parsers/canonical mapping/storage/replay/tests for these six channels are
**not implemented**; that is the next phase, tracked separately so a
citation error in the schema doc and an implementation defect are never in
the same diff.

#### Implementation pass (PR #22, this session)

Six channel groups (seven channel names: `trades`, `trades-all`,
`mark-price`, `index-tickers`, `funding-rate`, `open-interest`,
`liquidation-orders`) now go raw frame → parser → canonical event → durable
storage. Status by the states the task asked to distinguish, not "D11
COMPLETE":

| Aspect | State |
|---|---|
| Schema verified (official docs / captured frame) | **COMPLETE** — `docs/OKX_D11_CHANNEL_SCHEMAS.md`, PR #21 |
| Parser + canonical mapping implementation | **COMPLETE** — `collector/collector/adapters/okx.py`, all 8 declared channel names (`books` unchanged + 7 D11 names); `OKXAdapter.declared_channels() == OKXAdapter.implemented_channels()` is itself asserted by test |
| Storage wiring | **COMPLETE** for the 7 D11 streams (`okx_trades`, `okx_trades_all`, `okx_markprice`, `okx_indextickers`, `okx_fundingrate`, `okx_openinterest`, `okx_liquidation`) via new `run_okx_collector.py`. **NOT included:** `okx_orderbook` — a live OKX order-book collector needs the same `LocalBook`/quality-state-machine wiring Binance/Bybit have and was out of D11's scope; `books` already has its own raw-only capture path (`run_okx_capture.py`) unchanged by this PR. |
| Fixture tests | **COMPLETE** — `tests/test_okx_d11_channels.py` (happy path per channel from documented/captured examples, malformed/missing-field handling, empty-string vs missing-field distinction, repeated-seqId non-gap, channel purity, determinism) and `tests/test_okx_collector_storage.py` (namespace isolation, per-channel stream routing). Existing tests asserting the old `CHANNEL_NOT_IMPLEMENTED` behaviour updated to assert the new implemented behaviour (`tests/test_okx_capture.py`, `tests/test_raw_capture.py`, `tests/test_no_silent_discard.py`) rather than left contradicting the code. |
| Replay | **COMPLETE** as of this session — see "Non-orderbook replay (this session)" below. Was NOT COMPLETE when PR #22 merged: `_handle_wire` `continue`d past every canonical event type except `CanonicalOrderBookEvent`, for every venue, not just OKX; that cross-venue gap is now closed. |
| Live verification | **PENDING** (environment blocker, below) — unchanged from Phase 7. |

Five open questions from `docs/OKX_D11_CHANNEL_SCHEMAS.md` remain open;
implementation code handles all five defensively (reads the relevant field
with `.get()`/no assumption of presence or absence) rather than resolving
them by assumption — see per-question comments in
`collector/collector/adapters/okx.py` and the corresponding tests in
`tests/test_okx_d11_channels.py`.

Two related bugs fixed as part of this pass, found by writing the
implementation rather than pre-existing test coverage: (1) both
`OKXAdapter.subscribe_message` and `okx_capture.okx_subscribe_message`
previously applied one `instId` to every channel uniformly, which is the
wrong argument shape for `liquidation-orders` (`instType`-scoped, confirmed
in two independent doc mirrors) — fixed, channel-aware now, pinned by
`test_capture_subscribe_args_are_channel_aware`. (2) `index-tickers`'
subscription instId (the underlying index, not the SWAP instId) was
previously not distinguished anywhere in code; it is now an explicit,
overridable parameter (`OKXAdapter.index_inst_id` /
`okx_subscribe_message`'s `index_inst_id`) rather than silently reusing the
SWAP instId, since the correct value is open question #3 and this way a
verified correction is a one-argument change, not a code change.

**Suite:** **573 passed**, 0 failed (was 538 after PR #21).



This code has **never been run against the live venue**. The execution
container denies egress to `ws.okx.com`
(`x-deny-reason: host_not_allowed`; only package/registry hosts are
reachable). Every test drives the real client loop through a fake socket,
which verifies the logic and protocol shape but **cannot** verify that OKX
accepts the subscribe request or that the heartbeat satisfies it.

Treat "OKX capture works" as **UNVERIFIED against the venue** until
`run_okx_capture` has been executed somewhere with network access.

#### Smallest next action

Run, on a host with outbound access:

```bash
python -m collector.run_okx_capture --duration 600 --data-dir data
python -m collector.scripts.okx_schema_report --data-dir data
```

Then supply the report output plus the official push-data tables for the six
channels. With names observed and semantics documented, the parsers become a
mechanical, safe change.

#### Regression found and fixed during merge review (pre-merge, this PR)

Independent review before merging this PR found that `_consume`'s new
`control_frame=` keyword is passed to **every** `on_raw_frame` callback,
including `run_collector.CollectorApp._capture_raw_frame` (the live Binance
raw-capture path), whose signature had no such parameter and no `**kwargs`.
Every call therefore raised `TypeError` inside `_consume`'s fail-open
`try/except`, which logs a warning and continues -- meaning **live Binance
raw-wire capture would have gone completely dark**, silently, with no
exception surfacing past a per-frame log line and no quality event.

This is the exact "silent parser failure" class this project forbids, and it
passed CI: the only existing integration-level test for this call path
(`test_no_silent_discard.py`) used `def on_raw(frame, **kw): ...`, which is
strictly more permissive than the real method and could not have caught a
signature mismatch against it. This PR's own "Binance-unchanged" test
(`test_binance_style_client_starts_no_keepalive_task`) checked `on_message`
delivery and the absence of a keepalive task, and never exercised
`on_raw_frame` at all.

**Fixed**: `_capture_raw_frame` now accepts `control_frame: bool = False`
(unused for Binance, which has none). **Verified bidirectionally**: the new
regression test
(`test_production_capture_raw_frame_survives_every_on_raw_frame_call_shape`,
driving the real `WebSocketClient._consume` coroutine against the real
production callback) was confirmed red against the original signature
(reproducing the exact swallowed `TypeError`) and green after the fix, so
the test is known to actually exercise the failure mode rather than merely
share its blind spot. Suite: 462 passed (461 + 1).

### Phase 8 — Bybit live client — **PARTIAL, VERIFIED**

`collector/run_bybit_collector.py`: a standalone runner wiring the
already-correct `BybitAdapter` and venue-generic `LocalBook("BYBIT")` (both
pre-existing, unmodified) to a live `WebSocketClient` connection, raw wire
capture, and durable storage.

**Protocol facts verified 2026-09-19 against official documentation** (not
third-party SDKs, not memory):
- Endpoint `wss://stream.bybit.com/v5/public/linear` and subscribe envelope
  `{"op":"subscribe","args":[...]}` —
  https://bybit-exchange.github.io/docs/v5/ws/connect
- Orderbook depth levels (1/50/200/1000 for linear) —
  https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook.
  50 is used, an operational choice, not a protocol fact.
- Heartbeat is **JSON** (`{"op":"ping"}` sent every ~20s; public reply
  `{"success":true,"ret_msg":"pong","conn_id":"...","op":"ping"}`), unlike
  OKX's raw-text control frames.

**Design decision, stated not hidden:** the keepalive is fire-and-forget
(`expect=None`). `WebSocketClient`'s reply-tracking only recognises an exact
raw-text match against `control_frames`, built for OKX's literal
`"ping"`/`"pong"`. Bybit's JSON pong has a dynamic `conn_id` and can never
satisfy that match; wiring `expect=` would mark every ping as permanently
awaiting a reply this mechanism cannot see arrive, eventually forcing a
spurious reconnect on a healthy connection. Chosen deliberately conservative
given this session's P0 hotfix was exactly an insufficiently-tested change to
this same shared file — extending the reply-matching logic to recognise a
JSON pong generically is real future work, not a defect in what ships here.
BTCUSDT public channels push data continuously, so `_last_inbound_monotonic`
(reset by any frame) is the dominant liveness signal regardless; a genuinely
dead socket still surfaces via a failed `_send()`.

**Storage:** distinct `bybit_*` stream names throughout (`bybit_orderbook`,
`bybit_trades`, `bybit_markprice`, `bybit_openinterest`, `bybit_liquidation`,
`bybit_raw_wire`, `bybit_quality_events`) with new schemas mirroring the
Binance canonical shape minus derived microstructure features — never the
shared Binance-named streams or Binance canonical schemas, both for a
concrete reason (see `config.py`'s docstring above `BYBIT_ORDERBOOK_SCHEMA`):
the Binance canonical schemas have no exchange column, and PR #13's OKX
capture already reuses Binance's own `"raw_wire"`/`"quality_events"` stream
names, which risks a segment-sequence race between independent OS processes
(`ParquetWriter._seq` is a per-instance, uncoordinated in-memory counter).
That existing risk fails loud, not silently (`FileExistsError` guards
publish), so it is recorded here as a known defect rather than fixed as a
side effect of unrelated Bybit work.

> **Superseded by the storage-namespace phase (below).** The claim above that
> this risk "fails loud, not silently" was wrong: two writers on one stream
> open the same `.tmp` path before `FileExistsError` can fire, and the second
> writer's orphan recovery deletes the first's live segment.

**No derived microstructure features are computed for Bybit.**
`feature_computer.compute_orderbook_features` reads raw Binance-message keys
directly (`msg.get("E", ...)` with a silent fallback to local time when
absent, and per-message levels rather than persistent book state) — reusing
it on Bybit's payload shape would silently produce wrong values rather than
fail loudly. Feature computation belongs in one verified, causal, cross-venue
phase (item H), not bolted onto one exchange's ingestion wiring.

**Three real defects found and fixed** while wiring this, none of them
protocol guesses — all found by driving the actual
`WebSocketClient._consume()` coroutine with synthetic frames rather than
calling the handler methods directly (the same lesson as this session's P0
hotfix, applied proactively this time):
1. `on_open` is invoked as `await self.on_open(self._send)` — the client's
   bound `_send` method, not the client instance. A first draft assumed the
   latter and would have raised `AttributeError` on every connection.
2. `on_message` is `await`ed by `_consume()`; a first draft's handler was a
   plain synchronous method, which would have raised `TypeError` on the
   first frame, inside the same fail-open `try/except` responsible for the
   P0 hotfix — this bug is the exact shape of that one, caught before
   shipping instead of after.
3. `on_quality_event` is called with **4** positional arguments
   (`event_type, reason, connection_id, stream_group`); a first draft's
   handler took 2 and would have raised `TypeError` on the first malformed
   frame or disconnect.

A fourth issue was a test bug, not a production one, and is recorded to
distinguish it from the three above: a first draft of the sequence-gap test
asserted that an *increasing* jump in Bybit's `u` is a gap. It is not —
`BybitSequenceComparator` (pre-existing, unmodified) only flags a decrease or
exact repeat, matching official docs (`u` only guarantees non-decrease;
continuity is self-healed by a fresh snapshot, not a client-side bridge chain
the way Binance's `pu` rule works). The test encoded the wrong assumption,
not the implementation; corrected to assert what the protocol actually
guarantees.

**Also fixed, adjacent:** `ParquetWriter._emit_quality`/`_emit_drop`
hardcoded `"exchange": "BINANCE"` regardless of which venue's writer emitted
them — latent because nothing before this constructed a `ParquetWriter` for
a second venue. A Bybit writer's own storage failures would have been
durably misattributed to Binance. Fixed with an `exchange` constructor
parameter (default `"BINANCE"`, so every existing call site is unaffected),
with a regression test for both the new venue and the default.

**Tests:** `test_bybit_collector.py` (11, driving the real `_consume()`
coroutine) + 2 new `ParquetWriter` exchange-attribution tests. Full suite:
**509 passed** (496 + 13).

**Not claimed:**
- No live connection to Bybit was made or tested in this environment (no
  outbound network access to exchange domains from this sandbox — the same
  constraint noted for OKX). Everything above is verified against official
  protocol documentation and exercised with synthetic frames through the
  real client/adapter/book-engine code path, not a live session.
- No derived microstructure features, as stated above.
- The `ParquetWriter` multi-process same-stream-name collision risk
  (pre-existing, from PR #13's OKX capture reusing Binance's stream names)
  is recorded, not fixed.
- OKX's six unimplemented channels (D11) remain blocked exactly as recorded
  in Phase 7 — unrelated to this phase, not attempted here.

### Phase 8 addendum — ReplayEngine does not actually support Bybit yet — **DEFECT RECORDED, NOT FIXED**

Checked while looking at item E (deterministic replay) as Phase 8's natural
next dependency. `ReplayEngine.__init__(self, venue: str = "BINANCE", ...)`
correctly parameterizes `LocalBook(venue, ...)` (so the sequence comparator
is venue-correct), but **hardcodes `self.adapter = BinanceAdapter()`
unconditionally** — the `venue` parameter does not select which adapter
parses incoming frames. Passing `venue="BYBIT"` to `ReplayEngine` today would
silently attempt to parse Bybit-shaped raw frames with `BinanceAdapter`,
producing nothing usable, not an error.

Separately, `ReplayEngine.run()`'s REST-snapshot-bridge path
(`self.book.binance_snapshot(last_update_id, snapshot_event)`) is
Binance-specific by name and by protocol (USD-M's snapshot-then-bridge
model). Bybit's snapshot arrives as a `type: "snapshot"` message over the
same WS stream, handled through `LocalBook.apply()`'s general `is_snapshot`
branch — a different code path that this method does not call.

**Not fixed here.** Extending `ReplayEngine` to genuinely support a second
venue means parameterizing the adapter and branching the snapshot-handling
path correctly for each venue's actual protocol — a real, scoped change to
shared replay infrastructure, which is exactly the kind of narrow,
insufficiently-tested-before-shipping change that caused this session's P0
hotfix. Recorded precisely so a future session (or a person reading this
file) does not assume Bybit replay works because the constructor accepts a
`venue` argument.

### Phase 8 addendum, fixed — ReplayEngine is now venue-aware; replay/live quality-transition parity — **COMPLETE, VERIFIED**

Fixes both defects recorded immediately above, plus a second, related defect
found while writing the regression test for the first.

**`ReplayEngine.__init__` — defect fixed.** `self.adapter = BinanceAdapter()`
was unconditional. Now dispatches on `venue` through a small
`_ADAPTER_CLASSES` map (`{"BINANCE": BinanceAdapter, "BYBIT": BybitAdapter}`),
matching the same venue-keyed-dict shape `LocalBook.comparator` already uses.
`venue` is upper-cased once in the constructor so `"bybit"` and `"BYBIT"`
resolve identically; an unrecognised venue raises `ValueError` at
construction rather than silently defaulting to Binance. `_handle_snapshot`
(the Binance REST-snapshot-bridge path, `binance_snapshot`/`lastUpdateId`
shape) now refuses a `REST_SNAPSHOT` frame for any non-Binance venue instead
of parsing it as Binance's shape by coincidence — Bybit's snapshot arrives on
the wire itself (`type: "snapshot"`) and is already handled by `_handle_wire`
via `LocalBook.apply()`'s general `is_snapshot` branch, so no Bybit code path
was missing, only the guard against a REST frame that should never occur for
that venue.

**`ReplayEngine._handle_wire` — quality-transition parity defect fixed.**
Previously recorded a transition only when `after is SEQUENCE_GAP`. Bybit's
`is_resync_signal` (an `update_id` decrease, see `sequence.py`,
`BybitSequenceComparator`) drives the book straight from `VALID` to
`RECOVERING` via `BookQualityStateMachine.resync()` — never through
`SEQUENCE_GAP` — so that transition was silently omitted from every replay,
while `run_bybit_collector.BybitCollectorApp._apply_orderbook` recorded it
live using the broader `before is not after` condition. Binance's comparator
has no path that returns `is_resync_signal` (pinned by a new test,
`test_binance_comparator_never_emits_a_resync_signal`, that exercises every
id relationship it distinguishes), which is exactly why no existing
Binance-only replay test ever exercised this. Fixed by matching live's
condition and event-type choice exactly: `before is not after` records a
transition, typed `SEQUENCE_GAP` if `after` lands on `SEQUENCE_GAP` or
`RECOVERING`, else `RECOVERY`.

**Second defect found while testing the fix above, also fixed:**
`ReplayEngine._record_transition` read `previous_state`/`new_state` off
`LocalBook.last_transition` rather than the `before`/`after` values the
caller already had. `last_transition` is not updated by every state-changing
path in `LocalBook` — `LocalBook.snapshot()` (the plain websocket-snapshot
bridge Bybit's wire uses) changes state via `recovered()` without touching
it, while Binance's `_attempt_bridge` does set it — so a Bybit recovery
transition (`RECOVERING` → `VALID` via a fresh snapshot) was recorded with
stale or absent `previous_state`/`new_state`, even after the first fix made
replay attempt to record it at all. Fixed by changing `_record_transition`'s
signature to take `before`/`after` explicitly at every call site, matching
exactly what live's `_apply_orderbook` already does (compare its own local
`before`/`after`, never a stored last-transition object) — removing the
dependency on `LocalBook` remembering to set `last_transition` on every
current and future state-changing path, rather than patching around one
missing case.

**Tests (new, 11):** `tests/test_replay_bybit_parity.py` —
venue-aware adapter selection (default, explicit, case-insensitivity,
unknown-venue rejection); a full Bybit session actually advancing the book
end-to-end; a `REST_SNAPSHOT` frame refused for a non-Binance venue; a
regression test proving `VALID → RECOVERING` is recorded (fails under the
old `after is SEQUENCE_GAP` check, passes under the fix — verified by
reverting the fix in a scratch copy and confirming the test fails, then
restoring it); the symmetric `RECOVERING → VALID` recovery-recording test
that caught the second defect above; a direct-drive parity test mirroring
the existing `test_replay_reconstruction_matches_a_direct_book_drive` for
Bybit; and the Binance-comparator invariant test. Full suite: **520 passed**
(509 + 11 new). No existing test was changed.

**Not claimed:**
- Only Binance and Bybit are wired into `_ADAPTER_CLASSES`. OKX has no
  replay support and is not claimed here — Phase 7's raw-capture work
  predates OKX's adapter having a stable enough shape for this, and D11 (OKX
  channel schema verification) is still open per the Phase 6/7 notes above.
- Bybit's own live-collector code path (`run_bybit_collector.py`) was read
  for parity but not modified or re-tested in this pass; only replay changed.
- Multi-exchange storage namespace collision (`raw_wire`/`quality_events`
  stream-name collision between OKX and Binance, noted in Phase 7) is
  unrelated to this fix and remains open.
  *(Addressed afterwards in the storage-namespace phase below.)*

### Phase 9 — Leakage-safe label horizons and chronological splits — **PARTIAL, VERIFIED**

**Provenance.** Found as uncommitted work in the shared container (branch
`phase-09-leakage-safe-splits`, never pushed) while resolving the PR #13
hotfix. Origin — another concurrent session or the repository owner directly
— could not be established, so it is not claimed as originating from this
session. It was reviewed in full before being adopted: both diffs read
line-by-line, tests read and matched against the defects they claim to
cover, then run as a batch, then the full suite, then an independent
adversarial pass (boundary cases: single-date and near-empty inputs,
adjacent-day gap arithmetic, zero-horizon derivation when no `return_*`
column exists, monotonicity of the 70/85% split fractions) before commit.
Nothing here is taken on the strength of its own docstrings or the fact that
its own tests passed.

**`pipeline/label_generator.py` — defect fixed:** horizons were applied as a
row shift (`shift_rows = int(h*1000/grid_ms)`), which equals `h` seconds only
on a perfectly regular grid. The aligned grid has real outages, so
`return_1s` could silently measure an arbitrarily longer span across a gap,
and `int()` truncation could silently shorten it (`grid_ms=300, h=1` measured
900ms). Every downstream conditional statistic would then answer a different
question than its column name claims. Fixed: grid regularity is verified
from a timestamp column when available (absence is recorded as
`grid_verified=False`, never assumed regular); a horizon not evenly divisible
by `grid_ms` is rejected in `strict` mode and its realised span recorded
otherwise; the realised horizon in milliseconds is written to metadata
per-column, not just the requested one.

**`pipeline/split_generator.py` — defect fixed:** the previous embargo logic
(`dates[:train_end - embargo_days] if train_end > embargo_days else
dates[:train_end]`) silently dropped the embargo entirely whenever a split
was shorter than the requested gap, while the manifest still recorded the
requested `embargo_days` as if applied — the exact "verify programmatically
that an embargo actually exists" failure this project's rules name
explicitly. It also removed the gap from both sides of each boundary,
silently doubling it. Fixed: `purge_days` is derived from the *measured*
longest label horizon across every labeled file's schema (not the newest
file alone, and not a hardcoded constant that could drift out of step with
the label set); the gap is removed from the end of the earlier split only,
since forward-looking labels leak forward; `verify_manifest()` independently
re-derives ordering, disjointness and achieved gaps from the recorded dates
rather than trusting the arithmetic that produced them; an empty split or an
under-achieved gap raises `LeakageError` in `strict` mode rather than writing
a manifest that reports itself safe.

**Tests:** `test_leakage_safe_splits.py` (new, 31 tests) plus updates to the
two existing test files — 39 in the batch group, all passing. Full suite:
**496 passed** (462 + 34 net new/changed).

**Not claimed:**
- `pipeline/dataset_assembler.py`, `pipeline/cross_exchange_alignment.py`,
  `pipeline/stats_computer.py` are untouched and **not reviewed** against
  these same leakage rules in this pass. A brief read shows sound causal
  design in each (`dataset_assembler.py` uses `merge_asof(direction=
  "backward")` with explicit freshness tolerances and staleness flags rather
  than silent forward-fill, and bins trades forward to the next grid point
  so a grid row never uses a trade that has not yet happened;
  `cross_exchange_alignment.py`'s `causally_align()` explicitly discards any
  event later than the observation timestamp; `stats_computer.py` computes
  descriptive statistics only, no causal claims). This is a read, not an
  audit: no adversarial tests were written against any of the three, and
  `cross_exchange_alignment.py` and `stats_computer.py` have no dedicated
  test file at all (`dataset_assembler.py` has one, unmodified here).
- No real labeled dataset has been run through this end-to-end; correctness
  is verified against synthetic fixtures and property-style boundary cases,
  not a production run.
- Walk-forward / purged / embargoed *evaluation* (item N in the priority
  list) is distinct from the chronological single-pass split here and is not
  built.

### Storage-namespace phase — multi-venue stream namespaces and single-writer locks — **COMPLETE**

**Post-merge verification (independent, this session).** PR #19 merged as
`1c5d44e`. Confirmed: `1c5d44e` is current `origin/main` HEAD; `d2e2ea1`
(PR #19 base) and `7b04078` (storage-namespace commit) are both ancestors
of `main` via `git merge-base --is-ancestor`; full suite re-run clean —
**538 passed**; latest CI check-run on `1c5d44e` is `success`. Re-read of
`storage_layout.py` and `parquet_writer.py` confirms the writer lock is
acquired before sequence allocation and orphan recovery in `__init__`, so a
second writer's construction fails at the lock, before it can reach either —
this is what makes S2/S3 structurally impossible rather than merely
untriggered in tests.

**Provenance.** The previous session's OKX renames were never pushed and were
not present in the repository when this session began (`main` @ `d2e2ea1`,
clean tree, no stash, no branch carrying storage work). The work was redone
from the source, not adopted from a transcript.

**Defects found by inspection and reproduced on the real writer:**

| # | Defect | Severity |
|---|---|---|
| S1 | OKX capture shared Binance's `raw_wire` / `quality_events` stream directories. | High |
| S2 | Two live writers on one stream directory choose the same sequence and open the **same `.tmp` path**; the publish-time `FileExistsError` guard fires only afterwards. The earlier note that this "fails loud" was incorrect. | High — data corruption |
| S3 | A writer's orphan recovery deletes every `*.seg.tmp` in its directory: from a second process, that is the first process's live segment, reported as a `DATA_DROP`. | High — data loss + false alarm |
| S4 | The OKX runner built its writers without `exchange="OKX"`, so a crashed OKX segment was durably recorded as a **BINANCE** storage fault. | Medium — misattribution |
| S5 | `ReplaySource.from_directory` / `replay_directory` hard-coded Binance's streams and adapter: replaying Bybit or OKX from disk read Binance's frames, and Binance replay would read legacy OKX frames from the shared `raw_wire`. | High — cross-venue mixing in replay |

**Fixed:**
- `storage_layout`: `venue_stream`, `read_streams`, `check_stream_namespace` —
  one authority for venue → stream directory. Binance keeps its historical
  names; OKX is `okx_*`; Bybit was already `bybit_*`. Unregistered venues
  raise.
- `ParquetWriter`: refuses a stream that contradicts its declared venue, and
  holds an exclusive `flock` on `<stream_dir>/.writer.lock` for its lifetime
  (across hour rollover; released by public `close()`; kernel-released on
  crash). A second writer raises `StorageWriterLockedError` before touching
  anything.
- `run_okx_capture`: `okx_raw_wire` / `okx_quality_events`, `exchange="OKX"`.
- Readers: replay and `okx_schema_report` resolve streams by venue and keep a
  row only if its own `venue` column matches; excluded rows are counted, not
  hidden. `scripts/replay.py --venue` added.
- One existing assertion updated (`test_empty_parquet_sidecar_…`): it asserted
  the stream directory was entirely empty; it now asserts no segment,
  temporary segment or sidecar exists, since the lock file is a permanent
  non-segment file by design.

**Tests:** `test_storage_namespace_collision.py` (new, 18). Full suite: **538 passed** (520 on `main` + 18). Real OS processes
(six writers across three venues × two streams, all allocating sequence before
any writes, then released together), real runner classes, real replay. Each
new test group was mutation-checked: removing the lock fails the five lock
tests; restoring the original OKX runner fails the wiring and attribution
tests.

**Not claimed:** see "Known limits" in `docs/STORAGE_NAMESPACES.md` —
notably that compaction still covers only the five Binance canonical streams,
that the lock is single-host/local-filesystem, and that legacy OKX rows are
left in place (filtered by venue on read, not migrated).

### Non-orderbook replay, OKX registered in `ReplayEngine` — **COMPLETE, VERIFIED**

Closes the gap PR #22 explicitly declined to bundle in ("Replay | NOT
COMPLETE" above) — deliberately, since that PR was right not to mix a
parser defect and a replay-engine defect in one diff. Two defects fixed:

**`ReplayEngine` had no `"OKX"` entry in its venue→adapter map.**
`_ADAPTER_CLASSES` (added in the Phase 8 addendum fix above) only had
`BINANCE`/`BYBIT`; `ReplayEngine(venue="OKX")` raised `ValueError`. Now
`OKXAdapter` is registered the same way.

**`_handle_wire` silently dropped every non-order-book canonical event, for
every venue.** The loop was `if not isinstance(event, CanonicalOrderBookEvent):
continue` — trades, mark/index/funding, open interest, and liquidations
were parsed by the adapter and then discarded before the caller ever saw
them, for Binance and Bybit too, not only OKX. Fixed by routing every such
event into a new `ReplayResult.non_book_events: list[Any]`, storing the
actual frozen canonical-event dataclass instances `adapter.normalize()`
yielded — no replay-only reshaping, no dict conversion, and (per the task's
explicit instruction) no routing through `LocalBook`, which is order-book
reconstruction only and was never meant to hold a trade or a funding rate.
`ReplayResult.digest` now folds these in (`dataclasses.asdict` +
`json.dumps(sort_keys=True, default=str)`, tagged with the event's class
name so two different event types can never hash identically by
coincidence), so a changed trade price, a disappeared trade, a changed OI
reading, funding rate, or liquidation quantity all change the digest —
proven by direct test, not asserted from reading the code.

**What this does not touch, and did not need to:** order-book replay logic,
`LocalBook`, the quality-transition parity fix above, `ReplaySource`'s
venue-row filtering (PR #19) or its frame ordering (`(timestamp, kind_rank,
source_index)`, unchanged) — non-book events are appended in the same
single pass over the same already-ordered frame sequence, so they inherit
that same causal ordering and the same "no lookahead" guarantee without any
new code needing to reason about it. Bybit's ticker carried-forward
provenance (`CanonicalMarkPriceEvent.carried_forward`/`field_age_ms`) is
also unaffected in the sense that matters: replay drives the *same*
`BybitAdapter` instance, method by method, in the same order live would —
one `self.adapter = adapter_cls()` per `ReplayEngine`, never reconstructed
per frame — so carried-forward state accumulates identically to live by
construction, not by any special-casing in replay itself. Verified by a
dedicated test (`test_bybit_ticker_carried_forward_provenance_survives_replay_unaltered`)
rather than left as an inference from the architecture.

**Not claimed:**
- OKX's D11 semantic open questions (trades vs trades-all aggregation,
  `seqId` presence, index-tickers instId convention, OI's canonical unit,
  liquidation `ccy` semantics) are unchanged by this work and remain exactly
  as open as PR #22 left them. Nothing here resolves them, tests them as
  resolved, or reads them as more certain than PR #22 documented.
- Live OKX verification is not claimed. `ws.okx.com` was not reached in
  this environment; this is a parser/replay-correctness pass over
  documented/fixture frames, per the existing Phase 7 environment blocker.
- OKX order-book replay through `LocalBook`/quality-state-machine remains
  out of scope, as PR #22 also noted — `okx_orderbook` has no live
  collector yet (`books` is raw-capture-only per Phase 7), so there is
  nothing to replay through the book path for OKX specifically. Registering
  `OKXAdapter` in `_ADAPTER_CLASSES` makes `ReplayEngine("OKX")` work
  correctly for every OKX canonical event type that exists today (all
  non-book); it does not manufacture book replay support that has no
  upstream live collector to replay.

**Tests (new, 18):** `tests/test_replay_non_book_events.py` — OKX
registration; Binance trade/mark-price/liquidation survive replay
individually and alongside a real order-book session (the exact scenario
the old isinstance filter broke); Bybit trade/liquidation survive replay,
a single ticker frame correctly producing *both* a `CanonicalMarkPriceEvent`
and a `CanonicalOIEvent` where live would, and the carried-forward
provenance test above; OKX `trades`/`trades-all` kept distinct despite
sharing a canonical class, `mark-price`/`index-tickers` kept on separate
fields, `funding-rate`'s three-way current/next/settled distinction,
open-interest's three preserved units, `liquidation-orders`' explicit
no-instrument-filtering and empty-string-vs-missing `ccy` distinction; four
digest-sensitivity tests (trade price change, trade disappearance, OKX
funding/OI/liquidation edits); replay determinism across two runs; digest
order-independence for input-list order (not timestamp order, which still
governs); and a malformed-funding-rate test confirming a missing required
field still produces `frames_unhandled`, never a fabricated event. Plus one
existing test in `tests/test_storage_namespace_collision.py` updated: it
previously asserted `ReplayEngine(venue="OKX")` raised `ValueError` (correct
at the time it was written, under PR #19); now asserts OKX replay actually
produces the expected `CanonicalMarkPriceEvent`s from its fixture's
funding-rate frames (the fixture's data dicts were missing the required
`"ts"` field, which the old test never exercised because it expected
construction to fail before parsing ran).

Full suite: **591 passed** (573 on `main` at the time this started + 18
new). No existing test's assertions were weakened, only the one described
above, which was asserting the absence of a feature this session
implements.



See `docs/DATA_SUFFICIENCY.md` for which events have their data collection
implemented, which are blocked on data that has never been collected (spot/perp
basis), and which cross-exchange rows are blocked on live verification, a
causal alignment layer and the OI unit contract.

### Documentation truth pass and live-verification status — **COMPLETE (docs only; no code changed)**

Verified against `main` @ `e2c3c8a`: PRs #19-#23 merged and reachable; PR #24
(`docs/CARRY_FORWARD_AUDIT.md`) was still open and is untouched by this pass.
**591 tests pass** when run as CI runs them (`working-directory: collector`,
`PYTHONPATH` = repo root + `collector/`).

**Fixed:** `docs/REPLAY.md` (said Binance-only and no non-book replay; both
false since PRs #18/#23; also carried a D14 "unverified assumption" note that
was closed in Phase 6) and `docs/DATA_SUFFICIENCY.md` (said Bybit had no live
client and OKX was 1/7 channels; it also asserted Binance "data flowing: yes"
with no in-repo evidence). Both now separate IMPLEMENTED / TESTED /
REPLAY-VERIFIED / LIVE-VERIFIED and do not convert one into another.

**Live verification: LIVE-UNVERIFIED — ENVIRONMENT BLOCKED, all three venues.**
Measured from the execution container: `api.bybit.com`, `stream.bybit.com`,
`www.okx.com`, `fapi.binance.com`, `fstream.binance.com` return
`403 x-deny-reason: host_not_allowed`; `ws.okx.com:8443` gives no response. No
capture was attempted or faked. Nothing here can show that any venue accepts
the subscriptions, populates timestamp/sequence fields as documented, or that
reconnect behaves as it does against fake sockets.

**Findings recorded (not fixed here):**

| # | Finding | Impact |
|---|---|---|
| G1 | **`OIUnit` and `assert_comparable_oi()` do not exist** anywhere in code or docs, contrary to an earlier handoff that described them as existing safeguards. `CanonicalOIEvent.open_interest` is one untyped float; `canonical.py` calls the canonical unit "contracts" while describing Bybit's value as base-currency. Binance's OI unit is undocumented in the repo. | Cross-venue OI comparison would silently compare different quantities. **Blocks any cross-exchange OI work.** |
| G2 | Binance OI is recorded as a `raw_rest` row (`purpose="open_interest"`) and is not routed through `BinanceAdapter.normalize()`; replay consumes only `orderbook_snapshot` REST rows. | Binance OI has no shared live/replay path and is absent from `non_book_events`. |
| G3 | OKX `books` is parsed by the adapter but not collected by `run_okx_collector`; OKX book replay has no committed test; adapter emits `float` levels into a `Decimal`-typed event. | OKX book replay is unverified; no live OKX book data. |
| G4 | `tests/test_okx_collector_storage.py` uses a bare `from run_okx_collector import ...`, so `pytest collector` from the repo root fails at collection; CI is unaffected. | Test-invocation fragility only. |
| G5 | `pipeline/cross_exchange_alignment.py` has no test file. | Cannot be relied on for causal alignment until audited and tested. |

### OI unit contract — **IMPLEMENTED, TESTED; not COMPLETE until merged (main ancestry recorded on the PR)**

Closes G1 above. A handoff had described `OIUnit` / `assert_comparable_oi()` as
existing; they did not, so they were built.

- `canonical.py`: `OIUnit` (CONTRACTS / BASE_COIN / QUOTE_USD / UNKNOWN),
  `OIUnitError`, `CanonicalOIEvent.unit` (default **UNKNOWN**),
  `assert_comparable_oi()`, `base_coin_oi()`. The self-contradictory comment
  ("canonical unit is contracts" beside "Bybit's is base-currency") is replaced.
- Units: OKX **CONTRACTS** (documented). Bybit **UNKNOWN**: verified against the
  official ticker field table, which states no unit; the base-coin reading rests
  on an example's arithmetic, and the "both sides" counting convention is
  unresolved. Binance **UNKNOWN** and has no canonical OI event.
- `okx_openinterest` / `bybit_openinterest` schemas 1.0 -> 1.1: add `oi_unit`.
- Guard semantics: differing units refused; UNKNOWN refused across exchanges but
  allowed within one exchange (so single-venue OI change stays usable).

**Tests:** `test_oi_unit_contract.py` (new, 17), mutation-checked: promoting
Bybit to BASE_COIN fails 5, a permissive guard fails 1, dropping the persisted
unit fails 1.

**Not claimed:** no consumer calls the guard (none exists); Bybit/Binance units
are unresolved; instrument/contract-size differences are not modelled; Binance
OI (G2) is untouched; segments written before schema 1.1 have no `oi_unit`
column (fixed by stream).

### G4 — test collection discrepancy — CLOSED

Two test files used bare/rootdir-relative imports that only resolved when
pytest's cwd was `collector/`:

- `test_okx_collector_storage.py`: `from run_okx_collector import ...`
- `test_replay_non_book_events.py`: `from tests.test_replay import ...`

Every other test in the suite already uses the package-qualified form
(`from collector.run_collector import ...`, established since early phases).
These two were the only inconsistent instances — found by grep across the
whole suite, not assumed.

Fixed to the existing convention (`collector.run_okx_collector`,
`collector.tests.test_replay`). No `sys.path` hacks, no new conftest logic,
no architectural change — `collector/conftest.py` already puts the repo root
on `sys.path`, which is sufficient once imports are package-qualified.

**Canonical test command, now equivalent from any cwd:**
```
pytest                    # from repo root
pytest collector          # from repo root, explicit
cd collector && PYTHONPATH=<repo>:<repo>/collector pytest tests   # CI's own invocation
```
All three: **608 passed, 0 failed.**

### G2 — Binance OI replayability — CLOSED

`compute_openinterest_features()` used wall-clock time for both of its
timestamp fields, discarding the real request/response lineage raw capture
already had. `ReplaySource.from_records` excluded every `purpose ==
"open_interest"` REST row before it became a `ReplayFrame` -- live OI and
replayed OI were not the same pipeline, and replay could not reproduce OI
at all.

Fixed with a single shared normalizer, `binance_oi.normalize_binance_oi()`,
called by both `run_collector._poll_openinterest` (live) and the new
`ReplayEngine._handle_rest_oi` (replay) via a new `FrameKind.REST_OI`. Event
availability is the REST **response's** receive timestamp, never the
exchange's own `time` field and never wall-clock-at-write. The legacy
parser is confirmed removed from the live path by a structural test, not
merely superseded.

Unit remains `OIUnit.UNKNOWN` per the project's OI contract -- not verified
against current official documentation, not guessed.

Tests: `tests/test_binance_oi_replayability.py` (29). One existing test
(`test_non_snapshot_rest_purposes_do_not_drive_the_book`) asserted the old
"OI produces zero frames" contract; corrected to assert the real one --
OI produces a frame but never touches the book.

Mutation check performed: reverting the `from_records` purpose dispatch to
its old form (never recognising `"open_interest"`) fails 9 of the 29 new
tests, confirming they actually exercise the fix rather than passing
vacuously.

Suite: 637 passed, 0 failed, both `pytest` and `pytest collector` (G4
contract) invocations agree.

### P4 — Market State Engine V0 — **PARTIAL, VERIFIED**

`collector/collector/market_state.py`: pure-function, causal, venue-local
descriptive market state from validated canonical events. Full design
rationale, causal contract, and stated limitations in
`docs/MARKET_STATE_V0.md` — summarized here.

**Architecture gate performed first**, against actual current files, not
assumed ones: confirmed `feature_computer.py` is unchanged and still
Binance-message-shaped (`msg.get("E", timestamp)` silent wall-clock
fallback, no persistent-book state) and therefore not reused;
`dataset_assembler.py`'s `merge_asof(direction="backward")` remains causal;
`cross_exchange_alignment.py` remains a tested-nowhere 13-line primitive, so
V0 deliberately produces no cross-exchange state; the already-merged
`OIUnit`/`assert_comparable_oi` contract (PR #26) is reused, not
re-implemented, and not weakened.

**Design**: `snapshot(observation_ts)` recomputes state from scratch from
the full stored event list on every call, rather than mutating one running
state. Deliberate: it makes "a late event cannot rewrite an earlier
snapshot", "replay order doesn't matter", and "no wall-clock/network/random
access" structural properties of a pure function rather than properties that
merely happen to hold today. The last of these is enforced by an AST-based
test, not just a docstring claim.

**Dimensions implemented**: price (last trade, mark/index, price-vs-mark
gated on mark freshness), book (best bid/ask, mid, spread, book_imbalance —
explicitly not called "OFI", which requires observing flow over time, not
one snapshot), trade flow (cumulative and windowed buy/sell volume, CVD,
trade count), liquidation (count and quantity by raw venue-reported side
only — direction deliberately not relabelled long/short; see doc), OI/
funding (using the existing OIUnit contract; same-venue OI-change computed,
cross-venue comparison never attempted by this module at all since one
engine is one venue by construction). Regime descriptor (TREND/RANGE/etc.)
deferred — not needed to keep V0 auditable.

**Adversarial findings, both fixed**:
1. `_book_state` initially trusted callers to pre-filter non-authoritative
   book sources (`PARTIAL_DEPTH`), matching what `run_collector.py`/
   `run_bybit_collector.py` already do -- but a future third caller might
   not remember to. Added defense-in-depth: `update()` now refuses any
   event in `book_engine.NON_AUTHORITATIVE_BOOK_SOURCES` itself.
2. A pre-existing, unrelated test
   (`test_binance_oi_replayability.py::test_raw_rest_round_trips_an_oi_response`)
   used `glob.glob(...)[0]` with an unfiltered wildcard, which could select
   a `.seg.meta.json` sidecar file instead of the `.seg` parquet segment --
   `glob.glob()`'s result order is filesystem-dependent, not alphabetical.
   Surfaced only once wall-clock date/hour changed during this session
   (unrelated to Market State V0 itself); root-caused precisely (confirmed
   the sidecar file exists, confirmed unsorted glob order is the mechanism)
   and fixed with an explicit `*.seg` filter, verified stable across
   repeated runs before and after.

**Tests**: `test_market_state_v0.py` (35) covering empty engine, CVD
accumulation, liquidation accumulation without direction claims, book state
and zero-denominator OBI, staleness for book/mark/OI distinctly, OI unit
rejection cross-venue vs. allowed same-venue, causal ordering by
`local_receive_ts` (not `exchange_event_ts`, including a future-claimed
exchange timestamp that must not pull an event earlier), late-event
non-rewrite via snapshot immutability, determinism/digest equality
regardless of insertion order, explicit non-deduplication (documented, not
hidden), venue-mismatch rejection, two-engine venue isolation, an AST-based
structural check for wall-clock/network imports, and five separate
digest-changes-on-real-mutation tests (trade price, liquidation quantity,
funding, OI, orderbook level) plus one digest-unaffected-by-a-never-fed-event
test that mutation-tests the causal cutoff itself.
`test_market_state_replay_parity.py` (5) drives real recorded frames through
the real `ReplayEngine` then the real `MarketStateEngine` for non-book
dimensions, proving replay-twice equality and mutation sensitivity through
the full pipeline, not just the unit-level engine.

**Suite: 677 passed** (637 + 40, net of the one pre-existing test fixed
along the way). `compileall` clean.

**Not claimed**:
- Book-state replay parity — `ReplayResult.book_updates` only records
  `best_bid`/`best_ask` as strings, not full bid/ask arrays, so it cannot be
  turned back into a `CanonicalOrderBookEvent` for feeding this engine. Only
  trade flow/liquidation/mark/funding/OI have a replay-parity proof.
- No trade-flow or liquidation staleness flag exists yet (only book/mark/OI/
  funding do) — stated as a known limitation in `docs/MARKET_STATE_V0.md`,
  not silently absent.
- No live exchange session backs any of this; verified against recorded/
  synthetic frames through the real code path, consistent with every other
  phase's live-verification status in this document.
- Regime descriptor, cross-exchange state, spot/perp basis: explicitly
  deferred, not attempted.

### Legacy pipeline audit (Step 14, scoped narrowly per instruction)

- `feature_computer.py`: unchanged, still wall-clock/Binance-shaped. NEEDS
  REFACTOR if ever reused; not blocking, since Market State V0 does not
  depend on it.
- `dataset_assembler.py`: `merge_asof(direction="backward")` with explicit
  tolerances remains causal on inspection. SAFE as currently used; NEEDS
  TESTS for the leakage-specific adversarial cases this project's rules
  care about (none added in this pass -- out of scope for a "short audit").
- `cross_exchange_alignment.py`: 13 lines, no dedicated test file. NEEDS
  TESTS before any cross-exchange derived state is built on it -- this is
  exactly why V0 does not attempt cross-exchange state.
- `stats_computer.py`: no wall-clock calls; takes an explicit
  `max_allowed_end_ts` boundary parameter suggesting causal-boundary
  awareness already exists. SAFE on this pass's read; not exercised further.

No P0/P1 defect found in this narrow pass that blocks the current
architecture; nothing here was fixed beyond what's listed, per the
instruction not to let a short audit become uncontrolled scope expansion.

### P6 — Causal cross-exchange alignment — **COMPLETE**

**Post-merge verification (this session, P5 pass).** Merged as PR #31,
merge commit `d86bc07` (also current `main` HEAD at the time this note was
added). Ancestry and the 33-test count independently re-confirmed via
`git log`/`pytest` before starting P5 work on top of it.

Base `main` @ `f1a9d1d`, 677 tests. Closes G5. The previous session's P6 test
spec was never committed or pushed and was not in the repository; the tests were
rewritten from its 26-point description, not restored.

- Replaced the 13-line `causally_align()` (key `"exchange:stream"`, no staleness,
  no missingness) with `(exchange, market_type, stream)` identity, an explicit
  `AlignedObservation{event, age_ms, status}`, AVAILABLE / STALE / NEVER_OBSERVED,
  required `staleness_ms`, and `expected_keys`. Zero production callers, verified
  repo-wide; not wired into `MarketStateEngine`.
- `tests/test_cross_exchange_alignment.py` (33), mutation-checked: eligibility by
  exchange timestamp fails 4; dropping `market_type` from the key fails 16;
  exclusive boundary fails 5; silently dropping stale fails 2.

**Leakage audit.** Lookahead: eligibility is receive-time only (tested at T-1/T/T+1).
Timestamp leakage: exchange timestamps cannot affect eligibility (tested both directions).
Cross-venue synchronisation: no simultaneity status exists; venues with different receive
latency are judged independently. Staleness: STALE is returned and labelled, never fresh.
Missingness: NEVER_OBSERVED only for expected keys; nothing fabricated. Replay: events
from the real `ReplayEngine` align identically across runs. Instrument contamination:
perp/spot separated by `market_type`, **but** no instrument field exists (documented
limitation). Quality: availability is not quality; the original event is returned.

**Not claimed:** exact-timestamp ties depend on input order (documented); no consumer
exists; no live verification. P5 (below) is the first adapter to set `market_type`
explicitly (`"spot"`); `MarketStateEngine` still does not consume this module (see
`market_state.py`'s own docstring).

### P5 — Real BTC Spot ingestion — **COMPLETE** (live runner, replay integration, storage round-trip; live network verification blocked)

Branch `p5-spot-ingestion` off verified `d86bc07` (PR #31 merge commit).
Official sources: `github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md`
(trade stream payload, Diff. Depth Stream payload, "How to manage a local
order book correctly" Spot procedure), verified 2026-09-19.

**Done, tested, mutation-checked:**
- `sequence.SpotSequenceComparator` — Spot's `depthUpdate` has no `pu` field
  (confirmed absent from the official payload, unlike USD-M futures);
  continuity is verified arithmetically instead (`U == prev.u + 1`, per the
  official procedure's own wording). No stale/duplicate carve-out: unlike
  `BinanceSequenceComparator`, nothing in the official Spot procedure
  documents an exception for a retransmitted event, so this comparator does
  not invent one.
- `sequence.binance_spot_snapshot_bridge` — the one documented difference
  from futures' bridge formula: `U <= lastUpdateId+1 AND u >= lastUpdateId+1`
  (futures has no `+1`). Regression-tested against `binance_snapshot_bridge`
  directly on the same event to pin the off-by-one both ways.
- `book_engine.LocalBook("BINANCE_SPOT")` — venue-aware dispatch added for
  the snapshot-discard rule (`u <= lastUpdateId` discarded for Spot, vs.
  strict `<` for futures) and the bridge predicate in `_attempt_bridge`;
  Spot joins futures in `_BUFFER_UNTIL_BRIDGED_VENUES` (same
  buffer-until-bridged flow, different arithmetic). Full snapshot-bridge,
  post-bridge chain continuation, broken-chain, and missing-overlap cases
  tested against `LocalBook` directly, mirroring the existing USD-M test
  pattern in `tests/test_adapters_sequence.py`.
- `adapters/binance_spot.BinanceSpotAdapter` — `trade` and
  `depth`/`depth@100ms` streams. `market_type="spot"` explicit on every
  event; canonical `exchange="BINANCE"` (not a separate exchange name) so
  P6's `(exchange, market_type, stream)` identity is the actual
  differentiator, per the task's own instruction. `t` (raw per-execution
  Trade ID) mapped to `trade_id`, never `a` (aggTrade's aggregate ID) --
  **aggTrade is not implemented**; whether it should also be collected is
  left as an open question (task's own §5), not guessed.
- `storage_layout` — `BINANCE_SPOT` registered with its own `spot_` prefix,
  isolated from USD-M futures' unprefixed streams. Namespace-isolation
  regression test added (`check_stream_namespace` refuses cross-declaration
  either direction).
- P6 integration: two tests using the real `causally_align`/`alignment_key`
  (not a reimplementation) prove Case A (exchange-early, receive-late ->
  NOT available) and Case B (exchange-late, receive-early -> available) from
  the task's §21, plus that a Spot and a same-exchange futures event never
  collide as P6 keys.
- **Mutation testing actually performed, not merely claimed:** (1) removed
  `market_type="spot"` from both `BinanceSpotAdapter` constructions --
  confirmed the market-type test failed with `"linear_perpetual"` instead of
  `"spot"`, then restored. (2) swapped `LocalBook`'s `"BINANCE_SPOT"`
  comparator for `BinanceSequenceComparator` (the futures rule) -- confirmed
  3 of the `LocalBook` tests failed, including one on the exact reason
  string (`"pu_missing"` instead of `"u_chain_broken"`) that only a futures
  comparator would produce, then restored. Both mutations and restorations
  are in this session's tool history; nothing was left mutated.
- `tests/test_binance_spot_adapter.py`: 29 tests. Full suite: **739 passed**
  (was 710 before this branch).

**Completed this pass (`run_binance_spot_collector.py` + `ReplayEngine` Spot
registration):**

- `run_binance_spot_collector.py` -- a dedicated, standalone live runner
  (same single-file style as `run_okx_collector.py`; imports nothing from
  `run_collector.py`). Wires: raw wire capture before parsing, REST
  snapshot capture with request/response lineage, `BinanceSpotAdapter` ->
  `LocalBook("BINANCE_SPOT")`, durable quality events on every state
  transition, `spot_`-prefixed storage via `storage_layout.venue_stream`.
  Own `RecoveryController` instance (`name="binance_spot_orderbook"`) --
  never shares a recovery budget with USD-M futures'.
- **Pending-snapshot handling, fixed in both live and replay.** The
  official Spot procedure's step 4 ("if every buffered diff has
  `u < lastUpdateId`, the snapshot cannot yet bridge") is not a failure --
  the next diff will straddle it. The first version of this runner treated
  `LocalBook.binance_snapshot()` returning `False` as a hard error
  unconditionally, which is wrong for this specific `last_reason ==
  "snapshot_ahead_of_buffer"` case: it spent the bounded recovery budget on
  a problem that resolves itself on the next diff. Fixed by checking
  `last_reason` and calling `retry_pending_snapshot()` after each buffered
  diff. **`ReplayEngine._handle_wire`/`_handle_snapshot` had the identical
  gap** (pre-existing, not introduced this session, and not specific to
  Spot -- USD-M futures replay has the same code path) -- found via this
  session's own replay tests, fixed for parity so live and replay treat a
  pending snapshot identically.
- `ReplayEngine` now accepts `venue="BINANCE_SPOT"`
  (`_ADAPTER_CLASSES["BINANCE_SPOT"] = BinanceSpotAdapter`). The
  replay-constructed snapshot event's canonical identity is `exchange=
  "BINANCE", market_type="spot"` (not `exchange="BINANCE_SPOT"` -- that
  would make a replayed snapshot compare unequal to every diff
  `BinanceSpotAdapter` itself produces for the same book, a bug caught by
  `test_spot_replay_includes_trade_events`'s market_type assertion during
  mutation testing).
- **Storage round-trip test** (`test_storage_round_trip_matches_in_memory_replay`):
  synthetic frames -> `RawCapture` -> `ParquetWriter` -> disk -> `ReplaySource.from_directory`
  -> `ReplayEngine` -> digest, compared against the equivalent in-memory
  replay. Digests match. A second round-trip test
  (`test_storage_round_trip_never_pulls_in_futures_rows`) proves a
  Spot-and-futures directory read isolates by venue namespace.
- **Malformed-frame fixture suite**: missing/non-integer `U`/`u`, malformed
  bid/ask price/quantity, missing trade id (documented as valid --
  `trade_id=None` -- not malformed, since the adapter only requires
  price/quantity), malformed price/quantity, unroutable stream, empty
  payload, control response. Each pinned to its actual `UnhandledReason`.
- **Bridge-boundary mutation test**: `binance_spot_snapshot_bridge` tested
  at exactly `lastUpdateId+1`, one event late, one event short, and a wide
  straddling event -- plus a direct pin that the futures formula (no `+1`)
  and the Spot formula disagree on the same event.
- **Causality tests**: snapshot eligibility is `response_receive_ts`
  (verified both at the runner level via the captured `RawRestRecord`, and
  in `ReplaySource.from_records` directly), never a value from inside the
  payload -- the Spot snapshot response carries no exchange timestamp at
  all, so there is nothing to substitute even by accident.
- **Duplicate/gap tests, corrected to the actual documented contract**: a
  resent diff is **not** silently accepted -- `SpotSequenceComparator` has
  no duplicate carve-out (an earlier session's own deliberate decision,
  re-confirmed here, not re-litigated), so a retransmitted event fails
  `U == prev.u+1` and is classified `SEQUENCE_GAP`. An initial draft of this
  test asserted the opposite before the actual comparator behaviour was
  checked; corrected to match verified behaviour rather than an assumption.
- `tests/test_binance_spot_collector.py`: 42 tests. Full suite: **781
  passed** (was 739 before this branch), both `pytest` and
  `pytest collector` invocations agree (G4 contract preserved).
- **Mutation testing performed** (in addition to the prior session's two):
  (3) stripped `market_type="spot"` from `BinanceSpotAdapter`'s trade
  event -- confirmed `test_spot_replay_includes_trade_events` failed with
  `"linear_perpetual"`, then restored. (4) swapped `LocalBook`'s
  `BINANCE_SPOT` comparator for `BinanceSequenceComparator` -- confirmed 2
  tests failed (`test_contiguous_update_stays_valid`,
  `test_a_resent_diff_is_treated_as_a_gap_not_silently_accepted`), then
  restored and reverified the full suite (781 passed). Both mutations and
  restorations are in this session's tool history.

**Not done in this pass -- explicitly, not silently:**
- **Live network verification: `LIVE-UNVERIFIED, ENVIRONMENT BLOCKED`.**
  This environment's egress allowlist does not include
  `stream.binance.com`/`api.binance.com` (consistent with every other
  venue's prior finding in this collector; not independently re-tested
  this session since no attempt was made to reach the live network -- see
  §21 of the originating task). Everything above is offline-verified only:
  unit tests, replay, and a storage round-trip through real `ParquetWriter`
  instances and real Parquet files on disk -- not a live WebSocket session.
- `docs/DATA_SUFFICIENCY.md` and other cross-referenced docs are not
  updated for Spot's existence yet.
- No 24-hour WebSocket lifecycle / reconnect-storm test specific to Spot
  beyond what `WebSocketClient`'s own venue-agnostic tests already cover.
- `feature_computer`-style derived microstructure features (OBI, spread,
  micro_price) are not computed for Spot -- `SPOT_ORDERBOOK_RAW_SCHEMA`
  stores raw canonical book state only, matching
  `BINANCE_ORDERBOOK_RAW_SCHEMA`'s precedent, not the older
  feature-computed `ORDERBOOK_SCHEMA`. Deliberate: inventing a
  feature-computation path for one venue outside its own verified phase
  was explicitly out of scope (task's own §20).
- Whether `aggTrade` should also be collected for Spot remains an open
  question, unchanged from the prior pass -- not decided here either.

### First-class instrument identity — **IMPLEMENTED, TESTED; not COMPLETE until merged (ancestry recorded on the PR)**

Base `main` @ `3be06f7`, 781 tests. New: `collector/collector/instrument.py`,
`docs/INSTRUMENT_IDENTITY.md`, `tests/test_instrument_identity.py` (71).

- Real defect found: `MarketStateEngine("BINANCE")` accepted Binance Spot and USD-M
  events into one state (`BinanceSpotAdapter.venue == "BINANCE_SPOT"` but its events
  carry `exchange="BINANCE"`). Engines can now be bound to an instrument and refuse others.
- `InstrumentId(exchange, market_type, instrument, native_symbol)`: strict, immutable,
  all four fields in equality; existing market-type vocabulary reused; `None` = unidentified
  (no UNKNOWN identity). A base-class hook stamps every adapter's events and refuses
  contradictions. OKX `index-tickers` and non-BTC `liquidation-orders` rows are deliberately
  unidentified.
- Alignment key is now `(exchange, market_type, instrument_key, stream)`; semantics unchanged.
  16 existing alignment assertions were updated for the new key shape (no test removed or weakened).
- Mutation-checked: dropping exchange / market_type / instrument / native_symbol from the key
  fails 6 / 7 / 5 / 5; unwiring the alignment key 4; disabling stamping 9; dropping the MarketState
  guard 1; resolving BINANCE_SPOT+perpetual rows to perp 1; stamping every OKX liquidation as BTC 1.

**Not claimed:** canonical derived streams have no explicit instrument column (raw layer does);
namespaces are single-instrument and replay does not cross-check a row's `symbol` against the
adapter; Bybit spot / COIN-M / inverse unregistered; no live verification.

### Phase E - live/replay identity parity - **IMPLEMENTED, TESTED; not COMPLETE until merged (ancestry recorded on the PR)**

Base `main` @ `3389c3b`, 964 tests. Two real defects found by tracing, plus one false comment:

| # | Defect | Root cause | Fix |
|---|---|---|---|
| E1 | REST depth snapshot events carried **no identity**; the snapshot row persisted beside diff rows had a null `instrument_key` (accidentally lost, not legitimately unidentified) | snapshot event hand-built at 3 sites (replay, Spot runner, USD-M runner), bypassing the adapter stamp | one `snapshot_event()` on each Binance adapter, used by all three sites |
| E2 | Replayed Binance OI events were **unidentified**; live's were identified | replay called `normalize_binance_oi` without `symbol` | replay passes the adapter's native symbol |
| E3 | Bybit runner comment claimed the adapter attaches no identity | stale; `__init_subclass__` stamping is active | comment corrected; runner now cross-checks the event's identity and raises on contradiction |

An existing test (`test_live_and_replay_produce_identical_canonical_events_from_the_same_body`)
built its "live" side without `symbol`, mirroring replay's omission, so it passed while live != replay.
Its live side now uses `symbol=SYMBOL` as the runner does; no assertion was weakened.

`tests/test_phase_e_identity_parity.py` (19): full identity compared field by field for every
non-book event type on all four venues (live vs replay), snapshot and diff events fed to the book
engine, OKX legitimately-unidentified vs accidentally-lost, a structural guard against hand-built
book events, and raw -> adapter -> replay -> alignment end to end. Mutation-checked (all fail):
stamping removed 23, Bybit loses identity 10, Spot -> USD-M 9, market_type out of equality 6,
replay snapshot identity != live 2, OKX index-tickers faked 3, OI symbol dropped 2, snapshot
constructors unstamped 3+3, Bybit guard removed 1.

**Not verified this phase:** Phase D read-path cases 1-14 and mutations G/H were not re-run (covered by
Phase D's own tests); Phase F (alignment audit) not started beyond the end-to-end test; no live verification.
