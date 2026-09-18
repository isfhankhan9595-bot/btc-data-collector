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
| D11 | OKX adapter declares `trades`, `mark-price`, `index-tickers`, `open-interest`, `funding-rate`, `liquidation-orders` in `channel_event_types` but `normalize()` only implements `books`. Everything else silently returns `[]`. | 14 |
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

#### Environment blocker (hard)

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

### Phase 8 — Bybit live client — **NOT STARTED**

Adapter exists and parses its 4 declared channels; nothing ingests from it, so
Bybit contributes no data. Same environment blocker applies to live
verification. The `on_open` / `keepalive` / `control_frames` hooks added in
Phase 7 are the generic foundation this needs.

### Phases 9+ — market state, features, events — **NOT STARTED**

See `docs/DATA_SUFFICIENCY.md` for which events are feasible on Binance-only
data today (roughly two-thirds) and which are blocked on data that has never
been collected (cross-exchange, spot/perp basis).
