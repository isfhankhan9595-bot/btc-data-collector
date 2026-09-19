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

### Phases 10+ — market state, features, events — **NOT STARTED**

See `docs/DATA_SUFFICIENCY.md` for which events are feasible on Binance-only
data today (roughly two-thirds) and which are blocked on data that has never
been collected (cross-exchange, spot/perp basis).
