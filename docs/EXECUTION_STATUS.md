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
| D14 | `binance_snapshot_bridge` and the USD-M sequence rules are implemented from assumption and have not been re-verified against current official Binance documentation. | 4 |

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
