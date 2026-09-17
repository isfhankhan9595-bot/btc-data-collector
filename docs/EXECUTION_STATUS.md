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
| D9 | `scripts/replay_test.py` is not a replay engine. It loads a derived Parquet file and asserts bounds on columns. There is no recorded-clock replay, no raw event source, no determinism check. | 8 |
| D10 | No raw wire capture layer. `binance_orderbook_raw` stores normalised levels, not exact payloads; there is no connection id, no REST request/response lineage. Deterministic replay is not currently possible from stored data. | 6 |
| D11 | OKX adapter declares `trades`, `mark-price`, `index-tickers`, `open-interest`, `funding-rate`, `liquidation-orders` in `channel_event_types` but `normalize()` only implements `books`. Everything else silently returns `[]`. | 14 |
| D12 | Bybit adapter merges ticker deltas into shared `_ticker_state` and emits merged values without marking which fields were carried forward. Staleness is not observable. | 13 |
| D13 | WebSocket reconnect backoff has no jitter; no maximum retry cap; no recovery deduplication. | 23 |
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

### Phases 2+ — NOT STARTED

Remaining scope is tracked in the defect table above (D9–D14). No later phase
may be marked complete on the strength of a plan.
