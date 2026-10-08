# MASTER P0 AUDIT REPORT

Repository: isfhankhan9595-bot/btc-data-collector
Current main audited: `385a6e616fa8c86fb1ae17b7a8d6d62e1e6a76ae`
Audit branch: `master-audit-2026-10`
This report spans two sessions. Session 1 produced the full P0-1..P0-12 ledger,
PR inventory, and CI triage. Session 2 (this one) did a genuine line-by-line
audit of `collector/segment_dedup.py` (P0-4) and re-confirmed, by diffing file
history rather than re-reading every line, that the files underlying the
P0-2/6/7/8/9 findings have not changed since they were last read directly.
Tags used throughout: **VERIFIED** (I read the code or ran it this session or
a prior session in this same audit), **CARRIED FORWARD** (verified in a prior
session, file confirmed unchanged since), **INFERRED** (a reasoned judgment,
not a direct read), **NOT VERIFIED** (not checked).

No production code, tests, or PRs were modified. No commits landed anywhere
except this branch's two documentation files.

---

## 1. Executive summary

- Full suite on main: **1645 passed, 4 failed** (VERIFIED, run this session).
  Three of the four failures are stale tests, not production bugs (Section 16).
  The fourth is a sandbox-path artifact, also not a production bug.
- **No P0 is genuinely closed** in the sense of implemented + tested + merged +
  production-wired + live-verified. The furthest along are P0-6 and P0-9
  (merged, wired at the writer boundary, internally consistent) but neither
  has live-exchange verification.
- **The single highest-value finding this session:** P0-4's replacement dedup
  component (`SegmentDedupCoordinator`, PR #71, merged) is well-designed,
  internally fail-closed, and has 24 of its own tests — but it is **called by
  nothing in production**. `ExchangeAdapter.set_trade_dedup()` has exactly one
  caller in the entire repository: its own definition. Every live runner still
  falls back to the old unbounded `_seen_trade_ids` set by default. The
  integration points needed to wire it already exist on both sides
  (`ParquetWriter.__init__`'s `on_segment_published` callback parameter is
  already there and already shaped to accept
  `SegmentDedupCoordinator.on_segment_published`), so this is a precisely
  scoped, not a speculative, remaining task.
- Confirmed again this session: P0-2's WAL covers only `run_collector.py`
  (Binance USD-M); P0-3 is untouched on main (draft PR #76 only).

---

## 2. Master P0 ledger

Six-state columns: Implemented / Tested / Merged / Reachable from main /
Production-wired / Live-verified. Research-verified is called out separately
in prose where relevant, since only P0-5/6/7/9 bear on it.

| P0 | Impl | Test | Merged | Reachable | Prod-wired | Live-verified | Status |
|---|---|---|---|---|---|---|---|
| 1 WS decoupling | Y | Y | Y (#60,#66,#73,#77) | Y | Y | N | COMPONENT COMPLETE |
| 2 Quality WAL | Y | Y | Y (#69) | Y | **Partial — 1 of 4 runners** | N | PARTIAL |
| 3 Tiny files | Branch only | Unknown | **N** (#76 draft) | N | N | N | OPEN |
| 4 Trade dedup | Y | Y (24 tests) | Y (#71) | Y | **N — confirmed this session** | N | MERGED BUT INCOMPLETE |
| 5 Label horizon | Y | Y | Y (#58,#61,#65) | Y | Y | N/A (offline tool) | COMPONENT COMPLETE, one ordering risk |
| 6 Time contract | Y | Y | Y (#59) | Y | Binance USD-M assembler only | N | COMPONENT COMPLETE |
| 7 OI/liquidation | Y | Y | Y (#62,#64) | Y | Binance USD-M only | N | COMPONENT COMPLETE |
| 8 Book truth | Y | Y | Y (#68) | Y | INFERRED Y | N | COMPONENT COMPLETE |
| 9 Numeric precision | Y | Y | Y (#72) | Y | Y at writer boundary, all 4 venues | N | COMPONENT COMPLETE |
| 10 Receive-time semantics | Y | Y | Y (#70) | Y | Y, all four runners | N | COMPONENT COMPLETE |
| 11 Timestamp resolution | Y | Y | Y (#74) | Y | Y (raw capture + replay only) | N | COMPONENT COMPLETE |
| 12 Deployment | Y | Y | Y (#75) | Y | N/A | **N — not verified on EC2** | COMPONENT COMPLETE, offline only |

Changed since the prior session's ledger: P0-4 moved from "two unmerged
drafts" to "merged, unwired" (#71 merged); P0-11 moved from "open PR" to
"merged" (#74); P0-12 moved from "template only" to "merged, offline-verified"
(#75); P0-1 gained two more merged follow-ups (#73, #77), with #79 still an
open draft.

---

## 3. P0-4 deep audit (this session's primary task)

**File:** `collector/collector/segment_dedup.py`, 213 lines, read in full.

**Design, VERIFIED by direct reading:**
- `SegmentDedupIndex` wraps a SQLite file with two tables: `seen(identity_key
  PRIMARY KEY)` and `reconciled_segments(segment_key PRIMARY KEY,
  identity_count)`, both `WITHOUT ROWID`. `journal_mode=WAL`,
  `synchronous=FULL`.
- `commit_segment()` wraps the identity inserts and the reconciled-marker
  insert in one `BEGIN IMMEDIATE ... COMMIT` transaction, with an explicit
  `ROLLBACK` on any exception. The two facts ("these identities are seen" and
  "this segment is reconciled") become durable atomically together — there is
  no window where one is true and the other isn't.
- `SegmentDedupCoordinator` is the per-stream RAM authority for
  *not-yet-published* segments: `check_and_admit()` checks RAM first
  (`_admitted`, `_pending_index`), then falls through to the SQLite `seen`
  table. `note_written()` moves an admitted key into
  `_pending_by_token[token]`, scoped to the exact segment it landed in.
  `on_segment_published()` re-reads the just-published file from disk,
  commits its identities to SQLite, and only then releases that segment's
  keys from RAM.
- `startup_reconcile()` globs every `*.seg` file in a stream directory and
  indexes any that lack a `reconciled_segments` marker — this is the recovery
  path for "crashed after publish, before dedup commit."
- Every SQLite failure raises `DedupStateError`; nothing in this file maps a
  failure to "treat as new" or "treat as duplicate."

**Answering the 17 required questions:**

1. **Schema:** as above. VERIFIED.
2. **Transaction boundaries:** one transaction per segment, covering both
   tables. VERIFIED correct.
3. **Startup/restart recovery:** `startup_reconcile()` closes the
   publish-to-commit gap. VERIFIED by reading; not exercised against a real
   crash in this session (that is what the component's own 24 tests are for,
   which I did not re-run).
4. **Unfinished-segment recovery:** deliberately out of scope by design — a
   segment still in `.tmp` form, never renamed to `.seg`, is never globbed,
   never indexed. If it's later orphan-recovered and dropped (the existing
   `ParquetWriter` DATA_DROP path), a redelivery of that trade is correctly
   treated as new, since it was never durable. VERIFIED as a documented,
   reasoned choice (module docstring), not an oversight.
5. **Crash before canonical publication:** same reasoning as #4 — nothing
   durable exists yet, so no false suppression risk and no incorrectly
   skipped duplicate (the row isn't canonical at all).
6. **Crash after publication, before dedup commit:** closed by
   `startup_reconcile()` (#3).
7. **Crash after dedup commit:** fully durable; no issue. VERIFIED.
8. **Duplicate trade behavior:** `check_and_admit()` checks RAM then disk, in
   that order, both checked before admitting. VERIFIED logically sound.
9. **Legitimate-trade suppression risk:**
   - Identity key is a length-prefixed 5-tuple encoding
     `(exchange, market_type, instrument_key, stream, trade_id)` — this
     avoids the classic delimiter-collision bug where two different tuples
     could concatenate to the same string.
   - `trade_id is None` rows are never deduplicated (checked in
     `adapters/base.py`, consistent with this module's docstring).
   - **One real, open risk, not previously flagged:** the `seen` table is
     explicitly "non-evicting" (the class's own docstring). If a venue ever
     reused a `trade_id` for a genuinely different trade far enough apart in
     time that it's a different event, this design would falsely suppress
     it forever, with no TTL or pruning. I did not find evidence either way
     on whether any of the four venues' trade IDs are known to repeat over
     long time horizons — NOT VERIFIED, flagged as an open question rather
     than a confirmed defect.
10. **SQLite corruption/partial-write:** every `sqlite3.Error` is caught and
    re-raised as `DedupStateError`. VERIFIED fail-closed, matching the
    module's own stated invariant. What a live runner would actually do upon
    receiving this exception is NOT VERIFIED, since no runner calls this
    component at all today.
11. **Concurrent access:** `check_same_thread=False` plus WAL journal mode
    plus `BEGIN IMMEDIATE` (avoids SQLite's deferred-transaction upgrade
    race). Reasoned as correct for one coordinator instance used from
    multiple threads within one process. Whether two separate processes are
    ever meant to share one SQLite file is NOT VERIFIED (no wiring exists to
    check against).
12. **Segment rollover:** `note_written()`'s docstring requires it be called
    "after hour-rollover handling and before the append," which correctly
    scopes an identity to the exact segment/token it lands in even across an
    hour boundary. This is a contract on the *caller*; since there is no
    caller in production, whether it is honored in practice is NOT VERIFIED
    beyond the component's own test suite (not re-run this session).
13. **Memory boundedness:** `ram_identity_count` sums exactly the
    not-yet-published-segment structures; everything for a token is cleared
    the moment that segment is published. **RAM growth is bounded by
    (open segments × rows per open segment), not by total historical trade
    count** — this is the P0-4 guarantee, and it is correctly implemented.
    VERIFIED. Contrast with #9: RAM is bounded, but the on-disk SQLite index
    is not.
14. **Replay contamination:** nothing in this file touches `ReplayEngine`.
    Replay builds a fresh adapter per run (confirmed in a prior session),
    and `_trade_dedup` defaults to `None` on a fresh adapter, so replay
    cannot inherit a live coordinator's RAM state even once this is wired —
    there is no shared-instance path between live and replay in this design.
    VERIFIED by reading both sides.
15. **Wired into every applicable production runner? NO.** VERIFIED this
    session: `grep -rn "SegmentDedupCoordinator\|segment_dedup"
    run_*.py` returns nothing.
16. **Is `set_trade_dedup` invoked anywhere?** VERIFIED: a repo-wide grep for
    `set_trade_dedup` finds exactly one match — its own `def` line in
    `adapters/base.py`. No caller exists anywhere, including in
    `tests/test_segment_dedup.py` (24 tests, all exercising the coordinator
    directly, none going through `ExchangeAdapter.set_trade_dedup`). The
    adapter-to-coordinator integration seam is therefore **untested as well
    as unwired**.
17. **Integration semantics match the writer's publication boundary?**
    VERIFIED compatible: `ParquetWriter.__init__` already accepts an
    `on_segment_published: Optional[Callable[[Tuple[str, int], Path],
    None]]` parameter and already calls it at the exact point
    (`self.on_segment_published((self.current_hour, self._seq), final)`)
    that `SegmentDedupCoordinator.on_segment_published`'s signature expects.
    **The writer-side hook already exists and is already shaped correctly
    for this exact purpose.** No runner passes a coordinator's method as
    this argument today.

**Conclusion for P0-4:** the component satisfies NO SILENT TRADE LOSS (by
design: unpublished rows are never falsely treated as duplicates on restart),
NO UNBOUNDED RAM (verified bounded), and fails closed rather than silently
guessing on any SQLite error. It does **not** satisfy NO UNBOUNDED DISK GROWTH
(explicitly non-evicting) and CANNOT be credited with NO CROSS-RESTART
DUPLICATION or NO FALSE SUPPRESSION **in production**, because nothing in
production calls it — those properties are proven only for the component in
isolation, not for the system. The remaining work is narrow and concrete:
construct one `SegmentDedupIndex`/`SegmentDedupCoordinator` per trade stream
in each of the four runners, call `adapter.set_trade_dedup(coordinator)`,
call `coordinator.startup_reconcile(stream_dir)` before ingestion resumes,
and pass `coordinator.on_segment_published` into the trade `ParquetWriter`'s
existing `on_segment_published=` parameter.

---

## 4. P0-2, P0-6, P0-7, P0-8, P0-9 — re-confirmation

Per the continuation instructions, I checked `git log` for every file each of
these findings rests on. None has changed since the commit that last touched
it (listed below), so I am treating the prior session's direct reads as
**CARRIED FORWARD**, not stale, while being explicit that this session did
not re-read every line.

**P0-2** (`collector/collector/quality_wal.py`, last touched `dc70287`,
reachable from main): I did re-read this session, since its last commit was
unfamiliar. That commit ("WAL reads real checkpoint state, not start_seq
inference") fixed a self-documented real bug: checkpoint state used to be
*inferred* from `start_seq` rather than read from a durable
`checkpoint.json`, which could silently no-op a checkpoint call in a specific
recovery-ordering case. The fix now reads the real file. This is reassuring
evidence that the WAL has already been through one post-merge correctness
hardening pass, not alarming. **Unchanged finding:** `quality_wal`/
`QualityEventWAL` appears in only `run_collector.py` among the five
runner-like entry points (`run_collector.py`,
`run_binance_spot_collector.py`, `run_bybit_collector.py`,
`run_okx_collector.py`, `run_okx_capture.py`) — CARRIED FORWARD, re-grepped
this session to confirm the count (17 references, same as before).

**P0-6** (`collector/pipeline/dataset_assembler.py`, last touched by P0-9's
`28a33fa`, which only affects numeric columns, not the causal-join logic):
CARRIED FORWARD. Availability is `local_timestamp` (receive time); order
book, mark price, and OI join backward via `merge_asof` with explicit
staleness tolerances (500ms/5000ms/5000ms); trades and liquidations are
ceiling-binned so a later event cannot enter an earlier row; a missing
`local_timestamp` falls back to processing time and sets a `*_time_unknown`
flag rather than failing closed.

**P0-7** (same file): CARRIED FORWARD. OI has explicit gap flags; liquidation
has a day-level `liquidation_stream_available` flag but no intra-day
granularity; liquidation dedup is a heuristic on
`(exchange_ts, side, price, qty)`, which could collapse two genuinely
distinct liquidations sharing all four fields. **Still open, unresolved
since last session:** trade and liquidation absence (an empty bin, a missing
file, or a mid-day outage) all produce zero-valued columns with no
availability flag for trades specifically — a real research-integrity gap,
not fixed since it was first found.

**P0-8** (`collector/collector/feature_computer.py`, last touched by P0-9's
`28a33fa` and P0-8's own `441700e`): CARRIED FORWARD at the level of
"no-padding logic exists and is commented as deliberate." Not independently
re-run against a live book this session or the last.

**P0-9** (`collector/collector/numeric.py`, `adapters/bybit.py`, both
untouched since `28a33fa`, the P0-9 merge itself): CARRIED FORWARD.
`_as_float()` in the Bybit adapter actually returns `Decimal` via `dec()`
despite its name; `column_value()` in `parquet_writer.py` derives every
`_exact` column from the Decimal field, never from the float, so exact text
cannot regress to a float-rounded value. Trade IDs above 2^53 through
storage and replay remain **NOT VERIFIED** in either session.

---

## 5. CI / test health

Full suite, this session, on `385a6e6` (unmodified): **1645 passed, 4
failed.**

| Test | Root cause, verified this session | Real bug? |
|---|---|---|
| `test_p0_12_systemd_deployment.py::test_working_directory_is_this_repo_not_the_trading_bot_or_a_placeholder` | Asserts the checkout directory's basename equals `"btc-data-collector"`. This sandbox's checkout is at `/home/claude/repo`, so `PurePosixPath(wd).name` is `"repo"`. | **No — sandbox path artifact**, not a production bug. |
| `test_p0_12_systemd_deployment.py::test_stop_timeout_outlasts_the_p0_1_websocket_drain` | Regexes for a literal `timeout=<number>` in `websocket_client.py`. Current code reads `timeout=self.shutdown_drain_timeout_s` (now a named, configurable attribute, set to `30.0`), not a literal. | **No — stale regex.** PR #78 (open) is titled exactly "fix drain-timeout regex after P0-1/P0-11 refactor," confirming this is already a known, tracked issue. |
| `test_websocket_client.py::test_on_message_receives_the_arrival_timestamp_not_the_processing_timestamp` | Patches `wsc.time.time`. Current code stamps the receive time via a `ReceiveStamp` object (`wall_ns`/`wall_ms`) introduced by P0-11, not `time.time()` directly, so the patch no longer intercepts anything and the real wall clock leaks through. | **No — stale mock, post-P0-11.** |
| `test_websocket_client.py::test_receive_timestamp_also_holds_for_handlers_that_take_no_connection_id` | Same root cause as above. | **No — stale mock.** |

**Conclusion: CI is red, but none of the four failures indicates a
production regression.** Three are a known, already-flagged test staleness
from the P0-1/P0-11 refactor (PR #78 exists to fix the regex one); the fourth
is purely this sandbox's checkout path.

---

## 6. Critical remaining flaws

| ID | Severity | Location | Evidence | Impact | Fix required | Regression risk | Test required |
|---|---|---|---|---|---|---|---|
| C1 (carried forward) | High, research | `pipeline/dataset_assembler.py` | No per-bin trades-availability flag | An outage looks like a quiet market | Add a `trades_available`/coverage flag | Low | Drop a trade window, assert the flag |
| C2 (this session) | High, data-integrity | All four `run_*.py` | `set_trade_dedup` has one caller: its own definition | Every live runner still relies on the unbounded, in-process `_seen_trade_ids` set despite a tested replacement existing | Wire `SegmentDedupCoordinator` using the already-compatible `ParquetWriter.on_segment_published` hook | Low — the seam exists and is additive | Restart-duplication and crash-around-publish tests, run against the real runner wiring, not just the component in isolation |
| C3 (carried forward) | High, ops | All quality-event writers | `segment_rows=1` everywhere; P0-3 fix (#76) is an unmerged draft | File-count amplification on sustained quality events | Land and wire a P0-3 batching design compatible with the WAL's per-event-fsync assumption | Medium (coupled to P0-2 scope) | Burst/sustained quality-event file-count test |
| C4 (new, moderate-to-high) | Moderate | `segment_dedup.py` `seen` table | Explicitly "non-evicting" | Unbounded SQLite disk growth over process lifetime; a reused `trade_id` far apart in time would be falsely suppressed forever | Decide and document a retention/eviction policy, or confirm venues never reuse trade IDs | Low to design, none to existing behavior since unwired | A long-duration disk-growth test once wired |

---

## 7. Production readiness: 30/100

Unchanged from the prior session's assessment, re-affirmed with sharper
evidence: the P0-4 component existing and tested does not change production
behavior, since it is proven unwired this session rather than merely
suspected. Quality-event durability differs by venue, P0-3 is undone, and
deployment has zero live/EC2 verification (`test_working_directory...`'s
failure in this sandbox makes clear this test has never actually run against
the real target path either).

## 8. Research readiness: 45/100

Unchanged. The causal-join design (P0-6/7) is sound where it exists, but it
covers only Binance USD-M, and the trades-availability gap (C1) is confirmed
still open.

---

## 9. Top 5 next actions, in dependency order

1. **Wire P0-4 into all four runners** (construct index/coordinator per
   stream, call `set_trade_dedup`, call `startup_reconcile` before
   ingestion, pass the coordinator's publish hook into `ParquetWriter`), then
   test restart-duplication and crash-around-publish against the real
   wiring, not just the component.
2. **Decide the P0-4 disk-retention policy** (C4) before or alongside #1,
   since it changes the index's long-run storage contract.
3. **Land P0-3** with explicit reference to the WAL's per-event-fsync
   assumption, so batching doesn't silently weaken durability.
4. **Add the trades/liquidation availability flags** (C1) to the assembler.
5. **Fix or merge PR #78** (the stale drain-timeout regex) and update the two
   stale `test_websocket_client.py` mocks to patch the P0-11 `ReceiveStamp`
   path instead of `time.time`, to get CI genuinely green rather than
   red-for-known-reasons.

---

## 10. What remains genuinely unverified (not to be treated as proven in a
future session without saying so)

- `SegmentDedupCoordinator`'s own 24 tests were not re-run or read this
  session — only the source.
- Trade IDs above 2^53 through storage and replay, all four venues.
- Whether any venue's `trade_id` is ever reused across a long time horizon
  (bears directly on C4).
- P0-8 order-book correctness beyond reading the no-padding comments.
- Everything EC2/live-exchange: no P0 has this.
- PR #71's own git history/diff review (I read the merged result, not the
  review discussion or intermediate commits).
- The early phase-* PRs (#1-#55) are trusted from the PR inventory and prior
  sessions' spot checks, not re-diffed this session.
