# Master Audit Progress

AUDIT STATUS: SESSION 2 COMPLETE. P0-4 deep-verified line-by-line. P0-2/6/7/8/9
re-confirmed unchanged since prior direct reads (P0-2's WAL file re-read in
full this session since its last commit was unfamiliar; P0-6/7/8/9's files
confirmed untouched by diff, not re-read line by line). Audit is still NOT
EXHAUSTIVE -- see "Open questions" below and MASTER_AUDIT_REPORT.md Section 10.

CURRENT MAIN SHA: 385a6e616fa8c86fb1ae17b7a8d6d62e1e6a76ae (confirmed unchanged
across both sessions)
AUDIT BRANCH: master-audit-2026-10
LAST COMPLETED PHASE: P0-4 deep verification (all 17 required questions
answered with code evidence); P0-2 WAL checkpoint-fix commit read; CI failures
all four root-caused individually
CURRENT PHASE: none in progress; session closed out cleanly

P0S VERIFIED THIS SESSION (direct code reading): P0-4 (full,
`collector/collector/segment_dedup.py`, all 213 lines, plus confirming
`ParquetWriter`'s compatible `on_segment_published` hook and the zero-caller
status of `set_trade_dedup` repo-wide), P0-2 (re-read `quality_wal.py` in
full after finding an unfamiliar commit, `dc70287`)

P0S CARRIED FORWARD, CONFIRMED UNCHANGED BY FILE HISTORY (not re-read
line-by-line this session): P0-6, P0-7, P0-8, P0-9 -- `git log` on each
relevant file shows no commit since the one that was read in a prior session
(`28a33fa` for the P0-9-touched files; `dataset_assembler.py`'s P0-6/7 logic
untouched by that same commit, which only affects numeric columns)

P0S STILL NOT INDEPENDENTLY VERIFIED AT ALL WITHIN THIS AUDIT BRANCH: P0-1's
and P0-5's headline findings (shutdown isolation; manifest-publish ordering)
were verified in sessions prior to this master-audit-2026-10 branch existing,
and carried forward from that earlier work, not re-verified within either of
this branch's own two sessions.

PRS VERIFIED: full inventory (#1-#79) pulled from GitHub API in session 1,
unchanged in session 2 (no new fetch performed this session; if resuming,
re-fetch first since main may have moved).

FILES AUDITED THIS SESSION: `collector/collector/segment_dedup.py` (full),
`collector/collector/adapters/base.py` (set_trade_dedup call-site check),
`collector/collector/parquet_writer.py` (on_segment_published hook signature
check only, not full read), `collector/collector/quality_wal.py` (full,
re-read), `tests/test_segment_dedup.py` (existence + test count only, NOT
read or re-run), `tests/test_p0_12_systemd_deployment.py` and
`tests/test_websocket_client.py` (the 4 failing tests specifically).

KEY FINDINGS THIS SESSION: P0-4's SegmentDedupCoordinator is well-designed,
fail-closed, RAM-bounded, and has a ready-made, already-compatible
integration point on the ParquetWriter side -- but has literally zero
production callers (C2 in the report). The SQLite `seen` table is explicitly
non-evicting, an unbounded-disk-growth risk not previously flagged (C4). All
4 current CI failures are stale tests or a sandbox artifact, not production
regressions -- documented individually with root cause.

OPEN QUESTIONS:
- Whether any of the four venues reuse trade_id over long time horizons
  (determines how serious C4 really is).
- `test_segment_dedup.py`'s 24 tests were not read or re-run -- only their
  existence and count were confirmed.
- P0-1 and P0-5 have not been re-verified within this audit branch's two
  sessions; their status rests on pre-audit-branch session findings.
- Early phase-* PRs (#1-#55) still not re-diffed, only trusted from the PR
  inventory.

NEXT EXACT ACTION: if continuing, re-fetch and check whether main has moved
past 385a6e6 first. If unchanged, the highest-value remaining work is (a)
reading `tests/test_segment_dedup.py` to confirm the component's own test
suite actually exercises crash/restart scenarios rather than only happy-path
transactions, and (b) a fresh line-by-line pass of P0-1 and P0-5 within this
audit branch specifically, since both currently rest on evidence from before
this branch existed.

LAST COMMIT SHA: (recorded after this commit lands on master-audit-2026-10)
