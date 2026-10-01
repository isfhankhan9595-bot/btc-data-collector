# P0-4: segment-granularity exact trade dedup — final architecture

Supersedes the per-trade wiring PR #57 explored and rejected (see
`docs/P0_4_BOUNDED_TRADE_DEDUP.md` §11 for that proof). This document
describes what is **implemented** on this branch.

## Canonical source of truth

A **published Parquet segment** (`ParquetWriter._close_segment`'s
fsync+`os.replace` result) is the only thing ever treated as durable.
Nothing before that point — buffered rows, a flushed-but-unclosed `.tmp`
writer — is ever attributed to the persistent dedup index.

## What lives where

- **RAM**: `SegmentDedupCoordinator` holds identities for the segment(s)
  not yet published: `_admitted` (checked, not yet written to any row) and
  `_pending_by_token`/`_pending_index` (written into an open segment, keyed
  by `(hour, seq)`). Bounded by the *open segment's* row count
  (`segment_rows`, default 5,000) — proven in
  `test_ram_is_bounded_by_open_segment_not_by_process_lifetime` (2,000
  admissions, peak RAM identity accounting ≤ 2×`segment_rows+1`, never
  process-lifetime).
- **Disk**: `SegmentDedupIndex`, a SQLite table (`seen`) plus a
  `reconciled_segments` marker table. Never evicted — disk grows with the
  lifetime trade count, exactly as documented as an accepted tradeoff in
  `P0_4_BOUNDED_TRADE_DEDUP.md` §12 (`disk_size_bytes()` makes this
  observable).

## When an identity becomes durably deduped

Only inside `SegmentDedupIndex.commit_segment`, one SQLite transaction that
inserts every identity a published segment contains **and** its
`reconciled_segments` marker atomically — `BEGIN IMMEDIATE` /
`INSERT OR IGNORE` × N / `INSERT` marker / `COMMIT`, with `ROLLBACK` on any
failure (`test_state_E_crash_during_index_transaction_is_atomic_and_rerunnable`).
RAM identities for that segment are released **only after** this commit
returns (`test_release_happens_only_after_the_index_commit`).

## Segment publication semantics

`ParquetWriter.on_segment_published(token, path)` is called after the
segment is fully durable (fsync + rename done) —
`test_hook_runs_only_after_the_segment_is_durably_published` proves the
`.tmp` file no longer exists and the final path does at hook time. If the
hook raises, the writer marks itself failed and every subsequent `write()`
raises immediately (`test_hook_failure_fails_closed_on_the_next_write`) —
never silently continues with an uncertain dedup state. `write(record,
bind=...)`'s `bind` callback fires with the token *after* any hour
rollover and *before* the append, so attribution is exact even across a
rollover mid-call (`test_hour_rollover_attributes_identity_to_the_segment_that_receives_it`).

## Startup reconciliation

`SegmentDedupCoordinator.startup_reconcile(stream_dir)`, run before
ingestion resumes: for every `*.seg` file without a `reconciled_segments`
marker, read it back and commit its identities. Idempotent
(`test_repeated_reconciliation_is_idempotent`) and fully derivable — a lost
index directory is rebuilt from the published segments alone
(`test_missing_index_is_rebuilt_from_published_segments`). An unreadable
published segment fails closed at startup, not silently
(`test_unreadable_published_segment_fails_closed_at_startup`).

## Crash windows (proven, not merely argued)

| Window | Outcome | Test |
|---|---|---|
| Admitted, nothing written | Redelivery accepted | `test_state_A_pending_only_is_accepted_again_after_restart` |
| Written into an unpublished `.tmp` | `.tmp` discarded (`DATA_DROP`, unchanged existing contract), identity never indexed, redelivery accepted | `test_state_B_unpublished_tmp_segment_is_dropped_and_identity_not_indexed` |
| Published, index commit not yet run | Startup reconciliation indexes it; redelivery suppressed | `test_state_C_published_but_index_not_committed_is_reconciled_at_startup` |
| Published, index committed | Idempotent, no dup, no loss | `test_state_D_committed_then_crash_restart_is_idempotent` |
| Crash mid-transaction | Atomic rollback, rerunnable | `test_state_E_crash_during_index_transaction_is_atomic_and_rerunnable` |
| Index unavailable/corrupt | `DedupStateError`, never "new" or "duplicate" | `test_state_F_*` |
| Duplicate arrives in the post-publish/pre-commit window | Still-live RAM entry suppresses it | `test_state_G_duplicate_in_post_publication_pre_index_window_is_suppressed` |

## Failure policy

`DedupStateError` on any SQLite failure (open/insert/lookup/reconcile) or
unreadable published segment. Never mapped to "treat as new" or "treat as
duplicate" — mutations G and H (map lookup errors to `False`/`True`) are
each caught by a dedicated test.

## Replay semantics

`ReplayEngine` constructs its own `BinanceAdapter()`/etc. with
`_trade_dedup` at its default `None` — a live run's `SegmentDedupCoordinator`
is never installed into replay. `test_default_adapter_and_replay_do_not_inherit_persistent_live_state`
proves a live run's durable index does not suppress an identical trade in
an independent replay of the same wire frame, and that two independent
`ReplayEngine.run()` calls over the same frame are deterministic (same
event count).

## Multi-stream / multi-venue identity

Unchanged five-part identity, now length-prefix encoded (structurally
collision-free, not delimiter-joined) via `dedup_identity_key`. Exchange,
market_type, instrument, and stream isolation each individually proven in
`test_identity_isolation_across_every_component`.

## Production wiring status

**`adapters/base.py`'s default behavior is unchanged** — `_trade_dedup`
defaults to `None`, and in that state `_dedupe_trades` uses the exact same
lifetime `_seen_trade_ids` set as before this branch
(`test_adapter_with_backend_never_touches_the_lifetime_set` proves the
opposite: *with* a backend installed, the lifetime set stays empty). Wiring
a `SegmentDedupCoordinator` into a live runner's `CollectorApp.__init__`
(constructing a `SegmentDedupIndex`, passing `on_segment_published` to each
`ParquetWriter`, calling `startup_reconcile` before the websocket connects,
and threading `bind=` through every `writer.write(...)` call site) was
**not done in this session** — each of the four runners has its own
call-site shape (`run_collector.py`, `run_bybit_collector.py`,
`run_binance_spot_collector.py`, `run_okx_collector.py`), and wiring all
four with equal rigor, plus verifying no other consumer of `ParquetWriter`
depends on the previous no-`bind` `write()` signature, is scoped as the
concrete next step rather than rushed here.

## What remains unverified

- Target-EC2 throughput/latency for `commit_segment` (one transaction per
  segment close, i.e. every ~30s/5,000 rows — a small fraction of the
  per-trade rate P0-4's earlier sandbox benchmark measured). **NOT
  VERIFIED** against real hardware.
- The four live runners are not yet wired to this coordinator.
- Concurrent-process access to one `SegmentDedupIndex` file (single-writer
  design; each runner owns its own stream's index file, so cross-process
  sharing was never intended and is not tested here).
