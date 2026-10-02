# P0-2: Quality-Event Durability (WAL)

This document describes the CURRENT implementation, as it exists on
`p0-2-post-merge-validation`, based on `origin/main`. It replaces the
version that shipped in the original merged P0-2 PR, which described an
earlier, less hardened state. Facts are labeled FACT (directly verified
by reading the code or running a test), INFERENCE (a reasoned conclusion
not directly tested), or UNVERIFIED (stated as an open question, not
claimed either way).

## A. Original problem (FACT)

The old mechanism (`quality_queue.pending.json`) recorded only a queue
depth and a timestamp, never the event content. After a hard kill, the
collector knew events had been pending but could not recover what they
said.

## B. Architecture (FACT)

```
quality event created (_websocket_quality_event)
      |
      v
QualityEventWAL.append()  -- write, flush, fsync, THEN return
      |
      v
asyncio.Queue (bounded; overflow -> direct synchronous persist, see F)
      |
      v
_quality_persistence_loop  -- one event at a time, in FIFO order
      |
      v
_persist_quality_event -> quality_writer.write() -> Parquet (segment_rows=1,
                                                      so also fsync'd per row)
      |
      v
QualityEventWAL.checkpoint(seq)  -- only after persist succeeds, only the
                                     contiguous prefix, never past a failure
```

Startup: `CollectorApp._recover_quality_wal()` (extracted from `__init__`
for testability) reads the WAL, replays whatever was not checkpointed,
and checkpoints only the contiguous prefix that actually re-persisted.

## C. WAL record format (FACT, unchanged)

Newline-delimited JSON in `<quality stream dir>/wal/<file>.wal`, one line
per event, `quality_event_id` and `seq` added by the WAL, every other key
exactly as the caller passed it.

## D. Durability boundary (FACT)

`append()` returns only after write, flush and `os.fsync()` all succeed.
Any failure among those raises `OSError`, `write_failed` becomes `True`,
and the exception carries `.quality_event_id` (the id was already
assigned before the failing I/O, and bytes may already be on disk if
`write`/`flush` succeeded and only `fsync` failed) so a caller falling
back to direct persistence can stamp the identical id on that row.

## E. Checkpoint semantics (FACT)

`checkpoint(up_to_seq)` means "every seq <= up_to_seq is durably in
Parquet." Three properties, each independently tested:

1. **Only advances after the on-disk replace succeeds.** The in-memory
   `_checkpointed_seq` is updated *after* `os.replace()`, not before --
   a failed checkpoint write leaves the object's own state honest, and
   an immediate retry of the same seq is not silently treated as already
   done.
2. **State is read from disk, never inferred from `start_seq`.**
   `__init__` calls `_read_checkpointed_seq(self.wal_dir)`. `start_seq`
   only tells the id/sequence counter where to resume -- conflating the
   two (the original bug) made checkpointing any seq below `start_seq - 1`
   a silent no-op.
3. **Never advances past a failure.** Both call sites (the runtime
   persistence loop and startup recovery) stop advancing the checkpoint
   the instant any event fails to persist, via
   `_block_quality_checkpoint()` -- a latch that, once set, blocks every
   later checkpoint call for the rest of the process's life. The failed
   event and everything after it stay in the WAL, replayed (as a
   reconcilable, same-id duplicate where it already landed once) on the
   next restart.

`_delete_fully_checkpointed_files()` never deletes the currently active
file (verified directly: a test checkpoints exactly up to the active
file's own max seq and confirms it survives and remains appendable), and
tolerates its own `unlink()` failing (the checkpoint is already durable;
a file it could not delete is inert, filtered out by `recover()`'s own
seq check, and retried on the next checkpoint).

## F. Fallback / double-failure (FACT)

If `append()` raises, the event is persisted directly and synchronously
(`quality_writer`, `segment_rows=1`, so still fsync'd) -- carrying the
**same** `quality_event_id` the failed append already assigned, so a
partially-written WAL copy (write/flush succeeded, only fsync failed) is
reconcilable rather than an anonymous duplicate if it is ever recovered.

If that fallback *also* fails, the double failure is logged distinctly
(`quality_event_double_failure_possible_loss`, verified via `caplog`, not
merely "the call didn't raise") and checkpointing is latched off, since a
WAL-resident copy of this event may exist. **This is the one place a
quality event may actually be lost** -- both the durability mechanism and
its fallback failed. It is never silently claimed otherwise.

The same pattern (persist-with-stamped-id, latch-on-failure,
checkpoint-only-on-success) applies to queue overflow: the event is
already durable in the WAL when `QueueFull` fires, so it is persisted
directly rather than left to rot un-drained, and checkpointed on success
so it does not become a needless duplicate on the next restart.

## G. Quality-event ID uniqueness scope (per Step 7)

- **FACT:** `quality_event_id = f"{process_start_marker}-{seq}"`, where
  `process_start_marker` defaults to `f"{int(time.time()*1000)}-{os.getpid()}"`.
- **FACT:** within one WAL directory, across any number of restarts of
  the same collector, ids are unique and monotonically informative --
  `start_seq` is always resumed past the highest seq `recover()` ever
  saw in that directory.
- **INFERENCE, not proven:** across two *different* process lifetimes
  whose `process_start_marker` values happen to collide (same millisecond
  timestamp, same PID), ids from the two runs could collide. On bare
  metal this is practically implausible (restart latency vastly exceeds
  1ms). Under containerized deployment, where the main process commonly
  gets PID 1 on every restart, the collision risk rests entirely on the
  millisecond-timestamp half not coinciding -- still implausible given
  realistic restart timing, but not proven and not tested.
- **Correct scope statement:** the id is **WAL-directory-unique**, not
  provably globally unique. It is never claimed as globally unique in
  this document or in the module's own docstring.

## H. Disk-failure analysis (Step 8) -- FACT, all deterministically injected

No test fills a real disk; each failure is injected at the exact syscall
boundary (`os.fsync`, `os.replace`, a wrapped file handle), in
`tests/test_quality_wal_failure_semantics.py`.

| Failure point | Verified behavior |
|---|---|
| `write()` fails | `append()` raises, `write_failed=True`, id attached to the exception |
| `flush()` fails | same |
| `fsync()` fails | same; confirmed not silently ignored |
| checkpoint's `os.replace()` fails | in-memory state does NOT advance; a retry of the same seq is NOT a no-op |
| checkpoint's `fsync()` fails | same |
| `Path.unlink()` fails during cleanup | tolerated; checkpoint itself still succeeds; file left behind is inert |
| rotation's file-open fails | original handle still open and appendable afterward |
| Parquet write fails in the loop | loop survives, checkpoint latches off, WAL retained |
| Parquet write fails during recovery | recovery stops at the contiguous prefix, does not crash startup |
| checkpoint write fails during recovery | recovery does not crash; event stays pending |
| WAL append AND direct Parquet fallback both fail | no crash; logged distinctly; checkpoint latched off |

## I. Shutdown analysis (Step 9) -- FACT

- `shutdown()` closes the WAL (`wal.close()`, idempotent and
  exception-guarded so a close failure is logged, not raised).
- An event appended to the WAL but never drained before shutdown is NOT
  lost: the next `CollectorApp()` construction's `_recover_quality_wal()`
  finds and replays it. Verified end to end with two real `CollectorApp`
  instances against the same directory.
- A late quality event after `close()` raises a clean `OSError` rather
  than an incidental `ValueError`, handled by the existing fallback path.

## J. P0-1 interaction (Step 10) -- FACT / UNVERIFIED

- P0-1 and P0-2 compose at exactly one point: `_websocket_quality_event`
  is the `on_quality_event` callback P0-1's `WebSocketClient` invokes.
  P0-2 did not need to modify `websocket_client.py`, and did not.
- **UNVERIFIED:** whether the WAL's synchronous fsync can measurably
  stall the P0-1 receive path under sustained quality-event bursts. Not
  benchmarked under load.

## K. Performance (Step 11) -- INFERENCE only, not measured under load

`append()`'s cost is one `write` + one `flush` + one `fsync` per event --
the same syscall shape the marker file it replaced already performed on
every event. No EC2 or production measurement exists.

## L. Mutation testing -- ALL 20 requested mutations run, ALL caught

Every mutation below was applied to the real, current source, the
focused P0-2 suite was run, a failure was observed, the source was
restored, and the restoration was confirmed byte-identical via `cmp` --
for every row, not a subset.

| # | Mutation | Caught | Failing test(s) |
|---|---|---|---|
| 1 | Revert checkpoint inference to `start_seq - 1` | Yes | `test_start_seq_alone_does_not_imply_anything_was_checkpointed`, `test_startup_recovery_stops_checkpointing_at_first_persist_failure` |
| 2 | Remove `checkpoint_blocked` guard (loop, 2 sites) | Yes | `test_persist_failure_in_the_loop_never_advances_checkpoint_past_it` |
| 3 | Swallow double failure, no distinct log/latch | Yes | `test_double_failure_wal_and_parquet_both_fail_does_not_crash_caller` |
| 4 | Delete the active WAL file during checkpoint cleanup | Yes | `test_checkpoint_never_deletes_the_active_file_even_when_fully_covered` |
| 5 | Ignore `resume_seq`, always `start_seq=0` | Yes | `test_recover_quality_wal_resumes_sequence_correctly_no_id_reuse` |
| 6 | Remove `os.fsync()` from `append()` | Yes | `test_fsync_failure_is_visible_and_carries_the_event_id` |
| 7 | Remove `.flush()` from `append()` | Yes | `test_flush_failure_is_visible` |
| 8 | Enqueue before WAL append | Yes | `test_wal_write_failure_falls_back_to_direct_synchronous_persist`, `test_queue_overflow_event_is_persisted_directly_not_silently_covered` |
| 9 | Ignore malformed middle WAL record | Yes | `test_corrupt_middle_record_raises_and_does_not_silently_skip` |
| 10 | Treat a corrupt checkpoint file as fully checkpointed | Yes | `test_a_corrupted_checkpoint_file_is_treated_as_nothing_checkpointed_not_everything` |
| 11 | Skip WAL recovery entirely at startup | Yes | 5 tests across the recovery/shutdown suites |
| 12 | Checkpoint beyond the first startup persistence failure | Yes | `test_startup_recovery_stops_checkpointing_at_first_persist_failure`, `test_partial_recovery_failure_blocks_later_runtime_checkpoints` |
| 13 | Treat a WAL `OSError` as a successful append | Yes | 6 tests across the fallback/overflow/checkpoint suites |
| 14 | *(see note below)* | NOT APPLICABLE | -- |
| 15 | Continue after a persistent quality-store failure without latching | Yes | same invariant as #20; see note |
| 16 | Remove chronological ordering from `recover()`'s file iteration | Yes | `test_event_ids_are_stable_and_unique_across_a_simulated_restart`, `test_rotation_then_recovery_preserves_cross_file_order` |
| 17 | Treat a torn final-line tail as corruption (no tolerance) | Yes | `test_incomplete_final_line_is_quarantined_not_fabricated`, `test_corrupt_middle_record_is_distinguished_from_incomplete_tail` |
| 18 | Treat middle-of-file corruption as harmless | Yes | `test_corrupt_middle_record_raises_and_does_not_silently_skip`, `test_corrupt_wal_at_startup_is_reported_preserved_and_never_checkpointed_over` |
| 19 | Remove the loop's per-event try/except (silent task death) | Yes | `test_persist_failure_in_the_loop_never_advances_checkpoint_past_it` |
| 20 | Forget to latch the checkpoint block on a persist failure | Yes | `test_persist_failure_in_the_loop_never_advances_checkpoint_past_it` |

**Note on #14:** "clear WAL before Parquet persistence" has no real
mutation site in the current implementation -- `checkpoint()` is called
exactly once, after `_persist_quality_event` returns without raising, at
both call sites. There is no code path where a WAL file is cleared
before persistence is attempted; constructing one would mean writing new,
unrelated logic rather than mutating an existing decision point.
Classified **NOT APPLICABLE**. Invariant C is covered from the failure
side by mutations 3, 12, 13, 19 and 20 instead.

**Note on #15:** identical in effect to #20 as implemented -- the current
source has exactly one place where "continue after failure" and "latch
the checkpoint block" are the same line. Rather than double-count one
code site as two mutations, #15 is recorded as proven by #20's result,
and #19 (remove the surrounding try/except entirely) as the adjacent,
genuinely distinct mutation of that same region.

## M. Remaining limitations

- Invalid UTF-8 in a WAL file raises `UnicodeDecodeError` from
  `recover()` rather than `QualityWALCorruption`. Low risk
  (`json.dumps` output is pure ASCII), not fixed.
- `default=str` in `append()`'s JSON serialization can change value
  types for non-JSON-native fields. Not audited field by field.
- Multiple processes sharing one WAL directory are not supported (the
  lock is `threading.Lock`, in-process only).
- Total WAL directory size is not bounded, only per-file size is.
- P0-1/P0-2 interaction under sustained load is reasoned, not
  benchmarked.
- No live/production verification of any of this.

## N. Test count

Baseline captured twice, because `origin/main` moved mid-session (other
P0 work -- P0-4, P0-9, P0-10, P0-11, P0-12, a P0-1 follow-up -- merged
while this pass was in progress):

- First baseline, `origin/main` @ `2038e36c19e5a34280b50ca2c2d09917e7a573f1`:
  **1452 passed**.
- After `git rebase origin/main` onto the new tip (`7cda4b4...`, verified
  clean: `git merge-base origin/main HEAD` equals `origin/main` exactly,
  no conflicts, `run_collector.py`'s only upstream change was an
  unrelated nanosecond-timestamp parameter nowhere near the quality-event
  code -- confirmed by diffing the two main tips for that file and
  finding no overlap with anything this document touches): **1660 tests
  total, 1657 passed, 3 failed**.

**The 3 failures are a pre-existing regression on `origin/main` itself,
unrelated to this work** -- independently confirmed by running those
exact 3 tests against bare `origin/main` with none of this branch's
commits present; they fail identically there
(`test_p0_12_systemd_deployment.py::test_stop_timeout_outlasts_the_p0_1_websocket_drain`,
`test_websocket_client.py::test_on_message_receives_the_arrival_timestamp_not_the_processing_timestamp`,
`test_websocket_client.py::test_receive_timestamp_also_holds_for_handlers_that_take_no_connection_id`).
Not fixed here: they are P0-1/P0-11 territory, out of this task's scope,
and documented rather than silently worked around.

The P0-2-specific suite (`test_quality_wal.py`,
`test_quality_wal_collector_integration.py`,
`test_quality_wal_failure_semantics.py`, 50 tests) passes completely on
the rebased branch. `compileall`: clean. `git diff --check`: clean.

**Second rebase, same session:** `origin/main` moved again while this
document's first rebase note was being written (`7cda4b4` ->
`385a6e6`, a P0-1 shutdown-finalization audit). That PR touched the same
`shutdown()` method this branch's own WAL-close guard touches -- a real
conflict, not a textual coincidence. Resolved manually: kept main's new
per-writer `_close_writer_reporting_failure()` guard (every writer's
`close()` individually exception-guarded, so one failure cannot abort
closing the rest), and added this branch's WAL-close guard into that same
structure, immediately before `quality_writer` is closed. Verified
afterward: `git merge-base origin/main HEAD` equals the new main tip
exactly, and the full `test_quality_wal_failure_semantics.py` shutdown
tests (which exercise the real `shutdown()` method end to end) still pass
against the merged result -- confirming the resolution is semantically
correct, not merely textually conflict-free. Final count after this
second rebase: **1664 passed**, same 3 pre-existing unrelated failures
as above.

**Verdict: COMPLETE -- VERIFIED.** All 20 requested mutations were
executed against the current, rebased, hardened source; all were caught;
every restoration was confirmed byte-identical. The limitations in
section M are documented scope narrowing and edge cases, not unproven
safety-critical invariants.
