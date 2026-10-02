# Quality-event storage (P0-3)

## Root cause

`ParquetWriter("quality_events", ..., segment_rows=1, segment_seconds=1)`
made every `write()` call also close and publish its own segment. Each
quality event therefore became its own one-row Parquet file: under
sustained quality-event traffic (e.g. a burst of gap/reconnect events),
file count tracked event count 1:1, with proportional filesystem metadata
and per-file fsync overhead.

**Why it was chosen (found in the removed comment, not guessed):**
`segment_rows=1` gave the quality writer *synchronous-equivalent, per-event*
Parquet durability — every `write()` was fsync'd before returning — which
made the (then-nonexistent) WAL unnecessary for the direct `write()` call
sites. This was a real, deliberate durability property, not an oversight.
Simply raising `segment_rows` without anything else would have silently
weakened it.

## What changed since that rationale was written

P0-2 added `QualityEventWAL`: `_websocket_quality_event`'s hot path appends
to a fsync'd WAL *before* the event is queued, and the queue-drain loop
(`_quality_persistence_loop`) persists it once dequeued. That made
per-event Parquet durability no longer the *only* thing standing between a
crash and data loss, **but only for that one producer.**

**Regression found by audit, after this document's first version shipped:**
`_persist_quality_event` has roughly a dozen *other* call sites — adapter-
unhandled frames, malformed/unrouted messages, instrument mismatches, OI
errors, book-quality transitions, the queue-overflow counter, startup-
recovery-error reporting, and more — that call it directly, synchronously,
with no WAL involvement at all. Before this task, that was safe only as a
side effect of `segment_rows=1`: every one of those direct `write()` calls
was itself an immediate, fsync'd publish. Once batching removed that
property, those dozen call sites were left with a real gap: their events
could sit only in `ParquetWriter`'s RAM buffer, with no WAL record, until
the next publish. **The fix below was revised to close this**, and the
terms this document uses now mean exactly this:

- **WAL-backed event** / **directly durable event** — obsolete distinction.
  Both kinds of event now go through the identical mechanism (below); there
  is no longer an architectural difference between them.
- **Recoverable event** — appended to the WAL (by whichever caller) but not
  yet in a published Parquet segment. Survives a crash via WAL replay.
- **Published / durable event** — in a published Parquet segment. This is
  the only state from which a checkpoint may ever be issued.

## The fix

`segment_rows=1, segment_seconds=1` → `segment_rows=500, segment_seconds=30`
(`run_collector.py`, `CollectorApp.__init__`).

That alone would reintroduce exactly the bug this task fixes, one level up:
the drain loop used to call `wal.checkpoint(up_to_seq=wal_seq)` immediately
after `write()` returned — correct when `write()` also meant "published",
wrong once `write()` can mean "buffered in RAM, not yet on disk". A crash
between such a checkpoint and the batch's actual publish would permanently
lose every event in that unpublished batch, because the WAL had already
forgotten them.

Fixed by moving checkpoint-advancement from *after write()* to *after
publish*:

- `ParquetWriter` gained two small, additive (default-`None`, so every
  other stream's behaviour is byte-for-byte unchanged) hooks:
  `segment_publish_hook(path, record_count)` (distinct from an unrelated, same-named P0-4 `ParquetWriter.on_segment_published` attribute with a different signature; renamed to avoid the collision) — called once a segment is
  durably published (rename + directory fsync complete) — and
  `publish_open_segment()` / `abandon()` — explicit force-publish and
  crash-equivalent-discard, used by recovery and tests respectively.
- `_persist_quality_event` is now the **single WAL-durable choke point**
  every quality event passes through, direct callers included. If an event
  does not already carry WAL provenance (`_wal_seq` from
  `_websocket_quality_event`'s own append, or `seq` from a WAL-recovery
  replay — either means "already durably in the WAL, do not append again"),
  it is WAL-appended right there, synchronously, before it ever reaches
  `quality_writer.write()`. The dozen direct call sites needed zero changes
  of their own; fixing the one shared function fixed all of them.
- The same method also tracks `self._quality_pending_max_wal_seq` — the
  highest WAL seq among events `write()`-ed since the last checkpoint —
  *before* calling `quality_writer.write()` (a `write()` can synchronously
  trigger a publish inside that same call, once the batch fills), so
  `_on_quality_segment_published` (wired as the `segment_publish_hook`)
  always sees the correct value the moment it fires. This bookkeeping used
  to live separately in the queue-drain loop and in startup recovery; both
  copies were removed once it moved into the shared chokepoint.
- `_on_quality_segment_published` is the *only* place that calls
  `wal.checkpoint()` on the live path now. It checkpoints exactly the seq
  range that just became durable — never more, never speculatively.
- `_recover_quality_wal` (startup recovery) had the **same publish-before-
  checkpoint bug**, independently: it re-persists recovered rows and used
  to checkpoint immediately after the loop, regardless of whether those
  rows were actually published. A second crash right after recovery, before
  the next publish, would have permanently lost the very rows recovery just
  replayed. Fixed the same way, and now via the same shared chokepoint:
  `publish_open_segment()` forces the recovered batch durable once (a
  one-time startup cost, not a per-event one) before its callback
  checkpoints it.
- `checkpoint_blocked` (`self._quality_checkpoint_blocked`, an instance
  attribute so the callback can see it) is unchanged in effect: once any
  persist fails, checkpointing never advances again for the rest of the
  process's life, at any batch size. **Limitation, pre-existing and
  unchanged by this task:** only the queue-drain loop's own try/except sets
  this flag on a persist failure. A *direct* caller whose
  `_persist_quality_event` call raises does not set it automatically — the
  exception simply propagates to that caller, exactly as it did before
  P0-3. This is not a new gap; it was true when every direct call was its
  own synchronous fsync too.

## Durability semantics

An event is **published/durable** once it is in a published Parquet
segment; it is **recoverable** (via WAL replay) from the moment it is
WAL-appended — by `_websocket_quality_event` directly, or by
`_persist_quality_event`'s own fallback append for every other caller —
until it becomes durable. The checkpoint boundary is exactly the durable
boundary — never earlier, for any caller. This guarantee now covers EVERY
quality event uniformly, which is strictly broader than both the pre-P0-3
design (direct callers were synchronously durable, never merely
"recoverable") and this document's own first version (which, before the
corrective fix below, only described this guarantee for
`_websocket_quality_event`'s queue — the dozen direct callers had no WAL
protection in that version at all). What changed from pre-P0-3 is *how many
events* may be "recoverable-but-not-yet-durable" at once (up to one batch,
~500 events / ≤30s), not *whether* every event is one or the other.

## Crash / restart semantics

- A crash while a batch sits buffered, unpublished: on restart, `_recover_orphans`
  discards the unpublished `.seg.tmp` (pre-existing behaviour, unchanged for
  every stream). The events it held are NOT lost: their WAL seq was never
  checkpointed, so `_recover_quality_wal` replays them from the WAL into a
  fresh segment, force-published once, then checkpointed. Proven by
  `test_crash_with_a_full_unpublished_batch_loses_nothing` and
  `test_crash_exactly_at_a_batch_boundary_loses_nothing` (a batch that HAD
  already published, plus more events buffered after it, crashes — only the
  buffered tail needs recovery; the published rows aren't duplicated).
- A crash during recovery itself, before `publish_open_segment()` completes:
  the recovered-but-not-yet-published rows are, again, simply not
  checkpointed, so the *next* restart's recovery replays them again
  (harmless: `quality_event_id` lets duplicates be reconciled at research
  time, exactly as the pre-existing recovery docstring already documented).

## Shutdown semantics

`shutdown()` calls `quality_writer.close()` (unchanged call site), which
publishes any open segment and, via the same `segment_publish_hook`,
checkpoints it — so a graceful shutdown always leaves the WAL with nothing
pending. Verified directly against the real `shutdown()` source (not a
reimplementation) by
`test_production_shutdown_publishes_the_quality_writer_not_abandons_it`,
added after a mutation (H: swap `close()` for `abandon()` in `shutdown()`)
was **not** initially caught by any other test in the suite — every other
test called `quality_writer.close()` directly rather than exercising
`shutdown()` itself.

## Memory bound

The open segment's buffer never exceeds `segment_rows` (500) entries —
small `dict`s, a few hundred KB at most — regardless of total events
received (`test_open_segment_buffer_never_exceeds_segment_rows`). The async
queue remains bounded (`maxsize=1024`, unchanged); overflow beyond that was
already handled (WAL-durable before enqueue, counted, and reported as its
own quality event) and is unaffected by this change.

## File-count / storage impact (measured)

Synthetic, deterministic, real `ParquetWriter`, same machine, before vs
after:

| events | files before | files after | fsyncs before | fsyncs after | disk before | disk after |
|---|---|---|---|---|---|---|
| 100 | 100 | 1 | 500 | 5 | 1,204 KB | 20 KB |
| 1,000 | 1,000 | 2 | 5,000 | 10 | 12,004 KB | 60 KB |
| 5,000 | 5,000 | 10 | 25,000 | 50 | 60,004 KB | 284 KB |

Full-read time for the whole stream dropped ~80–480× alongside it (fewer,
larger files mean far less per-file Parquet footer/metadata overhead to
parse). Write throughput: ~300 events/s (fsync-per-row bound) before, tens
of thousands/s after — batching removes the fsync from the hot path
entirely for all but the (now rare) publishing write.

A sustained burst of quality events — the actual pathological case this
task targets — now produces `ceil(N / 500)` files instead of `N`.

## Legacy compatibility

Old `segment_rows=1`-era files (one row each) are read exactly as before —
nothing about compaction, `iter_segments`, or Parquet reading assumes a
particular row count per file. `test_legacy_one_row_per_file_segments_still_read_correctly`
constructs a directory of such files directly (bypassing the new
`ParquetWriter` defaults) and confirms every row is still readable.
`quality_events` is not currently part of `scripts/compact_daily.py`'s
`STREAM_SCHEMAS` (only market-data streams are compacted today), so no
compaction-path change was needed or made.

## Known limitations / unverified

- Peak RAM was reasoned about (bounded buffer size) rather than measured
  under a live process; the measurement above is synthetic, single-process,
  same-machine — not a production workload.
- `segment_rows=500` / `segment_seconds=30` are chosen values, not derived
  from a specific SLA; they are easy to retune (both are already
  constructor parameters) if a different staleness/file-count tradeoff is
  wanted later.
- The disk-full / permission-failure / writer-exception failure modes are
  covered by the existing `checkpoint_blocked` mechanism (a persist failure
  anywhere blocks all further checkpointing for the process's life, verified
  under the real batch size in `test_persistence_failure_never_advances_checkpoint_under_real_batching`)
  and by `ParquetWriter`'s pre-existing atomic-publish design (unmodified);
  no new disk-full-specific test was added, since none of P0-3's changes
  touch how a write failure is detected — only when a successful one is
  checkpointed.

## Corrective fix: the direct-caller durability regression

A hostile audit of this PR, before merge, found the gap described above
(the "Regression found by audit" paragraph) in this document's first
version. Summary of what changed to close it, kept here as the permanent
record rather than only in commit messages:

- `_persist_quality_event` became the single WAL-durable choke point for
  every quality event (see "The fix" above) — no call site needed its own
  changes.
- 8 new adversarial tests specifically for this regression, including the
  mandatory crash-with-a-real-direct-event-still-buffered scenario, using
  the real `_persist_quality_event` and `_record_book_quality` production
  call sites (not a reimplementation): `test_direct_call_site_event_is_wal_appended_before_being_buffered`,
  `test_direct_event_survives_a_crash_while_still_buffered_unpublished`,
  `test_real_record_book_quality_call_site_is_also_wal_protected`,
  `test_mixed_direct_and_websocket_events_in_the_same_batch_both_protected_and_checkpointed`,
  `test_persistence_failure_on_a_direct_event_blocks_further_checkpointing`,
  `test_recovery_replays_a_mix_of_direct_and_websocket_sourced_events`,
  `test_no_premature_checkpoint_for_a_direct_event_before_publication`.
- Of the 6 mutations the audit specifically requested for this regression:
  4 are reachable and were caught (bypass WAL for a direct event; checkpoint
  a direct event before publication; recovery discarding replayed events
  instead of re-persisting them; a real call site, `_record_book_quality`,
  losing protection). One ("restore `segment_rows=1` only for direct
  paths") is architecturally inapplicable now — there is only one quality
  writer and one path, by design; there is no longer a separate "direct"
  configuration to restore. One ("checkpoint mixed-batch WAL state
  incorrectly" via last-seq-wins instead of max-seq) is **provably
  unreachable**: `QualityEventWAL.append()` assigns seq strictly
  monotonically per instance (under a lock), every call site shares exactly
  one instance at a time, and WAL recovery replays strictly in original
  (ascending) append order before any live seq is issued — so "the last
  event persisted in a batch" is always "the event with the highest seq",
  for every call sequence this codebase can actually produce. An initial
  attempt at testing this mutation passed against both correct code and the
  mutated code, because it used a fabricated `_wal_seq` that was never truly
  written to the WAL — that test was invalid (proved nothing) and was
  replaced with `test_pending_seq_tracking_uses_max_not_last_write_as_a_defensive_invariant`,
  which documents the reachability finding and exercises the real
  (always-monotonic) code path instead.
- **Honesty correction, disclosed rather than quietly fixed:** this task's
  first completion report claimed mutation "unbounded queue" was caught. It
  was not — no test in the suite observed the queue's `maxsize` at all, so
  removing it produced zero failures. Found while re-verifying all mutations
  against the corrected source, disclosed, and closed with
  `test_production_quality_queue_is_bounded_not_unbounded` (a direct
  source-inspection test, the only tool that can catch this without
  actually exhausting memory in a unit test).
- `ParquetWriter`'s new `on_segment_published` hook (from this task's first
  version) collided by name with an unrelated, already-merged P0-4
  `on_segment_published` attribute (different signature:
  `((hour, seq), final_path)` vs this task's `(final_path, record_count)`).
  Renamed to `segment_publish_hook` during the rebase that discovered the
  collision; P0-4's attribute and its own call site were untouched.
