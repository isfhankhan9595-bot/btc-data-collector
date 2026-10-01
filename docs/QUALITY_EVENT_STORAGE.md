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

P0-2 added `QualityEventWAL`: `_websocket_quality_event`'s hot path now
appends to a fsync'd WAL *before* the event is queued, and the queue-drain
loop (`_quality_persistence_loop`) checkpoints the WAL once an event is
durable. The WAL is the authoritative durability layer for the one loss
window that matters (the in-process async queue between enqueue and
drain) — so per-event Parquet durability is no longer the only thing
standing between a crash and data loss, **provided the WAL checkpoint is
tied to the same "durable" definition the WAL exists to protect.**

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
  `on_segment_published(path, record_count)` — called once a segment is
  durably published (rename + directory fsync complete) — and
  `publish_open_segment()` / `abandon()` — explicit force-publish and
  crash-equivalent-discard, used by recovery and tests respectively.
- `run_collector.py` tracks `self._quality_pending_max_wal_seq`: the
  highest WAL seq among events `write()`-ed since the last checkpoint. Set
  *before* each `_persist_quality_event()` call (a `write()` can
  synchronously trigger a publish inside that same call, once the batch
  fills), so `_on_quality_segment_published` always sees the correct value
  the moment it fires.
- `_on_quality_segment_published` is the *only* place that calls
  `wal.checkpoint()` on the live path now. It checkpoints exactly the seq
  range that just became durable — never more, never speculatively.
- `_recover_quality_wal` (startup recovery) had the **same bug**,
  independently: it re-`write()`s recovered rows into the (now larger)
  buffer and used to checkpoint immediately after the loop, regardless of
  whether those rows were actually published. A second crash right after
  recovery, before the next publish, would have permanently lost the very
  rows recovery just replayed. Fixed the same way: the per-event pending-seq
  tracking runs inside the recovery loop too, and `publish_open_segment()`
  forces the recovered batch durable once (a one-time startup cost, not a
  per-event one) before its callback checkpoints it.
- `checkpoint_blocked` (renamed `self._quality_checkpoint_blocked`, an
  instance attribute so the callback can see it) is unchanged in effect:
  once any persist fails, checkpointing never advances again for the rest
  of the process's life, at any batch size.

## Durability semantics

An event is **durable** once it is in a published Parquet segment; it is
**recoverable** (via WAL replay) from the moment `_websocket_quality_event`
appends it to the WAL until it becomes durable. The checkpoint boundary is
exactly the durable boundary — never earlier. This is an equivalent
guarantee to the pre-P0-3 design, not a weaker one: what changed is *how
many events* may be "recoverable-but-not-yet-durable" at once (up to one
batch, ~500 events / ≤30s), not *whether* every event is one or the other.

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
publishes any open segment and, via the same `on_segment_published` hook,
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
