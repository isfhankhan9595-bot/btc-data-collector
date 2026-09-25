# P0-2: Quality-Event Durability (WAL)

## A. The existing problem

`_websocket_quality_event` is the collector's websocket-hot-path entry
point for quality events (connect/disconnect/rate-limit/etc.) -- it must
never block on Parquet I/O, so events were buffered in an in-memory
`asyncio.Queue` and drained later by a background loop. The only durable
artifact of that queue before this fix was `quality_queue.pending.json`,
a marker recording **the queue's depth and a timestamp** -- never the
events themselves. After a hard kill (SIGKILL, power loss) with events
still in the queue, the collector could know *"N quality events were
pending"* on restart, but could not recover *which* events, or what they
said. They were permanently lost; the marker only let the next process
emit one synthetic `DATA_DROP` event admitting that loss had occurred.

Every other `_persist_quality_event` call site (validator/book-integrity
drains, adapter-unhandled records, recovery transitions) was **not**
affected by this gap: they write directly and synchronously to
`quality_writer`, which is already configured with `segment_rows=1,
segment_seconds=1` -- confirmed by reading `ParquetWriter.write()`/
`flush()`/`_close_segment()` directly, every such write already performs
a full `os.fsync()` + atomic rename before returning. The websocket
hot-path queue was the one, narrow, real gap; this fix is scoped to it.

## B. New architecture

```
_websocket_quality_event (hot path)
        |
        v
  QualityEventWAL.append()     <- durability boundary: fsync'd before return
        |
        v
  asyncio.Queue (bounded, in-memory, may now safely overflow --
                 the event is already durable by this point)
        |
        v
  _quality_persistence_loop (background)
        |
        v
  _persist_quality_event -> quality_writer.write() -> Parquet (fsync'd)
        |
        v
  QualityEventWAL.checkpoint(seq) -> old, fully-covered WAL files deleted
```

On startup, `CollectorApp.__init__` calls `QualityEventWAL.recover()`
against the WAL directory *before* processing any new events, replays
every recovered record through `_persist_quality_event` (exact prior
content, not a marker), and checkpoints them.

## C. WAL record format

Newline-delimited JSON, one line per event, in `<quality stream
dir>/wal/<timestamp>-<process_start_marker>-<start_seq>.wal`:

```json
{"quality_event_id": "1758768000123-4821-7", "seq": 7, "exchange": "BINANCE",
 "stream": "websocket", "event_type": "DISCONNECT", "reason": "...",
 "connection_id": "...", "local_ts": 1758768000123}
```

`quality_event_id` and `seq` are added by the WAL itself; every other key
is the original event dict passed to `append()`, unmodified. This mirrors
the fields `_persist_quality_event` already knows how to read (it already
handled an open-ended dict via `.get()` with defaults) -- no new record
shape was invented beyond adding the two identity fields.

## D. Durability guarantee -- exactly, not aspirationally

- **Normal shutdown / SIGTERM**: every event. Graceful shutdown already
  drains the queue fully (`await self._quality_queue.join()`) before
  exiting; unchanged by this fix.
- **SIGKILL / process crash / machine power loss**: every event whose
  `QualityEventWAL.append()` call had already **returned** before the
  kill. `append()` is synchronous: write the line, flush the Python
  buffer, `os.fsync()` the file descriptor, then return -- nothing after
  that point can un-durable it short of physical media failure.
- **What can still be lost**: a single event whose `append()` call was
  itself interrupted mid-write by the kill. This is the one honest,
  narrow, unavoidable boundary of any WAL -- `fsync()` makes a *completed*
  write durable, it cannot protect a write that never finished. This is
  categorically smaller than the old failure mode (an unbounded in-memory
  queue, not one in-flight write).
- **Disk full**: `append()`'s `os.fsync()`/`write()` raise `OSError`
  (`ENOSPC`) like any other write in this condition; caught explicitly,
  logged (`quality_wal_append_failed`), and the event is persisted
  directly and synchronously instead of being queued (falling back to the
  same path every non-hot-path quality event already uses). If the disk
  is genuinely full, that direct write can also fail -- no software layer
  can write to a full disk; this is not claimed as a fix for that. The
  pre-existing `DiskMonitor` component is the collector's actual
  proactive defense against reaching that state, unchanged by this work.
- **Partial WAL write on disk**: handled explicitly as evidence, not
  loss -- see F below.

## E. Recovery, step by step

1. `QualityEventWAL.highest_recovered_seq(wal_dir)` scans every `*.wal`
   file for the highest `seq` present (checkpointed or not), so the new
   process's sequence counter starts strictly above anything on disk --
   IDs never collide across a restart.
2. `QualityEventWAL.recover(wal_dir)` reads every `*.wal` file, oldest
   first (filenames are timestamp-and-seq-prefixed, so filename sort is
   chronological), skips a genuinely incomplete final line (see F), and
   returns every record not already covered by the durable checkpoint
   file -- in original append order.
3. `CollectorApp.__init__` replays each recovered record through
   `_persist_quality_event` (a real Parquet row, exact content), then
   checkpoints the WAL up to the highest recovered `seq`, which deletes
   any now-fully-covered old `.wal` file.
4. If step 2 raises `QualityWALCorruption` (see F), recovery of that
   specific WAL is abandoned (the corrupted file is left on disk,
   untouched, for forensic inspection -- never deleted), one durable
   `ERROR`-type quality event records the corruption in Parquet, and
   startup **continues** rather than crashing the collector over a
   quality-journal problem. Market-data capture is the primary mission;
   quality-event WAL corruption must be loud, not fatal.

## F. Idempotency and corruption

**Idempotency**: `quality_event_id` is stable across the entire
WAL-to-Parquet lifecycle (assigned once, at `append()`, never
regenerated). If a crash lands between a successful Parquet write and its
checkpoint, the event is legitimately replayed and appears as **two**
Parquet rows on next startup -- the task's own acceptance framing permits
this ("effectively-once after event-ID deduplication"), and it is what
`quality_event_id` exists to make reconcilable: both rows carry the
identical ID, so a duplicate is never ambiguous, even though quality
events are not currently compacted (`QUALITY_EVENTS_SCHEMA` is
deliberately excluded from `compact_daily.py`, unchanged by this work) --
the ID is there and stable, ready for that dedup whenever a consumer
needs it, rather than absent.

**Corruption**: an incomplete final line (no trailing newline, unparsable
JSON) is treated as a normal, expected crash artifact -- silently
quarantined, every complete prior record still recovered. A malformed
record **anywhere else** in the file (not the final line) raises
`QualityWALCorruption` instead of being silently skipped -- the position,
not the content, is what distinguishes an expected torn tail from real
mid-file corruption, and the two are deliberately never treated the same.

## G. Files changed

- `collector/collector/quality_wal.py` (new): `QualityEventWAL`,
  `QualityWALCorruption`.
- `collector/run_collector.py`: constructor now recovers/replays/
  checkpoints on startup instead of emitting one "presumed lost" event;
  `_websocket_quality_event` appends to the WAL before enqueueing;
  `_quality_persistence_loop` checkpoints after each successful persist;
  `_persist_quality_event` now writes `quality_event_id` through to
  Parquet. `_write_quality_pending_marker` and the old
  `quality_queue.pending.json` mechanism are removed entirely -- the WAL
  is now the sole durability mechanism for this path.
- `collector/collector/config.py`: `QUALITY_EVENTS_SCHEMA` gains a
  nullable `quality_event_id` column (`schema_version` bumped `1.1` ->
  `1.2`). Nullable, so every pre-P0-2 row (which has none) reads back as
  legacy, never fabricated. `QUALITY_EVENTS_SCHEMA` is not part of
  `compact_daily.py`'s `STREAM_SCHEMAS`, so this has no legacy-column
  interaction with compaction to reconcile.

## H. Tests

- `collector/tests/test_quality_wal.py` (21 tests): append/recovery
  ordering, stable IDs across a simulated restart, crash-before-any-
  checkpoint, checkpoint idempotency and monotonicity, incomplete-tail
  vs. mid-file-corruption distinction (same bytes, different position,
  different outcome), rotation + cross-file recovery order, checkpoint-
  triggered cleanup that never deletes the active file, bounded per-
  instance memory (structural check), a 2,000-event burst round trip,
  8-thread concurrent-append integrity (no interleaved bytes, no
  duplicate/missing sequence numbers), write-failure visibility, and
  input purity/determinism.
- `collector/tests/test_quality_wal_collector_integration.py` (5 tests):
  the real `run_collector.py` wiring, not just the WAL primitive --
  survives-a-simulated-crash-before-drain, normal-drain-checkpoints,
  crash-before-checkpoint's legitimate-but-reconcilable duplicate, the
  actual startup recovery sequence, and the WAL-append-failure fallback.

**Baseline** (bare `origin/main`, captured properly via `git stash`
before any change, not assumed from an earlier session): **1241 passed**.
**Final**: **1267 passed** (1241 + 21 + 5). `compileall`: clean.
`git diff --check`: clean.

## I. Performance

Not separately benchmarked as a dedicated exercise -- the WAL append's
per-event cost is the same shape of operation
(open/write/flush/fsync/replace or write/flush/fsync) the marker file it
replaces already performed on every single event, so this is not a new
performance characteristic introduced by this fix, only a more complete
one. The 2,000-event burst test above completed in the same test run as
everything else, in well under a second total for the whole 26-test
combined suite -- adequate confirmation that this remains far from a
bottleneck for a stream this task itself describes as low-volume, without
constructing a separate formal benchmark for a claim nothing in this
fix's scope depended on.

## J. Remaining limitations / deferred

- No compaction step reads `quality_event_id` back for deduplication yet
  (there is no quality-event compaction at all, by pre-existing design --
  out of scope here). The ID is present and stable, ready for that.
- WAL rotation is size-triggered only (`maybe_rotate_for_size()`, called
  after each checkpoint); no separate age-based rotation. Given quality
  events are low-volume, a single WAL file is unlikely to become large
  enough for this to matter in practice, but it is a real, explicit gap
  against Section 14's "maximum bytes or maximum age" framing.
- Concurrency protection is a `threading.Lock` inside `QualityEventWAL`,
  correct for the actual single-asyncio-event-loop concurrency model this
  collector uses (and stress-tested here beyond that model, with real
  OS threads, to be conservative) -- not a multi-process lock; two
  separate `CollectorApp` processes must never point at the same WAL
  directory simultaneously (the same constraint `ParquetWriter`'s own
  stream-directory lock already enforces for Parquet segments, unchanged
  by this work).
- P0-3 through P0-12 are explicitly out of scope and untouched.
