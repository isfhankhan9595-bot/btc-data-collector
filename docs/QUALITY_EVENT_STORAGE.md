# Quality-Event Storage: Tiny-File Explosion Fix

## Scope note: the requesting document's premise was false, and this is stated plainly

The task that prompted this fix described itself as "P0-3", assuming
"P0-1" (WebSocket receive/processing decoupling) and "P0-2" (a durable
write-ahead log for quality events) were "already implemented and
validated". **Neither exists anywhere in this repository** — confirmed by
searching for a WAL class, "write-ahead", and any receive/processing
decoupling mechanism before writing any code. No fabricated WAL was built
to satisfy that false premise. This fix addresses the real, independently-
verifiable problem underneath the request — quality-event tiny-file
explosion — using the mechanisms that actually exist.

## The real, confirmed problem

Every live runner's quality-event `ParquetWriter` used
`segment_rows=1, segment_seconds=1`:

```
collector/run_collector.py:            segment_rows=1, segment_seconds=1
collector/run_bybit_collector.py:      segment_rows=1, segment_seconds=1
collector/run_okx_collector.py:        segment_rows=1, segment_seconds=1
collector/run_okx_capture.py:          segment_rows=1, segment_seconds=1
collector/run_binance_spot_collector.py: segment_rows=1, segment_seconds=1
```

One Parquet segment (plus its `.meta.json` sidecar) per single quality
event. For a 24/7 collector, any sustained run of gaps, recoveries,
duplicates, or errors creates one tiny file per incident — an operational
problem (inode pressure, slow directory listing, backup/rsync cost)
independent of correctness.

## What was NOT weakened

`ParquetWriter._close_segment()` already publishes every segment
atomically — `fsync` on the data file, `os.replace` for the rename,
`fsync` on the parent directory — regardless of `segment_rows`/
`segment_seconds` (confirmed by reading the method before changing
anything). **This fix does not touch that mechanism and does not weaken
the durability of any segment that has been published.**

Graceful shutdown already flushes a partial batch: `close()` calls
`_finalize_segment()`, which publishes whatever is in `self.buffer`
regardless of whether the row threshold was reached (confirmed by reading
the method; also proven by
`test_partial_batch_is_flushed_on_graceful_shutdown`).

## The real, honestly-stated tradeoff

Quality events sitting in `self.buffer` between flushes are lost if the
process crashes before the next flush. Raising `segment_rows`/
`segment_seconds` means a longer window of unflushed events than
`segment_rows=1` did. **There is no WAL in this repository to eliminate
that window.** This is a deliberate, bounded trade — a small, rare
crash-loss window in exchange for a large, certain reduction in file
count — not a claim that the loss window has been eliminated. A future
P0-2 (a real durable WAL, if one is ever built) would close this gap; it
does not exist today, and this fix does not pretend otherwise.

## Configuration

`collector/collector/config.py`:

```python
QUALITY_SEGMENT_ROWS = 500
QUALITY_SEGMENT_SECONDS = 30
```

One authoritative location, imported by all five live runners — not five
independently hardcoded numbers that could silently drift out of sync
(confirmed no longer possible by
`test_quality_segment_constants_are_shared_not_duplicated_per_runner`).

**Why 500/30, not some other pair:** targets roughly 10–20 segments for a
10,000-event burst (pinned exactly at 20 by
`test_burst_of_quality_sized_events_produces_far_fewer_segments_than_events`),
and keeps the crash-loss window small relative to how rarely quality
events fire in normal operation — each one represents a discrete incident
(a gap, a recovery, a disconnect), not routine per-message traffic the way
trades or order-book updates are.

## Tests

`test_parquet_writer.py` — 5 new tests: burst file-count reduction (pinned
to the exact expected count, not just "fewer than N"), a before/after
comparison proving the old `segment_rows=1` behavior really was one file
per event, partial-batch shutdown flush, write-order preservation within a
batched segment (forensic reconstruction depends on persisted order
matching write order, not a timestamp-based resort), and a regression
guard ensuring every runner references the shared config constants rather
than a locally hardcoded value.

Two pre-existing tests
(`test_bybit_collector.py::test_update_id_decrease_is_detected_and_recorded`,
`::test_writers_attribute_their_own_events_to_bybit_not_binance`) assumed
the old immediate-flush behavior and checked published `.seg` files on
disk directly. Fixed to check `writer.buffer` instead — the events are
still correctly recorded, just not yet flushed to a segment, which is the
intended behavior now.

**Full suite: 1246 passed** (1241 baseline + 5 new). `compileall` clean,
`git diff --check` clean, standalone import check clean for all five
runners.

## Not built (out of scope, and honestly so)

- **No write-ahead log.** The requesting document's entire crash-recovery
  architecture (WAL → bounded batch → Parquet → checkpoint) depends on a
  WAL that does not exist. Building one is a real, separate, larger
  undertaking (P0-2, if it is ever scoped as its own task) — not fabricated
  here to make a false premise true.
- **No byte-size threshold.** Quality-event payloads in this schema are
  small, fixed-shape rows (exchange, stream, event_type, reason, a handful
  of optional numeric/string fields) — not large contextual blobs. 500 rows
  of this shape do not risk an unexpectedly huge segment. Not added, since
  the existing event structure does not justify it.
- **No metrics/instrumentation** beyond what already exists (structured
  `closed_parquet_segment` logging). A larger metrics surface was judged
  out of scope for a sizing fix.
- **No live exchange session** backs any of this; verified with synthetic
  writes through the real `ParquetWriter` code path.
