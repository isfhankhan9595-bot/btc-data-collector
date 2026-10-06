# Quality-event durability (P0-2 + P0-3, unified)

## Contract

* **WAL durable** -- `QualityEventWAL.append()` returned: write, flush, fsync done. The event survives a process crash.
* **Published** -- the Parquet segment holding the event was fsync'd, atomically renamed, and its directory fsync'd. Only then does `ParquetWriter` call `on_segment_durable(path, record_count)`.
* **Checkpoint N** -- every WAL record with seq <= N is in a published segment. The checkpoint is written (fsync + atomic replace) *before* in-memory state advances.

`ParquetWriter.write()` returning means nothing about durability once batching is on.

## Single choke point

All ~14 direct callers, the queue loop, queue overflow and recovery replay go through `CollectorApp._persist_quality_event`:

1. no WAL provenance (`_wal_seq`) -> WAL-append first; the seq is tracked **in-flight**;
2. `quality_writer.write(row, bind=...)` -- `bind` fires after any hour rollover and before the append, so a seq is attributed to the segment that really contains the row;
3. if the WAL append failed, the event has no WAL backing: the open segment is **force-published** immediately (old one-file-per-event behaviour, only on this failure path); if that fails the exception propagates and the checkpoint is latched.

## Checkpoint rule

`_quality_wal_inflight` = seqs WAL-appended but not yet published (added at append time, so a queued event holds the checkpoint back). On `on_segment_durable`, that segment's seqs leave in-flight and the checkpoint moves to `min(inflight) - 1` (or the highest published seq if none are in flight). Never a max.
A latch (`_block_quality_checkpoint`) additionally stops all checkpointing for the process on: checkpoint-write failure, writer write/publish failure, WAL+direct double failure, queue-overflow direct-persist failure, recovery failure, corrupt WAL, quality_writer close failure. Everything stays in the WAL for the next start.

## Recovery

`_recover_quality_wal` replays in seq order, then force-publishes; the hook checkpoints the contiguous published prefix. WAL 0..4 with seq 2 failing -> checkpoint 1; 2,3,4 remain. Replays are at-least-once: a crash after publish but before checkpoint yields duplicate rows carrying the **same `quality_event_id`** (reconcile by id). The id is WAL-directory-unique, not globally unique (see P0-2 doc section G).

## Shutdown order

stop intake -> drain queue synchronously -> close other writers (their failure reports are WAL-protected quality events) -> close `quality_writer` (publishes the tail, hook checkpoints) -> close WAL. Each step is guarded; one failure never stops the rest.

## Time-based flush

`segment_seconds` is evaluated only inside `write()` -- there is no timer. An idle persistence loop therefore calls `publish_if_due()` every 0.1 s tick, and the quality writer uses `age_from_first_row=True` so the first event after a quiet spell no longer publishes as a 1-row file. Until publication the WAL is the recovery authority. Worst case is bounded by time, not event count: <= 1 file per 30 s (2880/day) per stream.

## API

`ParquetWriter(on_segment_durable=(path, record_count))` is distinct from P0-4's `on_segment_published((hour, seq), path)`: different signature, and a hook failure is logged and never poisons the writer or un-publishes the segment (the owner latches its own state). Also added: `publish_open_segment()`, `publish_if_due()`, `has_unpublished_rows()`, `age_from_first_row`. Defaults preserve existing behaviour for every other writer.

## Scope / not changed

Bybit, OKX and Binance-spot runners build their own `quality_events` writers with `segment_rows=1`. They have no WAL, so batching them would weaken durability; left as is.
