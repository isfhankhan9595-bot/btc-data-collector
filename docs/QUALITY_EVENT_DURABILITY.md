# Quality-event durability (P0-2 + P0-3, unified)

## Contract

* **WAL durable** -- `QualityEventWAL.append()` returned: write, flush, fsync done. The event survives a process crash, relying on the storage contract that `fsync` returns only once data is durable; the tests inject exceptions and do not demonstrate real power-loss behavior.
* **Published** -- the Parquet segment holding the event was fsync'd, atomically renamed, and its directory fsync'd. Only then does `ParquetWriter` call `on_segment_durable(path, record_count)`.
* **Checkpoint N** -- every WAL record with seq <= N is in a published segment. The checkpoint is written (fsync + atomic replace) *before* in-memory state advances.

`ParquetWriter.write()` returning means nothing about durability once batching is on.

## Aggregate WAL capacity and admission behavior

Only the Binance USD-M Futures and Bybit runners use this WAL. Its hard admission budget is
**100,000,000 bytes by default** (100 MB, decimal), configured before startup with
`COLLECTOR_QUALITY_WAL_MAX_TOTAL_BYTES`. The value must be a positive base-10 integer. Invalid
configuration fails startup; it is never coerced. The budget is aggregate across every `.wal` file
in that runner's WAL directory, not a per-file rotation threshold.

For each append, the WAL calculates the UTF-8 JSONL record size including its newline and checks
the current aggregate `.wal` bytes before writing. WAL-owned `seq` and `quality_event_id` fields
cannot be overridden by event data. If the candidate would cross the limit, or the aggregate size
cannot be safely measured, that record is refused **before bytes are written**, no WAL ID is issued,
and admission is latched closed until process restart. After a successful full recovery pass, runners
may remove files that are strictly validated and already covered by the durable checkpoint. If the
remaining WAL is at/over budget, cannot be measured, or recovery fails, unverified files are preserved
and admission remains fail-closed when the cap condition is detected. Uncheckpointed, corrupt or
unterminated records are never deleted to meet the limit.

When the cap is latched, runners route new quality events through the existing synchronous
`ParquetWriter.write()` plus immediate `publish_open_segment()` fallback. Those rows have no
`quality_event_id` because they do not have WAL records; the row is not falsely counted as
WAL-retained. Successful segment publication is the durability evidence. If the Parquet fallback
also fails, the existing degraded/lost/unconfirmed counters and alerts apply; this cap does not
manufacture durability or guarantee the quality event survives a failure of both paths. The
collector's raw and derived market-data routes are not stopped by a quality-WAL cap.

A write/flush/fsync error after an append has been admitted remains different: bytes may have been
partly written and the assigned ID is attached to the error so an uncertain duplicate can be
correlated. The WAL instance then refuses later appends until restart/recovery, preventing a new
valid JSON record from being appended behind a possible partial tail. Checkpoint replacement syncs
the parent directory before in-memory checkpoint advancement or deletion of covered WAL files;
WAL create/rotation also syncs the parent directory. Unsupported directory-sync operations fail
closed rather than being reported as durable.

`storage_pressure_status()` exposes `quality_wal_max_bytes` and reports
`wal_total_size_bounded: true` only when a current aggregate-size measurement is available and is
at or below the configured cap. `quality_channel_status()` separately exposes
`quality_wal_capacity_exhausted`, `quality_wal_capacity_reason` and `quality_wal_max_bytes`.
The cap can remain latched even after checkpointed files are later removed; restarting after
recovery re-evaluates the namespace. This deliberate fail-closed policy is visible in status.

Scope of the guarantee: the limit bounds the WAL files in one correctly single-writer WAL namespace.
It is not a cap on total dataset/Parquet size or on inode count. Under sustained cap mode, immediate
Parquet publication can create more quality segment files; disk free-space monitoring remains
necessary and continues to trigger controlled shutdown at its existing emergency threshold. These
are filesystem syscall-level guarantees; the test suite does not establish behavior under real
power loss, physical ENOSPC, or an unsupported filesystem.

## Single choke point

All ~14 direct callers, the queue loop, queue overflow and recovery replay go through `CollectorApp._persist_quality_event`:

1. no WAL provenance (`_wal_seq`) -> WAL-append first; the seq is tracked **in-flight**;
2. `quality_writer.write(row, bind=...)` -- `bind` fires after any hour rollover and before the append, so a seq is attributed to the segment that really contains the row;
3. if the WAL append failed, the event has no WAL backing: the open segment is **force-published** immediately (old one-file-per-event behaviour, only on this failure path); if that fails the exception propagates and the checkpoint is latched.

## Checkpoint rule

`_quality_wal_inflight` = seqs WAL-appended but not yet published (added at append time, so a queued event holds the checkpoint back). On `on_segment_durable`, that segment's seqs leave in-flight and the checkpoint moves to `min(inflight) - 1` (or the highest published seq if none are in flight). Never a max.
A latch (`_block_quality_checkpoint`) additionally stops all checkpointing for the process on: checkpoint-write failure, writer write/publish failure, WAL+direct double failure, queue-overflow direct-persist failure, recovery failure, corrupt WAL, quality_writer close failure. Everything stays in the WAL for the next start. While a latch is set, no checkpoint advances: events published after the latch remain uncheckpointed until restart, and the next start replays them, so their rows can be written a second time (see Delivery guarantee).

## Recovery

`_recover_quality_wal` replays in seq order, then force-publishes; the hook checkpoints the contiguous published prefix. WAL 0..4 with seq 2 failing -> checkpoint 1; 2,3,4 remain. Replay is **at-least-once**: a crash, or a checkpoint-write failure/latch, after publish but before the checkpoint covers the event yields duplicate rows carrying the **same `quality_event_id`**. The collector does not check published segments before replaying and does not deduplicate; reconciliation of published rows is **not implemented** (see Delivery guarantee). The id is WAL-directory-unique, not globally unique (see P0-2 doc section G).

## Delivery guarantee (at-least-once; no automatic deduplication)

* Delivery of a quality event to published Parquet is **at-least-once**, never exactly-once.
* `quality_event_id` identifies an event and lets a consumer recognise matching replay duplicates. No component in this repository removes or reconciles duplicate quality-event rows; `scripts/compact_daily.py` does not handle quality streams. Reconciliation is a downstream responsibility, not an automatic collector guarantee.
* Rows written without a WAL-issued id have a null `quality_event_id` (the column is nullable) and cannot be matched this way. Today that means, for example, rows from runners with no supported quality WAL (only the Futures and Bybit runners have one; Spot, OKX and OKX capture-only do not) and legacy rows written before the column existed. It does **not** mean "the WAL append failed": the WAL assigns the id before it writes, flushes or fsyncs, and an ordinary `OSError` from those steps carries the id on the exception, so a directly persisted fallback row is stamped with it and is not null. An append failure that does not carry the already-assigned id (for example a non-`OSError` raised while serialising the record) is the one narrow way a runner-written row can still end up with a null id.
* **Suggested** future-reader key (not existing code behavior): `(exchange, stream, quality_event_id)`, excluding null ids. The id alone is only WAL-directory-unique, hence the extra columns.
  * **Qualification:** that key is suitable only when the source / WAL namespace is unambiguous (for example one data directory, and therefore one quality WAL, per exchange). `exchange` and `stream` do not identify the WAL directory that issued an id, and the quality row schema has no source or WAL-namespace column. A reader that combines datasets from more than one WAL namespace (several hosts or deployments, restored or relocated data directories) may also need a stable source or WAL-namespace identity, which it would have to take from dataset provenance outside the row.
* Reproduced behavior (injected faults on a scratch copy, not a real power-loss test): a checkpoint-write failure after publish, or a directory-fsync failure after rename, followed by a restart on the same directory, produced two published rows with the same `quality_event_id`.

## Shutdown order

stop intake -> drain queue synchronously -> close other writers (their failure reports are WAL-protected quality events) -> close `quality_writer` (publishes the tail, hook checkpoints) -> close WAL. Each step is guarded; one failure never stops the rest.

## Time-based flush

`segment_seconds` is evaluated only inside `write()` -- there is no timer. An idle persistence loop therefore calls `publish_if_due()` every 0.1 s tick, and the quality writer uses `age_from_first_row=True` so the first event after a quiet spell no longer publishes as a 1-row file. Until publication the WAL is the recovery authority. Worst case is bounded by time, not event count: <= 1 file per 30 s (2880/day) per stream.

## API

`ParquetWriter(on_segment_durable=(path, record_count))` is distinct from P0-4's `on_segment_published((hour, seq), path)`: different signature, and a hook failure is logged and never poisons the writer or un-publishes the segment (the owner latches its own state). Also added: `publish_open_segment()`, `publish_if_due()`, `has_unpublished_rows()`, `age_from_first_row`. Defaults preserve existing behaviour for every other writer.

## Scope / not changed

The Bybit, OKX, OKX capture-only and Binance-spot runners build their own quality-events writers with `segment_rows=1`; P0-3 batching is not applied to them and is left as is.

WAL scope by runner:

* **Futures (`CollectorApp`)** -- has the quality-event WAL; its quality writer is batched (P0-3).
* **Bybit (`BybitCollectorApp`)** -- **has a quality-event WAL** (P0-2 parity), with the same append / in-flight / checkpoint-after-publish contract. It keeps one-row segments, so each event publishes its own segment.
* **Binance Spot, OKX and OKX capture-only** -- provide **no supported quality WAL** for replay or recovery. Their one-row segments are the only durability mechanism for quality events (see `docs/F5_FATAL_STORAGE_TOPOLOGY.md`, "No quality WAL").
