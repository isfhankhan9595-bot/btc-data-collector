# P0-3: Quality-Event Parquet Segmentation

## The problem

Every live runner's quality-event `ParquetWriter` used
`segment_rows=1, segment_seconds=1` — one Parquet segment (plus its
`.meta.json` sidecar) per single quality event. For a 24/7 collector, any
sustained run of gaps/recoveries/duplicates/errors creates one tiny file
per incident (inode pressure, slow directory listing, backup/rsync cost).

## Critical finding: `run_collector.py` is deliberately excluded

While implementing the fix, inspection of `run_collector.py`'s WAL
integration (P0-2, merged as PR #69) surfaced a real, not hypothetical,
interaction:

P0-2's checkpoint advances immediately after
`ParquetWriter.write()` returns without raising. Under the **old**
`segment_rows=1` configuration, that was safe: `write()` synchronously
triggers `_close_segment()` inline once the row threshold is hit, and with
a threshold of 1, every single `write()` call closed, fsync'd, and
published its own segment before returning — so "write() succeeded" and
"durably on disk" were the same moment.

Under **batched** `segment_rows`/`segment_seconds`, `write()` returns
immediately after merely appending to `self.buffer` for most calls. If
`run_collector.py`'s quality writer were batched the same way as the other
four runners, P0-2's checkpoint would advance to cover quality events that
are only sitting in memory — meaning a crash between checkpoint advancement
and the next segment flush would **both** mark those events as already
durable (so WAL recovery would never replay them) **and** actually lose
them, since they were never published. That is a genuine data-loss
interaction between two P0-level changes, not a theoretical concern.

**Resolution:** `run_collector.py`'s quality writer stays at
`segment_rows=1, segment_seconds=1`, unchanged, until P0-2's checkpoint
logic is made to wait for actual segment publication rather than `write()`
returning. That is P0-2's concern, not this task's — fixing it here would
expand scope into P0-2's architecture, which this task is explicitly not
authorized to do. The exclusion is pinned by
`test_run_collector_quality_writer_deliberately_stays_at_segment_rows_1`,
so a future change that batches it without first fixing the checkpoint
dependency will be caught, not silently reintroduce the interaction.

The other four live runners — Bybit, OKX collector, OKX capture utility,
Binance Spot — have **no WAL or checkpoint logic at all** (confirmed by
searching each for `_quality_wal`/`checkpoint`), so this interaction does
not apply to them, and all four are batched.

## What was NOT weakened, for the four batched runners

`ParquetWriter._close_segment()` already publishes every segment
atomically (`fsync` + `os.replace` + directory `fsync`) regardless of
`segment_rows`/`segment_seconds` — confirmed by reading the method before
changing anything. This fix does not touch that mechanism and does not
weaken the durability of any segment that has been published. Graceful
shutdown already flushes a partial batch via `close()` →
`_finalize_segment()` — confirmed by reading, and by
`test_partial_batch_is_flushed_on_graceful_shutdown`.

## The real, honestly-stated tradeoff (for the four batched runners)

Quality events sitting in the buffer between flushes are lost if the
process crashes before the next flush. Raising `segment_rows`/
`segment_seconds` means a longer unflushed window than `segment_rows=1`
did (up to 500 events or 30 seconds, whichever comes first, versus
effectively one event before). **There is no WAL for these four runners**
to eliminate that window — this is a deliberate, bounded trade of a small,
rare crash-loss window for a large, certain reduction in file count, not a
claim the window has been eliminated.

## Configuration

`collector/collector/config.py`:

```python
QUALITY_SEGMENT_ROWS = 500
QUALITY_SEGMENT_SECONDS = 30
```

One authoritative location, imported by the four eligible runners.
`run_collector.py` is excluded as described above.

**Why 500/30:** targets roughly 10–20 segments for a 10,000-event burst
(pinned exactly at 20 by
`test_burst_of_quality_sized_events_produces_far_fewer_segments_than_events`),
and keeps the crash-loss window small relative to how rarely quality
events fire in normal operation — each one represents a discrete incident
(a gap, a recovery, a disconnect), not routine per-message traffic.

## Empirically verified behavior of `ParquetWriter` (not assumed)

- The time threshold is checked **on the next `write()` call**, not by a
  background timer — confirmed by sleeping past the threshold with no
  further writes and observing zero segments published, then publishing
  exactly one segment on the next `write()` call
  (`test_timer_threshold_flushes_without_a_further_write_once_crossed_on_next_write`).
- Old config (1/1): 10, 50, 200 events → 10, 50, 200 segments respectively,
  reproduced fresh on this branch's `main`, not reused from an earlier
  report.
- New config (500/30): 10,000 events → exactly 20 segments.

## Mutation testing performed

| Mutation | Result |
|---|---|
| M1 — restore `segment_rows=1` for one runner (Bybit) | Caught by the config-consistency test |
| M3 — change `QUALITY_SEGMENT_ROWS` to a value that doesn't divide 10,000 cleanly (333) | Caught by the pinned-count burst test |

Both mutations were applied to real production source, confirmed to break
the relevant test, then restored and reverified green before proceeding.

## Tests

`test_parquet_writer.py` — 7 new tests: burst file-count reduction (pinned
exactly), before/after comparison on the current branch, timer-threshold
behavior (empirically characterized, not assumed), partial-batch shutdown
flush, write-order preservation within a batched segment, a regression
guard for the four eligible runners sharing the config, and a regression
guard pinning `run_collector.py`'s deliberate exclusion. Two pre-existing
tests in `test_bybit_collector.py` that assumed immediate-flush and read
published `.seg` files directly were fixed to check `writer.buffer`
instead — the events are still correctly recorded, just not yet flushed,
which is the intended new behavior.

**Full suite: 1459 passed.** `compileall` clean, `git diff --check` clean,
standalone import check clean for all five runners (including the
unmodified `run_collector.py`).

## Not built, explicitly

- `run_collector.py`'s quality writer is not batched — see above.
- No byte-size threshold — quality-event rows are small, fixed-shape, and
  don't justify one.
- No new metrics beyond existing structured logging.
- No live exchange session backs any of this; verified with synthetic
  writes and reproduced measurements through the real `ParquetWriter` code
  path on this branch.
