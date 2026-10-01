"""P0-3: quality-event tiny-file explosion fix.

segment_rows=1/segment_seconds=1 on the quality writer meant every quality
event published its own 1-row Parquet file. Fixed by batching
(segment_rows=500, segment_seconds=30), with the WAL checkpoint moved from
"right after write()" to "only once the segment holding that write is
actually durably published" (`ParquetWriter.segment_publish_hook`) -- so
batching a write into RAM never lets the WAL forget an event before it is
truly on disk. See docs/QUALITY_EVENT_STORAGE.md and
run_collector.py's `_on_quality_segment_published` / `_recover_quality_wal`.

This file exercises the real `CollectorApp`/`ParquetWriter`/`QualityEventWAL`
code, not a reimplementation, following test_quality_wal_collector_integration.py's
established convention.
"""
from __future__ import annotations

import asyncio
import glob
import os
import time

import pyarrow.parquet as pq
import pytest

from collector import run_collector as _run_collector
from collector.collector.book_engine import LocalBook
from collector.collector.config import QUALITY_EVENTS_SCHEMA
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import QualityEventType
from collector.collector.quality_wal import QualityEventWAL

CollectorApp = _run_collector.CollectorApp


def _app(tmp_path, *, segment_rows=500, segment_seconds=30):
    app = CollectorApp.__new__(CollectorApp)
    app.binance_book = LocalBook("BINANCE")
    app._quality_pending_max_wal_seq = None
    app._quality_checkpoint_blocked = False
    app.quality_writer = ParquetWriter("quality_events", QUALITY_EVENTS_SCHEMA, base_dir=str(tmp_path),
                                       segment_rows=segment_rows, segment_seconds=segment_seconds,
                                       segment_publish_hook=app._on_quality_segment_published)
    app._quality_wal = QualityEventWAL(app.quality_writer.stream_dir / "wal")
    app._quality_queue = asyncio.Queue(maxsize=8192)
    app._quality_overflow = 0
    return app


def _rows_dir(tmp_path):
    return os.path.join(str(tmp_path), "raw", "quality_events")


def _seg_files(tmp_path):
    return sorted(glob.glob(os.path.join(_rows_dir(tmp_path), "*.seg")))


def _rows(tmp_path):
    out = []
    for f in _seg_files(tmp_path):
        out.extend(pq.read_table(f).to_pylist())
    return out


def _enqueue_n(app, n, reason_prefix="ev"):
    for i in range(n):
        app._websocket_quality_event(QualityEventType.CONNECT, f"{reason_prefix}_{i}", connection_id=f"c{i}")


async def _drain(app):
    app.running = False  # loop exits once the queue empties, nothing re-enqueues
    await app._quality_persistence_loop()


# ---------------------------------------------------------------------------
# 1-7. File count / batch boundaries / rows-per-file
# ---------------------------------------------------------------------------

def test_default_config_is_batched_not_one_row_per_file():
    """The actual production constructor call, not a test default."""
    import inspect
    src = inspect.getsource(CollectorApp.__init__)
    assert 'ParquetWriter(\n            "quality_events"' in src or '"quality_events", QUALITY_EVENTS_SCHEMA' in src
    assert "segment_rows=1, segment_seconds=1" not in src
    assert "segment_rows=500, segment_seconds=30" in src


def test_one_quality_event_produces_one_file_one_row(tmp_path):
    app = _app(tmp_path)
    _enqueue_n(app, 1)
    asyncio.run(_drain(app))
    app.quality_writer.close()
    rows = _rows(tmp_path)
    assert len(rows) == 1
    assert len(_seg_files(tmp_path)) == 1


def test_many_events_below_the_batch_threshold_stay_in_one_open_segment_until_flushed(tmp_path):
    app = _app(tmp_path, segment_rows=500, segment_seconds=3600)
    _enqueue_n(app, 200)
    asyncio.run(_drain(app))
    # Not yet published: below segment_rows, and segment_seconds is huge.
    assert len(_seg_files(tmp_path)) == 0
    app.quality_writer.close()  # graceful shutdown flushes the tail
    rows = _rows(tmp_path)
    assert len(rows) == 200
    assert len(_seg_files(tmp_path)) == 1  # one file for 200 rows, not 200 files


def test_file_count_reduced_by_roughly_segment_rows_for_a_burst(tmp_path):
    """1500 events at segment_rows=500 -> 3 full files, not 1500 tiny ones."""
    app = _app(tmp_path, segment_rows=500, segment_seconds=3600)
    _enqueue_n(app, 1500)
    asyncio.run(_drain(app))
    app.quality_writer.close()
    files = _seg_files(tmp_path)
    rows_per_file = [pq.ParquetFile(f).metadata.num_rows for f in files]
    assert len(files) == 3
    assert rows_per_file == [500, 500, 500]
    assert len(_rows(tmp_path)) == 1500


def test_time_based_flush_publishes_a_trickle_below_segment_rows(tmp_path):
    """A low event rate must not wait forever for segment_rows: a short
    segment_seconds still bounds staleness. (Contrast:
    test_many_events_below_the_batch_threshold_stay_in_one_open_segment_until_flushed
    shows 200 events with a 1-hour segment_seconds stay unpublished
    indefinitely -- proving this test's publish is genuinely time-driven,
    not something segment_rows would have done anyway.)"""
    app = _app(tmp_path, segment_rows=500, segment_seconds=1)
    app._websocket_quality_event(QualityEventType.CONNECT, "trickle_0")
    asyncio.run(_drain(app))
    assert len(_seg_files(tmp_path)) == 0  # far below segment_rows=500: nothing published yet
    time.sleep(1.05)  # cross segment_seconds
    app._websocket_quality_event(QualityEventType.CONNECT, "trickle_1")
    asyncio.run(_drain(app))
    # The second write's segment-seconds check found the segment opened by
    # the first write stale, and published it -- proving TIME, not row
    # count, triggered this, since only 2 rows exist against segment_rows=500.
    assert len(_seg_files(tmp_path)) == 1
    assert len(_rows(tmp_path)) == 2
    app.quality_writer.close()  # nothing further pending; a no-op flush


def test_burst_of_thousands_of_events(tmp_path):
    app = _app(tmp_path, segment_rows=500, segment_seconds=3600)
    _enqueue_n(app, 4321)
    asyncio.run(_drain(app))
    app.quality_writer.close()
    rows = _rows(tmp_path)
    assert len(rows) == 4321
    assert len(_seg_files(tmp_path)) == 9  # ceil(4321/500)
    assert len({r["quality_event_id"] for r in rows}) == 4321  # no duplication


# ---------------------------------------------------------------------------
# 8-9. Ordering and no duplication
# ---------------------------------------------------------------------------

def test_event_ordering_is_preserved_across_batches(tmp_path):
    app = _app(tmp_path, segment_rows=100, segment_seconds=3600)
    _enqueue_n(app, 350, reason_prefix="ord")
    asyncio.run(_drain(app))
    app.quality_writer.close()
    rows = _rows(tmp_path)
    reasons = [r["reason"] for r in rows]
    assert reasons == [f"ord_{i}" for i in range(350)]


# ---------------------------------------------------------------------------
# 10-12. Crash with pending events: the core new-durability-property proof.
# This is the adversarial test proving batching cannot silently lose an
# event: a "crash" is simulated by abandoning the app WITHOUT calling
# close() (no publish, no flush) -- exactly what a process kill leaves
# behind -- and a fresh app recovers every event from the WAL.
# ---------------------------------------------------------------------------

def test_crash_with_a_full_unpublished_batch_loses_nothing(tmp_path):
    app = _app(tmp_path, segment_rows=500, segment_seconds=3600)
    _enqueue_n(app, 499, reason_prefix="crash")  # below segment_rows: stays buffered, unpublished
    asyncio.run(_drain(app))
    assert len(_seg_files(tmp_path)) == 0  # confirms nothing was durable yet
    wal_dir = app._quality_wal.wal_dir
    # NO publish: simulates os.kill(-9) exactly -- the in-process buffer
    # (499 unpublished rows) is simply gone. abandon() only releases the
    # directory lock so a fresh writer can attach below (a real process
    # exit releases the OS flock the same way); the tmp segment it leaves
    # behind is empty, matching write()'s own buffer-then-flush design.
    app.quality_writer.abandon()

    fresh = _app(tmp_path, segment_rows=500, segment_seconds=3600)
    fresh._quality_wal = QualityEventWAL(wal_dir, start_seq=QualityEventWAL.highest_recovered_seq(wal_dir))
    fresh.quality_writer._segment_publish_hook = fresh._on_quality_segment_published
    fresh._recover_quality_wal(wal_dir)
    fresh.quality_writer.close()

    rows = _rows(tmp_path)
    assert len(rows) == 499
    assert {r["reason"] for r in rows} == {f"crash_{i}" for i in range(499)}


def test_crash_exactly_at_a_batch_boundary_loses_nothing(tmp_path):
    """The batch DID publish (500 rows), but 50 more arrived after and were
    still buffered when the crash happened. Only the published 500 must be
    on disk before recovery; all 550 must exist after."""
    app = _app(tmp_path, segment_rows=500, segment_seconds=3600)
    _enqueue_n(app, 550, reason_prefix="boundary")
    asyncio.run(_drain(app))
    assert len(_rows(tmp_path)) == 500  # the published batch only
    wal_dir = app._quality_wal.wal_dir
    app.quality_writer.abandon()  # crash: release the lock only, nothing flushed

    fresh = _app(tmp_path, segment_rows=500, segment_seconds=3600)
    fresh._quality_wal = QualityEventWAL(wal_dir, start_seq=QualityEventWAL.highest_recovered_seq(wal_dir))
    fresh.quality_writer._segment_publish_hook = fresh._on_quality_segment_published
    fresh._recover_quality_wal(wal_dir)
    fresh.quality_writer.close()

    rows = _rows(tmp_path)
    assert len(rows) == 550
    assert len({r["quality_event_id"] for r in rows}) == 550  # the 500 already-published rows weren't re-duplicated


def test_persistence_failure_never_advances_checkpoint_under_real_batching(tmp_path):
    """The pre-existing checkpoint_blocked safety property, re-verified at
    the real production batch size (not the segment_rows=1 the original
    P0-2 tests happened to use, which made every write also a publish)."""
    app = _app(tmp_path, segment_rows=50, segment_seconds=3600)
    _enqueue_n(app, 10, reason_prefix="ok_before")
    app._websocket_quality_event(QualityEventType.SEQUENCE_GAP, "boom")
    _enqueue_n(app, 10, reason_prefix="ok_after")

    real_persist = app._persist_quality_event

    def _selectively_broken(event):
        if event.get("reason") == "boom":
            raise RuntimeError("simulated parquet failure")
        real_persist(event)
    app._persist_quality_event = _selectively_broken
    asyncio.run(_drain(app))
    app.quality_writer.close()

    # everything landed in Parquet (persist for the other 20 succeeded)...
    rows = _rows(tmp_path)
    assert len(rows) == 20
    # ...but NOTHING is checkpointed, since "boom" (seq 11) failed and the
    # safety rule blocks checkpointing past any failure for the rest of the
    # process's life -- even the 10 that succeeded before it.
    still_pending = QualityEventWAL.recover(app._quality_wal.wal_dir)
    assert len(still_pending) == 21  # 10 ok_before + boom + 10 ok_after, all still in the WAL


# ---------------------------------------------------------------------------
# 15-16, 20. Shutdown, retry/recovery, restart, concurrent arrival while flushing
# ---------------------------------------------------------------------------

def test_graceful_shutdown_publishes_and_checkpoints_the_tail(tmp_path):
    app = _app(tmp_path, segment_rows=500, segment_seconds=3600)
    _enqueue_n(app, 17)
    asyncio.run(_drain(app))
    app.quality_writer.close()  # the actual shutdown() call site
    assert len(_rows(tmp_path)) == 17
    assert QualityEventWAL.recover(app._quality_wal.wal_dir) == []  # nothing left pending


def test_restart_after_clean_shutdown_does_not_replay_anything(tmp_path):
    app = _app(tmp_path, segment_rows=500, segment_seconds=3600)
    _enqueue_n(app, 12)
    asyncio.run(_drain(app))
    app.quality_writer.close()
    wal_dir = app._quality_wal.wal_dir

    fresh = _app(tmp_path, segment_rows=500, segment_seconds=3600)
    fresh._quality_wal = QualityEventWAL(wal_dir, start_seq=QualityEventWAL.highest_recovered_seq(wal_dir))
    fresh.quality_writer._segment_publish_hook = fresh._on_quality_segment_published
    fresh._recover_quality_wal(wal_dir)
    fresh.quality_writer.close()
    assert len(_rows(tmp_path)) == 12  # unchanged: nothing duplicated on a clean restart


def test_concurrent_arrival_while_a_batch_is_being_persisted(tmp_path):
    """New events enqueued mid-drain must not be lost or reordered ahead of
    events already being persisted."""
    app = _app(tmp_path, segment_rows=500, segment_seconds=3600)

    async def scenario():
        app.running = True
        task = asyncio.create_task(app._quality_persistence_loop())
        _enqueue_n(app, 5, "first")
        await asyncio.sleep(0.05)
        _enqueue_n(app, 5, "second")
        await asyncio.sleep(0.05)
        app.running = False
        await task
    asyncio.run(scenario())
    app.quality_writer.close()
    rows = _rows(tmp_path)
    assert [r["reason"] for r in rows] == [f"first_{i}" for i in range(5)] + [f"second_{i}" for i in range(5)]


# ---------------------------------------------------------------------------
# 21-22. Queue backpressure and memory boundedness
# ---------------------------------------------------------------------------

def test_queue_overflow_does_not_lose_the_event_it_is_still_durable_in_the_wal(tmp_path):
    app = _app(tmp_path, segment_rows=500, segment_seconds=3600)
    app._quality_queue = asyncio.Queue(maxsize=2)
    _enqueue_n(app, 5)  # 2 fit the queue, 3 overflow (but each was WAL-appended first)
    assert app._quality_overflow == 3
    asyncio.run(_drain(app))
    app.quality_writer.close()
    # The overflow counter itself is recorded as a quality event (existing
    # behaviour, unaffected by batching); the 2 that fit the queue landed too.
    reasons = {r["reason"] for r in _rows(tmp_path)}
    assert "quality_queue_overflow" in reasons


def test_open_segment_buffer_never_exceeds_segment_rows(tmp_path):
    """Memory boundedness: the in-process buffer for an unpublished segment
    is bounded by segment_rows, not by total events received."""
    app = _app(tmp_path, segment_rows=100, segment_seconds=3600)
    _enqueue_n(app, 950)
    asyncio.run(_drain(app))
    assert len(app.quality_writer.buffer) < 100
    assert app.quality_writer.record_count < 100


# ---------------------------------------------------------------------------
# 24-25. rows_lost / timestamp integrity unaffected by batching
# ---------------------------------------------------------------------------

def test_rows_lost_and_timestamp_fields_pass_through_batching_unchanged(tmp_path):
    app = _app(tmp_path, segment_rows=500, segment_seconds=3600)
    receive_ts = 1_700_000_000_000
    process_ts = 1_700_000_000_777  # deliberately distinct from receive_ts
    app._persist_quality_event({"stream": "orderbook", "event_type": QualityEventType.DATA_DROP,
                                "reason": "gap", "rows_lost": 42,
                                "local_receive_ts": receive_ts, "local_process_ts": process_ts})
    app.quality_writer.close()
    row = _rows(tmp_path)[0]
    assert row["rows_lost"] == "42"  # schema stores rows_lost as text (existing contract)
    # P0-6 causal contract, unaffected by P0-3 batching: receive time and
    # processing time are distinct and neither substitutes for the other.
    # (timestamp columns read back as tz-aware datetimes; compare as epoch ms.)
    def ms(dt): return int(dt.timestamp() * 1000)
    assert ms(row["local_receive_ts"]) == receive_ts
    assert ms(row["local_process_ts"]) == process_ts
    assert row["local_receive_ts"] != row["local_process_ts"]


# ---------------------------------------------------------------------------
# 18-19. Legacy compatibility: old 1-row-per-file segments still read fine
# ---------------------------------------------------------------------------

def test_legacy_one_row_per_file_segments_still_read_correctly(tmp_path):
    """A directory of old segment_rows=1-era files (one row each) must
    still be readable exactly as before -- batching is a going-forward
    change, not a rewrite of history."""
    app = _app(tmp_path, segment_rows=1, segment_seconds=1)  # exactly the old config
    _enqueue_n(app, 5, reason_prefix="legacy")
    asyncio.run(_drain(app))
    app.quality_writer.close()
    files = _seg_files(tmp_path)
    assert len(files) == 5  # confirms the old pathological behaviour, for contrast
    assert all(pq.ParquetFile(f).metadata.num_rows == 1 for f in files)
    assert len(_rows(tmp_path)) == 5


def test_empty_quality_stream_produces_no_files(tmp_path):
    app = _app(tmp_path, segment_rows=500, segment_seconds=30)
    app.quality_writer.close()
    assert _seg_files(tmp_path) == []


# ---------------------------------------------------------------------------
# 13-14. Persistence failure / retry already covered by the checkpoint test
# above (test_persistence_failure_never_advances_checkpoint_under_real_batching)
# and by test_quality_wal_collector_integration.py's existing double-failure
# and WAL-write-failure tests, which are unaffected by this change (they
# exercise _websocket_quality_event's synchronous fallback, not batching).
# ---------------------------------------------------------------------------


def test_production_shutdown_publishes_the_quality_writer_not_abandons_it():
    """The real shutdown() method (not a reimplementation) must end up
    calling quality_writer.close() -- which publishes and checkpoints the
    tail -- never abandon() or any other discard path. A mutation that
    swaps this to abandon() would otherwise pass every other test in this
    file, since they all call quality_writer.close() directly rather than
    going through shutdown().

    P0-1's shutdown/finalization audit (merged after this test was first
    written) wraps every writer's close() in
    _close_writer_reporting_failure(writer_name) so one writer's failure
    can't abort closing the rest; shutdown() itself now calls
    self._close_writer_reporting_failure("quality_writer") rather than
    self.quality_writer.close() directly. Both are checked: shutdown()
    must route quality_writer through that helper (never abandon() or a
    bespoke path), and the helper itself must call .close() via getattr,
    never .abandon()."""
    import inspect
    shutdown_src = inspect.getsource(CollectorApp.shutdown)
    assert 'self._close_writer_reporting_failure("quality_writer")' in shutdown_src
    assert "self.quality_writer.abandon()" not in shutdown_src
    helper_src = inspect.getsource(CollectorApp._close_writer_reporting_failure)
    assert "writer.close()" in helper_src
    assert "writer.abandon()" not in helper_src
