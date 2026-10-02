"""P0-2: CollectorApp-level integration with the quality-event WAL.

Unlike test_quality_wal.py (which tests QualityEventWAL standalone),
these tests exercise the real wiring in run_collector.py:
_websocket_quality_event -> WAL append -> queue -> _persist_quality_event
-> checkpoint, and startup recovery. A minimal real CollectorApp is built
(CollectorApp.__new__, matching test_run_collector_routing.py's own
established convention) with real ParquetWriter/QualityEventWAL instances
pointed at tmp_path -- not mocks -- so the actual file-level durability
claims are the thing under test.
"""
from __future__ import annotations

import asyncio
import json

import pyarrow.parquet as pq
import pytest

from collector import run_collector as _run_collector
from collector.collector.book_engine import LocalBook
from collector.collector.config import QUALITY_EVENTS_SCHEMA
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import QualityEventType
from collector.collector.quality_wal import QualityEventWAL

CollectorApp = _run_collector.CollectorApp


def _minimal_app(tmp_path, *, wal_dir=None, segment_rows=1, segment_seconds=1):
    """A real quality_writer + real WAL, nothing else from __init__.

    Default segment_rows=1/segment_seconds=1 (pre-P0-3 durability model):
    every write() is also an immediate publish, so
    _on_quality_segment_published fires every time and checkpoint-on-publish
    is equivalent to the old checkpoint-on-write these tests assert on.
    """
    app = CollectorApp.__new__(CollectorApp)
    app.binance_book = LocalBook("BINANCE")
    app._quality_pending_max_wal_seq = None
    app._quality_checkpoint_blocked = False
    app.quality_writer = ParquetWriter("quality_events", QUALITY_EVENTS_SCHEMA, base_dir=str(tmp_path),
                                       segment_rows=segment_rows, segment_seconds=segment_seconds,
                                       segment_publish_hook=app._on_quality_segment_published)
    wal_dir = wal_dir or (app.quality_writer.stream_dir / "wal")
    app._quality_wal = QualityEventWAL(wal_dir)
    app._quality_queue = asyncio.Queue(maxsize=1024)
    app._quality_overflow = 0
    return app


def _quality_rows(tmp_path):
    files = sorted((tmp_path / "raw" / "quality_events").glob("*.seg"))
    rows = []
    for f in files:
        rows.extend(pq.read_table(f).to_pylist())
    return rows


def test_websocket_quality_event_survives_a_simulated_crash_before_drain(tmp_path):
    """The exact scenario P0-2 exists to fix: an event enters
    _websocket_quality_event's queue but the process is imagined to die
    before _quality_persistence_loop ever drains it. Old behavior: only a
    queue-depth marker survived, the event itself was gone. New behavior:
    the WAL has the exact event, recoverable by a fresh process."""
    app = _minimal_app(tmp_path)
    app._websocket_quality_event(QualityEventType.CONNECT, "ws_connected", connection_id="c1")
    # No drain of app._quality_queue at all -- simulates the crash.

    recovered = QualityEventWAL.recover(app._quality_wal.wal_dir)
    assert len(recovered) == 1
    assert recovered[0]["reason"] == "ws_connected"
    assert recovered[0]["connection_id"] == "c1"
    assert recovered[0]["exchange"] == "BINANCE"


def test_normal_drain_persists_to_parquet_and_checkpoints_the_wal(tmp_path):
    app = _minimal_app(tmp_path)
    app._websocket_quality_event(QualityEventType.DISCONNECT, "ws_dropped")
    event = app._quality_queue.get_nowait()
    app._persist_quality_event(event)
    app._quality_wal.checkpoint(up_to_seq=event["_wal_seq"])

    rows = _quality_rows(tmp_path)
    assert len(rows) == 1
    assert rows[0]["reason"] == "ws_dropped"
    assert rows[0]["quality_event_id"] == event["quality_event_id"]
    # Fully checkpointed and drained -- nothing left to recover.
    assert QualityEventWAL.recover(app._quality_wal.wal_dir) == []


def test_crash_before_checkpoint_can_duplicate_but_is_reconcilable_by_id(tmp_path):
    """Test 4's essence: WAL -> Parquet publish succeeds -> the checkpoint
    fsync itself fails/crashes right at that point -> restart. The event is
    legitimately replayed again (a duplicate Parquet row IS possible in
    this narrow window, and the task explicitly permits that) -- but both
    rows carry the identical quality_event_id, which is what makes the
    duplicate reconcilable rather than silently ambiguous. No event is
    ever silently lost.

    Exercises the REAL production path: _persist_quality_event (which
    itself WAL-appends, tracks the pending seq, and writes) and
    ParquetWriter's real on_segment_published callback -- only the WAL's
    own checkpoint() call is made to fail, simulating a crash/fsync
    failure landing in exactly that narrow window. ParquetWriter already
    catches and logs an on_segment_published exception rather than
    un-publishing the segment (see parquet_writer.py's _close_segment),
    so the Parquet row is durably published while the checkpoint is not --
    the exact race this test proves survives.
    """
    app = _minimal_app(tmp_path)  # segment_rows=1: write() == immediate publish
    real_checkpoint = app._quality_wal.checkpoint
    app._quality_wal.checkpoint = lambda **kw: (_ for _ in ()).throw(OSError("simulated crash"))

    app._websocket_quality_event(QualityEventType.RESYNC, "startup_resync")
    event = app._quality_queue.get_nowait()
    app._persist_quality_event(event)   # Parquet write succeeds; checkpoint fails (caught, logged)

    app._quality_wal.checkpoint = real_checkpoint  # "restart": a healthy WAL/checkpoint again

    # "Restart": a fresh WAL over the same directory recovers the
    # not-yet-checkpointed event and replays it, exactly as CollectorApp's
    # own __init__ does.
    resume_seq = QualityEventWAL.highest_recovered_seq(app._quality_wal.wal_dir)
    recovered = QualityEventWAL.recover(app._quality_wal.wal_dir)
    assert len(recovered) == 1
    for r in recovered:
        app._persist_quality_event(r)

    rows = _quality_rows(tmp_path)
    assert len(rows) == 2                                    # the duplicate is real and expected here
    assert rows[0]["quality_event_id"] == rows[1]["quality_event_id"]   # but reconcilable: same ID
    assert QualityEventWAL.recover(app._quality_wal.wal_dir) == []     # and now fully checkpointed, no further replay


def test_full_startup_recovery_path_matches_constructor_logic(tmp_path):
    """Runs the literal recovery block CollectorApp.__init__ executes
    (copied in spirit, not re-implemented differently) against a real WAL
    with an unflushed event, confirming the actual startup path -- not
    just the WAL primitive in isolation -- replays exact content."""
    app = _minimal_app(tmp_path)
    app._websocket_quality_event(QualityEventType.SEQUENCE_GAP, "gap_before_crash", connection_id="c9")
    app._quality_wal.close()   # simulates process death: no drain, no checkpoint

    wal_dir = app._quality_wal.wal_dir
    resume_seq = QualityEventWAL.highest_recovered_seq(wal_dir)
    recovered_events = QualityEventWAL.recover(wal_dir)
    fresh = CollectorApp.__new__(CollectorApp)
    fresh.binance_book = LocalBook("BINANCE")
    fresh.quality_writer = app.quality_writer
    for recovered in recovered_events:
        fresh._persist_quality_event(recovered)
    fresh._quality_wal = QualityEventWAL(wal_dir, start_seq=resume_seq)
    if recovered_events:
        fresh._quality_wal.checkpoint(up_to_seq=max(r["seq"] for r in recovered_events))

    rows = _quality_rows(tmp_path)
    assert len(rows) == 1
    assert rows[0]["reason"] == "gap_before_crash"
    assert rows[0]["connection_id"] == "c9"


def test_recover_quality_wal_resumes_sequence_correctly_no_id_reuse(tmp_path):
    """Independent audit finding: the WAL-primitive-level test for ID
    stability across a restart manually passes start_seq=resume_seq --
    it does not prove _recover_quality_wal (the real method __init__
    calls) actually wires that correctly itself. This does."""
    app = _minimal_app(tmp_path)
    app._websocket_quality_event(QualityEventType.CONNECT, "before_restart")
    old_id = app._quality_queue.get_nowait()["quality_event_id"]
    app._quality_wal.close()

    wal_dir = app._quality_wal.wal_dir
    fresh = CollectorApp.__new__(CollectorApp)
    fresh.binance_book = LocalBook("BINANCE")
    fresh._quality_pending_max_wal_seq = None
    fresh._quality_checkpoint_blocked = False
    fresh.quality_writer = app.quality_writer
    fresh.quality_writer._segment_publish_hook = fresh._on_quality_segment_published
    fresh._quality_queue = asyncio.Queue(maxsize=1024)
    fresh._recover_quality_wal(wal_dir)   # the real startup path

    fresh._websocket_quality_event(QualityEventType.CONNECT, "after_restart")
    new_id = fresh._quality_queue.get_nowait()["quality_event_id"]

    assert old_id != new_id
    new_seq = int(new_id.rsplit("-", 1)[-1])
    old_seq = int(old_id.rsplit("-", 1)[-1])
    assert new_seq > old_seq   # strictly resumed past the recovered high-water mark, never reused


def test_wal_write_failure_falls_back_to_direct_synchronous_persist(tmp_path, monkeypatch):
    """If the WAL append itself fails, the event must still reach Parquet
    directly rather than being silently dropped (Step 11)."""
    app = _minimal_app(tmp_path)

    def _broken_append(event):
        raise OSError("simulated disk failure")
    monkeypatch.setattr(app._quality_wal, "append", _broken_append)

    app._websocket_quality_event(QualityEventType.ERROR, "disk_trouble")
    assert app._quality_queue.empty()   # never queued -- persisted directly instead
    rows = _quality_rows(tmp_path)
    assert len(rows) == 1
    assert rows[0]["reason"] == "disk_trouble"


def test_double_failure_wal_and_parquet_both_fail_does_not_crash_caller(tmp_path, monkeypatch, caplog):
    """Hostile Area 2: WAL append fails AND the direct-persist fallback
    also fails. _websocket_quality_event is called synchronously from the
    websocket receive/processing path -- an uncaught exception here would
    crash far more than this one event. Must not raise, and the double
    failure must be logged distinctly (not silently folded into the
    single-failure log line) -- checked directly via caplog, not merely
    inferred from 'the call didn't raise'."""
    import logging
    app = _minimal_app(tmp_path)
    monkeypatch.setattr(app._quality_wal, "append",
                        lambda event: (_ for _ in ()).throw(OSError("wal disk full")))
    monkeypatch.setattr(app, "_persist_quality_event",
                        lambda event: (_ for _ in ()).throw(RuntimeError("parquet also broken")))

    with caplog.at_level(logging.ERROR):
        # Must not raise -- half the point of this test.
        app._websocket_quality_event(QualityEventType.ERROR, "catastrophe")

    messages = [r.message for r in caplog.records]
    assert any("quality_event_double_failure_possible_loss" in m for m in messages)


def test_persist_failure_in_the_loop_never_advances_checkpoint_past_it(tmp_path):
    """Hostile Area 4/14: if event N fails to persist but event N+1 (say)
    would have succeeded, checkpointing must never advance past N -- doing
    so would falsely claim N is durable in Parquet when it never landed.
    Drives the real _quality_persistence_loop coroutine, not a
    reimplementation of its logic."""
    import asyncio

    app = _minimal_app(tmp_path)
    app._websocket_quality_event(QualityEventType.CONNECT, "ok_1")
    app._websocket_quality_event(QualityEventType.SEQUENCE_GAP, "fails")
    app._websocket_quality_event(QualityEventType.CONNECT, "ok_2")

    real_persist = app._persist_quality_event

    def _selectively_broken(event):
        if event.get("reason") == "fails":
            raise RuntimeError("simulated parquet failure")
        real_persist(event)
    app._persist_quality_event = _selectively_broken
    app.running = False   # loop exits once the queue drains, since nothing re-enqueues

    asyncio.run(app._quality_persistence_loop())

    # seq 1 (ok_1) legitimately checkpointed; seq 2 (fails) and seq 3
    # (ok_2, even though it individually succeeded) must remain
    # recoverable, since checkpointing 3 would wrongly imply 2 is durable.
    recovered = QualityEventWAL.recover(app._quality_wal.wal_dir)
    recovered_reasons = {r["reason"] for r in recovered}
    assert "fails" in recovered_reasons
    assert "ok_2" in recovered_reasons
    assert "ok_1" not in recovered_reasons   # correctly checkpointed, no longer pending
    rows = _quality_rows(tmp_path)
    assert {r["reason"] for r in rows} == {"ok_1", "ok_2"}   # ok_2 DID land in Parquet -- just not checkpointed


def test_startup_recovery_stops_checkpointing_at_first_persist_failure(tmp_path):
    """Hostile Area 4/14 at the startup-recovery call site specifically:
    the real CollectorApp._recover_quality_wal method (the same one
    __init__ calls) must not checkpoint past a recovered event that fails
    to persist, even when later recovered events in the same batch would
    have succeeded."""
    app = _minimal_app(tmp_path)
    app._websocket_quality_event(QualityEventType.CONNECT, "r1")
    app._websocket_quality_event(QualityEventType.SEQUENCE_GAP, "r2_fails")
    app._websocket_quality_event(QualityEventType.CONNECT, "r3")
    app._quality_wal.close()   # simulate crash: nothing drained, nothing checkpointed

    wal_dir = app._quality_wal.wal_dir

    fresh = CollectorApp.__new__(CollectorApp)
    fresh.binance_book = LocalBook("BINANCE")
    fresh._quality_pending_max_wal_seq = None
    fresh._quality_checkpoint_blocked = False
    fresh.quality_writer = app.quality_writer
    fresh.quality_writer._segment_publish_hook = fresh._on_quality_segment_published
    real_persist = fresh._persist_quality_event

    def _selectively_broken(event):
        if event.get("reason") == "r2_fails":
            raise RuntimeError("simulated parquet failure during recovery")
        real_persist(event)
    fresh._persist_quality_event = _selectively_broken

    fresh._recover_quality_wal(wal_dir)   # the real code path __init__ calls

    # Only r1 (the contiguous prefix before the failure) is checkpointed.
    # r2 (failed) and r3 (never even attempted, since recovery stops at
    # the first failure) both remain recoverable.
    still_pending = QualityEventWAL.recover(wal_dir)
    pending_reasons = {r["reason"] for r in still_pending}
    assert pending_reasons == {"r2_fails", "r3"}
    rows = _quality_rows(tmp_path)
    assert {r["reason"] for r in rows} == {"r1"}
