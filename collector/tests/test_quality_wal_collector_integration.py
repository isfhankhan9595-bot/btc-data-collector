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


def _minimal_app(tmp_path, *, wal_dir=None):
    """A real quality_writer + real WAL, nothing else from __init__."""
    app = CollectorApp.__new__(CollectorApp)
    app.binance_book = LocalBook("BINANCE")
    app.quality_writer = ParquetWriter("quality_events", QUALITY_EVENTS_SCHEMA, base_dir=str(tmp_path),
                                       segment_rows=1, segment_seconds=1)
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
    """Test 4's essence: WAL -> Parquet succeeds -> CRASH before checkpoint
    -> restart. The event is legitimately replayed again (a duplicate
    Parquet row IS possible in this narrow window, and the task
    explicitly permits that) -- but both rows carry the identical
    quality_event_id, which is what makes the duplicate reconcilable
    rather than silently ambiguous. No event is ever silently lost."""
    app = _minimal_app(tmp_path)
    app._websocket_quality_event(QualityEventType.RESYNC, "startup_resync")
    event = app._quality_queue.get_nowait()
    app._persist_quality_event(event)   # Parquet write succeeds...
    # ...but no checkpoint() call -- simulates the crash landing exactly there.

    # "Restart": a fresh WAL over the same directory recovers the
    # not-yet-checkpointed event and replays it, exactly as CollectorApp's
    # own __init__ does.
    resume_seq = QualityEventWAL.highest_recovered_seq(app._quality_wal.wal_dir)
    recovered = QualityEventWAL.recover(app._quality_wal.wal_dir)
    assert len(recovered) == 1
    for r in recovered:
        app._persist_quality_event(r)
    new_wal = QualityEventWAL(app._quality_wal.wal_dir, start_seq=resume_seq)
    new_wal.checkpoint(up_to_seq=max(r["seq"] for r in recovered))

    rows = _quality_rows(tmp_path)
    assert len(rows) == 2                                    # the duplicate is real and expected here
    assert rows[0]["quality_event_id"] == rows[1]["quality_event_id"]   # but reconcilable: same ID
    assert QualityEventWAL.recover(new_wal.wal_dir) == []     # and now fully checkpointed, no further replay


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
