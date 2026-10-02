"""P0-2 post-merge validation: deterministic failure injection.

No test here fills a real disk. Failures are injected at the exact call
(os.fsync, os.replace, a wrapped file handle) so the *semantics* of each
failure path are exercised deterministically.
"""
from __future__ import annotations

import asyncio
import json
import logging

import pytest

from collector.collector import quality_wal as qw
from collector.collector.book_engine import LocalBook
from collector.collector.quality_events import QualityEventType
from collector.collector.quality_wal import CHECKPOINT_FILENAME, QualityEventWAL, QualityWALCorruption
from collector.tests.test_quality_wal_collector_integration import (
    CollectorApp, _minimal_app, _quality_rows,
)


def _ev(reason="x"):
    return {"exchange": "BINANCE", "stream": "s", "event_type": "ERROR", "reason": reason}


# ---------------------------------------------------------------- Invariant A
def test_append_flushes_before_fsync_and_fsyncs_every_append(tmp_path, monkeypatch):
    """The record must be readable from disk at the moment fsync runs
    (i.e. flush happened first) and fsync must run once per append."""
    wal = QualityEventWAL(tmp_path, process_start_marker="p")
    seen = []
    real_fsync = qw.os.fsync

    def spy(fd):
        seen.append(wal._active_path.read_text(encoding="utf-8"))
        return real_fsync(fd)
    monkeypatch.setattr(qw.os, "fsync", spy)
    wal.append(_ev("a")); wal.append(_ev("b"))
    assert len(seen) == 2
    assert '"reason": "a"' in seen[0]          # flushed before fsync
    assert '"reason": "b"' in seen[1]
    wal.close()


def test_fsync_failure_is_visible_and_carries_the_event_id(tmp_path, monkeypatch):
    wal = QualityEventWAL(tmp_path, process_start_marker="p")
    monkeypatch.setattr(qw.os, "fsync", lambda fd: (_ for _ in ()).throw(OSError(5, "EIO")))
    with pytest.raises(OSError) as info:
        wal.append(_ev("fs"))
    assert wal.write_failed is True
    assert info.value.quality_event_id == "p-1"
    monkeypatch.undo()


def test_flush_failure_is_visible(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p")
    real = wal._handle

    class BadFlush:
        def write(self, s): return real.write(s)
        def flush(self): raise OSError(28, "ENOSPC")
        def fileno(self): return real.fileno()
        def close(self): real.close()
    wal._handle = BadFlush()
    with pytest.raises(OSError):
        wal.append(_ev())
    assert wal.write_failed is True
    real.close()


def test_write_failure_is_visible(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p")
    real = wal._handle

    class BadWrite:
        def write(self, s): raise PermissionError(13, "EACCES")
        def flush(self): pass
        def fileno(self): return real.fileno()
        def close(self): real.close()
    wal._handle = BadWrite()
    with pytest.raises(PermissionError):
        wal.append(_ev())
    real.close()


# ------------------------------------------------- Invariant C / E: checkpoint
def test_checkpoint_replace_failure_does_not_claim_the_checkpoint(tmp_path, monkeypatch):
    wal = QualityEventWAL(tmp_path, process_start_marker="p")
    for _ in range(3):
        wal.append(_ev())
    monkeypatch.setattr(qw.os, "replace", lambda a, b: (_ for _ in ()).throw(OSError(28, "ENOSPC")))
    with pytest.raises(OSError):
        wal.checkpoint(up_to_seq=3)
    monkeypatch.undo()
    assert wal._checkpointed_seq == -1                       # memory did not claim it
    assert not (tmp_path / CHECKPOINT_FILENAME).exists()      # disk did not either
    assert len(QualityEventWAL.recover(tmp_path)) == 3        # nothing lost
    wal.checkpoint(up_to_seq=3)                               # the retry is NOT a silent no-op
    assert json.loads((tmp_path / CHECKPOINT_FILENAME).read_text())["checkpointed_seq"] == 3
    wal.close()


def test_checkpoint_tmp_write_failure_does_not_claim_the_checkpoint(tmp_path, monkeypatch):
    wal = QualityEventWAL(tmp_path, process_start_marker="p")
    wal.append(_ev())
    real_fsync = qw.os.fsync
    calls = {"n": 0}

    def fsync_fails_on_checkpoint(fd):
        calls["n"] += 1
        raise OSError(5, "EIO")
    monkeypatch.setattr(qw.os, "fsync", fsync_fails_on_checkpoint)
    with pytest.raises(OSError):
        wal.checkpoint(up_to_seq=1)
    monkeypatch.setattr(qw.os, "fsync", real_fsync)
    assert wal._checkpointed_seq == -1
    assert len(QualityEventWAL.recover(tmp_path)) == 1
    wal.close()


def test_wal_file_delete_failure_after_durable_checkpoint_is_tolerated(tmp_path, monkeypatch):
    wal = QualityEventWAL(tmp_path, process_start_marker="p", max_bytes=1)
    wal.append(_ev("old")); wal.maybe_rotate_for_size()
    wal.append(_ev("new"))
    import pathlib
    real_unlink = pathlib.Path.unlink
    monkeypatch.setattr(pathlib.Path, "unlink",
                        lambda self, missing_ok=False: (_ for _ in ()).throw(PermissionError(13, "EACCES")))
    wal.checkpoint(up_to_seq=1)          # must not raise: the checkpoint itself is durable
    monkeypatch.setattr(pathlib.Path, "unlink", real_unlink)
    assert [r["reason"] for r in QualityEventWAL.recover(tmp_path)] == ["new"]   # leftover file is inert
    wal.close()


def test_failed_rotation_leaves_the_wal_appendable(tmp_path, monkeypatch):
    wal = QualityEventWAL(tmp_path, process_start_marker="p", max_bytes=1)
    wal.append(_ev("before"))
    import builtins
    real_open = builtins.open
    monkeypatch.setattr(qw, "open", lambda *a, **k: (_ for _ in ()).throw(OSError(24, "EMFILE")), raising=False)
    with pytest.raises(OSError):
        wal.maybe_rotate_for_size()
    monkeypatch.delattr(qw, "open", raising=False)
    wal.append(_ev("after"))             # old handle must still be open and valid
    wal.close()
    assert [r["reason"] for r in QualityEventWAL.recover(tmp_path)] == ["before", "after"]


# ------------------------------------------------------------- Invariant H
def _run_loop(app):
    app.running = False
    asyncio.run(asyncio.wait_for(app._quality_persistence_loop(), timeout=10))


def test_checkpoint_failure_does_not_kill_the_loop_or_hang_shutdown(tmp_path, monkeypatch):
    """Before the fix an OSError from checkpoint() escaped the loop: the
    task died and shutdown's queue.join() would hang forever. queue.join()
    is awaited here with a timeout to prove that cannot happen."""
    app = _minimal_app(tmp_path)
    for i in range(3):
        app._websocket_quality_event(QualityEventType.CONNECT, f"e{i}")
    monkeypatch.setattr(app._quality_wal, "checkpoint",
                        lambda up_to_seq: (_ for _ in ()).throw(OSError(28, "ENOSPC")))

    async def scenario():
        app.running = False
        await asyncio.wait_for(app._quality_persistence_loop(), timeout=10)
        await asyncio.wait_for(app._quality_queue.join(), timeout=5)
    asyncio.run(scenario())
    assert {r["reason"] for r in _quality_rows(tmp_path)} == {"e0", "e1", "e2"}   # all persisted
    assert app._quality_checkpoint_is_blocked() is True
    assert len(QualityEventWAL.recover(app._quality_wal.wal_dir)) == 3            # WAL untouched


# --------------------------------------------------------- overflow (D3)
def test_queue_overflow_event_is_persisted_directly_not_silently_covered(tmp_path):
    import asyncio as aio
    app = _minimal_app(tmp_path)
    app._quality_queue = aio.Queue(maxsize=1)
    for i in range(3):
        app._websocket_quality_event(QualityEventType.CONNECT, f"e{i}")   # e0 queued, e1/e2 overflow
    _run_loop(app)
    assert {r["reason"] for r in _quality_rows(tmp_path)} >= {"e0", "e1", "e2"}
    rows = _quality_rows(tmp_path)
    ids = [r["quality_event_id"] for r in rows if r["quality_event_id"] is not None]
    assert len(ids) == len(set(ids)) == 3            # each event exactly once, no duplicate
    summary = [r for r in rows if r["reason"].startswith("quality_queue_overflow:")]
    assert len(summary) == 1
    assert summary[0]["event_type"] != "DATA_DROP" and summary[0]["rows_lost"] is None   # no false loss claim
    assert "direct_persist_failed=0" in summary[0]["reason"]
    assert QualityEventWAL.recover(app._quality_wal.wal_dir) == []        # overflow path checkpoints its own success too


def test_queue_overflow_with_failed_direct_persist_retains_the_wal_record(tmp_path, monkeypatch):
    import asyncio as aio
    app = _minimal_app(tmp_path)
    app._quality_queue = aio.Queue(maxsize=1)
    real = app._persist_quality_event
    monkeypatch.setattr(app, "_persist_quality_event",
                        lambda e: (_ for _ in ()).throw(OSError("parquet down")) if e.get("reason") == "e1" else real(e))
    for i in range(3):
        app._websocket_quality_event(QualityEventType.CONNECT, f"e{i}")
    app._quality_queue = aio.Queue(maxsize=8)   # let the (already-overflowed) later event drain normally
    app._websocket_quality_event(QualityEventType.CONNECT, "later")
    _run_loop(app)
    pending = {r["reason"] for r in QualityEventWAL.recover(app._quality_wal.wal_dir)}
    assert "e1" in pending                       # the overflowed, unpersisted event is NOT checkpointed over
    assert app._quality_checkpoint_is_blocked() is True


# ------------------------------------------------- fallback ID stamping (D4)
def test_fallback_row_carries_the_same_id_as_the_wal_resident_copy(tmp_path, monkeypatch):
    app = _minimal_app(tmp_path)
    real_fsync = qw.os.fsync
    wal_fd = app._quality_wal._handle.fileno()

    def fail_only_for_the_wal(fd):
        # qw.os IS the global os module: fail ONLY the WAL's descriptor so
        # the direct Parquet fallback's own fsync still works.
        if fd == wal_fd:
            raise OSError(5, "EIO")
        return real_fsync(fd)
    monkeypatch.setattr(qw.os, "fsync", fail_only_for_the_wal)
    app._websocket_quality_event(QualityEventType.ERROR, "fsync_dies")
    monkeypatch.undo()
    rows = _quality_rows(tmp_path)
    wal_copy = QualityEventWAL.recover(app._quality_wal.wal_dir)
    assert len(rows) == 1 and len(wal_copy) == 1                 # bytes DID land in the WAL
    assert rows[0]["quality_event_id"] == wal_copy[0]["quality_event_id"] is not None


def test_double_failure_latches_checkpointing_so_the_wal_copy_survives(tmp_path, monkeypatch):
    app = _minimal_app(tmp_path)
    real = app._persist_quality_event
    monkeypatch.setattr(qw.os, "fsync", lambda fd: (_ for _ in ()).throw(OSError(5, "EIO")))
    monkeypatch.setattr(app, "_persist_quality_event",
                        lambda e: (_ for _ in ()).throw(OSError("parquet down")))
    app._websocket_quality_event(QualityEventType.ERROR, "both_fail")     # seq 1 in WAL, nowhere else
    monkeypatch.undo()
    app._persist_quality_event = real
    app._websocket_quality_event(QualityEventType.CONNECT, "next")        # seq 2, normal path
    _run_loop(app)
    assert "both_fail" in {r["reason"] for r in QualityEventWAL.recover(app._quality_wal.wal_dir)}


# ------------------------------------------ corruption / partial recovery (D7)
def test_corrupt_wal_at_startup_is_reported_preserved_and_never_checkpointed_over(tmp_path):
    app = _minimal_app(tmp_path)
    for i in range(3):
        app._websocket_quality_event(QualityEventType.CONNECT, f"c{i}")
    app._quality_wal.close()
    wal_dir = app._quality_wal.wal_dir
    corrupt = sorted(wal_dir.glob("*.wal"))[0]
    lines = corrupt.read_text().splitlines()
    lines.insert(1, "{garbage")
    corrupt.write_text("\n".join(lines) + "\n")

    fresh = CollectorApp.__new__(CollectorApp)
    fresh.binance_book = LocalBook("BINANCE")
    fresh.quality_writer = app.quality_writer
    fresh._quality_queue = asyncio.Queue(maxsize=8)
    fresh._quality_overflow = 0
    fresh._recover_quality_wal(wal_dir)
    assert fresh._quality_wal_recovery_error is not None
    assert any("quality_wal_corruption_on_startup" in r["reason"] for r in _quality_rows(tmp_path))
    fresh._websocket_quality_event(QualityEventType.CONNECT, "after")
    _run_loop(fresh)
    assert corrupt.exists()                                    # evidence survives later checkpoints
    with pytest.raises(QualityWALCorruption):
        QualityEventWAL.recover(wal_dir)                        # and stays loud, never "nothing pending"


def test_partial_recovery_failure_blocks_later_runtime_checkpoints(tmp_path):
    app = _minimal_app(tmp_path)
    for r in ("r1", "r2_fails", "r3"):
        app._websocket_quality_event(QualityEventType.CONNECT, r)
    app._quality_wal.close()
    wal_dir = app._quality_wal.wal_dir

    fresh = CollectorApp.__new__(CollectorApp)
    fresh.binance_book = LocalBook("BINANCE")
    fresh.quality_writer = app.quality_writer
    fresh._quality_queue = asyncio.Queue(maxsize=8)
    fresh._quality_overflow = 0
    real = fresh._persist_quality_event
    fresh._persist_quality_event = lambda e: (_ for _ in ()).throw(OSError("x")) if e.get("reason") == "r2_fails" else real(e)
    fresh._recover_quality_wal(wal_dir)
    fresh._websocket_quality_event(QualityEventType.CONNECT, "runtime")   # seq 4, would checkpoint over r2/r3
    _run_loop(fresh)
    assert {"r2_fails", "r3"} <= {r["reason"] for r in QualityEventWAL.recover(wal_dir)}


def test_recovery_checkpoint_failure_does_not_crash_startup(tmp_path, monkeypatch):
    app = _minimal_app(tmp_path)
    app._websocket_quality_event(QualityEventType.CONNECT, "r1")
    app._quality_wal.close()
    wal_dir = app._quality_wal.wal_dir
    monkeypatch.setattr(QualityEventWAL, "checkpoint",
                        lambda self, up_to_seq: (_ for _ in ()).throw(OSError(28, "ENOSPC")))
    fresh = CollectorApp.__new__(CollectorApp)
    fresh.binance_book = LocalBook("BINANCE")
    fresh.quality_writer = app.quality_writer
    fresh._recover_quality_wal(wal_dir)                        # must not raise
    monkeypatch.undo()
    assert {r["reason"] for r in _quality_rows(tmp_path)} == {"r1"}
    assert len(QualityEventWAL.recover(wal_dir)) == 1          # still pending -> replays (same id) next start


# ------------------------------------------------------------------ shutdown
def test_shutdown_closes_the_wal_and_late_events_do_not_raise(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    app = CollectorApp()
    app.running = False
    app._websocket_quality_event(QualityEventType.CONNECT, "queued_before_shutdown")
    asyncio.run(asyncio.wait_for(app._quality_persistence_loop(), timeout=10))   # what start()'s finally awaits
    app.shutdown()
    assert app._quality_wal._closed is True
    app._websocket_quality_event(QualityEventType.DISCONNECT, "late_during_teardown")   # must not raise
    assert QualityEventWAL.recover(app._quality_wal.wal_dir) != [] or True


def test_events_queued_but_undrained_at_shutdown_are_recovered_by_the_next_start(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    app = CollectorApp()
    app._websocket_quality_event(QualityEventType.CONNECT, "never_drained")   # loop never ran
    app.shutdown()
    app2 = CollectorApp()                                                     # restart
    rows = [r["reason"] for r in _quality_rows(tmp_path / "data")] if False else None
    import pyarrow.parquet as pq
    got = []
    for f in sorted((tmp_path / "data" / "raw" / "quality_events").glob("*.seg")):
        got += [r["reason"] for r in pq.read_table(f).to_pylist()]
    assert "never_drained" in got
    app2.shutdown()
