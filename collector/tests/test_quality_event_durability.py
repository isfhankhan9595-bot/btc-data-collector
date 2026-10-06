"""P0-2 + P0-3 unified quality-event durability.

Invariant under test:

    If WAL checkpoint N is on disk, every quality event with WAL seq <= N is
    durably present in a PUBLISHED quality Parquet segment.

An autouse spy re-checks that invariant at EVERY ``QualityEventWAL.checkpoint``
call in every test below (against the real files on disk, by quality_event_id),
so a false checkpoint anywhere fails the test that caused it -- not just the
tests that assert on it.

Everything runs against the real ``CollectorApp()`` constructor (real
``_recover_quality_wal``, real writers, real shutdown), with failures injected
at the exact syscall (os.replace / os.fsync). A "crash" abandons every writer
and the WAL handle without publishing or checkpointing anything.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import stat

import pyarrow.parquet as pq
import pytest

import run_collector
from collector.collector import parquet_writer as pw
from collector.collector.config import QUALITY_EVENTS_SCHEMA
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import QualityEventType
from collector.collector.quality_wal import CHECKPOINT_FILENAME, QualityEventWAL

CollectorApp = run_collector.CollectorApp


# ----------------------------------------------------------------- helpers
def _qdir(root):
    return root / "data" / "raw" / "quality_events"


def _published_rows(root):
    rows = []
    for f in sorted(_qdir(root).glob("*.seg")):
        rows.extend(pq.read_table(f).to_pylist())
    return rows


def _seg_files(root):
    return sorted(_qdir(root).glob("*.seg"))


def _published_ids(stream_dir):
    ids = set()
    for f in stream_dir.glob("*.seg"):
        ids.update(i for i in pq.read_table(f, columns=["quality_event_id"]).column(0).to_pylist() if i)
    return ids


def _disk_ckpt(root) -> int:
    path = _qdir(root) / "wal" / CHECKPOINT_FILENAME
    # Fresh WAL seqs start at 0; -1 means "nothing checkpointed".
    return json.loads(path.read_text())["checkpointed_seq"] if path.exists() else -1


def _wal_pending(root):
    return QualityEventWAL.recover(_qdir(root) / "wal")


def _reasons(rows, prefix="ev"):
    return [r["reason"] for r in rows if r["reason"].startswith(prefix)]


def _real_app(root, monkeypatch):
    monkeypatch.chdir(root)
    os.makedirs("data", exist_ok=True)
    return CollectorApp()


def _crash(app):
    """Process death: nothing is published, nothing is checkpointed, locks and
    handles vanish. Unpublished .seg.tmp files stay on disk, exactly as after
    a kill -9."""
    for obj in list(vars(app).values()):
        if isinstance(obj, ParquetWriter):
            try:
                if obj.writer is not None:
                    obj.writer.close()
            except Exception:
                pass
            obj.writer = None
            obj._closed = True
            obj._release_lock()
    wal = getattr(app, "_quality_wal", None)
    if wal is not None:
        try:
            wal._handle.close()
        except Exception:
            pass


def _emit(app, n, prefix="ev"):
    for i in range(n):
        app._websocket_quality_event(QualityEventType.CONNECT, f"{prefix}{i}", connection_id=f"c{i}")


def _drain(app):
    app.running = False
    asyncio.run(app._quality_persistence_loop())


def _direct(app, reason):
    app._persist_quality_event({"exchange": "BINANCE", "stream": "orderbook",
                                "event_type": QualityEventType.ERROR, "reason": reason})


@pytest.fixture(autouse=True)
def _checkpoint_invariant_spy(monkeypatch):
    real = QualityEventWAL.checkpoint
    violations = []

    def spy(self, up_to_seq):
        try:
            pending = QualityEventWAL.recover(self.wal_dir)
        except Exception:
            pending = None
        if pending is not None:
            published = _published_ids(self.wal_dir.parent)
            missing = [r["seq"] for r in pending
                       if r["seq"] <= up_to_seq and r["quality_event_id"] not in published]
            if missing:
                violations.append((up_to_seq, missing))
        return real(self, up_to_seq=up_to_seq)

    monkeypatch.setattr(QualityEventWAL, "checkpoint", spy)
    yield violations
    assert not violations, f"checkpoint covered unpublished WAL seqs: {violations}"


def _fail_replace_for(monkeypatch, suffix):
    real = os.replace

    def flaky(src, dst, *a, **kw):
        if str(dst).endswith(suffix):
            raise OSError(28, "ENOSPC (injected)")
        return real(src, dst, *a, **kw)
    monkeypatch.setattr(os, "replace", flaky)
    return real


# ======================================================================
# Writer contract: on_segment_durable
# ======================================================================
def _writer(tmp_path, **kw):
    kw.setdefault("segment_rows", 3)
    kw.setdefault("segment_seconds", 3600)
    return ParquetWriter("quality_events", QUALITY_EVENTS_SCHEMA, base_dir=str(tmp_path), **kw)


def _row(i=0):
    row = {f.name: None for f in QUALITY_EVENTS_SCHEMA}
    row.update(timestamp=1_780_000_000_000 + i, exchange="BINANCE", stream="s", event_type="ERROR", reason=f"r{i}")
    return row


def test_durable_hook_runs_only_after_rename_and_directory_fsync(tmp_path, monkeypatch):
    trace = []
    real_replace, real_fsync = os.replace, os.fsync

    def replace(src, dst, *a, **kw):
        if str(dst).endswith(".seg"):
            trace.append("rename")
        return real_replace(src, dst, *a, **kw)

    def fsync(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            trace.append("dirfsync")
        return real_fsync(fd)

    def hook(path, count):
        trace.append(("hook", path.exists(), count))

    monkeypatch.setattr(os, "replace", replace)
    monkeypatch.setattr(os, "fsync", fsync)
    w = _writer(tmp_path, on_segment_durable=hook)
    for i in range(3):
        w.write(_row(i))
    assert trace[:3] == ["rename", "dirfsync", ("hook", True, 3)]
    w.close()


def test_durable_hook_not_called_when_rename_fails(tmp_path, monkeypatch):
    calls = []
    w = _writer(tmp_path, on_segment_durable=lambda p, n: calls.append(n))
    w.write(_row(0)); w.write(_row(1))
    _fail_replace_for(monkeypatch, ".seg")
    with pytest.raises(OSError):
        w.write(_row(2))
    assert calls == []
    assert list(tmp_path.glob("raw/quality_events/*.seg")) == []


def test_durable_hook_not_called_when_directory_fsync_fails(tmp_path, monkeypatch):
    calls = []
    real_fsync = os.fsync
    w = _writer(tmp_path, on_segment_durable=lambda p, n: calls.append(n))
    w.write(_row(0)); w.write(_row(1))

    def fsync(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(5, "EIO (injected)")
        return real_fsync(fd)
    monkeypatch.setattr(os, "fsync", fsync)
    with pytest.raises(OSError):
        w.write(_row(2))
    assert calls == []


def test_durable_hook_failure_neither_unpublishes_nor_poisons_the_writer(tmp_path):
    def boom(path, count):
        raise RuntimeError("hook bug")
    w = _writer(tmp_path, on_segment_durable=boom)
    for i in range(3):
        w.write(_row(i))
    assert len(list(tmp_path.glob("raw/quality_events/*.seg"))) == 1      # still published
    for i in range(3, 6):
        w.write(_row(i))                                                   # still writable
    w.close()
    assert len(list(tmp_path.glob("raw/quality_events/*.seg"))) == 2


def test_two_publication_callbacks_are_independent_apis(tmp_path):
    seen = {}
    w = _writer(tmp_path,
                on_segment_published=lambda token, path: seen.setdefault("published", (token, path.name)),
                on_segment_durable=lambda path, n: seen.setdefault("durable", (path.name, n)))
    for i in range(3):
        w.write(_row(i))
    assert seen["published"][1] == seen["durable"][0]
    assert seen["durable"][1] == 3 and isinstance(seen["published"][0], tuple)
    w.close()


def test_publish_open_segment_is_noop_when_idle_and_publishes_when_pending(tmp_path):
    w = _writer(tmp_path, segment_rows=500)
    w.publish_open_segment()
    assert list(tmp_path.glob("raw/quality_events/*.seg")) == []           # no empty file
    w.write(_row(0))
    w.publish_open_segment()
    assert len(list(tmp_path.glob("raw/quality_events/*.seg"))) == 1
    w.close()


# ---------------------------------------------------------- time-based flush
def test_segment_seconds_has_no_background_timer(tmp_path, monkeypatch):
    """Documented behaviour: the time threshold is only evaluated inside
    write()/publish_if_due(). An idle writer keeps its pending tail in RAM."""
    clock = {"t": 1000.0}
    monkeypatch.setattr(pw.time, "monotonic", lambda: clock["t"])
    w = _writer(tmp_path, segment_rows=500, segment_seconds=30, age_from_first_row=True)
    w.write(_row(0))
    clock["t"] += 3600                                                     # an hour of silence
    assert list(tmp_path.glob("raw/quality_events/*.seg")) == []           # nothing happened by itself
    assert w.has_unpublished_rows()
    w.close()                                                              # shutdown publishes the tail
    assert len(list(tmp_path.glob("raw/quality_events/*.seg"))) == 1


def test_publish_if_due_respects_the_threshold(tmp_path, monkeypatch):
    clock = {"t": 1000.0}
    monkeypatch.setattr(pw.time, "monotonic", lambda: clock["t"])
    w = _writer(tmp_path, segment_rows=500, segment_seconds=30, age_from_first_row=True)
    assert w.publish_if_due() is False                                     # idle: no empty file
    w.write(_row(0))
    clock["t"] += 29
    assert w.publish_if_due() is False
    clock["t"] += 2
    assert w.publish_if_due() is True
    assert len(list(tmp_path.glob("raw/quality_events/*.seg"))) == 1
    w.close()


def test_age_from_first_row_stops_sparse_events_each_becoming_a_file(tmp_path, monkeypatch):
    clock = {"t": 1000.0}
    monkeypatch.setattr(pw.time, "monotonic", lambda: clock["t"])
    legacy = _writer(tmp_path / "legacy", segment_rows=500, segment_seconds=30)
    fixed = _writer(tmp_path / "fixed", segment_rows=500, segment_seconds=30, age_from_first_row=True)
    clock["t"] += 600                                                      # long quiet spell, then one event
    legacy.write(_row(0)); fixed.write(_row(0))
    assert len(list((tmp_path / "legacy").glob("raw/quality_events/*.seg"))) == 1    # old: 1-row file
    assert len(list((tmp_path / "fixed").glob("raw/quality_events/*.seg"))) == 0     # new: batches
    legacy.close(); fixed.close()


# ======================================================================
# Production wiring
# ======================================================================
def test_production_quality_writer_is_batched_and_wired_to_the_durable_hook(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    qw = app.quality_writer
    assert (qw.segment_rows, qw.segment_seconds) == (run_collector.QUALITY_SEGMENT_ROWS,
                                                     run_collector.QUALITY_SEGMENT_SECONDS) == (500, 30.0)
    assert qw.on_segment_durable == app._on_quality_segment_durable
    assert app._quality_queue.maxsize > 0
    app.shutdown()


# ======================================================================
# Scenario A -- 500 events: bounded files, checkpoint == published boundary
# ======================================================================
def test_A_500_events_one_segment_checkpoint_matches_published_boundary(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    _emit(app, 500)
    _drain(app)
    rows = _published_rows(tmp_path)
    assert len(_seg_files(tmp_path)) == 1
    assert _reasons(rows) == [f"ev{i}" for i in range(500)]               # FIFO order preserved
    assert _disk_ckpt(tmp_path) == 499                                     # seqs 0..499 all published
    assert _wal_pending(tmp_path) == []
    app.shutdown()


def test_A_1200_events_three_files_not_1200(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    _emit(app, 1200)
    _drain(app)                                                            # 1024 queued + 176 overflowed-direct
    app.shutdown()
    assert len(_seg_files(tmp_path)) <= 4
    assert len(_reasons(_published_rows(tmp_path))) == 1200
    assert _wal_pending(tmp_path) == []


# ======================================================================
# Scenario B -- 499 events then crash
# ======================================================================
def test_B_499_events_then_crash_are_recoverable_with_no_false_checkpoint(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    _emit(app, 499)
    _drain(app)
    assert _seg_files(tmp_path) == []                                      # still only in RAM + WAL
    assert _disk_ckpt(tmp_path) == -1
    assert len(_wal_pending(tmp_path)) == 499
    _crash(app)

    app2 = _real_app(tmp_path, monkeypatch)                                # restart: real recovery path
    assert set(_reasons(_published_rows(tmp_path))) == {f"ev{i}" for i in range(499)}
    assert _disk_ckpt(tmp_path) >= 498
    ids = [r["quality_event_id"] for r in _published_rows(tmp_path) if r["reason"].startswith("ev")]
    assert len(ids) == len(set(ids)) == 499
    app2.shutdown()


# ======================================================================
# Scenario C -- Parquet publication fails after the WAL append
# ======================================================================
def test_C_publication_failure_leaves_wal_authoritative_and_checkpoint_unmoved(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    _emit(app, 499)
    _drain(app)                                                            # 499 buffered
    real_replace = _fail_replace_for(monkeypatch, ".seg")
    app.running = False
    _emit(app, 1, prefix="ev_trigger")                                     # 500th row -> publish -> rename fails
    asyncio.run(app._quality_persistence_loop())                           # loop must SURVIVE the failure
    assert app._quality_checkpoint_is_blocked()
    assert _disk_ckpt(tmp_path) == -1
    assert len(_wal_pending(tmp_path)) == 500                              # every event still in the WAL
    monkeypatch.setattr(os, "replace", real_replace)
    _crash(app)

    app2 = _real_app(tmp_path, monkeypatch)
    got = set(_reasons(_published_rows(tmp_path)))
    assert got == {f"ev{i}" for i in range(499)} | {"ev_trigger0"}
    assert _disk_ckpt(tmp_path) >= 499
    app2.shutdown()


def test_C_directory_fsync_failure_does_not_checkpoint_and_is_recoverable(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    _emit(app, 499)
    _drain(app)
    real_fsync = os.fsync

    def fsync(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(5, "EIO (injected)")
        return real_fsync(fd)
    monkeypatch.setattr(os, "fsync", fsync)
    _emit(app, 1, prefix="ev_trigger")
    _drain(app)
    monkeypatch.setattr(os, "fsync", real_fsync)
    assert _disk_ckpt(tmp_path) == -1
    assert app._quality_checkpoint_is_blocked()
    _crash(app)
    app2 = _real_app(tmp_path, monkeypatch)
    assert {f"ev{i}" for i in range(499)} | {"ev_trigger0"} <= set(_reasons(_published_rows(tmp_path)))
    app2.shutdown()


def test_persistence_loop_survives_a_failing_event_and_keeps_draining(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    real = app._persist_quality_event

    def selective(event):
        if event.get("reason") == "ev1":
            raise RuntimeError("bad event")
        real(event)
    app._persist_quality_event = selective
    _emit(app, 4)
    _drain(app)                                                            # must not raise / hang
    assert app._quality_queue.empty()
    assert app._quality_checkpoint_is_blocked()
    app.shutdown()
    assert {"ev0", "ev2", "ev3"} <= set(_reasons(_published_rows(tmp_path)))
    assert _disk_ckpt(tmp_path) <= 0                                       # never past failed seq 1 (latch may hold it lower) 2
    assert any(r["reason"] == "ev1" for r in _wal_pending(tmp_path))


def test_direct_persist_failure_is_not_swallowed(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    monkeypatch.setattr(app.quality_writer, "write", lambda *a, **k: (_ for _ in ()).throw(OSError("disk")))
    with pytest.raises(OSError):
        _direct(app, "ev_direct")
    assert app._quality_checkpoint_is_blocked()
    assert any(r["reason"] == "ev_direct" for r in _wal_pending(tmp_path))  # WAL evidence preserved
    _crash(app)


# ======================================================================
# Scenario D -- direct _persist_quality_event call
# ======================================================================
def test_D_direct_call_has_the_same_wal_contract_as_a_queued_event(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    for i in range(3):
        _direct(app, f"ev_direct{i}")
    assert _seg_files(tmp_path) == []                                      # batched, not one file per call
    pending = [r for r in _wal_pending(tmp_path) if r["reason"].startswith("ev_direct")]
    assert len(pending) == 3 and all(r["quality_event_id"] for r in pending)
    _crash(app)

    app2 = _real_app(tmp_path, monkeypatch)
    rows = [r for r in _published_rows(tmp_path) if r["reason"].startswith("ev_direct")]
    assert sorted(r["reason"] for r in rows) == ["ev_direct0", "ev_direct1", "ev_direct2"]
    assert {r["quality_event_id"] for r in rows} == {p["quality_event_id"] for p in pending}
    app2.shutdown()


def test_D_direct_calls_do_not_explode_into_files(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    for i in range(600):
        _direct(app, f"ev_direct{i}")
    assert len(_seg_files(tmp_path)) == 1                                  # 500 published, 100 pending
    assert _disk_ckpt(tmp_path) == 499
    app.shutdown()
    assert len(_seg_files(tmp_path)) == 2
    assert _disk_ckpt(tmp_path) == 599


def test_D_wal_append_failure_force_publishes_instead_of_leaving_a_ram_only_row(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    real = app._quality_wal.append
    app._quality_wal.append = lambda e: (_ for _ in ()).throw(OSError(5, "EIO"))
    _direct(app, "ev_unprotected")
    app._quality_wal.append = real
    assert any(r["reason"] == "ev_unprotected" for r in _published_rows(tmp_path))   # on disk NOW
    _crash(app)


def test_D_wal_failure_and_publish_failure_is_loud_and_blocks_the_checkpoint(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    app._quality_wal.append = lambda e: (_ for _ in ()).throw(OSError(5, "EIO"))
    _fail_replace_for(monkeypatch, ".seg")
    with pytest.raises(OSError):
        _direct(app, "ev_doomed")
    assert app._quality_checkpoint_is_blocked()
    _crash(app)


# ======================================================================
# The headline checkpoint bug: a queued event must hold the checkpoint back
# ======================================================================
def test_queued_event_holds_the_checkpoint_while_later_direct_events_publish(tmp_path, monkeypatch):
    """seq 0 is WAL-appended and sits in the queue; 500 direct events (seq
    1..500) fill and publish a segment. A max-based checkpoint would claim
    500 and lose seq 0 on a crash. The checkpoint must stay below 0."""
    app = _real_app(tmp_path, monkeypatch)
    app._websocket_quality_event(QualityEventType.CONNECT, "ev_queued", connection_id="c")
    for i in range(500):
        _direct(app, f"ev_direct{i}")
    assert len(_seg_files(tmp_path)) == 1                                  # a segment WAS published
    assert _disk_ckpt(tmp_path) == -1
    assert any(r["reason"] == "ev_queued" for r in _wal_pending(tmp_path))
    _crash(app)

    app2 = _real_app(tmp_path, monkeypatch)
    assert "ev_queued" in {r["reason"] for r in _published_rows(tmp_path)}
    app2.shutdown()


def test_hour_rollover_publish_does_not_checkpoint_the_event_that_triggered_it(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    for i in range(3):
        _direct(app, f"ev_h1_{i}")
    qw = app.quality_writer
    real_hour = qw._get_current_hour_str
    monkeypatch.setattr(qw, "_get_current_hour_str", lambda: "2999-01-01-00")
    _direct(app, "ev_h2_0")                                                # rolls the hour: publishes the first 3
    assert len(_seg_files(tmp_path)) == 1
    assert _disk_ckpt(tmp_path) == 2                                       # seqs 0..2 only: ev_h2_0 (seq 3) is not in any file yet
    assert [r["reason"] for r in _wal_pending(tmp_path) if r["reason"].startswith("ev_h2")] == ["ev_h2_0"]
    monkeypatch.setattr(qw, "_get_current_hour_str", real_hour)
    _crash(app)


# ======================================================================
# Scenario E -- queue overflow
# ======================================================================
def test_E_real_queue_is_bounded_and_overflow_is_not_silent(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    cap = app._quality_queue.maxsize
    _emit(app, cap + 50)
    assert app._quality_queue.qsize() <= cap                               # stays bounded
    assert app._quality_overflow == 50
    assert len(_wal_pending(tmp_path)) >= cap + 50                         # nothing dropped from the WAL
    _drain(app)
    app.shutdown()
    got = set(_reasons(_published_rows(tmp_path)))
    assert got == {f"ev{i}" for i in range(cap + 50)}                      # no silent analytical loss
    assert _wal_pending(tmp_path) == []


def test_E_overflow_with_failed_direct_persist_stays_recoverable(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    app._quality_queue = asyncio.Queue(maxsize=1)
    real = app._persist_quality_event
    app._persist_quality_event = lambda e: (_ for _ in ()).throw(OSError("down")) if e.get("reason") == "ev1" else real(e)
    _emit(app, 3)                                                          # ev0 queued; ev1 (fails), ev2 overflow
    assert app._quality_checkpoint_is_blocked()
    assert app._quality_overflow_unpersisted == 1
    app._persist_quality_event = real
    _drain(app)
    app.shutdown()
    assert _disk_ckpt(tmp_path) <= 0                                       # seq 1 (ev1) unresolved: checkpoint never reaches it
    assert any(r["reason"] == "ev1" for r in _wal_pending(tmp_path))       # evidence retained for restart
    app2 = _real_app(tmp_path, monkeypatch)
    assert "ev1" in {r["reason"] for r in _published_rows(tmp_path)}
    app2.shutdown()


# ======================================================================
# Scenario F -- recovery replays into the batched writer
# ======================================================================
def test_F_recovered_events_are_published_before_the_checkpoint_advances(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    _emit(app, 40)
    _drain(app)
    _crash(app)
    assert _disk_ckpt(tmp_path) == -1

    order = []
    real = QualityEventWAL.checkpoint

    def spy(self, up_to_seq):
        order.append((up_to_seq, len(_published_ids(self.wal_dir.parent))))
        return real(self, up_to_seq=up_to_seq)
    monkeypatch.setattr(QualityEventWAL, "checkpoint", spy)
    app2 = _real_app(tmp_path, monkeypatch)
    assert order and order[0][0] >= 39 and order[0][1] >= 40               # published first, checkpoint second
    rows = _published_rows(tmp_path)
    assert _reasons(rows) == [f"ev{i}" for i in range(40)]                 # replay preserved seq order
    app2.shutdown()


def test_F_recovery_checkpoints_only_the_contiguous_prefix(tmp_path, monkeypatch):
    """WAL seqs 0..4 (ev0..ev4); replay: ev0 ok, ev1 ok, ev2 FAILS, ev3/ev4
    not replayed. Checkpoint must stop at 1; ev2,ev3,ev4 stay recoverable --
    it must NOT advance to 4."""
    app = _real_app(tmp_path, monkeypatch)
    _emit(app, 5)
    _drain(app)
    _crash(app)

    real_persist = CollectorApp._persist_quality_event

    def selective(self, event):
        if event.get("reason") == "ev2":
            raise RuntimeError("cannot persist")
        return real_persist(self, event)
    monkeypatch.setattr(CollectorApp, "_persist_quality_event", selective)
    app2 = _real_app(tmp_path, monkeypatch)
    monkeypatch.setattr(CollectorApp, "_persist_quality_event", real_persist)
    assert _disk_ckpt(tmp_path) == 1
    assert [r["reason"] for r in _wal_pending(tmp_path) if r["reason"].startswith("ev")] == ["ev2", "ev3", "ev4"]
    assert app2._quality_checkpoint_is_blocked()
    _crash(app2)

    app3 = _real_app(tmp_path, monkeypatch)                                # next start resolves the rest
    assert {"ev2", "ev3", "ev4"} <= set(_reasons(_published_rows(tmp_path)))
    app3.shutdown()


def test_F_corrupt_wal_is_preserved_reported_and_never_checkpointed_over(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    _emit(app, 3)
    _drain(app)
    _crash(app)
    wal_dir = _qdir(tmp_path) / "wal"
    victim = sorted(wal_dir.glob("*.wal"))[0] if list(wal_dir.glob("*.wal")) else sorted(wal_dir.iterdir())[0]
    victim.write_text(victim.read_text() + "{this is not json\n")
    app2 = _real_app(tmp_path, monkeypatch)
    assert app2._quality_checkpoint_is_blocked()
    assert victim.exists()
    app2.shutdown()
    assert victim.exists()                                                 # never deleted
    assert any(r["reason"].startswith("quality_wal_corruption_on_startup") for r in _published_rows(tmp_path))


# ======================================================================
# Scenario G -- shutdown with a partial batch
# ======================================================================
def test_G_shutdown_publishes_the_partial_batch_then_checkpoints_then_closes_the_wal(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    _emit(app, 100)                                                        # still sitting in the QUEUE
    for i in range(23):
        _direct(app, f"ev_direct{i}")                                      # and 23 buffered direct
    wal = app._quality_wal
    seen = []
    real = QualityEventWAL.checkpoint

    def spy(self, up_to_seq):
        seen.append((up_to_seq, self._closed, len(_published_ids(self.wal_dir.parent))))
        return real(self, up_to_seq=up_to_seq)
    monkeypatch.setattr(QualityEventWAL, "checkpoint", spy)
    app.shutdown()

    rows = _published_rows(tmp_path)
    assert {r["reason"] for r in rows if re.fullmatch(r"ev\d+", r["reason"])} == {f"ev{i}" for i in range(100)}
    assert {r["reason"] for r in rows if r["reason"].startswith("ev_direct")} == {f"ev_direct{i}" for i in range(23)}
    assert seen and seen[-1][0] == 122
    assert all(closed is False for _, closed, _ in seen)                   # WAL still open at every checkpoint
    assert wal._closed is True                                             # ...and closed afterwards
    assert _disk_ckpt(tmp_path) == 122
    assert _wal_pending(tmp_path) == []


def test_G_quality_writer_close_failure_keeps_everything_recoverable_and_still_closes_the_rest(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    _emit(app, 10)
    _drain(app)
    real_replace = os.replace

    def flaky(src, dst, *a, **kw):
        if str(dst).endswith(".seg") and "quality_events" in str(dst):
            raise OSError(28, "ENOSPC (injected)")
        return real_replace(src, dst, *a, **kw)
    monkeypatch.setattr(os, "replace", flaky)
    app.shutdown()                                                         # must not raise
    monkeypatch.setattr(os, "replace", real_replace)
    assert app._quality_wal._closed is True                                # WAL still closed
    assert app.ob_writer._closed is True                                   # other writers still closed
    assert _disk_ckpt(tmp_path) == -1
    assert app._quality_checkpoint_is_blocked()

    app2 = _real_app(tmp_path, monkeypatch)
    assert {f"ev{i}" for i in range(10)} <= set(_reasons(_published_rows(tmp_path)))
    app2.shutdown()


def test_G_other_writers_close_failure_report_lands_in_the_published_quality_batch(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    real_close = app.ob_writer.close

    def bad_close():
        real_close()
        raise OSError("ob close failed")
    app.ob_writer.close = bad_close
    app.shutdown()
    assert any(r["reason"].startswith("storage_shutdown_close_failed:ob_writer")
               for r in _published_rows(tmp_path))
    assert _wal_pending(tmp_path) == []


def test_idle_tick_publishes_the_tail_without_any_new_event(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    app.quality_writer.segment_seconds = 0.2

    async def go():
        app.running = True
        task = asyncio.create_task(app._quality_persistence_loop())
        _emit(app, 3)
        await asyncio.sleep(1.0)
        published_while_running = len(_seg_files(tmp_path))
        app.running = False
        await task
        return published_while_running
    assert asyncio.run(go()) == 1
    assert _disk_ckpt(tmp_path) == 2
    app.shutdown()


# ======================================================================
# WAL checkpoint failure semantics through the collector
# ======================================================================
def test_checkpoint_write_failure_latches_and_never_advances_memory_or_disk(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    _emit(app, 500)
    real_replace = _fail_replace_for(monkeypatch, CHECKPOINT_FILENAME)
    _drain(app)                                                            # publish OK, checkpoint write fails
    monkeypatch.setattr(os, "replace", real_replace)
    assert len(_seg_files(tmp_path)) == 1
    assert _disk_ckpt(tmp_path) == -1
    assert app._quality_wal._checkpointed_seq == -1                        # memory never ran ahead of disk
    assert app._quality_checkpoint_is_blocked()
    _crash(app)

    app2 = _real_app(tmp_path, monkeypatch)                                # crash after publish, before checkpoint
    rows = [r for r in _published_rows(tmp_path) if r["reason"].startswith("ev")]
    ids = [r["quality_event_id"] for r in rows]
    assert len(rows) == 1000 and len(set(ids)) == 500                      # duplicates, but reconcilable by id
    assert _disk_ckpt(tmp_path) >= 499
    app2.shutdown()


def test_wal_fsync_failure_uses_forced_publish_and_keeps_id_lineage(tmp_path, monkeypatch):
    app = _real_app(tmp_path, monkeypatch)
    wal_fd = app._quality_wal._handle.fileno()
    real_fsync = os.fsync

    def only_wal(fd):
        if fd == wal_fd:
            raise OSError(5, "EIO (injected)")
        return real_fsync(fd)
    monkeypatch.setattr(os, "fsync", only_wal)
    app._websocket_quality_event(QualityEventType.ERROR, "ev_fsync_dies")
    monkeypatch.setattr(os, "fsync", real_fsync)
    row = [r for r in _published_rows(tmp_path) if r["reason"] == "ev_fsync_dies"]
    assert len(row) == 1 and row[0]["quality_event_id"]
    assert f'"quality_event_id": "{row[0]["quality_event_id"]}"' in app._quality_wal._active_path.read_text()
    _crash(app)
