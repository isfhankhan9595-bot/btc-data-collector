"""P0-3: Bybit quality-event batching (RECONSTRUCTED Stage 1 test file).

The Bybit quality writer batches in production (``QUALITY_SEGMENT_ROWS`` rows /
``QUALITY_SEGMENT_SECONDS`` s measured from the FIRST buffered row, plus an idle publisher task),
where it used to publish one Parquet file per event. Batching means ``write()`` no longer implies
"durable", which is safe only because every event is WAL-appended BEFORE it is buffered and the
checkpoint advances solely from ``on_segment_durable``.

This file pins the *batching* behaviour. Everything runs against the real ``BybitCollectorApp``
(real WAL, real ``ParquetWriter``, real idle task) on temporary storage, with the PRODUCTION
constants in force (it deliberately does not use ``one_row_bybit_quality_segments``). Published
segments and the WAL checkpoint are read back from disk.

States are kept distinct throughout:
    WAL-appended   -> a record exists in the WAL (``_wal_records``)
    in-flight      -> seq is in ``app._quality_wal_inflight`` (appended, not yet in a published segment)
    buffered       -> the row sits in the open writer segment (RAM / ``.seg.tmp``), NOT readable as ``.seg``
    published      -> a ``*.seg`` file on disk contains the row
    checkpointed   -> ``checkpoint.json`` on disk covers the seq

Determinism: ``ParquetWriter`` reads ``time.monotonic()`` through the ``time`` name in its own module.
That single module attribute is replaced with a proxy whose ``monotonic()`` is a test-controlled
clock and which delegates everything else to the real ``time`` module. Nothing global is patched
(not ``time.monotonic`` itself), so asyncio, pytest and every other module keep the real clock. No
test waits for 30 real seconds. The proxy is installed BEFORE the app is constructed so the writer's
segment-open timestamp is on the same clock.

This file is a reconstruction of work lost from a previous workspace; it is not byte-identical to it.

Stage 2A (failure durability and checkpoint ordering) is appended below the Stage 1 tests. It injects
faults at the two real filesystem boundaries -- ``os.replace`` of a segment (publication) and of
``checkpoint.json.tmp`` (checkpoint write) -- and uses the real ``_on_quality_segment_durable`` callback
and the real ``QualityEventWAL.checkpoint``. Nothing mocks a checkpoint value or the publish method.

Stage 2B-1 (WAL-cap fallback under batching) is appended after Stage 2A. The WAL's aggregate cap is
closed through its real admission arithmetic (``QualityEventWAL.append`` refuses the record itself, or
the startup check latches from the on-disk size), and the app's real capacity-fallback branch, writer,
durable callback and checkpoint then run unmodified.

Stage 2B-2 (shutdown and recovery) is appended after Stage 2B-1. ``shutdown()`` is driven for real: the
final partial batch is published by the real ``quality_writer.close()`` and checkpointed by the real
callback before the real WAL is closed. Publication failure is injected at ``os.replace`` of the segment
and at the directory ``fsync``; recovery is the real startup replay of a second app on the same storage.
The ordering tests wrap each real ``close()`` (they call through) so order is observed, not simulated.
"""
from __future__ import annotations

import asyncio
import os
import stat
import time as _real_time
from collections import Counter

import pyarrow.parquet as pq
import pytest

import collector.collector.parquet_writer as pw
import collector.run_bybit_collector as rbc
import collector.run_collector as rc
from collector.collector import notifications
from collector.collector.quality_wal import ENV_MAX_TOTAL_BYTES
from collector.collector.storage_errors import FatalStorageError
from collector.run_bybit_collector import BybitCollectorApp
from collector.tests.test_bybit_quality_wal import (
    _crash, _disk_ckpt, _ev, _pending, _qdir, _rows, _wal_for, _wal_records)
from collector.tests.test_parquet_writer_publication_failure import expect_fatal


# ----------------------------------------------------------------------------- deterministic clock
class _Clock:
    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start
        self.t0 = start

    def advance(self, seconds: float) -> None:
        self.now += seconds

    def set_elapsed(self, seconds: float) -> None:
        """Absolute offset from the clock's start, so thresholds are hit exactly (no float drift)."""
        self.now = self.t0 + seconds


class _TimeProxy:
    """Stands in for the ``time`` module inside ``parquet_writer`` only."""

    def __init__(self, clock: _Clock) -> None:
        self._clock = clock

    def monotonic(self) -> float:
        return self._clock.now

    def __getattr__(self, name):                   # time(), sleep(), ... stay real
        return getattr(_real_time, name)


@pytest.fixture
def clock(monkeypatch):
    c = _Clock()
    monkeypatch.setattr(pw, "time", _TimeProxy(c))
    return c


@pytest.fixture
def app(tmp_path, clock):
    """Real app, production batching constants, on the test clock. Torn down like a process exit."""
    a = BybitCollectorApp(data_dir=str(tmp_path))
    yield a
    _crash(a)


# ----------------------------------------------------------------------------- disk readers
def _segs(root):
    return sorted(_qdir(root).glob("*.seg"))


def _seq(event_id: str) -> int:
    return int(event_id.rsplit("-", 1)[-1])


def _persist(app, n, prefix="b"):
    for i in range(n):
        app._persist_quality_event(_ev(f"{prefix}{i}"))


class _StubClient:
    """Network stand-in: run() awaits start(); shutdown() calls stop()."""

    def __init__(self):
        self.running = False

    async def start(self):
        await asyncio.sleep(3600)

    def stop(self):
        self.running = False


# ======================================================================= 1-3. configuration parity
def test_bybit_thresholds_equal_the_futures_production_constants():
    assert rbc.QUALITY_SEGMENT_ROWS == rc.QUALITY_SEGMENT_ROWS == 500
    assert rbc.QUALITY_SEGMENT_SECONDS == rc.QUALITY_SEGMENT_SECONDS == 30.0


def test_the_real_writer_uses_those_thresholds_and_the_durability_callback(app):
    w = app.quality_writer
    assert w.segment_rows == rbc.QUALITY_SEGMENT_ROWS
    assert w.segment_seconds == rbc.QUALITY_SEGMENT_SECONDS
    assert w._age_from_first_row is True
    assert w.on_segment_durable == app._on_quality_segment_durable


def test_the_idle_tick_interval_is_positive_and_at_most_one_second():
    assert 0 < rbc.QUALITY_IDLE_TICK_SECONDS <= 1.0
    # A due batch must not wait materially longer than its age threshold.
    assert rbc.QUALITY_IDLE_TICK_SECONDS < rbc.QUALITY_SEGMENT_SECONDS


# ======================================================================= 4-6. burst batching
def test_a_burst_of_20_events_shares_one_durable_segment_when_published(app, tmp_path):
    _persist(app, 20)
    app.quality_writer.publish_open_segment()          # explicit publication of the open batch

    (seg,) = _segs(tmp_path)                           # ONE segment, not 20
    rows = _rows(tmp_path)
    wal = _wal_records(tmp_path)
    assert len(rows) == len(wal) == 20
    assert [r["reason"] for r in rows] == [f"b{i}" for i in range(20)]          # content + order
    assert [r["quality_event_id"] for r in rows] == [w["quality_event_id"] for w in wal]
    assert [_seq(r["quality_event_id"]) for r in rows] == sorted(_seq(r["quality_event_id"]) for r in rows)
    assert len({r["quality_event_id"] for r in rows}) == 20
    # published -> checkpointed, nothing left in flight
    assert _disk_ckpt(tmp_path) == max(w["seq"] for w in wal)
    assert app._quality_wal_inflight == set() and _pending(tmp_path) == []


def test_nothing_is_published_or_checkpointed_while_the_burst_is_only_buffered(app, tmp_path):
    _persist(app, 20)

    assert _segs(tmp_path) == []                                    # not published
    assert _disk_ckpt(tmp_path) == -1                               # not checkpointed
    wal = _wal_records(tmp_path)
    assert len(wal) == 20                                           # but WAL-appended
    assert app._quality_wal_inflight == {w["seq"] for w in wal}     # and tracked in flight
    assert len(app.quality_writer.buffer) + app.quality_writer.record_count == 20   # and buffered
    assert [e["seq"] for e in _pending(tmp_path)] == [w["seq"] for w in wal]        # recoverable


def test_at_501_events_the_first_500_publish_and_the_last_stays_in_flight(app, tmp_path):
    _persist(app, 501)

    (seg,) = _segs(tmp_path)
    rows = _rows(tmp_path)
    wal = _wal_records(tmp_path)
    assert len(wal) == 501 and len(rows) == 500
    assert [r["reason"] for r in rows] == [f"b{i}" for i in range(500)]
    last = wal[500]
    assert app._quality_wal_inflight == {last["seq"]}               # the 501st is in flight...
    assert [e["quality_event_id"] for e in _pending(tmp_path)] == [last["quality_event_id"]]
    assert _disk_ckpt(tmp_path) == wal[499]["seq"] < last["seq"]    # ...so the checkpoint stops just below it
    assert len(app.quality_writer.buffer) + app.quality_writer.record_count == 1

    app.quality_writer.publish_open_segment()                       # now the tail publishes too
    assert len(_segs(tmp_path)) == 2 and len(_rows(tmp_path)) == 501
    assert _disk_ckpt(tmp_path) == last["seq"] and app._quality_wal_inflight == set()


# ======================================================================= 7-8. age threshold
def test_a_single_event_is_not_published_at_29_9_seconds_but_is_at_30(app, clock, tmp_path):
    app._persist_quality_event(_ev("lonely"))
    assert _segs(tmp_path) == []

    clock.set_elapsed(29.9)
    app._quality_publish_if_due()                                   # the real idle-tick body
    assert _segs(tmp_path) == [] and _disk_ckpt(tmp_path) == -1
    assert len(app.quality_writer.buffer) + app.quality_writer.record_count == 1

    clock.set_elapsed(30.0)
    app._quality_publish_if_due()
    assert [r["reason"] for r in _rows(tmp_path)] == ["lonely"]
    (rec,) = _wal_records(tmp_path)
    assert _disk_ckpt(tmp_path) == rec["seq"] and app._quality_wal_inflight == set()


def test_age_counts_from_the_first_row_not_from_when_the_segment_opened(app, clock, tmp_path):
    clock.advance(1_000.0)                       # a long idle gap BEFORE the first event
    app._persist_quality_event(_ev("after_idle_gap"))

    # Aged from segment-open this row would be 1000 s old and publish inside write() (or on the first
    # tick). Aged from the first row it is 0 s old: it must stay buffered.
    assert _segs(tmp_path) == []
    app._quality_publish_if_due()
    assert _segs(tmp_path) == [] and _disk_ckpt(tmp_path) == -1

    clock.advance(29.9)
    app._quality_publish_if_due()
    assert _segs(tmp_path) == []                 # 29.9 s after the FIRST ROW: still not due
    clock.advance(0.1)
    app._quality_publish_if_due()
    assert [r["reason"] for r in _rows(tmp_path)] == ["after_idle_gap"]


# ======================================================================= 9-10. the real idle task
async def _until(predicate, *, timeout=5.0):
    deadline = _real_time.monotonic() + timeout
    while not predicate():
        assert _real_time.monotonic() < deadline, "condition not reached within the real-time bound"
        await asyncio.sleep(0.005)


async def test_the_idle_task_starts_via_run_and_publishes_a_quiet_event_without_new_traffic(
        app, clock, tmp_path, monkeypatch):
    monkeypatch.setattr(rbc, "QUALITY_IDLE_TICK_SECONDS", 0.01)     # read by the loop on every iteration
    app.client = _StubClient()
    runner = asyncio.create_task(app.run())                         # real lifecycle entry point
    try:
        await _until(lambda: getattr(app, "_quality_idle_task", None) is not None)
        task = app._quality_idle_task
        assert not task.done()

        app._persist_quality_event(_ev("quiet"))                    # one event, then silence
        await asyncio.sleep(0.05)                                   # several ticks run: not due yet
        assert _segs(tmp_path) == [] and _disk_ckpt(tmp_path) == -1

        clock.set_elapsed(30.0)                                     # age threshold reached; NO new event
        await _until(lambda: _segs(tmp_path) != [])
        assert [r["reason"] for r in _rows(tmp_path)] == ["quiet"]
        (rec,) = _wal_records(tmp_path)
        assert _disk_ckpt(tmp_path) == rec["seq"] and app._quality_wal_inflight == set()
        assert not task.done()                                      # the loop outlives a publication
    finally:
        await app.shutdown()
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)


async def test_shutdown_cancels_and_awaits_the_idle_task(app, clock, tmp_path, monkeypatch):
    monkeypatch.setattr(rbc, "QUALITY_IDLE_TICK_SECONDS", 0.01)
    app.client = _StubClient()
    runner = asyncio.create_task(app.run())
    try:
        await _until(lambda: getattr(app, "_quality_idle_task", None) is not None)
        task = app._quality_idle_task
        assert not task.done()

        await app.shutdown()

        assert task.done() and task.cancelled()                     # cancelled AND already awaited to completion
        assert app._quality_idle_task is None
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)


# ======================================================================= 11. idle ticks with nothing pending
def test_idle_ticks_with_no_pending_rows_create_no_empty_segments_and_do_not_move_the_checkpoint(
        app, clock, tmp_path):
    for _ in range(5):
        clock.advance(1_000.0)
        app._quality_publish_if_due()
    assert _segs(tmp_path) == [] and _wal_records(tmp_path) == [] and _disk_ckpt(tmp_path) == -1

    # And after a real publication, further idle ticks stay silent.
    app._persist_quality_event(_ev("one"))
    clock.advance(30.0)
    app._quality_publish_if_due()
    (seg,) = _segs(tmp_path)
    (rec,) = _wal_records(tmp_path)
    ckpt = _disk_ckpt(tmp_path)
    assert ckpt == rec["seq"]

    for _ in range(5):
        clock.advance(1_000.0)
        assert app.quality_writer.publish_if_due() is False
        app._quality_publish_if_due()
    assert _segs(tmp_path) == [seg]                                 # no second (empty) segment
    assert _disk_ckpt(tmp_path) == ckpt
    assert len(_rows(tmp_path)) == 1


# =========================================================================================== STAGE 2A
# Failure durability and checkpoint ordering. Segment boundaries are the real ones: the 500th buffered
# row publishes a segment from inside write(); the idle tick publishes a due tail; an hour change
# publishes the old segment before the next row is bound. Faults are injected where the filesystem
# would fail, so the real callback / WAL / latch code runs.
@pytest.fixture(autouse=True)
def _alerts():
    """Capture operator alerts (no real notification backend in tests) so they can be asserted."""
    sent: list[str] = []
    notifications.set_notifier(notifications.CallableNotifier(lambda message: sent.append(message) or True))
    yield sent
    notifications.set_notifier(None)


class _Faults:
    """Deterministic, switchable faults at the real publication / checkpoint-write boundaries.

    ``os.replace`` is wrapped (restored by monkeypatch teardown). Only a ``*.seg.tmp`` source (segment
    publication) or ``checkpoint.json.tmp`` (WAL checkpoint write) can fail, and only while armed, so a
    test can let segment 1 publish and fail segment 2. ``*_hits`` prove the fault really fired."""

    def __init__(self, monkeypatch):
        self.fail_segment_rename = False
        self.fail_checkpoint_replace = False
        self.segment_rename_hits = 0
        self.checkpoint_replace_hits = 0
        real = os.replace

        def replace(src, dst, *a, **kw):
            name = str(src)
            if self.fail_segment_rename and name.endswith(".seg.tmp"):
                self.segment_rename_hits += 1
                raise OSError(28, "No space left on device (injected segment rename)")
            if self.fail_checkpoint_replace and name.endswith("checkpoint.json.tmp"):
                self.checkpoint_replace_hits += 1
                raise OSError(5, "I/O error (injected checkpoint replace)")
            return real(src, dst, *a, **kw)
        monkeypatch.setattr(os, "replace", replace)


@pytest.fixture
def faults(monkeypatch):
    return _Faults(monkeypatch)


def _spy_durable(app, root):
    """Wrap (do not replace) the real durable callback: record what it was handed, then run it.

    The production wiring is asserted FIRST: wrapping would otherwise mask a missing or wrong callback
    in the constructor, which is exactly what these ordering tests must be able to detect."""
    assert app.quality_writer.on_segment_durable == app._on_quality_segment_durable, \
        "quality_writer must be wired to the real durable callback in production"
    calls = []
    real = app._on_quality_segment_durable

    def spy(path, record_count):
        call = {"path": path, "record_count": record_count,
                "bound": set(app._quality_segment_seqs),          # seqs attributed to THIS segment
                "inflight_before": set(app._quality_wal_inflight),
                "ckpt_before": _disk_ckpt(root)}
        calls.append(call)
        real(path, record_count)
        call["ckpt_after"] = _disk_ckpt(root)
    app.quality_writer.on_segment_durable = spy
    return calls


def _seqs_in_segment(path):
    return {_seq(r["quality_event_id"]) for r in pq.read_table(path).to_pylist()}


def _tmps(root):
    return sorted(_qdir(root).glob("*.seg.tmp"))


def _seq_range(wal, lo, hi):
    """Seqs of the WAL records at append positions lo..hi-1 (0-based positions, not seq values)."""
    return {w["seq"] for w in wal[lo:hi]}


# ---------------------------------------------------------------- A. actual publication failure
def test_a_failed_idle_publication_is_not_success_and_leaves_every_record_recoverable(
        app, clock, faults, tmp_path, _alerts):
    _persist(app, 3, "idle-")
    wal = _wal_records(tmp_path)
    ids = {w["quality_event_id"] for w in wal}
    seqs = {w["seq"] for w in wal}
    assert _segs(tmp_path) == [] and _disk_ckpt(tmp_path) == -1             # buffered, WAL-appended, in flight

    faults.fail_segment_rename = True
    clock.set_elapsed(30.0)
    app._quality_publish_if_due()                                           # the real idle-tick body

    assert faults.segment_rename_hits == 1                                  # the publication really failed
    # not reported as published or durable
    assert _segs(tmp_path) == [] and _rows(tmp_path) == []
    assert len(_tmps(tmp_path)) == 1                                        # writer contract: .tmp left for orphan recovery
    assert not list(_qdir(tmp_path).glob("*.meta.json"))                    # no publication marker for an unpublished segment
    # not checkpointed; still in flight and attributed to the segment that failed
    assert _disk_ckpt(tmp_path) == -1
    assert app._quality_wal_inflight == seqs and app._quality_segment_seqs == seqs
    assert {e["quality_event_id"] for e in _pending(tmp_path)} == ids       # every record recoverable, original ids
    # the existing latch / observability behaviour
    assert app._quality_checkpoint_blocked is True
    assert app._quality_checkpoint_block_reason == "idle_publish_failed:FatalStorageError"
    status = app.quality_channel_status()
    assert status["quality_degraded"] is True and status["quality_checkpoint_blocked"] is True
    assert status["quality_checkpoint_block_reason"] == "idle_publish_failed:FatalStorageError"
    assert status["quality_failure"]["stage"] == "rename"
    assert len([m for m in _alerts if "QUALITY WAL CHECKPOINT BLOCKED [BYBIT]" in m]) == 1
    # the FAILED writer can never look like a healthy idle stream
    with pytest.raises(FatalStorageError):
        app.quality_writer.publish_if_due()
    app._quality_publish_if_due()                                           # tick is observed, not raised, not a success
    assert _segs(tmp_path) == [] and _disk_ckpt(tmp_path) == -1
    assert len([m for m in _alerts if "QUALITY WAL CHECKPOINT BLOCKED [BYBIT]" in m]) == 1   # alerted once


def test_a_failed_idle_publication_is_replayed_once_with_original_ids_after_a_restart(
        app, clock, faults, tmp_path):
    _persist(app, 3, "replay-")
    originals = {w["quality_event_id"]: w["reason"] for w in _wal_records(tmp_path)}
    faults.fail_segment_rename = True
    clock.set_elapsed(30.0)
    app._quality_publish_if_due()
    assert _segs(tmp_path) == [] and _disk_ckpt(tmp_path) == -1
    _crash(app)

    faults.fail_segment_rename = False
    app2 = BybitCollectorApp(data_dir=str(tmp_path))          # real startup recovery, production batching
    try:
        rows = [r for r in _rows(tmp_path) if r["quality_event_id"] in originals]
        assert {r["quality_event_id"]: r["reason"] for r in rows} == originals     # once each, same ids
        assert len(rows) == 3
        assert [e for e in _pending(tmp_path) if e["quality_event_id"] in originals] == []   # now checkpointed
        assert _disk_ckpt(tmp_path) >= max(_seq(i) for i in originals)
        assert app2._quality_checkpoint_blocked is False
    finally:
        _crash(app2)


def test_a_failed_write_triggered_publication_raises_and_nothing_is_checkpointed(app, faults, tmp_path, _alerts):
    faults.fail_segment_rename = True
    _persist(app, 499)                                                      # buffered; no publication yet
    assert faults.segment_rename_hits == 0 and _disk_ckpt(tmp_path) == -1

    with expect_fatal(OSError, stage="rename") as info:
        app._persist_quality_event(_ev("the-500th"))                        # this row fills the segment
    assert "No space left" in str(info.value.__cause__)                     # the original error is chained
    assert faults.segment_rename_hits == 1

    wal = _wal_records(tmp_path)
    assert len(wal) == 500                                                  # the tripping event is WAL-backed too
    assert _segs(tmp_path) == [] and _rows(tmp_path) == [] and len(_tmps(tmp_path)) == 1
    assert _disk_ckpt(tmp_path) == -1
    assert {e["seq"] for e in _pending(tmp_path)} == {w["seq"] for w in wal}
    assert app._quality_wal_inflight == {w["seq"] for w in wal}
    assert app._quality_checkpoint_blocked is True
    assert app._quality_checkpoint_block_reason == "quality_writer_write_failed"
    assert app.quality_degraded is not None
    assert len([m for m in _alerts if "QUALITY WAL CHECKPOINT BLOCKED [BYBIT]" in m]) == 1


# ---------------------------------------------------------------- B. rollover ordering
def test_rollover_first_segment_checkpoints_only_its_own_sequences_and_the_next_stays_in_flight(
        app, clock, tmp_path):
    calls = _spy_durable(app, tmp_path)
    _persist(app, 537)                                                      # 500 + 37 across the real 500-row boundary
    wal = _wal_records(tmp_path)
    first, second = _seq_range(wal, 0, 500), _seq_range(wal, 500, 537)
    assert len(first) == 500 and len(second) == 37 and not first & second

    # the first segment's callback: attributed to exactly its own 500 sequences
    (call,) = calls
    assert call["record_count"] == 500
    assert call["bound"] == first == _seqs_in_segment(call["path"])         # callback attribution == file contents
    assert call["inflight_before"] == first                                 # the 501st did not exist yet
    assert call["ckpt_before"] == -1 and call["ckpt_after"] == wal[499]["seq"]
    # ...and nothing beyond it
    assert _disk_ckpt(tmp_path) == wal[499]["seq"] < wal[500]["seq"]
    assert app._quality_wal_inflight == second == app._quality_segment_seqs
    assert [e["seq"] for e in _pending(tmp_path)] == [w["seq"] for w in wal[500:]]
    assert len(_segs(tmp_path)) == 1

    # the next segment resolves only through its own durable publication
    clock.set_elapsed(30.0)
    app._quality_publish_if_due()
    assert len(calls) == 2 and len(_segs(tmp_path)) == 2
    assert calls[1]["bound"] == second == _seqs_in_segment(calls[1]["path"])
    assert calls[1]["ckpt_before"] == wal[499]["seq"] and calls[1]["ckpt_after"] == wal[536]["seq"]
    assert _seqs_in_segment(calls[0]["path"]).isdisjoint(_seqs_in_segment(calls[1]["path"]))
    assert app._quality_wal_inflight == set() and _pending(tmp_path) == []


def test_second_segment_publication_failure_keeps_the_checkpoint_at_the_first_segments_boundary(
        app, faults, tmp_path):
    calls = _spy_durable(app, tmp_path)
    _persist(app, 500, "s1-")                                               # segment 1 publishes cleanly
    (first_call,) = calls
    wal = _wal_records(tmp_path)
    seg1_last = wal[499]["seq"]
    assert _disk_ckpt(tmp_path) == seg1_last

    faults.fail_segment_rename = True                                       # segment 2 cannot be published
    _persist(app, 499, "s2-")
    assert faults.segment_rename_hits == 0 and _disk_ckpt(tmp_path) == seg1_last   # buffered: no movement
    with expect_fatal(OSError, stage="rename"):
        app._persist_quality_event(_ev("s2-499"))                           # the 1000th row trips the publish
    assert faults.segment_rename_hits == 1

    wal = _wal_records(tmp_path)
    seg2 = _seq_range(wal, 500, 1000)
    assert len(wal) == 1000 and len(seg2) == 500
    assert len(calls) == 1                                                  # the durable callback never ran for segment 2
    # published evidence: only segment 1 is on disk, and it is intact
    assert _segs(tmp_path) == [first_call["path"]]
    assert _seqs_in_segment(first_call["path"]) == _seq_range(wal, 0, 500)
    assert len(_tmps(tmp_path)) == 1
    # the checkpoint stays exactly at the first unresolved sequence minus one -- not -1, not further
    assert _disk_ckpt(tmp_path) == seg1_last == min(seg2) - 1
    assert app._quality_wal_inflight == seg2 and app._quality_segment_seqs == seg2
    assert {e["seq"] for e in _pending(tmp_path)} == seg2
    assert app._quality_checkpoint_blocked is True
    assert app._quality_checkpoint_block_reason == "quality_writer_write_failed"

    # nothing afterwards can publish or advance past it: the writer is FAILED
    with pytest.raises(FatalStorageError):
        app.quality_writer.publish_open_segment()
    app._persist_quality_event(_ev("after-failure"))                        # latched channel: WAL copy only
    assert _disk_ckpt(tmp_path) == seg1_last and len(_segs(tmp_path)) == 1
    assert max(e["seq"] for e in _pending(tmp_path)) > max(seg2)            # WAL-retained, still uncheckpointed


def test_a_checkpoint_write_failure_on_segment_1_cannot_be_skipped_by_a_later_successful_segment(
        app, faults, tmp_path):
    calls = _spy_durable(app, tmp_path)
    faults.fail_checkpoint_replace = True
    _persist(app, 500, "c1-")                                               # segment 1 publishes; its checkpoint write fails
    assert faults.checkpoint_replace_hits == 1 and len(_segs(tmp_path)) == 1
    assert _disk_ckpt(tmp_path) == -1
    assert app._quality_checkpoint_blocked is True
    assert app._quality_checkpoint_block_reason == "checkpoint_write_failed:OSError"
    assert app._quality_wal_inflight == set()                               # published => resolved in memory...

    faults.fail_checkpoint_replace = False                                  # ...and the disk is healthy again
    _persist(app, 500, "c2-")                                               # segment 2 publishes successfully
    assert len(calls) == 2 and len(_segs(tmp_path)) == 2
    assert faults.checkpoint_replace_hits == 1                              # the latch stopped any second attempt
    # a later successful callback did NOT checkpoint over the earlier, unresolved prefix
    assert _disk_ckpt(tmp_path) == -1 and calls[1]["ckpt_after"] == -1
    wal = _wal_records(tmp_path)
    assert {e["seq"] for e in _pending(tmp_path)} == {w["seq"] for w in wal}   # all 1000 replay (at-least-once)
    assert len(_rows(tmp_path)) == 1000


# ---------------------------------------------------------------- B. hour rollover attribution
def test_hour_rollover_binds_each_event_to_the_segment_that_really_contains_it(app, tmp_path):
    calls = _spy_durable(app, tmp_path)
    w = app.quality_writer
    hour = {"now": w.current_hour}
    w._get_current_hour_str = lambda: hour["now"]                           # instance-level, scoped to this app
    _persist(app, 3, "h1-")
    assert _segs(tmp_path) == [] and len(calls) == 0

    hour["now"] = "2099-01-01-00"                                           # the writer rolls over on the next write
    app._persist_quality_event(_ev("h2-0"))

    wal = _wal_records(tmp_path)
    old, new = _seq_range(wal, 0, 3), _seq_range(wal, 3, 4)
    (call,) = calls                                                         # the old segment was published by the rollover
    # the event that triggered the rollover was WAL-appended and in flight when the old segment
    # published, but it is NOT in that segment
    assert call["inflight_before"] == old | new
    assert call["bound"] == old == _seqs_in_segment(call["path"])
    assert call["ckpt_after"] == wal[2]["seq"] < wal[3]["seq"]
    assert app._quality_segment_seqs == new == app._quality_wal_inflight
    assert [e["quality_event_id"] for e in _pending(tmp_path)] == [wal[3]["quality_event_id"]]

    w.publish_open_segment()                                                # the new segment contains exactly the new event
    assert len(calls) == 2 and calls[1]["bound"] == new == _seqs_in_segment(calls[1]["path"])
    assert _disk_ckpt(tmp_path) == wal[3]["seq"] and app._quality_wal_inflight == set()


# ---------------------------------------------------------------- B. defence in depth (seeded, NOT reachable)
def test_defence_in_depth_callback_never_checkpoints_past_an_earlier_in_flight_sequence(app, tmp_path):
    """Seeded state -- by design NOT reachable through single-writer operation.

    ``_on_quality_segment_durable`` computes ``min(inflight) - 1``. For a lower, unresolved seq to
    coexist with a later published one, a later event would have to be written and published while an
    earlier one is WAL-appended-but-never-written. ``_persist_quality_event`` is synchronous and every
    failure path latches the writer FAILED, so that cannot happen; the real mechanisms that withhold the
    checkpoint are the latch (see the checkpoint-write test above) and the FAILED writer. This test pins
    the formula itself: a REAL WAL record is appended and held in flight, then the real writer publishes
    later events through the real callback."""
    app._persist_quality_event(_ev("before"))                               # seq a
    stuck = app._quality_wal.append({**_ev("stuck"), "event_type": "ERROR",
                                     "local_ts": 1, "local_process_ts": 1})  # seq a+1: WAL-appended, never written
    stuck_seq = _seq(stuck)
    app._quality_wal_inflight.add(stuck_seq)
    app._persist_quality_event(_ev("after"))                                # seq a+2
    wal = _wal_records(tmp_path)
    assert [w["reason"] for w in wal] == ["before", "stuck", "after"]

    app.quality_writer.publish_open_segment()                               # real publication; real callback

    assert [r["reason"] for r in _rows(tmp_path)] == ["before", "after"]    # both published...
    assert app._quality_wal_inflight == {stuck_seq}
    assert _disk_ckpt(tmp_path) == wal[0]["seq"]                            # ...but the checkpoint stops BEFORE `stuck`
    assert [e["reason"] for e in _pending(tmp_path)] == ["stuck", "after"]  # `after` replays (at-least-once); `stuck` is kept


# =========================================================================================== STAGE 2B-1
# WAL-cap fallback under batching. A quality event the WAL cap refuses has no WAL record, id or sequence;
# ``_persist_quality_event`` writes it into the CURRENT open segment (next to any buffered WAL-backed
# rows) and force-publishes that segment at once. Because the segment's WAL seqs are bound at write time
# and the fallback row binds none, the durable callback checkpoints exactly the WAL-backed rows that
# really were published -- and, as for any publication, only up to min(in-flight) - 1.
def _cap_wal_at_current_size(app):
    """Close WAL admission through the real admission arithmetic: the cap equals the bytes already on
    disk, so the next record cannot fit (``current + size > max``) and ``QualityEventWAL.append`` itself
    refuses it and latches. No flag is set and ``append`` is not replaced."""
    wal = app._quality_wal
    wal.max_total_bytes = wal._total_wal_bytes()
    assert wal.capacity_exhausted is False          # nothing has latched yet: the next append must do it
    return wal


def _backed_seqs(path):
    """WAL seqs of a published segment's rows; a cap-fallback row carries no WAL id and has none."""
    return {_seq(r["quality_event_id"]) for r in pq.read_table(path).to_pylist()
            if r["quality_event_id"] is not None}


def _alert_count(alerts, marker):
    return len([m for m in alerts if marker in m])


def test_a_cap_refused_event_joins_the_open_batch_and_publishes_it_without_a_fabricated_wal_id(
        app, tmp_path, _alerts):
    calls = _spy_durable(app, tmp_path)
    _persist(app, 3, "wal-")                                                # buffered, WAL-appended, in flight
    wal_before = _wal_records(tmp_path)
    seqs = {w["seq"] for w in wal_before}
    wal = _cap_wal_at_current_size(app)
    bytes_before, counter_before = wal._total_wal_bytes(), wal._seq
    assert _segs(tmp_path) == [] and _disk_ckpt(tmp_path) == -1

    app._persist_quality_event(_ev("fallback"))

    # the real admission refused it: no WAL record, id or sequence exists for the event
    assert wal.capacity_exhausted is True and wal.capacity_reason == "aggregate_budget_exhausted"
    assert _wal_records(tmp_path) == wal_before and wal._total_wal_bytes() == bytes_before
    assert wal._seq == counter_before == max(seqs)
    assert _wal_for(tmp_path, "fallback") == []
    # the fallback published at once (4 rows, far below 500 rows / 30 s): the event is neither dropped nor deferred
    (seg,) = _segs(tmp_path)
    rows = pq.read_table(seg).to_pylist()
    assert [r["reason"] for r in rows] == ["wal-0", "wal-1", "wal-2", "fallback"]       # once each, in order
    assert [r["quality_event_id"] for r in rows[:3]] == [w["quality_event_id"] for w in wal_before]
    assert rows[3]["quality_event_id"] is None
    # attribution and ordering: the callback bound exactly the WAL-backed rows, then the checkpoint covered them
    (call,) = calls
    assert call["record_count"] == 4
    assert call["bound"] == seqs == _backed_seqs(seg)
    assert call["inflight_before"] == seqs
    assert call["ckpt_before"] == -1 and call["ckpt_after"] == max(seqs) == _disk_ckpt(tmp_path)
    assert app._quality_wal_inflight == set() and app._quality_segment_seqs == set()
    assert _pending(tmp_path) == []                                         # nothing for a restart to replay or duplicate
    # observability: capacity exhaustion is reported, and it is NOT a storage failure or a loss
    status = app.quality_channel_status()
    assert status["quality_wal_capacity_exhausted"] is True
    assert status["quality_wal_capacity_reason"] == "aggregate_budget_exhausted"
    assert status["quality_degraded"] is False and status["quality_checkpoint_blocked"] is False
    assert (status["quality_events_lost"], status["quality_events_wal_only"],
            status["quality_events_wal_unconfirmed"]) == (0, 0, 0)
    assert _alert_count(_alerts, "QUALITY WAL CAPACITY EXHAUSTED [BYBIT]") == 1
    assert _alert_count(_alerts, "CHECKPOINT BLOCKED") == 0


def test_each_later_cap_refused_event_publishes_alone_exactly_once_and_never_moves_the_checkpoint(
        app, tmp_path, _alerts):
    calls = _spy_durable(app, tmp_path)
    _persist(app, 2, "wal-")
    wal = _cap_wal_at_current_size(app)
    app._persist_quality_event(_ev("fb-0"))                                 # the first refusal latches the cap
    last_backed = max(w["seq"] for w in _wal_records(tmp_path))
    assert len(calls) == 1 and _disk_ckpt(tmp_path) == last_backed
    wal_records, wal_bytes = _wal_records(tmp_path), wal._total_wal_bytes()

    for i in (1, 2, 3):                                                     # latched: refused before any append
        app._persist_quality_event(_ev(f"fb-{i}"))

    assert [c["record_count"] for c in calls] == [3, 1, 1, 1] and len(_segs(tmp_path)) == 4
    assert all(c["bound"] == set() for c in calls[1:])                      # nothing is attributed to a fallback row
    assert all(c["ckpt_before"] == c["ckpt_after"] == last_backed for c in calls[1:])
    rows = _rows(tmp_path)
    assert [r["reason"] for r in rows] == ["wal-0", "wal-1", "fb-0", "fb-1", "fb-2", "fb-3"]   # each exactly once
    assert all(r["quality_event_id"] is None for r in rows[2:])
    assert _wal_records(tmp_path) == wal_records and wal._total_wal_bytes() == wal_bytes   # the WAL did not grow
    assert _disk_ckpt(tmp_path) == last_backed and app._quality_wal_inflight == set()
    assert _alert_count(_alerts, "QUALITY WAL CAPACITY EXHAUSTED [BYBIT]") == 1             # alerted once per process


def test_a_cap_refused_event_in_a_new_hour_checkpoints_only_the_old_segment(app, tmp_path):
    calls = _spy_durable(app, tmp_path)
    w = app.quality_writer
    hour = {"now": w.current_hour}
    w._get_current_hour_str = lambda: hour["now"]                           # instance-level, scoped to this app
    _persist(app, 3, "h1-")
    old = {x["seq"] for x in _wal_records(tmp_path)}
    _cap_wal_at_current_size(app)

    hour["now"] = "2099-01-01-00"                                           # the fallback write rolls the hour over
    app._persist_quality_event(_ev("fb-h2"))

    first, second = calls            # the rollover publishes the OLD segment; the fallback then publishes its own
    old_rows = pq.read_table(first["path"]).to_pylist()
    assert [r["reason"] for r in old_rows] == ["h1-0", "h1-1", "h1-2"]      # the fallback row is NOT in the old segment
    assert first["bound"] == old == _backed_seqs(first["path"])
    assert first["ckpt_after"] == max(old)
    (new_row,) = pq.read_table(second["path"]).to_pylist()
    assert new_row["reason"] == "fb-h2" and new_row["quality_event_id"] is None
    assert second["bound"] == set() and second["record_count"] == 1
    assert second["ckpt_before"] == second["ckpt_after"] == max(old) == _disk_ckpt(tmp_path)
    assert app._quality_wal_inflight == set() and _pending(tmp_path) == []


def test_a_cap_refused_publication_cannot_checkpoint_over_an_earlier_unresolved_prefix(
        app, clock, faults, tmp_path, _alerts):
    calls = _spy_durable(app, tmp_path)
    _persist(app, 3, "e-")
    originals = {w["quality_event_id"]: w["reason"] for w in _wal_records(tmp_path)}
    faults.fail_checkpoint_replace = True
    clock.set_elapsed(30.0)
    app._quality_publish_if_due()                       # segment 1 publishes; its checkpoint write fails -> latch
    assert faults.checkpoint_replace_hits == 1 and len(_segs(tmp_path)) == 1 and _disk_ckpt(tmp_path) == -1
    assert app._quality_checkpoint_block_reason == "checkpoint_write_failed:OSError"

    faults.fail_checkpoint_replace = False              # the disk recovers; the latch must not
    _cap_wal_at_current_size(app)
    app._persist_quality_event(_ev("fb-after-latch"))   # refused by the cap, force-published

    assert len(_segs(tmp_path)) == 2 and len(calls) == 2
    (fb_row,) = pq.read_table(calls[1]["path"]).to_pylist()
    assert fb_row["reason"] == "fb-after-latch" and fb_row["quality_event_id"] is None
    assert calls[1]["bound"] == set()
    # the fallback publication did NOT checkpoint over the earlier, unresolved records
    assert _disk_ckpt(tmp_path) == -1 and faults.checkpoint_replace_hits == 1     # no second attempt either
    assert {e["quality_event_id"]: e["reason"] for e in _pending(tmp_path)} == originals
    assert app._quality_checkpoint_blocked is True
    status = app.quality_channel_status()
    assert status["quality_wal_capacity_exhausted"] is True and status["quality_checkpoint_blocked"] is True
    assert status["quality_degraded"] is False and status["quality_events_lost"] == 0   # publication succeeded

    _crash(app)                                         # restart: the WAL-backed records replay, the fallback row does not
    app2 = BybitCollectorApp(data_dir=str(tmp_path))
    try:
        rows = _rows(tmp_path)
        assert Counter(r["quality_event_id"] for r in rows if r["quality_event_id"] in originals) == \
            {i: 2 for i in originals}                   # at-least-once: the documented duplicate, same ids
        assert [r["reason"] for r in rows].count("fb-after-latch") == 1       # never duplicated, never lost
        assert [e for e in _pending(tmp_path) if e["quality_event_id"] in originals] == []
        assert _disk_ckpt(tmp_path) >= max(_seq(i) for i in originals)
    finally:
        _crash(app2)


def test_a_failed_cap_refused_publication_is_observable_and_leaves_every_wal_record_recoverable(
        app, faults, tmp_path, _alerts):
    _persist(app, 3, "p-")
    wal_rows = _wal_records(tmp_path)
    originals = {w["quality_event_id"]: w["reason"] for w in wal_rows}
    seqs = {w["seq"] for w in wal_rows}
    wal = _cap_wal_at_current_size(app)
    counter = wal._seq

    faults.fail_segment_rename = True
    with expect_fatal(OSError, stage="rename") as info:
        app._persist_quality_event(_ev("fb-fails"))
    assert "No space left" in str(info.value.__cause__)                     # the original error is chained
    assert faults.segment_rename_hits == 1

    # nothing is claimed durable
    assert _segs(tmp_path) == [] and _rows(tmp_path) == [] and len(_tmps(tmp_path)) == 1
    assert _disk_ckpt(tmp_path) == -1 and app._quality_wal_inflight == seqs
    # no fabricated WAL record for the refused event; every earlier record is intact and recoverable
    assert _wal_for(tmp_path, "fb-fails") == [] and wal._seq == counter
    assert {e["quality_event_id"]: e["reason"] for e in _pending(tmp_path)} == originals
    # the failure is observable: latch, degraded channel, a counted loss for the unprotected event, alerts
    assert app._quality_checkpoint_block_reason == "unprotected_event_publish_failed"
    status = app.quality_channel_status()
    assert status["quality_degraded"] is True and status["quality_failure"]["stage"] == "rename"
    assert status["quality_wal_capacity_exhausted"] is True
    assert status["quality_events_lost"] == 1                               # no WAL copy and publication failed
    assert status["quality_events_wal_only"] == 0 and status["quality_events_wal_unconfirmed"] == 0
    assert _alert_count(_alerts, "QUALITY WAL CAPACITY EXHAUSTED [BYBIT]") == 1
    assert _alert_count(_alerts, "QUALITY WAL CHECKPOINT BLOCKED [BYBIT]") == 1
    assert _alert_count(_alerts, "QUALITY EVENTS UNRECORDED [BYBIT]") == 1

    app._persist_quality_event(_ev("fb-after-failure"))                     # degraded: counted, never written
    assert app.quality_channel_status()["quality_events_lost"] == 2
    assert _segs(tmp_path) == [] and _disk_ckpt(tmp_path) == -1
    assert _wal_for(tmp_path, "fb-after-failure") == []
    assert _alert_count(_alerts, "QUALITY EVENTS UNRECORDED [BYBIT]") == 1

    _crash(app)
    faults.fail_segment_rename = False
    app2 = BybitCollectorApp(data_dir=str(tmp_path))
    try:
        rows = _rows(tmp_path)
        replayed = [r for r in rows if r["quality_event_id"] in originals]
        assert {r["quality_event_id"]: r["reason"] for r in replayed} == originals and len(replayed) == 3
        # the loss was reported at runtime (counter + alert above); a restart never fabricates the lost rows
        assert not [r for r in rows if r["reason"] in {"fb-fails", "fb-after-failure"}]
    finally:
        _crash(app2)


def test_a_wal_over_the_cap_at_startup_replays_once_and_new_events_use_the_fallback(
        app, tmp_path, monkeypatch, _alerts):
    _persist(app, 3, "r-")
    wal_rows = _wal_records(tmp_path)
    originals = {w["quality_event_id"]: w["reason"] for w in wal_rows}
    last_seq = max(w["seq"] for w in wal_rows)
    _crash(app)                                         # 3 WAL-backed records, none published or checkpointed

    monkeypatch.setenv(ENV_MAX_TOTAL_BYTES, "1")        # the configured cap is below what is already on disk
    app2 = BybitCollectorApp(data_dir=str(tmp_path))
    try:
        wal2 = app2._quality_wal
        assert wal2.capacity_exhausted is True and wal2.capacity_reason == "existing_wal_at_or_over_capacity"
        replayed = [r for r in _rows(tmp_path) if r["quality_event_id"] in originals]
        assert {r["quality_event_id"]: r["reason"] for r in replayed} == originals and len(replayed) == 3
        assert _disk_ckpt(tmp_path) == last_seq         # replay of WAL-backed records still checkpoints under the cap
        assert [e for e in _pending(tmp_path) if e["quality_event_id"] in originals] == []
        assert app2._quality_checkpoint_blocked is False

        app2._persist_quality_event(_ev("after-cap"))
        (row,) = [r for r in _rows(tmp_path) if r["reason"] == "after-cap"]
        assert row["quality_event_id"] is None          # published, once, with no fabricated id
        assert _wal_for(tmp_path, "after-cap") == [] and wal2.capacity_exhausted is True
        assert _disk_ckpt(tmp_path) == last_seq
        assert app2.quality_channel_status()["quality_wal_capacity_exhausted"] is True
        assert _alert_count(_alerts, "QUALITY WAL CAPACITY EXHAUSTED [BYBIT]") == 1
    finally:
        _crash(app2)


def test_defence_in_depth_a_cap_refused_publication_stops_the_checkpoint_before_an_unresolved_record(
        app, tmp_path):
    """Seeded state -- by design NOT reachable through single-writer operation (see the Stage 2A test of
    the same name). It pins that the FALLBACK publication, which is what runs the durable callback here,
    uses the same ``min(in-flight) - 1`` bound: a real WAL record is held in flight, the cap refuses the
    next event, and the real writer publishes the fallback row through the real callback."""
    app._persist_quality_event(_ev("before"))                               # seq a
    stuck = app._quality_wal.append({**_ev("stuck"), "event_type": "ERROR", "local_ts": 1, "local_process_ts": 1})
    stuck_seq = _seq(stuck)                                                 # seq a+1: WAL-appended, never written
    app._quality_wal_inflight.add(stuck_seq)
    app._persist_quality_event(_ev("after"))                                # seq a+2, published later than `stuck`
    _cap_wal_at_current_size(app)

    app._persist_quality_event(_ev("fallback"))          # refused by the cap; force-publishes the open segment

    wal = _wal_records(tmp_path)
    assert [w["reason"] for w in wal] == ["before", "stuck", "after"]       # the fallback never reached the WAL
    assert [r["reason"] for r in _rows(tmp_path)] == ["before", "after", "fallback"]
    assert app._quality_wal_inflight == {stuck_seq}
    # the published high-water mark is `after` (a+2); the checkpoint must still stop BEFORE `stuck` (a)
    assert _disk_ckpt(tmp_path) == wal[0]["seq"] < stuck_seq < wal[2]["seq"]
    assert [e["reason"] for e in _pending(tmp_path)] == ["stuck", "after"]  # `after` replays (at-least-once)


# =========================================================================================== STAGE 2B-2
# Shutdown and recovery. The open (partial) batch is published by the real ``quality_writer.close()``,
# checkpointed by the real durable callback, and only then is the real WAL closed.
_SIX_OTHER_WRITERS = ("raw_wire_writer", "ob_writer", "trades_writer", "mark_writer", "oi_writer", "liq_writer")


class _GatedStopClient:
    """Network stand-in whose ``stop()`` is ASYNCHRONOUS and stays suspended until released."""

    def __init__(self):
        self.running = False
        self.entered = asyncio.Event()      # set once stop() is suspended
        self.release = asyncio.Event()

    async def start(self):
        await asyncio.sleep(3600)

    async def stop(self):
        self.entered.set()
        await self.release.wait()


class _DirFsyncFault:
    """Fails the ``fsync`` of the quality segment DIRECTORY (and only that) while armed.

    This is the real ``dir_fsync`` publication stage: the segment has already been renamed into place, so
    its durability is unproven. Other fsyncs (tmp file, WAL, WAL directory) are untouched."""

    def __init__(self, monkeypatch, directory):
        self.armed = False
        self.hits = 0
        self._ino = os.stat(directory).st_ino
        real = os.fsync

        def fsync(fd):
            number = fd if isinstance(fd, int) else fd.fileno()
            st = os.fstat(number)
            if self.armed and stat.S_ISDIR(st.st_mode) and st.st_ino == self._ino:
                self.hits += 1
                raise OSError(5, "I/O error (injected segment directory fsync)")
            return real(fd)
        monkeypatch.setattr(os, "fsync", fsync)


def _record_close_order(app, monkeypatch, probe=lambda: None):
    """Wrap (never replace) every real ``close()``: record ``(name, probe())`` on entry, then run it."""
    order = []

    def wrap(name, obj):
        real = obj.close

        def close():
            order.append((name, probe()))
            return real()
        monkeypatch.setattr(obj, "close", close)
    for name in (*_SIX_OTHER_WRITERS, "quality_writer"):
        wrap(name, getattr(app, name))
    wrap("wal", app._quality_wal)
    return order


async def _start_running(app, monkeypatch, client=None):
    """Real ``run()`` with a fast idle tick; returns ``(runner, idle_task)``."""
    monkeypatch.setattr(rbc, "QUALITY_IDLE_TICK_SECONDS", 0.01)
    app.client = client if client is not None else _StubClient()
    runner = asyncio.create_task(app.run())
    await _until(lambda: getattr(app, "_quality_idle_task", None) is not None)
    return runner, app._quality_idle_task


async def test_shutdown_publishes_the_final_buffered_tail_checkpoints_it_and_closes_the_wal(app, tmp_path):
    app.client = _StubClient()
    _persist(app, 3, "tail-")
    wal = _wal_records(tmp_path)
    seqs = {w["seq"] for w in wal}
    assert _segs(tmp_path) == [] and _disk_ckpt(tmp_path) == -1             # only WAL-appended and buffered
    assert app._quality_wal_inflight == seqs

    await app.shutdown()

    (seg,) = _segs(tmp_path)
    assert _seqs_in_segment(seg) == seqs                                    # the tail really is in the segment
    assert sorted(r["reason"] for r in _rows(tmp_path)) == ["tail-0", "tail-1", "tail-2"]
    assert _disk_ckpt(tmp_path) == max(seqs)                                # checkpointed by the real callback
    assert _pending(tmp_path) == [] and app._quality_wal_inflight == set()
    assert app._quality_checkpoint_blocked is False
    assert app._quality_wal._closed is True and app.quality_writer._closed is True
    assert _tmps(tmp_path) == []


async def test_a_failed_shutdown_tail_publication_is_contained_and_every_record_stays_recoverable(
        app, faults, tmp_path, _alerts):
    app.client = _StubClient()
    _persist(app, 3, "sd-fail-")
    wal = _wal_records(tmp_path)
    ids = {w["quality_event_id"] for w in wal}
    faults.fail_segment_rename = True

    await app.shutdown()                                    # contained: shutdown itself does not raise

    assert faults.segment_rename_hits == 1                  # the publication really failed
    assert _segs(tmp_path) == [] and _rows(tmp_path) == []  # nothing reported as published
    assert len(_tmps(tmp_path)) == 1                        # .tmp left for orphan recovery
    assert _disk_ckpt(tmp_path) == -1                       # never checkpointed
    assert {e["quality_event_id"] for e in _pending(tmp_path)} == ids       # every record recoverable
    assert app._quality_checkpoint_blocked is True
    assert app._quality_checkpoint_block_reason == "quality_writer_close_failed"
    assert app._quality_wal._closed is True                 # the WAL is still closed, AFTER the failed publish
    assert len([m for m in _alerts if "QUALITY WAL CHECKPOINT BLOCKED [BYBIT]" in m]) == 1


async def test_a_failed_shutdown_tail_after_a_published_segment_checkpoints_only_the_published_prefix(
        app, faults, tmp_path):
    app.client = _StubClient()
    _persist(app, 503, "pp-")                               # 500 publish from write(); 3 stay buffered
    wal = _wal_records(tmp_path)
    first_seqs, last_first = _seq_range(wal, 0, 500), wal[499]["seq"]
    tail_ids = {w["quality_event_id"] for w in wal[500:]}
    all_ids = {w["quality_event_id"] for w in wal}
    (seg1,) = _segs(tmp_path)
    assert _seqs_in_segment(seg1) == first_seqs and _disk_ckpt(tmp_path) == last_first

    faults.fail_segment_rename = True
    await app.shutdown()

    assert faults.segment_rename_hits == 1
    assert _segs(tmp_path) == [seg1]                        # the failed tail never became a second segment
    assert _disk_ckpt(tmp_path) == last_first               # the checkpoint stops at the published prefix
    assert {e["quality_event_id"] for e in _pending(tmp_path)} == tail_ids  # exactly the tail is pending

    faults.fail_segment_rename = False
    app2 = BybitCollectorApp(data_dir=str(tmp_path))        # real startup recovery
    try:
        counts = Counter(r["quality_event_id"] for r in _rows(tmp_path))
        assert set(counts) == all_ids and set(counts.values()) == {1}       # 503 rows: the tail replayed once
        assert _disk_ckpt(tmp_path) == wal[502]["seq"] and _pending(tmp_path) == []
        assert app2._quality_checkpoint_blocked is False
    finally:
        _crash(app2)


async def test_a_directory_fsync_failure_at_shutdown_is_never_checkpointed_and_replays_at_least_once(
        app, tmp_path, monkeypatch):
    dirfault = _DirFsyncFault(monkeypatch, _qdir(tmp_path))
    app.client = _StubClient()
    _persist(app, 3, "dfs-")
    wal = _wal_records(tmp_path)
    ids = {w["quality_event_id"] for w in wal}
    dirfault.armed = True

    await app.shutdown()

    assert dirfault.hits == 1                               # the dir_fsync stage really failed
    assert len(_segs(tmp_path)) == 1                        # renamed into place, but durability was never proven
    assert _disk_ckpt(tmp_path) == -1                       # so it is NOT checkpointed
    assert {e["quality_event_id"] for e in _pending(tmp_path)} == ids
    assert app._quality_checkpoint_blocked is True
    assert app._quality_checkpoint_block_reason == "quality_writer_close_failed"
    assert app._quality_wal._closed is True

    dirfault.armed = False
    app2 = BybitCollectorApp(data_dir=str(tmp_path))
    try:
        counts = Counter(r["quality_event_id"] for r in _rows(tmp_path) if r["quality_event_id"] in ids)
        assert set(counts) == ids and min(counts.values()) >= 1             # at-least-once; same ids on replay
        assert _disk_ckpt(tmp_path) >= max(w["seq"] for w in wal) and _pending(tmp_path) == []
        assert app2._quality_checkpoint_blocked is False
    finally:
        _crash(app2)


async def test_the_idle_task_is_retired_and_awaited_before_the_first_writer_close(
        app, clock, tmp_path, monkeypatch):
    runner, task = await _start_running(app, monkeypatch)
    try:
        _persist(app, 2, "retire-")
        seqs = {w["seq"] for w in _wal_records(tmp_path)}
        order = _record_close_order(app, monkeypatch, probe=task.done)

        await app.shutdown()

        assert [name for name, _ in order][-2:] == ["quality_writer", "wal"] and len(order) == 8
        assert all(idle_done is True for _, idle_done in order)     # already finished at EVERY close
        assert task.cancelled() and app._quality_idle_task is None
        assert _seqs_in_segment(_segs(tmp_path)[0]) == seqs and len(_segs(tmp_path)) == 1
        assert _disk_ckpt(tmp_path) == max(seqs)
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)


async def test_cancelling_shutdown_while_client_stop_is_suspended_propagates_cancellation_and_closes_nothing(
        app, tmp_path, monkeypatch):
    client = _GatedStopClient()
    runner, _ = await _start_running(app, monkeypatch, client)
    try:
        _persist(app, 3, "cancel-")
        ids = {w["quality_event_id"] for w in _wal_records(tmp_path)}
        shutdown_task = asyncio.create_task(app.shutdown())
        await asyncio.wait_for(client.entered.wait(), 5.0)          # shutdown is now suspended in client.stop()

        shutdown_task.cancel()
        with pytest.raises(asyncio.CancelledError):                 # cancellation propagates; it is not swallowed
            await shutdown_task
        assert shutdown_task.cancelled()

        for name in (*_SIX_OTHER_WRITERS, "quality_writer"):        # nothing was closed prematurely
            assert getattr(app, name)._closed is False, name
        assert app._quality_wal._closed is False
        assert {e["quality_event_id"] for e in _pending(tmp_path)} == ids    # unresolved data still recoverable
        assert _segs(tmp_path) == [] and _disk_ckpt(tmp_path) == -1
    finally:
        client.release.set()
        idle = getattr(app, "_quality_idle_task", None)             # never leak the background task out of a test
        if idle is not None:
            idle.cancel()
            await asyncio.gather(idle, return_exceptions=True)
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)


async def test_cancelling_shutdown_during_an_async_client_stop_retires_the_idle_publisher_before_it_propagates(
        app, clock, tmp_path, monkeypatch):
    """Regression: ``shutdown()`` cancelled while awaiting an asynchronous ``client.stop()``.

    ``CancelledError`` is not an ``Exception``, so it used to skip ``_stop_quality_idle_publisher()``. The
    idle task then outlived ``shutdown()``: ``shutdown()`` has set ``running = False``, but the loop still
    runs one more tick body after its sleep, so it published the due tail and moved the WAL checkpoint AFTER
    shutdown had already raised, with the writers and the WAL still open.

    Made deterministic (no race on the real tick): the tick is 0.2 s and the test synchronises to just after
    a tick, marks the batch due, and cancels shutdown within microseconds -- the idle task is therefore
    mid-sleep, and would wake and publish ~0.2 s later if it were still alive."""
    monkeypatch.setattr(rbc, "QUALITY_IDLE_TICK_SECONDS", 0.2)
    ticks = []
    real_tick = app._quality_publish_if_due

    def counting_tick():
        ticks.append(1)
        real_tick()
    monkeypatch.setattr(app, "_quality_publish_if_due", counting_tick)
    client = _GatedStopClient()
    app.client = client
    runner = asyncio.create_task(app.run())
    try:
        await _until(lambda: len(ticks) >= 1)                        # the idle task has just ticked; it is now asleep
        idle = app._quality_idle_task
        _persist(app, 3, "regress-")
        wal = _wal_records(tmp_path)
        ids, seqs = {w["quality_event_id"] for w in wal}, {w["seq"] for w in wal}
        clock.set_elapsed(30.0)                                      # the age threshold is reached: the next tick WOULD publish
        shutdown_task = asyncio.create_task(app.shutdown())
        await asyncio.wait_for(client.entered.wait(), 5.0)           # suspended inside the ASYNCHRONOUS client.stop()
        assert not idle.done() and app.running is False

        shutdown_task.cancel()
        with pytest.raises(asyncio.CancelledError):                  # cancellation still propagates
            await shutdown_task
        assert shutdown_task.cancelled()

        # the idle publisher was retired AND awaited before the cancellation propagated
        assert idle.done() and idle.cancelled() and app._quality_idle_task is None
        # nothing was closed prematurely
        for name in (*_SIX_OTHER_WRITERS, "quality_writer"):
            assert getattr(app, name)._closed is False, name
        assert app._quality_wal._closed is False

        # no background task advances the checkpoint after shutdown has exited: outwait several ticks
        await asyncio.sleep(0.6)
        assert len(ticks) == 1                                       # the idle loop never ticked again
        assert _segs(tmp_path) == [] and _rows(tmp_path) == [] and _disk_ckpt(tmp_path) == -1
        # unresolved WAL data is untouched and recoverable; the tail is still in flight, not lost or published
        assert {e["quality_event_id"] for e in _pending(tmp_path)} == ids
        assert app._quality_wal_inflight == seqs
    finally:
        client.release.set()
        leaked = getattr(app, "_quality_idle_task", None)            # never leak a background task out of a test
        if leaked is not None:
            leaked.cancel()
            await asyncio.gather(leaked, return_exceptions=True)
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)


async def test_shutdown_closes_the_other_writers_first_then_the_quality_writer_and_only_then_the_wal(
        app, tmp_path, monkeypatch):
    app.client = _StubClient()
    _persist(app, 2, "order-")
    order = _record_close_order(app, monkeypatch)
    assert app.quality_writer.on_segment_durable == app._on_quality_segment_durable
    real_cb = app._on_quality_segment_durable
    wal_open_at_publish = []

    def durable(path, record_count):
        wal_open_at_publish.append(app._quality_wal._closed is False)
        order.append(("quality_segment_published", None))
        real_cb(path, record_count)
    app.quality_writer.on_segment_durable = durable

    await app.shutdown()

    names = [name for name, _ in order]
    assert set(names[:6]) == set(_SIX_OTHER_WRITERS) and len(names) == 9
    assert names[6:] == ["quality_writer", "quality_segment_published", "wal"]
    assert wal_open_at_publish == [True]                    # the final publish ran while the WAL was still open
    assert _disk_ckpt(tmp_path) == max(w["seq"] for w in _wal_records(tmp_path))


async def test_another_writers_close_failure_is_reported_through_the_still_open_quality_writer(
        app, tmp_path, monkeypatch):
    app.client = _StubClient()

    def boom():
        raise OSError("raw close failed (injected)")
    monkeypatch.setattr(app.raw_wire_writer, "close", boom)

    await app.shutdown()

    reason = "storage_shutdown_close_failed:raw_wire_writer:OSError"
    (rec,) = [w for w in _wal_records(tmp_path) if w["reason"] == reason]
    assert [r["reason"] for r in _rows(tmp_path)] == [reason]               # published by the final quality close
    assert _disk_ckpt(tmp_path) == rec["seq"] and _pending(tmp_path) == []
    assert app._quality_checkpoint_blocked is False and app._quality_wal._closed is True


async def test_shutdown_with_nothing_buffered_publishes_no_empty_segment_and_moves_no_checkpoint(
        app, clock, tmp_path):
    app.client = _StubClient()
    _persist(app, 2, "done-")
    clock.set_elapsed(30.0)
    app._quality_publish_if_due()                           # already published and checkpointed
    (seg,) = _segs(tmp_path)
    ckpt = _disk_ckpt(tmp_path)
    assert ckpt == max(w["seq"] for w in _wal_records(tmp_path))

    await app.shutdown()

    assert _segs(tmp_path) == [seg] and _disk_ckpt(tmp_path) == ckpt and len(_rows(tmp_path)) == 2
    assert _pending(tmp_path) == [] and app._quality_checkpoint_blocked is False
    assert app._quality_wal._closed is True
