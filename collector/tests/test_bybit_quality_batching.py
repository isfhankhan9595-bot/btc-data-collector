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
"""
from __future__ import annotations

import asyncio
import time as _real_time

import pytest

import collector.collector.parquet_writer as pw
import collector.run_bybit_collector as rbc
import collector.run_collector as rc
from collector.run_bybit_collector import BybitCollectorApp
from collector.tests.test_bybit_quality_wal import (
    _crash, _disk_ckpt, _ev, _pending, _qdir, _rows, _wal_records)


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
