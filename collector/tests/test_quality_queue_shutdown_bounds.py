"""The quality-queue part of shutdown: bounded, fail-closed, and never at the cost of an admitted event.

``CollectorApp.start()``'s finalization and ``_async_shutdown()`` used to wait for the quality persistence
loop without a deadline (``queue.join()``, ``await _quality_task``) BEFORE any websocket client was asked to
stop and before any writer was closed. Reproduced against the real ``start()`` / ``_async_shutdown()`` /
``shutdown()`` (only the network edge and the websocket clients faked), each leaving EVERY writer --
market data included -- unclosed and the exit status unlatched until systemd's SIGKILL:

* the loop dying while ``join()`` waited (the ``done()`` guard was checked once, before the wait);
* a loop that is alive but never finishing events (a substituted consumer: the real one has no await
  other than ``queue.get()``, so this one is a property of the wait, not a demonstrated in-tree trigger);
* Python 3.11 only: a producer that keeps emitting, so the queue never empties (``wait_for`` costs the
  loop ~3 event-loop turns per event there; 3.12 never yields while events are queued).

It also let the loop's own ``BaseException`` replace the caller's cancellation / the original exception.

What "no event lost" means here, and what these tests do NOT claim: every queued event already has a WAL
record (appended before ``put_nowait``) that holds the checkpoint back, so an event is either PUBLISHED to
a quality segment or still RECOVERABLE from the WAL -- ``_assert_no_event_lost`` checks exactly that, from
the segment files and ``QualityEventWAL.recover``. Nothing here demonstrates power-loss behaviour, a live
host, a live exchange or the 60 s systemd stop budget; the bounds are chosen, not measured.
"""
import asyncio

import pyarrow.parquet as pq
import pytest

import run_collector
from collector.collector.quality_wal import QualityEventWAL
from collector.collector.websocket_client import FINALIZATION_RESERVE_S
from collector.tests.test_ws_shutdown_late_producer import (
    _ALL_WRITERS, _closes, _idle_turns, _recovered, _spy_on_closes)
from collector.tests.test_ws_shutdown_startup_recovery import (
    _app_with_stubbed_neighbours, _drain, _quiet)

WAIT = 6.0           # a start()/shutdown that needs longer than this to finish is a hang


# --------------------------------------------------------------------------- helpers -----------

def _fast_bounds(monkeypatch, drain=0.05, stop=0.05, slack=0.05):
    monkeypatch.setattr(run_collector, "QUALITY_DRAIN_BOUND_S", drain, raising=False)
    monkeypatch.setattr(run_collector, "QUALITY_CONSUMER_STOP_BOUND_S", stop, raising=False)
    monkeypatch.setattr(run_collector, "WS_FALLBACK_SLACK_S", slack)


async def _boot(tmp_path, monkeypatch, *, consumer=None, ws=None):
    """A real CollectorApp whose start() is parked in its steady state."""
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    if ws is not None:
        app.ws_clients = [ws(app)]
    log = []
    _spy_on_closes(app, monkeypatch, log)
    monkeypatch.setattr(app, "_recover_binance_book", _recovered)
    if consumer is not None:
        monkeypatch.setattr(app, "_quality_persistence_loop", lambda: consumer(app))
    task = asyncio.create_task(app.start())
    while app._oi_task is None:
        await asyncio.sleep(0)
    return app, log, task


def _emit(app, reasons):
    for reason in reasons:
        app._websocket_quality_event("CONNECT", reason, "c1", "public")


def _emit_during_finalization(app, monkeypatch, n, tag="ev"):
    """Emit ``n`` queue events from INSIDE the finalization, right before its quality waits (the
    consumer is alive and start() is already past its gather). Returns the emitted reasons."""
    reasons = [f"{tag}-{i}" for i in range(n)]
    real = app._drain_integrity_quality_events_safely
    state = {"done": False}

    def hook(*a, **k):
        result = real(*a, **k)
        if not state["done"]:
            state["done"] = True
            _emit(app, reasons)
        return result
    monkeypatch.setattr(app, "_drain_integrity_quality_events_safely", hook)
    return reasons


def _published(app):
    rows = []
    for segment in sorted(app.quality_writer.stream_dir.rglob("*.seg")):
        rows.extend(pq.read_table(segment).to_pylist())
    return [r["reason"] for r in rows]


def _wal_recoverable(app):
    return [e.get("reason") for e in QualityEventWAL.recover(app._quality_wal.wal_dir)]


def _assert_no_event_lost(app, emitted):
    """Every emitted event is published or still recoverable from the WAL -- none silently gone."""
    pub, wal = set(_published(app)), set(_wal_recoverable(app))
    missing = [r for r in emitted if r not in pub and r not in wal]
    assert not missing, f"{len(missing)} admitted events are neither published nor in the WAL: {missing[:5]}"


def _assert_published_exactly_once(app, emitted):
    pub = [r for r in _published(app) if r in set(emitted)]
    assert sorted(pub) == sorted(emitted), (
        f"published {len(pub)} / distinct {len(set(pub))} of {len(emitted)} emitted")


def _assert_every_writer_closed_once_in_order(app, log):
    results = app._writer_close_results
    for name in _ALL_WRITERS:
        assert len(_closes(log, name)) == 1 and results.get(name) is True, (
            f"{name}: expected one successful close, got {_closes(log, name)} / {results.get(name)}")
    assert len(_closes(log, "quality_wal")) == 1
    assert log.index(("close_returned", "quality_writer")) < log.index(("close_called", "quality_wal")), (
        "the quality writer must finish publishing before the WAL is closed")
    assert getattr(app, "_shutdown_done", False)


def _assert_quality_storage_untouched(app, log):
    for name in ("quality_writer", "quality_wal"):
        assert _closes(log, name) == [], f"{name} was closed beneath a live user"
    assert not app.quality_writer._closed and not getattr(app, "_quality_wal_closed", False)
    assert not getattr(app, "_shutdown_done", False)


async def _finishes(task, timeout=WAIT):
    done, _ = await asyncio.wait({task}, timeout=timeout)
    return bool(done)


class _Die(BaseException):
    """Not an Exception: what the persistence loop's ``except Exception`` cannot absorb."""


# ------------------------------------------------------------- 1. cooperative consumer --------

@pytest.mark.asyncio
async def test_a_cooperative_consumer_drains_and_finalizes_in_order_with_nothing_latched(tmp_path, monkeypatch):
    app, log, task = await _boot(tmp_path, monkeypatch)
    emitted = _emit_during_finalization(app, monkeypatch, 20)
    try:
        task.cancel()
        assert await _finishes(task) and task.cancelled()
        _assert_every_writer_closed_once_in_order(app, log)
        _assert_published_exactly_once(app, emitted)
        assert app._quality_wal.checkpointed_seq == len(emitted) - 1       # covers exactly what was published
        assert app._quality_queue.qsize() == 0 and app._quality_queue._unfinished_tasks == 0
        assert app._quality_task.done()
        assert getattr(app, "shutdown_failure", None) is None and app.exit_code == 0
    finally:
        await _drain(app, task)


# ------------------------------------------------------------- 2. consumer failing -------------

@pytest.mark.asyncio
async def test_a_consumer_whose_persist_raises_ends_fail_closed_with_every_event_still_in_the_wal(
        tmp_path, monkeypatch):
    app, log, task = await _boot(tmp_path, monkeypatch)
    emitted = _emit_during_finalization(app, monkeypatch, 20)
    real_write = app.quality_writer.write

    def failing(row, **kw):
        if str(row.get("reason", "")).startswith("ev-"):
            raise RuntimeError("injected quality write failure")
        return real_write(row, **kw)
    monkeypatch.setattr(app.quality_writer, "write", failing)
    try:
        task.cancel()
        assert await _finishes(task) and task.cancelled()
        _assert_every_writer_closed_once_in_order(app, log)
        # nothing is claimed durable, nothing is lost: the WAL is the only record and is replayable
        assert app._quality_checkpoint_is_blocked() and app._quality_wal.checkpointed_seq == -1
        assert _published(app) == [] or not set(_published(app)) & set(emitted)
        assert sorted(_wal_recoverable(app)) == sorted(emitted)
        assert app.quality_channel_status()["quality_checkpoint_blocked"] is True
        assert app._quality_queue._unfinished_tasks == 0
    finally:
        await _drain(app, task)


@pytest.mark.asyncio
async def test_a_consumer_that_dies_while_start_waits_for_the_drain_no_longer_hangs_and_cannot_replace_the_cancel(
        tmp_path, monkeypatch):
    app, log, task = await _boot(tmp_path, monkeypatch)
    emitted = _emit_during_finalization(app, monkeypatch, 20)
    real, seen = app._persist_quality_event, []

    def dying(event):
        if str(event.get("reason", "")).startswith("ev-"):
            seen.append(event["reason"])
            if len(seen) == 3:
                raise _Die("loop killed mid-drain")
        return real(event)
    monkeypatch.setattr(app, "_persist_quality_event", dying)
    try:
        task.cancel()
        assert await _finishes(task), "start() is blocked on a queue nobody drains any more"
        assert task.cancelled(), "the loop's BaseException replaced the caller's cancellation"
        assert isinstance(app._quality_task.exception(), _Die)            # the failure is retained, not lost
        _assert_every_writer_closed_once_in_order(app, log)
        assert app._quality_queue.qsize() == 0 and app._quality_queue._unfinished_tasks == 0
        _assert_no_event_lost(app, emitted)
        killed = seen[2]                                                  # its handling never completed:
        assert killed in _wal_recoverable(app), "the event the loop died on must stay recoverable"
        assert app._quality_wal.checkpointed_seq < emitted.index(killed), (
            "the checkpoint must not cover an event that was never published")
    finally:
        await _drain(app, task)


@pytest.mark.asyncio
async def test_an_exception_from_a_child_task_is_what_start_raises_even_when_the_consumer_died(
        tmp_path, monkeypatch):
    (tmp_path / "data").mkdir()
    monkeypatch.chdir(tmp_path)
    app = run_collector.CollectorApp()
    from collector.tests.test_ws_shutdown_startup_recovery import _FakeWs
    app.ws_clients = [_FakeWs()]
    log, release = [], asyncio.Event()
    _spy_on_closes(app, monkeypatch, log)

    async def no_preflight():
        return False

    async def verify_then_fail(*a, **k):
        await release.wait()
        raise RuntimeError("startup stream inactivity")
    monkeypatch.setattr(app, "_storage_pressure_preflight_failed", no_preflight)
    monkeypatch.setattr(app.health_monitor, "start", _quiet)
    monkeypatch.setattr(app, "_storage_pressure_loop", _quiet)
    monkeypatch.setattr(app, "_verify_startup_streams", verify_then_fail)
    monkeypatch.setattr(app, "_poll_openinterest", _quiet)
    monkeypatch.setattr(app, "_recover_binance_book", _recovered)
    emitted = _emit_during_finalization(app, monkeypatch, 10)
    real, seen = app._persist_quality_event, []

    def dying(event):
        if str(event.get("reason", "")).startswith("ev-"):
            seen.append(1)
            if len(seen) == 2:
                raise _Die("loop killed")
        return real(event)
    monkeypatch.setattr(app, "_persist_quality_event", dying)
    task = asyncio.create_task(app.start())
    try:
        while app._oi_task is None:
            await asyncio.sleep(0)
        release.set()
        assert await _finishes(task)
        assert not task.cancelled() and isinstance(task.exception(), RuntimeError)
        assert str(task.exception()) == "startup stream inactivity"       # not _Die, not a CancelledError
        _assert_every_writer_closed_once_in_order(app, log)
        _assert_no_event_lost(app, emitted)
    finally:
        await _drain(app, task)


# ------------------------------------------------------------- 3. consumer blocked -------------

def _blocked_consumer(gate, *, swallow_cancel=False, log=None):
    async def consumer(app):
        # alive, makes no progress, never marks anything done
        while not gate.is_set():
            try:
                await gate.wait()
            except asyncio.CancelledError:
                if not swallow_cancel:
                    raise
                if log is not None:
                    log.append("cancel_swallowed")
    return consumer


@pytest.mark.asyncio
async def test_a_blocked_consumer_is_stopped_within_the_bounds_and_its_events_are_drained_synchronously(
        tmp_path, monkeypatch):
    _fast_bounds(monkeypatch)
    gate = asyncio.Event()
    app, log, task = await _boot(tmp_path, monkeypatch, consumer=_blocked_consumer(gate))
    emitted = [f"blk-{i}" for i in range(5)]
    try:
        _emit(app, emitted)
        assert app._quality_queue._unfinished_tasks == 5                  # unfinished, nobody working on them
        task.cancel()
        assert await _finishes(task), "start() waits for a consumer that makes no progress"
        assert task.cancelled()
        assert app._quality_task.cancelled()                              # stopped by the bounded escalation
        assert app._quality_queue.qsize() == 0 and app._quality_queue._unfinished_tasks == 0
        _assert_every_writer_closed_once_in_order(app, log)               # nothing closed under a live user
        _assert_published_exactly_once(app, emitted)                      # drained by shutdown(), not dropped
        assert getattr(app, "shutdown_failure", None) is None
    finally:
        gate.set()
        await _drain(app, task)


@pytest.mark.asyncio
async def test_a_consumer_that_swallows_the_stop_keeps_quality_storage_open_latches_and_is_finalized_late(
        tmp_path, monkeypatch):
    _fast_bounds(monkeypatch)
    gate, swallowed = asyncio.Event(), []
    app, log, task = await _boot(tmp_path, monkeypatch,
                                 consumer=_blocked_consumer(gate, swallow_cancel=True, log=swallowed))
    emitted = [f"stub-{i}" for i in range(5)]
    try:
        _emit(app, emitted)
        task.cancel()
        assert await _finishes(task), "an unstoppable consumer must not hold start() hostage"
        assert task.cancelled()
        # --- before the consumer ends: fail closed, nothing closed beneath it -------------------
        assert swallowed and not app._quality_task.done()
        assert app.shutdown_failure == "quality_consumer_not_stopped" and app.exit_code != 0
        _assert_quality_storage_untouched(app, log)
        for name in _ALL_WRITERS:
            if name != "quality_writer":
                assert len(_closes(log, name)) == 1, f"{name} must still be finalized exactly once"
        for _ in range(3):                                                # repeated shutdown: still no close
            app.shutdown()
        _assert_quality_storage_untouched(app, log)
        _assert_no_event_lost(app, emitted)
        # --- the consumer ends: the late retry finalizes, once, in order -----------------------
        log.append(("consumer_ended",))
        gate.set()
        assert await _finishes(app._quality_task)
        await _idle_turns()
        _assert_every_writer_closed_once_in_order(app, log)
        ended = log.index(("consumer_ended",))
        assert _closes(log, "quality_writer")[0] > ended and _closes(log, "quality_wal")[0] > ended
        _assert_published_exactly_once(app, emitted)
        for _ in range(2):
            app.shutdown()
        assert len(_closes(log, "quality_writer")) == 1 and len(_closes(log, "quality_wal")) == 1
        assert app.shutdown_failure == "quality_consumer_not_stopped" and app.exit_code != 0   # never cleared
    finally:
        gate.set()
        await _drain(app, task)


# ------------------------------------------------------------- 4. stubborn _recovery_task ------

@pytest.mark.asyncio
async def test_a_stubborn_recovery_task_keeps_emitting_events_into_open_storage_until_it_ends(
        tmp_path, monkeypatch):
    _fast_bounds(monkeypatch)
    app, log, task = await _boot(tmp_path, monkeypatch)
    release, emitted = asyncio.Event(), []

    async def stubborn_recovery(reason):
        while not release.is_set():
            name = f"rec-{len(emitted)}"
            emitted.append(name)
            app._emit_quality_event({"stream": "orderbook", "event_type": "ERROR", "reason": name, "local_ts": 1})
            try:
                await asyncio.sleep(0.005)
            except asyncio.CancelledError:
                pass
    monkeypatch.setattr(app, "_recover_binance_book", stubborn_recovery)
    try:
        app._schedule_recovery("probe")
        await asyncio.sleep(0.05)
        task.cancel()
        assert await _finishes(task) and task.cancelled()
        before = len(emitted)
        await asyncio.sleep(0.1)
        assert len(emitted) > before, "the producer is meant to still be emitting after start() returned"
        assert app.shutdown_failure == "rest_producer_not_stopped" and app.exit_code != 0
        _assert_quality_storage_untouched(app, log)
        _assert_no_event_lost(app, emitted)
        for _ in range(3):
            app.shutdown()
        _assert_quality_storage_untouched(app, log)
        log.append(("producer_ended",))
        release.set()
        await _idle_turns(60)
        await asyncio.sleep(0.1)
        _assert_every_writer_closed_once_in_order(app, log)
        assert _closes(log, "quality_writer")[0] > log.index(("producer_ended",))
        _assert_published_exactly_once(app, emitted)                      # every one, exactly once
        assert app._quality_wal.checkpointed_seq == len(emitted) - 1
        assert app.shutdown_failure == "rest_producer_not_stopped" and app.exit_code != 0
    finally:
        release.set()
        await _drain(app, task)


# ------------------------------------------------------------- 4b. stubborn queue producer -----

class _ProducerWs:
    """A websocket client double that EMITS queue events with the real lifecycle protocol, and (stubborn)
    ignores stop() and swallows cancels until released."""
    url = "ws://fake-producer"
    shutdown_deadline_s = 0.2
    worker_retire_timeout_s = 0.05

    def __init__(self, app, per_turn=1):
        self.app, self.per_turn = app, per_turn
        self._stop, self.release, self._ended = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self._ended.set()
        self.emitted, self._running = [], False

    lifecycle_idle = property(lambda self: not self._running)
    ingress_stopped = property(lambda self: not self._running)

    def stop(self):
        self._stop.set()

    async def wait_connected(self, timeout_seconds=0.0):
        return True

    async def wait_stopped(self, timeout_s):
        try:
            await asyncio.wait_for(self._ended.wait(), timeout_s)
            return True
        except asyncio.TimeoutError:
            return False

    async def start(self):
        self._running = True
        self._ended.clear()
        try:
            while not self.release.is_set():
                for _ in range(self.per_turn):
                    name = f"prod-{len(self.emitted)}"
                    self.emitted.append(name)
                    self.app._websocket_quality_event("CONNECT", name, "c1", "public")
                try:
                    await asyncio.sleep(0)
                except asyncio.CancelledError:
                    pass
        finally:
            self._running = False
            self._ended.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("per_turn", [1, 3])
@pytest.mark.parametrize("path", ["start_cancelled", "async_shutdown"])
async def test_a_producer_that_outpaces_the_consumer_cannot_stall_the_stop_and_loses_no_event(
        tmp_path, monkeypatch, path, per_turn):
    """The 3.11 starvation: the queue never empties while the producer lives. start()/_async_shutdown must
    still reach the websocket stop, fail closed (the worker is alive) and leave every event recoverable."""
    _fast_bounds(monkeypatch)
    app, log, task = await _boot(tmp_path, monkeypatch, ws=lambda a: _ProducerWs(a, per_turn))
    producer = app.ws_clients[0]
    try:
        await asyncio.sleep(0.05)
        if path == "start_cancelled":
            task.cancel()
            waiter = task
        else:
            waiter = asyncio.create_task(app._async_shutdown(15))
        assert await _finishes(waiter), "the quality wait starved behind a live producer"
        assert producer._stop.is_set(), "the websocket client was never asked to stop (the wait came first)"
        assert app._quality_task.done()
        assert app.shutdown_failure == "ws_client_worker_not_retired" and app.exit_code != 0
        _assert_quality_storage_untouched(app, log)                       # the worker is alive: nothing under it
        _assert_no_event_lost(app, list(producer.emitted))
        producer.release.set()
        await _finishes(task)
    finally:
        producer.release.set()
        await _drain(app, task)


# ------------------------------------------------------------- 5. saturation -------------------

@pytest.mark.asyncio
async def test_a_burst_larger_than_the_queue_during_finalization_is_fully_accounted(tmp_path, monkeypatch):
    app, log, task = await _boot(tmp_path, monkeypatch)
    emitted = _emit_during_finalization(app, monkeypatch, 1500)           # maxsize is 1024
    try:
        task.cancel()
        assert await _finishes(task) and task.cancelled()
        assert app._quality_overflow == 1500 - app._quality_queue.maxsize     # overflowed -> persisted directly
        _assert_published_exactly_once(app, emitted)
        assert not app._quality_checkpoint_is_blocked()
        _assert_every_writer_closed_once_in_order(app, log)
    finally:
        await _drain(app, task)


@pytest.mark.asyncio
async def test_events_arriving_after_the_consumer_ended_are_drained_by_shutdown_not_dropped(tmp_path, monkeypatch):
    app, log, task = await _boot(tmp_path, monkeypatch)
    emitted, seen = [f"late-{i}" for i in range(1100)], {}
    real = app._stop_rest_producers

    async def hooked(*a, **k):                                            # after BOTH quality waits
        seen["consumer_done"] = app._quality_task.done()
        _emit(app, emitted)
        seen["queued"], seen["overflow"] = app._quality_queue.qsize(), app._quality_overflow
        return await real(*a, **k)
    monkeypatch.setattr(app, "_stop_rest_producers", hooked)
    try:
        task.cancel()
        assert await _finishes(task) and task.cancelled()
        assert seen == {"consumer_done": True, "queued": 1024, "overflow": 76}
        assert app._quality_queue.qsize() == 0 and app._quality_queue._unfinished_tasks == 0
        _assert_published_exactly_once(app, emitted)
        _assert_every_writer_closed_once_in_order(app, log)
        assert getattr(app, "shutdown_failure", None) is None
    finally:
        await _drain(app, task)


# ------------------------------------------------------------- 7. cancellation during finalization

@pytest.mark.asyncio
async def test_a_second_cancel_during_the_bounded_wait_still_finalizes_and_propagates(tmp_path, monkeypatch):
    _fast_bounds(monkeypatch, drain=30.0, stop=30.0, slack=0.05)        # the wait is long: cancel lands in it
    gate = asyncio.Event()
    app, log, task = await _boot(tmp_path, monkeypatch, consumer=_blocked_consumer(gate, swallow_cancel=True))
    emitted = [f"c2-{i}" for i in range(4)]
    try:
        _emit(app, emitted)
        task.cancel()
        await asyncio.sleep(0.2)
        assert not task.done(), "start() should be inside its bounded quality wait"
        task.cancel()                                                     # the caller insists
        assert await _finishes(task, timeout=10) and task.cancelled()
        # the finalization after the wait still ran, and did not close the quality storage under the consumer
        assert app.shutdown_failure == "quality_consumer_not_stopped"
        _assert_quality_storage_untouched(app, log)
        for name in _ALL_WRITERS:
            if name != "quality_writer":
                assert len(_closes(log, name)) == 1
        _assert_no_event_lost(app, emitted)
        gate.set()
        assert await _finishes(app._quality_task)
        await _idle_turns()
        _assert_every_writer_closed_once_in_order(app, log)
        _assert_published_exactly_once(app, emitted)
    finally:
        gate.set()
        await _drain(app, task)


# ------------------------------------------------------------- budget / documentation -----------

def test_the_quality_bounds_fit_inside_the_documented_finalization_reserve_and_are_documented():
    from pathlib import Path
    worst = (run_collector.QUALITY_DRAIN_BOUND_S + run_collector.QUALITY_CONSUMER_STOP_BOUND_S
             + run_collector.WS_FALLBACK_SLACK_S)
    assert worst < FINALIZATION_RESERVE_S, (
        f"the quality stop can take {worst} s; it comes out of the {FINALIZATION_RESERVE_S} s reserve")
    doc = " ".join((Path(__file__).resolve().parents[2] / "docs" / "DEPLOYMENT.md").read_text().split())
    for needle in ("QUALITY_DRAIN_BOUND_S", "QUALITY_CONSUMER_STOP_BOUND_S", "quality_consumer_not_stopped"):
        assert needle in doc, f"{needle} is not documented in docs/DEPLOYMENT.md"
