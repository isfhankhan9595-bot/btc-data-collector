"""PR #111 final shutdown-safety audit: producers vs. writer closes, and the budget.

Companion to test_ws_shutdown_lifecycle.py (left untouched). Every test here is
deterministic (events/barriers, no sleep-based proofs) and drives the real
application / client code with fake sockets only. No live exchange, no systemd,
no power-loss behaviour is exercised.

Producer model used throughout (one state per producer, never inferred from
another):

  ingress stopped      the receive loop can no longer call on_raw_frame
  lifecycle finished   WebSocketClient.start() has left its tail
  worker retired       the ingest worker task has ended
  REST producers       recovery task / OI poller can no longer capture
  writer safe to close every producer able to reach it is stopped
"""
import asyncio
import json
import pathlib
import re
import signal
from unittest.mock import patch

import pytest

import run_binance_spot_collector
import run_collector
from collector.collector.websocket_client import WebSocketClient

REPO = pathlib.Path(__file__).resolve().parents[2]


# --------------------------------------------------------------- helpers ---

class _Conn:
    def __init__(self, frames, hold):
        self._frames, self._hold = frames, hold

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def send(self, _):
        return None

    def __aiter__(self):
        frames, hold = self._frames, self._hold

        async def gen():
            for frame in frames:
                yield frame
            await hold.wait()
        return gen()


class _SlowHandshake:
    """What ``websockets.connect(url)`` returns: awaitable (the opening handshake)
    and then usable as ``async with``. The handshake only completes when
    ``release`` is set, like a peer that accepts TCP but never answers."""

    def __init__(self, release, log):
        self._release, self._log = release, log

    def __await__(self):
        return self._handshake().__await__()

    async def _handshake(self):
        self._log.append("handshake_started")
        try:
            await self._release.wait()
        except asyncio.CancelledError:
            self._log.append("handshake_cancelled")
            raise
        self._log.append("handshake_completed")
        return _Conn([], asyncio.Event())


# ------------------------------------------- budget: connect phase vs stop ---

@pytest.mark.asyncio
async def test_stop_during_the_opening_handshake_does_not_wait_out_the_handshake():
    """websockets.connect() has its own 10 s open_timeout and is not woken by
    stop(). If stop() lands while it is pending, start() used to sit in the
    handshake for up to that long, on top of the close handshake, the drain and
    the worker retirement -- more than shutdown_deadline_s accounts for."""
    release, log, started = asyncio.Event(), [], asyncio.Event()

    def connect(url, **k):
        handshake = _SlowHandshake(release, log)
        started.set()
        return handshake

    client = WebSocketClient("ws://x", lambda *a, **k: None)
    task = None
    try:
        with patch("collector.collector.websocket_client.websockets.connect", new=connect):
            task = asyncio.create_task(client.start())
            await asyncio.wait_for(started.wait(), 5)
            await asyncio.sleep(0)                      # let the handshake begin
            client.stop()
            await asyncio.wait_for(task, 5)             # hangs until `release` without the fix
        assert not release.is_set(), "start() ended only because the handshake was released"
        assert "handshake_cancelled" in log and "handshake_completed" not in log
        assert client.lifecycle_idle and client.running is False
        assert await client.wait_stopped(1.0) is True
    finally:
        release.set()
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_handshake_that_completes_with_stop_is_closed_and_consumes_nothing():
    """The race the other way: the handshake finishes in the same loop turn stop()
    lands. The connection must still be closed (async-with exit) and no frame of
    it may be captured or enqueued."""
    closed, captured = [], []

    class _Closing(_Conn):
        async def __aexit__(self, *exc):
            closed.append(1)
            return False

    class _Done:
        def __await__(self):
            async def go():
                return _Closing(["{\"n\": 1}"], asyncio.Event())
            return go().__await__()

    client = WebSocketClient("ws://x", lambda *a, **k: None,
                             on_raw_frame=lambda *a, **k: captured.append(1))
    client.stop()                                       # stop requested first
    with patch("collector.collector.websocket_client.websockets.connect", new=lambda url, **k: _Done()):
        await asyncio.wait_for(client.start(), 5)
    assert captured == [] and client.frames_received == 0
    assert await client.wait_stopped(1.0) is True


# ----------------------------------------- ingress state is its own state ---

@pytest.mark.asyncio
async def test_ingress_stopped_is_independent_of_worker_and_lifecycle_state():
    hold, entered = asyncio.Event(), asyncio.Event()

    async def wedged(data, ts, connection_id=None):
        entered.set()
        await asyncio.Event().wait()

    client = WebSocketClient("ws://x", wedged, ingest_queue_maxsize=10)
    client.shutdown_drain_timeout_s = 30.0
    task = None
    try:
        assert client.ingress_stopped is False          # never stopped, never started
        with patch("collector.collector.websocket_client.websockets.connect",
                   new=lambda url, **k: _Conn([json.dumps({"n": 1})], hold)):
            task = asyncio.create_task(client.start())
            await asyncio.wait_for(entered.wait(), 5)
            assert client.ingress_stopped is False
            client.stop()
            assert client.ingress_stopped is True       # receive side is closed ...
            assert client.worker_active                 # ... while the worker is still alive
            assert client.lifecycle_idle is False       # ... and start() has not finished
    finally:
        hold.set()
        for t in (client._worker_task, task):
            if t is not None and not t.done():
                t.cancel()
                await asyncio.gather(t, return_exceptions=True)


@pytest.mark.asyncio
async def test_no_raw_frame_is_captured_after_stop_even_if_frames_are_still_available():
    """The sole producer of raw_wire_writer is the receive loop; once stop() has
    returned, no frame may reach on_raw_frame (there is no await between the
    running check and the capture, so a stopped loop cannot race a writer close)."""
    hold, captured = asyncio.Event(), []
    client = WebSocketClient("ws://x", lambda *a, **k: None,
                             on_raw_frame=lambda *a, **k: captured.append(1),
                             ingest_queue_maxsize=10)
    client.stop()
    with patch("collector.collector.websocket_client.websockets.connect",
               new=lambda url, **k: _Conn([json.dumps({"n": i}) for i in range(5)], hold)):
        await asyncio.wait_for(client.start(), 5)
    assert captured == []


# ----------------------------------------------- futures runner (USD-M) ---

class _StuckClient:
    """A websocket client whose worker never retires. stop() emits one quality
    event (what the real client's retirement/drain reporting does)."""
    url = "ws://stuck"
    shutdown_deadline_s = 0.05
    worker_retire_timeout_s = 0.05
    lifecycle_idle = False
    ingress_stopped = True

    def __init__(self, app):
        self.app, self.stops = app, 0

    async def start(self):
        await asyncio.Event().wait()

    async def wait_connected(self, timeout_seconds=0.0):
        return True

    def stop(self):
        self.stops += 1
        self.app._websocket_quality_event("ERROR", "ingest_worker_retirement_failed")

    async def wait_stopped(self, timeout_s):
        await asyncio.sleep(0)
        return False


def _futures_app(tmp_path, monkeypatch):
    (tmp_path / "data").mkdir()
    monkeypatch.chdir(tmp_path)
    return run_collector.CollectorApp()


async def _quiet_forever(*a, **k):
    await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_start_returns_after_an_incomplete_shutdown_instead_of_hanging_on_the_quality_queue(
        tmp_path, monkeypatch):
    """Fail-closed path: a client never goes idle, so shutdown() leaves the quality
    writer, WAL and queue alone. The quality persistence loop has already exited
    by then, so events queued during the stop (the retirement failure itself)
    have no consumer. start()'s own tail used to ``await _quality_queue.join()``
    -- which can then never return -- so the process never reached its final
    explicit non-zero exit and waited for systemd's SIGKILL."""
    app = _futures_app(tmp_path, monkeypatch)
    app.ws_clients = [_StuckClient(app)]
    monkeypatch.setattr(app, "_recover_binance_book", lambda *a, **k: asyncio.sleep(0))

    async def no_preflight():
        return False
    monkeypatch.setattr(app, "_storage_pressure_preflight_failed", no_preflight)
    monkeypatch.setattr(app.health_monitor, "start", _quiet_forever)
    monkeypatch.setattr(app, "_poll_openinterest", _quiet_forever)
    monkeypatch.setattr(app, "_verify_startup_streams", _quiet_forever)
    monkeypatch.setattr(app, "_storage_pressure_loop", _quiet_forever)

    start_task = asyncio.create_task(app.start())
    shutdown_task = None
    try:
        while app._quality_task is None or len(app.tasks) < 6:
            await asyncio.sleep(0)
        shutdown_task = asyncio.create_task(app._async_shutdown(signal.SIGTERM))
        await asyncio.wait_for(shutdown_task, 10)
        assert app._quality_queue.qsize() >= 1 and app._quality_task.done(), \
            "precondition: events queued with no live consumer"
        await asyncio.wait_for(start_task, 5)           # TimeoutError == the hang
        assert app.shutdown_failure == "ws_client_worker_not_retired"
        assert app.exit_code == run_collector.EXIT_RUN_TASK_CRASHED != 0
        for name in ("trades_writer", "ob_writer", "quality_writer"):
            assert getattr(app, name)._closed is False, f"{name} must stay open under a live worker"
        assert app._quality_wal_closed is False if hasattr(app, "_quality_wal_closed") else True
    finally:
        for t in (start_task, shutdown_task, *app.tasks):
            if t is not None and not t.done():
                t.cancel()
                await asyncio.gather(t, return_exceptions=True)
        app.ws_clients = []
        app.shutdown()


@pytest.mark.asyncio
async def test_usdm_no_recovery_producer_can_start_once_shutdown_has_begun(tmp_path, monkeypatch):
    """The ingest worker is alive (and may keep processing queued frames) until it
    is retired. ``_handle_binance_orderbook`` -> ``_schedule_recovery`` is on its
    path, so a worker alive after the top-of-shutdown recovery cancel could start
    a NEW REST producer that reaches raw_rest_writer. After the shutdown has
    begun no recovery may be started."""
    app = _futures_app(tmp_path, monkeypatch)
    started = []

    async def recover(reason):
        started.append(reason)
        await asyncio.Event().wait()
    monkeypatch.setattr(app, "_recover_binance_book", recover)
    live = _StuckClient(app)
    app.ws_clients = [live]
    try:
        app.running = False
        app.shutdown()                                  # fail-closed branch: worker still "alive"
        assert app._schedule_recovery("sequence_gap_or_initial_snapshot") is False
        await asyncio.sleep(0)
        assert started == [] and (app._recovery_task is None or app._recovery_task.done())
    finally:
        app.ws_clients = []
        app.shutdown()


@pytest.mark.asyncio
async def test_usdm_raw_rest_writer_is_not_closed_while_a_recovery_producer_is_unproven(tmp_path, monkeypatch):
    """Fail-closed branch: raw_rest_writer is closed only when no REST producer can
    still capture. A recovery task that survived its cancel request (here: one
    that swallows it) is a live producer, so the writer must stay open."""
    app = _futures_app(tmp_path, monkeypatch)
    release = asyncio.Event()

    async def stubborn_recovery(reason):
        while not release.is_set():
            try:
                await asyncio.sleep(0.005)
            except asyncio.CancelledError:
                pass
    app._recovery_task = asyncio.ensure_future(stubborn_recovery("x"))
    await asyncio.sleep(0)
    app.ws_clients = [_StuckClient(app)]
    try:
        app.shutdown()
        await asyncio.sleep(0.02)
        assert not app._recovery_task.done(), "precondition: the producer survived its cancel"
        assert app.raw_rest_writer._closed is False
        # The recovery can also persist reconstructed book rows and emit quality events, so what it
        # can still reach beyond raw_rest must stay open too.
        assert app.raw_book_writer._closed is False
        assert app.quality_writer._closed is False
        assert not getattr(app, "_quality_wal_closed", False)
        assert app.raw_wire_writer._closed is True       # receive loop stopped: safe either way
        assert app.shutdown_failure == "ws_client_worker_not_retired"
    finally:
        release.set()
        await asyncio.gather(app._recovery_task, return_exceptions=True)
        app.ws_clients = []
        app.shutdown()


@pytest.mark.asyncio
async def test_usdm_raw_wire_writer_is_not_closed_while_ingress_is_not_stopped(tmp_path, monkeypatch):
    """Only the receive loop writes raw_wire_writer. If it is not known to be
    stopped (e.g. stop() itself failed) the writer must not be closed beneath it."""
    app = _futures_app(tmp_path, monkeypatch)
    live = _StuckClient(app)
    live.ingress_stopped = False
    live.stop = lambda: (_ for _ in ()).throw(RuntimeError("stop broke"))
    app.ws_clients = [live]
    try:
        app.shutdown()
        assert app.raw_wire_writer._closed is False
        assert app.shutdown_failure == "ws_client_worker_not_retired"
    finally:
        live.ingress_stopped = True
        live.stop = lambda: None
        live.lifecycle_idle = True
        app.shutdown()


# ---------------------------------------------------- Spot runner (REST) ---

class _DrainingClient:
    """Idle-reporting client double whose graceful wait runs ``during_drain``
    (what the still-alive worker does while it drains queued frames)."""
    shutdown_deadline_s = 0.1
    worker_retire_timeout_s = 0.1
    ingress_stopped = True

    def __init__(self, during_drain, idle=True):
        self.during_drain, self.idle = during_drain, idle

    @property
    def lifecycle_idle(self):
        return self.idle

    def stop(self):
        return None

    async def wait_stopped(self, timeout_s):
        self.during_drain()
        await asyncio.sleep(0)
        return self.idle


@pytest.mark.parametrize("idle", [True, False])
@pytest.mark.asyncio
async def test_spot_raw_rest_writer_is_never_closed_beneath_a_recovery_started_during_the_drain(tmp_path, idle):
    """Spot cancels its recovery task BEFORE waiting for the client, then closes
    raw_rest_writer after the wait. The worker, alive during that wait, can start
    a new recovery (``_schedule_recovery`` is on its on_message path) that nobody
    cancels -- its REST capture would land on a closed writer."""
    app = run_binance_spot_collector.BinanceSpotCollectorApp(data_dir=str(tmp_path))
    log = []
    release = asyncio.Event()

    async def recover(reason):
        log.append("recovery_running")
        try:
            await release.wait()
        except asyncio.CancelledError:
            log.append("recovery_cancelled")
            raise
        log.append("recovery_captured_after_close" if app.raw_rest_writer._closed else "recovery_captured")
        return True
    app._recover_book = recover

    real_close = app.raw_rest_writer.close

    def recording_close():
        task = app._recovery_task
        log.append(f"raw_rest_close(recovery_alive={task is not None and not task.done()})")
        real_close()
    app.raw_rest_writer.close = recording_close

    app.client = _DrainingClient(lambda: app._schedule_recovery("sequence_gap"), idle=idle)
    try:
        await app.shutdown()
        await asyncio.sleep(0.01)
        assert "recovery_captured_after_close" not in log, log
        for entry in log:
            if entry.startswith("raw_rest_close"):
                assert entry == "raw_rest_close(recovery_alive=False)", log
    finally:
        release.set()
        task = app._recovery_task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        app.client = _DrainingClient(lambda: None)
        try:
            await app.shutdown()
        except Exception:
            pass


# ---------------------------------------------------------------- budget ---

def _unit_timeout_stop_s() -> float:
    text = (REPO / "collector" / "btc-collector.service").read_text()
    return float(re.search(r"^TimeoutStopSec=(\d+)\s*$", text, re.M).group(1))


def test_websockets_close_timeout_default_matches_the_budget_assumption():
    """shutdown_deadline_s budgets the close handshake at CLOSE_HANDSHAKE_S. The
    value is a websockets default (the client passes no override), so pin it: a
    dependency bump that raises it must fail here, not at 3 a.m."""
    import inspect
    import websockets
    from collector.collector.websocket_client import CLOSE_HANDSHAKE_S
    from websockets.asyncio.client import connect
    default = inspect.signature(connect.__init__).parameters["close_timeout"].default
    assert default is not None and default <= CLOSE_HANDSHAKE_S, (websockets.__version__, default)


def test_websocket_shutdown_budget_fits_systemd_timeout_with_finalization_reserve():
    """The part of the stop sequence that code bounds (client graceful wait plus
    the runner's hard fallback) must leave FINALIZATION_RESERVE_S of the unit's
    TimeoutStopSec for everything code does not bound (quality drain, ~10 writer
    finalizations with fsync, WAL close, interpreter teardown). The reserve is an
    engineering allowance, not a measurement."""
    from collector.collector.websocket_client import FINALIZATION_RESERVE_S
    client = WebSocketClient("ws://x", lambda *a, **k: None)
    graceful = client.shutdown_deadline_s
    fallback = client.worker_retire_timeout_s + run_collector.WS_FALLBACK_SLACK_S
    assert graceful + fallback + FINALIZATION_RESERVE_S <= _unit_timeout_stop_s(), (
        graceful, fallback, FINALIZATION_RESERVE_S, _unit_timeout_stop_s())


# ------------------------------------------------- older-interpreter path ---

@pytest.mark.asyncio
async def test_receive_fallback_without_eager_tasks_still_delivers_every_frame_and_stops():
    """Python < 3.12 has no ``eager_start``: Task() raises TypeError and the
    receive uses ensure_future. Forced here on any interpreter (CI runs 3.12
    only) -- ordering, delivery and stop() behaviour must be the same."""
    real_task = asyncio.Task

    class _NoEager(real_task):
        def __init__(self, coro, *, loop=None, name=None, context=None, **kw):
            if "eager_start" in kw:
                raise TypeError("Task.__init__() got an unexpected keyword argument 'eager_start'")
            super().__init__(coro, loop=loop, name=name, context=context)

    seen, hold = [], asyncio.Event()

    async def on_message(data, ts, connection_id=None):
        seen.append(data["n"])

    client = WebSocketClient("ws://x", on_message, ingest_queue_maxsize=10)
    task = None
    try:
        with patch("collector.collector.websocket_client.asyncio.Task", _NoEager), \
                patch("collector.collector.websocket_client.websockets.connect",
                      new=lambda url, **k: _Conn([json.dumps({"n": i}) for i in range(1, 6)], hold)):
            task = asyncio.create_task(client.start())
            while client.frames_processed < 5:
                await asyncio.sleep(0)
            client.stop()
            await asyncio.wait_for(task, 5)
        assert seen == [1, 2, 3, 4, 5]
        assert not hold.is_set() and client.lifecycle_idle
    finally:
        hold.set()
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


# ------------------------------------- standalone runners: raw_wire gating ---

@pytest.mark.parametrize("module,cls", [
    (run_binance_spot_collector, "BinanceSpotCollectorApp"),
    ("run_bybit_collector", "BybitCollectorApp"),
    ("run_okx_collector", "OKXCollectorApp"),
])
@pytest.mark.asyncio
async def test_standalone_raw_wire_writer_stays_open_while_ingress_is_not_stopped(tmp_path, module, cls):
    """The receive loop is raw_wire_writer's only producer. When the client does not
    report ingress stopped (e.g. its stop() failed) the writer must not be closed
    under it -- whatever the worker's state."""
    import importlib
    mod = importlib.import_module(module) if isinstance(module, str) else module
    app = getattr(mod, cls)(data_dir=str(tmp_path))

    class _Client(_DrainingClient):
        ingress_stopped = False
    app.client = _Client(lambda: None, idle=False)
    try:
        await app.shutdown()
        assert app.raw_wire_writer._closed is False
        assert app.failure_policy.shutdown_failure == "ws_client_worker_not_retired"
    finally:
        app.client = _DrainingClient(lambda: None)
        try:
            await app.shutdown()
        except Exception:
            pass
