"""Shutdown lifecycle of WebSocketClient and of the runners that own it.

Companion to the five regression tests in test_websocket_client.py (PR #111).
Those prove the USD-M runner path end to end; these pin the individual
mechanisms of the fix with deterministic events/barriers (no sleep-based
proofs): quiet-socket release, in-flight/backlog accounting, bounded fallback,
cancellation inside the drain, an uncancellable worker, repeated shutdown, the
typed-fatal path, and writer-close gating in every runner.

Scope: asyncio-level behaviour with fake sockets. No live exchange, no systemd,
no power-loss durability is exercised here.
"""
import asyncio
import json
from contextlib import contextmanager
from unittest.mock import patch

import pytest

import run_binance_spot_collector
import run_bybit_collector
import run_collector
import run_okx_collector
from collector.collector.standalone_failure_policy import (
    StandaloneFailurePolicy, await_client_idle, supervise_standalone_runner)
from collector.collector.storage_errors import FatalStorageError
from collector.collector.websocket_client import WebSocketClient, WorkerRetirementError


class _Conn:
    """websockets.connect() stand-in: yields ``frames`` then stays open and quiet
    until ``hold`` is set, so only stop() can end the receive loop."""

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


def _frames(n):
    return [json.dumps({"n": i}) for i in range(1, n + 1)]


@contextmanager
def _connect_patch(frames, hold):
    with patch("collector.collector.websocket_client.websockets.connect",
               new=lambda url, **k: _Conn(frames, hold)):
        yield


def _accounted(c):
    """Every enqueued frame must land in exactly one of these buckets."""
    return (c.frames_processed + c.processing_errors + c.frames_discarded
            + c.frames_abandoned_queued + c.frames_abandoned_in_flight)


async def _until(predicate):
    while not predicate():
        await asyncio.sleep(0)


async def _cleanup(client, task, *releases):
    for event in releases:
        event.set()
    for t in (client._worker_task, task):
        if t is not None and not t.done():
            t.cancel()
            await asyncio.gather(t, return_exceptions=True)


# ------------------------------------------------------------------ client ---

@pytest.mark.asyncio
async def test_stop_releases_a_quiet_socket_without_another_frame():
    hold, connected = asyncio.Event(), asyncio.Event()
    client = WebSocketClient("ws://x", lambda *a, **k: None, on_reconnect=connected.set,
                             ingest_queue_maxsize=10)
    task = None
    try:
        with _connect_patch([], hold):
            task = asyncio.create_task(client.start())
            await asyncio.wait_for(connected.wait(), 5)
            client.stop()
            await asyncio.wait_for(task, 5)           # would hang on a quiet socket before the fix
        assert not hold.is_set(), "start() ended only because the test released the socket"
        assert client.lifecycle_idle and client._worker_task is None
        assert await client.wait_stopped(1.0) is True
    finally:
        await _cleanup(client, task, hold)


@pytest.mark.asyncio
async def test_stop_requested_before_start_runs_never_connects():
    connects = []
    client = WebSocketClient("ws://x", lambda *a, **k: None)
    client.stop()
    with patch("collector.collector.websocket_client.websockets.connect",
               new=lambda url, **k: connects.append(url)):
        await asyncio.wait_for(client.start(), 5)
    assert connects == [] and client.running is False
    assert await client.wait_stopped(1.0) is True


@pytest.mark.asyncio
async def test_stop_cuts_a_reconnect_backoff_sleep_short():
    sleeping = asyncio.Event()
    client = WebSocketClient("ws://x", lambda *a, **k: None)

    def long_delay():
        sleeping.set()
        return 1000.0
    client.backoff.next_delay = long_delay

    def refuse(url, **k):
        raise ConnectionRefusedError("down")
    task = None
    try:
        with patch("collector.collector.websocket_client.websockets.connect", new=refuse):
            task = asyncio.create_task(client.start())
            await asyncio.wait_for(sleeping.wait(), 5)
            client.stop()
            await asyncio.wait_for(task, 5)           # not 1000 s
    finally:
        await _cleanup(client, task)


@pytest.mark.asyncio
async def test_in_flight_item_and_backlog_complete_in_fifo_order_on_graceful_stop():
    hold, in_flight, release, connected = (asyncio.Event() for _ in range(4))
    seen = []

    async def on_message(data, ts, connection_id=None):
        seen.append(data["n"])
        if len(seen) == 1:
            in_flight.set()
            await release.wait()

    client = WebSocketClient("ws://x", on_message, on_reconnect=connected.set, ingest_queue_maxsize=10)
    task = None
    try:
        with _connect_patch(_frames(4), hold):
            task = asyncio.create_task(client.start())
            await asyncio.wait_for(in_flight.wait(), 5)
            await asyncio.wait_for(_until(lambda: client.frames_enqueued == 4), 5)
            client.stop()
            asyncio.get_running_loop().call_soon(release.set)    # in-flight work finishes after stop
            await asyncio.wait_for(task, 10)
        assert seen == [1, 2, 3, 4]                                # FIFO, nothing skipped
        assert (client.frames_processed, client.processing_errors) == (4, 0)
        assert (client.frames_abandoned_queued, client.frames_abandoned_in_flight,
                client.frames_abandoned_at_shutdown) == (0, 0, 0)
        assert _accounted(client) == client.frames_enqueued
        assert client._worker_task is None and client._ingest_queue.empty()
    finally:
        await _cleanup(client, task, hold, release)


@pytest.mark.asyncio
async def test_blocked_callback_is_abandoned_once_and_counted_apart_from_the_queue():
    hold, in_flight = asyncio.Event(), asyncio.Event()
    events = []

    async def wedged(data, ts, connection_id=None):
        in_flight.set()
        await asyncio.Event().wait()

    client = WebSocketClient("ws://x", wedged, ingest_queue_maxsize=10,
                             on_quality_event=lambda *a: events.append(a[1]))
    client.shutdown_drain_timeout_s = 0.05
    task_done_calls = []
    real_task_done = client._ingest_queue.task_done
    client._ingest_queue.task_done = lambda: (task_done_calls.append(1), real_task_done())
    task = None
    try:
        with _connect_patch(_frames(4), hold):
            task = asyncio.create_task(client.start())
            await asyncio.wait_for(in_flight.wait(), 5)
            await asyncio.wait_for(_until(lambda: client.frames_enqueued == 4), 5)
            client.stop()
            await asyncio.wait_for(task, 10)                      # bounded: finishes, no exception
        assert (client.frames_abandoned_queued, client.frames_abandoned_in_flight) == (3, 1)
        assert client.frames_abandoned_at_shutdown == 4           # queued + in flight, not double counted
        assert (client.frames_processed, client.processing_errors) == (0, 0)
        assert _accounted(client) == client.frames_enqueued
        assert len(task_done_calls) == 1, "only the one dequeued item may receive task_done()"
        assert "ingest_queue_drain_timeout:3" in events and "ingest_item_abandoned_in_flight" in events
        assert client._worker_task is None
    finally:
        await _cleanup(client, task, hold)


@pytest.mark.asyncio
async def test_cancel_inside_the_drain_counts_queue_and_in_flight_once_and_retires_the_worker():
    hold, in_flight, draining = (asyncio.Event() for _ in range(3))

    async def wedged(data, ts, connection_id=None):
        in_flight.set()
        await asyncio.Event().wait()

    client = WebSocketClient("ws://x", wedged, ingest_queue_maxsize=10)
    real_drain = client._drain_and_stop_worker

    async def announcing():
        draining.set()
        await real_drain()
    client._drain_and_stop_worker = announcing
    task = None
    try:
        with _connect_patch(_frames(4), hold):
            task = asyncio.create_task(client.start())
            await asyncio.wait_for(in_flight.wait(), 5)
            await asyncio.wait_for(_until(lambda: client.frames_enqueued == 4), 5)
            worker = client._worker_task
            client.stop()
            await asyncio.wait_for(draining.wait(), 5)             # parked in the 30 s drain
            task.cancel()
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 5)
        assert task.cancelled(), "the cancellation must propagate, not be swallowed"
        assert worker.done()
        assert (client.frames_abandoned_queued, client.frames_abandoned_in_flight) == (3, 1)
        assert (client.frames_processed, client.processing_errors) == (0, 0)
        assert _accounted(client) == client.frames_enqueued
    finally:
        await _cleanup(client, task, hold)


@pytest.mark.asyncio
async def test_cancelling_start_while_receiving_still_runs_the_tail_and_retires_the_worker():
    """The hard-cancel path (a runner whose graceful wait overran): start() is
    cancelled while parked on the socket, not inside the drain. The tail must
    still run, so the worker is retired and the accounting is exact."""
    hold, in_flight = asyncio.Event(), asyncio.Event()

    async def wedged(data, ts, connection_id=None):
        in_flight.set()
        await asyncio.Event().wait()

    client = WebSocketClient("ws://x", wedged, ingest_queue_maxsize=10)
    client.shutdown_drain_timeout_s = 0.05
    task = None
    try:
        with _connect_patch(_frames(4), hold):
            task = asyncio.create_task(client.start())
            await asyncio.wait_for(in_flight.wait(), 5)
            await asyncio.wait_for(_until(lambda: client.frames_enqueued == 4), 5)
            worker = client._worker_task
            task.cancel()                                          # no stop(): cancelled mid-receive
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 10)
        assert task.cancelled() and worker.done()
        assert (client.frames_abandoned_queued, client.frames_abandoned_in_flight) == (3, 1)
        assert (client.frames_processed, client.processing_errors) == (0, 0)
        assert _accounted(client) == client.frames_enqueued
    finally:
        await _cleanup(client, task, hold)


@pytest.mark.asyncio
async def test_an_uncancellable_worker_is_reported_not_forgotten():
    hold, entered, let_go = (asyncio.Event() for _ in range(3))
    events = []

    async def stubborn(data, ts, connection_id=None):
        entered.set()
        while not let_go.is_set():
            try:
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:                          # pathological: ignores cancellation
                pass

    client = WebSocketClient("ws://x", stubborn, ingest_queue_maxsize=10,
                             on_quality_event=lambda *a: events.append(a[1]))
    client.shutdown_drain_timeout_s = 0.05
    client.worker_retire_timeout_s = 0.05
    task = None
    try:
        with _connect_patch(_frames(1), hold):
            task = asyncio.create_task(client.start())
            await asyncio.wait_for(entered.wait(), 5)
            client.stop()
            with pytest.raises(WorkerRetirementError):
                await asyncio.wait_for(task, 10)
        assert client.worker_retirement_failed and client.worker_active
        assert client.lifecycle_idle is False
        assert await client.wait_stopped(0.05) is False
        assert "ingest_worker_retirement_failed" in events
        assert client._worker_task is not None, "the live worker's reference must not be dropped"
    finally:
        await _cleanup(client, task, hold, let_go)


@pytest.mark.asyncio
async def test_repeated_stop_and_drain_are_idempotent():
    hold, connected = asyncio.Event(), asyncio.Event()
    client = WebSocketClient("ws://x", lambda *a, **k: None, on_reconnect=connected.set)
    task = None
    try:
        with _connect_patch([], hold):
            task = asyncio.create_task(client.start())
            await asyncio.wait_for(connected.wait(), 5)
            client.stop()
            await asyncio.wait_for(task, 5)
        before = (client.frames_abandoned_at_shutdown, client.processing_errors)
        client.stop()
        await client._drain_and_stop_worker()
        assert (client.frames_abandoned_at_shutdown, client.processing_errors) == before
        assert await client.wait_stopped(1.0) is True
    finally:
        await _cleanup(client, task, hold)


@pytest.mark.asyncio
async def test_a_typed_fatal_during_the_shutdown_drain_stays_fatal_not_a_processing_error():
    hold, in_flight, release = (asyncio.Event() for _ in range(3))
    seen, fatals = [], []

    async def on_message(data, ts, connection_id=None):
        seen.append(data["n"])
        if len(seen) == 1:
            in_flight.set()
            await release.wait()
        if data["n"] == 2:
            raise FatalStorageError("boom", stream="trades")

    client = WebSocketClient("ws://x", on_message, ingest_queue_maxsize=10,
                             on_fatal=lambda exc, origin: fatals.append(origin))
    task = None
    try:
        with _connect_patch(_frames(3), hold):
            task = asyncio.create_task(client.start())
            await asyncio.wait_for(in_flight.wait(), 5)
            await asyncio.wait_for(_until(lambda: client.frames_enqueued == 3), 5)
            client.stop()
            asyncio.get_running_loop().call_soon(release.set)
            await asyncio.wait_for(task, 10)
        assert fatals == ["worker"] and client.fatal_storage_errors == 1
        assert client.processing_errors == 0 and client.frames_processed == 2
    finally:
        await _cleanup(client, task, hold, release)


@pytest.mark.asyncio
async def test_await_client_idle_returns_only_after_the_worker_has_finished():
    """Standalone-runner ordering: stop() merely requests; the caller must not
    reach its writer closes until the in-flight callback is done."""
    hold, in_flight, release = (asyncio.Event() for _ in range(3))
    log = []

    async def on_message(data, ts, connection_id=None):
        in_flight.set()
        await release.wait()
        log.append("callback_done")

    client = WebSocketClient("ws://x", on_message, ingest_queue_maxsize=10)
    policy = StandaloneFailurePolicy(venue="T", streams={})
    task = None
    try:
        with _connect_patch(_frames(1), hold):
            task = asyncio.create_task(client.start())
            await asyncio.wait_for(in_flight.wait(), 5)
            client.stop()
            asyncio.get_running_loop().call_soon(release.set)
            assert await asyncio.wait_for(await_client_idle(client, policy), 10) is True
            log.append("writers_closed")
        assert log == ["callback_done", "writers_closed"]
        assert policy.shutdown_failure is None and policy.exit_code == 0
    finally:
        await _cleanup(client, task, hold, release)


# ----------------------------------------------------------------- runners ---

class _FakeClient:
    """Client double: reports a configurable idle state."""
    shutdown_deadline_s = 0.1
    worker_retire_timeout_s = 0.1

    def __init__(self, idle=True, wait_raises=False):
        self.idle, self.wait_raises, self.stops, self.waits = idle, wait_raises, 0, 0
        self.connected = False

    @property
    def lifecycle_idle(self):
        return self.idle

    def stop(self):
        self.stops += 1

    async def wait_stopped(self, timeout_s):
        self.waits += 1
        if self.wait_raises:
            raise RuntimeError("wait broke")
        return self.idle


def _futures_app(tmp_path, monkeypatch):
    (tmp_path / "data").mkdir()
    monkeypatch.chdir(tmp_path)
    return run_collector.CollectorApp()


@pytest.mark.asyncio
async def test_stop_ws_clients_waits_without_cancelling_when_every_client_is_idle(tmp_path, monkeypatch):
    app = _futures_app(tmp_path, monkeypatch)
    app.ws_clients = [_FakeClient(), _FakeClient()]
    cancelled = []

    class _T:
        def cancel(self):
            cancelled.append(1)
    app.tasks = [_T()]
    try:
        assert await app._stop_ws_clients() is True
        assert [c.stops for c in app.ws_clients] == [1, 1] and cancelled == []
    finally:
        app.ws_clients = []
        app.shutdown()


@pytest.mark.asyncio
async def test_stop_ws_clients_falls_back_to_cancel_then_reports_a_client_that_never_idles(tmp_path, monkeypatch):
    app = _futures_app(tmp_path, monkeypatch)
    stuck = _FakeClient(idle=False)
    app.ws_clients = [stuck, _FakeClient(wait_raises=True)]
    cancelled = []

    class _T:
        def cancel(self):
            cancelled.append(1)
    app.tasks = [_T()]
    try:
        assert await app._stop_ws_clients() is False
        assert cancelled == [1] and stuck.waits == 2          # bounded wait, cancel, bounded wait
    finally:
        app.ws_clients = []
        app.shutdown()


def test_usdm_shutdown_with_a_live_client_keeps_worker_writers_open_and_fails_explicitly(tmp_path, monkeypatch):
    app = _futures_app(tmp_path, monkeypatch)
    live = _FakeClient(idle=False)
    app.ws_clients = [live]
    try:
        app.shutdown()
        assert app.shutdown_failure == "ws_client_worker_not_retired"
        assert app.exit_code == run_collector.EXIT_RUN_TASK_CRASHED != 0
        assert app.raw_wire_writer._closed and app.raw_rest_writer._closed
        for name in ("trades_writer", "raw_trades_writer", "ob_writer", "raw_book_writer", "quality_writer"):
            assert getattr(app, name)._closed is False, name
        assert not getattr(app, "_shutdown_done", False)
        live.idle = True                                        # the worker finally ends: a retry completes
        app.shutdown()
        assert app._shutdown_done and app.trades_writer._closed and app.quality_writer._closed
    finally:
        live.idle = True
        app.shutdown()


@pytest.mark.parametrize("module,cls,raw,dependent", [
    (run_binance_spot_collector, "BinanceSpotCollectorApp", ("raw_wire_writer", "raw_rest_writer"),
     ("trades_writer", "ob_writer", "quality_writer")),
    (run_bybit_collector, "BybitCollectorApp", ("raw_wire_writer",),
     ("ob_writer", "trades_writer", "mark_writer", "quality_writer")),
    (run_okx_collector, "OKXCollectorApp", ("raw_wire_writer",),
     ("trades_writer", "mark_writer", "quality_writer")),
])
@pytest.mark.asyncio
async def test_standalone_runner_shutdown_does_not_close_worker_writers_under_a_live_client(
        tmp_path, module, cls, raw, dependent):
    app = getattr(module, cls)(data_dir=str(tmp_path))
    app.client = _FakeClient(idle=False)
    try:
        await app.shutdown()
        assert app.failure_policy.shutdown_failure == "ws_client_worker_not_retired"
        assert app.exit_code == 1
        for name in raw:
            assert getattr(app, name)._closed, name
        for name in dependent:
            assert getattr(app, name)._closed is False, name
    finally:
        app.client = _FakeClient(idle=True)
        try:
            await app.shutdown()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_supervisor_turns_a_worker_retirement_failure_into_a_nonzero_exit():
    stop = asyncio.Event()

    class _App:
        failure_policy = StandaloneFailurePolicy(venue="T", streams={})

        async def run(self):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise WorkerRetirementError("worker survived")

        async def shutdown(self):
            return None

    stop.set()
    assert await asyncio.wait_for(supervise_standalone_runner(_App(), stop, task_timeout_s=2.0), 10) == 1
