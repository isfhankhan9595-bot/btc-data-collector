"""P1: shutdown during the INLINE startup book recovery.

``CollectorApp.start()`` runs ``await self._recover_binance_book("startup")`` inline, in
the main ``start()`` task. That coroutine is a REST producer (HTTP snapshot, then
``_capture_rest`` -> ``raw_rest_writer``) but it is not ``_recovery_task`` (that handle
is only set by ``_schedule_recovery``), it is not in ``self.tasks``, and the signal
handler's ``_async_shutdown`` runs in a different task, so shutdown neither cancelled it
nor counted it as a producer: ``_rest_producers_stopped()`` was True and ``raw_rest_writer``
was closed while the snapshot request was still in flight.

These tests drive the real ``CollectorApp.start()`` and the real ``_async_shutdown`` with
only the network edge (``aiohttp``) and the websocket clients faked.
"""
import asyncio
import json
import signal

import pytest

import run_collector


class _FakeWs:
    """Websocket client double that is connected and fully idle (nothing to retire)."""
    url = "ws://fake"
    shutdown_deadline_s = 0.05
    worker_retire_timeout_s = 0.05
    lifecycle_idle = True
    ingress_stopped = True

    def __init__(self, connect_gate=None):
        self._connect_gate = connect_gate

    def stop(self):
        pass

    async def start(self):
        await asyncio.Event().wait()

    async def wait_connected(self, timeout_seconds=0.0):
        if self._connect_gate is not None:
            await self._connect_gate.wait()
        return True

    async def wait_stopped(self, timeout_s):
        return True


class _Response:
    status = 200
    headers = {}

    def __init__(self, body):
        self._body = body

    async def text(self):
        return self._body

    def raise_for_status(self):
        return None


class _Get:
    ended = asyncio.Event()                    # class-level: one request at a time in these tests

    def __init__(self, gate, started, body):
        self._gate, self._started, self._body = gate, started, body

    async def __aenter__(self):
        self._started.set()
        self.ended.clear()
        try:
            await self._gate.wait()            # the HTTP request is in flight until released OR cancelled
        except BaseException:
            self.ended.set()                   # abandoned (cancelled): the request is no longer in flight
            raise
        return _Response(self._body)

    async def __aexit__(self, *exc):
        self.ended.set()
        return False


class _Session:
    def __init__(self, gate, started, body, **kw):
        self._args = (gate, started, body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, **kw):
        return _Get(*self._args)


async def _quiet(*a, **k):
    await asyncio.Event().wait()


def _snapshot_body():
    return json.dumps({"lastUpdateId": 10, "bids": [["100.0", "1.0"]], "asks": [["101.0", "1.0"]]})


async def _run_sigterm_during_startup_snapshot(tmp_path, monkeypatch, *, phase="inline_recovery"):
    import aiohttp
    (tmp_path / "data").mkdir()
    monkeypatch.chdir(tmp_path)
    app = run_collector.CollectorApp()
    connect_gate = asyncio.Event() if phase == "pre_connect" else None
    app.ws_clients = [_FakeWs(connect_gate)]
    gate, request_started = asyncio.Event(), asyncio.Event()
    _Get.ended = asyncio.Event()
    monkeypatch.setattr(aiohttp, "ClientSession",
                        lambda **kw: _Session(gate, request_started, _snapshot_body()))

    async def no_preflight():
        return False
    monkeypatch.setattr(app, "_storage_pressure_preflight_failed", no_preflight)
    spawned_after_shutdown = []
    monkeypatch.setattr(app.health_monitor, "start", _quiet)
    monkeypatch.setattr(app, "_storage_pressure_loop", _quiet)
    monkeypatch.setattr(app, "_verify_startup_streams", _quiet)

    async def poll_oi():
        spawned_after_shutdown.append("oi_poll")
        await _quiet()
    monkeypatch.setattr(app, "_poll_openinterest", poll_oi)

    captured = []                                     # (writer closed at capture time, purpose)
    real_capture = app.raw_capture.capture_rest

    def recording_capture(record):
        captured.append((app.raw_rest_writer._closed, record.purpose))
        return real_capture(record)
    monkeypatch.setattr(app.raw_capture, "capture_rest", recording_capture)

    loop = asyncio.get_running_loop()
    start_task, shutdown_task = asyncio.create_task(app.start()), None
    try:
        if phase == "inline_recovery":
            await asyncio.wait_for(request_started.wait(), 10)    # the inline startup request is in flight
            assert app._recovery_task is None, "precondition: it is NOT the _recovery_task"
        else:
            while len(app.tasks) < 3:                             # start() is parked in wait_connected
                await asyncio.sleep(0)
        shutdown_task = asyncio.create_task(app._async_shutdown(signal.SIGTERM))
        await asyncio.wait_for(shutdown_task, 10)
        request_in_flight_at_shutdown_end = request_started.is_set() and not _Get.ended.is_set()
        closed_at_shutdown_end = app.raw_rest_writer._closed
        gate.set()                                            # the response finally arrives
        if connect_gate is not None:
            connect_gate.set()                                # start() resumes after shutdown
        done, _ = await asyncio.wait({start_task}, timeout=10)
        assert done, "start() never returned after shutdown"
        return app, captured, request_in_flight_at_shutdown_end, closed_at_shutdown_end, spawned_after_shutdown
    finally:
        gate.set()
        if connect_gate is not None:
            connect_gate.set()
        for t in (start_task, shutdown_task, *app.tasks):
            if t is not None and not t.done():
                t.cancel()
                await asyncio.gather(t, return_exceptions=True)
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        app.ws_clients = []
        app.shutdown()


@pytest.mark.asyncio
async def test_sigterm_during_the_inline_startup_snapshot_never_closes_raw_rest_beneath_it(tmp_path, monkeypatch):
    app, captured, in_flight, closed, _ = await _run_sigterm_during_startup_snapshot(tmp_path, monkeypatch)
    assert not (in_flight and closed), (
        "raw_rest_writer was closed while the startup snapshot request was still in flight")
    assert all(not was_closed for was_closed, _ in captured), (
        f"the startup snapshot response was captured into a CLOSED raw_rest writer: {captured}")


@pytest.mark.asyncio
async def test_a_startup_snapshot_abandoned_by_shutdown_is_not_a_failed_shutdown(tmp_path, monkeypatch):
    app, captured, _, _, _ = await _run_sigterm_during_startup_snapshot(tmp_path, monkeypatch)
    assert getattr(app, "shutdown_failure", None) is None and app.exit_code == 0
    assert captured == [], "an abandoned request produces no raw_rest record (same as a cancelled recovery task)"
    assert app.raw_rest_writer._closed and app._shutdown_done


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["pre_connect", "inline_recovery"])
async def test_start_spawns_no_new_tasks_after_shutdown_began_during_startup(tmp_path, monkeypatch, phase):
    _, _, _, _, spawned = await _run_sigterm_during_startup_snapshot(tmp_path, monkeypatch, phase=phase)
    assert spawned == [], f"start() resumed after shutdown and started: {spawned}"


# ---------------------------------------------- fail-closed and behaviour preservation ---

def _app_with_stubbed_neighbours(tmp_path, monkeypatch):
    (tmp_path / "data").mkdir()
    monkeypatch.chdir(tmp_path)
    app = run_collector.CollectorApp()
    app.ws_clients = [_FakeWs()]

    async def no_preflight():
        return False
    monkeypatch.setattr(app, "_storage_pressure_preflight_failed", no_preflight)
    monkeypatch.setattr(app.health_monitor, "start", _quiet)
    monkeypatch.setattr(app, "_storage_pressure_loop", _quiet)
    monkeypatch.setattr(app, "_verify_startup_streams", _quiet)
    monkeypatch.setattr(app, "_poll_openinterest", _quiet)
    return app


async def _drain(app, *tasks):
    loop = asyncio.get_running_loop()
    for t in (*tasks, *app.tasks, getattr(app, "_startup_recovery_task", None)):
        if t is not None and not t.done():
            t.cancel()
            await asyncio.gather(t, return_exceptions=True)
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.remove_signal_handler(sig)
    app.ws_clients = []
    app._rest_producers_live = False
    app.shutdown()


@pytest.mark.asyncio
async def test_a_startup_recovery_that_ignores_cancel_keeps_its_reachable_storage_open_and_fails_explicitly(
        tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def stubborn(reason):
        entered.set()
        while not release.is_set():
            try:
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                pass
    monkeypatch.setattr(app, "_recover_binance_book", stubborn)
    start_task = asyncio.create_task(app.start())
    shutdown_task = None
    try:
        await asyncio.wait_for(entered.wait(), 10)
        shutdown_task = asyncio.create_task(app._async_shutdown(signal.SIGTERM))
        await asyncio.wait_for(shutdown_task, 10)
        assert app.shutdown_failure == "rest_producer_not_stopped" and app.exit_code != 0
        # The startup recovery can still reach raw REST capture, reconstructed book rows and
        # the quality writer + WAL (REST_PRODUCER_REACH["recovery"]): all stay open ...
        for name in ("raw_rest_writer", "raw_book_writer", "quality_writer"):
            assert getattr(app, name)._closed is False, f"{name} closed beneath a live startup recovery"
        assert not getattr(app, "_quality_wal_closed", False)
        # ... while storage it cannot reach (and the OI poller never started) is finalized.
        assert app.oi_writer._closed and app.trades_writer._closed
        assert not getattr(app, "_shutdown_done", False)
        release.set()                                                         # the producer finally ends
        await asyncio.wait_for(asyncio.gather(start_task, return_exceptions=True), 10)
        app.shutdown()                                                        # retry now succeeds
        assert app.raw_rest_writer._closed and app.raw_book_writer._closed and app.quality_writer._closed
        assert app._shutdown_done
    finally:
        release.set()
        await _drain(app, start_task, shutdown_task)


@pytest.mark.asyncio
async def test_an_exception_from_the_startup_recovery_still_propagates_out_of_start(tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)

    async def boom(reason):
        raise ValueError("snapshot parse failed")
    monkeypatch.setattr(app, "_recover_binance_book", boom)
    start_task = asyncio.create_task(app.start())
    try:
        with pytest.raises(ValueError, match="snapshot parse failed"):
            await asyncio.wait_for(start_task, 10)
    finally:
        await _drain(app, start_task)


@pytest.mark.asyncio
async def test_cancelling_start_during_the_startup_recovery_leaves_no_producer_behind(tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    entered = asyncio.Event()

    async def slow(reason):
        entered.set()
        await asyncio.Event().wait()
    monkeypatch.setattr(app, "_recover_binance_book", slow)
    start_task = asyncio.create_task(app.start())
    try:
        await asyncio.wait_for(entered.wait(), 10)
        producer = app._startup_recovery_task
        start_task.cancel()
        await asyncio.gather(start_task, return_exceptions=True)
        assert producer.done(), "start() was cancelled but its startup recovery kept running"
    finally:
        await _drain(app, start_task)


@pytest.mark.asyncio
async def test_normal_startup_still_recovers_then_starts_the_poller(tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    order = []

    async def recover(reason):
        order.append(("recover", reason))

    async def poll():
        order.append("poll")
        await _quiet()
    monkeypatch.setattr(app, "_recover_binance_book", recover)
    monkeypatch.setattr(app, "_poll_openinterest", poll)
    start_task = asyncio.create_task(app.start())
    try:
        while "poll" not in order:
            await asyncio.sleep(0)
        assert order == [("recover", "startup"), "poll"]       # recovery completes BEFORE the poller starts
        assert app._oi_task is not None and not app._oi_task.done()
    finally:
        await _drain(app, start_task)


# ================================================================================================
# Final audit: startup-recovery lifecycle. What does the PARENT start() task do, and what does the
# shutdown task do, while the startup recovery is (a) cancelled, (b) cancellation-resistant, (c)
# ended by an exception, (d) cancelled for a reason that is not a shutdown?
#
# The cancellation-resistant cases hold the producer alive for the whole assertion block: it is
# released only AFTER the state under test has been asserted. Nothing here claims the application
# can stop such a coroutine; it states what the application does while one exists.
# ================================================================================================

def _await_chain(task):
    """Names of the coroutines a suspended task is awaiting, outermost first (exact, not timed)."""
    names, coro = [], task.get_coro()
    while coro is not None:
        code = getattr(coro, "cr_code", None) or getattr(coro, "gi_code", None)
        if code is None:
            break
        names.append(code.co_name)
        coro = getattr(coro, "cr_await", None) or getattr(coro, "gi_yieldfrom", None)
    return names


class _StubbornRecovery:
    """A startup recovery that swallows every cancel and ends only when ``release`` is set."""

    def __init__(self, on_release="return"):
        self.entered, self.cancel_swallowed, self.release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.swallowed, self.on_release = 0, on_release

    async def __call__(self, reason):
        self.entered.set()
        while not self.release.is_set():
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.swallowed += 1
                self.cancel_swallowed.set()
        if self.on_release == "raise":
            raise RuntimeError("startup recovery failed after it had ignored the cancel")


async def _sigterm_while_stubborn(app, monkeypatch, stubborn):
    """Start the real start(), wait until the stubborn recovery is running, run the real
    _async_shutdown to completion, and wait until the producer has demonstrably swallowed the
    cancel. The producer is NOT released here."""
    monkeypatch.setattr(app, "_recover_binance_book", stubborn)
    monkeypatch.setattr(run_collector, "WS_FALLBACK_SLACK_S", 0.05)      # bound waits, not the logic
    start_task = asyncio.create_task(app.start())
    await asyncio.wait_for(stubborn.entered.wait(), 10)
    shutdown_task = asyncio.create_task(app._async_shutdown(signal.SIGTERM))
    await asyncio.wait_for(shutdown_task, 10)
    await asyncio.wait_for(stubborn.cancel_swallowed.wait(), 10)
    for _ in range(50):                      # give start() every loop turn it could use to move on
        await asyncio.sleep(0)
    return start_task, shutdown_task


@pytest.mark.asyncio
async def test_while_the_startup_recovery_ignores_cancel_start_is_parked_and_the_failure_is_latched(
        tmp_path, monkeypatch):
    """Nothing is released before these assertions."""
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    stubborn = _StubbornRecovery()
    spawned = []

    async def poll():
        spawned.append("oi_poll")
        await _quiet()
    monkeypatch.setattr(app, "_poll_openinterest", poll)
    monkeypatch.setattr(app.health_monitor, "start", lambda: spawned.append("health_monitor") or _quiet())
    start_task = shutdown_task = None
    try:
        start_task, shutdown_task = await _sigterm_while_stubborn(app, monkeypatch, stubborn)
        producer = app._startup_recovery_task
        # shutdown task: finished, without an exception, having latched an explicit non-zero status
        assert shutdown_task.done() and shutdown_task.exception() is None
        assert app.shutdown_failure == "rest_producer_not_stopped"
        assert app.exit_code == run_collector.EXIT_RUN_TASK_CRASHED != 0
        # the producer received the cancel, ignored it, and is alive
        assert stubborn.swallowed >= 1 and not producer.done()
        # the PARENT start() task has NOT returned: it is suspended inside _run_startup_recovery
        assert not start_task.done()
        assert "_run_startup_recovery" in _await_chain(start_task), _await_chain(start_task)
        # ... which is why a runner (asyncio.run) cannot return the latched status: a task still exists
        assert producer in asyncio.all_tasks()
        # shutdown did not complete; exactly the storage the producer can reach is still open
        assert not getattr(app, "_shutdown_done", False)
        for name in ("raw_rest_writer", "raw_book_writer", "quality_writer"):
            assert getattr(app, name)._closed is False, f"{name} closed beneath a live startup recovery"
        assert not getattr(app, "_quality_wal_closed", False)
        assert app.oi_writer._closed and app.trades_writer._closed       # unreachable by that producer
        # no new producer was started after shutdown began, and every ordinary task is gone
        assert spawned == [] and app._oi_task is None
        assert all(t.done() for t in app.tasks)
        assert app.running is False and app._recovery_closed is True
    finally:
        stubborn.release.set()
        await _drain(app, start_task, shutdown_task)


@pytest.mark.asyncio
async def test_when_the_ignoring_startup_recovery_ends_the_deferred_close_completes_without_a_manual_retry(
        tmp_path, monkeypatch):
    """No manual shutdown() call: once the producer ends, shutdown()'s late retry finalizes the rest
    and the parked start() then resumes into a no-op tail and returns."""
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    stubborn = _StubbornRecovery()
    spawned = []

    async def poll():
        spawned.append("oi_poll")
        await _quiet()
    monkeypatch.setattr(app, "_poll_openinterest", poll)
    start_task = shutdown_task = None
    try:
        start_task, shutdown_task = await _sigterm_while_stubborn(app, monkeypatch, stubborn)
        assert not start_task.done() and not getattr(app, "_shutdown_done", False)     # state first ...
        stubborn.release.set()                                                         # ... then release
        done, _ = await asyncio.wait({start_task}, timeout=10)
        assert done, "start() never resumed after the producer ended"
        assert start_task.exception() is None
        assert getattr(app, "_shutdown_done", False), "the deferred close never completed"
        for name in ("raw_rest_writer", "raw_book_writer", "quality_writer", "oi_writer", "trades_writer"):
            assert getattr(app, name)._closed, f"{name} still open after the producer ended"
        assert app.shutdown_failure == "rest_producer_not_stopped" and app.exit_code != 0   # still latched
        assert spawned == [] and app._oi_task is None          # resuming did not start a poller
    finally:
        stubborn.release.set()
        await _drain(app, start_task, shutdown_task)


@pytest.mark.asyncio
@pytest.mark.parametrize("how", [
    pytest.param("raises_instead_of_cancelling", id="raises-when-cancelled"),
    pytest.param("ignores_cancel_then_raises", id="ignores-cancel-then-raises"),
])
async def test_a_startup_recovery_that_fails_after_shutdown_began_cannot_skip_the_close(
        tmp_path, monkeypatch, how):
    """An exception from the recovery used to propagate out of start() BEFORE its try/finally, so a
    producer that ended with an error after shutdown began left the storage it had held open for
    good. The failure is still reported (non-zero status) but start() reaches its tail."""
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    monkeypatch.setattr(run_collector, "WS_FALLBACK_SLACK_S", 0.05)
    entered = asyncio.Event()
    start_task = shutdown_task = None
    stubborn = _StubbornRecovery(on_release="raise")

    async def raises_on_cancel(reason):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise RuntimeError("cleanup failed after the cancel")
    try:
        if how == "ignores_cancel_then_raises":
            start_task, shutdown_task = await _sigterm_while_stubborn(app, monkeypatch, stubborn)
            assert not start_task.done()
            stubborn.release.set()
        else:
            monkeypatch.setattr(app, "_recover_binance_book", raises_on_cancel)
            start_task = asyncio.create_task(app.start())
            await asyncio.wait_for(entered.wait(), 10)
            shutdown_task = asyncio.create_task(app._async_shutdown(signal.SIGTERM))
            await asyncio.wait_for(shutdown_task, 10)
        done, _ = await asyncio.wait({start_task}, timeout=10)
        assert done, "start() never returned"
        assert not start_task.cancelled() and start_task.exception() is None
        assert getattr(app, "_shutdown_done", False), "writers left open although every producer had ended"
        assert app.raw_rest_writer._closed and app.raw_book_writer._closed and app.quality_writer._closed
        assert app.exit_code != 0 and getattr(app, "shutdown_failure", None) is not None   # not hidden
        assert app._oi_task is None
    finally:
        stubborn.release.set()
        await _drain(app, start_task, shutdown_task)


@pytest.mark.asyncio
async def test_an_exception_outside_shutdown_still_propagates_and_latches_nothing(tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)

    async def boom(reason):
        raise ValueError("snapshot parse failed")
    monkeypatch.setattr(app, "_recover_binance_book", boom)
    start_task = asyncio.create_task(app.start())
    try:
        with pytest.raises(ValueError, match="snapshot parse failed"):
            await asyncio.wait_for(start_task, 10)
        # nothing latched; finalization ran (that is what sets _recovery_closed, so it is no longer asserted False)
        assert getattr(app, "shutdown_failure", None) is None and getattr(app, "_shutdown_done", False)
        assert app._oi_task is None                      # no producer after a failed startup either
    finally:
        await _drain(app, start_task)


@pytest.mark.asyncio
async def test_a_cancelled_startup_recovery_that_is_not_a_shutdown_does_not_let_startup_continue(
        tmp_path, monkeypatch):
    """Before this PR the inline await let a CancelledError from the recovery cancel start(). A task
    that merely ended 'cancelled' must not be read as 'recovery finished' unless shutdown began."""
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    spawned = []

    async def poll():
        spawned.append("oi_poll")
        await _quiet()
    monkeypatch.setattr(app, "_poll_openinterest", poll)

    async def cancelled_for_its_own_reasons(reason):
        raise asyncio.CancelledError()
    monkeypatch.setattr(app, "_recover_binance_book", cancelled_for_its_own_reasons)
    start_task = asyncio.create_task(app.start())
    try:
        done, _ = await asyncio.wait({start_task}, timeout=10)
        assert done and start_task.cancelled(), "start() carried on after its recovery was cancelled"
        # nothing started; finalization ran (it sets _recovery_closed, so that is no longer asserted False)
        assert spawned == [] and app._oi_task is None
        assert getattr(app, "shutdown_failure", None) is None and getattr(app, "_shutdown_done", False)
    finally:
        await _drain(app, start_task)


@pytest.mark.asyncio
async def test_no_startup_recovery_task_is_created_when_shutdown_began_before_it(tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    gate, calls = asyncio.Event(), []
    app.ws_clients = [_FakeWs(gate)]

    async def recover(reason):
        calls.append(reason)
    monkeypatch.setattr(app, "_recover_binance_book", recover)
    start_task = asyncio.create_task(app.start())
    try:
        while len(app.tasks) < 3:                               # start() is parked in wait_connected
            await asyncio.sleep(0)
        await asyncio.wait_for(asyncio.create_task(app._async_shutdown(signal.SIGTERM)), 10)
        gate.set()                                              # start() resumes AFTER shutdown began
        done, _ = await asyncio.wait({start_task}, timeout=10)
        assert done and start_task.exception() is None
        assert calls == [] and app._startup_recovery_task is None and app._oi_task is None
    finally:
        gate.set()
        await _drain(app, start_task)
