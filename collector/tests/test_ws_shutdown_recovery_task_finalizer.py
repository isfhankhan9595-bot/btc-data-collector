"""start()'s finalizer must not wait unboundedly on a stubborn ``_recovery_task``.

``_recovery_task`` (set by ``_schedule_recovery``) is a REST producer distinct from
``_startup_recovery_task``. ``start()``'s ``finally`` used to cancel it and then
``await asyncio.gather(..., return_exceptions=True)`` with no bound, so a recovery that swallows
CancelledError parked start() there: ``_stop_rest_producers`` / ``shutdown()`` were never reached,
nothing was latched, and (once the producer finally ended) the late finalization reported a CLEAN
status. These tests drive the real ``CollectorApp.start()`` / ``shutdown()``; only the network edge
and the websocket clients are faked.

"Closed" means ``close()`` was CALLED and RETURNED (ordered event log) and the app's own verdict
(``_writer_close_results``) says it succeeded; a writer never attempted has neither. Nothing here
claims a cancellation-resistant coroutine can be stopped.
"""
import asyncio

import pytest

import run_collector
from collector.tests.test_ws_shutdown_late_producer import (
    _ALL_WRITERS, _RECOVERY_HELD, _assert_fully_finalized_after_producer_end,
    _assert_only_these_are_open, _closed, _closes, _idle_turns, _spy_on_closes)
from collector.tests.test_ws_shutdown_startup_recovery import (
    _app_with_stubbed_neighbours, _drain, _quiet)

DEADLINE_S = 10          # test deadline; the production bound is patched to 0.05 s


class _StubbornSteadyRecovery:
    """Recovery that finishes the startup call at once, then (for a later gap) swallows every cancel
    and ends only when ``release`` is set."""

    def __init__(self, log, on_release="return"):
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.swallowed, self.log, self.on_release = 0, log, on_release

    async def __call__(self, reason):
        if reason == "startup":
            return True
        self.entered.set()
        while not self.release.is_set():
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.swallowed += 1
        self.log.append(("producer_ended",))
        if self.on_release == "raise":
            raise RuntimeError("recovery failed after it had ignored the cancel")
        return True


async def _steady_state_with_stubborn_recovery(app, monkeypatch, log, on_release="return"):
    stubborn = _StubbornSteadyRecovery(log, on_release)
    monkeypatch.setattr(app, "_recover_binance_book", stubborn)
    monkeypatch.setattr(run_collector, "WS_FALLBACK_SLACK_S", 0.05)      # bound the waits, not the logic
    start_task = asyncio.create_task(app.start())
    while app._oi_task is None:                                          # start() reached the gather
        await asyncio.sleep(0)
    assert app._schedule_recovery("gap") is True
    await asyncio.wait_for(stubborn.entered.wait(), DEADLINE_S)
    assert app._recovery_task is not None and not app._recovery_task.done()
    assert app._startup_recovery_task.done(), "the producer under test must be _recovery_task alone"
    return start_task, stubborn


@pytest.mark.asyncio
async def test_caller_cancel_with_a_stubborn_recovery_task_reaches_finalization_and_preserves_storage(
        tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log = []
    _spy_on_closes(app, monkeypatch, log)
    start_task = None
    stubborn = None
    try:
        start_task, stubborn = await _steady_state_with_stubborn_recovery(app, monkeypatch, log)
        producer = app._recovery_task
        start_task.cancel()                                              # the caller cancels start()
        done, _ = await asyncio.wait({start_task}, timeout=DEADLINE_S)
        assert done, "start() is blocked in its finalizer on a _recovery_task that ignores cancellation"
        assert start_task.cancelled(), "caller cancellation must propagate, not be swallowed"
        # --- before the producer is released: ordered shutdown reached, producer still tracked ------
        assert not producer.done() and stubborn.swallowed >= 1 and app._recovery_task is producer
        assert app._closed is True and not getattr(app, "_shutdown_done", False)
        assert app.shutdown_failure == "rest_producer_not_stopped"
        assert app.exit_code == run_collector.EXIT_RUN_TASK_CRASHED != 0
        _assert_only_these_are_open(app, log, _RECOVERY_HELD)
        for _ in range(2):                                               # a repeat is a no-op, not a 2nd close
            app.shutdown()
            _assert_only_these_are_open(app, log, _RECOVERY_HELD)
        # --- release: the late retry finalizes the rest exactly once, in order ----------------------
        stubborn.release.set()
        assert (await asyncio.wait({producer}, timeout=DEADLINE_S))[0]
        await _idle_turns()
        _assert_fully_finalized_after_producer_end(app, log, _RECOVERY_HELD)
        app.shutdown()                                                   # still exactly once
        for name in (*_ALL_WRITERS, "quality_wal"):
            assert len(_closes(log, name)) == 1, f"{name} closed {len(_closes(log, name))} times"
        assert app.shutdown_failure == "rest_producer_not_stopped" and app.exit_code != 0
    finally:
        if stubborn is not None:
            stubborn.release.set()
        await _drain(app, start_task, app._recovery_task)


@pytest.mark.asyncio
async def test_exception_exit_with_a_stubborn_recovery_task_keeps_the_exception_and_fails_closed(
        tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log = []
    _spy_on_closes(app, monkeypatch, log)
    boom, gate = RuntimeError("steady-state task crashed"), asyncio.Event()

    async def crashing_stream_check():
        await gate.wait()
        raise boom
    monkeypatch.setattr(app, "_verify_startup_streams", crashing_stream_check)
    start_task = stubborn = None
    try:
        start_task, stubborn = await _steady_state_with_stubborn_recovery(app, monkeypatch, log)
        producer = app._recovery_task
        gate.set()                                                       # a task raises while it is alive
        done, _ = await asyncio.wait({start_task}, timeout=DEADLINE_S)
        assert done, "start() is blocked in its finalizer on a _recovery_task that ignores cancellation"
        assert not start_task.cancelled() and start_task.exception() is boom     # original exception visible
        assert not producer.done() and stubborn.swallowed >= 1
        assert app.shutdown_failure == "rest_producer_not_stopped" and app.exit_code != 0
        assert not getattr(app, "_shutdown_done", False)
        _assert_only_these_are_open(app, log, _RECOVERY_HELD)
        stubborn.release.set()
        assert (await asyncio.wait({producer}, timeout=DEADLINE_S))[0]
        await _idle_turns()
        _assert_fully_finalized_after_producer_end(app, log, _RECOVERY_HELD)
        assert app.shutdown_failure == "rest_producer_not_stopped" and app.exit_code != 0
        assert start_task.exception() is boom                            # still the original one
    finally:
        if stubborn is not None:
            stubborn.release.set()
        await _drain(app, start_task, app._recovery_task)


@pytest.mark.asyncio
async def test_a_recovery_task_that_never_ends_during_observation_has_nothing_closed_beneath_it(
        tmp_path, monkeypatch):
    """The producer stays alive for the whole observation window and is released only in cleanup."""
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log = []
    _spy_on_closes(app, monkeypatch, log)
    start_task = stubborn = None
    try:
        start_task, stubborn = await _steady_state_with_stubborn_recovery(app, monkeypatch, log)
        producer = app._recovery_task
        start_task.cancel()
        assert (await asyncio.wait({start_task}, timeout=DEADLINE_S))[0]
        pending = (await asyncio.wait({producer}, timeout=0.3))[1]       # observation window >> bound
        assert pending == {producer}, "the producer ended on its own"
        await _idle_turns()
        for _ in range(3):
            app.shutdown()
        # no reachable writer and no WAL was closed (or even attempted); no clean shutdown is reported
        _assert_only_these_are_open(app, log, _RECOVERY_HELD)
        assert not getattr(app, "_shutdown_done", False) and not getattr(app, "_quality_wal_closed", False)
        assert app._storage_held_by_rest_producers() >= {"raw_rest_writer", "raw_book_writer",
                                                         "quality_writer", "quality_wal"}
        assert app.shutdown_failure == "rest_producer_not_stopped" and app.exit_code != 0
        assert not producer.done()                                       # not falsely considered stopped
        assert not app._rest_producers_stopped()
    finally:
        if stubborn is not None:
            stubborn.release.set()                                       # no leaked task past the test
        await _drain(app, start_task, app._recovery_task)


@pytest.mark.asyncio
async def test_a_stubborn_recovery_that_fails_after_release_is_retrieved_and_never_turns_the_failure_clean(
        tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log, unhandled = [], []
    asyncio.get_running_loop().set_exception_handler(lambda loop, ctx: unhandled.append(ctx))
    _spy_on_closes(app, monkeypatch, log)
    start_task = stubborn = None
    try:
        start_task, stubborn = await _steady_state_with_stubborn_recovery(app, monkeypatch, log, "raise")
        producer = app._recovery_task
        start_task.cancel()
        assert (await asyncio.wait({start_task}, timeout=DEADLINE_S))[0] and start_task.cancelled()
        stubborn.release.set()
        assert (await asyncio.wait({producer}, timeout=DEADLINE_S))[0]
        await _idle_turns()
        _assert_fully_finalized_after_producer_end(app, log, _RECOVERY_HELD)
        assert isinstance(producer.exception(), RuntimeError)
        assert app.shutdown_failure == "rest_producer_not_stopped" and app.exit_code != 0
        producer = None
        import gc
        gc.collect()
        await _idle_turns()
        assert not [c for c in unhandled if "never retrieved" in str(c.get("message", ""))], unhandled
    finally:
        if stubborn is not None:
            stubborn.release.set()
        asyncio.get_running_loop().set_exception_handler(None)
        await _drain(app, start_task, app._recovery_task)


@pytest.mark.asyncio
async def test_a_well_behaved_recovery_task_is_still_cancelled_and_waited_for_before_finalization(
        tmp_path, monkeypatch):
    """The bound must not weaken the normal path: a cooperative recovery ends, nothing is latched."""
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log = []
    _spy_on_closes(app, monkeypatch, log)
    entered = asyncio.Event()

    async def cooperative(reason):
        if reason == "startup":
            return True
        entered.set()
        await asyncio.Event().wait()                                     # ends only by cancellation
    monkeypatch.setattr(app, "_recover_binance_book", cooperative)
    monkeypatch.setattr(run_collector, "WS_FALLBACK_SLACK_S", 0.05)
    start_task = asyncio.create_task(app.start())
    try:
        while app._oi_task is None:
            await asyncio.sleep(0)
        app._schedule_recovery("gap")
        await asyncio.wait_for(entered.wait(), DEADLINE_S)
        producer = app._recovery_task
        start_task.cancel()
        assert (await asyncio.wait({start_task}, timeout=DEADLINE_S))[0] and start_task.cancelled()
        assert producer.cancelled()
        assert getattr(app, "_shutdown_done", False) and getattr(app, "shutdown_failure", None) is None
        for name in _ALL_WRITERS:
            assert _closed(app, name) and len(_closes(log, name)) == 1
            assert app._writer_close_results.get(name) is True
        assert app.exit_code == 0
    finally:
        await _drain(app, start_task)
