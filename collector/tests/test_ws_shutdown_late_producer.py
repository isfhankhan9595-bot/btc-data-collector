"""Late REST-producer exit and the finalization of every ``start()`` exit.

Two lifecycle gaps, each reproduced against the real ``CollectorApp.start()`` /
``_async_shutdown()`` / ``shutdown()`` with only the network edge and the websocket clients faked:

A. A REST producer that swallows its cancel stays alive through ``shutdown()`` (which therefore
   keeps the storage it can reach open and latches a non-zero status) and ends LATER. Nothing
   retried ``shutdown()``, so those writers and the quality WAL were never finalized.
B. Not every ``start()`` exit reached the finalization path: a startup-recovery exception
   propagated before ``start()``'s try/finally, and a cancelled ``start()`` could block in an
   unbounded wait on a recovery that ignores cancellation.

What "closed" means here: ``close()`` was actually CALLED and RETURNED, recorded in an ordered
event log together with the moment the producer ended, and the app's own per-writer verdict
(``_writer_close_results``) says the close succeeded. A writer that merely was never closed has
no log entry and no verdict -- the tests assert that distinction in both directions, so "never
attempted" cannot pass for "finalized". Nothing here claims a cancellation-resistant coroutine can
be stopped: a producer that never ends is only ever observed to keep its storage open and the
failure latched.
"""
import asyncio
import signal
from pathlib import Path

import pytest

import run_collector
from collector.tests.test_ws_shutdown_startup_recovery import (
    _StubbornRecovery, _app_with_stubbed_neighbours, _drain, _quiet)

_ALL_WRITERS = ("ob_writer", "raw_book_writer", "trades_writer", "raw_trades_writer", "mark_writer",
                "oi_writer", "liq_writer", "raw_wire_writer", "raw_rest_writer", "quality_writer")
#: what a live OI poller / startup recovery can still write (REST_PRODUCER_REACH) -> must stay open
_OI_HELD = {"raw_rest_writer", "oi_writer", "quality_writer"}
_RECOVERY_HELD = {"raw_rest_writer", "raw_book_writer", "quality_writer"}


class _StubbornPoller:
    """An OI poller that swallows every cancel and ends only when ``release`` is set."""

    def __init__(self, log):
        self.entered, self.cancel_swallowed, self.release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.swallowed, self._log = 0, log

    async def __call__(self):
        self.entered.set()
        while not self.release.is_set():
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.swallowed += 1
                self.cancel_swallowed.set()
        self._log.append(("producer_ended",))


def _spy_on_closes(app, monkeypatch, log):
    """Record every real close() call (and that it returned) in ``log``, in order."""
    seen = {}
    for name in _ALL_WRITERS:
        writer = getattr(app, name, None)
        if writer is None or id(writer) in seen:
            continue
        seen[id(writer)] = name
        real = writer.close

        def spy(_real=real, _name=name):
            log.append(("close_called", _name))
            result = _real()
            log.append(("close_returned", _name))
            return result
        monkeypatch.setattr(writer, "close", spy)
    wal, real_wal = app._quality_wal, app._quality_wal.close

    def wal_spy():
        log.append(("close_called", "quality_wal"))
        result = real_wal()
        log.append(("close_returned", "quality_wal"))
        return result
    monkeypatch.setattr(wal, "close", wal_spy)


def _closed(app, name):
    return bool(getattr(app, name)._closed)


def _closes(log, name):
    return [i for i, event in enumerate(log) if event == ("close_called", name)]


def _assert_only_these_are_open(app, log, still_open):
    """The reachable storage is open and was never even attempted; everything else is genuinely closed."""
    results = getattr(app, "_writer_close_results", {})
    for name in _ALL_WRITERS:
        if name in still_open:
            assert not _closed(app, name), f"{name} closed beneath a live producer"
            assert _closes(log, name) == [], f"{name}: close() was attempted beneath a live producer"
            assert name not in results, f"{name}: a close verdict exists although it was never attempted"
        else:
            assert _closed(app, name), f"{name} left open although no producer can reach it"
            assert len(_closes(log, name)) == 1 and results.get(name) is True, (
                f"{name}: expected exactly one successful close, got {_closes(log, name)} / {results.get(name)}")
    assert ("close_called", "quality_wal") not in log and not getattr(app, "_quality_wal_closed", False)


def _assert_fully_finalized_after_producer_end(app, log, held):
    """Every writer and the WAL were closed exactly once, the ``held`` ones (what the live producer
    could reach) only AFTER the producer ended, and in the required order: quality_writer publishes
    its final segment, then the WAL closes."""
    assert getattr(app, "_shutdown_done", False), "shutdown never completed after the producer ended"
    results = app._writer_close_results
    ended = log.index(("producer_ended",))
    for name in _ALL_WRITERS:
        assert _closed(app, name)
        assert len(_closes(log, name)) == 1, f"{name} closed {len(_closes(log, name))} times"
        assert results.get(name) is True, f"{name}: close did not complete successfully"
    for name in ("quality_wal", *held):
        calls = _closes(log, name)
        assert len(calls) == 1 and calls[0] > ended, f"{name} was closed before its producer ended: {log}"
    assert getattr(app, "_quality_wal_closed", False)
    assert _closes(log, "quality_writer")[0] < _closes(log, "quality_wal")[0], "the WAL must outlive quality_writer"
    assert log.index(("close_returned", "quality_writer")) < log.index(("close_called", "quality_wal"))


async def _recovered(reason):
    return True


async def _idle_turns(n=30):
    for _ in range(n):
        await asyncio.sleep(0)


# ------------------------------------------------------------------ Issue A -----------------

async def _sigterm_with_stubborn_oi_poller(app, monkeypatch, poller):
    monkeypatch.setattr(app, "_recover_binance_book", _recovered)
    monkeypatch.setattr(app, "_poll_openinterest", poller)
    monkeypatch.setattr(run_collector, "WS_FALLBACK_SLACK_S", 0.05)
    start_task = asyncio.create_task(app.start())
    await asyncio.wait_for(poller.entered.wait(), 10)
    shutdown_task = asyncio.create_task(app._async_shutdown(signal.SIGTERM))
    await asyncio.wait_for(shutdown_task, 10)
    await asyncio.wait_for(poller.cancel_swallowed.wait(), 10)
    done, _ = await asyncio.wait({start_task}, timeout=10)
    assert done, "start() never returned"
    return start_task, shutdown_task


@pytest.mark.asyncio
async def test_an_oi_poller_that_ends_after_the_shutdown_check_gets_every_remaining_writer_finalized(
        tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log = []
    _spy_on_closes(app, monkeypatch, log)
    poller = _StubbornPoller(log)
    start_task = shutdown_task = None
    try:
        start_task, shutdown_task = await _sigterm_with_stubborn_oi_poller(app, monkeypatch, poller)
        producer = app._oi_task
        # --- while the producer is alive: failure latched, reachable storage preserved, nothing finalized
        assert not producer.done() and poller.swallowed >= 1
        assert app.shutdown_failure == "rest_producer_not_stopped" and app.exit_code != 0
        assert not getattr(app, "_shutdown_done", False)
        _assert_only_these_are_open(app, log, _OI_HELD)
        # --- the producer ends; the retry of shutdown() must finalize the rest, in order
        poller.release.set()
        done, _ = await asyncio.wait({producer}, timeout=10)
        assert done
        await _idle_turns()
        _assert_fully_finalized_after_producer_end(app, log, _OI_HELD)
        # a late close is not a clean shutdown: the latched status survives it
        assert app.shutdown_failure == "rest_producer_not_stopped" and app.exit_code != 0
    finally:
        poller.release.set()
        await _drain(app, start_task, shutdown_task)


@pytest.mark.asyncio
async def test_a_producer_that_never_ends_keeps_its_storage_open_and_never_reports_a_clean_shutdown(
        tmp_path, monkeypatch):
    """The producer stays alive for the whole bounded window; it is released only afterwards so the
    test itself can finish. Nothing is closed beneath it, not even by an explicit later shutdown()."""
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log = []
    _spy_on_closes(app, monkeypatch, log)
    poller = _StubbornPoller(log)
    start_task = shutdown_task = None
    try:
        start_task, shutdown_task = await _sigterm_with_stubborn_oi_poller(app, monkeypatch, poller)
        producer = app._oi_task
        pending = (await asyncio.wait({producer}, timeout=0.3))[1]          # the bounded window
        assert pending == {producer}, "the producer ended on its own"
        await _idle_turns()
        for _ in range(2):                                                   # an explicit retry is a no-op
            app.shutdown()
            assert not getattr(app, "_shutdown_done", False)
            _assert_only_these_are_open(app, log, _OI_HELD)
        assert app.shutdown_failure == "rest_producer_not_stopped"
        assert app.exit_code == run_collector.EXIT_RUN_TASK_CRASHED != 0
        assert not producer.done()                                           # still alive: not stoppable
    finally:
        poller.release.set()
        await _drain(app, start_task, shutdown_task)


def test_the_deployment_doc_states_the_process_level_limitation_accurately():
    text = (Path(__file__).resolve().parents[2] / "docs" / "DEPLOYMENT.md").read_text()
    start = text.index("### A REST producer that ignores cancellation")
    section = text[start:text.index("\n### ", start + 5)]
    flat = " ".join(section.split())
    assert "cannot be stopped by the application" in flat
    assert "process-level termination" in flat and "TimeoutStopSec" in flat and "SIGKILL" in flat
    assert "reports the kill, not the latched status" in flat
    assert "Nothing in the code guarantees that shutdown completes within 60 s" in flat
    assert "retries `shutdown()`" in flat or "retry of `shutdown()`" in flat      # the late retry is documented
    assert "observed, not fixed here" not in flat, "stale: the OI-poller gap is fixed by the late retry"
    flat_all = " ".join(text.split())
    assert "**7 s** (`FINALIZATION_RESERVE_S`)" in flat_all           # the reserve itself is unchanged ...
    assert "is NOT bounded by code and has not been measured" in flat_all, (
        "the seven-second finalization reserve must stay documented as unmeasured")


# ------------------------------------------------------------------ Issue B -----------------

@pytest.mark.asyncio
async def test_a_startup_recovery_exception_runs_the_finalization_path_and_stays_visible(tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log, spawned = [], []
    _spy_on_closes(app, monkeypatch, log)
    boom = ValueError("snapshot parse failed")

    async def failing(reason):
        raise boom

    async def poll():
        spawned.append("oi_poll")
        await _quiet()
    monkeypatch.setattr(app, "_recover_binance_book", failing)
    monkeypatch.setattr(app, "_poll_openinterest", poll)
    monkeypatch.setattr(app.health_monitor, "start", lambda: spawned.append("health") or _quiet())
    start_task = asyncio.create_task(app.start())
    try:
        done, _ = await asyncio.wait({start_task}, timeout=10)
        assert done
        assert start_task.exception() is boom                      # the failure is visible, not swallowed
        # ... and the common finalization path ran: nothing was merely left open
        assert getattr(app, "_shutdown_done", False), "start() exited without finalizing"
        for name in _ALL_WRITERS:
            assert _closed(app, name) and len(_closes(log, name)) == 1
            assert app._writer_close_results.get(name) is True
        assert getattr(app, "_quality_wal_closed", False) and len(_closes(log, "quality_wal")) == 1
        assert app.running is False and all(t.done() for t in app.tasks)
        assert app._quality_task.done() and spawned == [] and app._oi_task is None
        assert getattr(app, "shutdown_failure", None) is None      # a clean finalization is not a shutdown failure
    finally:
        await _drain(app, start_task)


@pytest.mark.asyncio
async def test_cancelling_start_while_the_recovery_ignores_cancellation_reaches_finalization_without_closing_beneath_it(
        tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log, spawned = [], []
    _spy_on_closes(app, monkeypatch, log)
    stubborn = _StubbornRecovery()

    async def poll():
        spawned.append("oi_poll")
        await _quiet()
    monkeypatch.setattr(app, "_recover_binance_book", stubborn)
    monkeypatch.setattr(app, "_poll_openinterest", poll)
    monkeypatch.setattr(run_collector, "WS_FALLBACK_SLACK_S", 0.05)
    start_task = asyncio.create_task(app.start())
    try:
        await asyncio.wait_for(stubborn.entered.wait(), 10)
        start_task.cancel()                                        # the caller cancels start()
        done, _ = await asyncio.wait({start_task}, timeout=10)
        assert done, "start() is blocked waiting for a producer that ignores cancellation"
        # --- states BEFORE the producer is released ------------------------------------------
        assert start_task.cancelled(), "caller cancellation must propagate, not be swallowed"
        producer = app._startup_recovery_task
        assert not producer.done() and stubborn.swallowed >= 1
        assert app.shutdown_failure == "rest_producer_not_stopped" and app.exit_code != 0
        assert not getattr(app, "_shutdown_done", False)
        _assert_only_these_are_open(app, log, _RECOVERY_HELD)
        assert spawned == [] and app._oi_task is None and all(t.done() for t in app.tasks)
        # --- the producer ends: the retry finalizes the rest, in order -------------------------
        log.append(("producer_ended",))
        stubborn.release.set()
        assert (await asyncio.wait({producer}, timeout=10))[0]
        await _idle_turns()
        _assert_fully_finalized_after_producer_end(app, log, _RECOVERY_HELD)
        assert app.shutdown_failure == "rest_producer_not_stopped" and app.exit_code != 0
    finally:
        stubborn.release.set()
        await _drain(app, start_task)


@pytest.mark.asyncio
async def test_start_cancelled_by_its_caller_is_not_swallowed_in_the_steady_state_either(tmp_path, monkeypatch):
    """After startup, a cancelled start() finalizes and then re-raises; children cancelled by the
    controlled shutdown (the normal SIGTERM path) still make start() return normally."""
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log = []
    _spy_on_closes(app, monkeypatch, log)
    monkeypatch.setattr(app, "_recover_binance_book", _recovered)
    start_task = asyncio.create_task(app.start())
    try:
        while app._oi_task is None:                                # start() reached the gather
            await asyncio.sleep(0)
        start_task.cancel()
        done, _ = await asyncio.wait({start_task}, timeout=10)
        assert done and start_task.cancelled()
        assert getattr(app, "_shutdown_done", False)
        for name in _ALL_WRITERS:
            assert _closed(app, name) and app._writer_close_results.get(name) is True
    finally:
        await _drain(app, start_task)
