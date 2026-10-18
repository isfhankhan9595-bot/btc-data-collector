"""Shutdown lifecycle of the OKX capture-only runner (``run_okx_capture.OKXCaptureApp.run``).

Question under test: can ``run()``'s ``finally`` close a writer while a task that survived cancellation
can still use it?

Everything here drives the REAL ``OKXCaptureApp.run()`` / ``close()``, the real ``OKXPublicCapture``, the
real ``WebSocketClient.start()`` tail (drain, bounded worker retirement) and REAL ``ParquetWriter``s on a
temp directory. Only the socket is fake (``websockets.connect`` stand-in) and one seam is held: the
client's ``on_message`` handler, which stands in for "a task that swallows its cancel" (a handler that
resists cancellation is the only way a production ingest worker outlives its bounded retirement). Once
released it runs the REAL ``OKXPublicCapture._on_message``, so the survivor's later writer use is the
production code path (error frame -> ``_emit`` -> ``_persist_quality_event`` -> ``quality_writer.write``).

Each writer's ``write`` / ``close`` is wrapped by a SPY that delegates to the real method and records, in
one ordered log, the attempt together with the client's live state at that instant. Nothing is inferred
from timing: shutdown is requested at a controlled point (the held worker has entered its in-flight
item) and assertions are about the ordered log.

Reachability, from the source (not from comments):
  raw_wire_writer  <- the receive loop only (``_consume`` -> ``_on_raw_frame`` -> ``RawCapture.capture_wire``)
  quality_writer   <- the receive loop / start() tail / keepalive (``on_quality_event``), the ingest worker
                      (``_on_message`` -> ``_emit``), and RawCapture's own ``quality_event_sink``
So: the raw writer may close once ingress is stopped; the quality writer only once the client is idle
(start() finished AND no worker alive). That is the invariant ``run_okx_collector`` already enforces
(``_finalize_storage``) and what is asserted here for the capture-only runner.
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pytest

from collector import run_okx_capture as rcap
from collector.collector import notifications
from collector.collector import parquet_writer as pw_module
from collector.collector.failure_topology import EXIT_FATAL_STORAGE, VERDICT_TERMINATE
from collector.collector.standalone_failure_policy import EXIT_RUN_TASK_CRASHED
from collector.collector.storage_errors import FatalStorageError
from collector.collector.websocket_client import WorkerRetirementError
from collector.tests.test_ws_shutdown_lifecycle import _connect_patch

DEADLINE_S = 10
REAL_REPLACE = os.replace
REPO_ROOT = Path(__file__).resolve().parents[2]

FRAME_ERROR = json.dumps({"event": "error", "code": "60012", "msg": "held"})
FRAME_DATA = json.dumps({"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [{"x": 1}]})
#: what the REAL ``_on_message`` emits for FRAME_ERROR once the survivor is released
SURVIVOR_EVENT = "okx_subscribe_error:60012:held"
#: what the REAL client emits about the survivor
RETIREMENT_EVENT = "ingest_worker_retirement_failed"
IN_FLIGHT_EVENT = "ingest_item_abandoned_in_flight"


@pytest.fixture(autouse=True)
def _quiet_alerts():
    notifications.set_notifier(notifications.CallableNotifier(lambda message: True))
    yield
    notifications.set_notifier(None)


@dataclass
class Event:
    kind: str                       # write_attempt | close_called | close_returned | close_raised
    writer: str
    worker_alive: bool
    client_idle: bool
    ingress_stopped: bool
    closed_before: bool             # a close of THIS writer had already been called
    detail: str = ""

    def __str__(self) -> str:
        return (f"{self.kind:14s} {self.writer:16s} worker_alive={self.worker_alive!s:5} "
                f"client_idle={self.client_idle!s:5} ingress_stopped={self.ingress_stopped!s:5} "
                f"closed_before={self.closed_before!s:5} {self.detail}")


class Scenario:
    """The real capture app, spied, with an optionally held (cancellation-resistant) ingest handler."""

    def __init__(self, tmp_path, monkeypatch, *, hold: bool):
        monkeypatch.chdir(tmp_path)
        self.app = rcap.OKXCaptureApp(["trades"], "BTC-USDT-SWAP", str(tmp_path / "data"), "ws://unused")
        self.client = self.app.capture.client
        # The bounds are the client's own attributes / the runner's own constant; shrinking them keeps the
        # REAL bounded waits in play without real-time sleeps.
        self.client.shutdown_drain_timeout_s = 0.05
        self.client.worker_retire_timeout_s = 0.05
        monkeypatch.setattr(rcap, "TERMINAL_STOP_GRACE_S", 0.05)
        self.log: list[Event] = []
        self.close_calls: dict[str, int] = {}
        self.entered = asyncio.Event()
        self.let_go = asyncio.Event()
        self.swallowed = 0
        self.worker: Optional[asyncio.Task] = None
        self._real_on_message = self.app.capture._on_message
        if hold:
            self.client.on_message = self._held_on_message
        self._spy("raw_wire_writer")
        self._spy("quality_writer")

    # -- the held handler ---------------------------------------------------------------------
    async def _held_on_message(self, parsed, local_receive_ts, connection_id=None):
        first = not self.entered.is_set()
        self.entered.set()
        if first:
            while not self.let_go.is_set():
                try:
                    await asyncio.sleep(0.005)
                except asyncio.CancelledError:
                    self.swallowed += 1                  # pathological: swallows its cancel
            # Released: the survivor does its REAL work (a quality write), then finally honours the cancel.
            await self._real_on_message(parsed, local_receive_ts, connection_id=connection_id)
            raise asyncio.CancelledError
        await self._real_on_message(parsed, local_receive_ts, connection_id=connection_id)

    def release(self) -> None:
        self.let_go.set()

    # -- spies --------------------------------------------------------------------------------
    def _event(self, kind: str, name: str, detail: str = "") -> None:
        c = self.client
        # ``close_calls`` is incremented before the close_called event, so a REPEAT close is > 1 there.
        closed_before = self.close_calls.get(name, 0) > (1 if kind == "close_called" else 0)
        self.log.append(Event(kind, name, c.worker_active, c.lifecycle_idle, c.ingress_stopped,
                              closed_before, detail))

    def _spy(self, name: str) -> None:
        writer = getattr(self.app, name)
        real_write, real_close = writer.write, writer.close

        def write(record, *a, **k):
            self._event("write_attempt", name, detail=str(record.get("reason", ""))[:60])
            return real_write(record, *a, **k)

        def close():
            self.close_calls[name] = self.close_calls.get(name, 0) + 1
            self._event("close_called", name)
            try:
                real_close()
            except BaseException as exc:
                self._event("close_raised", name, detail=type(exc).__name__)
                raise
            self._event("close_returned", name)

        writer.write, writer.close = write, close

    # -- views --------------------------------------------------------------------------------
    def dump(self) -> str:
        return "\n".join(f"  {i:2d} {e}" for i, e in enumerate(self.log))

    def events(self, kind: str, name: Optional[str] = None) -> list[Event]:
        return [e for e in self.log if e.kind == kind and (name is None or e.writer == name)]

    def index(self, kind: str, name: str) -> int:
        for i, e in enumerate(self.log):
            if e.kind == kind and e.writer == name:
                return i
        raise AssertionError(f"no {kind} for {name}\n{self.dump()}")

    def published_quality_reasons(self) -> list[str]:
        import pyarrow.parquet as pq
        reasons: list[str] = []
        for f in sorted(self.app.quality_writer.stream_dir.glob("*.seg")):
            reasons.extend(r["reason"] for r in pq.read_table(f).to_pylist())
        return reasons

    def published_raw_payloads(self) -> list[str]:
        import pyarrow.parquet as pq
        payloads: list[str] = []
        for f in sorted(self.app.raw_wire_writer.stream_dir.glob("*.seg")):
            payloads.extend(r["payload"] for r in pq.read_table(f).to_pylist())
        return payloads

    # -- invariants ---------------------------------------------------------------------------
    def assert_no_close_beneath_a_live_user(self) -> None:
        """No writer is closed while a task that can still use it is alive (see module docstring)."""
        for e in self.events("close_called"):
            if e.writer == "raw_wire_writer":
                assert e.ingress_stopped or e.client_idle, (
                    f"raw_wire_writer closed while the receive loop could still capture\n{self.dump()}")
            else:
                assert e.client_idle and not e.worker_alive, (
                    f"quality_writer closed while the client lifecycle / ingest worker was still live "
                    f"(worker_alive={e.worker_alive}, client_idle={e.client_idle})\n{self.dump()}")

    def assert_no_use_after_close(self) -> None:
        used_after = [e for e in self.events("write_attempt")
                      if self.close_calls.get(e.writer, 0) > 0 and self._closed_at(e)]
        assert not used_after, (
            "writer used after its close was called:\n" + "\n".join(f"  {e}" for e in used_after)
            + f"\n{self.dump()}")

    def _closed_at(self, event: Event) -> bool:
        """True if a close of event.writer was called BEFORE this event in the ordered log."""
        position = self.log.index(event)
        return any(e.kind == "close_called" and e.writer == event.writer for e in self.log[:position])

    def assert_each_writer_closed_at_most_once(self) -> None:
        assert all(n <= 1 for n in self.close_calls.values()), (
            f"close attempted more than once: {self.close_calls}\n{self.dump()}")


async def _wait_for(predicate, what: str) -> None:
    deadline = asyncio.get_running_loop().time() + DEADLINE_S
    while not predicate():
        assert asyncio.get_running_loop().time() < deadline, f"timed out waiting for {what}"
        await asyncio.sleep(0.005)


async def _idle_turns(n: int = 50) -> None:
    for _ in range(n):
        await asyncio.sleep(0)


async def _settle(run_task: "asyncio.Future"):
    """Wait for ``run()`` to finish; return its result, or the exception it raised (never leave it unretrieved)."""
    done, _ = await asyncio.wait({run_task}, timeout=DEADLINE_S)
    assert done, "run() never returned"
    if run_task.cancelled():
        return asyncio.CancelledError()
    return run_task.exception() or run_task.result()


async def _start_held(scn: Scenario, duration_s: float = 3600.0):
    """Start the real run() against a quiet fake socket and wait until the worker is inside its held item."""
    quiet = asyncio.Event()                                   # the socket stays open and quiet
    patch = _connect_patch([FRAME_ERROR, FRAME_DATA], quiet)
    patch.__enter__()
    run_task = asyncio.ensure_future(scn.app.run(duration_s))
    await asyncio.wait_for(scn.entered.wait(), DEADLINE_S)
    scn.worker = scn.client._worker_task
    assert scn.worker is not None and not scn.worker.done(), "precondition: the worker holds an in-flight item"
    return run_task, patch


async def _finish(scn: Scenario, run_task, patch) -> None:
    """Teardown that never leaves a live task behind: release the survivor and let everything end.
    Tolerates a setup that failed before ``run_task`` / ``patch`` existed."""
    scn.release()
    if scn.worker is not None:
        await asyncio.wait({scn.worker}, timeout=DEADLINE_S)
    if run_task is not None:
        await asyncio.wait({run_task}, timeout=DEADLINE_S)
        if not run_task.cancelled():
            run_task.exception()                              # retrieved: never an unobserved task
    await _idle_turns()
    if patch is not None:
        patch.__exit__(None, None, None)


def _latch_raw_fatal(app) -> None:
    exc = FatalStorageError("injected raw-wire failure", stream=app.raw_wire_writer.stream_name,
                            component="parquet_writer", stage="publish", durability="unpublished")
    assert app._on_fatal_storage(exc, "raw_frame") == VERDICT_TERMINATE


# =========================================================================== control: orderly shutdown
@pytest.mark.asyncio
@pytest.mark.parametrize("trigger", ["stop", "duration"])
async def test_orderly_shutdown_control(tmp_path, monkeypatch, trigger):
    """No survivor: every frame is processed, the worker retires, both writers close once, raw first."""
    scn = Scenario(tmp_path, monkeypatch, hold=False)
    quiet = asyncio.Event()
    with _connect_patch([FRAME_ERROR, FRAME_DATA], quiet):
        run_task = asyncio.ensure_future(scn.app.run(0.3 if trigger == "duration" else 3600.0))
        if trigger == "stop":
            await _wait_for(lambda: scn.app.capture.frame_counts["error"] >= 1
                            and scn.app.capture.frame_counts["data_push"] >= 1, "both frames processed")
            scn.app.capture.stop()                            # what run()'s SIGINT/SIGTERM handler calls
        result = await _settle(run_task)
    assert isinstance(result, dict), f"orderly run() must return its status, got {result!r}"
    scn.assert_no_close_beneath_a_live_user()
    scn.assert_no_use_after_close()
    scn.assert_each_writer_closed_at_most_once()
    assert scn.close_calls == {"raw_wire_writer": 1, "quality_writer": 1}, scn.dump()
    # order: raw evidence first; the quality writer last, after the raw close fully returned
    assert scn.index("close_returned", "raw_wire_writer") < scn.index("close_called", "quality_writer"), scn.dump()
    assert not scn.client.worker_active and scn.client.lifecycle_idle
    assert scn.app.failure_policy.shutdown_failure is None and scn.app.exit_code == 0
    if trigger == "stop":
        assert scn.published_quality_reasons().count(SURVIVOR_EVENT) == 1, scn.published_quality_reasons()
        assert len(scn.published_raw_payloads()) == 2, "both raw frames must be published, none lost"


@pytest.mark.asyncio
async def test_close_requested_while_the_capture_is_live_closes_nothing(tmp_path, monkeypatch):
    """``close()`` is public. Called while ingress is open and the client lifecycle is running, it must close
    NOTHING (the receive loop can still capture raw frames; the client can still emit quality events), and the
    real shutdown that follows must still finalize each writer exactly once, raw first."""
    scn = Scenario(tmp_path, monkeypatch, hold=False)
    quiet = asyncio.Event()
    with _connect_patch([FRAME_ERROR, FRAME_DATA], quiet):
        run_task = asyncio.ensure_future(scn.app.run(3600.0))
        await _wait_for(lambda: scn.app.capture.frame_counts["error"] >= 1
                        and scn.app.capture.frame_counts["data_push"] >= 1, "both frames processed")
        assert not scn.client.ingress_stopped and not scn.client.lifecycle_idle, "precondition: capture is live"
        scn.app.close()
        assert scn.close_calls == {}, f"close() closed a writer beneath a live capture\n{scn.dump()}"
        scn.app.capture.stop()
        result = await _settle(run_task)
    assert isinstance(result, dict), repr(result)
    scn.assert_no_close_beneath_a_live_user()
    scn.assert_no_use_after_close()
    assert scn.close_calls == {"raw_wire_writer": 1, "quality_writer": 1}, scn.dump()
    assert scn.index("close_returned", "raw_wire_writer") < scn.index("close_called", "quality_writer"), scn.dump()
    assert scn.app.exit_code == 0 and scn.app.failure_policy.shutdown_failure is None


@pytest.mark.asyncio
async def test_cancelled_run_without_a_survivor_closes_only_after_the_client_lifecycle_ended(tmp_path, monkeypatch):
    """No hostile handler at all: cancelling ``run()`` itself lands in ``except CancelledError`` while the capture
    task has only been asked to stop. Nothing may be closed until the client's own tail (drain, worker
    retirement) has finished, and the clean outcome must stay a clean exit."""
    scn = Scenario(tmp_path, monkeypatch, hold=False)
    quiet = asyncio.Event()
    with _connect_patch([FRAME_ERROR, FRAME_DATA], quiet):
        run_task = asyncio.ensure_future(scn.app.run(3600.0))
        await _wait_for(lambda: scn.app.capture.frame_counts["error"] >= 1
                        and scn.app.capture.frame_counts["data_push"] >= 1, "both frames processed")
        run_task.cancel()
        result = await _settle(run_task)
    assert isinstance(result, (dict, asyncio.CancelledError)), repr(result)
    await _idle_turns()
    scn.assert_no_close_beneath_a_live_user()
    scn.assert_no_use_after_close()
    assert scn.close_calls == {"raw_wire_writer": 1, "quality_writer": 1}, scn.dump()
    assert not scn.client.worker_active and scn.client.lifecycle_idle
    assert scn.app.exit_code == 0 and scn.app.failure_policy.shutdown_failure is None


# ====================================================================== survivor after a requested stop
@pytest.mark.asyncio
async def test_stop_with_a_surviving_worker_never_closes_a_writer_it_can_still_reach(tmp_path, monkeypatch):
    """Non-terminal path: ``stop()`` -> ``await task`` -> WorkerRetirementError -> finally."""
    scn = Scenario(tmp_path, monkeypatch, hold=True)
    run_task, patch = await _start_held(scn)
    try:
        scn.app.capture.stop()                                # the signal handler's action
        outcome = await _settle(run_task)
        assert isinstance(outcome, (dict, WorkerRetirementError)), repr(outcome)
        assert not scn.worker.done() and scn.swallowed >= 1, "precondition: the worker survived its bounded retirement"
        assert scn.client.worker_retirement_failed
        scn.assert_no_close_beneath_a_live_user()
    finally:
        await _finish(scn, run_task, patch)


@pytest.mark.asyncio
async def test_terminal_failure_with_a_surviving_worker_never_closes_a_writer_it_can_still_reach(tmp_path, monkeypatch):
    """Terminal path: latch -> grace wait -> cancel -> second bounded wait -> finally."""
    scn = Scenario(tmp_path, monkeypatch, hold=True)
    run_task, patch = await _start_held(scn)
    try:
        _latch_raw_fatal(scn.app)
        outcome = await _settle(run_task)
        assert isinstance(outcome, (dict, WorkerRetirementError)), repr(outcome)
        assert not scn.worker.done() and scn.swallowed >= 1, "precondition: the worker survived its bounded retirement"
        scn.assert_no_close_beneath_a_live_user()
        assert scn.app.exit_code == EXIT_FATAL_STORAGE
    finally:
        await _finish(scn, run_task, patch)


@pytest.mark.asyncio
async def test_cancelled_run_with_a_surviving_worker_never_closes_a_writer_it_can_still_reach(tmp_path, monkeypatch):
    """Cancellation of run() itself: the ``except CancelledError`` branch swallows it and falls into finally
    while the capture task has only been ASKED to stop."""
    scn = Scenario(tmp_path, monkeypatch, hold=True)
    run_task, patch = await _start_held(scn)
    try:
        run_task.cancel()
        outcome = await _settle(run_task)
        assert isinstance(outcome, (dict, WorkerRetirementError, asyncio.CancelledError)), repr(outcome)
        # Only "still alive" is a precondition here: on the unfixed runner run() returns BEFORE the client's
        # tail has even cancelled the worker (swallowed == 0), which is part of what is being demonstrated.
        assert not scn.worker.done(), "precondition: the held worker is alive when run() returns"
        scn.assert_no_close_beneath_a_live_user()
    finally:
        await _finish(scn, run_task, patch)


# ===================================================================== eventual terminal behaviour
TRIGGERS = ["stop", "terminal", "cancel"]


async def _survive_then_release(scn: Scenario, trigger: str):
    """Shutdown with a cancellation-resistant worker, wait for run() to return, THEN release the survivor and
    let it finish. Returns (run_task, patch) for ``_finish``; the worker has ended when this returns."""
    run_task, patch = await _start_held(scn)
    if trigger == "stop":
        scn.app.capture.stop()
    elif trigger == "terminal":
        _latch_raw_fatal(scn.app)
    else:
        run_task.cancel()
    await _settle(run_task)
    assert not scn.worker.done(), "precondition: the survivor is still alive when run() returned"
    scn.release()
    done, _ = await asyncio.wait({scn.worker}, timeout=DEADLINE_S)
    assert done, "the released worker must end"
    await _idle_turns()
    return run_task, patch


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger", TRIGGERS)
async def test_a_released_survivor_never_writes_to_a_closed_writer(tmp_path, monkeypatch, trigger):
    scn = Scenario(tmp_path, monkeypatch, hold=True)
    run_task = patch = None
    try:
        run_task, patch = await _survive_then_release(scn, trigger)
        scn.assert_no_use_after_close()
    finally:
        await _finish(scn, run_task, patch)


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger", TRIGGERS)
async def test_events_about_the_survivor_are_durable_exactly_once(tmp_path, monkeypatch, trigger):
    """The survivor's own quality event, the client's drain/retirement events and the in-flight abandonment
    event must each be published exactly once; raw evidence must be fully published."""
    scn = Scenario(tmp_path, monkeypatch, hold=True)
    run_task = patch = None
    try:
        run_task, patch = await _survive_then_release(scn, trigger)
        reasons = scn.published_quality_reasons()
        for reason in (SURVIVOR_EVENT, RETIREMENT_EVENT, IN_FLIGHT_EVENT):
            assert reasons.count(reason) == 1, (
                f"{reason!r} published {reasons.count(reason)}x (a lost event is only logged): {reasons}")
        assert len(scn.published_raw_payloads()) == 2, "raw evidence must be fully published"
    finally:
        await _finish(scn, run_task, patch)


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger", TRIGGERS)
async def test_each_writer_is_finalized_once_raw_first_quality_last_and_only_when_idle(
        tmp_path, monkeypatch, trigger):
    scn = Scenario(tmp_path, monkeypatch, hold=True)
    run_task = patch = None
    try:
        run_task, patch = await _survive_then_release(scn, trigger)
        scn.assert_each_writer_closed_at_most_once()
        assert scn.close_calls == {"raw_wire_writer": 1, "quality_writer": 1}, (
            f"both writers must eventually be finalized exactly once: {scn.close_calls}\n{scn.dump()}")
        assert scn.index("close_returned", "raw_wire_writer") < scn.index("close_called", "quality_writer"), scn.dump()
        scn.assert_no_close_beneath_a_live_user()
    finally:
        await _finish(scn, run_task, patch)


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger", TRIGGERS)
async def test_a_worker_that_outlived_its_retirement_is_never_a_clean_exit(tmp_path, monkeypatch, trigger):
    scn = Scenario(tmp_path, monkeypatch, hold=True)
    run_task = patch = None
    try:
        run_task, patch = await _survive_then_release(scn, trigger)
        expected = EXIT_FATAL_STORAGE if trigger == "terminal" else EXIT_RUN_TASK_CRASHED
        assert scn.app.exit_code == expected, (scn.app.exit_code, scn.app.failure_policy.shutdown_failure)
        assert scn.app.failure_policy.shutdown_failure == "ws_client_worker_not_retired"
    finally:
        await _finish(scn, run_task, patch)


# ================================================================ finalization failure and exit status
def _fail_raw_publish(monkeypatch, writer) -> None:
    prefix = str(writer.stream_dir)

    def replace(src, dst, *a, **k):
        target = str(dst)
        if target.endswith(".seg") and target.startswith(prefix):
            raise OSError(28, "No space left on device (injected)")
        return REAL_REPLACE(src, dst, *a, **k)
    monkeypatch.setattr(pw_module.os, "replace", replace)


@pytest.mark.asyncio
async def test_a_raw_writer_that_cannot_publish_at_close_is_not_a_clean_exit(tmp_path, monkeypatch):
    """The raw writer holds one captured frame unpublished (default segment size); publishing it at close
    fails for real (ENOSPC at ``os.replace``). Raw evidence was lost at finalization: exit status != 0."""
    scn = Scenario(tmp_path, monkeypatch, hold=False)
    _fail_raw_publish(monkeypatch, scn.app.raw_wire_writer)
    quiet = asyncio.Event()
    with _connect_patch([FRAME_DATA], quiet):
        run_task = asyncio.ensure_future(scn.app.run(3600.0))
        await _wait_for(lambda: scn.app.capture.frame_counts["data_push"] >= 1, "the frame to be captured")
        scn.app.capture.stop()
        result = await _settle(run_task)
    assert isinstance(result, dict), repr(result)
    assert scn.events("close_raised", "raw_wire_writer"), "precondition: the real close() failed\n" + scn.dump()
    assert scn.published_raw_payloads() == [], "precondition: the frame was never published"
    assert scn.app.exit_code != 0, "finalization lost raw evidence but the process would exit 0"
    assert scn.app.exit_code == EXIT_FATAL_STORAGE
    # the quality writer is still closed last, after the raw failure could be reported through it
    assert scn.index("close_called", "quality_writer") > scn.index("close_raised", "raw_wire_writer")
    scn.assert_each_writer_closed_at_most_once()


@pytest.mark.asyncio
async def test_an_untyped_close_failure_is_not_a_clean_exit(tmp_path, monkeypatch):
    scn = Scenario(tmp_path, monkeypatch, hold=False)
    real_close = scn.app.raw_wire_writer.close

    def failing_close():
        real_close()                                          # release the real lock / segment first
        raise OSError(5, "I/O error (injected, untyped)")
    scn.app.raw_wire_writer.close = failing_close
    # (the spy wrapped the original; re-wrap so the log still records the attempt)
    scn._spy("raw_wire_writer")
    quiet = asyncio.Event()
    with _connect_patch([FRAME_DATA], quiet):
        run_task = asyncio.ensure_future(scn.app.run(3600.0))
        await _wait_for(lambda: scn.app.capture.frame_counts["data_push"] >= 1, "the frame to be captured")
        scn.app.capture.stop()
        result = await _settle(run_task)
    assert isinstance(result, dict), repr(result)
    assert scn.events("close_raised", "raw_wire_writer"), scn.dump()
    assert scn.app.exit_code != 0, "an unfinished finalization must never exit 0"


@pytest.mark.asyncio
async def test_a_first_typed_failure_at_quality_close_degrades_the_channel_exactly_once(tmp_path, monkeypatch):
    """The quality writer's own close failing with a typed fatal is a NEW failure when the channel was healthy:
    it must be classified and latched (degraded channel, one record), not swallowed as a log line. Quality
    degradation is not a terminal verdict, so the exit status stays 0, and nothing is counted twice."""
    scn = Scenario(tmp_path, monkeypatch, hold=False)
    real_close = scn.app.quality_writer.close                 # already the spy: counts the real attempt once

    def failing_close():
        real_close()                                          # release the real lock / segment first
        raise FatalStorageError("injected quality close failure", stream=scn.app.quality_writer.stream_name,
                                component="parquet_writer", stage="publish", durability="unpublished")
    scn.app.quality_writer.close = failing_close
    quiet = asyncio.Event()
    with _connect_patch([FRAME_DATA], quiet):
        run_task = asyncio.ensure_future(scn.app.run(3600.0))
        await _wait_for(lambda: scn.app.capture.frame_counts["data_push"] >= 1, "the frame to be captured")
        scn.app.capture.stop()
        assert isinstance(await _settle(run_task), dict)
    policy = scn.app.failure_policy
    assert scn.close_calls == {"raw_wire_writer": 1, "quality_writer": 1}, scn.dump()
    assert scn.app.quality_degraded is not None and len(policy.failed_components) == 1, policy.failed_components
    assert policy.failure_repeats == {}, "a first failure is not a repeat observation"
    assert policy.terminal_failure is None and policy.shutdown_failure is None and scn.app.exit_code == 0


# ============================================================================== at-most-once finalization
@pytest.mark.asyncio
async def test_finalization_requested_again_does_not_close_a_writer_twice(tmp_path, monkeypatch):
    """``close()`` is public and a late finalizer can arrive after ``run()`` already finalized: a second
    request must return the first attempt's outcome, not call ``writer.close()`` again."""
    scn = Scenario(tmp_path, monkeypatch, hold=False)
    quiet = asyncio.Event()
    with _connect_patch([FRAME_DATA], quiet):
        run_task = asyncio.ensure_future(scn.app.run(3600.0))
        await _wait_for(lambda: scn.app.capture.frame_counts["data_push"] >= 1, "the frame to be captured")
        scn.app.capture.stop()
        assert isinstance(await _settle(run_task), dict)
    assert scn.close_calls == {"raw_wire_writer": 1, "quality_writer": 1}, scn.dump()
    scn.app.close()
    scn.app.close()
    assert scn.close_calls == {"raw_wire_writer": 1, "quality_writer": 1}, (
        f"repeat finalization re-ran writer.close(): {scn.close_calls}")


# ======================================================== process exit status of the real ``main()``
_SUBPROCESS_SCRIPT = textwrap.dedent('''
    import asyncio, json, os, sys
    from collector.collector import notifications, parquet_writer as pw, websocket_client as ws
    from collector import run_okx_capture as rcap
    notifications.set_notifier(notifications.NullNotifier())
    mode, data_dir = sys.argv[1], sys.argv[2]
    FRAME_ERROR = json.dumps({"event": "error", "code": "60012", "msg": "held"})
    FRAME_DATA = json.dumps({"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [{"x": 1}]})

    class Conn:                                   # open and quiet; only stop()/cancel ends the receive loop
        def __init__(self, frames): self.frames = frames
        async def __aenter__(self): return self
        async def __aexit__(self, *exc): return False
        async def send(self, _): return None
        def __aiter__(self):
            frames = self.frames
            async def gen():
                for f in frames: yield f
                await asyncio.Event().wait()
            return gen()

    init = rcap.OKXCaptureApp.__init__
    def patched_init(self, *a, **k):
        init(self, *a, **k)
        client = self.capture.client
        client.shutdown_drain_timeout_s = 0.05
        client.worker_retire_timeout_s = 0.05
        if mode == "survivor":
            state = {"swallowed": 0}
            async def held(parsed, ts, connection_id=None):
                while True:                       # swallows the retirement's cancel; honours the 2nd
                    try:
                        await asyncio.sleep(0.005)
                    except asyncio.CancelledError:
                        state["swallowed"] += 1
                        if state["swallowed"] >= 2:
                            raise
            client.on_message = held
    rcap.OKXCaptureApp.__init__ = patched_init

    if mode == "close_failure":
        REAL = os.replace
        def replace(src, dst, *a, **k):
            if str(dst).endswith(".seg") and "okx_raw_wire" in str(dst):
                raise OSError(28, "No space left on device (injected)")
            return REAL(src, dst, *a, **k)
        os.replace = replace

    frames = [FRAME_ERROR, FRAME_DATA] if mode == "survivor" else [FRAME_DATA]
    ws.websockets.connect = lambda url, **k: Conn(frames)
    sys.exit(rcap.main(["--duration", "0.4", "--data-dir", data_dir]))
''')


def _run_main_in_subprocess(mode: str, tmp_path):
    (tmp_path / "data").mkdir()
    env = {**os.environ, "PYTHONPATH": f"{REPO_ROOT}{os.pathsep}{REPO_ROOT / 'collector'}"}
    return subprocess.run([sys.executable, "-c", _SUBPROCESS_SCRIPT, mode, str(tmp_path / "data")],
                          cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120)


def _status_json(stdout: str) -> dict:
    return json.loads(stdout[stdout.index("{"):])


def test_main_exits_non_zero_and_reports_it_when_a_worker_outlives_its_retirement(tmp_path):
    """Real process: ``main()`` must RETURN the failure (status JSON + non-zero code), not die in a traceback
    after having closed storage beneath the worker."""
    done = _run_main_in_subprocess("survivor", tmp_path)
    assert done.returncode == EXIT_RUN_TASK_CRASHED, (done.returncode, done.stderr[-1500:])
    status = _status_json(done.stdout)
    assert status["shutdown_failure"] == "ws_client_worker_not_retired", status


def test_main_exits_with_the_fatal_storage_status_when_raw_evidence_cannot_be_published_at_close(tmp_path):
    done = _run_main_in_subprocess("close_failure", tmp_path)
    assert done.returncode == EXIT_FATAL_STORAGE, (done.returncode, done.stderr[-1500:])
    assert _status_json(done.stdout)["terminal_failure"]["stream"] == "okx_raw_wire"
