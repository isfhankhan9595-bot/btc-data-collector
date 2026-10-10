"""F5 part 2B: terminal client lifecycle and exact queue accounting.

Regression tests for the two defects found by the hostile review of part 2A:

A. ``WebSocketClient.start()`` set ``running = True`` unconditionally, so a client
   that was already latched into terminal discard mode (a failed-writer latch)
   could be revived by ``start()``: it reconnected, announced itself with a
   CONNECT quality event / ``on_reconnect`` / subscribe, and consumed frames.
B. A producer parked in the full-queue wait loop re-tried ``put_nowait`` after
   its sleep WITHOUT re-checking ``running``. If shutdown/discard happened while
   it slept and the worker drained the queue and exited in the meantime, the
   producer enqueued one last item that no worker would ever take: ``join()``
   then waited out the whole drain timeout and the item was reported abandoned.

Plus the evidence that an idle secondary client does not block the corrected
terminal-shutdown contract (finding C: pre-existing, needs no code change).
"""
from __future__ import annotations

import asyncio
import json
import time

import pytest

from collector.collector import websocket_client as ws_module
from collector.collector.storage_errors import FatalStorageError
from collector.collector.websocket_client import WebSocketClient


def _fatal(stream="raw_wire") -> FatalStorageError:
    return FatalStorageError("injected fatal", stream=stream, component="parquet_writer",
                             stage="rename", durability="unpublished")


class _FakeWs:
    def __init__(self, frames, on_done=None):
        self.frames = list(frames)
        self.on_done = on_done

    def __aiter__(self):
        async def gen():
            for frame in self.frames:
                yield frame
            if self.on_done is not None:
                self.on_done()
        return gen()


class _IdleWs:
    """A connected socket on which nothing ever arrives (a quiet secondary stream)."""
    def __aiter__(self):
        async def gen():
            await asyncio.Event().wait()
            yield  # pragma: no cover - never reached
        return gen()


def _frames(n):
    return [json.dumps({"n": i}) for i in range(n)]


# ===================================================================== A. pre-start latch
def test_a_client_latched_before_start_is_not_revived_by_start(monkeypatch):
    connects, processed = [], []

    class Conn:
        async def __aenter__(self):
            connects.append(1)
            return _FakeWs(_frames(3))

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(ws_module.websockets, "connect", lambda url: Conn())

    async def on_message(data, ts, connection_id=None):
        processed.append(data)

    async def run():
        client = WebSocketClient("ws://x", on_message, on_fatal=lambda exc, origin: None)
        client.enter_discard_mode("latched before start")
        await asyncio.wait_for(client.start(), 10)
        return client

    client = asyncio.run(run())
    assert connects == [], "a terminal client must never open a connection"
    assert processed == [] and client.frames_received == 0 and client.frames_enqueued == 0
    assert client.discard_mode and client.running is False and client.connected is False
    assert client.discard_reason == "latched before start", "the original latch reason is kept"


def test_a_pre_latched_client_with_no_classifier_still_surfaces_its_fatal(monkeypatch):
    """A fatal that was latched before start() must not evaporate into a
    quietly-finished task either (default-deny, as for a latch during a run)."""
    connects = []

    class Conn:
        async def __aenter__(self):
            connects.append(1)
            return _FakeWs(_frames(1))

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(ws_module.websockets, "connect", lambda url: Conn())

    async def run():
        client = WebSocketClient("ws://x", lambda data, ts: asyncio.sleep(0))   # no on_fatal hook
        client._handle_fatal(_fatal(), "raw_frame")        # latches fatal_error + discard mode
        assert client.discard_mode
        await asyncio.wait_for(client.start(), 10)

    with pytest.raises(FatalStorageError):
        asyncio.run(run())
    assert connects == []


def test_a_latch_that_lands_while_the_connection_is_being_established_does_not_announce_the_client(monkeypatch):
    """Reconnect must not revive a terminal client either: if the latch lands
    during the handshake, the new connection is dropped before on_reconnect, the
    CONNECT quality event and the subscribe are emitted."""
    reconnects, opens, events, processed = [], [], [], []
    holder = {}

    class Conn:
        async def __aenter__(self):
            return _FakeWs(_frames(2))

        async def __aexit__(self, *a):
            return False

    async def handshake():
        holder["client"].enter_discard_mode("latched during handshake")
        return Conn()

    monkeypatch.setattr(ws_module.websockets, "connect", lambda url: handshake())

    async def on_open(send):
        opens.append(1)

    async def on_message(data, ts, connection_id=None):
        processed.append(data)

    async def run():
        client = WebSocketClient("ws://x", on_message, on_reconnect=lambda: reconnects.append(1),
                                 on_open=on_open, on_quality_event=lambda *a: events.append(a),
                                 on_fatal=lambda exc, origin: None)
        holder["client"] = client
        await asyncio.wait_for(client.start(), 10)
        return client

    client = asyncio.run(run())
    assert reconnects == [] and opens == [], "a terminal client must not subscribe or reset reconnect state"
    assert not any(e[0] == "CONNECT" for e in events), "no CONNECT quality event for a terminal client"
    assert processed == [] and client.frames_received == 0 and client.frames_enqueued == 0
    assert client.connected is False and client.discard_mode and client.running is False


def test_fresh_startup_of_an_unlatched_client_is_unchanged(monkeypatch):
    connects, reconnects, processed, events = [], [], [], []
    holder = {}

    class Conn:
        async def __aenter__(self):
            connects.append(1)
            return _FakeWs(_frames(3), on_done=lambda: holder["client"].stop())

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(ws_module.websockets, "connect", lambda url: Conn())

    async def on_message(data, ts, connection_id=None):
        processed.append(data["n"])

    async def run():
        client = WebSocketClient("ws://x", on_message, on_reconnect=lambda: reconnects.append(1),
                                 on_quality_event=lambda *a: events.append(a))
        holder["client"] = client
        await asyncio.wait_for(client.start(), 10)
        return client

    client = asyncio.run(run())
    assert connects == [1] and reconnects == [1]
    assert processed == [0, 1, 2], "every frame of an ordinary run is processed, in order"
    assert any(e[0] == "CONNECT" for e in events)
    assert not client.discard_mode and client.frames_discarded == 0
    assert client._ingest_queue._unfinished_tasks == 0


# ============================================================ B. exact queue accounting
def _gated_sleep(monkeypatch, gate: asyncio.Event):
    """Hold ONLY the producer's 10 ms full-queue poll until ``gate`` opens, so a
    test can decide exactly when the parked producer wakes up."""
    real_sleep = asyncio.sleep

    async def sleep(delay, *a, **k):
        if delay == 0.01:
            await gate.wait()
            return
        await real_sleep(delay, *a, **k)

    monkeypatch.setattr(ws_module.asyncio, "sleep", sleep)


async def _park_producer_then(client, release, gate, shutdown):
    """Fill the queue behind a blocked worker, park the producer in its full-queue
    wait, request ``shutdown``, let the worker drain and EXIT, and only then let
    the producer wake."""
    worker = asyncio.create_task(client._process_queue())
    producer = asyncio.create_task(client._consume(_FakeWs(_frames(6))))
    for _ in range(500):
        await asyncio.sleep(0)
        if client.queue_backpressure_events:
            break
    assert client.queue_backpressure_events == 1 and not producer.done(), "producer is parked on the full queue"
    shutdown()
    release.set()                                           # the worker finishes the blocked item and drains
    await asyncio.wait_for(worker, 10)                      # ...and exits: running is False and the queue is empty
    assert client._ingest_queue.empty()
    gate.set()                                              # NOW the parked producer wakes
    await asyncio.wait_for(producer, 10)
    return worker


def test_a_parked_producer_does_not_enqueue_after_discard_mode_and_worker_exit(monkeypatch):
    gate, release = asyncio.Event(), asyncio.Event()
    processed = []

    async def run():
        _gated_sleep(monkeypatch, gate)

        async def on_message(data, ts, connection_id=None):
            await release.wait()
            processed.append(data["n"])

        client = WebSocketClient("ws://x", on_message, ingest_queue_maxsize=1)
        client.running = True
        await _park_producer_then(client, release, gate, lambda: client.enter_discard_mode("test"))
        # the point: join() completes immediately; nothing is stranded behind a dead worker
        await asyncio.wait_for(client._ingest_queue.join(), 1)
        return client

    client = asyncio.run(run())
    assert client._ingest_queue._unfinished_tasks == 0, "task_done() exactly once per item that entered the queue"
    assert client._ingest_queue.empty()
    assert client.frames_abandoned_at_shutdown == 1, "the parked frame is counted as abandoned, never silently lost"
    assert client.frames_discarded == 1, "the frame that was already queued is discarded, once"
    assert processed == [], "no frame reaches on_message after the latch"
    assert client.frames_enqueued == 1, "exactly the one frame that entered the queue before the latch"


def test_a_parked_producer_does_not_enqueue_after_stop_and_worker_exit(monkeypatch):
    """The same race exists for an ordinary stop() (P0-1 shutdown), not only for
    discard mode: the fix is on the shutdown flag, so it covers both."""
    gate, release = asyncio.Event(), asyncio.Event()

    async def run():
        _gated_sleep(monkeypatch, gate)

        async def on_message(data, ts, connection_id=None):
            await release.wait()

        client = WebSocketClient("ws://x", on_message, ingest_queue_maxsize=1)
        client.running = True
        await _park_producer_then(client, release, gate, client.stop)
        await asyncio.wait_for(client._ingest_queue.join(), 1)
        return client

    client = asyncio.run(run())
    assert client._ingest_queue._unfinished_tasks == 0 and client._ingest_queue.empty()
    assert client.frames_abandoned_at_shutdown == 1


def test_start_returns_promptly_with_no_drain_timeout_after_a_latch_while_backpressured(monkeypatch):
    """End to end through start(): a latch while the producer is backpressured must
    end in a prompt, exact shutdown -- no ``ingest_queue_drain_timeout``."""
    events, release = [], asyncio.Event()
    holder = {}

    class Conn:
        async def __aenter__(self):
            return _FakeWs(_frames(50))

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(ws_module.websockets, "connect", lambda url: Conn())

    async def on_message(data, ts, connection_id=None):
        if data["n"] == 0:
            holder["client"].enter_discard_mode("latched while backpressured")
        await release.wait()

    async def run():
        client = WebSocketClient("ws://x", on_message, ingest_queue_maxsize=1,
                                 on_quality_event=lambda *a: events.append(a), on_fatal=lambda e, o: None)
        client.shutdown_drain_timeout_s = 5.0
        holder["client"] = client
        task = asyncio.create_task(client.start())
        for _ in range(500):
            await asyncio.sleep(0.01)
            if client.discard_mode:
                break
        release.set()
        started = time.monotonic()
        await asyncio.wait_for(task, 10)
        return client, time.monotonic() - started

    client, elapsed = asyncio.run(run())
    assert elapsed < 2.0, f"shutdown must not wait out the 5 s drain timeout (took {elapsed:.2f}s)"
    assert not any("ingest_queue_drain_timeout" in str(e) for e in events)
    assert client._ingest_queue._unfinished_tasks == 0


# ===================================================== C. idle secondary client (pre-existing)
def test_an_idle_secondary_client_does_not_prevent_the_terminal_shutdown_contract(monkeypatch):
    """Finding C. A client blocked in recv() on a quiet socket cannot observe the
    latch (``running`` is read only when a frame arrives) -- on the base commit as
    well. The terminal contract does not depend on it: the supervisor cancels the
    client task, the cancellation is bounded, and nothing is left running."""
    sockets = []

    class Conn:
        async def __aenter__(self):
            sockets.append(1)
            return _IdleWs()

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(ws_module.websockets, "connect", lambda url: Conn())

    async def run():
        idle = WebSocketClient("ws://idle", lambda data, ts: asyncio.sleep(0))
        task = asyncio.create_task(idle.start())
        assert await idle.wait_connected(5.0)
        idle.enter_discard_mode("terminal failure latched by another client")
        await asyncio.sleep(0.05)
        assert not task.done(), "documented limit: an idle client does not notice the latch by itself"
        started = time.monotonic()
        task.cancel()                                       # what the terminal shutdown does to every client task
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        elapsed = time.monotonic() - started
        for _ in range(30):                                 # a leaked worker would still be alive here
            worker = idle._worker_task
            if worker is None or worker.done():
                break
            await asyncio.sleep(0.1)
        return idle, elapsed

    idle, elapsed = asyncio.run(run())
    assert sockets == [1], "no reconnect: the latch holds"
    assert elapsed < 1.0, "the cancellation is prompt"
    assert idle._worker_task is None or idle._worker_task.done(), "the worker exits by itself (running is False)"
    assert idle._ingest_queue._unfinished_tasks == 0


# ============================ cross-runner: quality-channel fatal through a writer's sink
from collector.collector.failure_topology import VERDICT_DEGRADE_QUALITY  # noqa: E402
from collector.tests.test_f5_standalone_runners import (  # noqa: E402,F401
    _break, _build_runner, _cleanup, alerts)


def test_bybit_quality_fatal_through_a_raw_writers_sink_is_contained_not_a_raw_failure(tmp_path, monkeypatch):
    """The writer's sink containment must use the RUNNER's own stream table: Bybit's quality stream is
    ``bybit_quality_events``, which the USD-M default table does not know (=> TERMINATE). Escaping from a
    raw writer's rollover/migration/drop emission, it would be read as a raw failure and end the process."""
    app = _build_runner("bybit", tmp_path, monkeypatch)
    try:
        _break(monkeypatch, app.quality_writer)
        with pytest.raises(FatalStorageError):                       # the quality writer latches FAILED
            app._persist_quality_event({"stream": "x", "event_type": "ERROR", "reason": "kill-quality"})
        # exactly what a raw writer does at a rollover that migrates a legacy hourly file (unguarded emission)
        app.raw_wire_writer._emit_quality("STORAGE_MIGRATION", "legacy_hourly_file_present; starting sequenced writer at 1")
        policy = app.failure_policy
        assert policy.terminal_failure is None and app.exit_code == 0, "a quality failure is never a raw terminal"
        assert policy.quality_degraded is not None and policy.quality_degraded.verdict == VERDICT_DEGRADE_QUALITY
    finally:
        _cleanup(app)


def test_bybit_sink_still_propagates_a_typed_fatal_of_any_other_stream(tmp_path, monkeypatch):
    app = _build_runner("bybit", tmp_path, monkeypatch)
    try:
        def other(event):
            raise FatalStorageError("not quality", stream="bybit_raw_wire", component="parquet_writer",
                                    stage="rename", durability="unpublished")
        monkeypatch.setattr(app, "_persist_quality_event", other)
        with pytest.raises(FatalStorageError):
            app.raw_wire_writer._emit_quality("STORAGE_MIGRATION", "x")
    finally:
        _cleanup(app)
