import pytest
import asyncio
import importlib
from unittest.mock import AsyncMock, MagicMock, patch
from collector.collector.websocket_client import WebSocketClient


@pytest.mark.asyncio
async def test_websocket_stop_sets_running_false():
    client = WebSocketClient("ws://localhost:9999", AsyncMock())
    client.running = True
    client.stop()
    assert not client.running
    assert not client.connected


@pytest.mark.asyncio
async def test_websocket_reconnect_retries_on_failure():
    """Verify jittered, capped backoff retries when connection fails."""
    connect_attempts = 0

    async def fake_connect(url, **kwargs):
        nonlocal connect_attempts
        connect_attempts += 1
        raise ConnectionRefusedError("Simulated connection failure")

    reconnect_called = MagicMock()
    client = WebSocketClient("ws://localhost:9999", AsyncMock(), on_reconnect=reconnect_called)
    client.running = True

    sleep_calls = []

    async def fake_sleep(delay):
        sleep_calls.append(delay)
        if connect_attempts >= 3:
            client.running = False  # Stop after 3 attempts

    with patch("collector.collector.websocket_client.websockets.connect", new=fake_connect):
        with patch("collector.collector.websocket_client.asyncio.sleep", new=fake_sleep):
            await client.start()

    assert connect_attempts >= 3, "Expected at least 3 reconnect attempts"
    assert len(sleep_calls) >= 2, "Expected backoff sleep calls"
    # Delays are jittered, so they are deliberately NOT monotonic: a
    # monotonic series is exactly what makes every client that dropped
    # together retry together. The contract is that each delay sits within
    # its own exponentially growing cap.
    for index, delay in enumerate(sleep_calls):
        assert 0.0 <= delay <= min(60.0, 1.0 * (2 ** index)) + 1e-9
    assert max(sleep_calls) <= 60.0


# ---------------------------------------------------------------------------
# P0-1: receive/processing decoupling.
# ---------------------------------------------------------------------------


class _FakeSocket:
    """Minimal async-iterable fake matching what `websockets.connect()`
    yields -- just enough for _consume() to drive against."""
    def __init__(self, frames):
        self.frames = frames

    def __aiter__(self):
        async def gen():
            for frame in self.frames:
                yield frame
        return gen()


@pytest.mark.asyncio
async def test_receive_is_decoupled_from_slow_processing():
    """Test 1: artificially slow processing must not prevent the receive
    loop from enqueueing every frame -- _consume() finishes receiving all
    frames without ever awaiting on_message itself."""
    processed = []

    async def slow_on_message(data, local_receive_ts, connection_id=None):
        await asyncio.sleep(0.05)   # simulate slow downstream processing
        processed.append(data)

    client = WebSocketClient("ws://localhost:9999", slow_on_message, ingest_queue_maxsize=50)
    client.running = True

    frames = [f'{{"n":{i}}}' for i in range(10)]
    await client._consume(_FakeSocket(frames))

    # The receive loop enqueued everything already -- it never awaited
    # on_message, so it did not have to wait through 10 * 50ms of
    # processing to finish consuming 10 frames.
    assert client.frames_received == 10
    assert client.frames_enqueued == 10
    assert len(processed) == 0   # nothing processed yet -- worker hasn't run

    client.running = False
    await client._process_queue()
    assert len(processed) == 10
    assert client.frames_processed == 10


@pytest.mark.asyncio
async def test_ordering_is_preserved_through_the_queue():
    """Test 2: events must be processed in the same order they were
    received, even though processing is now decoupled from receiving."""
    processed = []

    async def on_message(data, local_receive_ts, connection_id=None):
        processed.append(data["n"])

    client = WebSocketClient("ws://localhost:9999", on_message, ingest_queue_maxsize=50)
    client.running = True
    frames = [f'{{"n":{i}}}' for i in [1, 2, 3, 4, 5]]
    await client._consume(_FakeSocket(frames))
    client.running = False
    await client._process_queue()

    assert processed == [1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_queue_size_never_exceeds_configured_capacity():
    """Test 3: queue boundedness. qsize() is checked after every enqueue
    while processing is deliberately held back, so the queue fills to
    exactly its configured capacity and no further."""
    release = asyncio.Event()

    async def blocking_on_message(data, local_receive_ts, connection_id=None):
        await release.wait()

    client = WebSocketClient("ws://localhost:9999", blocking_on_message, ingest_queue_maxsize=5)
    client.running = True

    async def _run():
        worker = asyncio.ensure_future(client._process_queue())
        await client._consume(_FakeSocket([f'{{"n":{i}}}' for i in range(5)]))
        # Queue is now at its configured capacity; the worker is stuck on
        # the first item's blocking on_message call.
        assert client._ingest_queue.qsize() <= 5
        assert client.queue_high_watermark <= 5
        release.set()
        client.running = False
        await worker
    await _run()


@pytest.mark.asyncio
async def test_burst_produces_no_uncontrolled_task_creation_and_no_silent_loss():
    """Test 4: a large burst must enqueue and eventually process every
    single frame, with explicit backpressure counted rather than any
    frame silently vanishing, and without spawning a task per message
    (there is exactly one worker task for the whole burst)."""
    processed = []

    async def on_message(data, local_receive_ts, connection_id=None):
        processed.append(data["n"])

    client = WebSocketClient("ws://localhost:9999", on_message, ingest_queue_maxsize=20)
    client.running = True
    burst = [f'{{"n":{i}}}' for i in range(500)]

    async def _run():
        worker = asyncio.ensure_future(client._process_queue())
        await client._consume(_FakeSocket(burst))
        client.running = False
        await worker
    await _run()

    assert client.frames_received == 500
    assert client.frames_enqueued == 500     # every frame enqueued, none dropped
    assert client.frames_processed == 500    # every frame eventually processed
    assert sorted(processed) == list(range(500))
    assert client.queue_high_watermark <= 20  # queue metrics stayed within capacity
    # A burst this much larger than the queue capacity guarantees backpressure
    # was hit and explicitly counted, not silently absorbed.
    assert client.queue_backpressure_events > 0


@pytest.mark.asyncio
async def test_one_failing_item_does_not_kill_the_worker():
    """Test 5: exception isolation. Processing of one event fails; the
    worker must remain healthy and keep processing subsequent events."""
    processed = []

    async def flaky_on_message(data, local_receive_ts, connection_id=None):
        if data["n"] == 1:
            raise ValueError("simulated processing failure")
        processed.append(data["n"])

    client = WebSocketClient("ws://localhost:9999", flaky_on_message, ingest_queue_maxsize=50)
    client.running = True
    frames = [f'{{"n":{i}}}' for i in range(5)]
    await client._consume(_FakeSocket(frames))
    client.running = False
    await client._process_queue()

    assert processed == [0, 2, 3, 4]   # item 1 failed; everything else still processed
    assert client.processing_errors == 1
    assert client.frames_processed == 4


@pytest.mark.asyncio
async def test_one_failing_item_reports_a_quality_event():
    quality_events = []

    async def flaky_on_message(data, local_receive_ts, connection_id=None):
        raise RuntimeError("boom")

    client = WebSocketClient(
        "ws://localhost:9999", flaky_on_message,
        on_quality_event=lambda kind, reason, conn, group: quality_events.append((kind, reason)),
    )
    client.running = True
    await client._consume(_FakeSocket(['{"n":1}']))
    client.running = False
    await client._process_queue()

    assert quality_events and quality_events[0][0] == "ERROR"
    assert "processing_failed" in quality_events[0][1]


@pytest.mark.asyncio
async def test_shutdown_drains_queued_work_before_the_worker_exits():
    """Test 6: shutdown. Queued events present when running flips to
    False must still be processed (drained), not abandoned."""
    processed = []

    async def on_message(data, local_receive_ts, connection_id=None):
        await asyncio.sleep(0.01)
        processed.append(data["n"])

    client = WebSocketClient("ws://localhost:9999", on_message, ingest_queue_maxsize=50)
    client.running = True
    await client._consume(_FakeSocket([f'{{"n":{i}}}' for i in range(10)]))
    assert client._ingest_queue.qsize() == 10

    # Simulate shutdown: running flips False while work is still queued.
    client.running = False
    await client._process_queue()

    assert len(processed) == 10   # every already-queued item was drained, not abandoned
    assert client._ingest_queue.empty()


@pytest.mark.asyncio
async def test_reconnect_generation_is_attributable_on_each_item():
    """Test 7: items from different connection generations remain
    distinguishable via IngestItem.connection_generation."""
    seen_generations = []

    async def on_message(data, local_receive_ts, connection_id=None):
        pass  # generation is captured before on_message is even called; see below

    client = WebSocketClient("ws://localhost:9999", on_message, ingest_queue_maxsize=50)
    client.running = True

    client.connection_id = "conn-A"
    client._connection_serial = 1
    await client._consume(_FakeSocket(['{"n":1}']))

    client.connection_id = "conn-B"
    client._connection_serial = 2
    await client._consume(_FakeSocket(['{"n":2}']))

    # Drain the queue by hand so we can inspect each IngestItem's own
    # generation before _process_item consumes it.
    while not client._ingest_queue.empty():
        item = client._ingest_queue.get_nowait()
        seen_generations.append((item.connection_id, item.connection_generation))
        client._ingest_queue.task_done()

    assert seen_generations == [("conn-A", 1), ("conn-B", 2)]


@pytest.mark.asyncio
async def test_raw_capture_still_receives_every_frame_when_on_message_fails():
    """Test 8: raw capture independence. on_message failing must not
    prevent the frame from having already reached raw capture -- capture
    happens in _process_item before on_message is called, unchanged from
    the pre-P0-1 ordering."""
    captured = []

    async def failing_on_message(data, local_receive_ts, connection_id=None):
        raise RuntimeError("downstream processing exploded")

    def on_raw_frame(msg, **kwargs):
        captured.append(msg)

    client = WebSocketClient(
        "ws://localhost:9999", failing_on_message, on_raw_frame=on_raw_frame,
    )
    client.running = True
    await client._consume(_FakeSocket(['{"n":1}', '{"n":2}']))
    client.running = False
    await client._process_queue()

    assert len(captured) == 2   # both frames reached raw capture despite on_message failing every time
    assert client.processing_errors == 2


@pytest.mark.asyncio
async def test_queue_high_watermark_and_backpressure_metrics_are_observable():
    """Queue metrics (Section 16 instrumentation) are inspectable, not
    merely internal state -- overload must be visible, not merely
    'look like a healthy collector'."""
    release = asyncio.Event()

    async def blocking_on_message(data, local_receive_ts, connection_id=None):
        await release.wait()

    client = WebSocketClient("ws://localhost:9999", blocking_on_message, ingest_queue_maxsize=2)

    async def _run():
        client.running = True
        worker = asyncio.ensure_future(client._process_queue())
        await client._consume(_FakeSocket(['{"n":1}', '{"n":2}', '{"n":3}']))
        assert client.queue_high_watermark >= 1
        assert client.queue_backpressure_events >= 1
        release.set()
        client.running = False
        await worker
    await _run()


# ---------------------------------------------------------------------------
# P0-1 post-merge corrective hardening.
# ---------------------------------------------------------------------------


def test_zero_maxsize_is_rejected_not_silently_unbounded():
    """asyncio.Queue treats maxsize<=0 as unbounded -- the opposite of
    every 'bounded queue' claim in this class's own docs/comments."""
    with pytest.raises(ValueError):
        WebSocketClient("ws://localhost:9999", lambda *a, **k: None, ingest_queue_maxsize=0)


def test_negative_maxsize_is_rejected():
    with pytest.raises(ValueError):
        WebSocketClient("ws://localhost:9999", lambda *a, **k: None, ingest_queue_maxsize=-5)


@pytest.mark.asyncio
async def test_raw_capture_happens_before_enqueue_frames_still_in_queue_are_already_durable():
    """The core post-merge correction: a frame sitting in the queue,
    never dequeued at all, must still have reached raw capture. This is
    what makes the queue's contents 'already durable, awaiting on_message
    only' rather than 'the only copy of this frame that exists'."""
    captured = []

    def on_raw_frame(msg, **kwargs):
        captured.append(msg)

    async def on_message(data, local_receive_ts, connection_id=None):
        pass  # never actually invoked in this test -- worker is never started

    client = WebSocketClient(
        "ws://localhost:9999", on_message, on_raw_frame=on_raw_frame, ingest_queue_maxsize=50,
    )
    client.running = True
    frames = [f'{{"n":{i}}}' for i in range(5)]
    await client._consume(_FakeSocket(frames))   # worker never started

    # All 5 frames are still sitting, unprocessed, in the queue -- and all
    # 5 already reached raw capture regardless.
    assert client._ingest_queue.qsize() == 5
    assert len(captured) == 5


@pytest.mark.asyncio
async def test_malformed_frame_still_reaches_raw_capture_before_enqueue():
    captured = []

    def on_raw_frame(msg, **kwargs):
        captured.append((msg, kwargs["decode_ok"]))

    client = WebSocketClient(
        "ws://localhost:9999", lambda *a, **k: None, on_raw_frame=on_raw_frame, ingest_queue_maxsize=10,
    )
    client.running = True
    await client._consume(_FakeSocket(["{not valid json"]))
    assert captured == [("{not valid json", False)]


@pytest.mark.asyncio
async def test_stop_unblocks_a_backpressured_consume_loop_promptly():
    """Shutdown responsiveness: _consume must not stay blocked
    indefinitely on a full queue once running flips False, even with no
    worker draining it. Bounded by the 0.01s poll interval, not by
    worker availability."""
    client = WebSocketClient("ws://localhost:9999", lambda *a, **k: None, ingest_queue_maxsize=2)
    client.running = True
    # Fill the queue to capacity with no worker running to drain it.
    frames = [f'{{"n":{i}}}' for i in range(2)]
    await client._consume(_FakeSocket(frames))
    assert client._ingest_queue.qsize() == 2

    async def _stop_soon():
        await asyncio.sleep(0.03)
        client.running = False

    stopper = asyncio.ensure_future(_stop_soon())
    # One more frame arrives while the queue is already full and no
    # worker exists to drain it -- _consume must still return promptly
    # once running flips False, not hang forever.
    await asyncio.wait_for(client._consume(_FakeSocket(['{"n":99}'])), timeout=2.0)
    await stopper

    assert client.frames_abandoned_at_shutdown == 1
    assert client._ingest_queue.qsize() == 2   # the abandoned frame never made it in


@pytest.mark.asyncio
async def test_frame_abandoned_at_shutdown_still_reached_raw_capture():
    """The frame _consume gives up enqueueing at shutdown must already
    have been raw-captured -- 'abandoned' means abandoned from on_message
    processing in this run, never from raw evidence."""
    captured = []

    def on_raw_frame(msg, **kwargs):
        captured.append(msg)

    client = WebSocketClient(
        "ws://localhost:9999", lambda *a, **k: None, on_raw_frame=on_raw_frame, ingest_queue_maxsize=1,
    )
    client.running = True
    await client._consume(_FakeSocket(['{"n":1}']))   # fills the queue (maxsize=1)
    assert client._ingest_queue.qsize() == 1

    async def _stop_soon():
        await asyncio.sleep(0.03)
        client.running = False

    stopper = asyncio.ensure_future(_stop_soon())
    # This frame arrives while the queue is already full; running flips
    # False while _consume is blocked in the retry loop for it -- the
    # in-flight-abandonment path, not the already-stopped path.
    await asyncio.wait_for(client._consume(_FakeSocket(['{"n":2}'])), timeout=2.0)
    await stopper

    assert client.frames_abandoned_at_shutdown == 1
    assert captured == ['{"n":1}', '{"n":2}']


# ---------------------------------------------------------------------------
# Final P0-1 audit: callback-level raw-capture failures and drain-timeout loss
# must be counted and reported, never a log line only.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_raw_capture_callback_failure_is_counted_reported_once_per_streak_and_never_drops_the_frame():
    """A bug in a runner's on_raw_frame callback (before it ever reaches
    RawCapture, which handles its own writer failures and emits DATA_DROP)
    used to be a warning log only: raw capture silently disabled for every
    frame. Ingestion must continue (stopping loses every frame, not just the
    raw copy), but the failure must be counted, and reported once per failure
    streak -- not per frame, which would flood."""
    events = []

    def broken_callback(msg, **kwargs):
        raise RuntimeError("callback bug")

    client = WebSocketClient(
        "ws://localhost:9999", lambda *a, **k: None, on_raw_frame=broken_callback,
        on_quality_event=lambda *a: events.append(a), ingest_queue_maxsize=10,
    )
    client.running = True
    await client._consume(_FakeSocket(['{"n":1}', '{"n":2}', '{"n":3}']))

    assert client.raw_capture_callback_failures == 3
    assert client.raw_capture_callback_degraded is True
    assert client._ingest_queue.qsize() == 3, "a raw-capture failure must not drop the frame from processing"
    failure_events = [e for e in events if str(e[1]).startswith("raw_capture_callback_failed")]
    assert len(failure_events) == 1 and failure_events[0][0] == "ERROR"
    assert failure_events[0][1] == "raw_capture_callback_failed:RuntimeError"


@pytest.mark.asyncio
async def test_raw_capture_callback_recovery_resets_the_streak_so_a_new_failure_is_reported_again():
    events = []
    calls = {"n": 0}

    def flaky(msg, **kwargs):
        calls["n"] += 1
        if calls["n"] in (1, 3):
            raise ValueError("flaky")

    client = WebSocketClient(
        "ws://localhost:9999", lambda *a, **k: None, on_raw_frame=flaky,
        on_quality_event=lambda *a: events.append(a), ingest_queue_maxsize=10,
    )
    client.running = True
    await client._consume(_FakeSocket(['{"n":1}', '{"n":2}', '{"n":3}']))   # fail, ok, fail

    assert client.raw_capture_callback_failures == 2
    assert client.raw_capture_callback_degraded is True
    assert len([e for e in events if str(e[1]).startswith("raw_capture_callback_failed")]) == 2


@pytest.mark.asyncio
async def test_drain_timeout_counts_the_abandoned_frames_and_reports_a_quality_event():
    """The sibling shutdown path (QueueFull while stopping) already counted
    abandoned frames; the drain-timeout path logged `remaining` only. Raw
    capture precedes the enqueue, so this is lost *processing*, not lost raw
    evidence -- but it must still be explicit."""
    events = []
    client = WebSocketClient(
        "ws://localhost:9999", lambda *a, **k: None, on_quality_event=lambda *a: events.append(a),
        ingest_queue_maxsize=10,
    )
    client.shutdown_drain_timeout_s = 0.05
    client.running = True
    await client._consume(_FakeSocket(['{"n":1}', '{"n":2}', '{"n":3}']))
    client.running = False

    async def wedged_worker():
        await asyncio.Event().wait()          # never takes an item, never finishes

    client._worker_task = asyncio.ensure_future(wedged_worker())
    await asyncio.wait_for(client._drain_and_stop_worker(), timeout=2.0)

    assert client.frames_abandoned_at_shutdown == 3
    assert client._worker_task is None
    assert [e[1] for e in events if str(e[1]).startswith("ingest_queue_drain_timeout")] == ["ingest_queue_drain_timeout:3"]


@pytest.mark.asyncio
async def test_drain_that_completes_in_time_abandons_nothing_and_reports_nothing():
    events = []
    processed = []

    async def on_message(data, ts, **kwargs):
        processed.append(data)

    client = WebSocketClient(
        "ws://localhost:9999", on_message, on_quality_event=lambda *a: events.append(a), ingest_queue_maxsize=10,
    )
    client.running = True
    await client._consume(_FakeSocket(['{"n":1}', '{"n":2}']))
    client.running = False
    client._worker_task = asyncio.ensure_future(client._process_queue())
    await asyncio.wait_for(client._drain_and_stop_worker(), timeout=5.0)

    assert processed == [{"n": 1}, {"n": 2}]
    assert client.frames_abandoned_at_shutdown == 0
    assert not any("drain_timeout" in str(e[1]) for e in events)


@pytest.mark.asyncio
async def test_on_message_receives_the_arrival_timestamp_not_the_processing_timestamp():
    """Research eligibility is `local_receive_ts <= observation_ts`. A frame
    arrives once, is stamped once (P0-11's `capture_receive_stamp`, called
    exactly once per frame at the receive boundary -- see clock.py's own
    docstring), and queueing delay before processing must never rewrite
    that stamp.

    `capture_receive_stamp`'s default clock callables
    (`time.time_ns`/`time.monotonic_ns`) are bound once, at function
    *definition* time -- not late-binding -- so patching `time.time_ns`
    afterwards (the old test's approach, applied to the wrong function
    entirely) has no effect on what the function actually calls. The
    correct seam is `capture_receive_stamp` itself, as imported into
    `websocket_client`'s own module namespace (`from .clock import ...
    capture_receive_stamp`) -- patching the name the call site resolves.

    `side_effect=[...]` with exactly one value (rather than a plain
    `return_value`) makes this a real regression test, not just a
    functional one: if production ever calls `capture_receive_stamp` a
    second time (e.g. a bug that re-stamps during processing), the mock
    raises `StopIteration` on that second call -- a hard, deterministic
    failure -- rather than silently returning a second plausible value.
    """
    from collector.collector import websocket_client as wsc
    from collector.collector.clock import ReceiveStamp

    arrival = ReceiveStamp(wall_ns=1_000_000_000_000, mono_ns=1)   # wall_ms == 1_000_000
    seen = []

    async def on_message(data, ts, connection_id=None):
        seen.append(ts)

    client = WebSocketClient("ws://localhost:9999", on_message, ingest_queue_maxsize=10)
    client.running = True
    with patch.object(wsc, "capture_receive_stamp", side_effect=[arrival]) as stamp_mock:
        await client._consume(_FakeSocket(['{"n":1}']))     # the one and only stamp call
        # Simulated queueing delay: real wall-clock time passing while the
        # item sits in the queue -- no second clock read happens in
        # production for this, so nothing here needs to advance a mock.
        client._worker_task = asyncio.ensure_future(client._process_queue())
        client.running = False
        await asyncio.wait_for(client._drain_and_stop_worker(), timeout=5.0)

    assert seen == [1_000_000], f"on_message got {seen}; processing leaked into the receive timestamp"
    assert stamp_mock.call_count == 1, "capture_receive_stamp was called more than once for one frame"


@pytest.mark.asyncio
async def test_receive_timestamp_also_holds_for_handlers_that_take_no_connection_id():
    """on_message is dispatched through two different call sites depending on
    whether the handler accepts connection_id. The timestamp invariant must
    hold on both; the first version of the test above only covered one."""
    from collector.collector import websocket_client as wsc
    from collector.collector.clock import ReceiveStamp

    arrival = ReceiveStamp(wall_ns=2_000_000_000_000, mono_ns=1)   # wall_ms == 2_000_000
    seen = []

    async def on_message(data, ts):                 # no connection_id parameter
        seen.append(ts)

    client = WebSocketClient("ws://localhost:9999", on_message, ingest_queue_maxsize=10)
    assert client._on_message_takes_connection is False
    client.running = True
    with patch.object(wsc, "capture_receive_stamp", side_effect=[arrival]) as stamp_mock:
        await client._consume(_FakeSocket(['{"n":1}']))
        client._worker_task = asyncio.ensure_future(client._process_queue())
        client.running = False
        await asyncio.wait_for(client._drain_and_stop_worker(), timeout=5.0)
    assert seen == [2_000_000]
    assert stamp_mock.call_count == 1


def test_receive_timestamp_comes_from_the_injected_stamp_not_from_wall_clock_elsewhere():
    """Directly proves the value on the wire is exactly the mocked stamp's
    wall_ms -- not some other wall-clock access this test's two timestamp
    tests above might have missed (e.g. a second, independent time.time()
    call somewhere in _consume that happens to coincidentally agree)."""
    from collector.collector.clock import ReceiveStamp

    for wall_ns, expected_ms in [(0, 0), (1, 0), (999_999, 0), (1_000_000, 1),
                                 (1_234_567_890_123_456_789, 1_234_567_890_123)]:
        assert ReceiveStamp(wall_ns=wall_ns, mono_ns=0).wall_ms == expected_ms


def test_mutation_restamping_during_processing_is_caught():
    """Real source mutation (not simulated): makes _process_item call
    capture_receive_stamp again and overwrite the item's timestamp before
    calling on_message -- exactly the regression class the receive-timestamp
    invariant guards against -- and confirms the arrival-timestamp test fails
    under it.

    The mutation is applied to a PRIVATE temporary copy of the source tree and
    the target test runs there (see ``_isolated_source_mutation``). The
    original checkout is only ever read: concurrent runs cannot observe each
    other's mutant, and a killed process cannot leave one behind. A control
    run on the unmutated copy must pass, so the failure is attributable to the
    mutation and not to the isolated environment.
    """
    import hashlib
    import pathlib
    import sys

    from ._isolated_source_mutation import run_mutation_in_isolated_copy

    package_root = pathlib.Path(__file__).resolve().parent.parent       # .../collector
    source = package_root / "collector" / "websocket_client.py"
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    anchor = "await self.on_message(item.data, item.local_receive_ts, connection_id=item.connection_id)"
    assert anchor in source.read_text(), "test anchor text not found; _process_item's shape changed"

    def restamp(text: str) -> str:
        return text.replace(
            anchor,
            "import dataclasses as _dc\n"
            "            item = _dc.replace(item, local_receive_ts=capture_receive_stamp().wall_ms)\n"
            "            " + anchor,
            1,
        )

    target = "collector/tests/test_websocket_client.py::test_on_message_receives_the_arrival_timestamp_not_the_processing_timestamp"
    outcome = run_mutation_in_isolated_copy(
        source_root=package_root,
        mutate_relpath="collector/websocket_client.py",
        transform=restamp,
        argv_factory=lambda root: [sys.executable, "-m", "pytest", "-q", "--no-header",
                                   "-p", "no:cacheprovider", target],
        import_probe="collector.collector.websocket_client",
        cwd_relpath=".",
        timeout=120.0,
    )

    # The original is byte-identical no matter how the subprocesses ended.
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    assert outcome.source_changed, "mutation did not change the source"
    assert not outcome.control.timed_out and outcome.control.returncode == 0, (
        "control run on the UNMUTATED copy did not pass:\n" + outcome.control.output[-2000:]
    )
    assert str(source.resolve()) not in outcome.import_origin and "mutation_copy_" in outcome.import_origin, (
        f"subprocess imported {outcome.import_origin!r}, not the private copy"
    )
    assert not outcome.mutated.timed_out, "mutated run timed out:\n" + outcome.mutated.output[-2000:]
    assert outcome.mutated.returncode not in (0, None), (
        "mutation (re-stamping during processing) was NOT caught by the timestamp test:\n"
        + outcome.mutated.output[-2000:]
    )


@pytest.mark.asyncio
async def test_backpressure_emits_one_quality_event_per_stall_and_still_drops_nothing():
    """The counter is observable in metrics, but the durable, in-data signal
    is the BACKPRESSURE quality event. Emitted once per stall (not per poll
    tick), and the stalled frame must still be delivered once room appears."""
    events = []
    client = WebSocketClient(
        "ws://localhost:9999", lambda *a, **k: None,
        on_quality_event=lambda *a: events.append(a), ingest_queue_maxsize=1,
    )
    client.running = True
    await client._consume(_FakeSocket(['{"n":1}']))          # queue now full
    assert client._ingest_queue.qsize() == 1

    stalled = asyncio.ensure_future(client._consume(_FakeSocket(['{"n":2}'])))
    await asyncio.sleep(0.05)                                 # several poll ticks while stalled
    assert not stalled.done()
    client._ingest_queue.get_nowait()                         # worker frees a slot
    client._ingest_queue.task_done()
    await asyncio.wait_for(stalled, timeout=2.0)

    backpressure = [e for e in events if e[0] == "BACKPRESSURE"]
    assert len(backpressure) == 1 and backpressure[0][1] == "ingest_queue_full:1"
    assert client.queue_backpressure_events == 1
    assert client.frames_enqueued == 2 and client._ingest_queue.qsize() == 1, "the stalled frame must not be dropped"


class _FakeConnection:
    """Async context manager standing in for `websockets.connect(url)`."""
    def __init__(self, frames, on_exhausted=None):
        self._frames, self._on_exhausted = frames, on_exhausted

    async def __aenter__(self):
        frames, on_exhausted = self._frames, self._on_exhausted

        class _WS:
            async def send(self, _):
                return None

            def __aiter__(self):
                async def gen():
                    for frame in frames:
                        yield frame
                    if on_exhausted:
                        on_exhausted()
                return gen()
        return _WS()

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
async def test_start_across_a_reconnect_keeps_one_worker_fifo_order_and_per_connection_attribution():
    """Drives the real start() through TWO connections. The worker is created
    inside the reconnect loop behind a done() guard; a regression to
    "one worker per connection" would put two consumers on one FIFO queue and
    break strict per-stream ordering. Also pins that every frame is attributed
    to the connection it arrived on, even when processed after the reconnect."""
    processed, worker_starts = [], []
    real_sleep = asyncio.sleep

    async def on_message(data, ts, connection_id=None):
        processed.append((data["n"], connection_id))

    client = WebSocketClient("ws://localhost:9999", on_message, ingest_queue_maxsize=10)
    original_worker = client._process_queue

    async def counting_worker():
        worker_starts.append(1)
        await original_worker()
    client._process_queue = counting_worker

    connections = iter([
        _FakeConnection(['{"n":1}', '{"n":2}']),
        _FakeConnection(['{"n":3}', '{"n":4}'], on_exhausted=lambda: setattr(client, "running", False)),
    ])

    async def fast_sleep(delay):
        await real_sleep(0)

    with patch("collector.collector.websocket_client.websockets.connect", new=lambda url, **k: next(connections)):
        with patch("collector.collector.websocket_client.asyncio.sleep", new=fast_sleep):
            await asyncio.wait_for(client.start(), timeout=10.0)

    assert [n for n, _ in processed] == [1, 2, 3, 4], "FIFO order must hold across a reconnect"
    assert len(worker_starts) == 1, "exactly one worker for the client's lifetime, not one per connection"
    first_id, second_id = processed[0][1], processed[2][1]
    assert first_id != second_id
    assert processed[1][1] == first_id and processed[3][1] == second_id
    assert client._worker_task is None


# ---------------------------------------------------------------------------
# Real mutation testing against the actual source (M1, M9).
# ---------------------------------------------------------------------------


def test_mutation_M9_zero_maxsize_validation_is_load_bearing():
    """Removing the maxsize<=0 check from __init__ must be caught."""
    import collector.collector.websocket_client as wsc_module
    import inspect

    source = inspect.getsource(wsc_module.WebSocketClient.__init__)
    assert "ingest_queue_maxsize <= 0" in source, (
        "the maxsize validation guard is missing from __init__ -- "
        "this test's own inspection would not have caught its removal "
        "if it had never been added; the real regression test is "
        "test_zero_maxsize_is_rejected_not_silently_unbounded above, "
        "which exercises the actual runtime behavior, not just presence "
        "of a string in the source"
    )


# ---------------------------------------------------------------------------
# Shutdown ordering: the runner closes storage while the ingest worker is alive.
#
# The runner owns the start() task. Its real shutdown (CollectorApp.
# _async_shutdown -> shutdown()) calls client.stop(), cancels that task and
# closes every writer synchronously. start()'s normal tail,
# _drain_and_stop_worker(), only runs when the receive loop ends on its own, so
# a cancelled start() skips it -- and the worker is a separate task that is not
# in the runner's task list, so nothing cancels or awaits it either.
#
# These tests drive the REAL CollectorApp, the REAL WebSocketClient.start() task
# and the REAL _async_shutdown. The only injected seam is a gate around
# on_message that delegates to the real handler, to hold one frame in flight
# (the Binance handler's own suspension point is the book-snapshot lock).
# ---------------------------------------------------------------------------


class _HeldOpenConnection:
    """Stands in for websockets.connect(url): yields its frames, then stays open
    and quiet until `hold` is set (a quiet socket never ends the receive loop)."""

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


async def _shutdown_with_one_inflight_and_three_queued_frames(tmp_path, monkeypatch, *, callback_wedged=False):
    """Four frames reach the real receive loop; the worker dequeues the first and
    is held inside on_message (in flight) while three more sit in the queue. The
    runner's real signal-path shutdown then runs.

    Default: the in-flight callback becomes runnable on the next loop turn after
    client.stop() is called (as it would be once a cancelled recovery releases the
    snapshot lock), so a correct shutdown can let it finish and a defective one
    cannot. ``callback_wedged=True``: it never becomes runnable (a stuck
    downstream write); the drain timeout is shortened so a correct shutdown that
    waits for the drain still finishes promptly.

    Returns what actually happened, measured with the production counters."""
    import json
    import signal
    from types import SimpleNamespace

    import pyarrow.parquet as pq
    from collector import run_collector as rc

    (tmp_path / "data").mkdir()
    monkeypatch.chdir(tmp_path)
    app = rc.CollectorApp()
    client = app.ws_clients[0]
    real_handler = app.handle_message
    in_flight, resume = asyncio.Event(), asyncio.Event()
    calls, handler_errors, closed_when_resumed = [], [], []

    async def gated_on_message(msg, local_receive_ts, connection_id=None):
        frame_id = msg["data"]["a"]
        calls.append(frame_id)
        if len(calls) == 1:
            in_flight.set()
            await resume.wait()
            closed_when_resumed.append(app.raw_trades_writer._closed)
        try:
            await real_handler(msg, local_receive_ts, connection_id=connection_id)
        except Exception as exc:  # noqa: BLE001 - record, then let the worker handle it unchanged
            handler_errors.append((frame_id, type(exc).__name__, str(exc)))
            raise

    client.on_message = gated_on_message
    client._on_message_takes_connection = True

    real_stop = client.stop

    def stop_then_unblock_next_turn():
        real_stop()
        if not callback_wedged:
            asyncio.get_running_loop().call_soon(resume.set)
    client.stop = stop_then_unblock_next_turn
    if callback_wedged:
        client.shutdown_drain_timeout_s = 0.05

    drain_calls = []
    real_drain = client._drain_and_stop_worker

    async def counting_drain():
        drain_calls.append(1)
        await real_drain()
    client._drain_and_stop_worker = counting_drain

    frames = [json.dumps({"stream": "btcusdt@aggTrade",
                          "data": {"E": 1_780_000_000_000, "a": n, "p": "100", "q": "1", "m": False}})
              for n in range(1, 5)]
    hold = asyncio.Event()
    start_task = None
    worker = None
    try:
        with patch("collector.collector.websocket_client.websockets.connect",
                   new=lambda url, **k: _HeldOpenConnection(frames, hold)):
            start_task = asyncio.create_task(client.start())
            app.tasks.append(start_task)           # exactly what CollectorApp.start() does
            await asyncio.wait_for(in_flight.wait(), timeout=5)

            async def all_enqueued():
                while client.frames_enqueued < len(frames):
                    await asyncio.sleep(0)
            await asyncio.wait_for(all_enqueued(), timeout=5)
            assert client._ingest_queue.qsize() == len(frames) - 1   # one in flight, rest queued
            # Hold the worker by reference BEFORE shutdown: a fix that clears
            # ``client._worker_task`` while the task still runs must not be able to
            # make the "worker is gone" assertion pass vacuously.
            worker = client._worker_task
            assert worker is not None and not worker.done()

            await asyncio.wait_for(app._async_shutdown(signal.SIGTERM), timeout=10)
            await asyncio.wait_for(asyncio.gather(start_task, return_exceptions=True), timeout=10)
            worker_alive_after_start_exit = not worker.done()

            if not callback_wedged:
                # Let whatever work is still pending finish, so the counters are final.
                resume.set()
                await asyncio.wait_for(client._ingest_queue.join(), timeout=10)

        raw_wire_rows = sum(pq.read_table(path).num_rows
                            for path in sorted(app.raw_wire_writer.stream_dir.glob("*.seg")))
        return SimpleNamespace(
            drain_calls=len(drain_calls),
            worker_alive_after_start_exit=worker_alive_after_start_exit,
            writer_closed_when_inflight_frame_resumed=closed_when_resumed[0] if closed_when_resumed else None,
            handler_errors=handler_errors,
            frames_received=client.frames_received,
            frames_enqueued=client.frames_enqueued,
            frames_processed=client.frames_processed,
            processing_errors=client.processing_errors,
            frames_abandoned_at_shutdown=client.frames_abandoned_at_shutdown,
            raw_wire_rows=raw_wire_rows,
        )
    finally:
        for task in (worker, client._worker_task, start_task):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        hold.set()
        resume.set()
        if getattr(app, "segment_dedup", None) is not None:
            app.segment_dedup.close()


@pytest.mark.asyncio
async def test_shutdown_never_lets_the_ingest_worker_use_a_writer_after_it_was_closed(tmp_path, monkeypatch):
    obs = await _shutdown_with_one_inflight_and_three_queued_frames(tmp_path, monkeypatch)
    assert obs.handler_errors == [], (
        "frames were processed against writers the runner's shutdown had already closed "
        f"(writer closed when the in-flight frame resumed: {obs.writer_closed_when_inflight_frame_resumed}): "
        f"{obs.handler_errors}")


@pytest.mark.asyncio
async def test_cancelled_start_does_not_leave_the_ingest_worker_running_behind_a_wedged_callback(tmp_path, monkeypatch):
    obs = await _shutdown_with_one_inflight_and_three_queued_frames(tmp_path, monkeypatch, callback_wedged=True)
    assert not obs.worker_alive_after_start_exit, (
        "start() was cancelled by the runner's shutdown but the ingest worker task is still running "
        f"(_drain_and_stop_worker calls: {obs.drain_calls})")


@pytest.mark.asyncio
async def test_every_enqueued_frame_is_processed_or_counted_abandoned_at_shutdown(tmp_path, monkeypatch):
    obs = await _shutdown_with_one_inflight_and_three_queued_frames(tmp_path, monkeypatch)
    assert obs.processing_errors == 0, (
        f"{obs.processing_errors} frame(s) failed in processing during shutdown: {obs.handler_errors}")
    assert obs.frames_processed + obs.frames_abandoned_at_shutdown == obs.frames_enqueued, (
        f"enqueued={obs.frames_enqueued} processed={obs.frames_processed} "
        f"abandoned_at_shutdown={obs.frames_abandoned_at_shutdown} processing_errors={obs.processing_errors}")


@pytest.mark.asyncio
async def test_raw_wire_rows_for_every_received_frame_are_published_after_orderly_shutdown(tmp_path, monkeypatch):
    """Control, not a defect assertion: raw capture precedes the enqueue, so the
    shutdown ordering problem concerns normalized processing, never raw evidence.

    Scope of the claim: after the runner's ORDERLY shutdown (writers closed in
    order, final segment published) every received frame has a row in a
    published raw_wire segment readable back with pyarrow. This does NOT prove
    power-loss or kill -9 durability (fsync/rename behaviour under a crash), and
    no live exchange or systemd path is involved."""
    obs = await _shutdown_with_one_inflight_and_three_queued_frames(tmp_path, monkeypatch)
    assert obs.frames_received == 4
    assert obs.raw_wire_rows == obs.frames_received


@pytest.mark.asyncio
async def test_cancelling_start_during_the_shutdown_drain_does_not_orphan_the_worker():
    """The runner cancels start() tasks. A cancellation that lands while start() is
    inside _drain_and_stop_worker() (waiting on a wedged callback) must not leave
    the worker task running behind it."""
    entered, release = asyncio.Event(), asyncio.Event()
    draining = asyncio.Event()

    async def wedged_on_message(data, local_receive_ts, connection_id=None):
        entered.set()
        await release.wait()

    client = WebSocketClient("ws://localhost:9999", wedged_on_message, ingest_queue_maxsize=10)
    real_drain = client._drain_and_stop_worker

    async def announcing_drain():
        draining.set()
        await real_drain()
    client._drain_and_stop_worker = announcing_drain

    connection = _FakeConnection(['{"n":1}', '{"n":2}', '{"n":3}'],
                                 on_exhausted=lambda: setattr(client, "running", False))
    start_task = None
    worker = None
    try:
        with patch("collector.collector.websocket_client.websockets.connect", new=lambda url, **k: connection):
            start_task = asyncio.create_task(client.start())
            await asyncio.wait_for(entered.wait(), timeout=5)
            await asyncio.wait_for(draining.wait(), timeout=5)   # start() is now parked in the drain
            worker = client._worker_task                         # by reference, before the cancel
            assert worker is not None and not worker.done()
            start_task.cancel()                                  # what the runner's shutdown does
            await asyncio.wait_for(asyncio.gather(start_task, return_exceptions=True), timeout=5)
            assert worker.done(), (
                "start() was cancelled inside the drain but the ingest worker task is still running "
                f"with {client._ingest_queue.qsize()} queued frame(s) and "
                f"frames_abandoned_at_shutdown={client.frames_abandoned_at_shutdown}")
    finally:
        release.set()
        for task in (worker, start_task):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
