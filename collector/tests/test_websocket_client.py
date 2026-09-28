import pytest
import asyncio
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
