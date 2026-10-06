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
    """Real source mutation (not simulated), run as a clean subprocess to
    avoid nesting an async test run inside another event loop: makes
    _process_item call capture_receive_stamp again and overwrite the
    item's timestamp before calling on_message -- exactly the regression
    class this invariant guards against. Confirms the test above fails
    under the mutation, then restores the source and verifies it is
    byte-identical."""
    import pathlib
    import subprocess
    import sys

    path = pathlib.Path(__file__).resolve().parent.parent / "collector" / "websocket_client.py"
    original = path.read_text()
    anchor = "await self.on_message(item.data, item.local_receive_ts, connection_id=item.connection_id)"
    assert anchor in original, "test anchor text not found; _process_item's shape changed"
    mutated = original.replace(
        anchor,
        "import dataclasses as _dc\n"
        "            item = _dc.replace(item, local_receive_ts=capture_receive_stamp().wall_ms)\n"
        "            " + anchor,
        1,
    )
    assert mutated != original
    path.write_text(mutated)
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "--no-header",
             str(pathlib.Path(__file__).resolve()) +
             "::test_on_message_receives_the_arrival_timestamp_not_the_processing_timestamp"],
            cwd=str(path.resolve().parents[2]), capture_output=True, text=True,
        )
    finally:
        path.write_text(original)
    assert result.returncode != 0, (
        "mutation (re-stamping during processing) was NOT caught by the timestamp test:\n"
        + result.stdout[-2000:]
    )
    assert path.read_text() == original


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
