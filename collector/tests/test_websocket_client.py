import pytest
import asyncio
import json
import websockets
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
# P0-1: bounded receive/processing separation.
# ---------------------------------------------------------------------------


class _FakeSocket:
    def __init__(self, frames):
        self.frames = frames

    def __aiter__(self):
        async def gen():
            for frame in self.frames:
                yield frame
        return gen()


async def _drain(client):
    """Run the worker until the queue is empty, then shut it down cleanly
    -- the same pattern the real start()'s finally block uses, isolated
    here for tests that call _consume() directly rather than start()."""
    worker = asyncio.ensure_future(client._processing_worker())
    await client._processing_queue.join()
    client._processing_queue.put_nowait(client._WORKER_SHUTDOWN)
    await worker


@pytest.mark.asyncio
async def test_consume_does_not_block_on_a_slow_on_message():
    """The core P0-1 property: _consume's own loop (receive + decode +
    raw capture) must finish reading every frame regardless of how slow
    on_message is -- proven by a handler that would still be asleep on
    the first message when _consume itself has already returned."""
    processed = []

    async def slow_on_message(data, local_receive_ts):
        if not processed:
            await asyncio.sleep(0.2)   # still "processing" message 1 when message 2 arrives
        processed.append(data)

    client = WebSocketClient("ws://localhost:9999", slow_on_message)
    client.running = True
    frames = ['{"n": 1}', '{"n": 2}', '{"n": 3}']

    start = asyncio.get_event_loop().time()
    await client._consume(_FakeSocket(frames))
    consume_elapsed = asyncio.get_event_loop().time() - start
    assert consume_elapsed < 0.1   # _consume returned long before the 0.2s handler sleep finished
    assert processed == []          # nothing processed yet -- still queued

    await _drain(client)
    assert processed == [{"n": 1}, {"n": 2}, {"n": 3}]   # eventually all processed, in order


@pytest.mark.asyncio
async def test_processing_order_is_preserved():
    order = []

    async def on_message(data, local_receive_ts):
        order.append(data["n"])

    client = WebSocketClient("ws://localhost:9999", on_message)
    client.running = True
    frames = [json.dumps({"n": i}) for i in range(20)]
    await client._consume(_FakeSocket(frames))
    await _drain(client)
    assert order == list(range(20))


@pytest.mark.asyncio
async def test_raw_capture_happens_before_enqueue_regardless_of_processing_backlog():
    """on_raw_frame must fire for every frame even while the processing
    queue is completely backed up -- raw capture is the fast path and
    must never depend on processing keeping pace."""
    captured = []

    async def never_runs_during_this_test(data, local_receive_ts):
        await asyncio.sleep(10)   # never actually awaited to completion here

    client = WebSocketClient(
        "ws://localhost:9999", never_runs_during_this_test,
        on_raw_frame=lambda msg, **kw: captured.append(msg),
        processing_queue_maxsize=2,
    )
    client.running = True
    frames = ['{"n": 1}', '{"n": 2}', '{"n": 3}', '{"n": 4}', '{"n": 5}']
    await client._consume(_FakeSocket(frames))
    assert captured == frames   # every frame captured, even the ones the tiny queue had to drop


@pytest.mark.asyncio
async def test_processing_queue_overflow_drops_newest_without_blocking_or_raising():
    """A bounded queue must, under sustained overload, shed the newest
    arrivals rather than block _consume (which would reintroduce the
    coupling this design removes) or raise out of _consume."""
    events = []

    async def never_finishes(data, local_receive_ts):
        await asyncio.sleep(10)

    client = WebSocketClient(
        "ws://localhost:9999", never_finishes,
        on_quality_event=lambda etype, reason, cid, group: events.append((etype, reason)),
        processing_queue_maxsize=2,
    )
    client.running = True
    frames = [json.dumps({"n": i}) for i in range(5)]
    await client._consume(_FakeSocket(frames))   # must complete without raising

    assert client.processing_queue_overflow == 3   # 5 frames, queue holds 2 -> 3 dropped
    assert client._processing_queue.qsize() == 2
    overflow_events = [e for e in events if e[0] == "DATA_DROP"]
    assert len(overflow_events) == 3
    assert all("processing_queue_overflow" in reason for _etype, reason in overflow_events)


@pytest.mark.asyncio
async def test_graceful_stop_drains_every_already_queued_message():
    """stop() + awaiting start() must not return until every message
    already accepted into the queue has actually been processed --
    partial processing on shutdown is exactly the ordering/completeness
    guarantee P0-1 exists to preserve."""
    processed = []

    async def on_message(data, local_receive_ts):
        await asyncio.sleep(0.05)   # still mid-processing when the connection "closes"
        processed.append(data["n"])

    def on_quality_event(event_type, reason, connection_id, stream_group):
        if event_type == "DISCONNECT":
            # The real disconnect hook -- used here to call stop() at the
            # exact moment the connection ends, so start()'s reconnect
            # check sees running=False and exits into the drain, deterministically,
            # with no timing-sensitive sleep needed to coordinate it.
            client.stop()

    client = WebSocketClient("ws://localhost:9999", on_message, on_quality_event=on_quality_event)

    class _ClosesAfterFramesSocket:
        """A real connection behaves exactly like this on a clean server-
        side close: it yields whatever arrived, then the iterator itself
        raises ConnectionClosed -- the same exception start()'s own
        except clause already handles."""
        def __aiter__(self):
            async def gen():
                for i in range(10):
                    yield json.dumps({"n": i})
                raise websockets.ConnectionClosed(None, None)
            return gen()

    class _AsyncCM:
        async def __aenter__(self):
            return _ClosesAfterFramesSocket()
        async def __aexit__(self, *exc):
            return False

    async def fake_connect(url, **kwargs):
        return _AsyncCM()

    with patch("collector.collector.websocket_client.websockets.connect", new=fake_connect):
        await asyncio.wait_for(client.start(), timeout=2.0)   # returns only once the finally block's drain completes

    assert sorted(processed) == list(range(10))   # every accepted message was processed before start() returned
