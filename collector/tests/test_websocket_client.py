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
