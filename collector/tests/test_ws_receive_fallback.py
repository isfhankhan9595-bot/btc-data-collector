"""Receive-start path for interpreters without eager tasks (Python 3.11).

CI runs Python 3.12 only, while the pinned dependencies (pandas 3.0.3 / numpy 2.4.6:
Requires-Python >=3.11) also install on 3.11. ``_next_frame`` starts each receive with
``asyncio.Task(..., eager_start=True)`` and falls back on ``TypeError``; these tests force
that fallback on whatever interpreter runs them, and the same file was also run on a real
3.11. The fallback must keep the receive loop's scheduling identical to a plain ``async for``:
a frame that is already available costs no event-loop turn (otherwise a burst can never fill
the bounded queue, and the backpressure/observability tests fail).
"""
import asyncio
from unittest.mock import patch

import pytest

from collector.collector import websocket_client as wsc
from collector.collector.websocket_client import WebSocketClient


class _NoEager(asyncio.Task):
    def __init__(self, coro, *, loop=None, name=None, context=None, **kw):
        if "eager_start" in kw:
            raise TypeError("Task.__init__() got an unexpected keyword argument 'eager_start'")
        super().__init__(coro, loop=loop, name=name, context=context)


@pytest.fixture(params=["native", "no_eager_fallback"])
def mode(request):
    if request.param == "native":
        yield "native"
        return
    with patch("collector.collector.websocket_client.asyncio.Task", _NoEager):
        yield "no_eager_fallback"


def _client():
    return WebSocketClient("ws://x", lambda *a, **k: None)


@pytest.mark.asyncio
async def test_ready_frames_cost_no_event_loop_turn(mode):
    async def gen():
        for i in range(100):
            yield i
    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            await asyncio.sleep(0)
            ticks += 1
    t = asyncio.create_task(ticker())
    await asyncio.sleep(0)
    base, client, it = ticks, _client(), gen().__aiter__()
    got = [await client._next_frame(it) for _ in range(100)]
    assert got == list(range(100)) and ticks == base, "a ready frame must not yield to the event loop"
    assert await client._next_frame(it) is wsc._STREAM_END
    t.cancel()
    await asyncio.gather(t, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_pending_receive_is_woken_by_stop_and_leaves_no_task_behind(mode):
    gate = asyncio.Event()

    async def gen():
        yield 1
        await gate.wait()
        yield 2
    client, it = _client(), None
    it = gen().__aiter__()
    assert await client._next_frame(it) == 1
    before = len(asyncio.all_tasks())
    waiter = asyncio.create_task(client._next_frame(it))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    client.stop()
    assert await asyncio.wait_for(waiter, 5) is wsc._STREAM_END
    await asyncio.sleep(0)
    assert len(asyncio.all_tasks()) == before, "the pending receive must not outlive the call"


@pytest.mark.asyncio
async def test_a_suspended_receive_delivers_its_frame_and_propagates_errors(mode):
    gate = asyncio.Event()

    async def gen():
        await gate.wait()
        yield "late"
        raise ValueError("socket error")
    client, it = _client(), None
    it = gen().__aiter__()
    pending = asyncio.create_task(client._next_frame(it))
    await asyncio.sleep(0)
    gate.set()
    assert await asyncio.wait_for(pending, 5) == "late"
    with pytest.raises(ValueError):
        await asyncio.wait_for(client._next_frame(it), 5)


@pytest.mark.asyncio
async def test_an_error_raised_without_suspending_propagates(mode):
    async def gen():
        raise ValueError("immediate")
        yield 1
    with pytest.raises(ValueError, match="immediate"):
        await _client()._next_frame(gen().__aiter__())


@pytest.mark.asyncio
async def test_cancelling_the_caller_cancels_the_pending_receive(mode):
    gate, cancelled = asyncio.Event(), []

    async def gen():
        try:
            await gate.wait()
            yield 1
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
    waiter = asyncio.create_task(_client()._next_frame(gen().__aiter__()))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    waiter.cancel()
    await asyncio.gather(waiter, return_exceptions=True)
    assert waiter.cancelled() and cancelled == [True]
