import asyncio
import inspect
import json
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, FrozenSet, Optional

import websockets

from .backoff import BackoffExhausted, ExponentialBackoff
from .utils import logger


@dataclass(frozen=True)
class Keepalive:
    """Application-level heartbeat for venues that require one.

    Binance USD-M needs nothing here: the server drives protocol-level pings
    and the ``websockets`` library answers them. OKX v5 is different -- it
    closes a connection that has neither pushed data nor received a request
    for more than 30 seconds, and the documented client obligation is to send
    the literal string ``ping`` and expect the literal string ``pong`` back.

    ``interval_s`` is measured from the last *inbound* frame, not from the
    last ping, because any inbound frame proves the link is alive and a venue
    that is pushing data needs no heartbeat at all.
    """

    payload: str
    interval_s: float
    expect: Optional[str] = None
    timeout_s: float = 10.0

    def __post_init__(self) -> None:
        if self.interval_s <= 0 or self.timeout_s <= 0:
            raise ValueError("keepalive interval_s and timeout_s must be positive")

class WebSocketClient:
    #: Sentinel enqueued to make the processing worker exit after it has
    #: drained every message already in the queue -- never used as a
    #: signal to discard anything, only to know when to stop pulling.
    _WORKER_SHUTDOWN = object()

    def __init__(self, url: str, on_message: Callable[[dict], Awaitable[None]], on_reconnect: Callable[[], None] = None, on_quality_event: Optional[Callable[[str, str], None]] = None, stream_group: str = "websocket",
                 on_raw_frame: Optional[Callable[..., None]] = None,
                 backoff: Optional[ExponentialBackoff] = None,
                 on_open: Optional[Callable[..., Awaitable[None]]] = None,
                 keepalive: Optional[Keepalive] = None,
                 control_frames: FrozenSet[str] = frozenset(),
                 processing_queue_maxsize: int = 2000):
        self.url = url
        self.stream_group = stream_group
        self.on_message = on_message
        self.on_reconnect = on_reconnect
        self.on_quality_event = on_quality_event
        # Raw capture hook. Invoked with the undecoded frame text before
        # json.loads so a malformed payload is still preserved.
        self.on_raw_frame = on_raw_frame
        self._on_message_takes_connection = self._accepts_connection_id(on_message)
        self.running = False
        self.connected = False
        # Full jitter and an attempt budget. The previous loop doubled a
        # bare delay forever, so every client that dropped together
        # retried together, indefinitely.
        self.backoff = backoff or ExponentialBackoff(
            base_delay=1.0, max_delay=60.0, max_attempts=64)
        self.retry_delay = self.backoff.peek_cap()
        self.attempt = 0
        self.reconnect_budget_exhausted = False
        self.connection_id = None
        self._connection_serial = 0
        self.malformed_frames = 0
        # Venue protocol hooks. All optional; the defaults reproduce the
        # previous Binance-only behaviour exactly.
        #: Called after connect with a ``send`` coroutine so a venue can issue
        #: its subscribe request. Binance carries streams in the URL and needs
        #: none; OKX and Bybit must subscribe over the socket.
        self.on_open = on_open
        self.keepalive = keepalive
        #: Literal non-JSON text frames that are valid protocol, e.g. OKX's
        #: ``pong``. Without this they would be counted as malformed frames
        #: and would each write a durable ERROR quality event -- a false
        #: data-quality signal for a perfectly healthy connection.
        self.control_frames = frozenset(control_frames)
        self.control_frames_received = 0
        self.keepalive_timeouts = 0
        # P0-1: bounded receive/processing separation. `_consume` (the raw
        # read loop below) must never block on `on_message` -- a slow
        # handler (e.g. a REST snapshot-bridge fetch awaited inside it)
        # would otherwise stall the next `async for msg in ws` read,
        # risking exchange-side backpressure or disconnection for being a
        # slow reader. Raw capture (`on_raw_frame`) already happens before
        # this point, synchronously, on the fast path -- unaffected by any
        # of this. A queue-overflow here therefore drops a message from
        # *live processing* only, never from the durable raw record: the
        # exact bytes remain replayable later even if live book state
        # falls behind under sustained overload. `processing_queue_maxsize`
        # of 2000: chosen for a single-symbol, single-venue collector where
        # combined orderbook+trade traffic realistically peaks in the low
        # hundreds of messages/sec even during high volatility, and the
        # slowest realistic per-message stall (a REST snapshot-bridge
        # fetch) resolves in low single-digit seconds even under a poor
        # network -- 2000 buffers roughly that worst case at a materially
        # higher rate than observed traffic, without holding an unbounded
        # amount of memory.
        self._processing_queue: asyncio.Queue = asyncio.Queue(maxsize=processing_queue_maxsize)
        self.processing_queue_overflow = 0
        self._processing_worker_task = None
        self._ws = None
        self._last_inbound_monotonic = 0.0
        self._awaiting_reply_since = None

    @staticmethod
    def _accepts_connection_id(callback) -> bool:
        """Probe the handler once rather than guessing on every frame.

        Older handlers take ``(data, local_receive_ts)``. Handlers that want
        raw lineage take a ``connection_id`` keyword. Detecting this once at
        construction avoids a per-frame try/except that could swallow a
        genuine TypeError raised inside the handler itself.
        """
        if callback is None:
            return False
        try:
            signature = inspect.signature(callback)
        except (TypeError, ValueError):
            return False
        parameters = signature.parameters
        if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values()):
            return True
        return "connection_id" in parameters

    async def start(self):
        self.running = True
        logger.info("Starting WebSocket client", url=self.url)
        self._processing_worker_task = asyncio.ensure_future(self._processing_worker())

        try:
            while self.running:
                try:
                    connection = websockets.connect(self.url)
                    if hasattr(connection, "__await__"):
                        connection = await connection

                    async with connection as ws:
                        self.connected = True
                        self._connection_serial += 1
                        self.connection_id = f"{self.stream_group}-{self._connection_serial}"
                        self.backoff.reset()
                        self.retry_delay = self.backoff.peek_cap()
                        self.attempt = 0
                        logger.info("WebSocket connected")

                        if self.on_quality_event:
                            self.on_quality_event("CONNECT", "websocket_connected", self.connection_id, self.stream_group)

                        if self.on_reconnect:
                            self.on_reconnect()

                        self._ws = ws
                        self._last_inbound_monotonic = self._monotonic()
                        self._awaiting_reply_since = None

                        if self.on_open is not None:
                            # A failed subscribe must be visible: it is the
                            # difference between "quiet market" and "we are not
                            # subscribed to anything".
                            try:
                                await self.on_open(self._send)
                            except Exception as exc:  # noqa: BLE001
                                logger.error("websocket_on_open_failed", error=str(exc))
                                if self.on_quality_event:
                                    self.on_quality_event(
                                        "ERROR", f"subscribe_failed:{type(exc).__name__}",
                                        self.connection_id, self.stream_group)
                                raise

                        keepalive_task = None
                        if self.keepalive is not None:
                            keepalive_task = asyncio.ensure_future(self._keepalive_loop())

                        try:
                            await self._consume(ws)
                        finally:
                            if keepalive_task is not None:
                                keepalive_task.cancel()
                                # Awaiting the cancellation stops a pending task
                                # from outliving its connection across reconnects.
                                try:
                                    await keepalive_task
                                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                                    pass
                            self._ws = None

                except websockets.ConnectionClosed as e:
                    self.connected = False
                    logger.warning("WebSocket connection closed", error=str(e))
                    if self.on_quality_event:
                        self.on_quality_event("DISCONNECT", str(e), self.connection_id, self.stream_group)
                except Exception as e:
                    self.connected = False
                    logger.error("WebSocket error", error=str(e))
                    if self.on_quality_event:
                        self.on_quality_event("DISCONNECT", str(e), self.connection_id, self.stream_group)

                if self.running:
                    self.attempt += 1
                    try:
                        delay = self.backoff.next_delay()
                    except BackoffExhausted:
                        # A permanently broken endpoint must stop being hammered,
                        # and that must be visible in the data, not just in logs.
                        self.reconnect_budget_exhausted = True
                        self.running = False
                        logger.error("reconnect_budget_exhausted",
                                     url=self.url, attempts=self.attempt)
                        if self.on_quality_event:
                            self.on_quality_event(
                                "ERROR", f"reconnect_budget_exhausted:{self.attempt}",
                                self.connection_id, self.stream_group)
                        break
                    self.retry_delay = delay
                    logger.info("Reconnecting WebSocket", attempt=self.attempt, delay=delay)
                    await asyncio.sleep(delay)

        finally:
            # Graceful drain (P0-1): let the worker finish whatever is
            # already queued before start() itself returns, so a
            # caller that awaits this coroutine after calling stop()
            # observes every already-accepted message actually
            # processed -- not merely the connection closed.
            await self._processing_queue.put(self._WORKER_SHUTDOWN)
            await self._processing_worker_task

    @staticmethod
    def _monotonic() -> float:
        return time.monotonic()

    async def _send(self, message: Any) -> None:
        """Send a frame. Dicts are JSON-encoded; strings go out verbatim.

        Verbatim strings matter: OKX's heartbeat is the bare text ``ping``,
        not a JSON object, so encoding it would break the protocol.
        """
        if self._ws is None:
            raise RuntimeError("websocket is not connected")
        payload = message if isinstance(message, str) else json.dumps(message)
        await self._ws.send(payload)

    async def _processing_worker(self) -> None:
        """Drains the processing queue in strict FIFO order, one item at a
        time, calling on_message exactly as _consume used to call it
        inline. Runs for the whole lifetime of start() (across
        reconnects), not per-connection, so a message queued just before a
        disconnect is still processed afterward rather than lost."""
        while True:
            item = await self._processing_queue.get()
            if item is self._WORKER_SHUTDOWN:
                self._processing_queue.task_done()
                return
            data, local_receive_ts, connection_id = item
            try:
                if connection_id is not None:
                    await self.on_message(data, local_receive_ts, connection_id=connection_id)
                else:
                    await self.on_message(data, local_receive_ts)
            except Exception as exc:  # noqa: BLE001 - one bad message must not stop the worker,
                # matching on_raw_frame's own established fail-open policy above.
                logger.error("processing_worker_message_failed", error=str(exc), stream_group=self.stream_group)
            finally:
                self._processing_queue.task_done()

    async def _consume(self, ws) -> None:
        async for msg in ws:
            if not self.running:
                break
            # Capture arrival time before decoding so downstream research can
            # distinguish network arrival from work performed after parsing.
            local_receive_ts = int(time.time() * 1000)
            self._last_inbound_monotonic = self._monotonic()

            text = msg if isinstance(msg, str) else None
            is_control = text is not None and text in self.control_frames
            if is_control:
                self._awaiting_reply_since = None

            decode_error = None
            data = None
            if not is_control:
                try:
                    data = json.loads(msg)
                except (json.JSONDecodeError, TypeError, ValueError) as exc:
                    decode_error = str(exc)

            # Raw capture precedes every lossy step, including a failed decode
            # and including control frames: a frame that did not parse, or that
            # carried no market data, is still a frame that arrived.
            if self.on_raw_frame is not None:
                try:
                    self.on_raw_frame(
                        msg,
                        local_receive_ts=local_receive_ts,
                        connection_id=self.connection_id,
                        connection_generation=self._connection_serial,
                        decode_ok=decode_error is None,
                        decode_error=decode_error,
                        parsed=data,
                        control_frame=is_control,
                    )
                except Exception as exc:  # noqa: BLE001 - capture fails open
                    logger.warning("raw_frame_capture_failed", error=str(exc))

            if is_control:
                # Healthy protocol traffic. Counted, never reported as
                # malformed, and not forwarded to the market-data handler.
                self.control_frames_received += 1
                continue

            if decode_error is not None:
                self.malformed_frames += 1
                logger.error("Failed to parse JSON from WebSocket", error=decode_error)
                if self.on_quality_event:
                    self.on_quality_event(
                        "ERROR", f"malformed_frame:{decode_error}",
                        self.connection_id, self.stream_group,
                    )
                continue

            item = (data, local_receive_ts, self.connection_id if self._on_message_takes_connection else None)
            try:
                self._processing_queue.put_nowait(item)
            except asyncio.QueueFull:
                # The raw bytes are already durably captured above (before
                # this point) -- only *live processing* of this message is
                # dropped, and it remains replayable from raw capture
                # later. Never block here: blocking would reintroduce the
                # exact receive/processing coupling this queue exists to
                # remove, and risk exchange-side backpressure.
                self.processing_queue_overflow += 1
                logger.error("processing_queue_overflow", dropped=self.processing_queue_overflow,
                             stream_group=self.stream_group)
                if self.on_quality_event:
                    self.on_quality_event(
                        "DATA_DROP", f"processing_queue_overflow:{self.processing_queue_overflow}",
                        self.connection_id, self.stream_group)

    async def _keepalive_loop(self) -> None:
        """Heartbeat only when the link has gone quiet.

        A venue that is pushing data needs no ping, so the timer is driven by
        the last inbound frame. If a ping goes unanswered within
        ``timeout_s`` the connection is closed so the normal reconnect path
        runs -- a silently dead socket is worse than a visible reconnect.
        """
        config = self.keepalive
        if config is None:   # not an assert: must still hold under python -O
            return
        while self.running and self._ws is not None:
            await asyncio.sleep(min(config.interval_s, config.timeout_s) / 2.0)
            if not self.running or self._ws is None:
                return
            now = self._monotonic()
            pending = self._awaiting_reply_since
            if pending is not None:
                if now - pending >= config.timeout_s:
                    self.keepalive_timeouts += 1
                    logger.warning("keepalive_timeout", url=self.url)
                    if self.on_quality_event:
                        self.on_quality_event(
                            "DISCONNECT", "keepalive_timeout",
                            self.connection_id, self.stream_group)
                    self._awaiting_reply_since = None
                    try:
                        await self._ws.close()
                    except Exception:  # noqa: BLE001
                        pass
                    return
                continue
            if now - self._last_inbound_monotonic >= config.interval_s:
                try:
                    await self._send(config.payload)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("keepalive_send_failed", error=str(exc))
                    return
                if config.expect is not None:
                    self._awaiting_reply_since = now

    async def wait_connected(self, timeout_seconds: float = 30.0) -> bool:
        deadline = asyncio.get_event_loop().time() + timeout_seconds
        while asyncio.get_event_loop().time() < deadline:
            if self.connected:
                return True
            await asyncio.sleep(0.1)
        return False

    def stop(self):
        self.running = False
        self.connected = False
