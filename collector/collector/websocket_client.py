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

@dataclass(frozen=True)
class IngestItem:
    """One received frame, captured but not yet decoded or processed.

    This is the entire boundary between the fast receive loop and the
    processing worker (P0-1): everything the receive loop needs to record
    about a frame *before* any JSON decoding, raw-wire persistence, or
    adapter/book-reconstruction work happens. Nothing expensive is done to
    produce one of these -- that is the whole point.
    """
    msg: Any
    local_receive_ts: int
    connection_id: Optional[str]
    connection_generation: int


class WebSocketClient:
    def __init__(self, url: str, on_message: Callable[[dict], Awaitable[None]], on_reconnect: Callable[[], None] = None, on_quality_event: Optional[Callable[[str, str], None]] = None, stream_group: str = "websocket",
                 on_raw_frame: Optional[Callable[..., None]] = None,
                 backoff: Optional[ExponentialBackoff] = None,
                 on_open: Optional[Callable[..., Awaitable[None]]] = None,
                 keepalive: Optional[Keepalive] = None,
                 control_frames: FrozenSet[str] = frozenset(),
                 ingest_queue_maxsize: int = 2000):
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
        self._ws = None
        self._last_inbound_monotonic = 0.0
        self._awaiting_reply_since = None

        # --- P0-1: receive/processing decoupling ----------------------
        # The receive loop (_consume) only ever captures a frame and its
        # arrival metadata into an IngestItem and enqueues it. All expensive
        # work -- JSON decoding, raw-wire persistence, on_message (adapter,
        # sequence validation, book reconstruction, canonical events) --
        # happens in _process_queue, a single long-lived worker that drains
        # the queue in strict FIFO order. One worker, not a pool: this
        # collector has streams (an order book, in particular) where
        # processing order must match arrival order, and a worker pool
        # would have to solve stream-partitioned ordering to be safe. A
        # single worker preserves ordering trivially, at the cost of not
        # parallelizing CPU-bound processing -- an acceptable trade for a
        # single-symbol collector; revisit only if profiling ever shows
        # this worker is the actual bottleneck, not a guess now.
        self.ingest_queue_maxsize = ingest_queue_maxsize
        self._ingest_queue: asyncio.Queue = asyncio.Queue(maxsize=ingest_queue_maxsize)
        self._worker_task: Optional[asyncio.Task] = None
        #: Frames pulled off the socket by _consume, whether or not they
        #: were ever successfully enqueued (see frames_enqueued).
        self.frames_received = 0
        #: Frames that made it into the queue. frames_received -
        #: frames_enqueued is only ever nonzero during the brief window a
        #: backpressured put() is in flight -- there is no code path that
        #: drops a frame between these two counters (see _consume).
        self.frames_enqueued = 0
        self.frames_processed = 0
        self.processing_errors = 0
        self.queue_high_watermark = 0
        #: Count of times the receive loop found the queue already full
        #: and had to wait for the worker to make room -- i.e. genuine,
        #: observable backpressure, distinct from routine operation. Never
        #: a dropped frame: put() is always awaited to completion, never
        #: put_nowait()-and-discard, because silently discarding a market
        #: data frame is unacceptable (unlike the quality-event queue,
        #: which is diagnostic and may drop under extreme load).
        self.queue_backpressure_events = 0

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

                    if self._worker_task is None or self._worker_task.done():
                        # One worker for the client's whole lifetime, not
                        # per-connection: it must keep draining across
                        # reconnects so ordering and backlog survive a
                        # reconnect exactly as they would without one.
                        self._worker_task = asyncio.ensure_future(self._process_queue())

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

        # STOP RECEIVING -> DRAIN PROCESSING QUEUE -> STOP WORKER -> EXIT.
        # By this point self.running is False and no more frames can be
        # enqueued (the receive loop has exited), so draining here is
        # bounded, not a race against new arrivals. An explicit timeout
        # keeps a stuck worker (e.g. a wedged downstream write) from
        # hanging shutdown forever -- the timeout firing is itself an
        # observable, logged condition, not a silent hang.
        if self._worker_task is not None:
            try:
                await asyncio.wait_for(self._ingest_queue.join(), timeout=30.0)
            except asyncio.TimeoutError:
                logger.error("ingest_queue_drain_timeout",
                             remaining=self._ingest_queue.qsize())
            self._worker_task.cancel()
            try:
                await self._worker_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._worker_task = None

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

    async def _consume(self, ws) -> None:
        """Fast receive loop: capture and enqueue only.

        P0-1: this used to also decode JSON, persist the raw frame, and
        await the full on_message pipeline (adapter -> sequence validation
        -> book reconstruction -> canonical events) inline, per frame,
        before looping back to read the next one. Any slowness in that
        chain -- a Parquet segment rollover doing real file I/O, a burst
        of messages, a slow disk -- directly delayed reading the next
        frame off the socket. Now this loop does only the minimum needed
        to preserve the frame and its arrival metadata, then immediately
        loops back for the next one; _process_queue does everything else.
        """
        async for msg in ws:
            if not self.running:
                break
            # Capture arrival time before anything else -- including
            # before the enqueue, which can itself take time under
            # backpressure -- so downstream research can distinguish
            # network arrival from any work performed after it, including
            # queueing delay.
            local_receive_ts = int(time.time() * 1000)
            self._last_inbound_monotonic = self._monotonic()
            self.frames_received += 1

            item = IngestItem(
                msg=msg, local_receive_ts=local_receive_ts,
                connection_id=self.connection_id,
                connection_generation=self._connection_serial,
            )

            # Bounded queue, blocking put, never a dropped frame: silently
            # discarding an exchange frame is unacceptable for research-
            # grade market data (unlike the diagnostic quality-event queue
            # elsewhere in this codebase, which may drop under extreme
            # load). A full queue means the receive loop now waits for the
            # worker to make room -- real, visible backpressure -- rather
            # than either losing data or (the old behaviour) blocking on
            # processing work that had nothing to do with the socket.
            if self._ingest_queue.full():
                self.queue_backpressure_events += 1
                logger.warning("ingest_queue_backpressure",
                                stream_group=self.stream_group,
                                queue_maxsize=self.ingest_queue_maxsize)
                if self.on_quality_event:
                    self.on_quality_event(
                        "BACKPRESSURE", f"ingest_queue_full:{self.ingest_queue_maxsize}",
                        self.connection_id, self.stream_group)
            await self._ingest_queue.put(item)
            self.frames_enqueued += 1
            depth = self._ingest_queue.qsize()
            if depth > self.queue_high_watermark:
                self.queue_high_watermark = depth

    async def _process_queue(self) -> None:
        """Processing worker: everything _consume used to do inline.

        A single long-lived worker per client, started once in ``start()``
        and persisting across reconnects, draining ``_ingest_queue`` in
        strict FIFO order -- this is what keeps stream ordering intact
        (see the architectural note in ``__init__``) without needing any
        per-stream partitioning. One failing item is isolated (Test 5):
        an exception here is logged and counted, never allowed to kill the
        worker loop, because a single malformed or unexpected message must
        not stop every subsequent frame from being processed.
        """
        while self.running or not self._ingest_queue.empty():
            try:
                item = await asyncio.wait_for(self._ingest_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            try:
                await self._process_item(item)
                self.frames_processed += 1
            except Exception as exc:  # noqa: BLE001 - isolate one bad item
                self.processing_errors += 1
                logger.error("ingest_item_processing_failed",
                             error=str(exc), stream_group=self.stream_group)
                if self.on_quality_event:
                    self.on_quality_event(
                        "ERROR", f"processing_failed:{type(exc).__name__}",
                        item.connection_id, self.stream_group)
            finally:
                self._ingest_queue.task_done()

    async def _process_item(self, item: "IngestItem") -> None:
        """The exact per-frame work _consume used to do inline, unchanged
        in substance: control-frame handling, JSON decode, raw-frame
        capture (still ordered before every lossy step, including a
        failed decode and including control frames -- a frame that did
        not parse, or carried no market data, is still a frame that
        arrived), then on_message. Only *when* this runs changed -- from
        inline in the receive loop to here, in the worker -- not what it
        does or in what order.
        """
        msg = item.msg
        local_receive_ts = item.local_receive_ts

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

        if self.on_raw_frame is not None:
            try:
                self.on_raw_frame(
                    msg,
                    local_receive_ts=local_receive_ts,
                    connection_id=item.connection_id,
                    connection_generation=item.connection_generation,
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
            return

        if decode_error is not None:
            self.malformed_frames += 1
            logger.error("Failed to parse JSON from WebSocket", error=decode_error)
            if self.on_quality_event:
                self.on_quality_event(
                    "ERROR", f"malformed_frame:{decode_error}",
                    item.connection_id, self.stream_group,
                )
            return

        if self._on_message_takes_connection:
            await self.on_message(data, local_receive_ts, connection_id=item.connection_id)
        else:
            await self.on_message(data, local_receive_ts)

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
