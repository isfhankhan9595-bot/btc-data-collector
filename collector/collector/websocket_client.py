import asyncio
import inspect
import json
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, FrozenSet, Optional

import websockets

from .backoff import BackoffExhausted, ExponentialBackoff
from .clock import ReceiveStamp, capture_receive_stamp
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
    """One received frame: already decoded and already durably captured,
    but not yet handed to the venue's on_message pipeline.

    Post-merge correction (see _consume's docstring): raw capture and JSON
    decoding both happen in the receive loop now, before this item is ever
    built, not later in the worker. What crosses the queue boundary is
    everything needed to call on_message -- nothing more expensive than
    that remains to be done to the frame itself once it is enqueued.
    """
    msg: Any
    local_receive_ts: int
    connection_id: Optional[str]
    connection_generation: int
    data: Optional[dict]
    decode_error: Optional[str]
    is_control: bool
    #: P0-11: the receive-boundary stamp, taken before decode/capture/queue.
    #: Carried through unchanged; the worker never re-stamps.
    receive_stamp: Optional[ReceiveStamp] = None


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
        self._raw_hook_takes_ns: Optional[bool] = None
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
        # asyncio.Queue treats maxsize <= 0 as UNBOUNDED -- not "0 items
        # allowed", the opposite. Every comment and every piece of
        # documentation in this class claims a bounded queue; nothing
        # previously enforced it. A caller passing 0 (or a negative value,
        # e.g. from a misread config default) would have silently gotten
        # an unbounded queue with no error anywhere.
        if ingest_queue_maxsize <= 0:
            raise ValueError(
                f"ingest_queue_maxsize must be positive (asyncio.Queue treats "
                f"<= 0 as unbounded, which is exactly the opposite of what "
                f"this class promises); got {ingest_queue_maxsize!r}")
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
        #: Frames that were durably raw-captured (see _consume) but whose
        #: enqueue was abandoned because shutdown was requested while the
        #: queue stayed full. Never a raw-data loss -- on_raw_frame already
        #: ran for these -- but they will not reach on_message/canonical
        #: processing in this process run. Explicit and counted, per the
        #: "any loss/degradation is explicit and observable" invariant,
        #: rather than either hanging shutdown forever or pretending the
        #: frame was fully processed.
        self.frames_abandoned_at_shutdown = 0
        #: Failures of the on_raw_frame *callback itself* (a bug in a runner's
        #: _capture_raw_frame before it ever reaches RawCapture.capture_wire).
        #: A writer failure is NOT counted here: RawCapture already catches
        #: those, counts them in its own capture_failures, and emits a durable
        #: DATA_DROP quality event per lost frame. This counter exists for the
        #: layer above that, which used to swallow with only a log line.
        #: Upper bound on how long shutdown waits for the worker to drain the
        #: ingest queue. Firing is counted, not just logged.
        self.shutdown_drain_timeout_s = 30.0
        self.raw_capture_callback_failures = 0
        #: True from the first callback failure until the next success. One
        #: quality event per failure *streak*, not per frame: a callback that
        #: is broken fails on every frame, and a per-frame event would flood.
        self.raw_capture_callback_degraded = False

    def _receive_kwargs(self, stamp: ReceiveStamp) -> dict:
        """ns/monotonic kwargs for on_raw_frame, only when it accepts them.

        Probed once (cached) so legacy hooks with a fixed signature keep
        working unchanged and a genuine TypeError inside a hook is never
        mistaken for a signature mismatch.
        """
        if self._raw_hook_takes_ns is None:
            hook = self.on_raw_frame
            try:
                params = inspect.signature(hook).parameters
                self._raw_hook_takes_ns = (
                    "local_receive_ns" in params
                    or any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())
                )
            except (TypeError, ValueError):
                self._raw_hook_takes_ns = False
        if not self._raw_hook_takes_ns:
            return {}
        return {"local_receive_ns": stamp.wall_ns, "receive_mono_ns": stamp.mono_ns}

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
        await self._drain_and_stop_worker()

    async def _drain_and_stop_worker(self) -> None:
        """Drain the ingest queue (bounded), then stop the worker.

        Extracted from start() unchanged in behaviour so the timeout path is
        testable; the timeout is an attribute instead of a literal.
        """
        if self._worker_task is not None:
            try:
                await asyncio.wait_for(self._ingest_queue.join(), timeout=self.shutdown_drain_timeout_s)
            except asyncio.TimeoutError:
                # Frames still queued when the drain gives up will not reach
                # on_message in this run. Their raw copy already exists (raw
                # capture precedes the enqueue), so this is lost *processing*,
                # not lost raw evidence -- but it was logged only, uncounted,
                # unlike the sibling shutdown-abandonment path. qsize() does
                # not include an item the worker is mid-way through.
                remaining = self._ingest_queue.qsize()
                self.frames_abandoned_at_shutdown += remaining
                logger.error("ingest_queue_drain_timeout", remaining=remaining)
                if self.on_quality_event:
                    self.on_quality_event(
                        "ERROR", f"ingest_queue_drain_timeout:{remaining}",
                        self.connection_id, self.stream_group)
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
        """Fast-but-durable receive loop: decode, raw-capture, then enqueue.

        Post-merge correction: the originally merged P0-1 moved JSON
        decoding *and* raw-frame capture into the worker, alongside
        on_message. That reopened exactly the durability gap P0-1 was
        supposed to close from the other direction: a frame that had been
        received, timestamped, and handed to asyncio.Queue.put() existed
        only in process memory until the worker got around to it. A crash
        or SIGKILL in that window -- which can hold up to
        ``ingest_queue_maxsize`` frames at once -- lost raw evidence for
        every frame still sitting in the queue, silently, with nothing in
        the architecture to detect or report it. That is a straightforward
        violation of this project's central invariant: raw exchange data
        is authoritative and durable before anything else happens to it.

        The fix keeps this loop fast without reintroducing that gap: JSON
        decoding is pure CPU work (no I/O, no unbounded blocking) and stays
        here; raw-frame capture (``on_raw_frame``) also moves back here, so
        every frame is durably captured *before* it is ever enqueued --
        the same ordering the pre-P0-1 architecture had. This does
        reintroduce a bounded, infrequent blocking risk: the raw writer's
        common-case write is an in-memory buffer append, but it can
        occasionally do real, synchronous file I/O at a Parquet segment
        rollover boundary. That risk is real but narrow (once per rollover
        threshold, not once per message) and is the deliberate, smaller
        risk this architecture accepts in exchange for closing a real
        data-loss window -- "data truth comes before performance" is the
        explicit priority for this correction, not a slogan. Only
        on_message (adapter, sequence validation, book reconstruction,
        canonical events -- the actually unbounded-latency, per-venue work)
        remains decoupled into the worker, which is what P0-1 was for.
        """
        async for msg in ws:
            if not self.running:
                break
            # Capture arrival time before anything else, including before
            # decoding, so it reflects actual network arrival rather than
            # any work performed on the frame afterward.
            receive_stamp = capture_receive_stamp()
            local_receive_ts = receive_stamp.wall_ms  # same clock read as wall_ns
            self._last_inbound_monotonic = self._monotonic()
            self.frames_received += 1

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

            # Raw capture precedes every lossy step, including a failed
            # decode and including control frames -- a frame that did not
            # parse, or carried no market data, is still a frame that
            # arrived -- and now precedes the queue boundary too, so it is
            # durable before this frame's fate depends on anything else
            # (queue capacity, worker availability, a later crash).
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
                        **self._receive_kwargs(receive_stamp),
                    )
                except Exception as exc:  # noqa: BLE001 - fails open, but never silently
                    # Ingestion continues (stopping would lose every frame,
                    # not just the raw copy), but this is a raw-evidence
                    # failure that used to be a log line only: no counter,
                    # no quality event. If a runner's callback broke, raw
                    # capture was disabled for every frame with nothing in
                    # the data to say so.
                    self.raw_capture_callback_failures += 1
                    if not self.raw_capture_callback_degraded:
                        self.raw_capture_callback_degraded = True
                        logger.error("raw_capture_callback_failed",
                                     error=str(exc), stream_group=self.stream_group)
                        if self.on_quality_event:
                            self.on_quality_event(
                                "ERROR",
                                f"raw_capture_callback_failed:{type(exc).__name__}",
                                self.connection_id, self.stream_group)
                else:
                    if self.raw_capture_callback_degraded:
                        self.raw_capture_callback_degraded = False
                        logger.warning("raw_capture_callback_recovered",
                                       failures=self.raw_capture_callback_failures,
                                       stream_group=self.stream_group)

            item = IngestItem(
                msg=msg, local_receive_ts=local_receive_ts,
                connection_id=self.connection_id,
                connection_generation=self._connection_serial,
                data=data, decode_error=decode_error, is_control=is_control,
                receive_stamp=receive_stamp,
            )

            # Bounded queue, and never a dropped frame under normal
            # operation: silently discarding an exchange frame is
            # unacceptable for research-grade market data (unlike the
            # diagnostic quality-event queue elsewhere in this codebase,
            # which may drop under extreme load). A full queue means this
            # loop waits for the worker to make room -- real, visible
            # backpressure -- rather than losing data. The wait is polled
            # rather than a bare blocking put(), specifically so stop()
            # can still take effect promptly even while backpressured
            # (see the shutdown-abandonment branch below): the raw frame
            # is already durably captured by this point regardless of
            # which way this resolves, so responsiveness to shutdown no
            # longer trades off against raw-data safety the way it would
            # have before this correction.
            first_wait = True
            while True:
                try:
                    self._ingest_queue.put_nowait(item)
                    break
                except asyncio.QueueFull:
                    if first_wait:
                        self.queue_backpressure_events += 1
                        logger.warning("ingest_queue_backpressure",
                                        stream_group=self.stream_group,
                                        queue_maxsize=self.ingest_queue_maxsize)
                        if self.on_quality_event:
                            self.on_quality_event(
                                "BACKPRESSURE", f"ingest_queue_full:{self.ingest_queue_maxsize}",
                                self.connection_id, self.stream_group)
                        first_wait = False
                    if not self.running:
                        # Shutdown was requested while the queue stayed
                        # full. The frame's raw evidence already exists
                        # (on_raw_frame already ran above); only its
                        # on_message processing in *this* run is being
                        # abandoned, and that is counted and reported,
                        # never silent.
                        self.frames_abandoned_at_shutdown += 1
                        logger.error("ingest_enqueue_abandoned_at_shutdown",
                                     stream_group=self.stream_group)
                        if self.on_quality_event:
                            self.on_quality_event(
                                "ERROR", "ingest_enqueue_abandoned_at_shutdown",
                                self.connection_id, self.stream_group)
                        return
                    await asyncio.sleep(0.01)
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
        """The remaining per-frame work after receive-time decode and raw
        capture: classify control frames, report malformed frames, and
        call on_message. Decoding and raw capture already happened in
        _consume (see its docstring for why that moved back there); this
        method no longer repeats either.
        """
        if item.is_control:
            # Healthy protocol traffic. Counted, never reported as
            # malformed, and not forwarded to the market-data handler.
            self.control_frames_received += 1
            return

        if item.decode_error is not None:
            self.malformed_frames += 1
            logger.error("Failed to parse JSON from WebSocket", error=item.decode_error)
            if self.on_quality_event:
                self.on_quality_event(
                    "ERROR", f"malformed_frame:{item.decode_error}",
                    item.connection_id, self.stream_group,
                )
            return

        if self._on_message_takes_connection:
            await self.on_message(item.data, item.local_receive_ts, connection_id=item.connection_id)
        else:
            await self.on_message(item.data, item.local_receive_ts)

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
