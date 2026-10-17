import asyncio
import inspect
import json
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, FrozenSet, Optional

import websockets

from .backoff import BackoffExhausted, ExponentialBackoff
from .clock import ReceiveStamp, capture_receive_stamp
from .storage_errors import FatalStorageError
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

class WorkerRetirementError(RuntimeError):
    """The ingest worker could not be retired within its bound.

    Deliberately NOT a ``FatalStorageError``: this is a lifecycle failure, not
    a storage-policy verdict. It exists so a worker that is still alive after
    shutdown surfaces as an explicit, terminal failure instead of the runner
    closing storage beneath it and reporting success.
    """


#: Returned by ``_next_frame`` when the receive loop must end (the iterator is
#: exhausted, or shutdown ingress was stopped while no frame was in hand).
_STREAM_END = object()


#: Shutdown-budget inputs (see ``WebSocketClient.shutdown_deadline_s``).
#: ``websockets`` closes a connection within its ``close_timeout``, which this
#: client does not override: the library default (10 s, pinned by a test) is the
#: bound. The opening handshake (``open_timeout``, also 10 s) is NOT part of the
#: post-stop budget because stop() cancels it (``_connect_or_stop``).
CLOSE_HANDSHAKE_S = 10.0
#: Allowance for the quality-event / subscribe work around the close handshake.
CONNECTION_PHASE_SLACK_S = 2.0
#: What the 60 s systemd TimeoutStopSec leaves for everything the code does NOT
#: bound (quality drain, ~10 writer finalizations incl. fsync, WAL close,
#: interpreter teardown). It is TimeoutStopSec minus the bounded websocket part
#: today (60 - 47 - 6): an allowance, NOT a measurement. Raising any client timeout
#: without raising TimeoutStopSec fails test_ws_shutdown_producers.
FINALIZATION_RESERVE_S = 7.0


class _ResumedAwaitable:
    """Continue a coroutine that was already stepped once by hand (``yield from`` delegation).

    Re-yields what the coroutine yielded (the future the event loop must wait on), then feeds
    its result -- or exception, cancellation included -- back in until it returns."""

    def __init__(self, coro, yielded):
        self._coro, self._yielded = coro, yielded

    def __await__(self):
        coro, to_yield = self._coro, self._yielded
        while True:
            try:
                sent = yield to_yield
            except GeneratorExit:
                coro.close()
                raise
            except BaseException as exc:  # noqa: BLE001 - forwarded into the coroutine verbatim
                try:
                    to_yield = coro.throw(exc)
                except StopIteration as stop:
                    return stop.value
            else:
                try:
                    to_yield = coro.send(sent)
                except StopIteration as stop:
                    return stop.value


def _start_receive_without_eager_tasks(coro, loop):
    """Python < 3.12 equivalent of ``Task(coro, eager_start=True)``.

    Steps ``coro`` once by hand: if it finishes without ever suspending (a frame is already
    available) the returned future is already done, so the receive loop does not yield to the
    event loop between buffered frames -- exactly what the plain ``async for`` it replaced did,
    and what lets a burst fill the bounded queue and register backpressure. If it suspends, the
    rest runs in an ordinary task. (``ensure_future`` instead costs one loop turn per frame,
    which changes that scheduling: the burst/backpressure tests fail on 3.11 with it.)"""
    try:
        yielded = coro.send(None)
    except StopIteration as done:
        future = loop.create_future()
        future.set_result(done.value)
        return future
    except Exception as exc:  # noqa: BLE001 - surfaced through the future, as a Task would
        future = loop.create_future()
        future.set_exception(exc)
        return future

    async def _rest():
        return await _ResumedAwaitable(coro, yielded)
    return loop.create_task(_rest())


async def _anext_or_end(iterator):
    """One ``__anext__`` of ``iterator``; ``_STREAM_END`` instead of raising
    ``StopAsyncIteration`` (which must not travel through a Task)."""
    try:
        return await iterator.__anext__()
    except StopAsyncIteration:
        return _STREAM_END


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
                 ingest_queue_maxsize: int = 2000,
                 on_fatal: Optional[Callable[[FatalStorageError, str], None]] = None):
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
        #: Upper bound on how long shutdown waits for the *cancelled* worker to
        #: actually finish. Distinct from the drain timeout: after the drain gives
        #: up (or is interrupted) the worker is cancelled, and a worker that still
        #: has not ended after this long is a terminal retirement failure.
        self.worker_retire_timeout_s = 5.0
        #: Queued-but-never-processed frames abandoned by an interrupted or
        #: timed-out drain, and the in-flight item (if any) a cancelled worker was
        #: holding. Both are INCLUDED in ``frames_abandoned_at_shutdown``; they are
        #: broken out so the accounting identity is checkable without
        #: double-counting a queued item and an in-flight one.
        self.frames_abandoned_queued = 0
        self.frames_abandoned_in_flight = 0
        #: True once a worker could not be retired (see WorkerRetirementError).
        self.worker_retirement_failed = False
        self.raw_capture_callback_failures = 0
        #: True from the first callback failure until the next success. One
        #: quality event per failure *streak*, not per frame: a callback that
        #: is broken fails on every frame, and a per-frame event would flood.
        self.raw_capture_callback_degraded = False

        # --- F5: typed fatal-storage boundary ---------------------------
        #: ``on_fatal(exc, origin)`` hands a ``FatalStorageError`` to the
        #: application's own classifier. ``origin`` is ``"raw_frame"`` (the
        #: on_raw_frame callback: the raw-evidence boundary, always terminal
        #: for this client), ``"worker"`` (on_message / _process_item) or
        #: ``"client"`` (the connection loop). It is called synchronously and
        #: must not block; it must not call back into this client's shutdown.
        #: With no hook there is nobody to classify the failure, so the client
        #: defaults to DENY: it stops ingesting and start() re-raises.
        self.on_fatal = on_fatal
        #: Typed fatals seen at any boundary of this client.
        self.fatal_storage_errors = 0
        #: The first typed fatal this client saw (never cleared, never
        #: overwritten): the client does not repair itself by reconnecting.
        self.fatal_error: Optional[FatalStorageError] = None
        #: Terminal "discard mode": no further ingestion or reconnect, and the
        #: worker only ``task_done()``s what is already queued, so the queue
        #: drains and ``join()`` completes without any frame being processed.
        self._discard_mode = False
        self.discard_reason: Optional[str] = None
        # --- shutdown lifecycle ------------------------------------------
        #: Latched by stop(): ingress is closed for good. start() on a stopped
        #: client runs no connection loop (a stop() that lands before start()'s
        #: first turn is therefore not lost).
        self._stop_requested = False
        #: Future completed by stop(); raced against every receive and every
        #: reconnect backoff so a quiet socket / a long sleep cannot hold
        #: shutdown hostage. Created lazily inside the running loop.
        self._stop_future: Optional[asyncio.Future] = None
        #: start() has been entered / has finished its shutdown tail (drain +
        #: worker retirement). ``wait_stopped`` is built on these.
        self._lifecycle_started = False
        self._lifecycle_finished: Optional[asyncio.Event] = None
        #: Queued frames dropped unprocessed in discard mode (their raw copy,
        #: if the raw boundary still held, already exists: see _consume).
        self.frames_discarded = 0

    @property
    def discard_mode(self) -> bool:
        return self._discard_mode

    def enter_discard_mode(self, reason: str) -> None:
        """Latch the terminal discard state. Idempotent; never un-latched.

        Stops ingestion and reconnection (``running`` goes False, so the
        receive loop exits at the next frame and ``start()`` does not
        reconnect) but deliberately does NOT cancel the worker or touch the
        queue: the worker keeps draining, counting each item as discarded and
        calling ``task_done()`` exactly once, so the producer can never block
        on a full queue and ``join()`` still completes.
        """
        if not self._discard_mode:
            self._discard_mode = True
            self.discard_reason = reason
            logger.error("websocket_discard_mode_entered",
                         stream_group=self.stream_group, reason=reason)
        self.running = False
        self.connected = False

    def _handle_fatal(self, exc: FatalStorageError, origin: str) -> None:
        """Route a typed fatal to the application; never raises.

        Called from ``except FatalStorageError`` branches that are placed
        BEFORE the generic ``except Exception`` ones, so a typed fatal can
        never be demoted to an ordinary processing error / reconnect.
        """
        self.fatal_storage_errors += 1
        if self.fatal_error is None:
            self.fatal_error = exc
        terminal = origin == "raw_frame"      # the raw-evidence boundary itself
        if self.on_fatal is None:
            terminal = True                   # nobody to classify it: default-deny
        else:
            try:
                self.on_fatal(exc, origin)
            except Exception as hook_exc:  # noqa: BLE001 - a broken classifier must fail CLOSED
                terminal = True
                logger.error("websocket_on_fatal_hook_failed", stream_group=self.stream_group,
                             error=f"{type(hook_exc).__name__}: {hook_exc}")
        logger.error("websocket_fatal_storage_error", stream_group=self.stream_group,
                     origin=origin, terminal=terminal, **exc.describe())
        if terminal:
            self.enter_discard_mode(f"fatal_storage_error:{origin}")

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
        """Run the connection loop, then ALWAYS run the shutdown tail.

        The tail (drain the ingest queue, retire the worker) used to run only
        when the receive loop ended by itself, so a cancelled start() -- which is
        what a runner's shutdown does -- skipped it and left the separate worker
        task alive and able to use storage the runner was about to close. It now
        runs in a ``finally``: on normal exit, on error, and on cancellation.
        """
        self._lifecycle_started = True
        if self._lifecycle_finished is None:
            self._lifecycle_finished = asyncio.Event()
        self._lifecycle_finished.clear()
        try:
            try:
                await self._run_connection_loop()
            finally:
                # STOP RECEIVING -> DRAIN PROCESSING QUEUE -> STOP WORKER -> EXIT.
                # The receive loop has exited (or been cancelled), so nothing more
                # can be enqueued and the drain is bounded, not a race against new
                # arrivals. A stuck worker (e.g. a wedged downstream write) cannot
                # hang shutdown: the drain times out (counted, logged, quality
                # event), the worker is cancelled, and retirement is itself
                # bounded -- a worker that survives that raises
                # WorkerRetirementError rather than being quietly orphaned.
                await self._drain_and_stop_worker()
        finally:
            self._lifecycle_finished.set()
        if self.fatal_error is not None and self.on_fatal is None:
            # No application classifier was wired: the failure must not
            # evaporate into a quietly-finished task.
            raise self.fatal_error

    async def _run_connection_loop(self):
        if self._stop_requested:
            # stop() landed before this loop's first turn: ingress is closed for
            # good, so there is nothing to connect to (only the tail runs).
            self.running = False
            self.connected = False
            logger.info("websocket_start_skipped_stop_requested", stream_group=self.stream_group)
        elif self._discard_mode:
            # Terminal latch (a failed-writer fatal): the client is never
            # revived by start(). ``running`` stays False so the loop below is
            # skipped and only the shutdown tail runs (nothing to drain, and
            # a latched fatal with no classifier is still raised). A fresh,
            # un-latched client takes the ordinary path.
            self.running = False
            self.connected = False
            logger.error("websocket_start_refused_terminal_latch",
                         stream_group=self.stream_group, reason=self.discard_reason)
        else:
            self.running = True
            logger.info("Starting WebSocket client", url=self.url)

        while self.running:
            try:
                connection = websockets.connect(self.url)
                if hasattr(connection, "__await__"):
                    # stop() must not wait out the opening handshake (open_timeout,
                    # 10 s): that is time shutdown_deadline_s does not budget.
                    connection = await self._connect_or_stop(connection)
                    if connection is _STREAM_END:
                        break

                async with connection as ws:
                    if self._discard_mode:
                        # The terminal latch landed while this connection was
                        # being established. A latched client is never revived
                        # by a reconnect: drop the socket before announcing it
                        # (no CONNECT quality event, no on_reconnect, no
                        # subscribe) and leave the loop without a backoff.
                        self.connected = False
                        logger.error("websocket_connection_dropped_terminal_latch",
                                     stream_group=self.stream_group, reason=self.discard_reason)
                        break
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
            except FatalStorageError as exc:
                # F5: must precede the generic handler below, which would
                # otherwise treat a fatal storage failure as a transient
                # connection error and reconnect around it.
                self.connected = False
                self._handle_fatal(exc, "client")
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
                await self._interruptible_sleep(delay)


    async def _drain_and_stop_worker(self) -> None:
        """Drain the ingest queue (bounded), then retire the worker.

        The worker is retired on EVERY exit from the drain -- normal completion,
        timeout, or a cancellation landing while the drain is waiting -- so a
        cancelled start() can never leave a live, unowned worker behind.
        Accounting: a drain that gives up (timeout or interruption) counts the
        frames still *queued* as abandoned; the single item the worker may be
        holding in flight is counted separately, by the worker itself, when its
        cancellation lands (see ``_process_queue``). ``qsize()`` never includes
        the in-flight item, so the two are never double-counted.
        """
        worker = self._worker_task
        if worker is None:
            return
        queued_counted = False
        try:
            try:
                await asyncio.wait_for(self._ingest_queue.join(), timeout=self.shutdown_drain_timeout_s)
            except asyncio.TimeoutError:
                # Frames still queued when the drain gives up will not reach
                # on_message in this run. Their raw copy already exists (raw
                # capture precedes the enqueue), so this is lost *processing*,
                # not lost raw evidence.
                queued_counted = True
                remaining = self._abandon_queued_frames()
                logger.error("ingest_queue_drain_timeout", remaining=remaining)
                if self.on_quality_event:
                    self.on_quality_event(
                        "ERROR", f"ingest_queue_drain_timeout:{remaining}",
                        self.connection_id, self.stream_group)
            except BaseException:
                # Cancelled (or otherwise interrupted) while draining: whatever
                # is still queued will not be processed by this run. Count it
                # here, once, then let the cancellation continue after the
                # worker has been retired in the ``finally`` below.
                queued_counted = True
                remaining = self._abandon_queued_frames()
                logger.error("ingest_queue_drain_interrupted", remaining=remaining)
                if self.on_quality_event:
                    self.on_quality_event(
                        "ERROR", f"ingest_queue_drain_interrupted:{remaining}",
                        self.connection_id, self.stream_group)
                raise
        finally:
            await self._retire_worker(worker)

    def _abandon_queued_frames(self) -> int:
        """Count the frames still sitting in the queue as abandoned (not the
        in-flight one: ``qsize()`` excludes an item the worker has dequeued)."""
        remaining = self._ingest_queue.qsize()
        self.frames_abandoned_queued += remaining
        self.frames_abandoned_at_shutdown += remaining
        return remaining

    async def _retire_worker(self, worker: "asyncio.Task") -> None:
        """Cancel ``worker`` and wait, bounded, until it has actually ended.

        The reference is cleared only once the worker is truly done. A worker
        that survives ``worker_retire_timeout_s`` is NOT forgotten: the reference
        is kept, ``worker_retirement_failed`` latches, and WorkerRetirementError
        is raised so no caller can mistake it for a clean shutdown. Cancellation
        of this coroutine itself propagates (it is never swallowed); the worker's
        own cancel has already been requested by then.
        """
        if not worker.done():
            worker.cancel()
        _, pending = await asyncio.wait({worker}, timeout=self.worker_retire_timeout_s)
        if pending:
            self.worker_retirement_failed = True
            logger.error("ingest_worker_retirement_failed",
                         stream_group=self.stream_group, timeout_s=self.worker_retire_timeout_s)
            if self.on_quality_event:
                self.on_quality_event(
                    "ERROR", "ingest_worker_retirement_failed",
                    self.connection_id, self.stream_group)
            raise WorkerRetirementError(
                f"ingest worker for {self.stream_group!r} still running "
                f"{self.worker_retire_timeout_s}s after cancellation")
        if not worker.cancelled() and worker.exception() is not None:
            # Observe it so it is never "exception was never retrieved".
            logger.error("ingest_worker_ended_with_error", stream_group=self.stream_group,
                         error=f"{type(worker.exception()).__name__}: {worker.exception()}")
        if self._worker_task is worker:
            self._worker_task = None

    def _stop_signal(self) -> "asyncio.Future":
        """The future stop() completes. Created lazily inside the running loop;
        already complete if stop() was requested before it existed."""
        future = self._stop_future
        if future is None or future.get_loop() is not asyncio.get_running_loop():
            future = asyncio.get_running_loop().create_future()
            self._stop_future = future
        if self._stop_requested and not future.done():
            future.set_result(None)
        return future

    async def _next_frame(self, iterator):
        """Next frame from ``iterator``, or ``_STREAM_END``.

        Races the receive against stop(). If stop wins while no frame is in hand,
        the pending receive is cancelled (cancelling ``recv`` is safe in
        ``websockets``: it cannot lose a message) and awaited, so no receive task
        outlives the call. If a frame and the stop complete together the frame is
        returned -- the caller's own ``running`` check decides its fate, exactly
        as before.

        Scheduling is deliberately unchanged for the common case: the receive is
        started eagerly (Python >= 3.12), so a frame that is already available
        completes without suspending and the loop does not yield to the event
        loop between buffered frames -- the same behaviour as the plain
        ``async for`` this replaced, which is what lets a burst fill the bounded
        queue and register backpressure. On older interpreters (3.11) the same is achieved by
        stepping the receive once by hand (``_start_receive_without_eager_tasks``).
        """
        if self._stop_requested:
            return _STREAM_END
        coro = _anext_or_end(iterator)
        loop = asyncio.get_running_loop()
        try:
            receive = asyncio.Task(coro, loop=loop, eager_start=True)
        except TypeError:                      # Python < 3.12: no eager tasks
            receive = _start_receive_without_eager_tasks(coro, loop)
        if not receive.done():
            stop = self._stop_signal()
            try:
                await asyncio.wait({receive, stop}, return_when=asyncio.FIRST_COMPLETED)
            except BaseException:
                receive.cancel()
                await asyncio.gather(receive, return_exceptions=True)
                raise
        if receive.done():
            return receive.result()
        receive.cancel()
        await asyncio.gather(receive, return_exceptions=True)
        return _STREAM_END

    async def _connect_or_stop(self, pending):
        """Await the opening handshake ``pending`` unless stop() lands first.

        Returns the connection, or ``_STREAM_END`` when stop won (the handshake is
        cancelled -- ``websockets`` aborts the transport on cancellation -- and
        awaited, so nothing outlives the call). A connect error propagates exactly
        as the plain ``await`` did. A connection that completed together with stop
        is returned: the caller's ``async with`` closes it and its receive loop
        consumes nothing because ingress is already stopped.
        """
        if self._stop_requested:
            return _STREAM_END
        attempt = asyncio.ensure_future(pending)
        stop = self._stop_signal()
        try:
            await asyncio.wait({attempt, stop}, return_when=asyncio.FIRST_COMPLETED)
        except BaseException:
            attempt.cancel()
            await asyncio.gather(attempt, return_exceptions=True)
            raise
        if attempt.done():
            return attempt.result()
        attempt.cancel()
        await asyncio.gather(attempt, return_exceptions=True)
        return _STREAM_END

    async def _interruptible_sleep(self, delay: float) -> None:
        """``asyncio.sleep(delay)`` that stop() can cut short (reconnect backoff
        can be up to a minute; shutdown must not wait it out)."""
        if self._stop_requested:
            return
        stop = self._stop_signal()
        sleeper = asyncio.ensure_future(asyncio.sleep(delay))
        try:
            await asyncio.wait({sleeper, stop}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            if not sleeper.done():
                sleeper.cancel()
                await asyncio.gather(sleeper, return_exceptions=True)

    def live_lifecycle_tasks(self) -> list:
        """Tasks that keep ``lifecycle_idle`` False without being the ``start()`` task itself: the ingest
        worker while it is alive. A worker that survived its bounded retirement outlives ``start()``, so
        its end is the only event that makes the client idle again; a caller that must act then (the
        runner's late finalization) registers a done-callback on it. Empty when nothing is alive."""
        worker = self._worker_task
        return [worker] if worker is not None and not worker.done() else []

    @property
    def worker_active(self) -> bool:
        """True while an ingest worker task exists and has not ended."""
        worker = self._worker_task
        return worker is not None and not worker.done()

    @property
    def ingress_stopped(self) -> bool:
        """True when the receive loop can no longer call ``on_raw_frame`` -- the only
        producer of the raw-wire writer. Independent of the worker and of whether
        start() has finished: once stop() has returned (or start()'s lifecycle has
        ended) no frame can be captured, because ``_consume`` has no ``await``
        between its ``running`` check and the capture."""
        if self._stop_requested:
            return True
        return bool(self._lifecycle_started and self._lifecycle_finished is not None
                    and self._lifecycle_finished.is_set())

    @property
    def lifecycle_idle(self) -> bool:
        """True when nothing of this client can still touch storage: start() is
        not mid-lifecycle (never entered, or its tail has finished) and no ingest
        worker is alive. Synchronous, so a non-async ``shutdown()`` can gate
        writer closes on it."""
        if self.worker_active:
            return False
        if self._lifecycle_started and self._lifecycle_finished is not None:
            return self._lifecycle_finished.is_set()
        return True

    @property
    def shutdown_deadline_s(self) -> float:
        """Worst-case time start() needs after stop(): the connection's close
        handshake (websockets ``close_timeout``, 10 s) plus slack, the bounded
        drain and the bounded worker retirement. The opening handshake is not
        included because stop() cancels it. Runners use it as their wait bound."""
        return (CLOSE_HANDSHAKE_S + CONNECTION_PHASE_SLACK_S
                + self.shutdown_drain_timeout_s + self.worker_retire_timeout_s)

    async def wait_stopped(self, timeout_s: float) -> bool:
        """Wait (bounded) for this client's whole lifecycle to finish.

        True when start() was never entered, or has completed its tail (drain +
        worker retirement) -- and no worker is still alive. False on timeout, or
        if a worker survived. Never raises on timeout and never cancels anything:
        the caller decides what a False means.
        """
        if self._lifecycle_started and self._lifecycle_finished is not None:
            try:
                await asyncio.wait_for(self._lifecycle_finished.wait(), timeout=timeout_s)
            except asyncio.TimeoutError:
                return False
        return not self.worker_active

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
        iterator = ws.__aiter__()
        while True:
            # Race every receive against stop(): a quiet or blocked socket must
            # not hold shutdown until the exchange happens to send another frame.
            msg = await self._next_frame(iterator)
            if msg is _STREAM_END:
                break
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
                except FatalStorageError as exc:
                    # F5: the raw-evidence boundary is gone. Placed before the
                    # fail-open handler on purpose. Raw evidence is
                    # irrecoverable, so continuing to enqueue frames that were
                    # NOT durably captured is exactly what this must prevent.
                    self._handle_fatal(exc, "raw_frame")
                    return
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
                    await asyncio.sleep(0.01)
                    if not self.running:
                        # Shutdown (or the terminal discard latch) was
                        # requested while the queue stayed full. The frame's
                        # raw evidence already exists (on_raw_frame already
                        # ran above); only its on_message processing in
                        # *this* run is being abandoned, and that is counted
                        # and reported, never silent.
                        #
                        # Checked AFTER the wait, immediately before the next
                        # put_nowait (no await in between): the worker leaves
                        # its loop once ``running`` is False and the queue is
                        # empty, so a put that landed after that point would
                        # strand one item behind a dead worker and make
                        # join() wait out the whole drain timeout.
                        self.frames_abandoned_at_shutdown += 1
                        logger.error("ingest_enqueue_abandoned_at_shutdown",
                                     stream_group=self.stream_group)
                        if self.on_quality_event:
                            self.on_quality_event(
                                "ERROR", "ingest_enqueue_abandoned_at_shutdown",
                                self.connection_id, self.stream_group)
                        return
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
                if self._discard_mode:
                    # Terminal: account for the item and fall through to
                    # task_done() in ``finally``; never process it.
                    self.frames_discarded += 1
                else:
                    await self._process_item(item)
                    self.frames_processed += 1
            except FatalStorageError as exc:
                # F5: placed BEFORE the generic isolate-one-bad-item handler
                # so a typed fatal is classified by the application and can
                # never be counted as an ordinary processing error.
                self._handle_fatal(exc, "worker")
            except asyncio.CancelledError:
                # Retirement cancelled the worker while this item was in flight.
                # It is neither processed nor a processing failure: it is work
                # intentionally abandoned at shutdown, counted exactly once
                # (``task_done()`` still runs once, in ``finally``).
                self.frames_abandoned_in_flight += 1
                self.frames_abandoned_at_shutdown += 1
                logger.error("ingest_item_abandoned_in_flight", stream_group=self.stream_group)
                if self.on_quality_event:
                    self.on_quality_event(
                        "ERROR", "ingest_item_abandoned_in_flight",
                        item.connection_id, self.stream_group)
                raise
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
        """Close ingress for good and wake anything waiting on the network.

        Synchronous and idempotent (runners call it as a plain method). Setting
        ``running = False`` alone does not wake a receive loop parked on a quiet
        socket or a reconnect backoff sleep, so this also completes the stop
        future those waits are raced against.
        """
        self._stop_requested = True
        self.running = False
        self.connected = False
        future = self._stop_future
        if future is not None and not future.done():
            try:
                future.set_result(None)
            except RuntimeError:   # its loop is already closed: nothing left to wake
                pass
