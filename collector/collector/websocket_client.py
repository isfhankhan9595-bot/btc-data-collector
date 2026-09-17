import asyncio
import inspect
import json
import time
import websockets
from typing import Callable, Awaitable, Optional
from .backoff import BackoffExhausted, ExponentialBackoff
from .utils import logger

class WebSocketClient:
    def __init__(self, url: str, on_message: Callable[[dict], Awaitable[None]], on_reconnect: Callable[[], None] = None, on_quality_event: Optional[Callable[[str, str], None]] = None, stream_group: str = "websocket",
                 on_raw_frame: Optional[Callable[..., None]] = None,
                 backoff: Optional[ExponentialBackoff] = None):
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

                    async for msg in ws:
                        if not self.running:
                            break
                        # Capture arrival time before decoding so downstream
                        # research can distinguish network arrival from work
                        # performed after JSON parsing.
                        local_receive_ts = int(time.time() * 1000)
                        decode_error = None
                        data = None
                        try:
                            data = json.loads(msg)
                        except (json.JSONDecodeError, TypeError, ValueError) as exc:
                            decode_error = str(exc)

                        # Raw capture precedes every lossy step, including a
                        # failed decode: a frame that did not parse is still a
                        # frame that arrived and must remain observable.
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
                                )
                            except Exception as exc:  # noqa: BLE001 - capture fails open
                                logger.warning("raw_frame_capture_failed", error=str(exc))

                        if decode_error is not None:
                            self.malformed_frames += 1
                            logger.error("Failed to parse JSON from WebSocket", error=decode_error)
                            if self.on_quality_event:
                                # Previously log-only, so malformed frames were
                                # invisible in the data. Now durable.
                                self.on_quality_event(
                                    "ERROR", f"malformed_frame:{decode_error}",
                                    self.connection_id, self.stream_group,
                                )
                            continue

                        if self._on_message_takes_connection:
                            await self.on_message(data, local_receive_ts, connection_id=self.connection_id)
                        else:
                            await self.on_message(data, local_receive_ts)

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
