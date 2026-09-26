import asyncio
import signal
import time
import json
import os
from decimal import Decimal
from urllib.parse import parse_qs, urlparse
from dataclasses import replace
from collector.collector.utils import logger, send_telegram_alert, validate_telegram_startup
from collector.collector.config import (
    BINANCE_MARKET_WS_URL,
    BINANCE_PUBLIC_WS_URL,
    ORDERBOOK_SCHEMA,
    TRADES_SCHEMA,
    MARKPRICE_SCHEMA,
    OPENINTEREST_SCHEMA,
    LIQUIDATION_SCHEMA,
    QUALITY_EVENTS_SCHEMA,
    BINANCE_ORDERBOOK_RAW_SCHEMA,
    BINANCE_TRADES_RAW_SCHEMA,
    RAW_REST_SCHEMA,
    RAW_WIRE_SCHEMA,
    SYMBOL,
)
from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.instrument import BINANCE_USDM_BTCUSDT
from collector.collector.raw_capture import RawCapture, RawRestRecord, RawWireRecord
from collector.collector.backoff import ExponentialBackoff, rate_limit_penalty
from collector.collector.recovery_control import RecoveryController
from collector.collector.binance_oi import normalize_binance_oi, BinanceOIParseError
from collector.collector.book_engine import LocalBook
from collector.collector.canonical import CanonicalOrderBookEvent
from collector.collector.quality_events import BookQuality, QualityEvent, QualityEventType
from collector.collector.feature_computer import (
    compute_liquidation_features,
    compute_markprice_features,
    compute_openinterest_features,
    compute_orderbook_features,
    compute_trades_features,
)
from collector.collector.validator import Validator
from collector.collector.gap_detector import GapDetector
from collector.collector.disk_monitor import DiskMonitor
from collector.collector.health_monitor import HealthMonitor
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.websocket_client import WebSocketClient

STREAM_INACTIVE_STARTUP_SECONDS = 60
RAW_LOG_LIMIT = 20
OI_POLL_INTERVAL_S = 3.0
OI_URL = "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"
BINANCE_DEPTH_SNAPSHOT_URL = "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000"

class CollectorApp:
    def __init__(self):
        self.running = False
        self._closed = False
        self.raw_messages_logged = 0
        self.stream_counters = {
            "orderbook": {"received": 0, "computed": 0, "empty_features": 0, "validated": 0, "rejected": 0, "written": 0},
            "trades": {"received": 0, "computed": 0, "empty_features": 0, "validated": 0, "rejected": 0, "written": 0},
            "markprice": {"received": 0, "computed": 0, "empty_features": 0, "validated": 0, "rejected": 0, "written": 0},
            "openinterest": {"received": 0, "computed": 0, "empty_features": 0, "validated": 0, "rejected": 0, "written": 0},
            "liquidation": {"received": 0, "computed": 0, "empty_features": 0, "validated": 0, "rejected": 0, "written": 0},
            "unrouted": {"received": 0},
            "malformed_envelope": {"received": 0},
            "adapter_unhandled": {"received": 0},
        }
        self.validation_fail_reasons = {"orderbook": {}, "trades": {}, "markprice": {}, "openinterest": {}, "liquidation": {}}

        self.disk_monitor = DiskMonitor(shutdown_callback=self.shutdown)
        self.disk_monitor.check_disk_space()

        self.validator = Validator()
        self.gap_detector = GapDetector()
        self.binance_adapter = BinanceAdapter()
        self.binance_book = LocalBook("BINANCE")
        self._book_snapshot_lock = asyncio.Lock()
        self._recovery_task = None
        # One gap must not become hundreds of REST snapshot requests.
        self.recovery_controller = RecoveryController(
            name="binance_orderbook", min_interval_s=1.0, max_per_window=5,
            window_s=60.0,
            backoff=ExponentialBackoff(base_delay=1.0, max_delay=60.0, max_attempts=10),
            quality_sink=self._persist_quality_event)
        self._quality_queue = asyncio.Queue(maxsize=1024)
        self._quality_task = None
        self._quality_overflow = 0

        self.quality_writer = ParquetWriter("quality_events", QUALITY_EVENTS_SCHEMA, segment_rows=1, segment_seconds=1)
        self._quality_journal_path = self.quality_writer.stream_dir / "quality_queue.pending.json"
        if self._quality_journal_path.exists():
            self._persist_quality_event({"stream":"quality_events", "event_type":QualityEventType.DATA_DROP,
                "reason":"quality_queue_unfinished_on_previous_process", "rows_lost":None})
            self._quality_journal_path.unlink(missing_ok=True)
        self.ob_writer = ParquetWriter("orderbook", ORDERBOOK_SCHEMA, quality_event_sink=self._persist_quality_event)
        self.raw_book_writer = ParquetWriter("binance_orderbook_raw", BINANCE_ORDERBOOK_RAW_SCHEMA, quality_event_sink=self._persist_quality_event)
        self.trades_writer = ParquetWriter("trades", TRADES_SCHEMA)
        self.raw_trades_writer = ParquetWriter("binance_trades_raw", BINANCE_TRADES_RAW_SCHEMA)
        self.mark_writer = ParquetWriter("markprice", MARKPRICE_SCHEMA)
        self.oi_writer = ParquetWriter("openinterest", OPENINTEREST_SCHEMA)
        self.liq_writer = ParquetWriter("liquidation", LIQUIDATION_SCHEMA)
        self.raw_wire_writer = ParquetWriter("raw_wire", RAW_WIRE_SCHEMA, quality_event_sink=self._persist_quality_event)
        self.raw_rest_writer = ParquetWriter("raw_rest", RAW_REST_SCHEMA, quality_event_sink=self._persist_quality_event)
        self.raw_capture = RawCapture(self.raw_wire_writer, self.raw_rest_writer,
                                      quality_event_sink=self._persist_quality_event)
        # Adapter drops become durable quality events instead of vanishing.
        self.binance_adapter.set_unhandled_sink(self._record_adapter_unhandled)

        self.ws_clients = [
            WebSocketClient(
                url=BINANCE_PUBLIC_WS_URL,
                on_message=self.handle_message,
                on_reconnect=self._make_reconnect_handler(BINANCE_PUBLIC_WS_URL),
                on_quality_event=self._websocket_quality_event, stream_group="public",
                on_raw_frame=self._capture_raw_frame
            ),
            WebSocketClient(
                url=BINANCE_MARKET_WS_URL,
                on_message=self.handle_message,
                on_reconnect=self._make_reconnect_handler(BINANCE_MARKET_WS_URL),
                on_quality_event=self._websocket_quality_event, stream_group="market",
                on_raw_frame=self._capture_raw_frame
            ),
        ]

        self.health_monitor = HealthMonitor(self.disk_monitor, self.validator, self)
        self.tasks = []

    @property
    def connected(self):
        return all(client.connected for client in self.ws_clients)

    def _requested_streams(self, url: str):
        parsed = urlparse(url)
        streams = parse_qs(parsed.query).get("streams", [""])[0]
        return [stream for stream in streams.split("/") if stream]

    def _route_stream(self, stream: str):
        normalized_stream = stream.lower()
        symbol_prefix = f"{SYMBOL.lower()}@"
        if not normalized_stream.startswith(symbol_prefix):
            return None
        if "@depth" in normalized_stream:
            return "orderbook"
        if "@aggtrade" in normalized_stream:
            return "trades"
        if "@markprice" in normalized_stream:
            return "markprice"
        if "@forceorder" in normalized_stream:
            return "liquidation"
        return None

    def _log_raw_sample(self, stream: str, msg: dict):
        if self.raw_messages_logged >= RAW_LOG_LIMIT:
            return
        logger.info(f"RAW_STREAM={stream}")
        logger.info(f"RAW_MESSAGE={msg}")
        self.raw_messages_logged += 1

    def _capture_raw_frame(self, frame, *, local_receive_ts, connection_id=None,
                           connection_generation=None, decode_ok=True,
                           decode_error=None, parsed=None, control_frame=False):
        """Persist the exact frame before any lossy transformation.

        Venue-native identifiers are copied verbatim when the frame decoded;
        they are never derived, and absence stays null.

        ``control_frame`` exists because ``WebSocketClient`` now calls every
        ``on_raw_frame`` callback with it (added for OKX's non-JSON ``pong``
        heartbeat). Binance has no control frames, so this always arrives
        ``False`` here and nothing below reads it -- but the parameter must
        exist. Regression: without it, every call raised ``TypeError``
        inside the client's fail-open ``try/except``, so it never propagated
        as an error -- it just meant zero frames were captured, silently,
        for the life of the process. No test caught this because the only
        integration-level raw-frame test used a ``**kwargs`` double, which is
        strictly more permissive than this method's real signature.
        """
        capture = getattr(self, "raw_capture", None)
        if capture is None:
            return
        stream = channel = None
        exchange_event_ts = update_id = first_update_id = previous_update_id = None
        if isinstance(parsed, dict):
            stream = parsed.get("stream")
            channel = self._route_stream(stream) if isinstance(stream, str) else None
            payload = parsed.get("data")
            if isinstance(payload, dict):
                exchange_event_ts = payload.get("E")
                update_id = payload.get("u")
                first_update_id = payload.get("U")
                previous_update_id = payload.get("pu")
        capture.capture_wire(RawWireRecord(
            local_receive_ts=local_receive_ts,
            payload=frame if isinstance(frame, str) else str(frame),
            venue="BINANCE", connection_id=connection_id,
            connection_generation=connection_generation,
            channel=channel, stream=stream, symbol=SYMBOL,
            decode_ok=decode_ok, decode_error=decode_error,
            exchange_event_ts=exchange_event_ts, update_id=update_id,
            first_update_id=first_update_id, previous_update_id=previous_update_id,
            local_capture_ts=int(time.time() * 1000),
        ))

    def _record_adapter_unhandled(self, message):
        """An adapter could not turn a message into events. Make it durable."""
        self.stream_counters["adapter_unhandled"]["received"] += 1
        self._persist_quality_event(message.to_quality_event())

    def _capture_rest(self, record):
        capture = getattr(self, "raw_capture", None)
        if capture is not None:
            capture.capture_rest(record)

    def _record_validation_rejection(self, stream_name: str, reason: str):
        reasons = self.validation_fail_reasons.setdefault(stream_name, {})
        reasons[reason] = reasons.get(reason, 0) + 1

    def _drain_integrity_quality_events(self):
        """Move validator/book integrity events into the durable quality stream.

        Validation and reconstruction create events in bounded-in-scope in-memory
        queues so the hot path never performs parquet I/O. The collector drains
        them immediately after processing each event and uses the existing quality
        writer, preserving event metadata such as drift and rows_lost.
        """
        for source in (getattr(self, "binance_book", None), getattr(self, "validator", None)):
            drain = getattr(source, "drain_quality_events", None)
            if drain is None:
                continue
            for event in drain():
                if isinstance(event, QualityEvent):
                    self._persist_quality_event(event.record())
                elif isinstance(event, dict):
                    self._persist_quality_event(event)

    async def handle_message(self, msg: dict, local_receive_ts: int | None = None,
                             connection_id: str | None = None):
        if local_receive_ts is None:
            local_receive_ts = int(time.time() * 1000)
        if not isinstance(msg, dict) or "stream" not in msg or "data" not in msg:
            # Previously a bare `return`: subscription acks, error envelopes and
            # any unexpected shape vanished with no counter and no record. A
            # frame that does not match the combined-stream envelope is still
            # information, and its absence must not look like silence.
            self.stream_counters["malformed_envelope"]["received"] += 1
            keys = sorted(str(k) for k in msg.keys()) if isinstance(msg, dict) else []
            self._persist_quality_event({
                "stream": "unrouted", "event_type": QualityEventType.DATA_DROP,
                "reason": f"non_envelope_frame:keys={','.join(keys) or type(msg).__name__}",
                "rows_lost": 1, "connection_id": connection_id,
                "local_receive_ts": local_receive_ts, "local_ts": local_receive_ts,
            })
            return
        stream = msg["stream"]
        data = msg["data"]
        self._log_raw_sample(stream, msg)
        route = self._route_stream(stream)
        if route is None:
            self.stream_counters["unrouted"]["received"] += 1
            logger.warning("Unrouted stream message", stream=stream)
            # Durable, not log-only: an unrouted stream is data the collector
            # received and chose not to process.
            self._persist_quality_event({
                "stream": "unrouted", "event_type": QualityEventType.DATA_DROP,
                "reason": f"unrouted_stream:{stream}", "rows_lost": 1,
                "connection_id": connection_id,
                "local_receive_ts": local_receive_ts, "local_ts": local_receive_ts,
            })
        elif route == "orderbook":
            await self._handle_binance_orderbook(msg, local_receive_ts)
        elif route == "trades":
            self._handle_binance_trade(msg, stream, local_receive_ts)
        elif route == "markprice":
            self._handle_markprice(data, stream, local_receive_ts)
        elif route == "liquidation":
            self._handle_liquidation(data, stream)
        self._drain_integrity_quality_events()
        if self.validator.check_failure_rate():
            logger.error("Validation spike detected: >0.1% failures in 60s window")
            send_telegram_alert("Validation spike detected: >0.1% failures in 60s window")

    def _websocket_quality_event(self, event_type, reason, connection_id=None, stream_group="websocket"):
        """Websocket hot path: bounded non-blocking enqueue only, never parquet I/O."""
        event={"exchange":"BINANCE", "stream":stream_group, "event_type":event_type, "reason":reason,
               "connection_id":connection_id, "local_ts":int(time.time()*1000)}
        if not hasattr(self, "_quality_queue"):
            self._persist_quality_event(event)
            return
        try:
            self._quality_queue.put_nowait(event)
            self._write_quality_pending_marker()
        except asyncio.QueueFull:
            self._quality_overflow += 1
            logger.error("quality_event_queue_overflow", dropped=self._quality_overflow)

    def _write_quality_pending_marker(self):
        path = self._quality_journal_path
        temporary = path.with_suffix(".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump({"pending": self._quality_queue.qsize(), "updated_ms": int(time.time() * 1000)}, handle)
            handle.flush(); os.fsync(handle.fileno())
        os.replace(temporary, path)

    async def _quality_persistence_loop(self):
        while self.running or not self._quality_queue.empty():
            try:
                event=await asyncio.wait_for(self._quality_queue.get(), timeout=0.1)
            except asyncio.TimeoutError:
                continue
            self._persist_quality_event(event)
            self._quality_queue.task_done()
            if self._quality_queue.empty():
                self._quality_journal_path.unlink(missing_ok=True)
        if self._quality_overflow:
            self._persist_quality_event({"stream":"quality_events", "event_type":QualityEventType.DATA_DROP,
                "reason":"quality_queue_overflow", "rows_lost":self._quality_overflow})

    def _persist_quality_event(self, event: dict):
        """Persist versioned lineage. Missing values remain null rather than fabricated."""
        rows_lost = event.get("rows_lost"); event_type = event.get("event_type", QualityEventType.ERROR.value)
        if isinstance(event_type, QualityEventType): event_type = event_type.value
        local_ts = event.get("local_ts", int(time.time() * 1000))
        self.quality_writer.write({"timestamp": local_ts, "exchange": event.get("exchange", "BINANCE"),
            "stream": event.get("stream", "orderbook"), "event_type": event_type, "reason": event.get("reason", ""),
            "gap_size_ms": event.get("gap_size_ms"), "rows_lost": None if rows_lost is None else str(rows_lost),
            "quality_state": event.get("new_state", event.get("quality_state", self.binance_book.state.state.value)),
            "connection_id": event.get("connection_id"), "previous_state": event.get("previous_state"),
            "new_state": event.get("new_state"), "expected_previous_update_id": event.get("expected_previous_update_id"),
            "actual_previous_update_id": event.get("actual_previous_update_id"), "update_id": event.get("update_id"), "first_update_id": event.get("first_update_id"),
            "previous_update_id": event.get("previous_update_id"),
            "local_receive_ts": event.get("local_receive_ts"), "local_process_ts": event.get("local_process_ts", local_ts)})

    def _record_book_quality(self, kind, reason, transition=None, event=None):
        transition=transition or self.binance_book.last_transition
        event=event or getattr(transition, "event", None)
        self._persist_quality_event({"exchange":"BINANCE", "stream":"orderbook", "event_type":kind, "reason":reason,
            "local_ts":int(time.time()*1000), "previous_state":getattr(getattr(transition,"previous_state",None),"value",None),
            "new_state":getattr(getattr(transition,"new_state",None),"value",None),
            "expected_previous_update_id":getattr(transition,"expected_previous_update_id",None),
            "actual_previous_update_id":getattr(event,"previous_update_id",None), "update_id":getattr(event,"update_id",None),
            "first_update_id":getattr(event,"first_update_id",None), "previous_update_id":getattr(event,"previous_update_id",None),
            "local_receive_ts":getattr(event,"local_receive_ts",None), "local_process_ts":getattr(event,"local_process_ts",None)})

    def _schedule_recovery(self, reason: str) -> bool:
        """Start a recovery only if the controller permits it.

        Suppressed requests are counted by the controller and surfaced as
        transition events, not one event per suppressed gap.
        """
        controller = getattr(self, "recovery_controller", None)
        if controller is None:
            if self._recovery_task is None or self._recovery_task.done():
                self._recovery_task = asyncio.create_task(
                    self._recover_binance_book(reason))
                return True
            return False
        # Ask the controller first. Checking the task handle first would
        # short-circuit before the suppression could be counted, so a gap
        # burst would be silently invisible in the counters -- the very
        # thing this controller exists to surface.
        if not controller.request(reason).allowed:
            return False
        if self._recovery_task is not None and not self._recovery_task.done():
            return False
        self._recovery_task = asyncio.create_task(self._recover_binance_book(reason))
        # Mark in flight synchronously: between create_task and the
        # coroutine's first line there is a window in which further gaps
        # would otherwise be allowed through.
        if not controller.in_flight:
            controller.begin()
        return True

    async def _recover_binance_book(self, reason: str):
        async with self._book_snapshot_lock:
            self.binance_book.state.resync()
            self._record_book_quality(QualityEventType.RESYNC, reason)
        request_ts=int(time.time()*1000); status=None; body=None
        controller=getattr(self, "recovery_controller", None)
        if controller is not None and not controller.in_flight:
            controller.begin()
        try:
            import aiohttp
            timeout=aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(BINANCE_DEPTH_SNAPSHOT_URL) as response:
                    status=response.status
                    # Read the body before raise_for_status so an error
                    # response is captured rather than discarded.
                    body=await response.text()
                    receive_ts=int(time.time()*1000)
                    penalty=rate_limit_penalty(status, getattr(response, "headers", None))
                    if penalty is not None:
                        # The venue's Retry-After overrides local pacing.
                        if controller is not None:
                            controller.note_rate_limit(penalty)
                        self._capture_rest(RawRestRecord(
                            request_ts=request_ts, response_receive_ts=receive_ts,
                            endpoint=BINANCE_DEPTH_SNAPSHOT_URL, purpose="orderbook_snapshot",
                            request_params={"symbol": SYMBOL, "limit": 1000},
                            http_status=status, ok=False,
                            error=f"rate_limited:{penalty.status}:{penalty.source}",
                            payload=body, symbol=SYMBOL))
                        async with self._book_snapshot_lock:
                            self.binance_book.state.gap()
                            self._record_book_quality(QualityEventType.RATE_LIMIT,
                                                      f"snapshot_rate_limited:{penalty.status}")
                        self._drain_integrity_quality_events()
                        return False
                    response.raise_for_status()
                    snapshot=json.loads(body)
            process_ts=int(time.time()*1000)
            # Record the exact snapshot so deterministic replay can bridge
            # from recorded data instead of contacting the live exchange.
            self._capture_rest(RawRestRecord(
                request_ts=request_ts, response_receive_ts=receive_ts,
                endpoint=BINANCE_DEPTH_SNAPSHOT_URL, purpose="orderbook_snapshot",
                request_params={"symbol": SYMBOL, "limit": 1000},
                http_status=status, ok=True, payload=body, symbol=SYMBOL,
                local_process_ts=process_ts))
            if not isinstance(snapshot, dict) or "lastUpdateId" not in snapshot: raise ValueError("missing_last_update_id")
            if not snapshot.get("bids") or not snapshot.get("asks"): raise ValueError("empty_snapshot")
            snapshot_event=self.binance_adapter.snapshot_event(
                int(snapshot["lastUpdateId"]),
                tuple((Decimal(p),Decimal(q)) for p,q in snapshot["bids"]),
                tuple((Decimal(p),Decimal(q)) for p,q in snapshot["asks"]),
                local_receive_ts=receive_ts, local_process_ts=process_ts)
            async with self._book_snapshot_lock:
                if not self.binance_book.binance_snapshot(snapshot_event.update_id,snapshot_event):
                    why=self.binance_book.last_reason
                    if why == "snapshot_ahead_of_buffer":
                        # The REST call succeeded and the snapshot is valid and
                        # retained; it simply landed ahead of every buffered
                        # diff, which is the common case on a cold start with
                        # an empty buffer. The next diff bridges it with no
                        # further REST call, so counting this as a failure
                        # would escalate backoff toward exhaustion during
                        # entirely normal operation.
                        self._record_book_quality(QualityEventType.RECOVERY, why)
                        if controller is not None: controller.succeed()
                        return True
                    self._record_book_quality(QualityEventType.ERROR, why)
                    if controller is not None: controller.fail(why)
                    return False
                self._persist_reconstructed_books(self.binance_book.committed_recovery_events)
                self.binance_book.committed_recovery_events=[]
                self._record_book_quality(QualityEventType.RECOVERY,"snapshot_bridge_completed")
                self._drain_integrity_quality_events()
                if controller is not None: controller.succeed()
                return True
        except asyncio.TimeoutError:
            why="snapshot_timeout"
        except ValueError as exc:
            why=str(exc)
        except Exception as exc:
            why="snapshot_http_error"
            logger.error("binance_book_snapshot_failed", error=str(exc))
        # A failed snapshot is lineage too: replay must be able to see that
        # recovery was attempted and why it did not produce a bridge.
        self._capture_rest(RawRestRecord(
            request_ts=request_ts, response_receive_ts=None,
            endpoint=BINANCE_DEPTH_SNAPSHOT_URL, purpose="orderbook_snapshot",
            request_params={"symbol": SYMBOL, "limit": 1000},
            http_status=status, ok=False, error=why, payload=body, symbol=SYMBOL))
        if controller is not None:
            controller.fail(why)
        async with self._book_snapshot_lock:
            self.binance_book.state.gap()
            self._record_book_quality(QualityEventType.ERROR,why)
        self._drain_integrity_quality_events()
        return False

    async def _handle_binance_orderbook(self, raw: dict, local_receive_ts: int):
        self.stream_counters["orderbook"]["received"] += 1
        if getattr(self, "_book_snapshot_lock", None) is None: self._book_snapshot_lock = asyncio.Lock()
        if not hasattr(self, "_recovery_task"): self._recovery_task = None
        for event in self.binance_adapter.normalize(raw, local_receive_ts=local_receive_ts):
            if event.book_source != "DIFF_DEPTH_RECONSTRUCTED":
                self._record_book_quality(QualityEventType.BOOK_INVALID,"partial_depth_not_authoritative"); return
            event=replace(event,local_process_ts=int(time.time()*1000))
            async with self._book_snapshot_lock:
                before=self.binance_book.state.state; applied=self.binance_book.apply(event); after=self.binance_book.state.state
                if after == BookQuality.SEQUENCE_GAP and before != after:
                    self._record_book_quality(QualityEventType.SEQUENCE_GAP,self.binance_book.last_reason,event=event)
                needs_recovery=applied is None and after in (BookQuality.SEQUENCE_GAP, BookQuality.RECOVERING)
                bridged=[]
                if needs_recovery and self.binance_book.retry_pending_snapshot():
                    # A snapshot that had landed ahead of the buffer has now
                    # been straddled by this diff. The bridge is proven from
                    # already-recorded data, so no REST slot is consumed.
                    bridged=self.binance_book.committed_recovery_events
                    self.binance_book.committed_recovery_events=[]
                    needs_recovery=False
                    self._record_book_quality(QualityEventType.RECOVERY,"pending_snapshot_bridge_completed",event=event)
                if self.binance_book.duplicate_count:
                    self._record_book_quality(QualityEventType.DUPLICATE,"binance_duplicate_update",event=event); self.binance_book.duplicate_count=0
            if bridged:
                self._persist_reconstructed_books(bridged)
            self._drain_integrity_quality_events()
            if needs_recovery:
                self._schedule_recovery("sequence_gap_or_initial_snapshot")
                return
            if applied is None: return
            self._persist_reconstructed_books([(applied, "NORMAL_INCREMENTAL", self.binance_book.recovery_generation)])
            data={"E":applied.exchange_event_ts,"b":[[str(p),str(q)] for p,q in applied.bids],"a":[[str(p),str(q)] for p,q in applied.asks]}
            features=compute_orderbook_features(data)
            if not features: self.stream_counters["orderbook"]["empty_features"] += 1; return
            features["timestamp"]=applied.local_process_ts; features["local_timestamp"]=applied.local_receive_ts; features["exchange_timestamp"]=applied.exchange_event_ts
            # Same per-event stamp as raw_book_writer's row above -- not re-derived,
            # just carried from the same `applied` event into this sibling writer.
            features["instrument_key"]=applied.instrument.key if applied.instrument is not None else None
            self._write_orderbook_features(features)

    def _persist_reconstructed_books(self, rows):
        raw_writer=getattr(self, "raw_book_writer", None)
        if raw_writer is None:
            return
        for applied, event_kind, generation in rows:
            raw_writer.write({"timestamp":applied.local_process_ts,"exchange_timestamp":applied.exchange_event_ts,
                "local_receive_ts":applied.local_receive_ts,"local_process_ts":applied.local_process_ts,
                "bids":[[str(p),str(q)] for p,q in applied.bids],"asks":[[str(p),str(q)] for p,q in applied.asks],"update_id":applied.update_id,
                "first_update_id":applied.first_update_id,"previous_update_id":applied.previous_update_id,
                "book_source":applied.book_source,"event_kind":event_kind,"recovery_generation":generation,"quality_state":applied.quality_state,
                # Carried through from BinanceAdapter.normalize()'s per-event stamp (see
                # adapters/base.py); this row IS that same event after book-state merge
                # (book_engine.LocalBook uses dataclasses.replace, which preserves fields
                # it does not touch), never re-derived here.
                "instrument_key": applied.instrument.key if applied.instrument is not None else None})

    def _write_orderbook_features(self, features: dict):
        self.stream_counters["orderbook"]["computed"] += 1
        valid, reason = self.validator.validate_orderbook(features)
        self._drain_integrity_quality_events()
        if not valid:
            self.stream_counters["orderbook"]["rejected"] += 1
            self._record_validation_rejection("orderbook", reason)
            return
        self.stream_counters["orderbook"]["validated"] += 1
        self.gap_detector.check_gap("orderbook", features["exchange_timestamp"])
        self.ob_writer.write(features)
        self.stream_counters["orderbook"]["written"] += 1
        self.health_monitor.record_message("orderbook", features["local_timestamp"])

    @staticmethod
    def _lossless_legacy_trade_id(value):
        if value is None:
            raise ValueError("missing_native_trade_id")
        if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
            raise ValueError("non_numeric_native_trade_id")
        numeric = int(value)
        if numeric > 2**63 - 1 or str(numeric) != value:
            raise ValueError("non_lossless_native_trade_id")
        return numeric

    def _handle_binance_trade(self, raw: dict, stream: str, local_receive_ts: int):
        self.stream_counters["trades"]["received"] += 1
        events = self.binance_adapter.normalize(raw, local_receive_ts=local_receive_ts)
        for event in events:
            try: trade_id = self._lossless_legacy_trade_id(event.trade_id)
            except (TypeError, ValueError): trade_id = None
            process_ts = int(time.time() * 1000)
            instrument_key = event.instrument.key if event.instrument is not None else None
            raw_trade_writer=getattr(self, "raw_trades_writer", None)
            if raw_trade_writer is not None:
                raw_trade_writer.write({"timestamp":process_ts, "local_receive_ts":event.local_receive_ts,
                    "exchange_timestamp":event.exchange_transaction_ts or event.exchange_event_ts, "trade_id":trade_id,
                    "native_trade_id":event.trade_id, "price":event.price, "quantity":event.quantity,
                    "instrument_key": instrument_key})
            if trade_id is None:
                self.stream_counters["trades"]["rejected"] += 1
                self._record_validation_rejection("trades", "legacy_trade_id_not_lossless")
                self._persist_quality_event({"stream":"trades", "event_type":QualityEventType.ERROR, "reason":"legacy_trade_id_not_lossless"})
                continue
            features = {
                "timestamp": process_ts, "local_timestamp": event.local_receive_ts,
                "exchange_timestamp": event.exchange_transaction_ts or event.exchange_event_ts,
                "trade_id": trade_id, "price": event.price, "quantity": event.quantity,
                "is_buyer_maker": event.side == "SELL",
                "side_sign": -1 if event.side == "SELL" else 1,
                "signed_qty": -event.quantity if event.side == "SELL" else event.quantity,
                "instrument_key": instrument_key,
            }
            self._handle_trade_features(features)
        self._drain_integrity_quality_events()

    def _handle_trade_features(self, features: dict):
        self.stream_counters["trades"]["computed"] += 1
        valid, reason = self.validator.validate_trade(features)
        self._drain_integrity_quality_events()
        if not valid:
            self.stream_counters["trades"]["rejected"] += 1
            self._record_validation_rejection("trades", reason)
            return
        self.stream_counters["trades"]["validated"] += 1
        self.gap_detector.check_gap("trades", features["exchange_timestamp"])
        self.trades_writer.write(features)
        self.stream_counters["trades"]["written"] += 1
        self.health_monitor.record_message("trades", features["local_timestamp"])

    def _handle_orderbook(self, data: dict, stream: str):
        self.stream_counters["orderbook"]["received"] += 1
        features = compute_orderbook_features(data)
        if not features:
            self.stream_counters["orderbook"]["empty_features"] += 1
            logger.warning("Feature extraction returned empty", stream="orderbook", raw_stream=stream, keys=sorted(data.keys()))
            return
        self.stream_counters["orderbook"]["computed"] += 1
        valid, reason = self.validator.validate_orderbook(features)
        self._drain_integrity_quality_events()
        if not valid:
            self.stream_counters["orderbook"]["rejected"] += 1
            self._record_validation_rejection("orderbook", reason)
            logger.info("Validation result", stream="orderbook", validation_pass=False, validation_fail_reason=reason)
            return
        self.stream_counters["orderbook"]["validated"] += 1
        logger.info("Validation result", stream="orderbook", validation_pass=True, validation_fail_reason="")
        self.gap_detector.check_gap("orderbook", features["exchange_timestamp"])
        self.ob_writer.write(features)
        self.stream_counters["orderbook"]["written"] += 1
        self.health_monitor.record_message("orderbook", features["timestamp"])

    def _handle_trades(self, data: dict, stream: str):
        self.stream_counters["trades"]["received"] += 1
        features = compute_trades_features(data)
        if not features:
            self.stream_counters["trades"]["empty_features"] += 1
            logger.warning("Feature extraction returned empty", stream="trades", raw_stream=stream, keys=sorted(data.keys()))
            return
        self.stream_counters["trades"]["computed"] += 1
        valid, reason = self.validator.validate_trade(features)
        self._drain_integrity_quality_events()
        if not valid:
            self.stream_counters["trades"]["rejected"] += 1
            self._record_validation_rejection("trades", reason)
            logger.info("Validation result", stream="trades", validation_pass=False, validation_fail_reason=reason)
            return
        self.stream_counters["trades"]["validated"] += 1
        logger.info("Validation result", stream="trades", validation_pass=True, validation_fail_reason="")
        self.gap_detector.check_gap("trades", features["exchange_timestamp"])
        self.trades_writer.write(features)
        self.stream_counters["trades"]["written"] += 1
        self.health_monitor.record_message("trades", features["timestamp"])

    def _binance_usdm_instrument_key(self, native_symbol, *, stream_name: str):
        """The validated BINANCE_USDM_BTCUSDT key, or None if the payload contradicts it.

        Used by the legacy raw-dict handlers (markprice, liquidation) that bypass
        BinanceAdapter.normalize() and so never get the per-event instrument stamp
        adapters/base.py provides. Both this collector's WS URLs are hardcoded to
        BTCUSDT-only streams and _route_stream already re-checks the envelope's
        stream name prefix before either handler is ever reached -- two independent
        layers proving this process cannot receive another instrument -- but a
        payload's own symbol field (when the venue includes one) is still checked
        rather than trusted blindly. Absence of the field is not a contradiction
        (older/partial payloads); only an explicit mismatch is.
        """
        if native_symbol is not None and native_symbol != SYMBOL:
            self._persist_quality_event({
                "stream": stream_name, "event_type": QualityEventType.ERROR,
                "reason": f"symbol_contradicts_configured_instrument:{native_symbol}",
                "rows_lost": 1, "local_ts": int(time.time() * 1000)})
            return None
        return BINANCE_USDM_BTCUSDT.key

    def _handle_liquidation(self, data: dict, stream: str):
        self.stream_counters["liquidation"]["received"] += 1
        try:
            instrument_key = self._binance_usdm_instrument_key(
                data.get("o", {}).get("s") if isinstance(data.get("o"), dict) else None,
                stream_name="liquidation")
            if instrument_key is None:
                self.stream_counters["liquidation"]["rejected"] += 1
                self._record_validation_rejection("liquidation", "symbol_contradicts_configured_instrument")
                return
            features = compute_liquidation_features(data)
            if not features:
                self.stream_counters["liquidation"]["empty_features"] += 1
                logger.warning("Feature extraction returned empty", stream="liquidation", raw_stream=stream, keys=sorted(data.keys()))
                return
            features["instrument_key"] = instrument_key
            self.stream_counters["liquidation"]["computed"] += 1
            valid, reason = self.validator.validate_liquidation(features)
            self._drain_integrity_quality_events()
            if not valid:
                self.stream_counters["liquidation"]["rejected"] += 1
                self._record_validation_rejection("liquidation", reason)
                logger.info("Validation result", stream="liquidation", validation_pass=False, validation_fail_reason=reason)
                return
            self.stream_counters["liquidation"]["validated"] += 1
            logger.info("Validation result", stream="liquidation", validation_pass=True, validation_fail_reason="")
            self.liq_writer.write(features)
            self.stream_counters["liquidation"]["written"] += 1
            self.health_monitor.record_message("liquidation", features["timestamp"])
        except Exception as exc:
            self.stream_counters["liquidation"]["rejected"] += 1
            self._record_validation_rejection("liquidation", type(exc).__name__)
            logger.error("Liquidation handling failed", stream="liquidation", raw_stream=stream, error=str(exc))

    def _handle_markprice(self, data: dict, stream: str, local_receive_ts: int):
        self.stream_counters["markprice"]["received"] += 1
        instrument_key = self._binance_usdm_instrument_key(data.get("s"), stream_name="markprice")
        if instrument_key is None:
            self.stream_counters["markprice"]["rejected"] += 1
            self._record_validation_rejection("markprice", "symbol_contradicts_configured_instrument")
            return
        features = compute_markprice_features(data)
        if not features:
            self.stream_counters["markprice"]["empty_features"] += 1
            logger.warning("Feature extraction returned empty", stream="markprice", raw_stream=stream, keys=sorted(data.keys()))
            return
        # P0-6: "timestamp" from compute_markprice_features is a fresh
        # time.time() call taken when this handler runs -- processing time,
        # not the collector's actual receive time. Before this fix,
        # "local_timestamp" silently duplicated that same processing-time
        # value, so no genuine availability clock existed for markprice at
        # all. local_receive_ts is the frame's real receive time, captured
        # once at handle_message's entry (the same clock raw_capture uses),
        # so it is stamped here exactly as the orderbook/trades adapter
        # paths already do for their own local_timestamp.
        features["local_timestamp"] = local_receive_ts
        features["instrument_key"] = instrument_key
        self.stream_counters["markprice"]["computed"] += 1
        valid, reason = self.validator.validate_markprice(features)
        self._drain_integrity_quality_events()
        if not valid:
            self.stream_counters["markprice"]["rejected"] += 1
            self._record_validation_rejection("markprice", reason)
            logger.info("Validation result", stream="markprice", validation_pass=False, validation_fail_reason=reason)
            return
        self.stream_counters["markprice"]["validated"] += 1
        logger.info("Validation result", stream="markprice", validation_pass=True, validation_fail_reason="")
        self.gap_detector.check_gap("markprice", features["exchange_timestamp"])
        self.mark_writer.write(features)
        self.stream_counters["markprice"]["written"] += 1
        self.health_monitor.record_message("markprice", features["timestamp"])

    def _make_reconnect_handler(self, url: str):
        streams_for_url = self._requested_streams(url)
        def handler():
            logger.info("Resetting validation and gap tracking on reconnect", url=url, streams=streams_for_url)
            for stream_name_fragment in streams_for_url:
                route = self._route_stream(stream_name_fragment)
                if route:
                    preserved_last_mid_price = self.validator.last_mid_price if route == "orderbook" else None
                    self.validator.reset_stream(route)
                    if route == "orderbook":
                        self.validator.last_mid_price = preserved_last_mid_price
                    self.gap_detector.reset_stream(route)
                    if route == "orderbook":
                        if getattr(self, "binance_book", None) is None:
                            self.binance_book = LocalBook("BINANCE")
                        self.binance_book.invalidate("websocket_reconnect")
            self._drain_integrity_quality_events()
        return handler

    async def start(self):
        logger.info("Starting Collector Application")
        logger.info("Collector WebSocket subscription", url=BINANCE_PUBLIC_WS_URL, requested_streams=self._requested_streams(BINANCE_PUBLIC_WS_URL))
        logger.info("Collector WebSocket subscription", url=BINANCE_MARKET_WS_URL, requested_streams=self._requested_streams(BINANCE_MARKET_WS_URL))
        send_telegram_alert("Collector Application Started")
        self.running = True
        self._quality_task = asyncio.create_task(self._quality_persistence_loop())
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._async_shutdown(s)))
        for ws_client in self.ws_clients:
            self.tasks.append(asyncio.create_task(ws_client.start()))
        for ws_client in self.ws_clients:
            connected = await ws_client.wait_connected(timeout_seconds=30.0)
            if not connected:
                logger.warning("ws_client_pre_connect_timeout", url=ws_client.url)
        await self._recover_binance_book("startup")
        self.tasks.append(asyncio.create_task(self.health_monitor.start()))
        self.tasks.append(asyncio.create_task(self._poll_openinterest()))
        self.tasks.append(asyncio.create_task(self._verify_startup_streams()))
        try:
            await asyncio.gather(*self.tasks)
        except asyncio.CancelledError:
            pass
        finally:
            if self._recovery_task is not None and not self._recovery_task.done():
                self._recovery_task.cancel()
                await asyncio.gather(self._recovery_task, return_exceptions=True)
            self._drain_integrity_quality_events()
            await self._quality_queue.join()
            self.running = False
            if self._quality_task is not None:
                await self._quality_task
            self.shutdown()

    async def _poll_openinterest(self):
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=5)
        while self.running:
            request_ts = int(time.time() * 1000)
            status = None
            body = None
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(OI_URL) as resp:
                        status = resp.status
                        body = await resp.text()
                        receive_ts = int(time.time() * 1000)
                        resp.raise_for_status()
                        data = json.loads(body)
                # REST polling time is not exchange observation time; both are
                # preserved separately so research can tell them apart.
                process_ts = int(time.time() * 1000)
                self._capture_rest(RawRestRecord(
                    request_ts=request_ts, response_receive_ts=receive_ts,
                    endpoint=OI_URL, purpose="open_interest",
                    request_params={"symbol": SYMBOL}, http_status=status,
                    ok=True, payload=body, symbol=SYMBOL,
                    local_process_ts=process_ts))
                self.stream_counters["openinterest"]["received"] += 1
                # G2: the same normalizer replay uses. local_receive_ts is the
                # RESPONSE's receive time, not wall-clock-at-write and not the
                # exchange's own `time` field -- a slow response must not
                # claim availability earlier than it actually arrived.
                try:
                    event = normalize_binance_oi(
                        body, response_receive_ts=receive_ts,
                        local_process_ts=process_ts, symbol=SYMBOL)
                except BinanceOIParseError as exc:
                    self.stream_counters["openinterest"]["empty_features"] += 1
                    self._persist_quality_event({
                        "stream": "openinterest", "event_type": QualityEventType.ERROR,
                        "reason": f"oi_malformed_response:{exc}",
                        "local_ts": process_ts})
                else:
                    features = {
                        "timestamp": event.local_receive_ts,
                        "exchange_timestamp": event.exchange_event_ts,
                        "local_timestamp": event.local_receive_ts,
                        "open_interest": event.open_interest,
                        # Resolved in normalize_binance_oi from the REST response's own
                        # 'symbol' field via instrument.resolve_instrument -- not stamped
                        # here from a constant.
                        "instrument_key": event.instrument.key if event.instrument is not None else None,
                    }
                    self.stream_counters["openinterest"]["computed"] += 1
                    self.stream_counters["openinterest"]["validated"] += 1
                    self.oi_writer.write(features)
                    self.stream_counters["openinterest"]["written"] += 1
                    if event.exchange_event_ts is not None:
                        self.gap_detector.check_gap("openinterest", event.exchange_event_ts)
                    self.health_monitor.record_message("openinterest", event.local_receive_ts)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Previously log-only, so a REST outage left no trace in the
                # data and looked identical to a period of no change.
                logger.error("OI poll failed", error=str(e))
                self._capture_rest(RawRestRecord(
                    request_ts=request_ts, response_receive_ts=None,
                    endpoint=OI_URL, purpose="open_interest",
                    request_params={"symbol": SYMBOL}, http_status=status,
                    ok=False, error=f"{type(e).__name__}:{e}", payload=body,
                    symbol=SYMBOL))
                self._persist_quality_event({
                    "stream": "openinterest", "event_type": QualityEventType.ERROR,
                    "reason": f"oi_poll_failed:{type(e).__name__}",
                    "local_ts": int(time.time() * 1000)})
            await asyncio.sleep(OI_POLL_INTERVAL_S)

    async def _verify_startup_streams(self):
        await asyncio.sleep(STREAM_INACTIVE_STARTUP_SECONDS)
        inactive_streams = [stream_name for stream_name in ("orderbook", "trades", "markprice") if self.stream_counters[stream_name]["received"] == 0]
        if inactive_streams:
            msg = f"Startup stream inactivity after {STREAM_INACTIVE_STARTUP_SECONDS}s: {', '.join(inactive_streams)}"
            logger.error(msg, stream_counters=self.stream_counters, validation_fail_reasons=self.validation_fail_reasons)
            send_telegram_alert(msg)
            raise RuntimeError(msg)
        logger.info("Startup stream verification passed", stream_counters=self.stream_counters, validation_fail_reasons=self.validation_fail_reasons)

    async def _async_shutdown(self, signum: int):
        logger.info("Received signal, initiating async shutdown", signum=signum)
        self.running = False
        if self._recovery_task is not None and not self._recovery_task.done():
            self._recovery_task.cancel()
            await asyncio.gather(self._recovery_task, return_exceptions=True)
        self._drain_integrity_quality_events()
        if self._quality_task is not None:
            await self._quality_task
        self.shutdown()
        for task in self.tasks:
            task.cancel()

    def shutdown(self):
        if self._closed:
            return
        self._closed = True
        logger.info("Shutting down Collector Application...", stream_counters=self.stream_counters, validation_fail_reasons=self.validation_fail_reasons)
        self.running = False
        for ws_client in self.ws_clients:
            ws_client.stop()
        self.health_monitor.stop()
        self._drain_integrity_quality_events()
        for task in self.tasks:
            task.cancel()
        if self._recovery_task is not None and not self._recovery_task.done():
            self._recovery_task.cancel()
        self.ob_writer.close()
        self.raw_book_writer.close()
        self.trades_writer.close()
        self.raw_trades_writer.close()
        self.mark_writer.close()
        self.oi_writer.close()
        self.liq_writer.close()
        for raw_writer_name in ("raw_wire_writer", "raw_rest_writer"):
            raw_writer = getattr(self, raw_writer_name, None)
            if raw_writer is not None:
                raw_writer.close()
        self.quality_writer.close()
        msg = "Collector Application Shutdown"
        logger.info(msg)
        send_telegram_alert(msg)

if __name__ == "__main__":
    validate_telegram_startup()
    app = CollectorApp()
    try:
        asyncio.run(app.start())
    except KeyboardInterrupt:
        pass
