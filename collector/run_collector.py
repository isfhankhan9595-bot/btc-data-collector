import asyncio
import signal
import sys
import time
import json
import os
from decimal import Decimal
from typing import Optional
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
from collector.collector.segment_dedup import DedupStateError, StreamSpec, attach_segment_dedup, bind_arg
from collector.collector.raw_capture import RawCapture, RawRestRecord, RawWireRecord
from collector.collector.backoff import ExponentialBackoff, rate_limit_penalty
from collector.collector.recovery_control import RecoveryController
from collector.collector.binance_oi import normalize_binance_oi, BinanceOIParseError
from collector.collector.book_engine import LocalBook
from collector.collector.canonical import CanonicalOrderBookEvent
from collector.collector.quality_events import BookQuality, QualityEvent, QualityEventType
from collector.collector.quality_wal import QualityEventWAL, QualityWALCapacityExceeded, QualityWALCorruption
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
from collector.collector.disk_pressure import (
    EXIT_DISK_MONITOR_UNAVAILABLE, EXIT_DISK_PRESSURE, DiskPressureConfig, Severity, StoragePressureMonitor,
)
from collector.collector.health_monitor import HealthMonitor
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.storage_errors import (
    EVENT_PUBLISHED, EVENT_UNCONFIRMED, FatalStorageError, WriterFailureSnapshot)
from collector.collector.failure_topology import (
    EXIT_FATAL_STORAGE, FailureRecord, VERDICT_DEGRADE_QUALITY, VERDICT_ISOLATE, VERDICT_TERMINATE,
    classify_stream,
)
from collector.collector.websocket_client import WebSocketClient
from collector.collector.standalone_failure_policy import EXIT_RUN_TASK_CRASHED

STREAM_INACTIVE_STARTUP_SECONDS = 60
#: Extra wait, after the hard-cancel fallback, for a client's tail to retire its worker
#: (``worker_retire_timeout_s`` + this). Part of the shutdown budget (see
#: websocket_client.FINALIZATION_RESERVE_S).
WS_FALLBACK_SLACK_S = 1.0
#: Deadlines for the quality persistence loop's part of shutdown (``_stop_quality_consumer``). They come
#: out of the "everything else" reserve in docs/DEPLOYMENT.md, are chosen here, and are NOT measured on
#: the production host. The quality queue is telemetry: waiting on it must never stand between a stop
#: request and the finalization of the market-data writers. A timeout loses nothing: every queued event
#: already has a WAL record, ``shutdown()`` drains what is left synchronously, and an event nobody drained
#: stays in the WAL for the next start.
#:   QUALITY_DRAIN_BOUND_S          wait for the loop to hand every queued event to the writer
#:   QUALITY_CONSUMER_STOP_BOUND_S  then, after asking it to stop, wait for it to end before cancelling it
QUALITY_DRAIN_BOUND_S = 1.0
QUALITY_CONSUMER_STOP_BOUND_S = 0.5

#: Storage each REST producer can still write, from tracing the producer's coroutine through
#: every method it calls (tests/test_ws_rest_producer_reach.py recomputes this with an AST
#: closure, so a new write path cannot silently escape it). "quality_wal" is the quality
#: writer's WAL. A producer that is still alive keeps exactly its own set open -- no broader
#: guess about which writers are "probably safe".
#:   recovery / startup recovery: raw REST capture, reconstructed book rows
#:       (_persist_reconstructed_books), quality events (_record_book_quality ->
#:       _persist_quality_event -> WAL + quality_writer).
#:   open-interest poller: raw REST capture, oi_writer, quality events.
REST_PRODUCER_REACH = {
    "recovery": frozenset({"raw_rest_writer", "raw_book_writer", "quality_writer", "quality_wal"}),
    "oi_poll": frozenset({"raw_rest_writer", "oi_writer", "quality_writer", "quality_wal"}),
}
RAW_LOG_LIMIT = 20
OI_POLL_INTERVAL_S = 3.0
OI_URL = "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"
BINANCE_DEPTH_SNAPSHOT_URL = "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000"

#: P0-3 quality-event batching (single source of truth for the production
#: quality writer). Was 1 row / 1 s = one Parquet file per event.
QUALITY_SEGMENT_ROWS = 500
QUALITY_SEGMENT_SECONDS = 30.0

#: F5: how often the supervisor looks for a writer failure that latched while
#: that writer was quiet. A handful of attribute reads: not expensive polling.
FAILURE_SUPERVISOR_INTERVAL_S = 1.0
# F5: ``EXIT_FATAL_STORAGE`` (process exit status of a terminal raw-evidence
# storage failure) is defined once, in failure_topology, and shared with the
# standalone venue runners; it stays importable from this module.
#: Writers the supervisor inspects (attribute name on the app).
SUPERVISED_WRITERS = ("ob_writer", "raw_book_writer", "trades_writer", "raw_trades_writer",
                      "mark_writer", "oi_writer", "liq_writer", "raw_wire_writer",
                      "raw_rest_writer", "quality_writer")


class CollectorApp:
    def __init__(self, enable_segment_dedup: bool = True):
        self.running = False
        self._closed = False
        self._init_failure_state()
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

        # F5-QA-04: no disk check and no shutdown callback here. The old constructor-time
        # ``check_disk_space`` could invoke ``shutdown()`` on a half-built app. Disk pressure is
        # now enforced by ``storage_pressure`` (built once the quality WAL exists, below):
        # a startup preflight in ``start()`` plus a periodic task.

        self.validator = Validator()
        self.gap_detector = GapDetector()
        self.binance_adapter = BinanceAdapter()
        self.binance_book = LocalBook("BINANCE")
        self._book_snapshot_lock = asyncio.Lock()
        self._recovery_task = None
        self._startup_recovery_task = None   # the inline start() recovery, tracked as a REST producer
        self._oi_task = None
        # One gap must not become hundreds of REST snapshot requests.
        self.recovery_controller = RecoveryController(
            name="binance_orderbook", min_interval_s=1.0, max_per_window=5,
            window_s=60.0,
            backoff=ExponentialBackoff(base_delay=1.0, max_delay=60.0, max_attempts=10),
            quality_sink=self._emit_quality_event)
        self._quality_queue = asyncio.Queue(maxsize=1024)
        self._quality_task = None
        self._quality_loop_stop = False   # set by _stop_quality_consumer: leave the loop even if events are queued
        self._quality_overflow = 0

        # P0-3: batched (was segment_rows=1 / segment_seconds=1 = one Parquet
        # file per quality event). Batching means write() no longer implies
        # "durable", so EVERY event entering _persist_quality_event is WAL-
        # protected first (P0-2) and the WAL checkpoint only advances from
        # on_segment_durable, i.e. after the segment holding the event is
        # published (fsync + rename + directory fsync). Written state is set
        # up BEFORE the writer exists because its hook references it.
        self._init_quality_durability_state()
        self.quality_writer = ParquetWriter(
            "quality_events", QUALITY_EVENTS_SCHEMA,
            segment_rows=QUALITY_SEGMENT_ROWS, segment_seconds=QUALITY_SEGMENT_SECONDS,
            on_segment_durable=self._on_quality_segment_durable, age_from_first_row=True)
        # P0-2: the durable quality-event WAL replaces the old
        # quality_queue.pending.json marker (which only recorded a queue
        # depth, never the events). With batching it protects ALL quality
        # events, not just the websocket queue: see _persist_quality_event.
        self._recover_quality_wal(self.quality_writer.stream_dir / "wal")
        self.ob_writer = ParquetWriter("orderbook", ORDERBOOK_SCHEMA, quality_event_sink=self._persist_quality_event)
        self.raw_book_writer = ParquetWriter("binance_orderbook_raw", BINANCE_ORDERBOOK_RAW_SCHEMA, quality_event_sink=self._persist_quality_event)
        # P0: the canonical and raw trade writers now report their own storage
        # failures (publication failure, unflushed-row DATA_DROP, and the
        # restart-time DATA_DROP for a crashed/failed segment) through the
        # durable quality path. Without a sink those were log lines only.
        # The sink cannot recurse: _persist_quality_event writes to
        # quality_writer, which has no sink of its own, and ParquetWriter
        # guards the sink call so a sink failure can never make a failed
        # writer look healthy (the latch, not the sink, enforces fail-closed).
        self.trades_writer = ParquetWriter("trades", TRADES_SCHEMA, quality_event_sink=self._persist_quality_event)
        self.raw_trades_writer = ParquetWriter("binance_trades_raw", BINANCE_TRADES_RAW_SCHEMA, quality_event_sink=self._persist_quality_event)
        # F5: these three used to have no sink, so their storage failures were
        # log lines only. Same non-recursive sink as every other derived writer.
        self.mark_writer = ParquetWriter("markprice", MARKPRICE_SCHEMA, quality_event_sink=self._persist_quality_event)
        self.oi_writer = ParquetWriter("openinterest", OPENINTEREST_SCHEMA, quality_event_sink=self._persist_quality_event)
        self.liq_writer = ParquetWriter("liquidation", LIQUIDATION_SCHEMA, quality_event_sink=self._persist_quality_event)
        self.raw_wire_writer = ParquetWriter("raw_wire", RAW_WIRE_SCHEMA, quality_event_sink=self._persist_quality_event)
        self.raw_rest_writer = ParquetWriter("raw_rest", RAW_REST_SCHEMA, quality_event_sink=self._persist_quality_event)
        # P0-4: the raw (least-processed) writer is the dedup recovery
        # anchor -- it receives every admitted trade unconditionally,
        # whereas trades_writer (computed features) additionally depends
        # on validate_trade() succeeding and can miss a row the raw writer
        # got. Binding the durable commit to the writer that is guaranteed
        # to receive the row is the correct anchor; see segment_dedup.py's
        # module docstring for why a published segment is the anchor at all.
        # Failure here (index open/reconcile) raises DedupStateError, which
        # aborts CollectorApp() construction -- the collector must not
        # start with an uncertain dedup state (Invariant: fail closed).
        self.segment_dedup = None
        if enable_segment_dedup:
            self.segment_dedup = attach_segment_dedup(self.binance_adapter, [
                StreamSpec("trades", self.raw_trades_writer, "BINANCE", "linear_perpetual",
                          trade_id_field="native_trade_id"),
            ])
        # F5: raw_wire / raw_rest are the irrecoverable-evidence boundary, so a
        # typed storage fatal from either must reach the application instead of
        # being absorbed (ordinary capture failures still fail open).
        self.raw_capture = RawCapture(self.raw_wire_writer, self.raw_rest_writer,
                                      quality_event_sink=self._persist_quality_event,
                                      fail_closed_on_fatal_storage=True)
        # Adapter drops become durable quality events instead of vanishing.
        self.binance_adapter.set_unhandled_sink(self._record_adapter_unhandled)

        self.ws_clients = [
            WebSocketClient(
                url=BINANCE_PUBLIC_WS_URL,
                on_message=self.handle_message,
                on_reconnect=self._make_reconnect_handler(BINANCE_PUBLIC_WS_URL),
                on_quality_event=self._websocket_quality_event, stream_group="public",
                on_raw_frame=self._capture_raw_frame, on_fatal=self._on_fatal_storage
            ),
            WebSocketClient(
                url=BINANCE_MARKET_WS_URL,
                on_message=self.handle_message,
                on_reconnect=self._make_reconnect_handler(BINANCE_MARKET_WS_URL),
                on_quality_event=self._websocket_quality_event, stream_group="market",
                on_raw_frame=self._capture_raw_frame, on_fatal=self._on_fatal_storage
            ),
        ]

        # The health summary probes the filesystem that actually holds the collector's data.
        self.disk_monitor = DiskMonitor(data_dir=str(self.quality_writer.base_dir))
        self.health_monitor = HealthMonitor(self.disk_monitor, self.validator, self)
        self._init_storage_pressure()
        self.tasks = []

    @property
    def connected(self):
        return all(client.connected for client in self.ws_clients)

    # ------------------------------------------------------------------
    # F5: typed fatal-storage topology (see docs/F5_FATAL_STORAGE_TOPOLOGY.md).
    #
    #   raw_wire / raw_rest failure  -> TERMINATE (controlled, non-zero exit)
    #   derived writer failure       -> ISOLATE that route only
    #   quality writer failure       -> DEGRADE the quality channel only
    #   unknown typed fatal          -> TERMINATE (default-deny)
    #   ordinary exception           -> not handled here: unchanged (P0-1)
    #
    # Classification is ``isinstance(exc, FatalStorageError)`` plus the writer's
    # own ``stream`` (failure_topology); never RuntimeError, never message text.
    # Nothing below resurrects a failed writer: a latch only ever closes, and
    # only a process restart (F1/P0-4 startup reconciliation) reopens a stream.
    # ------------------------------------------------------------------
    def _init_failure_state(self) -> None:
        #: component key -> FailureRecord (first failure per component wins).
        self.failed_components: dict = {}
        #: route -> FailureRecord for routes whose handler/writer is cut off.
        self.isolated_routes: dict = {}
        #: First TERMINATE-verdict failure; once set the process must exit non-zero.
        self.terminal_failure = None
        #: First quality-channel failure (telemetry degraded, market data intact).
        self.quality_degraded = None
        #: Frames short-circuited per isolated route (a counter, never one event per frame).
        self.route_short_circuits: dict = {}
        #: Repeat observations of an already-latched component (no re-report).
        self.failure_repeats: dict = {}
        self._reporting_failure = False
        self._terminal_shutdown_task = None

    def _ensure_failure_state(self) -> None:
        if not hasattr(self, "failed_components"):
            self._init_failure_state()

    @property
    def exit_code(self) -> int:
        self._ensure_failure_state()
        if self.terminal_failure is not None:
            return EXIT_FATAL_STORAGE          # 70 keeps priority: raw evidence was lost
        if getattr(self, "_disk_emergency", None) is not None:
            return EXIT_DISK_PRESSURE          # 1: confirmed disk-pressure emergency stop (F5-QA-04)
        if getattr(self, "_disk_monitor_unavailable", None) is not None:
            return EXIT_DISK_MONITOR_UNAVAILABLE   # 69: storage could not be measured (fail closed)
        if getattr(self, "shutdown_failure", None) is not None:
            return EXIT_RUN_TASK_CRASHED           # 1: shutdown could not retire a websocket worker
        return 0

    def _on_fatal_storage(self, exc, origin: str = "handler") -> str:
        """Classify and latch one typed storage fatal. Idempotent and non-raising
        for every input a ``FatalStorageError`` can be; returns the verdict.

        Safe to call from the websocket worker: it never shuts anything down.
        A TERMINATE verdict only latches state and puts the websocket clients in
        discard mode; the supervisor task performs the controlled shutdown."""
        self._ensure_failure_state()
        if not isinstance(exc, FatalStorageError):
            raise TypeError(f"_on_fatal_storage requires FatalStorageError, got {type(exc).__name__}")
        record = FailureRecord.from_exception(exc, origin=origin, now_ms=int(time.time() * 1000))
        return self._latch_failure(record)

    def _latch_failure(self, record: "FailureRecord") -> str:
        self._ensure_failure_state()
        key = record.key
        if key in self.failed_components:
            # Already latched: count, do not re-report. This is what stops an
            # error flood when every later frame trips the same dead writer.
            self.failure_repeats[key] = self.failure_repeats.get(key, 0) + 1
            return self.failed_components[key].verdict
        self.failed_components[key] = record
        if record.verdict == VERDICT_ISOLATE and record.route is not None:
            self.isolated_routes.setdefault(record.route, record)
        elif record.verdict == VERDICT_DEGRADE_QUALITY:
            if self.quality_degraded is None:
                self.quality_degraded = record
            if not self._quality_checkpoint_is_blocked():
                self._block_quality_checkpoint("quality_writer_fatal_storage")
        else:  # VERDICT_TERMINATE, including every unmapped typed fatal
            if self.terminal_failure is None:
                self.terminal_failure = record
            self._enter_terminal_mode()
        self._report_storage_failure(record)
        return record.verdict

    def _enter_terminal_mode(self) -> None:
        """Stop normal ingestion without shutting anything down from here.

        Discard mode keeps each client's worker draining (task_done exactly
        once per item) so no producer blocks and queue.join() completes."""
        for client in getattr(self, "ws_clients", ()):
            try:
                client.enter_discard_mode("app_terminal_failure")
            except Exception as exc:  # noqa: BLE001 - latching must never raise into a worker
                logger.error("terminal_mode_client_stop_failed", error=f"{type(exc).__name__}: {exc}")

    def _report_storage_failure(self, record: "FailureRecord") -> None:
        """Recursion-safe failure report. NEVER touches ``quality_writer`` nor
        ``_persist_quality_event`` (either may be the failed component):
        synchronous structured log -> the quality WAL directly -> operator alert.
        Never raises."""
        if self._reporting_failure:
            return
        self._reporting_failure = True
        try:
            logger.error("storage_failure_latched", **record.as_dict())
            self._report_failure_to_wal(record)
            try:
                send_telegram_alert(
                    f"STORAGE FAILURE [{record.verdict}] stream={record.stream} component={record.component} "
                    f"stage={record.stage} durability={record.durability} origin={record.origin}")
            except Exception as exc:  # noqa: BLE001 - alerting fails open
                logger.error("storage_failure_alert_failed", error=f"{type(exc).__name__}: {exc}")
        except Exception as exc:  # noqa: BLE001 - reporting must never abort the caller
            logger.error("storage_failure_report_failed", error=f"{type(exc).__name__}: {exc}")
        finally:
            self._reporting_failure = False

    def _report_failure_to_wal(self, record: "FailureRecord") -> None:
        """Append the failure straight to the quality WAL (durable, fsynced).

        The seq is tracked in-flight so no later checkpoint can cover it: the
        next start's WAL recovery replays it into quality Parquet. Bypasses
        quality_writer entirely, so it works when quality_writer is the thing
        that failed."""
        wal = getattr(self, "_quality_wal", None)
        if wal is None:
            return
        self._ensure_quality_state()
        event = {"exchange": "BINANCE", "stream": "storage_failure", "event_type": QualityEventType.ERROR.value,
                 "reason": (f"fatal_storage:{record.verdict}:component={record.component}:stream={record.stream}:"
                            f"stage={record.stage}:durability={record.durability}:origin={record.origin}"),
                 "rows_lost": None, "local_ts": record.first_observed_ts}
        try:
            event_id = wal.append(event)
        except Exception as exc:  # noqa: BLE001 - the log line above is still the record
            logger.error("storage_failure_wal_append_failed", error=f"{type(exc).__name__}: {exc}")
            return
        self._quality_wal_inflight.add(int(event_id.rsplit("-", 1)[-1]))

    def supervise_once(self) -> None:
        """One supervisor pass: latch any writer failure nobody has seen yet.

        A writer can latch FAILED (or its publication gate) while quiet -- e.g.
        a hook failure at a segment close -- and the exception only surfaces on
        the NEXT write, which may never come. This reads the latches; it never
        clears them and never touches a writer's state."""
        self._ensure_failure_state()
        for name in SUPERVISED_WRITERS:
            writer = getattr(self, name, None)
            snapshot_fn = getattr(writer, "failure_snapshot", None)
            if snapshot_fn is None:
                continue
            snapshot = snapshot_fn()
            if not isinstance(snapshot, WriterFailureSnapshot):
                continue
            self._latch_failure(FailureRecord.from_snapshot(
                snapshot, origin="supervisor", now_ms=int(time.time() * 1000)))

    async def _failure_supervisor_loop(self) -> None:
        """Poll for latent writer failures; on a terminal failure, start the
        controlled shutdown from HERE (never from a websocket worker, which
        would be shutting down its own parent task)."""
        while self.running and not self._closed:
            try:
                self.supervise_once()
            except Exception as exc:  # noqa: BLE001 - the supervisor must not die
                logger.error("failure_supervisor_pass_failed", error=f"{type(exc).__name__}: {exc}")
            if self.terminal_failure is not None:
                self._terminal_shutdown_task = asyncio.create_task(self._async_shutdown(0, terminal=True))
                return
            await asyncio.sleep(FAILURE_SUPERVISOR_INTERVAL_S)

    def _route_is_isolated(self, route) -> bool:
        return route in self.isolated_routes

    def _note_short_circuit(self, route: str) -> None:
        self.route_short_circuits[route] = self.route_short_circuits.get(route, 0) + 1


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
                           decode_error=None, parsed=None, control_frame=False,
                           local_receive_ns=None, receive_mono_ns=None):
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
            local_receive_ts=local_receive_ts, local_receive_ns=local_receive_ns,
            receive_mono_ns=receive_mono_ns,
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
        self._emit_quality_event(message.to_quality_event())

    def _capture_rest(self, record) -> bool:
        """Persist one raw REST record. Returns ``False`` ONLY when the raw_rest
        evidence boundary is lost (terminal failure latched): the caller must
        then not treat the response as authoritative -- a REST answer reflects
        the exchange at request time and cannot be reconstructed later, which
        is why raw_rest is a raw-evidence boundary (TERMINATE), not a route.
        Ordinary capture failures still fail open inside RawCapture."""
        capture = getattr(self, "raw_capture", None)
        if capture is None:
            return True
        try:
            capture.capture_rest(record)
        except FatalStorageError as exc:
            self._on_fatal_storage(exc, origin="raw_rest")
            return False
        return True

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
                    self._emit_quality_event(event.record())
                elif isinstance(event, dict):
                    self._emit_quality_event(event)

    def _drain_integrity_quality_events_safely(self) -> None:
        """Shutdown-path variant of ``_drain_integrity_quality_events``: it never
        raises and one failing event does not forfeit the events after it.

        Quality reporting must never be a precondition of terminal shutdown: when
        the quality writer is the thing that just failed, the first persist raises,
        and the strict drain would abort ``_async_shutdown`` / ``shutdown`` before
        a single writer was closed. Each event is already WAL-protected before it
        reaches the writer (``_persist_quality_event``), so a failed persist only
        latches the checkpoint closed and leaves the event for the next start's
        WAL recovery."""
        for source in (getattr(self, "binance_book", None), getattr(self, "validator", None)):
            drain = getattr(source, "drain_quality_events", None)
            if drain is None:
                continue
            try:
                events = list(drain())
            except Exception as exc:  # noqa: BLE001
                logger.error("integrity_quality_drain_failed", error=f"{type(exc).__name__}: {exc}")
                self._block_quality_checkpoint("shutdown_integrity_drain_failed")
                continue
            for event in events:
                try:
                    if isinstance(event, QualityEvent):
                        self._persist_quality_event(event.record())
                    elif isinstance(event, dict):
                        self._persist_quality_event(event)
                except Exception as exc:  # noqa: BLE001
                    logger.error("integrity_quality_event_shutdown_persist_failed",
                                 error=f"{type(exc).__name__}: {exc}")
                    self._block_quality_checkpoint("shutdown_integrity_persist_failed")

    async def handle_message(self, msg: dict, local_receive_ts: int | None = None,
                             connection_id: str | None = None):
        """Route one decoded frame. F5 boundary: a typed storage fatal raised by
        any route's writer is classified here (isolate / degrade / terminate)
        instead of surfacing as an ordinary processing error. Ordinary
        exceptions are untouched: they propagate to the worker exactly as before
        (P0-1 isolation). The raw frame was captured before this point, so
        abandoning a frame whose route just failed loses no raw evidence."""
        try:
            await self._handle_message_routed(msg, local_receive_ts, connection_id)
        except FatalStorageError as exc:
            self._on_fatal_storage(exc, origin="handler")

    async def _handle_message_routed(self, msg: dict, local_receive_ts: int | None = None,
                                     connection_id: str | None = None):
        self._ensure_failure_state()
        if local_receive_ts is None:
            local_receive_ts = int(time.time() * 1000)
        if not isinstance(msg, dict) or "stream" not in msg or "data" not in msg:
            # Previously a bare `return`: subscription acks, error envelopes and
            # any unexpected shape vanished with no counter and no record. A
            # frame that does not match the combined-stream envelope is still
            # information, and its absence must not look like silence.
            self.stream_counters["malformed_envelope"]["received"] += 1
            keys = sorted(str(k) for k in msg.keys()) if isinstance(msg, dict) else []
            self._emit_quality_event({
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
        if route is not None and self._route_is_isolated(route):
            # F5: the route's writer failed. Do not invoke its handler or writer
            # again, and do not turn every later frame into a quality event.
            self._note_short_circuit(route)
            return
        if route is None:
            self.stream_counters["unrouted"]["received"] += 1
            logger.warning("Unrouted stream message", stream=stream)
            # Durable, not log-only: an unrouted stream is data the collector
            # received and chose not to process.
            self._emit_quality_event({
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
            self._handle_liquidation(data, stream, local_receive_ts)
        self._drain_integrity_quality_events()
        if self.validator.check_failure_rate():
            logger.error("Validation spike detected: >0.1% failures in 60s window")
            send_telegram_alert("Validation spike detected: >0.1% failures in 60s window")

    # ------------------------------------------------------------------
    # P0-2 / P0-3 quality-event durability state.
    #
    # Invariant: wal checkpoint N on disk  =>  every quality event with WAL
    # seq <= N is durably in a PUBLISHED quality Parquet segment.
    #   _quality_wal_inflight  seqs WAL-appended but not yet in a published
    #                          segment (added at append time, so an event
    #                          still sitting in the queue holds the
    #                          checkpoint back).
    #   _quality_segment_seqs  seqs written into the currently open segment.
    # On publication the open segment's seqs leave inflight and the
    # checkpoint moves to min(inflight)-1 (or the highest published seq when
    # nothing is in flight) -- a contiguous prefix, never a max.
    # ------------------------------------------------------------------
    def _init_quality_durability_state(self) -> None:
        self._quality_checkpoint_blocked = False
        self._quality_checkpoint_block_reason = None
        self._quality_checkpoint_block_alerted = False   # one operator alert per process, set BEFORE sending
        self._quality_wal_capacity_alerted = False      # one operator alert for the aggregate hard cap
        self._quality_wal_inflight = set()
        self._quality_segment_seqs = set()
        self._quality_published_hwm = None
        self._quality_overflow_unpersisted = 0
        # F5 degraded-channel accounting (see _account_degraded_quality_event). An event
        # is "WAL-retained" ONLY when its WAL record is established.
        self._quality_events_wal_only = 0          # WAL record established; replayed next start
        self._quality_events_wal_unconfirmed = 0   # append failed after assigning an id: a record MAY exist
        self._quality_events_unrecorded = 0        # no WAL record and no writer: the event is lost
        self._quality_unrecorded_alerted = False

    def _ensure_quality_state(self) -> None:
        if not hasattr(self, "_quality_wal_inflight"):
            self._init_quality_durability_state()

    def _quality_checkpoint_is_blocked(self) -> bool:
        return getattr(self, "_quality_checkpoint_blocked", False)

    def _block_quality_checkpoint(self, why: str) -> None:
        """Latch: stop advancing the WAL checkpoint for the rest of this
        process's life. Everything stays in the WAL and is REPLAYED by the next
        startup's recovery (at-least-once). Events published while the latch is
        set remain uncheckpointed until restart, so the replay can write a
        second row with the same quality_event_id. The collector does not
        reconcile or deduplicate those rows; that is a downstream
        responsibility. Fail closed.

        The latch and its reason are recorded FIRST and unconditionally; the log
        line and the one-time operator alert come after and are each contained,
        so a failing logger or alert backend can neither undo the latch, mask the
        original reason, nor raise into the caller (several callers, e.g.
        ``_latch_failure``, are documented non-raising)."""
        self._ensure_quality_state()
        first = not self._quality_checkpoint_blocked
        if first:
            self._quality_checkpoint_block_reason = why
        self._quality_checkpoint_blocked = True
        try:
            logger.error("quality_checkpoint_blocked", reason=why)
        except Exception:  # noqa: BLE001 - the latch above is the record; logging is best-effort
            pass
        if first:
            self._alert_quality_checkpoint_blocked(why)

    def _alert_quality_checkpoint_blocked(self, why: str) -> None:
        """ONE operator alert, on the first transition into the checkpoint-blocked
        state. Never touches ``quality_writer``, ``_persist_quality_event`` or the
        WAL (so it cannot re-enter failure reporting), never raises, and marks
        itself sent BEFORE sending so a failing or re-entrant backend cannot
        produce a second alert. The state it reports is already latched."""
        if getattr(self, "_quality_checkpoint_block_alerted", False):
            return
        self._quality_checkpoint_block_alerted = True
        try:
            send_telegram_alert(
                f"QUALITY WAL CHECKPOINT BLOCKED [FUTURES]: reason={why}. Checkpointing is blocked for the "
                "rest of this process. Quality events may keep publishing to Parquet while their WAL records "
                "stay uncheckpointed, so the quality WAL can keep growing and the next start will replay "
                "them (duplicate rows with the same quality_event_id are possible). Market data and raw "
                "capture are unaffected. Inspect the quality WAL directory and restart/recover when safe.")
        except Exception as exc:  # noqa: BLE001 - alerting fails open; the latch is already set
            try:
                logger.error("quality_checkpoint_blocked_alert_failed", error=f"{type(exc).__name__}: {exc}")
            except Exception:  # noqa: BLE001
                pass

    def quality_channel_status(self) -> dict:
        """Operator view of the quality channel (same shape as the Bybit runner). Market-data
        capture is unaffected by a degraded channel. The fields answer different questions and
        must not be read as one health flag:

        * ``quality_degraded`` / ``quality_failure``  -- the quality Parquet writer itself failed.
        * ``quality_checkpoint_blocked`` / ``quality_checkpoint_block_reason`` -- the quality WAL
          can no longer advance its checkpoint for this process. The writer may still be accepting
          and publishing rows; uncheckpointed WAL records stay on disk and are replayed at the next
          start (at-least-once; duplicates carry the same ``quality_event_id``).
        * ``quality_events_wal_unconfirmed`` -- a WAL append raised after assigning an id, or a
          segment's publication could not be proven: a record MAY exist; neither retained nor lost.
        * ``quality_events_lost`` -- no WAL record and no usable writer: not durably recorded.
        * ``quality_events_wal_only`` -- the WAL record is established and will be replayed."""
        self._ensure_quality_state()
        record = getattr(self, "quality_degraded", None)
        wal = getattr(self, "_quality_wal", None)
        return {"quality_degraded": record is not None,
                "quality_wal_capacity_exhausted": bool(getattr(wal, "capacity_exhausted", False)),
                "quality_wal_capacity_reason": getattr(wal, "capacity_reason", None),
                "quality_wal_max_bytes": getattr(wal, "max_total_bytes", None),
                "quality_events_wal_only": self._quality_events_wal_only,
                "quality_events_wal_unconfirmed": self._quality_events_wal_unconfirmed,
                "quality_events_lost": self._quality_events_unrecorded,
                "quality_checkpoint_blocked": self._quality_checkpoint_is_blocked(),
                "quality_checkpoint_block_reason": getattr(self, "_quality_checkpoint_block_reason", None),
                "quality_failure": None if record is None else record.as_dict()}

    def _on_quality_segment_durable(self, path, record_count: int) -> None:
        """Called by quality_writer only after a segment is durably published.
        Never raises: any failure latches the checkpoint closed instead."""
        self._ensure_quality_state()
        durable = self._quality_segment_seqs
        self._quality_segment_seqs = set()
        if durable:
            self._quality_wal_inflight -= durable
            top = max(durable)
            if self._quality_published_hwm is None or top > self._quality_published_hwm:
                self._quality_published_hwm = top
        wal = getattr(self, "_quality_wal", None)
        if wal is None or self._quality_checkpoint_is_blocked() or self._quality_published_hwm is None:
            return
        inflight = self._quality_wal_inflight
        target = (min(inflight) - 1) if inflight else self._quality_published_hwm
        try:
            wal.checkpoint(up_to_seq=target)
        except Exception as exc:  # noqa: BLE001 - must not escape the writer hook
            self._block_quality_checkpoint(f"checkpoint_write_failed:{type(exc).__name__}")
            return
        try:
            wal.maybe_rotate_for_size()
        except Exception:  # noqa: BLE001 - rotation failure leaves the WAL handle valid
            logger.error("quality_wal_rotation_failed")

    def _quality_publish_if_due(self) -> None:
        """Idle tick: segment_seconds is only evaluated inside write(), so a
        stream that goes quiet would otherwise keep its tail in RAM."""
        try:
            self.quality_writer.publish_if_due()
        except Exception as exc:  # noqa: BLE001
            self._block_quality_checkpoint(f"idle_publish_failed:{type(exc).__name__}")
            # A typed fatal here is the quality writer failing while quiet: latch the
            # channel degraded (reported once) instead of leaving it a log line.
            if self._is_quality_channel_failure(exc) and getattr(self, "quality_degraded", None) is None:
                self._note_quality_writer_failure(exc)

    def _drain_quality_queue_sync(self) -> None:
        """Persist whatever is still queued, synchronously. Shutdown-only:
        events enqueued after the persistence loop has exited (e.g. websocket
        disconnect events raised by ws_client.stop()) would otherwise never
        leave the queue."""
        queue = getattr(self, "_quality_queue", None)
        if queue is None:
            return
        while True:
            try:
                event = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                self._persist_quality_event(event)
            except Exception:  # noqa: BLE001
                logger.error("quality_event_shutdown_drain_persist_failed")
                self._block_quality_checkpoint("shutdown_drain_persist_failed")
            finally:
                queue.task_done()

    def _recover_quality_wal(self, wal_dir) -> None:
        """Startup recovery: discover the WAL, recover exact event content
        not yet checkpointed, replay it into Parquet, and checkpoint only
        the contiguous prefix that actually succeeded. Extracted as its
        own method (called once, from __init__) so it is directly
        testable against the real code path rather than a reimplementation
        of its logic.
        """
        self._ensure_quality_state()
        self._quality_wal_recovery_error = None
        resume_seq = QualityEventWAL.highest_recovered_seq(wal_dir)
        try:
            recovered_events = QualityEventWAL.recover(wal_dir)
        except QualityWALCorruption as exc:
            # A corrupted WAL must be loud, never silently treated as
            # healthy -- but it must also not prevent the collector from
            # starting, since market-data capture is the primary mission
            # and the corrupted file is preserved on disk for forensic
            # inspection (never deleted here). Reported as a durable
            # quality event as soon as quality_writer exists, below.
            recovered_events = []
            self._quality_wal_recovery_error = str(exc)
            # The corrupt file (and any valid records in it we could not
            # replay) must survive: a later checkpoint(N) would otherwise
            # cover its seqs and the file would be deleted -- destroying the
            # evidence. Checkpointing stays off until an operator resolves it.
            self._block_quality_checkpoint("wal_corruption_on_startup")
        else:
            # Recovery successfully parsed every WAL record; only now is it
            # safe to remove old, strictly validated files already covered
            # by the durable checkpoint.
            QualityEventWAL.cleanup_fully_checkpointed_files(wal_dir)
        self._quality_wal = QualityEventWAL(wal_dir, start_seq=resume_seq)
        if self._quality_wal_recovery_error is not None:
            self._emit_quality_event({
                "stream": "quality_events", "event_type": QualityEventType.ERROR,
                "reason": f"quality_wal_corruption_on_startup:{self._quality_wal_recovery_error}",
                "rows_lost": None})
        # Persist recovered events, but only checkpoint the CONTIGUOUS
        # prefix that actually succeeded (Area 4 / Area 14: checkpointing
        # seq N must prove every seq <= N is durably in Parquet -- if
        # event 7 fails to persist while 8/9 (say) would have succeeded,
        # checkpointing past 7 would falsely mark it durable). recover()
        # returns events in strict seq order, so the first failure is the
        # correct place to stop advancing the checkpoint; the failed event
        # and everything after it remain in the WAL, recoverable on the
        # next restart (potentially re-persisted as a duplicate row with the
        # same quality_event_id: replay is at-least-once and the collector
        # does not deduplicate; reconciliation is downstream).
        # Replay in strict seq order. Each event is tracked in-flight BEFORE
        # it is written and only leaves in-flight when its segment is
        # published, so the checkpoint can only ever reach the contiguous
        # successfully-published prefix: on a failure at seq k the event and
        # everything after it stay in the WAL (k remains in-flight, which
        # also pins the checkpoint below k for the rest of this process).
        replay_failed = False
        for recovered in recovered_events:
            try:
                # "_wal_seq" = this event is already durably in the WAL (do
                # not append again); quality_event_id flows through so a
                # replayed row carries the same id as any earlier copy,
                # which lets a downstream consumer identify the duplicate
                # (no automatic deduplication exists in the collector).
                self._persist_quality_event({**recovered, "_wal_seq": recovered["seq"]})
            except Exception:
                logger.error("quality_event_recovery_persist_failed", seq=recovered.get("seq"))
                replay_failed = True
                break
        # Batching: replayed rows sit in quality_writer's buffer. Force them
        # durable now (one-time startup cost); on_segment_durable then
        # checkpoints exactly the contiguous published prefix. A crash before
        # this completes leaves them in the WAL, uncheckpointed -- correct.
        try:
            self.quality_writer.publish_open_segment()
        except Exception as exc:
            logger.error("quality_event_recovery_publish_failed", error=str(exc))
            self._block_quality_checkpoint("recovery_publish_failed")
        if replay_failed:
            self._block_quality_checkpoint("recovery_persist_failed")

    def _websocket_quality_event(self, event_type, reason, connection_id=None, stream_group="websocket"):
        """Websocket hot path: bounded non-blocking enqueue only, never parquet I/O.

        The WAL append IS a synchronous file write -- not "never I/O" in
        the literal sense -- but it is the same lightweight, already-
        proven-cheap-enough operation the marker file this replaces was
        already doing on this exact call site (open+write+flush+fsync+
        replace, every single event); only the payload written grew from
        a queue-depth integer to the actual event. See quality_wal.py's
        own docstring for why this remains appropriate for a low-volume
        stream. Parquet I/O itself still only ever happens later, off
        this path, in _quality_persistence_loop.
        """
        event={"exchange":"BINANCE", "stream":stream_group, "event_type":event_type, "reason":reason,
               "connection_id":connection_id, "local_ts":int(time.time()*1000)}
        if not hasattr(self, "_quality_queue"):
            self._emit_quality_event(event)
            return
        self._ensure_quality_state()
        wal = getattr(self, "_quality_wal", None)
        if wal is not None:
            try:
                event_type_value = event_type.value if isinstance(event_type, QualityEventType) else event_type
                event_id = wal.append({**event, "event_type": event_type_value})
            except QualityWALCapacityExceeded as wal_exc:
                # This event was refused before any bytes were written. Do not
                # attach an ID or seq: only the direct, forced Parquet publish
                # can establish its durability now.
                event["_wal_capacity_exhausted"] = True
                self._note_quality_wal_capacity_exhausted(
                    reason=wal_exc.reason, current_bytes=wal_exc.current_bytes,
                    requested_bytes=wal_exc.requested_bytes, limit_bytes=wal_exc.max_total_bytes)
                try:
                    self._persist_quality_event(event)
                except Exception:  # noqa: BLE001
                    logger.error("quality_event_capacity_direct_persist_failed", reason=reason)
                    self._block_quality_checkpoint("wal_capacity_direct_persist_failed")
                return
            except Exception as wal_exc:  # noqa: BLE001 - any append failure, not only OSError
                # The WAL could not durably record this event. Queueing it
                # would reintroduce the loss window, so it is persisted
                # directly instead -- and _persist_quality_event, seeing no
                # WAL provenance, force-publishes it (never a RAM-only row).
                # The id was assigned before the failure and bytes may already
                # be in the WAL file (write+flush ok, fsync failed): stamping
                # it gives any WAL-resident copy the same id, so a downstream
                # consumer can identify it (it is not removed automatically).
                logger.error("quality_wal_append_failed", reason=reason)
                event["quality_event_id"] = getattr(wal_exc, "quality_event_id", None)
                try:
                    self._persist_quality_event(event)
                except Exception:  # noqa: BLE001
                    # WAL and direct persistence both failed. A WAL copy may
                    # exist and may be the only record: never let a later
                    # checkpoint cover it. Not re-raised -- this is the
                    # websocket hot path and market data is the mission.
                    logger.error("quality_event_double_failure_possible_loss", reason=reason)
                    self._block_quality_checkpoint("wal_and_direct_persist_both_failed")
                return
            event["quality_event_id"] = event_id
            event["_wal_seq"] = int(event_id.rsplit("-", 1)[-1])
            # In-flight from the moment it is WAL-durable: while this event
            # sits in the queue it must hold the checkpoint back.
            self._quality_wal_inflight.add(event["_wal_seq"])
        try:
            self._quality_queue.put_nowait(event)
        except asyncio.QueueFull:
            # The queue stays bounded. The event is durable in the WAL but
            # will never be drained by the persistence loop, and its seq is
            # in-flight (it would pin the checkpoint forever), so it is
            # persisted directly and synchronously -- deliberate: overflow
            # means persistence has fallen behind. If even that fails the
            # checkpoint is latched off and the WAL copy is retained for
            # restart recovery.
            self._quality_overflow += 1
            logger.error("quality_event_queue_overflow", overflowed=self._quality_overflow)
            try:
                self._persist_quality_event(event)
            except Exception:  # noqa: BLE001
                self._quality_overflow_unpersisted += 1
                logger.error("quality_event_overflow_direct_persist_failed", reason=reason)
                self._block_quality_checkpoint("queue_overflow_direct_persist_failed")

    async def _quality_persistence_loop(self):
        """Drain the queue into Parquet. Must survive any single bad event: a
        dead loop leaves the queue undrained (shutdown no longer waits for it
        without a deadline -- _stop_quality_consumer -- and drains what is
        left synchronously). Checkpointing is NOT done here -- it happens only
        in _on_quality_segment_durable, after a segment is published."""
        while (self.running or not self._quality_queue.empty()) and not getattr(self, "_quality_loop_stop", False):
            try:
                event = await asyncio.wait_for(self._quality_queue.get(), timeout=0.1)
            except asyncio.TimeoutError:
                self._quality_publish_if_due()
                continue
            try:
                self._persist_quality_event(event)
            except Exception:  # noqa: BLE001
                logger.error("quality_event_persist_failed", event_type=event.get("event_type"))
                self._block_quality_checkpoint("persist_failed")
            finally:
                self._quality_queue.task_done()
        if self._quality_overflow:
            # Not a loss claim: overflowed events were persisted directly (or,
            # if that failed, retained in the WAL with checkpointing latched).
            unpersisted = getattr(self, "_quality_overflow_unpersisted", 0)
            try:
                self._persist_quality_event({"stream": "quality_events", "event_type": QualityEventType.ERROR,
                    "reason": f"quality_queue_overflow:total={self._quality_overflow};"
                              f"direct_persist_failed={unpersisted};wal_retained_for_recovery={unpersisted}",
                    "rows_lost": None})
            except Exception:  # noqa: BLE001
                logger.error("quality_overflow_summary_persist_failed")
                self._block_quality_checkpoint("overflow_summary_persist_failed")

    def _note_quality_wal_capacity_exhausted(self, *, reason, current_bytes=None,
                                             requested_bytes=None, limit_bytes=None) -> None:
        """Report aggregate WAL admission closure once, without re-entering quality persistence."""
        self._ensure_quality_state()
        if getattr(self, "_quality_wal_capacity_alerted", False):
            return
        self._quality_wal_capacity_alerted = True  # set before logger/alert callbacks
        try:
            logger.error("quality_wal_capacity_exhausted", reason=reason,
                         current_bytes=current_bytes, requested_bytes=requested_bytes,
                         limit_bytes=limit_bytes)
        except Exception:  # noqa: BLE001
            pass
        try:
            send_telegram_alert(
                "QUALITY WAL CAPACITY EXHAUSTED [FUTURES]: aggregate WAL admission is closed "
                f"(reason={reason}, limit_bytes={limit_bytes}, current_bytes={current_bytes}). "
                "Uncheckpointed WAL evidence is preserved. New quality events are force-published "
                "to Parquet without a WAL ID; inspect/recover the WAL and restart after correction.")
        except Exception:  # noqa: BLE001 - alerting cannot affect market-data capture
            pass

    def _persist_quality_event(self, event: dict):
        """Single choke point for EVERY quality event (~14 direct call sites,
        the queue loop, recovery replay).

        Durability contract: before the event can sit in quality_writer's RAM
        buffer it is WAL-protected -- unless it already carries WAL provenance
        (``_wal_seq``: appended by _websocket_quality_event or replayed from
        the WAL). Its seq is tracked in-flight until the segment holding it is
        published; only then may the checkpoint cover it.

        If the WAL append fails the event is still written, but with no WAL
        record backing it a crash would lose it, so the open segment is
        force-published immediately (the pre-batching behaviour). If THAT
        fails the exception propagates and the checkpoint is latched.
        """
        self._ensure_quality_state()
        wal = getattr(self, "_quality_wal", None)
        wal_seq = event.get("_wal_seq")
        wal_protected = wal is not None and wal_seq is not None
        capacity_fallback = bool(event.get("_wal_capacity_exhausted", False))
        if (wal is not None and wal_seq is None and not capacity_fallback
                and getattr(wal, "capacity_exhausted", False)):
            capacity_fallback = True
            event = {k: v for k, v in event.items()
                     if k not in {"quality_event_id", "_wal_seq"}}
            event["_wal_capacity_exhausted"] = True
            self._note_quality_wal_capacity_exhausted(
                reason=wal.capacity_reason, current_bytes=getattr(wal, "_capacity_current_bytes", None),
                limit_bytes=wal.max_total_bytes)
        if wal is not None and wal_seq is None and not capacity_fallback:
            event_type_for_wal = event.get("event_type", QualityEventType.ERROR.value)
            if isinstance(event_type_for_wal, QualityEventType):
                event_type_for_wal = event_type_for_wal.value
            try:
                event_id = wal.append({k: v for k, v in event.items() if k != "_wal_seq"}
                                      | {"event_type": event_type_for_wal})
            except QualityWALCapacityExceeded as exc:
                capacity_fallback = True
                event = {k: v for k, v in event.items()
                         if k not in {"quality_event_id", "_wal_seq"}}
                event["_wal_capacity_exhausted"] = True
                self._note_quality_wal_capacity_exhausted(
                    reason=exc.reason, current_bytes=exc.current_bytes,
                    requested_bytes=exc.requested_bytes, limit_bytes=exc.max_total_bytes)
            except Exception as exc:  # noqa: BLE001
                logger.error("quality_event_wal_append_failed_in_persist", reason=event.get("reason"))
                failed_id = getattr(exc, "quality_event_id", None)
                if failed_id is not None:
                    # A WAL record may exist for this seq: keep the id on the
                    # row and hold the checkpoint below it until published.
                    if event.get("quality_event_id") is None:
                        event = {**event, "quality_event_id": failed_id}
                    wal_seq = int(failed_id.rsplit("-", 1)[-1])
            else:
                if event.get("quality_event_id") is None:
                    event = {**event, "quality_event_id": event_id}
                wal_seq = int(event_id.rsplit("-", 1)[-1])
                wal_protected = True
        if wal_seq is not None:
            self._quality_wal_inflight.add(wal_seq)
        rows_lost = event.get("rows_lost"); event_type = event.get("event_type", QualityEventType.ERROR.value)
        if isinstance(event_type, QualityEventType): event_type = event_type.value
        local_ts = event.get("local_ts", event.get("timestamp", int(time.time() * 1000)))
        row = {"timestamp": local_ts, "exchange": event.get("exchange", "BINANCE"),
            "stream": event.get("stream", "orderbook"), "event_type": event_type, "reason": event.get("reason", ""),
            "gap_size_ms": event.get("gap_size_ms"), "rows_lost": None if rows_lost is None else str(rows_lost),
            "quality_state": event.get("new_state", event.get("quality_state", self.binance_book.state.state.value)),
            "connection_id": event.get("connection_id"), "previous_state": event.get("previous_state"),
            "new_state": event.get("new_state"), "expected_previous_update_id": event.get("expected_previous_update_id"),
            "actual_previous_update_id": event.get("actual_previous_update_id"), "update_id": event.get("update_id"), "first_update_id": event.get("first_update_id"),
            "previous_update_id": event.get("previous_update_id"),
            "local_receive_ts": event.get("local_receive_ts"), "local_process_ts": event.get("local_process_ts", local_ts),
            "quality_event_id": event.get("quality_event_id")}

        def _bind(_token, _seq=wal_seq):
            # Called by the writer AFTER any hour rollover and BEFORE the
            # append: attributes this seq to the segment that will really
            # contain the row (a rollover publish inside this very write()
            # must not count an event that is not in that segment).
            if _seq is not None:
                self._quality_segment_seqs.add(_seq)
        if getattr(self, "quality_degraded", None) is not None:
            # F5: the quality channel is latched degraded. When the event's WAL copy
            # (written above) is established it is the durable record and the next
            # start replays it. Calling the failed writer again would only raise
            # once per event, so it is never called -- but the event is counted as
            # WAL-retained ONLY if that WAL protection is real.
            self._account_degraded_quality_event(event, wal_protected=wal_protected, wal_seq=wal_seq)
            return
        try:
            self.quality_writer.write(row, bind=_bind)
        except Exception as exc:
            # Writer state is now uncertain (buffered row may be unwritable,
            # segment may be half-closed): fail closed, event stays in WAL.
            self._block_quality_checkpoint("quality_writer_write_failed")
            self._note_quality_writer_failure(exc)
            self._account_tripping_quality_event(exc, event, wal_protected=wal_protected, wal_seq=wal_seq)
            raise
        if not wal_protected:
            try:
                self.quality_writer.publish_open_segment()
            except Exception as exc:
                self._block_quality_checkpoint("unprotected_event_publish_failed")
                self._note_quality_writer_failure(exc)
                self._account_tripping_quality_event(exc, event, wal_protected=wal_protected, wal_seq=wal_seq,
                                                     record_admitted=True)
                raise

    def _account_tripping_quality_event(self, exc: BaseException, event: dict, *,
                                        wal_protected: bool, wal_seq, record_admitted=None) -> None:
        """Account the event whose own write/publish latched the quality channel degraded.

        What happened to THIS event decides the counters, from the writer's evidence
        (``exc.trigger_event_disposition``; ``record_admitted=True`` when the failure came from
        ``publish_open_segment()`` after ``write()`` returned, where the exception carries no
        admission evidence of its own) and the WAL evidence:

        * published, and no WAL record exists (``wal_seq is None``)  -> nothing: it is in a
          durable segment and nothing will replay it. No loss alert.
        * published, WAL seq covered by the durable checkpoint       -> nothing: it will not be
          replayed, so it is neither "WAL-only" nor lost.
        * published but NOT checkpoint-covered (checkpoint blocked/failed)  -> by WAL evidence
          (it will be replayed at next start).
        * unconfirmed with no WAL evidence                            -> ``wal_unconfirmed``
          (publication unproven; never claimed lost, no loss alert).
        * lost (never admitted / unpublished)                         -> by WAL evidence, as before.

        Only a typed fatal that really degraded the channel qualifies; an ordinary exception
        leaves the counters alone. Never raises, so the caller's ``raise`` is never replaced."""
        try:
            if self._is_quality_channel_failure(exc) and getattr(self, "quality_degraded", None) is not None:
                disposition = exc.trigger_event_disposition(record_admitted=record_admitted)
                if disposition == EVENT_PUBLISHED and (
                        wal_seq is None or self._quality_seq_checkpoint_covered(wal_seq)):
                    return
                self._account_degraded_quality_event(
                    event, wal_protected=wal_protected, wal_seq=wal_seq,
                    publication_unproven=(disposition == EVENT_UNCONFIRMED))
        except Exception as account_exc:  # noqa: BLE001 - accounting must not mask the original failure
            logger.error("quality_event_accounting_failed", error=f"{type(account_exc).__name__}: {account_exc}")

    def _quality_seq_checkpoint_covered(self, seq) -> bool:
        """True when the DURABLE WAL checkpoint already covers ``seq``: that event's segment was
        published and the checkpoint advanced past it, so the next start will NOT replay it."""
        wal = getattr(self, "_quality_wal", None)
        try:
            return wal is not None and seq is not None and wal.checkpointed_seq >= seq
        except Exception:  # noqa: BLE001 - evidence unavailable == not proven covered
            return False

    def _note_quality_writer_failure(self, exc: BaseException) -> None:
        """Latch the quality channel degraded when ITS writer raised a typed fatal.

        Never calls ``quality_writer`` or ``_persist_quality_event`` (see
        ``_report_storage_failure``), so a failed quality writer cannot recurse
        into itself. Ordinary exceptions are ignored here: they stay ordinary."""
        if isinstance(exc, FatalStorageError):
            self._on_fatal_storage(exc, origin="quality_writer")

    @staticmethod
    def _is_quality_channel_failure(exc: BaseException) -> bool:
        """True only for a TYPED fatal that failure_topology maps to the quality
        channel. Never by RuntimeError-ness, message text or class name."""
        return isinstance(exc, FatalStorageError) and classify_stream(exc.stream)[0] == VERDICT_DEGRADE_QUALITY

    def _emit_quality_event(self, event: dict) -> bool:
        """Quality telemetry from a MARKET-DATA path (handlers, recovery, drains).

        ``_persist_quality_event`` keeps its contract -- it raises when the quality
        writer fails -- because the persistence loop, startup replay, overflow and
        shutdown paths need that to hold the checkpoint back. A market-data task
        needs the opposite: telemetry failing must not take the task down.

        Contained here, and ONLY here: a typed fatal of the quality channel
        (``_persist_quality_event`` has already latched the channel degraded, blocked
        the checkpoint and reported it once through the recursion-safe path; the
        event itself is in the WAL). Everything else propagates unchanged: ordinary
        exceptions (P0-1) and a typed fatal of any OTHER stream, which is never
        quality telemetry. Returns ``True`` when the event reached the writer."""
        try:
            self._persist_quality_event(event)
        except FatalStorageError as exc:
            if not self._is_quality_channel_failure(exc):
                raise
            if getattr(self, "quality_degraded", None) is None:
                self._note_quality_writer_failure(exc)
            return False
        return True

    def _account_degraded_quality_event(self, event: dict, *, wal_protected: bool, wal_seq,
                                        publication_unproven: bool = False) -> None:
        """Count one event that arrived while the quality channel is degraded.

        * ``wal_protected``  -> its WAL record is established: ``_quality_events_wal_only``.
        * ``wal_seq`` only   -> the append raised after assigning an id (write ok, fsync
          failed): a record MAY exist. Not proven, so NOT "retained"; its seq is already
          in-flight, which pins the checkpoint below it.
        * neither            -> no WAL record and the writer is dead: the event is LOST.
          Reported as unrecorded, never as retained, and never through the failed
          writer: a bounded log line (first, then powers of two) plus ONE operator alert.

        Nothing here advances a checkpoint, calls ``quality_writer``, or raises."""
        if wal_protected:
            self._quality_events_wal_only = getattr(self, "_quality_events_wal_only", 0) + 1
            return
        if wal_seq is not None or publication_unproven:
            # ``publication_unproven``: no WAL evidence, but the writer could not establish
            # whether the event's segment is durable (renamed, directory fsync failed). Not lost.
            self._quality_events_wal_unconfirmed = getattr(self, "_quality_events_wal_unconfirmed", 0) + 1
            return
        count = getattr(self, "_quality_events_unrecorded", 0) + 1
        self._quality_events_unrecorded = count
        if not self._quality_checkpoint_is_blocked():
            self._block_quality_checkpoint("quality_event_unrecorded")
        try:
            if count & (count - 1) == 0:            # 1, 2, 4, 8, ...: bounded, never one line per event
                logger.error("quality_event_unrecorded", count=count, stream=event.get("stream"),
                             event_type=str(event.get("event_type")), reason=event.get("reason"),
                             wal_only=getattr(self, "_quality_events_wal_only", 0))
            if not getattr(self, "_quality_unrecorded_alerted", False):
                self._quality_unrecorded_alerted = True
                send_telegram_alert(
                    "QUALITY EVENTS UNRECORDED: the quality channel is degraded and the quality WAL could "
                    "not record events (market data and raw capture unaffected; quality telemetry is being lost)")
        except Exception as exc:  # noqa: BLE001 - reporting a loss must never raise into market data
            logger.error("quality_event_unrecorded_report_failed", error=f"{type(exc).__name__}: {exc}")

    def _record_book_quality(self, kind, reason, transition=None, event=None):
        transition=transition or self.binance_book.last_transition
        event=event or getattr(transition, "event", None)
        self._emit_quality_event({"exchange":"BINANCE", "stream":"orderbook", "event_type":kind, "reason":reason,
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
        if getattr(self, "_recovery_closed", False):
            # Shutdown has begun. The ingest worker stays alive while it drains, and this
            # is on its on_message path: a recovery started now would be a REST producer
            # that nobody cancels and that reaches raw_rest_writer.
            return False
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
        if getattr(self, "_recovery_closed", False):
            return False        # shutdown began before this recovery did anything
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
            if self._capture_rest(RawRestRecord(
                request_ts=request_ts, response_receive_ts=receive_ts,
                endpoint=BINANCE_DEPTH_SNAPSHOT_URL, purpose="orderbook_snapshot",
                request_params={"symbol": SYMBOL, "limit": 1000},
                http_status=status, ok=True, payload=body, symbol=SYMBOL,
                local_process_ts=process_ts)) is False:
                # F5: the raw_rest boundary is lost (terminal latched). A snapshot
                # that was not durably captured must not bridge the book.
                return False
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
        except FatalStorageError as exc:
            # F5: must precede the broad handlers below, which would report a
            # dead writer as "snapshot_http_error" and keep going. Classified by
            # the failing writer's own stream (orderbook raw writer -> isolate).
            self._on_fatal_storage(exc, origin="recovery")
            return False
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
        # P0-4: normalize() admits identities up front; any exit (return, validator
        # rejection, writer/dedup exception) must still end the message so an
        # admitted-but-unwritten identity never suppresses a later redelivery.
        try:
            self._handle_binance_trade_events(raw, stream, local_receive_ts)
        finally:
            segment_dedup = getattr(self, "segment_dedup", None)
            if segment_dedup is not None:
                segment_dedup.end_message()

    def _handle_binance_trade_events(self, raw: dict, stream: str, local_receive_ts: int):
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
                    "instrument_key": instrument_key}, bind=bind_arg(getattr(self, "segment_dedup", None), event))
            if trade_id is None:
                self.stream_counters["trades"]["rejected"] += 1
                self._record_validation_rejection("trades", "legacy_trade_id_not_lossless")
                self._emit_quality_event({"stream":"trades", "event_type":QualityEventType.ERROR, "reason":"legacy_trade_id_not_lossless"})
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
            self._emit_quality_event({
                "stream": stream_name, "event_type": QualityEventType.ERROR,
                "reason": f"symbol_contradicts_configured_instrument:{native_symbol}",
                "rows_lost": 1, "local_ts": int(time.time() * 1000)})
            return None
        return BINANCE_USDM_BTCUSDT.key

    def _handle_liquidation(self, data: dict, stream: str, local_receive_ts: int):
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
            # P0-7: compute_liquidation_features's "timestamp" is a fresh
            # time.time() call taken when this handler runs -- processing
            # time, not receive time. Before this fix, "local_timestamp"
            # silently duplicated that same value, so (as with markprice
            # before its P0-6 fix) there was no genuine availability clock
            # for liquidation at all. local_receive_ts is the frame's real
            # receive time, captured once at handle_message's entry (the
            # same clock the replay-side CanonicalLiquidationEvent uses via
            # BinanceAdapter.normalize()), so live and replay now agree.
            features["local_timestamp"] = local_receive_ts
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
        except FatalStorageError as exc:
            # F5: before the blanket handler, which used to swallow a dead
            # liquidation writer as a "rejected" frame and keep calling it.
            # Classified by the failing writer's own stream: a quality-channel
            # fatal raised from inside this handler degrades QUALITY, it does not
            # isolate liquidation; an unmapped stream terminates.
            self._on_fatal_storage(exc, origin="route:liquidation")
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
        # F5-QA-04: startup disk preflight. The app is fully constructed (so shutdown() is
        # safe) and no websocket client has started yet.
        if await self._storage_pressure_preflight_failed():
            return
        logger.info("Starting Collector Application")
        logger.info("Collector WebSocket subscription", url=BINANCE_PUBLIC_WS_URL, requested_streams=self._requested_streams(BINANCE_PUBLIC_WS_URL))
        logger.info("Collector WebSocket subscription", url=BINANCE_MARKET_WS_URL, requested_streams=self._requested_streams(BINANCE_MARKET_WS_URL))
        send_telegram_alert("Collector Application Started")
        self.running = True
        # Every exit from here on reaches the finalization in the ``finally`` below: a normal return, an
        # exception from the startup recovery or from any task, and the caller cancelling start().
        # Nothing is closed to make start() return: shutdown() closes a writer only once every
        # producer that can reach it has ended, and arms a retry for the ones that have not.
        try:
            self._quality_task = asyncio.create_task(self._quality_persistence_loop())
            # F5: latent writer-failure detection + the controlled terminal shutdown.
            self.tasks.append(asyncio.create_task(self._failure_supervisor_loop()))
            self.tasks.append(asyncio.create_task(self._storage_pressure_loop()))
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._async_shutdown(s)))
            for ws_client in self.ws_clients:
                self.tasks.append(asyncio.create_task(ws_client.start()))
            for ws_client in self.ws_clients:
                connected = await ws_client.wait_connected(timeout_seconds=30.0)
                if not connected:
                    logger.warning("ws_client_pre_connect_timeout", url=ws_client.url)
            await self._run_startup_recovery()
            if not getattr(self, "_recovery_closed", False):
                self.tasks.append(asyncio.create_task(self.health_monitor.start()))
                self._oi_task = asyncio.create_task(self._poll_openinterest())
                self.tasks.append(self._oi_task)
                self.tasks.append(asyncio.create_task(self._verify_startup_streams()))
            # else: shutdown began while start() was still starting up, so nothing new may be started.
            # start() only gets here once the startup recovery TASK has ended. A recovery that swallows
            # its cancel keeps start() parked inside _run_startup_recovery() (see its docstring).
            try:
                await asyncio.gather(*self.tasks)
            except asyncio.CancelledError:
                # gather() raises CancelledError when a CHILD was cancelled -- what the controlled
                # shutdown does to every task -- and that is the normal way out. start() itself being
                # cancelled (its caller's request) is not swallowed: the finalization runs, then it
                # propagates. Only this wait is covered: a CancelledError from the startup above
                # (e.g. a recovery cancelled for its own reasons) is never read as "normal".
                current = asyncio.current_task()
                if current is not None and current.cancelling():
                    raise
        finally:
            # F5: every step is guarded and ``shutdown()`` is reached on EVERY
            # path. Quality reporting is telemetry: a failing quality writer here
            # used to raise out of this block, skip ``shutdown()`` (all writers
            # left unclosed) and turn the intended exit 70 into a traceback.
            self._recovery_closed = True
            try:
                try:
                    recovery = self._recovery_task
                    if recovery is not None and not recovery.done():
                        recovery.cancel()
                        # Bounded, like _async_shutdown and _stop_rest_producers: a recovery that
                        # swallows its cancel must not hold this finalizer (and so shutdown())
                        # hostage. Whether it ended is judged later by
                        # _storage_held_by_rest_producers(), never assumed from this wait returning:
                        # a survivor stays tracked, shutdown() keeps its reachable storage open,
                        # latches rest_producer_not_stopped and arms the late retry.
                        await asyncio.wait({recovery}, timeout=WS_FALLBACK_SLACK_S)
                        if not recovery.done():
                            logger.error("recovery_task_ignored_cancel", bound_s=WS_FALLBACK_SLACK_S)
                    if recovery is not None and recovery.done() and not recovery.cancelled():
                        error = recovery.exception()   # retrieved, never "never retrieved"; not a clean-up
                        if error is not None:
                            logger.error("recovery_task_failed_during_shutdown",
                                         error=f"{type(error).__name__}: {error}")
                except Exception as exc:  # noqa: BLE001
                    logger.error("shutdown_recovery_cancel_failed", error=f"{type(exc).__name__}: {exc}")
                self._drain_integrity_quality_events_safely()
                try:
                    # Bounded (see _stop_quality_consumer): the wait for the persistence loop used to be
                    # unbounded and ended only if a live loop kept finishing events -- not when it died
                    # while join() waited, stalled, or (3.11) was outpaced by a live producer. Nothing
                    # here is raised out of the finalization: the caller's cancellation or the original
                    # exception must stay what start() ends with.
                    await self._stop_quality_consumer(drain=True)
                except Exception as exc:  # noqa: BLE001
                    logger.error("shutdown_quality_consumer_stop_failed", error=f"{type(exc).__name__}: {exc}")
                self.running = False
            finally:
                try:
                    # Same ordering as _async_shutdown: clients retire first.
                    await self._stop_rest_producers(WS_FALLBACK_SLACK_S)
                    await self._stop_ws_clients()
                except Exception as exc:  # noqa: BLE001
                    logger.error("shutdown_ws_clients_stop_failed", error=f"{type(exc).__name__}: {exc}")
                finally:
                    self.shutdown()

    async def _run_startup_recovery(self) -> None:
        """Run the startup book recovery as a TRACKED task, so shutdown can cancel it and prove it ended.

        It used to be awaited inline in start()'s own task: a REST producer (HTTP snapshot, then
        ``_capture_rest`` -> ``raw_rest_writer``) that was neither ``_recovery_task`` (set only by
        ``_schedule_recovery``) nor in ``self.tasks``, and that ``_async_shutdown`` (a different
        task) could not cancel -- so ``raw_rest_writer`` was closed while its request was in
        flight and the late response was captured into a closed writer. The producer's whole
        lifetime is the task's lifetime, so ``_rest_producers_stopped()`` can now prove it.

        Outcomes (``_recovery_closed`` is set by every shutdown path before it cancels anything):

        * shutdown already began      -> no task is created: no producer starts after shutdown began.
        * finishes normally           -> returns; start() goes on to start the other tasks.
        * raises, shutdown NOT begun  -> the exception propagates out of start(), as the inline await did.
        * cancelled, shutdown NOT begun -> CancelledError propagates, as the inline await did; a
          cancelled task is only read as "stopped by shutdown" when shutdown actually began.
        * cancelled / raises after shutdown began -> returns, so start() reaches its tail (it starts
          nothing: ``_recovery_closed``). An error is logged and latched as a shutdown failure
          (non-zero status) rather than raised, because raising here would skip that tail.
        * start() itself cancelled    -> the producer is cancelled and waited for at most
          ``WS_FALLBACK_SLACK_S``, then CancelledError propagates through start()'s finalization.
          A producer that ignores the cancel stays tracked, so that finalization keeps exactly its
          storage open, latches ``rest_producer_not_stopped`` and arms the late retry.
        * IGNORES its cancel (shutdown path) -> this wait does not end: start() stays parked here
          and so does any runner (``asyncio.run``) around it. ``_async_shutdown`` has already latched
          ``rest_producer_not_stopped`` and ``shutdown()`` keeps open exactly the storage the producer
          can reach. When the producer does end, ``shutdown()``'s late retry closes the rest (it no
          longer depends on this wait). The application cannot stop such a coroutine; see
          docs/DEPLOYMENT.md "A REST producer that ignores cancellation". The wait is left unbounded
          because bounding it would only move the block into ``asyncio.run``'s task teardown.
        """
        if getattr(self, "_recovery_closed", False):
            return
        task = asyncio.create_task(self._recover_binance_book("startup"))
        self._startup_recovery_task = task
        try:
            await asyncio.wait({task})
        except BaseException:               # start() itself was cancelled: do not leave the producer behind
            task.cancel()
            # Bounded: a recovery that ignores cancellation must not hold start()'s own cancellation
            # hostage. If it is still alive afterwards it stays tracked (_startup_recovery_task):
            # start()'s finalization then keeps exactly its storage open, latches the failure and
            # shutdown() arms the late retry for when it does end.
            await asyncio.wait({task}, timeout=WS_FALLBACK_SLACK_S)
            if not task.done():
                logger.error("startup_recovery_ignored_cancel", bound_s=WS_FALLBACK_SLACK_S)
            elif not task.cancelled():
                task.exception()            # retrieved here; nothing is left to log as "never retrieved"
            raise
        shutting_down = getattr(self, "_recovery_closed", False)
        if task.cancelled():
            if not shutting_down:
                raise asyncio.CancelledError()
            return
        error = task.exception()
        if error is not None and shutting_down:
            logger.error("startup_recovery_failed_during_shutdown", error=f"{type(error).__name__}: {error}")
            self._record_shutdown_failure("startup_recovery_failed_during_shutdown")
            return
        task.result()                       # re-raise exactly what the inline await raised

    def _rest_producer_tasks(self):
        """Every task that can reach ``raw_rest_writer`` / ``oi_writer`` (REST capture)."""
        return [t for t in (getattr(self, "_recovery_task", None),
                            getattr(self, "_startup_recovery_task", None),
                            getattr(self, "_oi_task", None)) if t is not None]

    def _cancel_rest_producers(self) -> None:
        for task in self._rest_producer_tasks():
            if not task.done():
                task.cancel()

    async def _stop_rest_producers(self, bound_s: float) -> bool:
        """Cancel every REST producer and wait (at most ``bound_s``) until each has ended."""
        live = [t for t in self._rest_producer_tasks() if not t.done()]
        for task in live:
            task.cancel()
        if live:
            await asyncio.wait(live, timeout=bound_s)
        return self._rest_producers_stopped()

    async def _poll_openinterest(self):
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=5)
        self._ensure_failure_state()
        while self.running:
            if self.terminal_failure is not None:
                return  # raw-evidence boundary lost: no further polling, shutdown is in progress
            request_ts = int(time.time() * 1000)
            status = None
            body = None
            raw_captured = False
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
                if self._capture_rest(RawRestRecord(
                    request_ts=request_ts, response_receive_ts=receive_ts,
                    endpoint=OI_URL, purpose="open_interest",
                    request_params={"symbol": SYMBOL}, http_status=status,
                    ok=True, payload=body, symbol=SYMBOL,
                    local_process_ts=process_ts)) is False:
                    # F5: raw_rest lost => TERMINATE. The response was NOT durably
                    # captured, so it is not authoritative and must not feed the
                    # canonical OI writer.
                    return
                raw_captured = True
                self.stream_counters["openinterest"]["received"] += 1
                if self._route_is_isolated("openinterest"):
                    # F5: canonical OI writer failed earlier. Raw REST capture
                    # (above) keeps running -- replay can rebuild canonical OI
                    # from it -- but the failed writer is never called again.
                    self._note_short_circuit("openinterest")
                    await asyncio.sleep(OI_POLL_INTERVAL_S)
                    continue
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
                    self._emit_quality_event({
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
            except FatalStorageError as exc:
                # F5: before the blanket handler, which would log a second,
                # contradictory ok=False raw_rest record for a response that was
                # already captured ok=True. Classified by the failing writer's
                # own stream (openinterest -> isolate the canonical route).
                self._on_fatal_storage(exc, origin="route:openinterest")
            except Exception as e:
                # Previously log-only, so a REST outage left no trace in the
                # data and looked identical to a period of no change.
                logger.error("OI poll failed", error=str(e))
                if not raw_captured:
                    # F5: a response already captured ok=True must not also be
                    # recorded as a failed request (contradictory raw_rest state).
                    self._capture_rest(RawRestRecord(
                        request_ts=request_ts, response_receive_ts=None,
                        endpoint=OI_URL, purpose="open_interest",
                        request_params={"symbol": SYMBOL}, http_status=status,
                        ok=False, error=f"{type(e).__name__}:{e}", payload=body,
                        symbol=SYMBOL))
                self._emit_quality_event({
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

    def _quality_consumer_stopped(self) -> bool:
        """True when no quality persistence loop can still write ``quality_writer`` or its WAL: none was
        started, or it has ended (cleanly, with an exception, or cancelled)."""
        task = getattr(self, "_quality_task", None)
        return task is None or task.done()

    async def _stop_quality_consumer(self, *, drain: bool) -> bool:
        """End the quality persistence loop within fixed bounds. True only when it has ended.

        The loop used to be waited for without a deadline (``queue.join()`` and ``await _quality_task``,
        in ``start()``'s finalization and in ``_async_shutdown``), before any websocket client was asked to
        stop and before any writer was closed. Three things reproduced a wait that never ends, each leaving
        every writer -- market data included -- unclosed and the exit status unlatched until systemd's
        SIGKILL: the loop dying while ``join()`` waited (the ``done()`` check was made once, before the
        wait); a loop that is alive but not finishing events; and, on Python 3.11, a producer that keeps
        emitting so the queue never empties (``wait_for`` costs the loop about three event-loop turns per
        event there; 3.12 never yields while events are queued, so it did not show on 3.12 -- scheduling,
        not a guarantee). The quality queue is telemetry and cannot stand in front of that finalization.

        Sequence, each wait bounded:

        1. drain: ``drain=True`` waits until the loop has marked every queued event done OR the loop ended,
           at most ``QUALITY_DRAIN_BOUND_S``; ``drain=False`` (``_async_shutdown``) only waits for the
           loop to end by itself. A timeout is reported (``quality_queue_drain_incomplete``), never read as
           success.
        2. ``running`` is cleared; the loop gets ``QUALITY_CONSUMER_STOP_BOUND_S`` to leave on its own.
        3. Still alive: the loop is told to leave even though events are queued, and is cancelled
           (``WS_FALLBACK_SLACK_S``). Cancelling loses nothing: its only await is ``queue.get()`` (an event
           it has not taken stays queued; one it has taken is handled before its next await).
        4. Still alive after that: it can still write the quality writer, so ``shutdown()`` keeps that
           writer and its WAL open (see there), the failure ``quality_consumer_not_stopped`` is latched
           (non-zero ``exit_code``) and the late retry finalizes them when the loop does end.

        Events left in the queue are not dropped by a timeout or a stop: each has a WAL record and holds
        the checkpoint back, ``shutdown()`` drains them synchronously once no producer can still enqueue,
        and ones nobody drains stay in the WAL for the next start. The loop's own failure (an exception, or
        a ``BaseException`` that killed it) is read from the task and logged, never re-raised here: raising
        it out of a ``finally`` would replace the caller's cancellation or the original exception.
        ``CancelledError`` of the CURRENT task is not caught.
        """
        consumer = getattr(self, "_quality_task", None)
        if consumer is None:
            self.running = False
            return True
        if not consumer.done():
            if drain:
                joiner = asyncio.ensure_future(self._quality_queue.join())
                try:
                    await asyncio.wait({joiner, consumer}, timeout=QUALITY_DRAIN_BOUND_S,
                                       return_when=asyncio.FIRST_COMPLETED)
                    drained = joiner.done()
                finally:
                    if not joiner.done():
                        joiner.cancel()
                if not drained and not consumer.done():
                    logger.error("quality_queue_drain_incomplete", bound_s=QUALITY_DRAIN_BOUND_S,
                                 queued=self._quality_queue.qsize())
            else:
                self.running = False
                await asyncio.wait({consumer}, timeout=QUALITY_DRAIN_BOUND_S)
        self.running = False
        if not consumer.done():
            await asyncio.wait({consumer}, timeout=QUALITY_CONSUMER_STOP_BOUND_S)
        if not consumer.done():
            logger.error("quality_consumer_stop_timeout_cancelling", queued=self._quality_queue.qsize())
            self._quality_loop_stop = True
            consumer.cancel()
            await asyncio.wait({consumer}, timeout=WS_FALLBACK_SLACK_S)
        if not consumer.done():
            logger.error("quality_consumer_not_stopped", queued=self._quality_queue.qsize())
            self._record_shutdown_failure("quality_consumer_not_stopped")
            return False
        if not consumer.cancelled():
            error = consumer.exception()            # retrieved here; never raised into the caller
            if error is not None:
                logger.error("shutdown_quality_task_failed", error=f"{type(error).__name__}: {error}")
        return True

    async def _stop_ws_clients(self) -> bool:
        """Request every websocket client's shutdown and wait (bounded) for it to
        finish: receive loop ended, ingest queue drained, worker retired.

        This is the step that has to precede any writer close. The previous
        shutdown cancelled the ``start()`` tasks and closed every writer in one
        synchronous pass, so the separate ingest worker -- which ``start()``'s
        skipped drain tail would have stopped -- kept running against closed
        writers. Here the clients are asked to stop first, are NOT cancelled while
        they drain, and only an overrun of their own documented bound triggers the
        hard fallback (cancel the tasks; ``start()``'s ``finally`` then still
        retires the worker). Returns True only when every client is fully idle.
        Idempotent: an already-stopped client answers immediately.
        """
        clients = list(getattr(self, "ws_clients", ()))
        for client in clients:
            self._shutdown_step("ws_client_stop", client.stop)

        async def _wait(client, bound: float) -> bool:
            waiter = getattr(client, "wait_stopped", None)
            return True if waiter is None else bool(await waiter(bound))

        results = await asyncio.gather(
            *(_wait(c, getattr(c, "shutdown_deadline_s", 60.0)) for c in clients),
            return_exceptions=True)
        if all(result is True for result in results):
            return True
        logger.error("ws_client_graceful_stop_incomplete",
                     results=[repr(r) for r in results])
        for task in self.tasks:          # bounded fallback: start()'s finally retires the worker
            task.cancel()
        bound = max([getattr(c, "worker_retire_timeout_s", 5.0) for c in clients] or [5.0]) + WS_FALLBACK_SLACK_S
        rest = [t for t in self._rest_producer_tasks() if not t.done()]
        for task in rest:
            task.cancel()

        async def _rest_done() -> bool:
            # Concurrent with the client waits, so it adds nothing to the budget.
            if rest:
                await asyncio.wait(rest, timeout=bound)
            return True
        results = await asyncio.gather(
            *(_wait(c, getattr(c, "worker_retire_timeout_s", 5.0) + WS_FALLBACK_SLACK_S) for c in clients),
            _rest_done(), return_exceptions=True)
        return all(result is True for result in results)

    async def _async_shutdown(self, signum: int, terminal: bool = False, reason: Optional[str] = None):
        if reason is not None:
            logger.error("Controlled shutdown requested", reason=reason)
        elif terminal:
            logger.error("Terminal storage failure, initiating controlled shutdown",
                         failure=None if self.terminal_failure is None else self.terminal_failure.as_dict())
        else:
            logger.info("Received signal, initiating async shutdown", signum=signum)
        self.running = False
        self._recovery_closed = True       # no new REST producer from here on (see _schedule_recovery)
        # Cancel every existing REST producer now (synchronously). The clients' graceful stop that
        # follows takes many loop turns, so a well-behaved producer has ended long before
        # shutdown() asks _rest_producers_stopped(); one that ignores the cancel is then reported.
        self._cancel_rest_producers()
        # F5: exception-safe. Quality reporting is telemetry and must never be a
        # precondition of the terminal path: when the quality writer is the thing
        # that failed, its first persist raises, and this method used to abort
        # here -- ``shutdown()`` never ran, the writers stayed unclosed, the
        # tasks (health monitor included) kept running and the process never
        # exited 70. ``shutdown()`` and the task cancellation below are in a
        # ``finally`` so they happen whatever the steps above do. This coroutine
        # runs in the supervisor's (or the signal handler's) task, never in a
        # websocket worker, so cancelling ``self.tasks`` cannot cancel itself.
        try:
            try:
                if self._recovery_task is not None and not self._recovery_task.done():
                    self._recovery_task.cancel()
                    # Bounded: a recovery that ignores cancellation must not hang shutdown past the
                    # unit's TimeoutStopSec. Whether it ended is judged later, by
                    # _storage_held_by_rest_producers(), never assumed from this wait returning.
                    await asyncio.wait({self._recovery_task}, timeout=WS_FALLBACK_SLACK_S)
            except Exception as exc:  # noqa: BLE001
                logger.error("shutdown_recovery_cancel_failed", error=f"{type(exc).__name__}: {exc}")
            self._drain_integrity_quality_events_safely()
            try:
                # Bounded for the same reason as in start(): this wait precedes _stop_ws_clients() and
                # shutdown(), and a producer that keeps the queue non-empty kept the loop alive forever.
                await self._stop_quality_consumer(drain=False)
            except Exception as exc:  # noqa: BLE001
                logger.error("shutdown_quality_consumer_stop_failed", error=f"{type(exc).__name__}: {exc}")
        finally:
            try:
                try:
                    # Clients first: no writer is closed while a websocket worker
                    # or callback can still use it (see _stop_ws_clients).
                    await self._stop_ws_clients()
                except Exception as exc:  # noqa: BLE001 - never a precondition of the writer closes
                    logger.error("shutdown_ws_clients_stop_failed", error=f"{type(exc).__name__}: {exc}")
                finally:
                    self.shutdown()
            except Exception as exc:  # noqa: BLE001 - shutdown() is itself exception-safe; belt and braces
                logger.error("shutdown_failed", error=f"{type(exc).__name__}: {exc}")
            finally:
                for task in self.tasks:
                    task.cancel()

    # ------------------------------------------------------------------
    # F5-QA-04: continuous storage-pressure policy (read-only monitor + controlled stop)
    # ------------------------------------------------------------------
    def _init_storage_pressure(self) -> None:
        """Build the monitor for THIS runner's configured data / quality-WAL directories.
        An invalid threshold configuration raises here: startup fails, nothing is coerced."""
        self.storage_pressure = StoragePressureMonitor(
            label="FUTURES", config=DiskPressureConfig.from_env(),
            paths=lambda: [self.quality_writer.base_dir, self._quality_wal.wal_dir],
            wal_dir=lambda: self._quality_wal.wal_dir,
            wal_max_bytes=lambda: self._quality_wal.max_total_bytes,
            checkpoint_state=lambda: (self._quality_checkpoint_is_blocked(),
                                      getattr(self, "_quality_checkpoint_block_reason", None)))
        self._disk_emergency = None
        self._disk_monitor_unavailable = None
        self._disk_shutdown_task = None

    def storage_pressure_status(self) -> dict:
        """Read-only storage-pressure view, separate from ``quality_channel_status`` (writer
        failure / lost / unconfirmed / WAL-only counters are not touched by disk pressure)."""
        monitor = getattr(self, "storage_pressure", None)
        if monitor is None:
            return {"monitoring": False, "severity": Severity.UNKNOWN.value}
        return monitor.status()

    async def _storage_pressure_preflight_failed(self) -> bool:
        """Startup gate: True when ingestion must NOT start.

        ``establish_measurement`` retries the probe (bounded by the unknown grace period) until a
        valid reading exists. A reading below the emergency threshold, or no valid reading at all
        within the grace period, ends in the normal exception-safe, writer-ordered ``shutdown()``
        before any websocket client starts. UNKNOWN is never treated as OK."""
        monitor = getattr(self, "storage_pressure", None)
        if monitor is None:
            return False
        result = await monitor.establish_measurement()
        if result.measurement_ok and result.severity != Severity.EMERGENCY:
            return False
        if result.measurement_ok:
            self._disk_emergency = monitor.status()
            logger.error("storage_pressure_emergency_at_startup", exit_status=EXIT_DISK_PRESSURE)
        else:
            self._disk_monitor_unavailable = monitor.status()
            logger.error("storage_pressure_monitor_unavailable_at_startup",
                         exit_status=EXIT_DISK_MONITOR_UNAVAILABLE,
                         error=self._disk_monitor_unavailable["last_monitor_error"],
                         unknown_for_s=self._disk_monitor_unavailable["monitor_failure_duration_s"])
        self.running = False
        self.shutdown()
        return True

    def _request_disk_shutdown(self, reason: str) -> None:
        """Run the controlled shutdown from its own task, exactly once for any disk reason."""
        if getattr(self, "_disk_shutdown_task", None) is None:
            self._disk_shutdown_task = asyncio.create_task(self._async_shutdown(0, reason=reason))

    def _on_disk_emergency(self, result) -> None:
        """Runtime EMERGENCY (confirmed low space): latch (exit status 1) and stop."""
        monitor = getattr(self, "storage_pressure", None)
        self._disk_emergency = monitor.status() if monitor is not None else {"severity": "EMERGENCY"}
        logger.error("storage_pressure_emergency_shutdown", exit_status=EXIT_DISK_PRESSURE)
        self._request_disk_shutdown("disk_pressure_emergency")

    def _on_disk_monitor_unavailable(self, result) -> None:
        """Runtime: storage could not be measured for the whole grace period. Latch separately
        (exit status 69, never 1) and stop; this is NOT a confirmed low-disk emergency."""
        monitor = getattr(self, "storage_pressure", None)
        self._disk_monitor_unavailable = (monitor.status() if monitor is not None
                                          else {"severity": "UNKNOWN"})
        logger.error("storage_pressure_monitor_unavailable_shutdown",
                     exit_status=EXIT_DISK_MONITOR_UNAVAILABLE,
                     error=self._disk_monitor_unavailable.get("last_monitor_error"),
                     unknown_for_s=self._disk_monitor_unavailable.get("monitor_failure_duration_s"))
        self._request_disk_shutdown("disk_monitor_unavailable")

    async def _storage_pressure_loop(self) -> None:
        monitor = getattr(self, "storage_pressure", None)
        if monitor is None:
            return
        await monitor.run(self._on_disk_emergency, self._on_disk_monitor_unavailable)

    def _shutdown_step(self, step: str, function, *args) -> bool:
        """Run one shutdown step; a failure is logged and never stops the next one."""
        try:
            function(*args)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error("shutdown_step_failed", step=step, error=f"{type(exc).__name__}: {exc}")
            return False

    def shutdown(self):
        """Stop intake and finalize every writer. Exception-safe and retryable.

        ``_closed`` means "shutdown has begun" (the supervisor loop reads it); it
        no longer means "cleanup is done". Cleanup completion is
        ``_shutdown_done``, set only after a full pass, and each writer's close is
        attempted at most once (``_writer_close_results``). So a first call that
        fails partway -- or is interrupted -- never leaves writers unclosed
        behind a latched ``_closed``: a later call attempts exactly the steps not
        yet attempted, and an already-attempted writer is never closed twice."""
        if getattr(self, "_shutdown_done", False):
            return
        self._closed = True
        self._recovery_closed = True
        if not hasattr(self, "_writer_close_results"):
            self._writer_close_results = {}
        logger.info("Shutting down Collector Application...", stream_counters=self.stream_counters, validation_fail_reasons=self.validation_fail_reasons)
        self.running = False
        for ws_client in self.ws_clients:
            self._shutdown_step("ws_client_stop", ws_client.stop)
        self._shutdown_step("health_monitor_stop", self.health_monitor.stop)
        self._drain_integrity_quality_events_safely()
        for task in self.tasks:
            self._shutdown_step("task_cancel", task.cancel)
        if self._recovery_task is not None and not self._recovery_task.done():
            self._shutdown_step("recovery_task_cancel", self._recovery_task.cancel)
        # A REST producer that is still alive keeps its storage open below. If it ends later, this
        # retry finalizes the rest -- nothing else would, once start() has returned.
        self._shutdown_step("arm_late_shutdown_retry", self._arm_late_shutdown_retry)
        if not self._ws_clients_idle():
            # A websocket ingest worker (or a start() still inside its tail) is
            # alive, so closing the writers it feeds would put storage beneath a
            # live user -- the exact defect this ordering exists to prevent. Fail
            # EXPLICITLY instead of reporting a clean shutdown. Only the raw
            # evidence writers are closed (their users are the receive loop and
            # the cancelled REST tasks, never the worker's on_message path), so
            # received frames' raw capture is still published. Everything the
            # worker can reach -- market-data writers, the quality writer and its
            # WAL -- is left open: the WAL stays the recovery source and the next
            # start replays it. ``_shutdown_done`` is deliberately NOT set, so a
            # later call (once the worker has ended) finishes the remaining closes.
            self._record_shutdown_failure("ws_client_worker_not_retired")
            # Each raw writer is closed only when ITS producers are proven stopped, which is
            # not implied by the worker being alive (or dead):
            #   raw_wire_writer: only the receive loop -> ingress stopped for every client.
            #   raw_rest_writer: recovery task and OI poller (a live worker can start a
            #       recovery; that is latched off above, so only the existing tasks count).
            if self._ws_clients_ingress_stopped():
                self._close_writer_once("raw_wire_writer")
            else:
                logger.error("shutdown_raw_wire_writer_left_open_ingress_not_stopped")
            if "raw_rest_writer" not in self._storage_held_by_rest_producers():
                self._close_writer_once("raw_rest_writer")
            else:
                logger.error("shutdown_raw_rest_writer_left_open_rest_producer_alive")
            return
        # Hostile-audit finding (this session): none of these close() calls
        # were guarded. _close_segment's flush/pyarrow-close/fsync/rename have
        # no failure handling of their own (only the metadata sidecar step
        # does), so any of them raising -- a real disk-full/fsync/rename
        # failure, exactly what this audit traces -- would propagate straight
        # out of shutdown() and abort every writer still left in this list,
        # including quality_writer itself. One writer's finalization failure
        # must never silently cost every OTHER writer's still-buffered data.
        #
        # P0-2/P0-3 order: intake is stopped above -> drain whatever is still
        # queued -> close the other writers (their failure reports are
        # quality events, WAL-protected and buffered) -> close quality_writer,
        # which PUBLISHES the final partial segment and thereby checkpoints
        # its WAL seqs -> only then close the WAL (the publish hook needs it).
        self._shutdown_step("quality_queue_drain", self._drain_quality_queue_sync)
        # A writer is closed only once every producer that can reach it is PROVEN ended (the async
        # callers cancel the REST producers and let them run to completion before this point).
        # A producer that ignored its cancel keeps exactly the storage it can still write open --
        # see REST_PRODUCER_REACH: raw REST capture, reconstructed book rows, the oi_writer, and
        # the quality writer + WAL (its recovery/poll events). The failure is explicit.
        held = self._storage_held_by_rest_producers()
        rest_unproven = bool(held)
        if rest_unproven:
            self._record_shutdown_failure("rest_producer_not_stopped")
            logger.error("shutdown_storage_left_open_rest_producer_alive", held=sorted(held))
        if not self._quality_consumer_stopped():
            # The quality persistence loop is a user of quality_writer and its WAL exactly like a REST
            # producer, and a shutdown that could not end it (_stop_quality_consumer, or no caller got
            # that far) must not close either beneath it: it would write a closed writer, and a WAL
            # closed under the publish hook cannot checkpoint. Everything it cannot reach is finalized
            # above/below as usual. Events not yet published stay in the WAL (the recovery source) and
            # the next start replays them. The late retry (armed above) finalizes both once it ends.
            held = held | frozenset({"quality_writer", "quality_wal"})
            self._record_shutdown_failure("quality_consumer_not_stopped")
            logger.error("shutdown_quality_writer_left_open_consumer_alive")
        for writer_name in ("ob_writer", "raw_book_writer", "trades_writer", "raw_trades_writer",
                            "mark_writer", "oi_writer", "liq_writer", "raw_wire_writer", "raw_rest_writer"):
            if writer_name in held:
                continue
            self._close_writer_once(writer_name)
        if "quality_writer" in held or "quality_wal" in held:
            # quality_writer must outlive the live producer and its WAL must outlive quality_writer
            # (the publish hook checkpoints through it): neither is closed. Un-published events
            # stay in the WAL, which the next startup replays.
            return
        if not self._close_writer_once("quality_writer"):
            # Final segment not published: its events stay in the WAL,
            # uncheckpointed, and are replayed by the next startup.
            self._block_quality_checkpoint("quality_writer_close_failed")
        wal = getattr(self, "_quality_wal", None)
        if wal is not None and not getattr(self, "_quality_wal_closed", False):
            self._quality_wal_closed = True
            try:
                wal.close()
            except Exception as exc:  # noqa: BLE001 - must not abort the rest of shutdown
                logger.error("quality_wal_close_failed", error=str(exc))
        self._shutdown_done = True

    def _ws_clients_idle(self) -> bool:
        """True when no websocket client can still touch storage (no live ingest
        worker, no start() mid-tail). A client without the attribute (a test
        double) counts as idle."""
        return all(getattr(client, "lifecycle_idle", True) for client in getattr(self, "ws_clients", ()))

    def _ws_clients_ingress_stopped(self) -> bool:
        """True when no websocket receive loop can still capture a raw frame. A client
        without the attribute (a test double) counts as stopped."""
        return all(getattr(client, "ingress_stopped", True) for client in getattr(self, "ws_clients", ()))

    def _storage_held_by_rest_producers(self) -> frozenset:
        """The storage a still-running REST producer can write (empty when all have ended)."""
        producers = {"recovery": (getattr(self, "_recovery_task", None), getattr(self, "_startup_recovery_task", None)),
                     "oi_poll": (getattr(self, "_oi_task", None),)}
        held = set()
        for name, tasks in producers.items():
            if any(task is not None and not task.done() for task in tasks):
                held |= REST_PRODUCER_REACH[name]
        return frozenset(held)

    def _arm_late_shutdown_retry(self) -> None:
        """Retry ``shutdown()`` when the last REST producer that outlived it ends (idempotent).

        ``shutdown()`` never closes storage beneath a live producer: it keeps that producer's
        reachable writers and the quality WAL open and latches ``rest_producer_not_stopped``. A
        producer that swallows its cancel can end later, after ``start()`` has returned and with
        nobody left to finalize what was held. The done-callback is that retry; it only calls
        ``shutdown()``, which re-checks every producer itself (a retry while one is still alive is
        a no-op) and closes each writer at most once, in the original order. It cannot make a
        producer that never ends end, and it never clears the latched failure: a late close is
        not a clean shutdown (``exit_code`` stays non-zero).
        """
        armed = self.__dict__.setdefault("_late_retry_armed", set())
        tasks = list(self._rest_producer_tasks())
        consumer = getattr(self, "_quality_task", None)
        if consumer is not None:
            tasks.append(consumer)          # the quality loop is a user of quality_writer / its WAL too
        for task in tasks:
            if not task.done() and task not in armed:
                armed.add(task)
                task.add_done_callback(self._on_rest_producer_ended_late)

    def _on_rest_producer_ended_late(self, task) -> None:
        if not task.cancelled():
            error = task.exception()        # retrieved here; reported instead of lost
            if error is not None:
                logger.error("quality_consumer_failed_after_shutdown_began"
                             if task is getattr(self, "_quality_task", None)
                             else "rest_producer_failed_after_shutdown_began",
                             error=f"{type(error).__name__}: {error}")
        if getattr(self, "_shutdown_done", False) or not getattr(self, "_closed", False):
            return
        if not self._rest_producers_stopped() or not self._quality_consumer_stopped():
            return                          # another producer / the quality loop is alive: ITS end retries
        logger.info("late_shutdown_retry", reason="rest_producer_ended_after_shutdown")
        self._shutdown_step("late_shutdown_retry", self.shutdown)

    def _rest_producers_stopped(self) -> bool:
        """True when no REST producer (recovery, startup recovery, OI poller) is still running."""
        return all(task.done() for task in self._rest_producer_tasks())

    def _record_shutdown_failure(self, reason: str) -> None:
        """Latch an explicit terminal shutdown failure (first reason wins). It is
        reflected in ``exit_code``; it never raises."""
        if getattr(self, "shutdown_failure", None) is None:
            self.shutdown_failure = reason
        logger.error("shutdown_terminal_failure", reason=reason, exit_status=EXIT_RUN_TASK_CRASHED)

    def _close_writer_once(self, writer_name: str) -> bool:
        """Attempt ``writer_name``'s close at most once per process; a repeat call
        returns the first attempt's outcome instead of closing it again."""
        results = self._writer_close_results
        if writer_name not in results:
            results[writer_name] = self._close_writer_reporting_failure(writer_name)
        return results[writer_name]

    def _close_writer_reporting_failure(self, writer_name: str) -> bool:
        """Close one writer; on failure, report it and still return.

        Never lets one writer's close() raise past this point: shutdown must
        finalize every OTHER writer regardless. The failure is reported
        through quality_writer -- itself still open at this point for every
        writer_name except "quality_writer", which is always closed last.
        """
        writer = getattr(self, writer_name, None)
        if writer is None:
            return True
        closed_ok = True
        try:
            writer.close()
        except Exception as exc:  # noqa: BLE001 - must not abort closing the rest
            closed_ok = False
            logger.error("storage_shutdown_close_failed", writer=writer_name, error=str(exc))
            if writer_name != "quality_writer":
                try:
                    self._persist_quality_event({
                        "exchange": "BINANCE", "stream": getattr(writer, "stream_name", writer_name),
                        "event_type": "ERROR",
                        "reason": (f"storage_shutdown_close_failed:{writer_name}:{type(exc).__name__}"
                                   # F5: the typed fatal chains the original storage error; keep that evidence.
                                   + ("" if exc.__cause__ is None else f":cause={type(exc.__cause__).__name__}")),
                    })
                except Exception as sink_exc:  # noqa: BLE001 - reporting must not itself abort shutdown
                    logger.error("storage_shutdown_close_failure_report_failed",
                                 writer=writer_name, error=str(sink_exc))
        msg = "Collector Application Shutdown"
        logger.info(msg)
        send_telegram_alert(msg)
        return closed_ok

def main(app_factory=None) -> int:
    """Run the collector and return its process exit status.

    0 for a normal / signalled stop; ``EXIT_FATAL_STORAGE`` (non-zero) when a
    raw-evidence storage failure latched a terminal failure, so systemd sees a
    failed exit. Plain ``sys.exit`` from the main thread only: no ``os._exit``,
    and nothing in a worker calls it."""
    validate_telegram_startup()
    app = (app_factory or CollectorApp)()
    try:
        asyncio.run(app.start())
    except KeyboardInterrupt:
        pass
    except Exception as exc:  # noqa: BLE001
        if app.terminal_failure is None:
            raise                       # an unrelated crash keeps its traceback and status
        # A terminal storage failure was latched: the intended exit status is
        # EXIT_FATAL_STORAGE, not whatever the teardown tripped over afterwards.
        logger.error("collector_exception_after_terminal_failure",
                     error=f"{type(exc).__name__}: {exc}")
    return app.exit_code


if __name__ == "__main__":
    sys.exit(main())
