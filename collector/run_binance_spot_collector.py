"""Live Binance **Spot** collector (P5).

Wires ``BinanceSpotAdapter`` (P5's already-implemented adapter, with its
Spot-specific ``SpotSequenceComparator``/``binance_spot_snapshot_bridge``) to
a real WebSocket connection, REST snapshot recovery, raw capture, and
durable storage -- the piece the module docstring in
``collector/collector/adapters/binance_spot.py`` explicitly named as not yet
built.

Architecture (mirrors ``run_collector.py``'s USD-M futures bridging, on
Spot's own protocol; mirrors ``run_okx_collector.py``'s single-file runner
style):

    Binance Spot WebSocket
            |
            v
    raw wire capture (RawCapture, before parsing)
            |
            v
    BinanceSpotAdapter.normalize()
            |
            v
    Canonical events
            |
      +-----+-----+
      |           |
      v           v
    LocalBook   CanonicalTradeEvent
  ("BINANCE_SPOT")   |
      |               v
      v         spot_trades storage
  quality state
      |
      v
  spot_orderbook_raw storage + durable quality events

Order-book bridging
--------------------
Spot buffers diffs until a REST snapshot bridges the chain -- the same
buffer-until-bridged flow USD-M futures uses (``LocalBook``'s
``_BUFFER_UNTIL_BRIDGED_VENUES``), but with Spot's own discard rule
(``u <= lastUpdateId``) and bridge predicate
(``U <= lastUpdateId+1 <= u``, the ``+1`` Spot's official procedure
documents that futures' formula does not have). Both live in
``sequence.py``/``book_engine.py`` already; this runner does not
reimplement them, only drives ``LocalBook.binance_snapshot()`` with a real
REST response.

Recovery is bounded through the same ``RecoveryController`` USD-M futures
uses (own instance, own state -- Spot and futures must never share a
recovery budget, since one venue's outage should not throttle the other's).

Causality
---------
The REST snapshot's eligibility is ``response_receive_ts`` -- when the HTTP
response actually landed -- never the exchange-reported data inside it and
never wall-clock-at-write. See ``docs/BINANCE_OI.md`` for the same argument
made about OI polling; the reasoning is identical here.

Storage
-------
Own ``spot_``-prefixed streams via ``storage_layout.venue_stream`` (already
registered: ``VENUE_STREAM_PREFIX["BINANCE_SPOT"] == "spot_"``). Never
writes into USD-M futures' unprefixed ``orderbook``/``trades``/``raw_wire``
streams -- verified in ``tests/test_binance_spot_collector.py`` by asserting
the two venues' resolved stream names never collide.

Standalone, like ``run_okx_collector.py`` and ``run_bybit_collector.py``:
imports nothing from ``run_collector.py``, no shared mutable state. Running
this alongside the USD-M futures runner against the same ``--data-dir`` is
fully supported precisely because the two write disjoint stream namespaces.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import signal
import time

from collector.collector.adapters.binance_spot import BinanceSpotAdapter
from collector.collector.backoff import ExponentialBackoff
from collector.collector.book_engine import LocalBook
from collector.collector.canonical import CanonicalOrderBookEvent, CanonicalTradeEvent
from collector.collector.config import (
    BINANCE_SPOT_DEPTH_SNAPSHOT_URL,
    BINANCE_SPOT_WS_URL,
    QUALITY_EVENTS_SCHEMA,
    SPOT_ORDERBOOK_RAW_SCHEMA,
    SPOT_TRADES_SCHEMA,
    SYMBOL,
)
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import BookQuality, QualityEventType
from collector.collector.raw_capture import (
    RAW_REST_SCHEMA,
    RAW_WIRE_SCHEMA,
    RawCapture,
    RawRestRecord,
    RawWireRecord,
)
from collector.collector.recovery_control import RecoveryController
from collector.collector.storage_layout import venue_stream
from collector.collector.utils import logger
from collector.collector.websocket_client import WebSocketClient

VENUE = "BINANCE_SPOT"


class BinanceSpotCollectorApp:
    def __init__(self, data_dir: str = "data", url: str = BINANCE_SPOT_WS_URL,
                 snapshot_url: str = BINANCE_SPOT_DEPTH_SNAPSHOT_URL) -> None:
        self.url = url
        self.snapshot_url = snapshot_url

        self.quality_writer = ParquetWriter(
            venue_stream(VENUE, "quality_events"), QUALITY_EVENTS_SCHEMA,
            base_dir=data_dir, exchange="BINANCE_SPOT", segment_rows=1, segment_seconds=1)
        self.raw_wire_writer = ParquetWriter(
            venue_stream(VENUE, "raw_wire"), RAW_WIRE_SCHEMA, base_dir=data_dir,
            exchange="BINANCE_SPOT", quality_event_sink=self._persist_quality_event)
        self.raw_rest_writer = ParquetWriter(
            venue_stream(VENUE, "raw_rest"), RAW_REST_SCHEMA, base_dir=data_dir,
            exchange="BINANCE_SPOT", quality_event_sink=self._persist_quality_event)
        self.raw_capture = RawCapture(
            self.raw_wire_writer, self.raw_rest_writer,
            quality_event_sink=self._persist_quality_event)

        self.trades_writer = ParquetWriter(
            venue_stream(VENUE, "trades"), SPOT_TRADES_SCHEMA, base_dir=data_dir, exchange="BINANCE_SPOT")
        self.ob_writer = ParquetWriter(
            venue_stream(VENUE, "orderbook_raw"), SPOT_ORDERBOOK_RAW_SCHEMA,
            base_dir=data_dir, exchange="BINANCE_SPOT")

        self.adapter = BinanceSpotAdapter()
        self.adapter.set_unhandled_sink(self._record_adapter_unhandled)
        self.book = LocalBook(VENUE)
        self._book_lock = asyncio.Lock()
        self._recovery_task: asyncio.Task | None = None

        # Own instance, own budget -- never shared with USD-M futures'
        # RecoveryController. One venue's outage must not throttle the
        # other's recovery.
        self.recovery_controller = RecoveryController(
            name="binance_spot_orderbook", min_interval_s=1.0, max_per_window=5,
            window_s=60.0,
            backoff=ExponentialBackoff(base_delay=1.0, max_delay=60.0, max_attempts=10),
            quality_sink=self._persist_quality_event)

        self.stream_counters = {
            "orderbook": {"received": 0, "written": 0},
            "trades": {"received": 0, "written": 0},
            "unrouted": {"received": 0},
            "malformed_envelope": {"received": 0},
            "adapter_unhandled": {"received": 0},
        }

        self.client = WebSocketClient(
            url=self.url,
            on_message=self._handle_message,
            on_raw_frame=self._capture_raw_frame,
            on_open=self._on_open,
            on_quality_event=self._on_client_quality_event,
            stream_group="binance_spot",
        )

    # -- raw capture --------------------------------------------------------

    def _capture_raw_frame(self, payload, *, local_receive_ts, connection_id=None,
                           connection_generation=None, decode_ok=True,
                           decode_error=None, parsed=None, control_frame=False):
        """Persist the exact frame before any lossy transformation.

        ``control_frame`` exists because ``WebSocketClient._consume()`` calls
        every ``on_raw_frame`` callback with it unconditionally (added for
        OKX's non-JSON ping/pong control frames). Binance Spot has none, so
        this always arrives ``False`` and nothing below reads it -- but the
        parameter must exist. Without it, every single call raised
        ``TypeError`` inside ``_consume``'s fail-open ``try/except``, so raw
        capture silently produced zero rows -- this was the exact same
        regression already found and fixed once this session for
        ``run_collector.py`` (CollectorApp, the P0 hotfix) and guarded
        against by design in ``run_bybit_collector.py`` and
        ``run_okx_collector.py``, but this file predates that fix and was
        never updated to match. Confirmed live-impacting: this runner is
        the one described as currently deployed on the user's VPS.
        """
        stream = channel = None
        if isinstance(parsed, dict):
            stream = parsed.get("stream")
            if isinstance(stream, str):
                channel = self.adapter.route_message(parsed)
        self.raw_capture.capture_wire(RawWireRecord(
            local_receive_ts=local_receive_ts,
            payload=payload if isinstance(payload, str) else str(payload),
            venue=VENUE, connection_id=connection_id,
            connection_generation=connection_generation, channel=channel,
            stream=stream, symbol=SYMBOL, market_type="spot",
            decode_ok=bool(decode_ok), decode_error=decode_error,
        ))

    async def _on_open(self, send) -> None:
        await send(self.adapter.subscribe_message(
            ["btcusdt@trade", "btcusdt@depth@100ms"]))

    def _capture_rest(self, record: RawRestRecord) -> None:
        self.raw_capture.capture_rest(record)

    # -- quality events -------------------------------------------------------

    def _persist_quality_event(self, event: dict) -> None:
        event_type = event.get("event_type", QualityEventType.ERROR.value)
        if isinstance(event_type, QualityEventType):
            event_type = event_type.value
        rows_lost = event.get("rows_lost")
        local_ts = event.get("local_ts", int(time.time() * 1000))
        try:
            self.quality_writer.write({
                "timestamp": local_ts, "exchange": "BINANCE_SPOT",
                "stream": event.get("stream", "spot_orderbook"), "event_type": event_type,
                "reason": event.get("reason", ""), "gap_size_ms": event.get("gap_size_ms"),
                "rows_lost": None if rows_lost is None else str(rows_lost),
                "quality_state": event.get("quality_state"),
                "connection_id": event.get("connection_id"),
                "previous_state": event.get("previous_state"), "new_state": event.get("new_state"),
                "expected_previous_update_id": event.get("expected_previous_update_id"),
                "actual_previous_update_id": event.get("actual_previous_update_id"),
                "update_id": event.get("update_id"), "first_update_id": event.get("first_update_id"),
                "previous_update_id": event.get("previous_update_id"),
                "local_receive_ts": event.get("local_receive_ts"),
                "local_process_ts": int(time.time() * 1000),
            })
        except Exception as exc:  # noqa: BLE001 - quality write must not break ingest
            logger.error("spot_quality_write_failed", error=str(exc))

    def _on_client_quality_event(self, event_type, reason, connection_id=None, stream_group=None) -> None:
        self._persist_quality_event({
            "stream": stream_group or "spot_orderbook", "event_type": event_type, "reason": reason,
            "connection_id": connection_id, "local_ts": int(time.time() * 1000)})

    def _record_adapter_unhandled(self, message) -> None:
        self.stream_counters["adapter_unhandled"]["received"] += 1
        self._persist_quality_event(message.to_quality_event())

    # -- message handling ---------------------------------------------------

    async def _handle_message(self, data, local_receive_ts: int, connection_id=None) -> None:
        if not isinstance(data, dict) or "stream" not in data or "data" not in data:
            self.stream_counters["malformed_envelope"]["received"] += 1
            keys = sorted(str(k) for k in data.keys()) if isinstance(data, dict) else []
            self._persist_quality_event({
                "stream": "unrouted", "event_type": QualityEventType.DATA_DROP.value,
                "reason": f"non_envelope_frame:keys={','.join(keys) or type(data).__name__}",
                "rows_lost": 1, "connection_id": connection_id,
                "local_receive_ts": local_receive_ts, "local_ts": local_receive_ts,
            })
            return

        route = self.adapter.route_message(data)
        if route is None:
            self.stream_counters["unrouted"]["received"] += 1
            self._persist_quality_event({
                "stream": "unrouted", "event_type": QualityEventType.DATA_DROP.value,
                "reason": f"unrouted_stream:{data.get('stream')}", "rows_lost": 1,
                "connection_id": connection_id,
                "local_receive_ts": local_receive_ts, "local_ts": local_receive_ts,
            })

        events = self.adapter.normalize(data, local_receive_ts=local_receive_ts)
        for event in events:
            await self._persist_event(event)

    async def _persist_event(self, event) -> None:
        if isinstance(event, CanonicalTradeEvent):
            self.stream_counters["trades"]["received"] += 1
            self.trades_writer.write({
                "timestamp": event.local_receive_ts,
                "exchange_timestamp": event.exchange_event_ts,
                "local_timestamp": event.local_receive_ts,
                "trade_id": event.trade_id, "price": event.price,
                "quantity": event.quantity, "side": event.side,
            })
            self.stream_counters["trades"]["written"] += 1
            return

        if isinstance(event, CanonicalOrderBookEvent):
            self.stream_counters["orderbook"]["received"] += 1
            await self._apply_orderbook(event)

    async def _apply_orderbook(self, event: CanonicalOrderBookEvent) -> None:
        async with self._book_lock:
            before = self.book.state.state
            applied = self.book.apply(event)
            after = self.book.state.state
            if applied is None and self.book.previous is None:
                # This diff was buffered while unbridged. Give a
                # snapshot-ahead-of-buffer retained snapshot a chance to
                # bridge against the now-larger buffer.
                if self.book.retry_pending_snapshot():
                    after = self.book.state.state
                    for pending_applied, event_kind, generation in self.book.committed_recovery_events:
                        self.ob_writer.write({
                            "timestamp": pending_applied.local_receive_ts,
                            "exchange_timestamp": pending_applied.exchange_event_ts,
                            "local_receive_ts": pending_applied.local_receive_ts,
                            "local_process_ts": int(time.time() * 1000),
                            "bids": [[str(p), str(q)] for p, q in pending_applied.bids],
                            "asks": [[str(p), str(q)] for p, q in pending_applied.asks],
                            "update_id": pending_applied.update_id,
                            "first_update_id": pending_applied.first_update_id,
                            "previous_update_id": pending_applied.previous_update_id,
                            "book_source": pending_applied.book_source, "event_kind": event_kind,
                            "recovery_generation": generation, "quality_state": pending_applied.quality_state,
                        })
                        self.stream_counters["orderbook"]["written"] += 1
                    self.book.committed_recovery_events = []
        if before is not after:
            kind = (QualityEventType.SEQUENCE_GAP.value
                    if after in (BookQuality.SEQUENCE_GAP, BookQuality.RECOVERING)
                    else QualityEventType.RECOVERY.value)
            self._persist_quality_event({
                "stream": "spot_orderbook", "event_type": kind,
                "reason": self.book.last_reason, "quality_state": after.value,
                "previous_state": before.value, "new_state": after.value,
            })
        for quality_event in self.book.drain_quality_events():
            record = quality_event.record() if hasattr(quality_event, "record") else dict(quality_event)
            record.setdefault("stream", "spot_orderbook")
            self._persist_quality_event(record)

        if applied is not None:
            self.ob_writer.write({
                "timestamp": applied.local_receive_ts,
                "exchange_timestamp": applied.exchange_event_ts,
                "local_receive_ts": applied.local_receive_ts,
                "local_process_ts": int(time.time() * 1000),
                "bids": [[str(p), str(q)] for p, q in applied.bids],
                "asks": [[str(p), str(q)] for p, q in applied.asks],
                "update_id": applied.update_id, "first_update_id": applied.first_update_id,
                "previous_update_id": applied.previous_update_id,
                "book_source": applied.book_source, "event_kind": "NORMAL_INCREMENTAL",
                "recovery_generation": self.book.recovery_generation,
                "quality_state": applied.quality_state,
            })
            self.stream_counters["orderbook"]["written"] += 1

        if after in (BookQuality.SEQUENCE_GAP, BookQuality.RECOVERING) and before is BookQuality.VALID:
            self._schedule_recovery("sequence_gap")
        elif self.book.previous is None:
            # Never bridged: keep trying until a snapshot succeeds, bounded
            # by the same controller as an explicit gap.
            self._schedule_recovery("initial_snapshot")

    # -- recovery -------------------------------------------------------------

    def _schedule_recovery(self, reason: str) -> bool:
        if not self.recovery_controller.request(reason).allowed:
            return False
        if self._recovery_task is not None and not self._recovery_task.done():
            return False
        self._recovery_task = asyncio.ensure_future(self._recover_book(reason))
        if not self.recovery_controller.in_flight:
            self.recovery_controller.begin()
        return True

    async def _recover_book(self, reason: str) -> bool:
        import aiohttp

        request_ts = int(time.time() * 1000)
        status = None
        body = None
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.snapshot_url) as response:
                    status = response.status
                    body = await response.text()
                    receive_ts = int(time.time() * 1000)
                    response.raise_for_status()
                    snapshot = json.loads(body)
            process_ts = int(time.time() * 1000)
            self._capture_rest(RawRestRecord(
                request_ts=request_ts, response_receive_ts=receive_ts,
                endpoint=self.snapshot_url, purpose="orderbook_snapshot", venue=VENUE,
                request_params={"symbol": SYMBOL, "limit": 1000}, http_status=status,
                ok=True, payload=body, symbol=SYMBOL, market_type="spot",
                local_process_ts=process_ts))

            from decimal import Decimal
            last_update_id = int(snapshot["lastUpdateId"])
            bids = tuple((Decimal(p), Decimal(q)) for p, q in snapshot["bids"])
            asks = tuple((Decimal(p), Decimal(q)) for p, q in snapshot["asks"])
            snapshot_event = CanonicalOrderBookEvent(
                "BINANCE", "spot_orderbook", None, None, receive_ts,
                local_process_ts=process_ts, market_type="spot",
                bids=bids, asks=asks, update_id=last_update_id, is_snapshot=True,
                book_source="DIFF_DEPTH_RECONSTRUCTED")

            async with self._book_lock:
                bridged = self.book.binance_snapshot(last_update_id, snapshot_event)
                pending_reason = self.book.last_reason

            if not bridged:
                if pending_reason == "snapshot_ahead_of_buffer":
                    # Not a failure: every buffered diff has u < lastUpdateId,
                    # so the next diff to arrive will straddle it and bridge
                    # automatically via retry_pending_snapshot() (see
                    # book_engine.LocalBook.binance_snapshot's docstring).
                    # Consuming the recovery budget here would spend it on a
                    # problem that resolves itself in milliseconds.
                    self.recovery_controller.succeed()
                    self._persist_quality_event({
                        "stream": "spot_orderbook", "event_type": QualityEventType.RECOVERY.value,
                        "reason": "snapshot_ahead_of_buffer_pending", "quality_state": self.book.state.state.value,
                    })
                    return True
                self.recovery_controller.fail(pending_reason or "snapshot_rejected")
                self._persist_quality_event({
                    "stream": "spot_orderbook", "event_type": QualityEventType.ERROR.value,
                    "reason": pending_reason, "quality_state": self.book.state.state.value,
                })
                return False

            self.recovery_controller.succeed()
            for applied, event_kind, generation in self.book.committed_recovery_events:
                self.ob_writer.write({
                    "timestamp": applied.local_receive_ts,
                    "exchange_timestamp": applied.exchange_event_ts,
                    "local_receive_ts": applied.local_receive_ts,
                    "local_process_ts": int(time.time() * 1000),
                    "bids": [[str(p), str(q)] for p, q in applied.bids],
                    "asks": [[str(p), str(q)] for p, q in applied.asks],
                    "update_id": applied.update_id, "first_update_id": applied.first_update_id,
                    "previous_update_id": applied.previous_update_id,
                    "book_source": applied.book_source, "event_kind": event_kind,
                    "recovery_generation": generation, "quality_state": applied.quality_state,
                })
            self.book.committed_recovery_events = []
            self._persist_quality_event({
                "stream": "spot_orderbook", "event_type": QualityEventType.RECOVERY.value,
                "reason": "snapshot_bridge_completed", "quality_state": self.book.state.state.value,
            })
            return True

        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - a failed recovery must not crash the collector
            why = f"{type(exc).__name__}:{exc}"
            logger.error("spot_book_snapshot_failed", error=str(exc))
            self._capture_rest(RawRestRecord(
                request_ts=request_ts, response_receive_ts=None,
                endpoint=self.snapshot_url, purpose="orderbook_snapshot", venue=VENUE,
                request_params={"symbol": SYMBOL, "limit": 1000}, http_status=status,
                ok=False, error=why, payload=body, symbol=SYMBOL, market_type="spot"))
            self.recovery_controller.fail(why)
            async with self._book_lock:
                self.book.state.gap()
            self._persist_quality_event({
                "stream": "spot_orderbook", "event_type": QualityEventType.ERROR.value,
                "reason": why, "quality_state": self.book.state.state.value,
            })
            return False

    # -- lifecycle ------------------------------------------------------------

    async def run(self) -> None:
        # Request the initial bridge immediately -- the book starts
        # RECOVERING (see quality_events.BookQualityStateMachine) and would
        # otherwise wait for the first diff to arrive before trying.
        self._schedule_recovery("initial_snapshot")
        await self.client.start()

    async def shutdown(self) -> None:
        await self.client.stop()
        if self._recovery_task is not None and not self._recovery_task.done():
            self._recovery_task.cancel()
            await asyncio.gather(self._recovery_task, return_exceptions=True)
        for writer in (self.quality_writer, self.raw_wire_writer, self.raw_rest_writer,
                       self.trades_writer, self.ob_writer):
            writer.close()


async def _main(data_dir: str, url: str) -> None:
    app = BinanceSpotCollectorApp(data_dir=data_dir, url=url)
    loop = asyncio.get_event_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    run_task = asyncio.ensure_future(app.run())
    await stop.wait()
    await app.shutdown()
    run_task.cancel()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--url", default=BINANCE_SPOT_WS_URL)
    args = parser.parse_args()
    logger.info("binance_spot_collector_starting", url=args.url, data_dir=args.data_dir,
               note="requires outbound access to stream.binance.com:9443 and "
                    "api.binance.com; not available in every environment "
                    "(see docs/EXECUTION_STATUS.md)")
    asyncio.run(_main(args.data_dir, args.url))
