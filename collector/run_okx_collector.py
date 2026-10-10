"""Live OKX v5 public collector for the seven D11 channels.

Scope
-----
Wires raw capture + canonical parsing + durable storage for the seven
channels covered by D11 (``trades``, ``trades-all``, ``mark-price``,
``index-tickers``, ``funding-rate``, ``open-interest``,
``liquidation-orders``). Protocol facts (endpoint, subscribe/unsubscribe
envelope, heartbeat, rate limits) are documented in
``collector/collector/okx_capture.py``; per-channel field schemas are in
``docs/OKX_D11_CHANNEL_SCHEMAS.md``.

Deliberately NOT included: ``books`` (order-book) storage. That channel
already has its own dedicated raw-only capture path
(``run_okx_capture.py``); a full live order-book collector needs the same
quality-state-machine wiring Binance/Bybit have (``LocalBook``,
sequence-gap detection via ``OKXSequenceComparator``) and is out of D11's
scope -- D11 is specifically the six non-book channels the OKX adapter
declared but did not implement, plus ``trades-all``. Standing up OKX order
book storage is a separate, already-partially-scaffolded phase.

Storage: own ``okx_*`` streams via ``storage_layout.venue_stream`` -- never
Binance's or Bybit's stream names, same reasoning as
``run_okx_capture.OKXCaptureApp`` and ``run_bybit_collector.py``.

Standalone by design, like ``run_bybit_collector.py``: imports nothing from
``run_collector.py`` or ``run_okx_capture.py``, no shared mutable state.
Running this and ``run_okx_capture.py`` against the same ``--data-dir``
simultaneously is NOT supported -- both would try to write
``okx_raw_wire``/``okx_quality_events``, and the single-writer lock (PR #19)
means the second process to start fails outright rather than silently
sharing the segment sequence. Run one or the other.
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import signal
import time

from collector.collector.adapters.okx import OKXAdapter
from collector.collector.segment_dedup import StreamSpec, attach_segment_dedup, bind_arg
from collector.collector.canonical import (
    CanonicalLiquidationEvent,
    CanonicalMarkPriceEvent,
    CanonicalOIEvent,
    CanonicalOrderBookEvent,
    CanonicalTradeEvent,
)
from collector.collector.config import (
    OKX_FUNDINGRATE_SCHEMA,
    OKX_INDEXTICKERS_SCHEMA,
    OKX_LIQUIDATION_SCHEMA,
    OKX_MARKPRICE_SCHEMA,
    OKX_OPENINTEREST_SCHEMA,
    OKX_TRADES_ALL_SCHEMA,
    OKX_TRADES_SCHEMA,
    QUALITY_EVENTS_SCHEMA,
)
from collector.collector.okx_capture import (
    OKX_BTC_INDEX_INST_ID,
    OKX_BTC_SWAP_INST_ID,
    OKX_PUBLIC_WS_URL,
    okx_subscribe_message,
)
from collector.collector.failure_topology import FailureRecord
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import QualityEventType
from collector.collector.raw_capture import RAW_WIRE_SCHEMA, RawCapture, RawWireRecord
from collector.collector.standalone_failure_policy import (
    StandaloneFailurePolicy,
    build_stream_table,
    supervise_standalone_runner,
    tag_route,
)
from collector.collector.storage_errors import FatalStorageError
from collector.collector.storage_layout import venue_stream
from collector.collector.utils import logger
from collector.collector.websocket_client import Keepalive, WebSocketClient

#: The seven D11 channels. Not "books" -- see module docstring.
D11_CHANNELS = (
    "trades", "trades-all", "mark-price", "index-tickers",
    "funding-rate", "open-interest", "liquidation-orders",
)

OKX_KEEPALIVE = Keepalive(payload="ping", interval_s=20.0, expect="pong", timeout_s=10.0)


class OKXCollectorApp:
    def __init__(self, data_dir: str = "data", url: str = OKX_PUBLIC_WS_URL,
                 inst_id: str = OKX_BTC_SWAP_INST_ID,
                 index_inst_id: str = OKX_BTC_INDEX_INST_ID,
                 channels=D11_CHANNELS, enable_segment_dedup: bool = True) -> None:
        self.url = url
        self.inst_id = inst_id
        self.channels = tuple(channels)

        # Quality events NOT persisted because the quality channel is degraded:
        # the event that tripped the latch plus every later one, which is
        # short-circuited without calling the failed writer. A counter, never a
        # log line per event. Set before any writer exists: a writer may emit
        # through ``_persist_quality_event`` while it is being constructed.
        self.quality_events_lost = 0

        # Own okx_-prefixed streams (PR #19 storage-namespace phase);
        # exchange="OKX" so this runner's own storage faults are never
        # attributed to Binance/Bybit (ParquetWriter's default).
        self.quality_writer = ParquetWriter(
            venue_stream("OKX", "quality_events"), QUALITY_EVENTS_SCHEMA,
            base_dir=data_dir, exchange="OKX", segment_rows=1, segment_seconds=1)
        self.raw_wire_writer = ParquetWriter(
            venue_stream("OKX", "raw_wire"), RAW_WIRE_SCHEMA, base_dir=data_dir,
            exchange="OKX", quality_event_sink=self._persist_quality_event)
        # F5 raw-evidence contract: a typed fatal from the raw writer is NOT
        # absorbed by the fail-open path; it reaches the shared client's
        # raw-frame boundary, is classified TERMINATE by ``failure_policy`` and
        # ends in a controlled non-zero exit.
        self.raw_capture = RawCapture(
            self.raw_wire_writer, None, quality_event_sink=self._persist_quality_event,
            fail_closed_on_fatal_storage=True)

        self.trades_writer = ParquetWriter(
            venue_stream("OKX", "trades"), OKX_TRADES_SCHEMA, base_dir=data_dir, exchange="OKX")
        self.trades_all_writer = ParquetWriter(
            venue_stream("OKX", "trades_all"), OKX_TRADES_ALL_SCHEMA, base_dir=data_dir, exchange="OKX")
        self.mark_writer = ParquetWriter(
            venue_stream("OKX", "markprice"), OKX_MARKPRICE_SCHEMA, base_dir=data_dir, exchange="OKX")
        self.index_writer = ParquetWriter(
            venue_stream("OKX", "indextickers"), OKX_INDEXTICKERS_SCHEMA, base_dir=data_dir, exchange="OKX")
        self.funding_writer = ParquetWriter(
            venue_stream("OKX", "fundingrate"), OKX_FUNDINGRATE_SCHEMA, base_dir=data_dir, exchange="OKX")
        self.oi_writer = ParquetWriter(
            venue_stream("OKX", "openinterest"), OKX_OPENINTEREST_SCHEMA, base_dir=data_dir, exchange="OKX")
        self.liq_writer = ParquetWriter(
            venue_stream("OKX", "liquidation"), OKX_LIQUIDATION_SCHEMA, base_dir=data_dir, exchange="OKX")

        # F5 failure topology for THIS runner's streams (okx_*), built from the
        # live writers: raw -> terminate, derived -> isolate the route, quality
        # -> degrade, anything unlisted -> terminate (default-deny). OKX has TWO
        # trade streams with independent writers and dedup indexes, so they are
        # two routes ("trades", "trades_all").
        self.failure_policy = StandaloneFailurePolicy(
            venue="OKX",
            streams=build_stream_table(
                raw=(self.raw_wire_writer,),
                derived={self.trades_writer.stream_name: "trades",
                         self.trades_all_writer.stream_name: "trades_all",
                         self.mark_writer.stream_name: "markprice",
                         self.index_writer.stream_name: "indextickers",
                         self.funding_writer.stream_name: "fundingrate",
                         self.oi_writer.stream_name: "openinterest",
                         self.liq_writer.stream_name: "liquidation"},
                quality=(self.quality_writer,), dedup_route="trades"),
            clients=lambda: (self.client,),
            report=self._report_storage_failure)

        self.adapter = OKXAdapter(inst_id=inst_id, index_inst_id=index_inst_id)
        # P0-4: two independent trade representations, two anchors -- OKX's
        # "trades" and "trades-all" are distinct streams (never merged, see
        # adapters/okx.py's own docstring on why that question stays open),
        # each with its own writer that receives every admitted event for
        # that stream unconditionally.
        self.segment_dedup = None
        if enable_segment_dedup:
            self.segment_dedup = attach_segment_dedup(self.adapter, [
                StreamSpec("trades", self.trades_writer, "OKX", "linear_perpetual"),
                StreamSpec("trades-all", self.trades_all_writer, "OKX", "linear_perpetual"),
            ])
        self.adapter.set_unhandled_sink(self._record_adapter_unhandled)
        self.messages_handled = 0

        self._subscribe_message = okx_subscribe_message(
            self.channels, self.inst_id, request_id="sub-1", index_inst_id=index_inst_id)

        self.client = WebSocketClient(
            url=self.url,
            on_message=self._handle_message,
            on_raw_frame=self._capture_raw_frame,
            on_open=self._on_open,
            on_quality_event=self._on_client_quality_event,
            keepalive=OKX_KEEPALIVE,
            control_frames=frozenset({"pong"}),
            stream_group="okx",
            on_fatal=self._on_fatal_storage,
        )

    # -- F5 failure topology ------------------------------------------------

    @property
    def exit_code(self) -> int:
        return self.failure_policy.exit_code

    def _on_fatal_storage(self, exc, origin: str = "handler", *, route=None) -> str:
        """Classify and latch one typed storage fatal (see ``failure_policy``).
        Safe to call from the websocket worker: it only latches."""
        return self.failure_policy.on_fatal(exc, origin, route=route)

    def _report_storage_failure(self, record: FailureRecord) -> None:
        """Durable record of a NEW derived/raw failure in this venue's quality
        stream. Never called for a quality failure (the policy skips it).
        ``_persist_quality_event`` contains a quality-channel fatal itself; the
        policy guards this reporter against anything else it raises."""
        self._persist_quality_event({
            "exchange": "OKX", "stream": "storage_failure", "event_type": QualityEventType.ERROR.value,
            "reason": (f"fatal_storage:{record.verdict}:component={record.component}:stream={record.stream}:"
                       f"stage={record.stage}:durability={record.durability}:origin={record.origin}"),
            "local_ts": record.first_observed_ts})

    # -- raw capture --------------------------------------------------------

    def _capture_raw_frame(self, payload, *, local_receive_ts, connection_id=None,
                           connection_generation=None, decode_ok=True,
                           decode_error=None, parsed=None, control_frame=False,
                           local_receive_ns=None, receive_mono_ns=None):
        channel = None
        symbol = None
        if isinstance(parsed, dict):
            arg = parsed.get("arg")
            if isinstance(arg, dict):
                channel = arg.get("channel")
                symbol = arg.get("instId")
        if control_frame:
            channel = "__control__"
        record = RawWireRecord(
            local_receive_ts=local_receive_ts, local_receive_ns=local_receive_ns,
            receive_mono_ns=receive_mono_ns,
            payload=payload if isinstance(payload, str) else str(payload),
            venue="OKX", connection_id=connection_id,
            connection_generation=connection_generation, channel=channel,
            stream="okx_public", symbol=symbol if isinstance(symbol, str) else None,
            market_type="linear_perpetual", decode_ok=bool(decode_ok), decode_error=decode_error,
        )
        self.raw_capture.capture_wire(record)

    async def _on_open(self, send) -> None:
        await send(self._subscribe_message)

    # -- quality events -------------------------------------------------------

    @property
    def quality_degraded(self):
        """The latched quality-channel ``FailureRecord`` (or ``None`` while healthy)."""
        return self.failure_policy.quality_degraded

    def quality_channel_status(self) -> dict:
        """Operator view of the quality channel. Market-data capture is unaffected
        by a degraded channel. This runner keeps NO durable quality WAL, so every
        event counted in ``quality_events_lost`` is gone, not retained."""
        record = self.failure_policy.quality_degraded
        return {"quality_degraded": record is not None,
                "quality_events_lost": self.quality_events_lost,
                "quality_failure": None if record is None else record.as_dict()}

    def _persist_quality_event(self, event: dict) -> None:
        """Single choke point for every OKX quality event.

        F5: a typed ``FatalStorageError`` from THIS runner's quality writer means
        that writer is FAILED. It is latched exactly once through
        ``failure_policy`` (degrade the quality channel; origin
        ``quality_writer``) and contained -- market-data capture keeps running.
        After the latch the failed writer is never called again and later events
        only bump ``quality_events_lost``: no error log and no alert per event.
        A typed fatal of any OTHER stream is re-raised (never swallowed by the
        quality handler); an ordinary exception keeps the log-and-continue path."""
        policy = getattr(self, "failure_policy", None)
        if policy is not None and policy.quality_degraded is not None:
            self.quality_events_lost += 1
            return
        event_type = event.get("event_type", QualityEventType.ERROR.value)
        if isinstance(event_type, QualityEventType):
            event_type = event_type.value
        rows_lost = event.get("rows_lost")
        local_ts = event.get("local_ts", int(time.time() * 1000))
        try:
            self.quality_writer.write({
                "timestamp": local_ts, "exchange": event.get("exchange", "OKX"),
                "stream": event.get("stream", "okx_public"), "event_type": event_type,
                "reason": event.get("reason", ""), "gap_size_ms": event.get("gap_size_ms"),
                "rows_lost": None if rows_lost is None else str(rows_lost),
                "connection_id": event.get("connection_id"),
                "local_receive_ts": event.get("local_receive_ts"), "local_process_ts": int(time.time() * 1000),
            })
        except FatalStorageError as exc:
            if policy is None or not policy.is_quality_channel_failure(exc):
                raise
            policy.on_fatal(exc, origin="quality_writer")
            self.quality_events_lost += 1
        except Exception as exc:  # noqa: BLE001 - quality write must not break ingest
            logger.error("okx_quality_write_failed", error=str(exc))

    def _on_client_quality_event(self, event_type, reason, connection_id=None, stream_group=None) -> None:
        self._persist_quality_event({
            "stream": stream_group or "okx_public", "event_type": event_type, "reason": reason,
            "connection_id": connection_id, "local_ts": int(time.time() * 1000)})

    def _record_adapter_unhandled(self, message) -> None:
        self._persist_quality_event(message.to_quality_event())

    # -- message handling ---------------------------------------------------

    #: adapter channel -> the F5 route a message on that channel feeds.
    _CHANNEL_ROUTES = {"trades": "trades", "trades-all": "trades_all", "mark-price": "markprice",
                       "index-tickers": "indextickers", "funding-rate": "fundingrate",
                       "open-interest": "openinterest", "liquidation-orders": "liquidation"}

    def _message_route(self, data):
        """F5 route a decoded message feeds; ``None`` when unknown. Never raises."""
        try:
            return self._CHANNEL_ROUTES.get(self.adapter.route_message(data)) if isinstance(data, dict) else None
        except Exception:  # noqa: BLE001 - a routing probe must not become a new failure mode
            return None

    @staticmethod
    def _event_route(event):
        """The F5 route an event is persisted on (``None``: not persisted)."""
        if isinstance(event, CanonicalTradeEvent):
            return "trades_all" if event.stream == "trades-all" else "trades"
        if isinstance(event, CanonicalMarkPriceEvent):
            return {"mark-price": "markprice", "index-tickers": "indextickers",
                    "funding-rate": "fundingrate"}.get(event.stream)
        if isinstance(event, CanonicalOIEvent):
            return "openinterest"
        if isinstance(event, CanonicalLiquidationEvent):
            return "liquidation"
        return None

    async def _handle_message(self, data: dict, local_receive_ts: int, connection_id=None) -> None:
        self.messages_handled += 1
        message_route = self._message_route(data)
        if message_route is not None and self.failure_policy.route_is_isolated(message_route):
            # F5: this route's writer is FAILED and the route is cut off. Skip it
            # BEFORE normalize (which runs trade dedup): a dead route costs a
            # counter, not an error per frame. The raw frame was captured already.
            self.failure_policy.note_short_circuit(message_route)
            return
        # F5: a typed storage fatal is NOT caught here. It propagates to the
        # websocket worker boundary (the P0-4 contract), where the client calls
        # ``on_fatal`` -> ``failure_policy`` -> isolate the route / terminate. This
        # handler only records which route it was serving. normalize() runs trade
        # dedup, and the message's own channel names the route: the two OKX trade
        # streams must not be confused.
        served_route = message_route or "trades"
        try:
            events = self.adapter.normalize(data, local_receive_ts=local_receive_ts)
            for event in events:
                event_route = self._event_route(event)
                if event_route is not None and self.failure_policy.route_is_isolated(event_route):
                    self.failure_policy.note_short_circuit(event_route)
                    continue
                served_route = event_route
                self._persist_event(event)
        except FatalStorageError as exc:
            tag_route(exc, served_route)
            raise
        finally:
            # P0-4: message boundary on EVERY exit path (see SegmentDedupHandle.end_message).
            segment_dedup = getattr(self, "segment_dedup", None)
            if segment_dedup is not None:
                segment_dedup.end_message()

    def _persist_event(self, event) -> None:
        if isinstance(event, CanonicalOrderBookEvent):
            return  # books storage is out of scope here; see module docstring.
        base = {
            "timestamp": event.local_receive_ts,
            "exchange_timestamp": event.exchange_event_ts,
            "local_timestamp": event.local_receive_ts,
            # event.instrument is set (or deliberately left None) by
            # OKXAdapter.normalize() itself -- see its per-channel
            # _instrument_scoped exceptions (index-tickers is the index
            # pair, not the swap; liquidation-orders is only identified
            # when its own inst_id matches this adapter's configured
            # instrument). Nothing here re-derives or overrides that
            # decision; a None here is exactly as meaningful as a key.
            "instrument_key": event.instrument.key if event.instrument else None,
        }
        if isinstance(event, CanonicalTradeEvent):
            writer = self.trades_all_writer if event.stream == "trades-all" else self.trades_writer
            row = {**base, "trade_id": event.trade_id, "price": event.price,
                   "quantity": event.quantity, "side": event.side,
                   "venue_sequence": event.venue_sequence}
            if event.stream == "trades-all":
                row["source"] = event.source
            writer.write(row, bind=bind_arg(getattr(self, "segment_dedup", None), event))
        elif isinstance(event, CanonicalMarkPriceEvent):
            if event.stream == "mark-price":
                self.mark_writer.write({**base, "mark_price": event.mark_price})
            elif event.stream == "index-tickers":
                self.index_writer.write({**base, "index_price": event.index_price})
            elif event.stream == "funding-rate":
                self.funding_writer.write({
                    **base, "funding_rate": event.funding_rate, "funding_time": event.funding_time,
                    "next_funding_rate": event.next_funding_rate, "next_funding_time": event.next_funding_time,
                    "sett_funding_rate": event.sett_funding_rate, "sett_state": event.sett_state,
                    "premium": event.premium, "interest_rate": event.interest_rate,
                    "max_funding_rate": event.max_funding_rate, "min_funding_rate": event.min_funding_rate,
                    "formula_type": event.formula_type, "method": event.method,
                    "impact_value": event.impact_value,
                })
        elif isinstance(event, CanonicalOIEvent):
            self.oi_writer.write({
                **base, "open_interest": event.open_interest,
                "oi_ccy": event.oi_ccy, "oi_usd": event.oi_usd,
                "oi_unit": event.unit.value,
            })
        elif isinstance(event, CanonicalLiquidationEvent):
            self.liq_writer.write({
                **base, "inst_id": event.inst_id, "side": event.side, "price": event.price,
                "quantity": event.quantity, "bk_loss": event.bk_loss, "ccy": event.ccy,
                "pos_side": event.pos_side, "inst_family": event.inst_family, "uly": event.uly,
            })

    # -- lifecycle ------------------------------------------------------------

    async def run(self) -> None:
        await self.client.start()

    async def shutdown(self) -> None:
        # Every step is guarded: none may stop the writer closes below.
        # ``WebSocketClient.stop()`` is a plain method returning ``None``;
        # awaiting it unconditionally raised ``TypeError`` here and skipped
        # every writer close.
        try:
            stopped = self.client.stop()
            if inspect.isawaitable(stopped):
                await stopped
        except Exception as exc:  # noqa: BLE001
            logger.error("okx_client_stop_failed", error=f"{type(exc).__name__}: {exc}")
        self._close_writer_reporting_failure(self.raw_wire_writer, "raw_wire_writer", "OKX")
        self._close_writer_reporting_failure(self.trades_writer, "trades_writer", "OKX")
        self._close_writer_reporting_failure(self.trades_all_writer, "trades_all_writer", "OKX")
        self._close_writer_reporting_failure(self.mark_writer, "mark_writer", "OKX")
        self._close_writer_reporting_failure(self.index_writer, "index_writer", "OKX")
        self._close_writer_reporting_failure(self.funding_writer, "funding_writer", "OKX")
        self._close_writer_reporting_failure(self.oi_writer, "oi_writer", "OKX")
        self._close_writer_reporting_failure(self.liq_writer, "liq_writer", "OKX")
        self._close_writer_reporting_failure(self.quality_writer, "quality_writer", "OKX")

    def _close_writer_reporting_failure(self, writer, writer_name: str, exchange: str) -> None:
        """Close one writer; on failure, report it and still return.

        Hostile-audit finding (this session): none of these close() calls
        were guarded, so one writer's finalization failure (a real
        disk-full/fsync/rename failure -- _close_segment's core publish
        steps have no failure handling of their own, only the metadata
        sidecar step does) would propagate out of shutdown() and abort
        every writer still left to close, including quality_writer.
        Reported through quality_writer -- still open at this point for
        every writer except quality_writer itself, which is always closed
        last precisely so this reporting path works.
        """
        try:
            writer.close()
        except Exception as exc:  # noqa: BLE001 - must not abort closing the rest
            logger.error("storage_shutdown_close_failed", writer=writer_name, error=str(exc))
            if writer_name != "quality_writer":
                try:
                    self._persist_quality_event({
                        "exchange": exchange, "stream": getattr(writer, "stream_name", writer_name),
                        "event_type": "ERROR",
                        "reason": f"storage_shutdown_close_failed:{writer_name}:{type(exc).__name__}",
                    })
                except Exception as sink_exc:  # noqa: BLE001 - reporting must not itself abort shutdown
                    logger.error("storage_shutdown_close_failure_report_failed",
                                 writer=writer_name, error=str(sink_exc))


async def _main(data_dir: str, url: str) -> int:
    """Run the collector; return the process exit status (0 for a signalled
    stop, ``EXIT_FATAL_STORAGE`` for a terminal raw-evidence failure). The
    application task is supervised, never left as an unobserved background task."""
    app = OKXCollectorApp(data_dir=data_dir, url=url)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    return await supervise_standalone_runner(app, stop)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--url", default=OKX_PUBLIC_WS_URL)
    args = parser.parse_args()
    logger.info("okx_collector_starting", url=args.url, data_dir=args.data_dir,
               note="requires outbound access to ws.okx.com:8443; "
                    "not available in every environment (see docs/EXECUTION_STATUS.md)")
    raise SystemExit(asyncio.run(_main(args.data_dir, args.url)))
