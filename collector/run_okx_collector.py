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
import json
import signal
import time

from collector.collector.adapters.okx import OKXAdapter
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
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import QualityEventType
from collector.collector.raw_capture import RAW_WIRE_SCHEMA, RawCapture, RawWireRecord
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
                 channels=D11_CHANNELS) -> None:
        self.url = url
        self.inst_id = inst_id
        self.channels = tuple(channels)

        # Own okx_-prefixed streams (PR #19 storage-namespace phase);
        # exchange="OKX" so this runner's own storage faults are never
        # attributed to Binance/Bybit (ParquetWriter's default).
        self.quality_writer = ParquetWriter(
            venue_stream("OKX", "quality_events"), QUALITY_EVENTS_SCHEMA,
            base_dir=data_dir, exchange="OKX", segment_rows=1, segment_seconds=1)
        self.raw_wire_writer = ParquetWriter(
            venue_stream("OKX", "raw_wire"), RAW_WIRE_SCHEMA, base_dir=data_dir,
            exchange="OKX", quality_event_sink=self._persist_quality_event)
        self.raw_capture = RawCapture(
            self.raw_wire_writer, None, quality_event_sink=self._persist_quality_event)

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

        self.adapter = OKXAdapter(inst_id=inst_id, index_inst_id=index_inst_id)
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
        )

    # -- raw capture --------------------------------------------------------

    def _capture_raw_frame(self, payload, *, local_receive_ts, connection_id=None,
                           connection_generation=None, decode_ok=True,
                           decode_error=None, parsed=None, control_frame=False):
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
            local_receive_ts=local_receive_ts,
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

    def _persist_quality_event(self, event: dict) -> None:
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
        except Exception as exc:  # noqa: BLE001 - quality write must not break ingest
            logger.error("okx_quality_write_failed", error=str(exc))

    def _on_client_quality_event(self, event_type, reason, connection_id=None, stream_group=None) -> None:
        self._persist_quality_event({
            "stream": stream_group or "okx_public", "event_type": event_type, "reason": reason,
            "connection_id": connection_id, "local_ts": int(time.time() * 1000)})

    def _record_adapter_unhandled(self, message) -> None:
        self._persist_quality_event(message.to_quality_event())

    # -- message handling ---------------------------------------------------

    async def _handle_message(self, data: dict, local_receive_ts: int, connection_id=None) -> None:
        self.messages_handled += 1
        events = self.adapter.normalize(data, local_receive_ts=local_receive_ts)
        for event in events:
            self._persist_event(event)

    def _persist_event(self, event) -> None:
        if isinstance(event, CanonicalOrderBookEvent):
            return  # books storage is out of scope here; see module docstring.
        base = {
            "timestamp": event.local_receive_ts,
            "exchange_timestamp": event.exchange_event_ts,
            "local_timestamp": event.local_receive_ts,
        }
        if isinstance(event, CanonicalTradeEvent):
            writer = self.trades_all_writer if event.stream == "trades-all" else self.trades_writer
            row = {**base, "trade_id": event.trade_id, "price": event.price,
                   "quantity": event.quantity, "side": event.side,
                   "venue_sequence": event.venue_sequence}
            if event.stream == "trades-all":
                row["source"] = event.source
            writer.write(row)
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
        await self.client.stop()
        for writer in (self.quality_writer, self.raw_wire_writer, self.trades_writer,
                       self.trades_all_writer, self.mark_writer, self.index_writer,
                       self.funding_writer, self.oi_writer, self.liq_writer):
            writer.close()


async def _main(data_dir: str, url: str) -> None:
    app = OKXCollectorApp(data_dir=data_dir, url=url)
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
    parser.add_argument("--url", default=OKX_PUBLIC_WS_URL)
    args = parser.parse_args()
    logger.info("okx_collector_starting", url=args.url, data_dir=args.data_dir,
               note="requires outbound access to ws.okx.com:8443; "
                    "not available in every environment (see docs/EXECUTION_STATUS.md)")
    asyncio.run(_main(args.data_dir, args.url))
