"""Live Bybit v5 linear-perpetual public collector.

Protocol facts used here were verified 2026-09-19 against official Bybit
documentation, not assumed or taken from third-party SDKs:

    https://bybit-exchange.github.io/docs/v5/ws/connect
    https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook

- Endpoint: wss://stream.bybit.com/v5/public/linear
- Subscribe: {"op": "subscribe", "args": [<topics>]}
- Heartbeat: client sends {"op": "ping"} every ~20s; the documented public
  reply is {"success": true, "ret_msg": "pong", "conn_id": "...", "op":
  "ping"} -- JSON, not a raw-text control frame the way OKX's is.

Why the keepalive here is fire-and-forget (``expect=None``)
-------------------------------------------------------------
``WebSocketClient``'s reply-tracking (``Keepalive.expect`` /
``_awaiting_reply_since``) only clears on an exact raw-text match against
``control_frames`` -- built for OKX's literal ``"ping"``/``"pong"`` strings.
Bybit's pong is JSON with a dynamic ``conn_id``, so it can never satisfy an
exact-string match; wiring ``expect`` here would mark every ping as awaiting
a reply that this mechanism can never recognise as arriving, and eventually
force a spurious reconnect on an otherwise healthy connection. Extending the
reply-matching logic to also inspect decoded JSON is a real option, but not
one to make now: it touches the same shared file whose narrow, insufficiently
tested change (the ``control_frame`` kwarg) took down live Binance raw
capture earlier this session (see docs/EXECUTION_STATUS.md, "P0 hotfix").
Fire-and-forget is the conservative choice while that lesson is fresh --
BTCUSDT public channels push data essentially continuously, so
``_last_inbound_monotonic`` (reset by any inbound frame, not just a pong)
is the dominant liveness signal regardless; a genuinely dead socket still
surfaces via a failed ``_send()`` in the keepalive loop. This is a stated,
conservative simplification, not a protocol guess -- every fact above is
verified. Extending the reply-matching mechanism to cover JSON pongs
generically (Bybit and any future venue with a JSON heartbeat) is future
work, not a defect in what ships here.

Storage: separate ``bybit_*`` streams (see config.py's docstring by
BYBIT_ORDERBOOK_SCHEMA for why) -- never the shared Binance-named streams,
and never the Binance canonical schemas, which have no exchange column.

Standalone by design: imports nothing from run_collector.py, no shared
mutable state with the Binance process. Two independent OS processes running
this and run_collector.py against the same --data-dir is the intended
deployment shape.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import signal
import time

from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.book_engine import LocalBook
from collector.collector.canonical import (
    CanonicalLiquidationEvent,
    CanonicalMarkPriceEvent,
    CanonicalOIEvent,
    CanonicalOrderBookEvent,
    CanonicalTradeEvent,
)
from collector.collector.config import (
    BYBIT_LIQUIDATION_SCHEMA,
    BYBIT_MARKPRICE_SCHEMA,
    BYBIT_OPENINTEREST_SCHEMA,
    BYBIT_ORDERBOOK_DEPTH,
    BYBIT_ORDERBOOK_SCHEMA,
    BYBIT_PUBLIC_WS_URL,
    BYBIT_TRADES_SCHEMA,
    QUALITY_EVENTS_SCHEMA,
    SYMBOL,
)
from collector.collector.backoff import ExponentialBackoff
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import BookQuality, QualityEventType
from collector.collector.raw_capture import RAW_WIRE_SCHEMA, RawCapture, RawWireRecord
from collector.collector.utils import logger
from collector.collector.websocket_client import Keepalive, WebSocketClient


def _topics() -> list[str]:
    """The four channels BybitAdapter declares, with a real depth substituted.

    "orderbook.{depth}." is a template key in channel_event_types, not a
    literal topic -- the real depth is substituted only here, at the wire
    boundary; the adapter's own routing matches on the "orderbook." prefix
    regardless of depth, so this substitution cannot desynchronise the two.
    """
    return [
        f"orderbook.{BYBIT_ORDERBOOK_DEPTH}.{SYMBOL}",
        f"publicTrade.{SYMBOL}",
        f"tickers.{SYMBOL}",
        f"allLiquidation.{SYMBOL}",
    ]


class BybitCollectorApp:
    def __init__(self, data_dir: str = "data", url: str = BYBIT_PUBLIC_WS_URL) -> None:
        self.url = url
        self.quality_writer = ParquetWriter(
            "bybit_quality_events", QUALITY_EVENTS_SCHEMA, base_dir=data_dir,
            exchange="BYBIT", segment_rows=1, segment_seconds=1)
        self.raw_wire_writer = ParquetWriter(
            "bybit_raw_wire", RAW_WIRE_SCHEMA, base_dir=data_dir,
            exchange="BYBIT", quality_event_sink=self._persist_quality_event)
        self.raw_capture = RawCapture(
            self.raw_wire_writer, None, quality_event_sink=self._persist_quality_event)

        self.ob_writer = ParquetWriter(
            "bybit_orderbook", BYBIT_ORDERBOOK_SCHEMA, base_dir=data_dir,
            exchange="BYBIT", quality_event_sink=self._persist_quality_event)
        self.trades_writer = ParquetWriter(
            "bybit_trades", BYBIT_TRADES_SCHEMA, base_dir=data_dir, exchange="BYBIT")
        self.mark_writer = ParquetWriter(
            "bybit_markprice", BYBIT_MARKPRICE_SCHEMA, base_dir=data_dir, exchange="BYBIT")
        self.oi_writer = ParquetWriter(
            "bybit_openinterest", BYBIT_OPENINTEREST_SCHEMA, base_dir=data_dir, exchange="BYBIT")
        self.liq_writer = ParquetWriter(
            "bybit_liquidation", BYBIT_LIQUIDATION_SCHEMA, base_dir=data_dir, exchange="BYBIT")

        self.adapter = BybitAdapter()
        self.book = LocalBook("BYBIT")
        self.running = False
        self.messages_handled = 0

        self.client = WebSocketClient(
            url=self.url,
            on_message=self._handle_message,
            on_raw_frame=self._capture_raw_frame,
            on_open=self._on_open,
            on_quality_event=self._on_client_quality_event,
            keepalive=Keepalive(payload=json.dumps({"op": "ping"}), interval_s=20.0,
                                expect=None, timeout_s=10.0),
            backoff=ExponentialBackoff(base_delay=1.0, max_delay=30.0, max_attempts=None),
            stream_group="bybit",
        )

    # -- raw capture ---------------------------------------------------

    def _capture_raw_frame(self, frame, *, local_receive_ts, connection_id=None,
                           connection_generation=None, decode_ok=True,
                           decode_error=None, parsed=None, control_frame=False):
        """Persist the exact frame before any lossy transformation.

        Signature matches run_collector.CollectorApp._capture_raw_frame,
        control_frame included, deliberately -- see docs/EXECUTION_STATUS.md
        for why every on_raw_frame callback must accept it now regardless of
        whether the venue uses raw-text control frames (Bybit does not).
        """
        if self.raw_capture is None:
            return
        record = RawWireRecord(
            local_receive_ts=local_receive_ts, payload=frame, venue="BYBIT",
            connection_id=connection_id, connection_generation=connection_generation,
            symbol=SYMBOL, market_type="linear_perpetual",
            decode_ok=decode_ok, decode_error=decode_error,
            exchange_event_ts=(parsed or {}).get("ts") if decode_ok else None,
        )
        self.raw_capture.capture_wire(record)

    async def _on_open(self, send) -> None:
        """``WebSocketClient`` calls this with its ``_send`` bound method
        directly (``await self.on_open(self._send)``), not the client
        instance -- confirmed by reading the call site, not assumed."""
        await send(json.dumps({"op": "subscribe", "args": _topics()}))

    # -- quality events --------------------------------------------------

    def _persist_quality_event(self, event: dict) -> None:
        event_type = event.get("event_type", QualityEventType.ERROR)
        if isinstance(event_type, QualityEventType):
            event_type = event_type.value
        row = {
            "timestamp": event.get("local_ts", int(time.time() * 1000)),
            "exchange": "BYBIT", "stream": event.get("stream", "bybit"),
            "event_type": event_type, "reason": event.get("reason"),
            "gap_size_ms": event.get("gap_size_ms"), "rows_lost": str(event.get("rows_lost")),
            "quality_state": event.get("quality_state"), "connection_id": event.get("connection_id"),
            "previous_state": event.get("previous_state"), "new_state": event.get("new_state"),
            "expected_previous_update_id": event.get("expected_previous_update_id"),
            "actual_previous_update_id": event.get("actual_previous_update_id"),
            "update_id": event.get("update_id"), "first_update_id": event.get("first_update_id"),
            "previous_update_id": event.get("previous_update_id"),
            "local_receive_ts": event.get("local_receive_ts"),
            "local_process_ts": int(time.time() * 1000),
        }
        try:
            self.quality_writer.write(row)
        except Exception as exc:  # noqa: BLE001 - a quality write must not crash ingestion
            logger.warning("bybit_quality_write_failed", error=str(exc))

    def _on_client_quality_event(self, event_type: str, reason: str,
                                 connection_id=None, stream_group=None) -> None:
        self._persist_quality_event({"stream": stream_group or "bybit_websocket",
                                     "event_type": event_type, "reason": reason,
                                     "connection_id": connection_id,
                                     "local_ts": int(time.time() * 1000)})

    # -- message handling --------------------------------------------------

    async def _handle_message(self, data: dict, local_receive_ts: int, connection_id=None) -> None:
        self.messages_handled += 1
        events = self.adapter.normalize(data, local_receive_ts=local_receive_ts)
        for event in events:
            self._persist_event(event)

    def _persist_event(self, event) -> None:
        base = {
            "timestamp": event.local_receive_ts,
            "exchange_timestamp": event.exchange_event_ts,
            "local_timestamp": event.local_receive_ts,
        }
        if isinstance(event, CanonicalOrderBookEvent):
            self._apply_orderbook(event, base)
        elif isinstance(event, CanonicalTradeEvent):
            self.trades_writer.write({
                **base, "trade_id": event.trade_id, "price": event.price,
                "quantity": event.quantity, "side": event.side,
                "venue_sequence": event.venue_sequence,
                "block_trade": bool(event.block_trade) if event.block_trade is not None else None,
                "rpi": bool(event.rpi) if event.rpi is not None else None,
            })
        elif isinstance(event, CanonicalMarkPriceEvent):
            self.mark_writer.write({
                **base, "mark_price": event.mark_price, "index_price": event.index_price,
                "funding_rate": event.funding_rate, "next_funding_time": event.next_funding_time,
                "carried_forward": list(event.carried_forward),
            })
        elif isinstance(event, CanonicalOIEvent):
            self.oi_writer.write({
                **base, "open_interest": event.open_interest,
                "carried_forward": list(event.carried_forward),
            })
        elif isinstance(event, CanonicalLiquidationEvent):
            self.liq_writer.write({
                **base, "side": event.side, "price": event.price, "quantity": event.quantity,
            })

    def _apply_orderbook(self, event: CanonicalOrderBookEvent, base: dict) -> None:
        before = self.book.state.state
        applied = self.book.apply(event)
        after = self.book.state.state
        if before is not after:
            self._persist_quality_event({
                "stream": "bybit_orderbook", "event_type": QualityEventType.SEQUENCE_GAP.value
                if after in (BookQuality.SEQUENCE_GAP, BookQuality.RECOVERING)
                else QualityEventType.RECOVERY.value,
                "reason": self.book.last_reason, "previous_state": before.value,
                "new_state": after.value, "local_ts": int(time.time() * 1000),
            })
        if applied is None:
            return
        bids, asks = applied.bids, applied.asks
        self.ob_writer.write({
            **base,
            "bids_price": [float(p) for p, _ in bids], "bids_qty": [float(q) for _, q in bids],
            "asks_price": [float(p) for p, _ in asks], "asks_qty": [float(q) for _, q in asks],
            "update_id": event.update_id, "sequence": event.sequence,
            "is_snapshot": event.is_snapshot,
        })

    # -- lifecycle ---------------------------------------------------------

    async def run(self) -> None:
        self.running = True
        await self.client.start()

    async def shutdown(self) -> None:
        self.running = False
        await self.client.stop()
        for writer in (self.quality_writer, self.raw_wire_writer, self.ob_writer,
                       self.trades_writer, self.mark_writer, self.oi_writer, self.liq_writer):
            writer.close()


async def _main(data_dir: str, url: str) -> None:
    app = BybitCollectorApp(data_dir=data_dir, url=url)
    loop = asyncio.get_event_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass  # not available on this platform

    run_task = asyncio.ensure_future(app.run())
    await stop.wait()
    await app.shutdown()
    run_task.cancel()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--url", default=BYBIT_PUBLIC_WS_URL,
                        help="Override for testnet: wss://stream-testnet.bybit.com/v5/public/linear")
    args = parser.parse_args()
    logger.info("bybit_collector_starting", url=args.url, data_dir=args.data_dir,
               note="requires outbound access to stream.bybit.com:443; "
                    "not available in every environment")
    asyncio.run(_main(args.data_dir, args.url))
