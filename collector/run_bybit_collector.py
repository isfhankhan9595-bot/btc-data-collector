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
from collector.collector.segment_dedup import StreamSpec, attach_segment_dedup, bind_arg
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
from collector.collector.instrument import BYBIT_LINEAR_BTCUSDT, InstrumentIdError
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import BookQuality, QualityEventType
from collector.collector.quality_wal import QualityEventWAL, QualityWALCorruption
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
    def __init__(self, data_dir: str = "data", url: str = BYBIT_PUBLIC_WS_URL,
                enable_segment_dedup: bool = True) -> None:
        self.url = url
        # P0-2 parity with run_collector.CollectorApp (PR #94): every event
        # entering _persist_quality_event is WAL-protected first, and the WAL
        # checkpoint advances only from on_segment_durable, i.e. after the
        # segment holding the event is published (fsync + rename + directory
        # fsync). The state is set up BEFORE the writer exists because the
        # writer's hook references it. Segmenting is deliberately UNCHANGED
        # (segment_rows=1 / segment_seconds=1): P0-3 batching is not part of
        # this change, so each write() still publishes its own segment.
        self._init_quality_durability_state()
        self.quality_writer = ParquetWriter(
            "bybit_quality_events", QUALITY_EVENTS_SCHEMA, base_dir=data_dir,
            exchange="BYBIT", segment_rows=1, segment_seconds=1,
            on_segment_durable=self._on_quality_segment_durable)
        # Replays anything a previous process left un-checkpointed. Needs only
        # quality_writer, which exists now. It must run before the other writers
        # are built: their constructors can report orphan recovery through
        # _persist_quality_event, and those reports must find the WAL already open.
        self._recover_quality_wal(self.quality_writer.stream_dir / "wal")
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
        self.adapter.set_unhandled_sink(self._record_adapter_unhandled)
        # P0-4: trades_writer is this runner's ONLY trade writer and
        # receives every admitted trade unconditionally -- the correct
        # recovery anchor (see run_collector.py's identical reasoning).
        self.segment_dedup = None
        if enable_segment_dedup:
            self.segment_dedup = attach_segment_dedup(self.adapter, [
                StreamSpec("trades", self.trades_writer, "BYBIT", "linear_perpetual"),
            ])
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
                           decode_error=None, parsed=None, control_frame=False,
                           local_receive_ns=None, receive_mono_ns=None):
        """Persist the exact frame before any lossy transformation.

        Signature matches run_collector.CollectorApp._capture_raw_frame,
        control_frame included, deliberately -- see docs/EXECUTION_STATUS.md
        for why every on_raw_frame callback must accept it now regardless of
        whether the venue uses raw-text control frames (Bybit does not).
        """
        if self.raw_capture is None:
            return
        record = RawWireRecord(
            local_receive_ts=local_receive_ts, local_receive_ns=local_receive_ns,
            receive_mono_ns=receive_mono_ns, payload=frame, venue="BYBIT",
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

    # ------------------------------------------------------------------
    # P0-2 quality-event durability state (mirrors run_collector.CollectorApp).
    #
    # Invariant: wal checkpoint N on disk  =>  every quality event with WAL
    # seq <= N is durably in a PUBLISHED bybit_quality_events segment.
    #   _quality_wal_inflight  seqs WAL-appended but not yet in a published
    #                          segment.
    #   _quality_segment_seqs  seqs written into the currently open segment.
    # On publication the open segment's seqs leave inflight and the
    # checkpoint moves to min(inflight)-1 (or the highest published seq when
    # nothing is in flight) -- a contiguous prefix, never a max.
    # ------------------------------------------------------------------
    def _init_quality_durability_state(self) -> None:
        self._quality_checkpoint_blocked = False
        self._quality_checkpoint_block_reason = None
        self._quality_wal_inflight = set()
        self._quality_segment_seqs = set()
        self._quality_published_hwm = None

    def _ensure_quality_state(self) -> None:
        if not hasattr(self, "_quality_wal_inflight"):
            self._init_quality_durability_state()

    def _quality_checkpoint_is_blocked(self) -> bool:
        return getattr(self, "_quality_checkpoint_blocked", False)

    def _block_quality_checkpoint(self, why: str) -> None:
        """Latch: stop advancing the WAL checkpoint for the rest of this
        process's life. Everything stays in the WAL and is reconciled (same
        quality_event_id) by the next startup's recovery. Fail closed."""
        self._ensure_quality_state()
        if not self._quality_checkpoint_blocked:
            self._quality_checkpoint_block_reason = why
        self._quality_checkpoint_blocked = True
        logger.error("bybit_quality_checkpoint_blocked", reason=why)

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
            logger.error("bybit_quality_wal_rotation_failed")

    def _recover_quality_wal(self, wal_dir) -> None:
        """Startup recovery: discover the WAL, recover exact event content not
        yet checkpointed, replay it into Parquet, and checkpoint only the
        contiguous prefix that actually succeeded (same contract and same
        reasoning as run_collector.CollectorApp._recover_quality_wal)."""
        self._ensure_quality_state()
        self._quality_wal_recovery_error = None
        resume_seq = QualityEventWAL.highest_recovered_seq(wal_dir)
        try:
            recovered_events = QualityEventWAL.recover(wal_dir)
        except QualityWALCorruption as exc:
            # Loud, never silently healthy -- but it must not stop market-data
            # capture. The corrupt file is preserved (never deleted here) and
            # checkpointing stays off until an operator resolves it, so a
            # later checkpoint(N) cannot cover its seqs and delete the evidence.
            recovered_events = []
            self._quality_wal_recovery_error = str(exc)
            self._quality_checkpoint_blocked = True
            self._quality_checkpoint_block_reason = "wal_corruption_on_startup"
        self._quality_wal = QualityEventWAL(wal_dir, start_seq=resume_seq)
        if self._quality_wal_recovery_error is not None:
            self._persist_quality_event({
                "stream": "bybit_quality_events", "event_type": QualityEventType.ERROR,
                "reason": f"quality_wal_corruption_on_startup:{self._quality_wal_recovery_error}",
                "rows_lost": None})
        # Strict seq order. Each event is tracked in-flight BEFORE it is
        # written and leaves in-flight only when its segment is published, so
        # on a failure at seq k the event and everything after it stay in the
        # WAL (k remains in-flight, pinning the checkpoint below k).
        replay_failed = False
        for recovered in recovered_events:
            try:
                # "_wal_seq" = already durably in the WAL (do not append again);
                # quality_event_id flows through so the replayed row is
                # reconcilable with any earlier copy by id.
                self._persist_quality_event({**recovered, "_wal_seq": recovered["seq"]})
            except Exception:  # noqa: BLE001
                logger.error("bybit_quality_event_recovery_persist_failed", seq=recovered.get("seq"))
                replay_failed = True
                break
        try:
            self.quality_writer.publish_open_segment()
        except Exception as exc:  # noqa: BLE001
            logger.error("bybit_quality_event_recovery_publish_failed", error=str(exc))
            self._block_quality_checkpoint("recovery_publish_failed")
        if replay_failed:
            self._block_quality_checkpoint("recovery_persist_failed")

    def _persist_quality_event(self, event: dict) -> None:
        """Single choke point for EVERY Bybit quality event (writer sinks,
        websocket client, adapter-unhandled, book transitions, shutdown
        reports, recovery replay).

        Durability contract (same as run_collector.CollectorApp): before the
        event can sit in quality_writer's buffer it is WAL-protected -- unless
        it already carries WAL provenance (``_wal_seq``: replayed from the
        WAL). Its seq is tracked in-flight until the segment holding it is
        published; only then may the checkpoint cover it.

        If the WAL append fails the event is still written (stamped with the
        id the failed append assigned, so any WAL-resident copy is
        reconcilable), and since no WAL record is known to back it the open
        segment is force-published immediately. A write or publish failure
        latches the checkpoint closed and PROPAGATES -- never swallowed.
        """
        self._ensure_quality_state()
        wal = getattr(self, "_quality_wal", None)
        wal_seq = event.get("_wal_seq")
        wal_protected = wal is not None and wal_seq is not None
        if wal is not None and wal_seq is None:
            # The row below takes timestamp / local_process_ts from the wall
            # clock when the event does not carry them. Pin exactly those
            # values into the WAL record NOW, so a replayed row carries the
            # original instants instead of the replay time. Same values the
            # direct path writes; nothing is invented.
            now_ms = int(time.time() * 1000)
            event = {k: v for k, v in event.items() if k != "_wal_seq"}
            event.setdefault("local_ts", now_ms)
            event.setdefault("local_process_ts", now_ms)
            event_type_for_wal = event.get("event_type", QualityEventType.ERROR)
            if isinstance(event_type_for_wal, QualityEventType):
                event_type_for_wal = event_type_for_wal.value
            try:
                event_id = wal.append({**event, "event_type": event_type_for_wal})
            except Exception as exc:  # noqa: BLE001 - any append failure, not only OSError
                logger.error("bybit_quality_event_wal_append_failed_in_persist", reason=event.get("reason"))
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
            "local_process_ts": event.get("local_process_ts", int(time.time() * 1000)),
            "quality_event_id": event.get("quality_event_id"),
        }

        def _bind(_token, _seq=wal_seq):
            # Called by the writer AFTER any hour rollover and BEFORE the
            # append: attributes this seq to the segment that will really
            # contain the row.
            if _seq is not None:
                self._quality_segment_seqs.add(_seq)
        try:
            self.quality_writer.write(row, bind=_bind)
        except Exception:
            # Writer state is now uncertain: fail closed, the event stays in
            # the WAL (if it got there), and the failure is NOT swallowed.
            self._block_quality_checkpoint("quality_writer_write_failed")
            raise
        if not wal_protected:
            try:
                self.quality_writer.publish_open_segment()
            except Exception:
                self._block_quality_checkpoint("unprotected_event_publish_failed")
                raise

    def _on_client_quality_event(self, event_type: str, reason: str,
                                 connection_id=None, stream_group=None) -> None:
        """WebSocketClient calls this UNGUARDED from its connect/disconnect/
        error paths (a raise here would escape into the client's own handler,
        and from its except-branch out of the connection loop). So this
        boundary -- and only this one -- does not re-raise: a persistence
        failure is logged at ERROR and the checkpoint is already latched by
        _persist_quality_event, leaving any WAL copy for restart recovery."""
        try:
            self._persist_quality_event({"stream": stream_group or "bybit_websocket",
                                         "event_type": event_type, "reason": reason,
                                         "connection_id": connection_id,
                                         "local_ts": int(time.time() * 1000)})
        except Exception:  # noqa: BLE001
            logger.error("bybit_websocket_quality_event_persist_failed",
                         event_type=event_type, reason=reason)
            self._block_quality_checkpoint("websocket_quality_event_persist_failed")

    def _record_adapter_unhandled(self, message) -> None:
        """Wires BybitAdapter's unhandled/duplicate outcomes into durable
        quality events -- previously missing entirely for this venue (the
        other three runners already had this; unrouted/malformed/duplicate
        Bybit messages were counted on the adapter but never persisted).
        Added here because trade deduplication's DUPLICATE quality event
        would otherwise be silently invisible for Bybit specifically,
        undermining this feature's cross-venue uniformity -- a direct
        prerequisite for this task, not unrelated scope creep."""
        self._persist_quality_event(message.to_quality_event())

    # -- message handling --------------------------------------------------

    async def _handle_message(self, data: dict, local_receive_ts: int, connection_id=None) -> None:
        self.messages_handled += 1
        try:
            events = self.adapter.normalize(data, local_receive_ts=local_receive_ts)
            for event in events:
                self._persist_event(event)
        finally:
            # P0-4: message boundary on EVERY exit path (see SegmentDedupHandle.end_message).
            segment_dedup = getattr(self, "segment_dedup", None)
            if segment_dedup is not None:
                segment_dedup.end_message()

    def _persist_event(self, event) -> None:
        base = {
            "timestamp": event.local_receive_ts,
            "exchange_timestamp": event.exchange_event_ts,
            "local_timestamp": event.local_receive_ts,
            # Every one of this runner's five canonical writers is safe to
            # stamp with a single validated constant, not a per-event
            # resolution: _topics() builds every subscription from the one
            # module-level SYMBOL constant (config.py), so this process
            # cannot receive any instrument other than BYBIT_LINEAR_BTCUSDT
            # -- there is no other symbol for the wire to disagree with.
            # BybitAdapter DOES stamp every instrument-scoped event with
            # BYBIT_LINEAR_BTCUSDT (ExchangeAdapter.__init_subclass__ wraps its
            # normalize()). The constant is therefore not a substitute for the
            # event's identity: it is cross-checked against it below, and a
            # contradiction fails loudly instead of being persisted.
            "instrument_key": BYBIT_LINEAR_BTCUSDT.key,
        }
        if event.instrument is not None and event.instrument != BYBIT_LINEAR_BTCUSDT:
            raise InstrumentIdError(
                f"Bybit event carries {event.instrument.key}, not {BYBIT_LINEAR_BTCUSDT.key}")
        if isinstance(event, CanonicalOrderBookEvent):
            self._apply_orderbook(event, base)
        elif isinstance(event, CanonicalTradeEvent):
            self.trades_writer.write({
                **base, "trade_id": event.trade_id, "price": event.price,
                "quantity": event.quantity, "side": event.side,
                "venue_sequence": event.venue_sequence,
                "block_trade": bool(event.block_trade) if event.block_trade is not None else None,
                "rpi": bool(event.rpi) if event.rpi is not None else None,
            }, bind=bind_arg(getattr(self, "segment_dedup", None), event))
        elif isinstance(event, CanonicalMarkPriceEvent):
            self.mark_writer.write({
                **base, "mark_price": event.mark_price, "index_price": event.index_price,
                "funding_rate": event.funding_rate, "next_funding_time": event.next_funding_time,
                "carried_forward": list(event.carried_forward),
            })
        elif isinstance(event, CanonicalOIEvent):
            self.oi_writer.write({
                **base, "open_interest": event.open_interest,
                "oi_unit": event.unit.value,
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
            # P0-9: the book engine holds exact Decimals; pass them through and let
            # numeric.column_value (the single boundary) derive the float64
            # columns and the exact-text companions.
            "bids_price": [p for p, _ in bids], "bids_qty": [q for _, q in bids],
            "asks_price": [p for p, _ in asks], "asks_qty": [q for _, q in asks],
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
        self._close_writer_reporting_failure(self.raw_wire_writer, "raw_wire_writer", "BYBIT")
        self._close_writer_reporting_failure(self.ob_writer, "ob_writer", "BYBIT")
        self._close_writer_reporting_failure(self.trades_writer, "trades_writer", "BYBIT")
        self._close_writer_reporting_failure(self.mark_writer, "mark_writer", "BYBIT")
        self._close_writer_reporting_failure(self.oi_writer, "oi_writer", "BYBIT")
        self._close_writer_reporting_failure(self.liq_writer, "liq_writer", "BYBIT")
        # quality_writer closes LAST: its close PUBLISHES the final partial
        # segment and thereby checkpoints that segment's WAL seqs. If that
        # publish fails its events stay in the WAL, uncheckpointed, to be
        # replayed by the next startup. Only then is the WAL closed (the
        # publish hook needs it open).
        quality_closed = self._close_writer_reporting_failure(self.quality_writer, "quality_writer", "BYBIT")
        if not quality_closed:
            self._block_quality_checkpoint("quality_writer_close_failed")
        wal = getattr(self, "_quality_wal", None)
        if wal is not None:
            try:
                wal.close()
            except Exception as exc:  # noqa: BLE001 - must not abort the rest of shutdown
                logger.error("bybit_quality_wal_close_failed", error=str(exc))

    def _close_writer_reporting_failure(self, writer, writer_name: str, exchange: str) -> bool:
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
        closed_ok = True
        try:
            writer.close()
        except Exception as exc:  # noqa: BLE001 - must not abort closing the rest
            closed_ok = False
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
        return closed_ok


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
