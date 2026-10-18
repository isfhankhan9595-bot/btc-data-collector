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
import inspect
import json
import signal
import time
from typing import Optional

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
from collector.collector.failure_topology import FailureRecord
from collector.collector.instrument import BYBIT_LINEAR_BTCUSDT, InstrumentIdError
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import BookQuality, QualityEventType
from collector.collector.quality_wal import QualityEventWAL, QualityWALCapacityExceeded, QualityWALCorruption
from collector.collector.raw_capture import RAW_WIRE_SCHEMA, RawCapture, RawWireRecord
from collector.collector.loop_teardown import run_bounded, teardown_exit_status
from collector.collector.standalone_failure_policy import (
    CloseOnce,
    LateFinalizer,
    StandaloneFailurePolicy,
    await_client_idle,
    writer_already_failed,
    build_stream_table,
    supervise_standalone_runner,
    tag_route,
)
from collector.collector.storage_errors import EVENT_PUBLISHED, EVENT_UNCONFIRMED, FatalStorageError
from collector.collector.utils import logger, send_telegram_alert
from collector.collector.disk_pressure import (
    EXIT_DISK_MONITOR_UNAVAILABLE, EXIT_DISK_PRESSURE, DiskPressureConfig, Severity, StoragePressureMonitor,
)
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


#: P0-3 quality-event batching for the Bybit quality writer. These deliberately
#: equal ``run_collector.QUALITY_SEGMENT_ROWS`` / ``QUALITY_SEGMENT_SECONDS`` (the
#: Futures reference; a test pins the equality) but are defined here so this runner
#: does not import the Futures runner. Was segment_rows=1 / segment_seconds=1 = one
#: Parquet file per quality event.
QUALITY_SEGMENT_ROWS = 500
QUALITY_SEGMENT_SECONDS = 30.0
#: How often the idle publisher looks for a due batch. ``segment_seconds`` is only
#: evaluated inside ``ParquetWriter.write()``, so a quiet stream needs this tick.
#: Worst-case publication delay of a buffered event = SECONDS + this tick.
QUALITY_IDLE_TICK_SECONDS = 1.0


class BybitCollectorApp:
    def __init__(self, data_dir: str = "data", url: str = BYBIT_PUBLIC_WS_URL,
                enable_segment_dedup: bool = True) -> None:
        self.url = url
        # P0-2 parity with run_collector.CollectorApp (PR #94): every event
        # entering _persist_quality_event is WAL-protected first, and the WAL
        # checkpoint advances only from on_segment_durable, i.e. after the
        # segment holding the event is published (fsync + rename + directory
        # fsync). The state is set up BEFORE the writer exists because the
        # writer's hook references it.
        #
        # P0-3: batched (was segment_rows=1 / segment_seconds=1 = one Parquet
        # file per event). Batching means write() no longer implies "durable",
        # which is safe ONLY because every WAL-protected event is appended to the
        # WAL before it is buffered and the checkpoint advances solely from
        # on_segment_durable. ``age_from_first_row`` makes the time threshold run
        # from the first buffered row, and ``_quality_idle_task`` (started in
        # run()) publishes a quiet tail via ``publish_if_due()``.
        self._init_quality_durability_state()
        self._quality_idle_task = None
        self.quality_writer = ParquetWriter(
            "bybit_quality_events", QUALITY_EVENTS_SCHEMA, base_dir=data_dir,
            exchange="BYBIT", segment_rows=QUALITY_SEGMENT_ROWS,
            segment_seconds=QUALITY_SEGMENT_SECONDS,
            on_segment_durable=self._on_quality_segment_durable, age_from_first_row=True)
        # Replays anything a previous process left un-checkpointed. Needs only
        # quality_writer, which exists now. It must run before the other writers
        # are built: their constructors can report orphan recovery through
        # _persist_quality_event, and those reports must find the WAL already open.
        self._recover_quality_wal(self.quality_writer.stream_dir / "wal")
        self.raw_wire_writer = ParquetWriter(
            "bybit_raw_wire", RAW_WIRE_SCHEMA, base_dir=data_dir,
            exchange="BYBIT", quality_event_sink=self._writer_quality_sink)
        # F5 raw-evidence contract: a typed fatal from the raw writer is NOT
        # absorbed by the fail-open path; it reaches the shared client's
        # raw-frame boundary, is classified TERMINATE by ``failure_policy`` and
        # ends in a controlled non-zero exit.
        self.raw_capture = RawCapture(
            self.raw_wire_writer, None, quality_event_sink=self._writer_quality_sink,
            fail_closed_on_fatal_storage=True)

        self.ob_writer = ParquetWriter(
            "bybit_orderbook", BYBIT_ORDERBOOK_SCHEMA, base_dir=data_dir,
            exchange="BYBIT", quality_event_sink=self._writer_quality_sink)
        self.trades_writer = ParquetWriter(
            "bybit_trades", BYBIT_TRADES_SCHEMA, base_dir=data_dir, exchange="BYBIT")
        self.mark_writer = ParquetWriter(
            "bybit_markprice", BYBIT_MARKPRICE_SCHEMA, base_dir=data_dir, exchange="BYBIT")
        self.oi_writer = ParquetWriter(
            "bybit_openinterest", BYBIT_OPENINTEREST_SCHEMA, base_dir=data_dir, exchange="BYBIT")
        self.liq_writer = ParquetWriter(
            "bybit_liquidation", BYBIT_LIQUIDATION_SCHEMA, base_dir=data_dir, exchange="BYBIT")

        # F5 failure topology for THIS runner's streams (bybit_*), built from the
        # live writers: raw -> terminate, derived -> isolate the route, quality
        # -> degrade, anything unlisted -> terminate (default-deny).
        self.failure_policy = StandaloneFailurePolicy(
            venue="BYBIT",
            streams=build_stream_table(
                raw=(self.raw_wire_writer,),
                derived={self.ob_writer.stream_name: "orderbook",
                         self.trades_writer.stream_name: "trades",
                         self.mark_writer.stream_name: "markprice",
                         self.oi_writer.stream_name: "openinterest",
                         self.liq_writer.stream_name: "liquidation"},
                quality=(self.quality_writer,), dedup_route="trades"),
            clients=lambda: (self.client,),
            report=self._report_storage_failure)

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
            on_fatal=self._on_fatal_storage,
        )
        # Late finalization (see ``_finalize_storage``): each writer's close is attempted at most
        # once, and a done-callback on every storage user that outlived ``shutdown()`` retries the
        # finalization when the LAST of them ends. ``_run_task`` is the supervisor's task running
        # ``run()`` (the client's start() lifecycle); it is recorded by ``run()``.
        self._close_once = CloseOnce()
        self._run_task = None
        self._late_finalizer = LateFinalizer(
            venue="BYBIT", finalize=lambda: self._finalize_storage(),
            users_stopped=self._storage_users_stopped, watched=self._storage_user_tasks)
        # F5-QA-04: continuous storage-pressure monitor for THIS runner's data / WAL directories.
        # An invalid threshold configuration raises here: startup fails, nothing is coerced.
        self._disk_emergency = None
        self._disk_monitor_unavailable = None
        self._disk_task = None
        self.stop_event = None      # set by ``_run_supervised``: the supervisor's own stop request
        self.storage_pressure = StoragePressureMonitor(
            label="BYBIT", config=DiskPressureConfig.from_env(),
            paths=lambda: [self.quality_writer.base_dir, self._quality_wal.wal_dir],
            wal_dir=lambda: self._quality_wal.wal_dir,
            wal_max_bytes=lambda: self._quality_wal.max_total_bytes,
            checkpoint_state=lambda: (self._quality_checkpoint_is_blocked(),
                                      getattr(self, "_quality_checkpoint_block_reason", None)))

    # -- F5 failure topology ------------------------------------------------

    @property
    def exit_code(self) -> int:
        code = self.failure_policy.exit_code
        if code != 0:
            return code                        # 70 keeps priority over any disk status
        if getattr(self, "_disk_emergency", None) is not None:
            return EXIT_DISK_PRESSURE          # 1: confirmed disk-pressure emergency
        if getattr(self, "_disk_monitor_unavailable", None) is not None:
            return EXIT_DISK_MONITOR_UNAVAILABLE   # 69: storage could not be measured (fail closed)
        return 0

    # -- F5-QA-04 storage pressure ---------------------------------------------

    def storage_pressure_status(self) -> dict:
        """Read-only storage-pressure view, separate from ``quality_channel_status`` (writer
        failure / lost / unconfirmed / WAL-only counters are not touched by disk pressure)."""
        monitor = getattr(self, "storage_pressure", None)
        if monitor is None:
            return {"monitoring": False, "severity": Severity.UNKNOWN.value}
        return monitor.status()

    @property
    def disk_emergency(self):
        return getattr(self, "_disk_emergency", None)

    @property
    def disk_monitor_unavailable(self):
        return getattr(self, "_disk_monitor_unavailable", None)

    async def storage_pressure_preflight_failed(self) -> bool:
        """Startup gate: True when ingestion must NOT start.

        ``establish_measurement`` retries the probe (bounded by the unknown grace period) until a
        valid reading exists. A reading below the emergency threshold, or no valid reading at all
        within the grace period, means the caller shuts down through ``shutdown()`` and never
        starts the websocket client. UNKNOWN is never treated as OK."""
        monitor = getattr(self, "storage_pressure", None)
        if monitor is None:
            return False
        result = await monitor.establish_measurement()
        if result.measurement_ok and result.severity != Severity.EMERGENCY:
            return False
        if result.measurement_ok:
            self._disk_emergency = monitor.status()
            logger.error("bybit_storage_pressure_emergency_at_startup", exit_status=EXIT_DISK_PRESSURE)
        else:
            self._disk_monitor_unavailable = monitor.status()
            logger.error("bybit_storage_pressure_monitor_unavailable_at_startup",
                         exit_status=EXIT_DISK_MONITOR_UNAVAILABLE,
                         error=self._disk_monitor_unavailable["last_monitor_error"],
                         unknown_for_s=self._disk_monitor_unavailable["monitor_failure_duration_s"])
        return True

    def _on_disk_emergency(self, result) -> None:
        """Runtime EMERGENCY: latch (exit status 1) and ask the supervisor for its normal
        controlled stop (the same event a SIGTERM sets). Raises if no stop channel exists, so
        the monitor retries instead of silently giving up."""
        monitor = getattr(self, "storage_pressure", None)
        self._disk_emergency = monitor.status() if monitor is not None else {"severity": "EMERGENCY"}
        logger.error("bybit_storage_pressure_emergency_shutdown", exit_status=EXIT_DISK_PRESSURE)
        stop = getattr(self, "stop_event", None)
        if stop is None:
            raise RuntimeError("no supervisor stop channel for the disk-pressure emergency")
        stop.set()

    def _on_disk_monitor_unavailable(self, result) -> None:
        """Runtime: storage could not be measured for the whole grace period. Latch separately
        (exit status 69, never 1) and ask the supervisor for its normal controlled stop. This is
        NOT a confirmed low-disk emergency. Raises if no stop channel exists (retried)."""
        monitor = getattr(self, "storage_pressure", None)
        self._disk_monitor_unavailable = (monitor.status() if monitor is not None
                                          else {"severity": "UNKNOWN"})
        logger.error("bybit_storage_pressure_monitor_unavailable_shutdown",
                     exit_status=EXIT_DISK_MONITOR_UNAVAILABLE,
                     error=self._disk_monitor_unavailable.get("last_monitor_error"),
                     unknown_for_s=self._disk_monitor_unavailable.get("monitor_failure_duration_s"))
        stop = getattr(self, "stop_event", None)
        if stop is None:
            raise RuntimeError("no supervisor stop channel for the disk-monitor-unavailable stop")
        stop.set()

    async def _storage_pressure_loop(self) -> None:
        monitor = getattr(self, "storage_pressure", None)
        if monitor is not None:
            await monitor.run(self._on_disk_emergency, self._on_disk_monitor_unavailable)

    def _on_fatal_storage(self, exc, origin: str = "handler", *, route=None) -> str:
        """Classify and latch one typed storage fatal (see ``failure_policy``).
        Safe to call from the websocket worker: it only latches."""
        return self.failure_policy.on_fatal(exc, origin, route=route)

    def _writer_quality_sink(self, event: dict) -> None:
        """Quality sink handed to every writer / RawCapture.

        ``_persist_quality_event`` re-raises when the quality writer is FAILED. A
        raw or derived writer calls its sink unguarded at rollover / migration /
        orphan-drop time, so that fatal must not escape into the OTHER writer's
        write(): the raw boundary would read it as a raw-evidence failure. A
        typed fatal of THIS runner's quality channel is latched (degrade, once)
        and contained; a typed fatal of any other stream and every ordinary
        exception propagate unchanged (see ``_emit_quality_event``)."""
        self._emit_quality_event(event)

    # -- quality channel status ---------------------------------------------

    @property
    def quality_degraded(self):
        """The latched quality-channel ``FailureRecord`` (or ``None`` while healthy)."""
        return self.failure_policy.quality_degraded

    def quality_channel_status(self) -> dict:
        """Operator view of the quality channel. Market-data capture is unaffected by
        a degraded channel. The three counters are disjoint and mean exactly this:

        * ``quality_events_wal_only``       -- the event's quality-WAL record is established
          (the WAL append returned); it is replayed into Parquet by the next start.
        * ``quality_events_wal_unconfirmed`` -- the WAL append raised after assigning an id,
          so a record MAY exist; or the event's segment was renamed into place but the directory
          fsync failed and no WAL record exists (publication unproven). Neither counted as
          retained nor as lost.
        * ``quality_events_lost``           -- no WAL record and no usable writer: not
          durably recorded, so counted as lost.

        ``quality_degraded`` (the quality writer itself failed) and ``quality_checkpoint_blocked``
        (the quality WAL can no longer advance its checkpoint; ``quality_checkpoint_block_reason``
        says why) are independent: a blocked checkpoint is NOT healthy merely because the writer can
        still publish rows. While blocked, published events stay uncheckpointed and are replayed at the
        next start (at-least-once; duplicates carry the same ``quality_event_id``).
        """
        record = self.failure_policy.quality_degraded
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

    def _report_storage_failure(self, record: FailureRecord) -> None:
        """Durable record of a NEW derived/raw failure in this venue's quality
        stream. Never called for a quality failure (the policy skips it).
        ``_persist_quality_event`` re-raises here, so it is guarded: reporting
        must never abort the failure path."""
        try:
            self._persist_quality_event({
                "exchange": "BYBIT", "stream": "storage_failure", "event_type": QualityEventType.ERROR.value,
                "reason": (f"fatal_storage:{record.verdict}:component={record.component}:stream={record.stream}:"
                           f"stage={record.stage}:durability={record.durability}:origin={record.origin}"),
                "local_ts": record.first_observed_ts})
        except Exception:  # noqa: BLE001 - the structured log line is still the record
            logger.error("bybit_storage_failure_quality_report_failed", stream=record.stream)

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
        self._quality_checkpoint_block_alerted = False   # one operator alert per process, set BEFORE sending
        self._quality_wal_capacity_alerted = False      # one operator alert for the aggregate hard cap
        self._quality_wal_inflight = set()
        self._quality_segment_seqs = set()
        self._quality_published_hwm = None
        # F5 degraded-channel accounting (see _account_degraded_quality_event). An event
        # counts as retained ONLY when its quality-WAL record is established.
        self._quality_events_wal_only = 0          # WAL record established; replayed at next start
        self._quality_events_wal_unconfirmed = 0   # WAL append failed after assigning an id: a record MAY exist
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
        original reason, nor raise into the caller."""
        self._ensure_quality_state()
        first = not self._quality_checkpoint_blocked
        if first:
            self._quality_checkpoint_block_reason = why
        self._quality_checkpoint_blocked = True
        try:
            logger.error("bybit_quality_checkpoint_blocked", reason=why)
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
                f"QUALITY WAL CHECKPOINT BLOCKED [BYBIT]: reason={why}. Checkpointing is blocked for the "
                "rest of this process. Quality events may keep publishing to Parquet while their WAL records "
                "stay uncheckpointed, so the quality WAL can keep growing and the next start will replay "
                "them (duplicate rows with the same quality_event_id are possible). Market data and raw "
                "capture are unaffected. Inspect the quality WAL directory and restart/recover when safe.")
        except Exception as exc:  # noqa: BLE001 - alerting fails open; the latch is already set
            try:
                logger.error("bybit_quality_checkpoint_blocked_alert_failed",
                             error=f"{type(exc).__name__}: {exc}")
            except Exception:  # noqa: BLE001
                pass

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
            self._block_quality_checkpoint("wal_corruption_on_startup")
        else:
            # Recovery parsed every WAL record successfully; now clean only
            # strictly validated files already covered by the durable checkpoint.
            QualityEventWAL.cleanup_fully_checkpointed_files(wal_dir)
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
                # quality_event_id flows through so the replayed row carries
                # the same id as any earlier copy, which lets a downstream
                # consumer identify the duplicate (the collector does not
                # deduplicate; replay is at-least-once).
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

    def _note_quality_wal_capacity_exhausted(self, *, reason, current_bytes=None,
                                             requested_bytes=None, limit_bytes=None) -> None:
        """Report aggregate WAL admission closure once, without re-entering quality persistence."""
        self._ensure_quality_state()
        if getattr(self, "_quality_wal_capacity_alerted", False):
            return
        self._quality_wal_capacity_alerted = True
        try:
            logger.error("bybit_quality_wal_capacity_exhausted", reason=reason,
                         current_bytes=current_bytes, requested_bytes=requested_bytes,
                         limit_bytes=limit_bytes)
        except Exception:  # noqa: BLE001
            pass
        try:
            send_telegram_alert(
                "QUALITY WAL CAPACITY EXHAUSTED [BYBIT]: aggregate WAL admission is closed "
                f"(reason={reason}, limit_bytes={limit_bytes}, current_bytes={current_bytes}). "
                "Uncheckpointed WAL evidence is preserved. New quality events are force-published "
                "to Parquet without a WAL ID; inspect/recover the WAL and restart after correction.")
        except Exception:  # noqa: BLE001
            pass

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
        id the failed append assigned, so a downstream consumer can match any
        WAL-resident copy by id; it is not deduplicated automatically), and
        since no WAL record is known to back it the open
        segment is force-published immediately. A write or publish failure
        latches the checkpoint closed and PROPAGATES -- never swallowed.
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
            except QualityWALCapacityExceeded as exc:
                capacity_fallback = True
                event = {k: v for k, v in event.items()
                         if k not in {"quality_event_id", "_wal_seq"}}
                event["_wal_capacity_exhausted"] = True
                self._note_quality_wal_capacity_exhausted(
                    reason=exc.reason, current_bytes=exc.current_bytes,
                    requested_bytes=exc.requested_bytes, limit_bytes=exc.max_total_bytes)
            except Exception as exc:  # noqa: BLE001 - any append failure, not only OSError
                _policy = getattr(self, "failure_policy", None)
                if _policy is None or _policy.quality_degraded is None:
                    # While the channel is degraded this failure is counted (and logged
                    # at powers of two) by _account_degraded_quality_event instead of
                    # being logged once per event.
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
        # ``failure_policy`` is built AFTER the writers (startup WAL recovery runs
        # through this method before it exists), hence the getattr.
        policy = getattr(self, "failure_policy", None)
        if policy is not None and policy.quality_degraded is not None:
            # F5: the quality channel is latched degraded. The event's WAL record
            # (written above) is its durable copy and the next start replays it.
            # Calling the FAILED writer again could only raise once per event, so
            # it is never called -- and the event is counted as retained ONLY if
            # its WAL protection is real.
            self._account_degraded_quality_event(event, wal_protected=wal_protected, wal_seq=wal_seq)
            return
        try:
            self.quality_writer.write(row, bind=_bind)
        except Exception as exc:
            # Writer state is now uncertain: fail closed, the event stays in
            # the WAL (if it got there), and the failure is NOT swallowed.
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

    def _quality_publish_if_due(self) -> None:
        """Idle tick (parity with run_collector.CollectorApp._quality_publish_if_due).

        ``segment_seconds`` is only evaluated inside ``write()``, so a stream that goes
        quiet would otherwise keep its tail in RAM. A publish failure is never turned
        into success: the checkpoint is latched closed (the buffered events stay in the
        WAL, uncheckpointed, for the next start to replay) and a typed fatal of this
        runner's quality channel degrades it, exactly as a failing write() does.

        Once the channel is latched degraded the writer is terminally FAILED
        (``ParquetWriter._storage_failure`` is never cleared; only a restart resolves it) and
        ``publish_if_due()`` can only raise again. Asking it every tick would re-run the failure
        handling below once per second: ``_block_quality_checkpoint`` logs on every call (only its
        operator alert is one-time). The failure has already been latched, reported and alerted by the
        first tick, and the buffered events are in the WAL, so later ticks return here -- the same
        condition ``_persist_quality_event`` uses to stop calling the failed writer. The guard is
        keyed on ``quality_degraded`` (the writer's terminal failure) and NOT on a blocked checkpoint:
        those are independent (see ``quality_channel_status``), and a healthy writer whose checkpoint
        is blocked must keep publishing."""
        policy = getattr(self, "failure_policy", None)
        if policy is not None and policy.quality_degraded is not None:
            return
        try:
            self.quality_writer.publish_if_due()
        except Exception as exc:  # noqa: BLE001
            self._block_quality_checkpoint(f"idle_publish_failed:{type(exc).__name__}")
            self._note_quality_writer_failure(exc)

    async def _quality_idle_publish_loop(self) -> None:
        """Background tick: call ``_quality_publish_if_due`` every
        ``QUALITY_IDLE_TICK_SECONDS`` while the app runs. Everything the tick does is
        synchronous (no await between reading and publishing), so it cannot interleave
        with ``_persist_quality_event``. A failure never ends the loop; cancellation does."""
        while self.running:
            await asyncio.sleep(QUALITY_IDLE_TICK_SECONDS)
            try:
                self._quality_publish_if_due()
            except Exception as exc:  # noqa: BLE001 - the tick must outlive any single failure
                logger.error("bybit_quality_idle_publish_tick_failed",
                             error=f"{type(exc).__name__}: {exc}")

    def _start_quality_idle_publisher(self) -> None:
        task = getattr(self, "_quality_idle_task", None)
        if task is None or task.done():
            self._quality_idle_task = asyncio.create_task(self._quality_idle_publish_loop())

    def _request_quality_idle_publisher_stop(self) -> None:
        """Retire the idle publisher WITHOUT suspending: request its cancellation and return.

        An earlier revision awaited the task's retirement. On the production path ``client.stop()`` is a
        plain method, so that await was the only suspension point ``shutdown()`` gained, and a
        ``CancelledError`` delivered there skipped every writer close. This request has no suspension point.

        Not waiting for the task to finish is safe, and relies on two facts verified against this code:

        * The idle task and ``shutdown()`` run on the SAME event loop. The task is created by
          ``asyncio.create_task`` in ``run()``; ``shutdown()`` is awaited from the supervisor's main task on
          that loop; and the only thread use in this package is the read-only disk-pressure poll
          (``asyncio.to_thread``), which touches no writer or WAL. Nothing runs the tick body
          concurrently with ``shutdown()``.
        * The tick body (``_quality_publish_if_due``) is synchronous, so the idle task is only ever
          suspended at its ``await asyncio.sleep(...)``, never inside a publication.

        ``Task.cancel()`` therefore guarantees the tick body cannot run again: a task suspended in its
        sleep has ``CancelledError`` thrown in, and a task whose wake-up is already queued has its
        cancellation flagged, so asyncio throws ``CancelledError`` at that ``await`` instead of resuming
        it. No statement after the sleep executes. Everything ``shutdown()`` does afterwards (the writer
        closes and the WAL close) is synchronous, so the loop cannot run the idle task part-way through
        it either. Hence no idle publication can happen after shutdown begins or after any writer closes,
        and there is nothing to await.

        Observability: the task's outcome is retrieved by ``_report_quality_idle_task_result`` -- now if
        it has already finished, otherwise from a done-callback -- so a failure is logged and is never an
        unobserved task exception. Cancellation is the expected outcome and is silent."""
        task = getattr(self, "_quality_idle_task", None)
        self._quality_idle_task = None
        if task is None:
            return
        if task.done():
            self._report_quality_idle_task_result(task)
            return
        task.cancel()
        task.add_done_callback(self._report_quality_idle_task_result)

    def _report_quality_idle_task_result(self, task) -> None:
        """Retrieve the finished idle task's outcome (marking it observed) and log a failure. Never raises:
        it runs inside ``shutdown()`` and as a loop callback."""
        try:
            if task.cancelled():
                return
            exc = task.exception()
            if exc is not None:
                logger.error("bybit_quality_idle_publisher_failed",
                             error=f"{type(exc).__name__}: {exc}")
        except Exception:  # noqa: BLE001 - observing the task must never raise into shutdown
            pass

    def _note_quality_writer_failure(self, exc: BaseException) -> None:
        """Latch the quality channel degraded when ITS writer raised a typed fatal.

        Only a typed fatal that Bybit's OWN failure policy maps to the quality channel
        is latched here. Any other exception -- an ordinary one, or a typed fatal of
        another stream -- is left to propagate to the boundary that classifies it, so
        this path can neither swallow nor re-label it. Never calls ``quality_writer``
        or ``_persist_quality_event``: a failed quality writer cannot recurse into
        itself, and the policy never reports a quality failure through the quality
        writer (``StandaloneFailurePolicy._report``)."""
        policy = getattr(self, "failure_policy", None)
        if policy is not None and policy.quality_degraded is None and policy.is_quality_channel_failure(exc):
            policy.on_fatal(exc, origin="quality_writer")

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

        Only a typed fatal that really degraded the channel qualifies: an ordinary exception, or a
        fatal that was classified otherwise, leaves the counters alone. Never raises, so the
        caller's ``raise`` of the original exception is never replaced."""
        try:
            policy = getattr(self, "failure_policy", None)
            if policy is not None and policy.quality_degraded is not None and policy.is_quality_channel_failure(exc):
                disposition = exc.trigger_event_disposition(record_admitted=record_admitted)
                if disposition == EVENT_PUBLISHED and (
                        wal_seq is None or self._quality_seq_checkpoint_covered(wal_seq)):
                    return
                self._account_degraded_quality_event(
                    event, wal_protected=wal_protected, wal_seq=wal_seq,
                    publication_unproven=(disposition == EVENT_UNCONFIRMED))
        except Exception as account_exc:  # noqa: BLE001 - accounting must not mask the original failure
            logger.error("bybit_quality_event_accounting_failed",
                         error=f"{type(account_exc).__name__}: {account_exc}")

    def _quality_seq_checkpoint_covered(self, seq) -> bool:
        """True when the DURABLE WAL checkpoint already covers ``seq``: that event's segment was
        published and the checkpoint advanced past it, so the next start will NOT replay it."""
        wal = getattr(self, "_quality_wal", None)
        try:
            return wal is not None and seq is not None and wal.checkpointed_seq >= seq
        except Exception:  # noqa: BLE001 - evidence unavailable == not proven covered
            return False

    def _account_degraded_quality_event(self, event: dict, *, wal_protected: bool, wal_seq,
                                        publication_unproven: bool = False) -> None:
        """Count one event that arrived while the quality channel is degraded.

        * ``wal_protected`` -> its WAL record is established: ``_quality_events_wal_only``.
        * ``wal_seq`` only  -> the append raised after assigning an id (bytes written,
          fsync failed): a record MAY exist. Not proven, so NOT "retained"; its seq is
          already in-flight, which pins the checkpoint below it.
        * neither           -> no WAL record and the writer is dead: the event is LOST.
          Counted as unrecorded, never as retained, and never written through the
          failed writer: a bounded log line (first, then powers of two) plus ONE
          operator alert.

        Nothing here advances a checkpoint, calls ``quality_writer`` or raises."""
        if wal_protected:
            self._quality_events_wal_only += 1
            return
        if wal_seq is not None or publication_unproven:
            # ``publication_unproven``: no WAL evidence, but the writer could not establish whether
            # the event's segment is durable (renamed, directory fsync failed). Not lost.
            self._quality_events_wal_unconfirmed += 1
            unconfirmed = self._quality_events_wal_unconfirmed
            if unconfirmed & (unconfirmed - 1) == 0:      # bounded: first, then powers of two
                try:
                    logger.error("bybit_quality_event_wal_unconfirmed", count=unconfirmed,
                                 stream=event.get("stream"), reason=event.get("reason"))
                except Exception:  # noqa: BLE001 - logging must never raise into market data
                    pass
            return
        count = self._quality_events_unrecorded + 1
        self._quality_events_unrecorded = count
        try:
            if not self._quality_checkpoint_is_blocked():
                self._block_quality_checkpoint("quality_event_unrecorded")
            if count & (count - 1) == 0:            # 1, 2, 4, 8, ...: bounded, never one line per event
                logger.error("bybit_quality_event_unrecorded", count=count, stream=event.get("stream"),
                             event_type=str(event.get("event_type")), reason=event.get("reason"),
                             wal_only=self._quality_events_wal_only)
            if not self._quality_unrecorded_alerted:
                self._quality_unrecorded_alerted = True
                send_telegram_alert(
                    "QUALITY EVENTS UNRECORDED [BYBIT]: the quality channel is degraded and the quality WAL "
                    "could not record events (market data and raw capture unaffected; quality telemetry "
                    "is being lost)")
        except Exception as exc:  # noqa: BLE001 - reporting a loss must never raise into market data
            logger.error("bybit_quality_event_unrecorded_report_failed", error=f"{type(exc).__name__}: {exc}")

    def _emit_quality_event(self, event: dict) -> bool:
        """Quality telemetry from a MARKET-DATA path (book transitions, adapter
        unhandled/duplicate messages, writer sinks, websocket events).

        ``_persist_quality_event`` keeps its contract -- it raises when the quality
        writer fails -- because startup replay, the shutdown reports and the
        failure reporter need that to hold the checkpoint back. A market-data task
        needs the opposite: telemetry failing must not take the task down.

        Contained here, and ONLY here: a typed fatal that Bybit's own failure policy
        classifies as a failure of ITS quality channel (``_persist_quality_event``
        has already latched the channel, blocked the checkpoint and reported it once
        through the recursion-safe path; the event itself is in the WAL). Everything
        else propagates unchanged: an ordinary exception (P0-1) and a typed fatal of
        any OTHER stream, which is never quality telemetry. Returns ``True`` when the
        event reached the writer."""
        try:
            self._persist_quality_event(event)
        except FatalStorageError as exc:
            policy = getattr(self, "failure_policy", None)
            if policy is None or not policy.is_quality_channel_failure(exc):
                raise                  # cannot be classified as OUR quality channel: never swallowed
            if policy.quality_degraded is None:
                self._note_quality_writer_failure(exc)
            return False
        return True

    def _on_client_quality_event(self, event_type: str, reason: str,
                                 connection_id=None, stream_group=None) -> None:
        """WebSocketClient calls this UNGUARDED from its connect/disconnect/
        error paths (a raise here would escape into the client's own handler,
        and from its except-branch out of the connection loop). So this
        boundary -- and only this one -- does not re-raise: a persistence
        failure is logged at ERROR and the checkpoint is already latched by
        _persist_quality_event, leaving any WAL copy for restart recovery.
        A quality-channel fatal is contained by ``_emit_quality_event`` (latched and
        reported once there), so this handler logs only for a NEW kind of failure.

        A typed fatal of ANY OTHER stream (``_emit_quality_event`` lets it propagate: it
        is not quality telemetry) is classified here by this runner's own
        ``failure_policy`` BEFORE it is contained, so the default-deny rule holds: a
        raw-evidence or unknown stream latches TERMINATE (client discard mode, supervisor
        wake-up, non-zero exit), a derived stream isolates its own route, and it is never
        latched as a quality-channel failure. It is contained afterwards rather than
        re-raised: this callback is the client's unguarded boundary, and the latch is what
        the supervisor acts on. The policy logs, alerts and reports it once and merely
        counts repeats, so nothing here logs per event or re-enters the quality writer."""
        try:
            self._emit_quality_event({"stream": stream_group or "bybit_websocket",
                                      "event_type": event_type, "reason": reason,
                                      "connection_id": connection_id,
                                      "local_ts": int(time.time() * 1000)})
        except FatalStorageError as exc:
            policy = getattr(self, "failure_policy", None)
            if policy is None:
                raise                  # cannot be classified: never swallowed
            policy.on_fatal(exc, origin="client_quality_event")
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
        prerequisite for this task, not unrelated scope creep.

        F5: this runs INSIDE ``adapter.normalize()``, i.e. on the market-data path,
        and the adapter invokes its sink under ``except Exception: pass``
        (``ExchangeAdapter.unhandled``: "sink must never break ingest"). So:

        * a quality-channel failure goes through ``_emit_quality_event``: latched
          and reported ONCE here, instead of being swallowed by the adapter with the
          dead writer called again for every duplicate / unrouted frame;
        * a typed fatal that is NOT this channel's quality failure would be dropped
          silently by that catch-all, so it is handed to the failure policy BEFORE
          it propagates (classified by its own stream: terminate / isolate) and then
          re-raised unchanged for any direct caller;
        * an ordinary exception keeps the adapter's P0-1 behaviour (ingest goes on)."""
        try:
            self._emit_quality_event(message.to_quality_event())
        except FatalStorageError as exc:
            policy = getattr(self, "failure_policy", None)
            if policy is not None:
                policy.on_fatal(exc, origin="adapter_unhandled")
            raise

    # -- message handling --------------------------------------------------

    #: adapter route -> the F5 routes a message of that route feeds.
    _MESSAGE_ROUTES = {"orderbook": ("orderbook",), "trades": ("trades",),
                       "ticker": ("markprice", "openinterest"), "liquidation": ("liquidation",)}

    def _message_routes(self, data) -> tuple:
        """F5 routes a decoded message feeds; ``()`` when unknown. Never raises."""
        try:
            return self._MESSAGE_ROUTES.get(self.adapter.route_message(data), ()) if isinstance(data, dict) else ()
        except Exception:  # noqa: BLE001 - a routing probe must not become a new failure mode
            return ()

    @staticmethod
    def _event_route(event):
        """The F5 route an event is persisted on (``None``: not persisted)."""
        if isinstance(event, CanonicalOrderBookEvent):
            return "orderbook"
        if isinstance(event, CanonicalTradeEvent):
            return "trades"
        if isinstance(event, CanonicalMarkPriceEvent):
            return "markprice"
        if isinstance(event, CanonicalOIEvent):
            return "openinterest"
        if isinstance(event, CanonicalLiquidationEvent):
            return "liquidation"
        return None

    async def _handle_message(self, data: dict, local_receive_ts: int, connection_id=None) -> None:
        self.messages_handled += 1
        routes = self._message_routes(data)
        if routes and all(self.failure_policy.route_is_isolated(r) for r in routes):
            # F5: every route this message feeds is cut off. Skip BEFORE
            # normalize (which runs trade dedup): a dead route costs a counter,
            # not an error per frame. The raw frame was captured already.
            for route in routes:
                self.failure_policy.note_short_circuit(route)
            return
        # F5: a typed storage fatal is NOT caught here. It propagates to the
        # websocket worker boundary (the P0-4 contract), where the client calls
        # ``on_fatal`` -> ``failure_policy`` -> isolate the route / terminate. This
        # handler only records which route it was serving, so a failure that names
        # a generic stream (``DedupStateError`` -> ``trades``) is still attributed
        # to the right route.
        served_route = "trades"          # normalize() runs trade dedup
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
            # F5: a book-state event is telemetry. If the quality channel is down it
            # is latched/accounted by ``_emit_quality_event`` and the (VALID) order-book
            # row below is still written; it must never stop this route.
            self._emit_quality_event({
                "stream": "bybit_orderbook", "event_type": QualityEventType.SEQUENCE_GAP.value
                if after in (BookQuality.SEQUENCE_GAP, BookQuality.RECOVERING)
                else QualityEventType.RECOVERY.value,
                "reason": self.book.last_reason, "previous_state": before.value,
                "new_state": after.value, "local_ts": int(time.time() * 1000),
            })
        if applied is None:
            return
        if applied.quality_state != BookQuality.VALID.value:
            # The orderbook row has no quality_state column, so a row written
            # from an untrusted book would be indistinguishable from an
            # authoritative one. Persist only VALID state, whatever apply()
            # returned.
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
        self._run_task = asyncio.current_task()
        self._disk_task = asyncio.create_task(self._storage_pressure_loop())
        self._start_quality_idle_publisher()
        await self.client.start()

    async def shutdown(self) -> None:
        self.running = False
        # P0-3: request retirement of the quality idle publisher NOW, before this coroutine can suspend
        # and before any writer is closed, so it can publish nothing once shutdown has begun. It is a
        # plain cancellation request (no await): see ``_request_quality_idle_publisher_stop`` for why
        # that is sufficient on this event loop. The final partial batch is published by the
        # quality_writer close below.
        self._request_quality_idle_publisher_stop()
        # F5-QA-04: stop the (read-only) monitor next; it touches no writer or WAL.
        task = getattr(self, "_disk_task", None)
        if task is not None and not task.done():
            task.cancel()
        # Every step is guarded: none may stop the writer closes below.
        # ``WebSocketClient.stop()`` is a plain method returning ``None``;
        # awaiting it unconditionally raised ``TypeError`` here and skipped
        # every writer close.
        try:
            stopped = self.client.stop()
            if inspect.isawaitable(stopped):
                await stopped
        except Exception as exc:  # noqa: BLE001
            logger.error("bybit_client_stop_failed", error=f"{type(exc).__name__}: {exc}")
        # If shutdown() is cancelled while awaiting an asynchronous ``client.stop()``, the
        # ``CancelledError`` (not an ``Exception``) skips the ``except`` above and propagates: the
        # writers and the WAL stay open and the WAL remains the recovery source. The idle publisher's
        # cancellation was already requested at the top, so it cannot publish after shutdown raised.
        #
        # ``stop()`` only REQUESTS shutdown: the receive loop, the queue drain and the ingest
        # worker's retirement finish inside the client's own start() task (this app's run_task),
        # which the supervisor does not cancel until this coroutine returns. Wait for it here, before
        # closing any writer the worker can reach. The same CancelledError rule applies to this wait.
        client_idle = await await_client_idle(self.client, self.failure_policy)
        # Each writer is closed only when every producer that can reach it is stopped (see
        # ``_finalize_storage``). If a worker is still alive the market-data and quality writers stay
        # open and the WAL remains the recovery source; ``_late_finalizer`` finalizes them when the
        # LAST live user ends -- nothing else would, once this has returned.
        self._finalize_storage(client_idle)
        if not client_idle:
            self._late_finalizer.arm()

    # -- storage finalization ---------------------------------------------------

    def _storage_users_stopped(self) -> bool:
        """True when NOTHING that can still write this venue's storage is alive: the client's whole
        lifecycle (start() tail + ingest worker) has ended. Bybit has no REST producer; the quality
        idle publisher is retired by a cancellation REQUEST at the top of ``shutdown()`` that
        provably cannot run its tick again (see ``_request_quality_idle_publisher_stop``)."""
        return bool(getattr(self.client, "lifecycle_idle", True))

    def _storage_user_tasks(self) -> list:
        """Every task whose end can make a held writer safe to close."""
        tasks = [self._run_task]
        getter = getattr(self.client, "live_lifecycle_tasks", None)
        if getter is not None:
            tasks.extend(getter())
        return tasks

    def _finalize_storage(self, client_idle: Optional[bool] = None) -> None:
        """Close, at most once each, every writer whose users are ALL proven stopped. Idempotent.

        ``client_idle`` is the verdict ``shutdown()`` just obtained by waiting for the client; the late
        callback passes None, which reads the client's live state instead.

        Reachability, judged from LIVE state on every call:
          raw_wire_writer: the receive loop only           -> ingress stopped (whatever the worker does).
          ob/trades/mark/oi/liq writers, quality_writer, quality WAL: the ingest worker (on_message)
                                                            -> client idle.
        quality_writer closes LAST among the writers: its close PUBLISHES the final partial segment and
        thereby checkpoints that segment's WAL seqs. If that publish fails its events stay in the WAL,
        uncheckpointed, to be replayed by the next startup. Only then is the WAL closed (the publish
        hook needs it open)."""
        self._close_raw_wire_writer_if_ingress_stopped()
        if client_idle is None:
            client_idle = bool(getattr(self.client, "lifecycle_idle", True))
        if not client_idle:
            # A worker is still alive: market-data and quality writers stay open and the WAL
            # remains the recovery source; the failure is latched and the exit status non-zero.
            return
        if self._close_once.attempted("quality_writer"):
            return          # a full finalization already ran (checkpoint latch and WAL close included)
        self._close_writer_reporting_failure(self.ob_writer, "ob_writer", "BYBIT")
        self._close_writer_reporting_failure(self.trades_writer, "trades_writer", "BYBIT")
        self._close_writer_reporting_failure(self.mark_writer, "mark_writer", "BYBIT")
        self._close_writer_reporting_failure(self.oi_writer, "oi_writer", "BYBIT")
        self._close_writer_reporting_failure(self.liq_writer, "liq_writer", "BYBIT")
        quality_closed = self._close_writer_reporting_failure(self.quality_writer, "quality_writer", "BYBIT")
        if not quality_closed:
            self._block_quality_checkpoint("quality_writer_close_failed")
        wal = getattr(self, "_quality_wal", None)
        if wal is not None:
            def _close_wal() -> bool:
                try:
                    wal.close()
                    return True
                except Exception as exc:  # noqa: BLE001 - must not abort the rest of shutdown
                    logger.error("bybit_quality_wal_close_failed", error=str(exc))
                    return False
            self._close_once.run("quality_wal", _close_wal)

    def _close_raw_wire_writer_if_ingress_stopped(self) -> None:
        if getattr(self.client, "ingress_stopped", True):
            self._close_writer_reporting_failure(self.raw_wire_writer, "raw_wire_writer", "BYBIT")
        else:
            logger.error("bybit_raw_wire_writer_left_open_ingress_not_stopped")

    def _close_writer_reporting_failure(self, writer, writer_name: str, exchange: str) -> bool:
        """Close ``writer`` at most once per process (``CloseOnce``); a repeat request returns the
        first attempt's outcome without calling ``close()`` again."""
        return self._close_once.run(
            writer_name, lambda: self._close_writer_attempt(writer, writer_name, exchange))

    def _close_writer_attempt(self, writer, writer_name: str, exchange: str) -> bool:
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
        was_failed = writer_already_failed(writer)
        try:
            writer.close()
        except Exception as exc:  # noqa: BLE001 - must not abort closing the rest
            closed_ok = False
            logger.error("storage_shutdown_close_failed", writer=writer_name, error=str(exc))
            if not was_failed:      # a healthy writer failed to publish its final segment: never exit 0
                self.failure_policy.record_shutdown_failure(f"writer_close_failed:{writer_name}")
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


async def _main(data_dir: str, url: str) -> int:
    """Run the collector; return the process exit status (0 for a signalled
    stop, ``EXIT_FATAL_STORAGE`` for a terminal raw-evidence failure). The
    application task is supervised, never left as an unobserved background task."""
    app = BybitCollectorApp(data_dir=data_dir, url=url)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass  # not available on this platform
    return await _run_supervised(app, stop)


async def _run_supervised(app: "BybitCollectorApp", stop: asyncio.Event) -> int:
    """Startup disk preflight, then the standard supervised run (F5-QA-04).

    A confirmed disk-pressure EMERGENCY (at startup or at runtime) ends in the supervisor's normal
    controlled shutdown and exit status ``EXIT_DISK_PRESSURE`` (1); storage that cannot be measured
    for the whole grace period ends the same way with ``EXIT_DISK_MONITOR_UNAVAILABLE`` (69). Status
    70 (a terminal raw-evidence storage failure) and an ordinary task crash keep their own statuses.
    Nothing restarts Bybit: there is no systemd unit for this runner, so after an emergency
    stop it stays stopped unless an external supervisor is configured."""
    app.stop_event = stop
    if await app.storage_pressure_preflight_failed():
        try:
            await app.shutdown()
        except Exception as exc:  # noqa: BLE001 - controlled and logged, never masks the exit status
            logger.error("bybit_startup_disk_shutdown_failed", error=f"{type(exc).__name__}: {exc}")
        return app.exit_code               # 70 > 1 (disk emergency) > 69 (monitor unavailable)
    status = await supervise_standalone_runner(app, stop)
    if status == 0:
        return app.exit_code               # 1 / 69 when a disk stop was requested, else 0
    return status


def _entry(data_dir: str, url: str) -> int:
    """The process entry: ``_main`` under a BOUNDED event-loop teardown (``loop_teardown``).

    ``asyncio.run`` would wait without limit for a task that swallowed its cancellation, ignoring SIGTERM and
    SIGINT meanwhile and losing the latched exit status to a SIGKILL. Never returns 0 with a task left alive."""
    status, stuck = run_bounded(_main(data_dir, url))
    return teardown_exit_status(status, stuck)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--url", default=BYBIT_PUBLIC_WS_URL,
                        help="Override for testnet: wss://stream-testnet.bybit.com/v5/public/linear")
    args = parser.parse_args()
    logger.info("bybit_collector_starting", url=args.url, data_dir=args.data_dir,
               note="requires outbound access to stream.bybit.com:443; "
                    "not available in every environment")
    raise SystemExit(_entry(args.data_dir, args.url))
