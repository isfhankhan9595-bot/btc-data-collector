"""Standalone OKX v5 public raw-frame capture.

Runs the smallest thing that unblocks D11: connect to the documented public
endpoint, subscribe to every channel the adapter *declares* (including the
six it does not implement -- those are precisely the ones whose schemas need
observing), and persist the raw frames with lineage.

No derived stream is written. The six channels' field names are unverified,
so nothing here interprets ``data``; the follow-up step is
``collector/scripts/okx_schema_report.py``, which reads the captured frames
and reports the field structure actually observed on the wire.

Usage
-----
    python -m collector.run_okx_capture --duration 600 --data-dir data

Requires outbound access to ``ws.okx.com:8443``. It is not available in every
environment; the process reports a connection failure as a durable quality
event rather than exiting silently.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import signal
import time
from typing import Optional

from collector.collector.adapters.okx import OKXAdapter
from collector.collector.config import QUALITY_EVENTS_SCHEMA, RAW_WIRE_SCHEMA
from collector.collector.okx_capture import (
    OKX_BTC_SWAP_INST_ID,
    OKX_PUBLIC_WS_URL,
    OKXPublicCapture,
)
from collector.collector.failure_topology import FailureRecord
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import QualityEventType
from collector.collector.raw_capture import RawCapture
from collector.collector.loop_teardown import run_bounded, teardown_exit_status
from collector.collector.standalone_failure_policy import (
    CloseOnce, DegradedQualityLossReporting, LateFinalizer, StandaloneFailurePolicy, await_client_idle,
    build_stream_table)
from collector.collector.storage_errors import EVENT_LOST, EVENT_UNCONFIRMED, FatalStorageError
from collector.collector.storage_layout import venue_stream
from collector.collector.utils import logger
from collector.collector.websocket_client import WorkerRetirementError


#: Seconds a terminal capture waits for the client to drain and stop before the
#: task is cancelled. Bounded so a quiet connection cannot hold a terminal exit.
TERMINAL_STOP_GRACE_S = 10.0


class OKXCaptureApp(DegradedQualityLossReporting):
    quality_log_prefix = "okx_capture"

    def __init__(self, channels, inst_id: str, data_dir: str, url: str) -> None:
        # Quality events NOT persisted because the quality channel is degraded:
        # every later event (short-circuited without calling the failed writer)
        # plus the event that tripped the latch IF it is demonstrably not
        # durable. The tripping event is judged from the writer's evidence
        # (``_account_trigger_event``): published -> counted nowhere,
        # unconfirmed -> ``quality_events_unconfirmed``, lost -> here. A counter
        # reported at a bounded cadence (first, then powers of two), never a log line
        # per event. Set before any writer exists: a writer may
        # emit through ``_persist_quality_event`` while it is being constructed.
        self.quality_events_lost = 0
        # The tripping event whose durability could not be established either way
        # (renamed into place, directory fsync unconfirmed). Neither lost nor
        # retained: it is reported as unconfirmed, never as a definite outcome.
        self.quality_events_unconfirmed = 0
        # A typed fatal from THIS runner's own quality writer raised BEFORE
        # ``failure_policy`` exists (a raw writer's orphan recovery emits a quality
        # event while it is being constructed). There is no table to classify with
        # yet, so it is only HELD here and latched exactly once, through the
        # runner's own policy, the moment that policy is built. Never a fabricated
        # record and never a retried write: the dead writer is not called again.
        self._pre_policy_quality_failure = None
        # Own stream directories, not Binance's ``raw_wire``/``quality_events``:
        # segment sequence numbers and .tmp files are scoped to a stream
        # directory, so sharing one with the Binance runner shares both. The
        # writers also declare exchange="OKX" so their own storage faults are
        # not attributed to Binance (the ParquetWriter default).
        self.quality_writer = ParquetWriter(
            venue_stream("OKX", "quality_events"), QUALITY_EVENTS_SCHEMA,
            base_dir=data_dir, exchange="OKX", segment_rows=1, segment_seconds=1)
        self.raw_wire_writer = ParquetWriter(
            venue_stream("OKX", "raw_wire"), RAW_WIRE_SCHEMA, base_dir=data_dir,
            exchange="OKX", quality_event_sink=self._persist_quality_event)
        # F5 raw-evidence contract. This process writes NOTHING but raw frames,
        # so a raw-writer fatal means it is capturing nothing: it must not carry
        # on as a healthy-looking process (the fail-open default did exactly
        # that). The typed fatal propagates to the shared client's raw-frame
        # boundary, is classified TERMINATE and ends in a controlled non-zero exit.
        self.raw_capture = RawCapture(
            self.raw_wire_writer, None,
            quality_event_sink=self._persist_quality_event,
            fail_closed_on_fatal_storage=True)
        self.failure_policy = StandaloneFailurePolicy(
            venue="OKX_CAPTURE",
            streams=build_stream_table(
                raw=(self.raw_wire_writer,), derived={}, quality=(self.quality_writer,)),
            clients=lambda: (self.capture.client,),
            report=self._report_storage_failure)
        self._latch_pre_policy_quality_failure()
        self.capture = OKXPublicCapture(
            channels, inst_id=inst_id, raw_capture=self.raw_capture,
            quality_sink=self._persist_quality_event, url=url,
            on_fatal=self._on_fatal_storage)
        # Shutdown finalization (``_shutdown_storage``): a writer's close is ATTEMPTED at most once per
        # process, and storage a still-live task can reach is finalized by the end of the LAST such task.
        self._capture_task: Optional[asyncio.Future] = None
        self._close_once = CloseOnce()
        self._late_finalizer = LateFinalizer(
            venue="OKX_CAPTURE", finalize=self._finalize_storage,
            users_stopped=self._storage_users_stopped, watched=self._storage_user_tasks)

    # -- F5 failure topology --------------------------------------------------

    @property
    def exit_code(self) -> int:
        return self.failure_policy.exit_code

    def _on_fatal_storage(self, exc, origin: str = "handler", *, route=None) -> str:
        """Classify and latch one typed storage fatal (see ``failure_policy``).
        Safe to call from the websocket worker: it only latches."""
        return self.failure_policy.on_fatal(exc, origin, route=route)

    def _report_storage_failure(self, record: FailureRecord) -> None:
        """Durable record of a NEW raw failure in this venue's quality stream.
        Never called for a quality failure (the policy skips it).
        ``_persist_quality_event`` contains a quality-channel fatal itself; the
        policy guards this reporter against anything else it raises."""
        self._persist_quality_event({
            "exchange": "OKX", "stream": "storage_failure", "event_type": QualityEventType.ERROR.value,
            "reason": (f"fatal_storage:{record.verdict}:component={record.component}:stream={record.stream}:"
                       f"stage={record.stage}:durability={record.durability}:origin={record.origin}"),
            "local_ts": record.first_observed_ts})

    @property
    def quality_degraded(self):
        """The latched quality-channel ``FailureRecord`` (or ``None`` while healthy)."""
        return self.failure_policy.quality_degraded

    def quality_channel_status(self) -> dict:
        """Operator view of the quality channel. Raw capture is unaffected by a
        degraded channel. ``quality_events_lost`` counts events not durably
        published through the supported quality-storage path;
        ``quality_events_unconfirmed`` counts the tripping event whose
        durability could not be established (neither lost nor retained).
        ``quality_wal`` is always ``False``: this runner provides no supported
        quality WAL for replay or recovery, and an unpublished ``.tmp`` file that
        may remain is not guaranteed to be complete, recoverable or authoritative."""
        record = self.failure_policy.quality_degraded
        return {"quality_degraded": record is not None,
                "quality_events_lost": self.quality_events_lost,
                "quality_events_unconfirmed": self.quality_events_unconfirmed,
                "quality_wal": False,
                "quality_failure": None if record is None else record.as_dict()}

    def _account_trigger_event(self, exc: FatalStorageError, event: dict | None = None) -> None:
        """Count the ONE event whose ``quality_writer.write`` raised ``exc``, from
        the writer's own evidence and never from the fact that it raised. That
        event may already be durable (a full segment published, then opening the
        next one failed), may be of unconfirmed durability (renamed, directory
        fsync failed), or may be demonstrably not durable (rename failed, flush
        failed, or the call failed before the event was admitted). Only the last
        is ``quality_events_lost``; a published event is counted nowhere; an
        unconfirmed one is never given a definite retained/lost outcome."""
        disposition = exc.trigger_event_disposition()
        if disposition == EVENT_LOST:
            self._count_quality_event_lost(event)
        elif disposition == EVENT_UNCONFIRMED:
            self._count_quality_event_unconfirmed(event)

    def _latch_pre_policy_quality_failure(self) -> None:
        """Latch, once, a quality-writer failure held while the policy did not
        exist. Goes through the real ``on_fatal`` (classified by this runner's own
        table, one alert, never re-reported); it does not touch the failed writer.
        The counters already account for the event that tripped it."""
        pending, self._pre_policy_quality_failure = self._pre_policy_quality_failure, None
        if pending is not None:
            self.failure_policy.on_fatal(pending, origin="quality_writer")

    def _persist_quality_event(self, event: dict) -> None:
        """Single choke point for every OKX capture quality event.

        F5: a typed ``FatalStorageError`` from THIS runner's quality writer means
        that writer is FAILED. It is latched exactly once through
        ``failure_policy`` (degrade the quality channel; origin
        ``quality_writer``) and contained -- raw capture keeps running. After the
        latch the failed writer is never called again and later events only bump
        ``quality_events_lost``: no alert per event, and a log line only at a bounded
        cadence (first loss, then powers of two; see ``DegradedQualityLossReporting``). A typed
        fatal of any OTHER stream is re-raised (never swallowed by the quality
        handler); an ordinary exception keeps the log-and-continue path."""
        policy = getattr(self, "failure_policy", None)
        if (policy is not None and policy.quality_degraded is not None) \
                or self._pre_policy_quality_failure is not None:
            self._count_quality_event_lost(event)
            return
        event_type = event.get("event_type", QualityEventType.ERROR.value)
        if isinstance(event_type, QualityEventType):
            event_type = event_type.value
        rows_lost = event.get("rows_lost")
        local_ts = event.get("local_ts", int(time.time() * 1000))
        try:
            self.quality_writer.write({
                "timestamp": local_ts,
                "exchange": event.get("exchange", "OKX"),
                "stream": event.get("stream", "okx_public"),
                "event_type": event_type,
                "reason": event.get("reason", ""),
                "gap_size_ms": event.get("gap_size_ms"),
                "rows_lost": None if rows_lost is None else str(rows_lost),
                "connection_id": event.get("connection_id"),
                "local_receive_ts": event.get("local_receive_ts"),
                "local_ts": local_ts,
            })
        except FatalStorageError as exc:
            if policy is None:
                # Startup, before the policy exists. Only a typed fatal that names
                # THIS runner's own quality stream is held for the policy; every
                # other typed fatal (raw evidence, derived, another venue's
                # stream, unknown) propagates exactly as it did before.
                if exc.stream != self.quality_writer.stream_name:
                    raise
                self._pre_policy_quality_failure = exc
                self._account_trigger_event(exc, event)
                return
            if not policy.is_quality_channel_failure(exc):
                raise
            policy.on_fatal(exc, origin="quality_writer")
            self._account_trigger_event(exc, event)
        except Exception as exc:  # noqa: BLE001 - never break capture
            logger.error("okx_quality_write_failed", error=str(exc))

    async def run(self, duration_s: float | None) -> dict:
        task = asyncio.ensure_future(self.capture.run())
        self._capture_task = task
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.capture.stop)
            except (NotImplementedError, RuntimeError):
                pass
        # The capture ends for one of three reasons: the duration elapsed, the
        # task ended (signal / crash), or a terminal storage failure latched.
        # Waiting on all three is what stops a raw-writer failure from being
        # carried out silently to the end of ``--duration`` / ``--forever``.
        terminal_wait = asyncio.ensure_future(self.failure_policy.terminal_event.wait())
        sleeper = asyncio.ensure_future(asyncio.sleep(duration_s)) if duration_s is not None else None
        helpers = [terminal_wait] + ([sleeper] if sleeper is not None else [])
        try:
            await asyncio.wait({task, *helpers}, return_when=asyncio.FIRST_COMPLETED)
            self.capture.stop()
            if self.failure_policy.terminal_failure is not None:
                # Controlled terminal stop, from here (never from the worker).
                # Bounded: a quiet connection must not hold the exit.
                done, _ = await asyncio.wait({task}, timeout=TERMINAL_STOP_GRACE_S)
                if not done:
                    task.cancel()
                    await asyncio.wait({task}, timeout=TERMINAL_STOP_GRACE_S)
                if task.done() and not task.cancelled():
                    task.exception()            # observe it: never an unretrieved task
            else:
                await task
        except asyncio.CancelledError:
            self.capture.stop()
        except WorkerRetirementError:
            # start()'s own tail reports an ingest worker that survived its bounded retirement. Not swallowed:
            # it is latched as a shutdown failure (non-zero exit, named in the status) and the writers that
            # worker can reach stay open until it ends (``_shutdown_storage``).
            self.failure_policy.record_shutdown_failure("ws_client_worker_not_retired")
        finally:
            for helper in helpers:
                helper.cancel()
            await asyncio.gather(*helpers, return_exceptions=True)
            await self._shutdown_storage(task)
        return {**self.capture.status(), **self.quality_channel_status()}

    # -- storage finalization ---------------------------------------------------

    async def _shutdown_storage(self, task) -> None:
        """Finalize storage, from ``run()``'s ``finally``, in the only safe order.

        ``stop()`` and a task cancel only REQUEST the end of the capture task: the queue drain and the ingest
        worker's retirement run in the client's own ``start()`` tail, and a worker that swallowed its cancel
        outlives even that. A bounded wait that timed out is not proof that anything stopped, so a close is
        never gated on how long ``run()`` waited, only on the client's LIVE state (``_finalize_storage``).
        Whatever a live task can still reach stays open and is finalized when the last such task ends."""
        client_idle = False
        try:
            self.capture.stop()         # idempotent: no exit path may close storage with ingress still open
            client_idle = await await_client_idle(self.capture.client, self.failure_policy)
        finally:
            self._observe_capture_task(task)
            if not client_idle and self.failure_policy.shutdown_failure is None:
                # The wait itself was interrupted (a second cancel): the ordering could not be completed.
                self.failure_policy.record_shutdown_failure("ws_client_shutdown_interrupted")
            self._finalize_storage(client_idle)
            if not client_idle:
                self._late_finalizer.arm()

    def _observe_capture_task(self, task) -> None:
        """Retrieve the capture task's outcome so it is never an unobserved background failure. A worker that
        outlived its retirement is a shutdown failure whichever path noticed it first."""
        if task is None or not task.done() or task.cancelled():
            return
        exc = task.exception()
        if isinstance(exc, WorkerRetirementError):
            self.failure_policy.record_shutdown_failure("ws_client_worker_not_retired")
        elif exc is not None:
            logger.error("okx_capture_task_failed", error=f"{type(exc).__name__}: {exc}")

    def _storage_users_stopped(self) -> bool:
        """True when nothing of the client can still touch storage: start() is not mid-lifecycle and no ingest
        worker is alive. (This runner has no REST producer.)"""
        return bool(getattr(self.capture.client, "lifecycle_idle", True))

    def _storage_user_tasks(self) -> list:
        """Every task whose end can make a held writer safe to close."""
        tasks = [self._capture_task]
        getter = getattr(self.capture.client, "live_lifecycle_tasks", None)
        if getter is not None:
            tasks.extend(getter())
        return tasks

    def close(self) -> None:
        """Close, at most once each, every writer whose users have ALL stopped (see ``_finalize_storage``).
        Idempotent; what a live task can still reach is left open and finalized when that task ends."""
        self._finalize_storage()

    def _finalize_storage(self, client_idle: Optional[bool] = None) -> None:
        """``client_idle`` is the verdict ``_shutdown_storage`` just obtained by waiting for the client; the
        late finalizer passes None, which reads the client's live state instead.

        Reachability, from the source (and judged from LIVE state on every call):
          raw_wire_writer: the receive loop only (``_on_raw_frame``)  -> ingress stopped, whatever the worker does.
          quality_writer:  start() tail / keepalive / ingest worker (``on_quality_event``, ``_on_message``)
                                                                      -> client idle.
        Order: raw first, so a raw close failure can still be reported through the quality writer, which
        closes last and is followed by the final quality-channel report."""
        client = self.capture.client
        if getattr(client, "ingress_stopped", True) or getattr(client, "lifecycle_idle", True):
            self._close_writer(self.raw_wire_writer, "raw_wire_writer")
        else:
            logger.error("okx_capture_raw_wire_writer_left_open_ingress_not_stopped")
        if client_idle is None:
            client_idle = bool(getattr(client, "lifecycle_idle", True))
        if not client_idle:
            return                      # a live task can still reach the quality writer: it stays open
        if self._close_once.attempted("quality_writer"):
            return                      # a full finalization already ran
        self._close_writer(self.quality_writer, "quality_writer")
        # Last, so the counts include anything the closes above reported. Cannot raise.
        self._report_quality_channel_final()

    def _close_writer(self, writer, name: str) -> bool:
        """Close ``writer`` at most once per process (``CloseOnce``); a repeat request returns the first
        attempt's outcome without calling ``close()`` again."""
        return self._close_once.run(name, lambda: self._close_writer_attempt(writer, name))

    def _close_writer_attempt(self, writer, name: str) -> bool:
        """One close attempt. A failure is never just a log line: a typed fatal goes through this runner's own
        classification (raw writer -> terminal verdict, non-zero ``EXIT_FATAL_STORAGE``; quality writer ->
        degraded channel), anything else on the raw writer latches a shutdown failure (non-zero exit) and is
        reported durably while the quality writer is still open."""
        try:
            writer.close()
            return True
        except Exception as exc:  # noqa: BLE001 - must not abort closing the rest
            logger.error("okx_writer_close_failed", writer=name, error=f"{type(exc).__name__}: {exc}")
            if isinstance(exc, FatalStorageError):
                # A quality writer that already degraded the channel re-raises its LATCHED failure here: that is
                # not a new observation, so it is not fed back to the policy (no repeat count, no re-report),
                # exactly as ``_persist_quality_event`` never calls a degraded writer again. A first failure
                # at close is still classified and latched.
                if name != "quality_writer" or self.failure_policy.quality_degraded is None:
                    self.failure_policy.on_fatal(exc, origin="shutdown_close")
            elif name != "quality_writer":
                self.failure_policy.record_shutdown_failure(f"writer_close_failed:{name}")
                try:
                    self._persist_quality_event({
                        "exchange": "OKX", "stream": getattr(writer, "stream_name", name),
                        "event_type": QualityEventType.ERROR.value,
                        "reason": f"storage_shutdown_close_failed:{name}:{type(exc).__name__}"})
                except Exception as sink_exc:  # noqa: BLE001 - reporting must not itself abort shutdown
                    logger.error("okx_shutdown_close_failure_report_failed", writer=name, error=str(sink_exc))
            return False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Capture OKX v5 public raw frames")
    parser.add_argument("--duration", type=float, default=300.0,
                        help="seconds to capture; omit with --forever")
    parser.add_argument("--forever", action="store_true",
                        help="run until SIGINT/SIGTERM")
    parser.add_argument("--inst-id", default=OKX_BTC_SWAP_INST_ID)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--url", default=OKX_PUBLIC_WS_URL)
    parser.add_argument(
        "--channels", default=None,
        help="comma-separated channel list; defaults to every channel the "
             "OKX adapter declares, including the unimplemented ones, since "
             "those are the schemas that need observing")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.channels:
        channels = [c.strip() for c in args.channels.split(",") if c.strip()]
    else:
        channels = sorted(OKXAdapter().declared_channels())
    app = OKXCaptureApp(channels, args.inst_id, args.data_dir, args.url)
    duration = None if args.forever else args.duration
    status, stuck = run_bounded(app.run(duration))
    if app.failure_policy.terminal_failure is not None:
        status = {**status, "terminal_failure": app.failure_policy.terminal_failure.as_dict()}
    if app.failure_policy.shutdown_failure is not None:
        status = {**status, "shutdown_failure": app.failure_policy.shutdown_failure}
    print(json.dumps(status, indent=2, default=str))
    return teardown_exit_status(app.exit_code, stuck)


if __name__ == "__main__":
    raise SystemExit(main())
