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
from collector.collector.standalone_failure_policy import StandaloneFailurePolicy, build_stream_table
from collector.collector.storage_errors import FatalStorageError
from collector.collector.storage_layout import venue_stream
from collector.collector.utils import logger


#: Seconds a terminal capture waits for the client to drain and stop before the
#: task is cancelled. Bounded so a quiet connection cannot hold a terminal exit.
TERMINAL_STOP_GRACE_S = 10.0


class OKXCaptureApp:
    def __init__(self, channels, inst_id: str, data_dir: str, url: str) -> None:
        # Quality events NOT persisted because the quality channel is degraded:
        # the event that tripped the latch plus every later one, which is
        # short-circuited without calling the failed writer. A counter, never a
        # log line per event. Set before any writer exists: a writer may emit
        # through ``_persist_quality_event`` while it is being constructed.
        self.quality_events_lost = 0
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
        self.capture = OKXPublicCapture(
            channels, inst_id=inst_id, raw_capture=self.raw_capture,
            quality_sink=self._persist_quality_event, url=url,
            on_fatal=self._on_fatal_storage)

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
        degraded channel. This runner keeps NO durable quality WAL, so every
        event counted in ``quality_events_lost`` is gone, not retained."""
        record = self.failure_policy.quality_degraded
        return {"quality_degraded": record is not None,
                "quality_events_lost": self.quality_events_lost,
                "quality_failure": None if record is None else record.as_dict()}

    def _persist_quality_event(self, event: dict) -> None:
        """Single choke point for every OKX capture quality event.

        F5: a typed ``FatalStorageError`` from THIS runner's quality writer means
        that writer is FAILED. It is latched exactly once through
        ``failure_policy`` (degrade the quality channel; origin
        ``quality_writer``) and contained -- raw capture keeps running. After the
        latch the failed writer is never called again and later events only bump
        ``quality_events_lost``: no error log and no alert per event. A typed
        fatal of any OTHER stream is re-raised (never swallowed by the quality
        handler); an ordinary exception keeps the log-and-continue path."""
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
            if policy is None or not policy.is_quality_channel_failure(exc):
                raise
            policy.on_fatal(exc, origin="quality_writer")
            self.quality_events_lost += 1
        except Exception as exc:  # noqa: BLE001 - never break capture
            logger.error("okx_quality_write_failed", error=str(exc))

    async def run(self, duration_s: float | None) -> dict:
        task = asyncio.ensure_future(self.capture.run())
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
        finally:
            for helper in helpers:
                helper.cancel()
            await asyncio.gather(*helpers, return_exceptions=True)
            self.close()
        return {**self.capture.status(), **self.quality_channel_status()}

    def close(self) -> None:
        for writer in (self.raw_wire_writer, self.quality_writer):
            try:
                writer.close()
            except Exception as exc:  # noqa: BLE001
                logger.error("okx_writer_close_failed", error=str(exc))


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
    status = asyncio.run(app.run(duration))
    if app.failure_policy.terminal_failure is not None:
        status = {**status, "terminal_failure": app.failure_policy.terminal_failure.as_dict()}
    print(json.dumps(status, indent=2, default=str))
    return app.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
