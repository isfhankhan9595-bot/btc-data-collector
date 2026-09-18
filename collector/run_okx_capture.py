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
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import QualityEventType
from collector.collector.raw_capture import RawCapture
from collector.collector.utils import logger


class OKXCaptureApp:
    def __init__(self, channels, inst_id: str, data_dir: str, url: str) -> None:
        self.quality_writer = ParquetWriter(
            "quality_events", QUALITY_EVENTS_SCHEMA, base_dir=data_dir,
            segment_rows=1, segment_seconds=1)
        self.raw_wire_writer = ParquetWriter(
            "raw_wire", RAW_WIRE_SCHEMA, base_dir=data_dir,
            quality_event_sink=self._persist_quality_event)
        self.raw_capture = RawCapture(
            self.raw_wire_writer, None,
            quality_event_sink=self._persist_quality_event)
        self.capture = OKXPublicCapture(
            channels, inst_id=inst_id, raw_capture=self.raw_capture,
            quality_sink=self._persist_quality_event, url=url)

    def _persist_quality_event(self, event: dict) -> None:
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
        try:
            if duration_s is not None:
                await asyncio.sleep(duration_s)
                self.capture.stop()
            await task
        except asyncio.CancelledError:
            self.capture.stop()
        finally:
            self.close()
        return self.capture.status()

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
    print(json.dumps(status, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
