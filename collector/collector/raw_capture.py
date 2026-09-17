"""Lossless raw wire capture.

Deterministic replay is only possible if enough original information was
persisted to reproduce ingestion. Before this module the collector stored
*derived* data: ``binance_orderbook_raw`` holds normalised levels produced
*after* reconstruction, REST snapshot payloads were discarded once applied,
and the websocket frame text was thrown away immediately after
``json.loads``. Replaying from that storage would require contacting the
live exchange for the missing history, which is not replay.

This layer records the wire itself.

Two record kinds
----------------

``RawWireRecord``
    One websocket frame, exactly as received, before any lossy transform.
    Carries the frame text verbatim plus the lineage needed to re-drive the
    pipeline: connection id, venue, channel, symbol, market type, and the
    receive timestamp taken *before* decoding.

``RawRestRecord``
    One REST request/response exchange: endpoint, request parameters,
    request timestamp, response receive timestamp, HTTP status and the
    response body verbatim. Recording the body is what allows replay to use
    recorded snapshots instead of live ones.

Design rules
------------

* **Capture precedes parsing.** A frame that fails to decode is still a
  frame that arrived; it is captured with ``decode_ok=False`` so malformed
  payloads remain observable rather than vanishing into a log line.
* **Never fabricate.** Fields the wire did not carry stay ``None``. The
  capture layer does not parse, enrich, or repair.
* **Capture must not be able to stop ingestion.** A raw-capture failure is
  surfaced as a durable quality event and the frame still flows. Losing
  research fidelity is bad; losing the live feed is worse.
* **Bounded.** Payloads above ``max_payload_bytes`` are stored truncated
  with ``truncated=True`` and the original byte length recorded, so an
  abnormally large frame cannot exhaust memory or disk silently.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional

import pyarrow as pa

__all__ = [
    "RawWireRecord",
    "RawRestRecord",
    "RAW_WIRE_SCHEMA",
    "RAW_REST_SCHEMA",
    "RawCapture",
    "DEFAULT_MAX_PAYLOAD_BYTES",
]

#: Frames larger than this are stored truncated. Binance depth1000 snapshots
#: run ~100KB; 4MB leaves generous headroom while bounding a pathological frame.
DEFAULT_MAX_PAYLOAD_BYTES = 4 * 1024 * 1024


RAW_WIRE_SCHEMA = pa.schema(
    [
        # Canonical ordering timestamp for the segment writer.
        ("timestamp", pa.timestamp("ms", tz="UTC")),
        # Lineage: captured before decoding wherever technically possible.
        ("local_receive_ts", pa.timestamp("ms", tz="UTC")),
        ("local_capture_ts", pa.timestamp("ms", tz="UTC")),
        ("venue", pa.string()),
        ("market_type", pa.string()),
        ("symbol", pa.string()),
        ("channel", pa.string()),
        ("stream", pa.string()),
        ("connection_id", pa.string()),
        ("connection_generation", pa.int64()),
        # The wire itself.
        ("payload", pa.string()),
        ("payload_bytes", pa.int64()),
        ("truncated", pa.bool_()),
        ("decode_ok", pa.bool_()),
        ("decode_error", pa.string()),
        # Venue-native identifiers, copied verbatim when present. Never derived.
        ("exchange_event_ts", pa.timestamp("ms", tz="UTC")),
        ("update_id", pa.int64()),
        ("first_update_id", pa.int64()),
        ("previous_update_id", pa.int64()),
    ],
    metadata={
        "schema_version": "1.0",
        "stream_name": "raw_wire",
        "contract": "payload is the exact frame text; capture precedes parsing",
    },
)


RAW_REST_SCHEMA = pa.schema(
    [
        ("timestamp", pa.timestamp("ms", tz="UTC")),
        ("request_ts", pa.timestamp("ms", tz="UTC")),
        ("response_receive_ts", pa.timestamp("ms", tz="UTC")),
        ("local_process_ts", pa.timestamp("ms", tz="UTC")),
        ("venue", pa.string()),
        ("market_type", pa.string()),
        ("symbol", pa.string()),
        ("purpose", pa.string()),
        ("method", pa.string()),
        ("endpoint", pa.string()),
        ("request_params", pa.string()),
        ("http_status", pa.int64()),
        ("ok", pa.bool_()),
        ("error", pa.string()),
        ("payload", pa.string()),
        ("payload_bytes", pa.int64()),
        ("truncated", pa.bool_()),
    ],
    metadata={
        "schema_version": "1.0",
        "stream_name": "raw_rest",
        "contract": "payload is the exact response body; replay must use this, never the live exchange",
    },
)


def _truncate(text: str, limit: int) -> tuple[str, int, bool]:
    """Return ``(stored_text, original_bytes, truncated)``.

    Byte length is measured on the original so an abnormally large frame is
    visible in the data even when its body was clipped.
    """
    encoded = text.encode("utf-8", errors="replace")
    original = len(encoded)
    if original <= limit:
        return text, original, False
    return encoded[:limit].decode("utf-8", errors="replace"), original, True


@dataclass(frozen=True)
class RawWireRecord:
    """One websocket frame plus its lineage."""

    local_receive_ts: int
    payload: str
    venue: str
    connection_id: Optional[str] = None
    connection_generation: Optional[int] = None
    channel: Optional[str] = None
    stream: Optional[str] = None
    symbol: Optional[str] = None
    market_type: str = "linear_perpetual"
    decode_ok: bool = True
    decode_error: Optional[str] = None
    exchange_event_ts: Optional[int] = None
    update_id: Optional[int] = None
    first_update_id: Optional[int] = None
    previous_update_id: Optional[int] = None
    local_capture_ts: Optional[int] = None

    def to_row(self, max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES) -> dict[str, Any]:
        stored, original, truncated = _truncate(self.payload, max_payload_bytes)
        capture_ts = (
            self.local_capture_ts
            if self.local_capture_ts is not None
            else int(time.time() * 1000)
        )
        return {
            "timestamp": self.local_receive_ts,
            "local_receive_ts": self.local_receive_ts,
            "local_capture_ts": capture_ts,
            "venue": self.venue,
            "market_type": self.market_type,
            "symbol": self.symbol,
            "channel": self.channel,
            "stream": self.stream,
            "connection_id": self.connection_id,
            "connection_generation": self.connection_generation,
            "payload": stored,
            "payload_bytes": original,
            "truncated": truncated,
            "decode_ok": self.decode_ok,
            "decode_error": self.decode_error,
            "exchange_event_ts": self.exchange_event_ts,
            "update_id": self.update_id,
            "first_update_id": self.first_update_id,
            "previous_update_id": self.previous_update_id,
        }


@dataclass(frozen=True)
class RawRestRecord:
    """One REST request/response exchange plus its lineage."""

    request_ts: int
    response_receive_ts: Optional[int]
    endpoint: str
    purpose: str
    venue: str = "BINANCE"
    method: str = "GET"
    request_params: Mapping[str, Any] = field(default_factory=dict)
    http_status: Optional[int] = None
    ok: bool = False
    error: Optional[str] = None
    payload: Optional[str] = None
    symbol: Optional[str] = None
    market_type: str = "linear_perpetual"
    local_process_ts: Optional[int] = None

    def to_row(self, max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES) -> dict[str, Any]:
        body = self.payload or ""
        stored, original, truncated = _truncate(body, max_payload_bytes)
        try:
            params = json.dumps(dict(self.request_params), sort_keys=True)
        except (TypeError, ValueError):
            params = str(dict(self.request_params))
        return {
            # A failed request has no response; order by the request instead
            # rather than inventing a response time.
            "timestamp": self.response_receive_ts or self.request_ts,
            "request_ts": self.request_ts,
            "response_receive_ts": self.response_receive_ts,
            "local_process_ts": self.local_process_ts,
            "venue": self.venue,
            "market_type": self.market_type,
            "symbol": self.symbol,
            "purpose": self.purpose,
            "method": self.method,
            "endpoint": self.endpoint,
            "request_params": params,
            "http_status": self.http_status,
            "ok": self.ok,
            "error": self.error,
            "payload": stored if body else None,
            "payload_bytes": original if body else 0,
            "truncated": truncated,
        }


class RawCapture:
    """Persist raw records, failing open into the quality stream.

    ``wire_writer`` and ``rest_writer`` are anything exposing ``write(dict)``
    (in production, ``ParquetWriter``). Either may be ``None``, which disables
    that half of capture -- useful in tests and in deployments that only want
    REST lineage.
    """

    def __init__(
        self,
        wire_writer: Any = None,
        rest_writer: Any = None,
        *,
        quality_event_sink: Optional[Callable[[dict], None]] = None,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        enabled: bool = True,
    ) -> None:
        self.wire_writer = wire_writer
        self.rest_writer = rest_writer
        self.quality_event_sink = quality_event_sink
        self.max_payload_bytes = max_payload_bytes
        self.enabled = enabled
        self.wire_captured = 0
        self.rest_captured = 0
        self.capture_failures = 0
        self.truncations = 0

    def _fail_open(self, kind: str, exc: BaseException) -> None:
        """Raw capture must never take the ingest path down with it."""
        self.capture_failures += 1
        if self.quality_event_sink is None:
            return
        try:
            self.quality_event_sink(
                {
                    "stream": "raw_capture",
                    "event_type": "DATA_DROP",
                    "reason": f"raw_capture_failed:{kind}:{type(exc).__name__}",
                    "rows_lost": 1,
                    "local_ts": int(time.time() * 1000),
                }
            )
        except Exception:  # noqa: BLE001 - the sink itself must not propagate
            pass

    def capture_wire(self, record: RawWireRecord) -> bool:
        if not self.enabled or self.wire_writer is None:
            return False
        try:
            row = record.to_row(self.max_payload_bytes)
            if row["truncated"]:
                self.truncations += 1
                self._emit_truncation(row)
            self.wire_writer.write(row)
        except Exception as exc:  # noqa: BLE001 - deliberately fails open
            self._fail_open("wire", exc)
            return False
        self.wire_captured += 1
        return True

    def capture_rest(self, record: RawRestRecord) -> bool:
        if not self.enabled or self.rest_writer is None:
            return False
        try:
            row = record.to_row(self.max_payload_bytes)
            if row["truncated"]:
                self.truncations += 1
                self._emit_truncation(row)
            self.rest_writer.write(row)
        except Exception as exc:  # noqa: BLE001 - deliberately fails open
            self._fail_open("rest", exc)
            return False
        self.rest_captured += 1
        return True

    def _emit_truncation(self, row: Mapping[str, Any]) -> None:
        """A truncated payload is partial raw data and must be observable."""
        if self.quality_event_sink is None:
            return
        try:
            self.quality_event_sink(
                {
                    "stream": "raw_capture",
                    "event_type": "DATA_DROP",
                    "reason": "raw_payload_truncated",
                    "rows_lost": row.get("payload_bytes"),
                    "connection_id": row.get("connection_id"),
                    "local_ts": int(time.time() * 1000),
                }
            )
        except Exception:  # noqa: BLE001
            pass

    def stats(self) -> dict[str, int]:
        return {
            "wire_captured": self.wire_captured,
            "rest_captured": self.rest_captured,
            "capture_failures": self.capture_failures,
            "truncations": self.truncations,
        }
