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
* **Capture failure policy (F5).** An ORDINARY capture failure (a row that
  cannot be built, a writer that raised something other than a typed fatal) is
  surfaced as a durable quality event and the frame still flows: losing research
  fidelity is bad, losing the live feed is worse. A ``FatalStorageError`` from
  the raw writer is different: raw evidence is irrecoverable, and the durable
  writer is gone. A ``RawCapture`` built with ``fail_closed_on_fatal_storage=True``
  (the Binance USD-M collector) re-raises it, WITHOUT consulting the quality
  sink (which may itself be the failing component), so the application can stop.
  The default stays fail-open so runners that have no application-level
  termination path (Bybit / OKX / Binance Spot) are unchanged by F5.
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

from .clock import (
    EXCHANGE_TS_PRECISION,
    local_receive_precision_for,
    ms_from_ns,
    require_epoch_ns,
)
from .storage_errors import FatalStorageError

__all__ = [
    "RawWireRecord",
    "RawRestRecord",
    "RAW_WIRE_SCHEMA",
    "CAUSAL_RAW_WIRE_COLUMN",
    "NON_CAUSAL_RAW_WIRE_COLUMNS",
    "RAW_REST_SCHEMA",
    "RawCapture",
    "DEFAULT_MAX_PAYLOAD_BYTES",
]

#: Frames larger than this are stored truncated. Binance depth1000 snapshots
#: run ~100KB; 4MB leaves generous headroom while bounding a pathological frame.
DEFAULT_MAX_PAYLOAD_BYTES = 4 * 1024 * 1024


#: The only raw-wire column that may decide whether information was available
#: to the collector at a given time (P0-6/P0-10 causal contract).
CAUSAL_RAW_WIRE_COLUMN = "local_receive_ts"

#: Raw-wire columns that are provenance/diagnostics only. They record *when
#: something else happened* (row construction), not when the information
#: became known, so no causal path may read them. Enforced by
#: tests/test_p0_10_receive_time_semantics.py.
NON_CAUSAL_RAW_WIRE_COLUMNS = frozenset({"local_capture_ts"})

RAW_WIRE_SCHEMA = pa.schema(
    [
        # Canonical ordering timestamp for the segment writer.
        ("timestamp", pa.timestamp("ms", tz="UTC")),
        # Lineage: captured before decoding wherever technically possible.
        # local_receive_ts is the ONE causal clock: the instant the frame
        # became available to the collector (WebSocketClient._consume,
        # before decode/capture/enqueue). It equals ``timestamp`` above.
        ("local_receive_ts", pa.timestamp("ms", tz="UTC")),
        # LEGACY / NON-AUTHORITATIVE (P0-10). A wall-clock reading taken when
        # this raw *row was built* -- after JSON decode, before persistence.
        # It is NOT receive time, NOT exchange time, NOT persistence time and
        # NOT a feature-availability time; never use it for eligibility,
        # ordering, joins, staleness or splits. The column name is kept only
        # because renaming a persisted column would create mixed-schema raw
        # segments (daily compaction) for a column nothing reads. See
        # NON_CAUSAL_RAW_WIRE_COLUMNS and docs/RAW_CAPTURE.md.
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
        # --- schema 1.1 (P0-11), additive and nullable -------------------
        # Legacy 1.0 files lack these columns entirely; a reader must treat
        # absence/null as "millisecond precision only", never as zero.
        # Epoch nanoseconds, local wall clock, taken at the receive boundary
        # before decode/capture/queue. int64 (not timestamp[ns]) so pandas
        # cannot silently round-trip it through float64.
        ("local_receive_ns", pa.int64()),
        # Local monotonic ns: valid only as a difference within one process
        # run (same connection_id lineage). NOT an epoch value; never
        # comparable across restarts.
        ("receive_mono_ns", pa.int64()),
        # Precision *provenance*, so nanosecond-typed storage cannot be read
        # as nanosecond-measured exchange data.
        ("local_receive_precision", pa.string()),
        ("exchange_event_ts_precision", pa.string()),
    ],
    metadata={
        "schema_version": "1.1",
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
    #: LEGACY / NON-AUTHORITATIVE row-construction wall clock (see
    #: RAW_WIRE_SCHEMA). ``None`` means "stamp it when the row is built",
    #: which is still row-build time, never receive time. It can never
    #: influence ``timestamp`` / ``local_receive_ts`` below.
    local_capture_ts: Optional[int] = None
    #: P0-11: receive-boundary stamps (see collector.clock). Optional so
    #: legacy callers keep working; absence is recorded as ms-only precision.
    local_receive_ns: Optional[int] = None
    receive_mono_ns: Optional[int] = None

    def to_row(self, max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES) -> dict[str, Any]:
        stored, original, truncated = _truncate(self.payload, max_payload_bytes)
        if self.local_receive_ns is not None:
            require_epoch_ns(self.local_receive_ns, "local_receive_ns")
            # The legacy ms column and the ns column come from ONE clock
            # read; a mismatch means two clock domains were mixed.
            if ms_from_ns(self.local_receive_ns) != self.local_receive_ts:
                raise ValueError(
                    "local_receive_ts (ms) disagrees with local_receive_ns: "
                    f"{self.local_receive_ts} != {ms_from_ns(self.local_receive_ns)}"
                )
        # Row-build wall clock only (legacy provenance, P0-10). Deliberately
        # never substituted for local_receive_ts, which is a required field.
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
            "local_receive_ns": self.local_receive_ns,
            "receive_mono_ns": self.receive_mono_ns,
            "local_receive_precision": local_receive_precision_for(self.local_receive_ns),
            # Every supported venue stamps exchange time in ms; recorded only
            # when there is an exchange timestamp to qualify.
            "exchange_event_ts_precision": (
                EXCHANGE_TS_PRECISION if self.exchange_event_ts is not None else None
            ),
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
    """Persist raw records; ordinary failures fail open into the quality stream.

    ``wire_writer`` and ``rest_writer`` are anything exposing ``write(dict)``
    (in production, ``ParquetWriter``). Either may be ``None``, which disables
    that half of capture -- useful in tests and in deployments that only want
    REST lineage.

    ``fail_closed_on_fatal_storage`` (F5): when True, a ``FatalStorageError``
    from either writer propagates to the caller instead of being absorbed by the
    fail-open path. The check is ``isinstance(exc, FatalStorageError)`` and is
    placed BEFORE the generic ``Exception`` handler; no message or class-name
    matching is involved, and an ordinary ``RuntimeError`` stays ordinary.
    """

    def __init__(
        self,
        wire_writer: Any = None,
        rest_writer: Any = None,
        *,
        quality_event_sink: Optional[Callable[[dict], None]] = None,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        enabled: bool = True,
        fail_closed_on_fatal_storage: bool = False,
    ) -> None:
        self.wire_writer = wire_writer
        self.rest_writer = rest_writer
        self.quality_event_sink = quality_event_sink
        self.max_payload_bytes = max_payload_bytes
        self.enabled = enabled
        self.fail_closed_on_fatal_storage = fail_closed_on_fatal_storage
        self.wire_captured = 0
        self.rest_captured = 0
        self.capture_failures = 0
        self.truncations = 0
        #: FatalStorageErrors seen from either writer (counted whether or not
        #: they were re-raised). Never one-per-frame spam: a fail-closed
        #: application stops ingesting after the first.
        self.fatal_capture_failures = 0

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
        except FatalStorageError as exc:
            # MUST stay ahead of the generic handler. The raw writer is the
            # irrecoverable-evidence boundary: no quality-sink call here (the
            # sink may be the very thing that is broken), just count and, when
            # fail-closed, let the application's fatal boundary decide.
            self.fatal_capture_failures += 1
            self.capture_failures += 1
            if self.fail_closed_on_fatal_storage:
                raise
            self._fail_open("wire", exc)
            return False
        except Exception as exc:  # noqa: BLE001 - ordinary failures fail open
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
        except FatalStorageError as exc:
            # See capture_wire: typed branch first, never via the quality sink.
            self.fatal_capture_failures += 1
            self.capture_failures += 1
            if self.fail_closed_on_fatal_storage:
                raise
            self._fail_open("rest", exc)
            return False
        except Exception as exc:  # noqa: BLE001 - ordinary failures fail open
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
                    # ``rows_lost`` is a ROW count everywhere else in the
                    # quality-event contract (1/0/N events). One truncated
                    # frame is one affected row -- never its byte length.
                    "rows_lost": 1,
                    # The original (pre-truncation) byte length is separate
                    # evidence. It is carried on the event as an extra key
                    # (unknown keys are ignored by the quality-event writers,
                    # so this is schema-compatible) and is durably recorded on
                    # the raw row itself as ``payload_bytes`` + ``truncated``.
                    "payload_bytes": row.get("payload_bytes"),
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
