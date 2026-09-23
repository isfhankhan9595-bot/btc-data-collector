"""Offline analysis for trade-duplicate and ID-ordering evidence.

See docs/DEDUP_EVIDENCE_EXPERIMENT_DESIGN.md for the full design rationale
(sections A-J below map directly to that document's lettered sections).
This module is a pure function of whatever records it is given -- it does
not capture, fetch, or fabricate data. Every test exercising it uses
synthetic, explicitly-labeled fixtures; nothing here is evidence about
real exchange behavior, only a verified tool for analyzing such evidence
if and when it exists.

Nothing in this module changes, reads, or duplicates the production
dedup logic in adapters/base.py. It defines its own notion of "duplicate"
deliberately mirroring `_dedupe_trades`'s identity tuple and None-id
exemption exactly (see design doc, section C), so the two can never
silently drift apart, but this module has no import dependency on that
one and vice versa.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence

from .adapters.binance import BinanceAdapter
from .adapters.binance_spot import BinanceSpotAdapter
from .adapters.bybit import BybitAdapter
from .adapters.okx import OKXAdapter
from .canonical import CanonicalTradeEvent

EXACT_DUPLICATE = "EXACT_DUPLICATE"
IDENTITY_PAYLOAD_CONFLICT = "IDENTITY_PAYLOAD_CONFLICT"


@dataclass(frozen=True)
class DedupEvidenceRecord:
    """One observed trade message, for offline analysis.

    ``local_receive_ts`` is the only timestamp any statistic below is
    computed from -- ``exchange_event_ts`` is carried for context and
    provenance only, exactly as every other causal module in this
    repository treats exchange timestamps.
    """
    exchange: str
    market_type: str
    instrument_key: Optional[str]
    stream: str
    trade_id: Optional[str]
    exchange_event_ts: Optional[int]
    local_receive_ts: Any
    local_processing_ts: Optional[int] = None
    connection_id: Optional[str] = None
    reconnect_marker: Optional[str] = None
    connection_generation: Optional[int] = None
    raw_payload_sha256: Optional[str] = None
    raw_payload: Optional[str] = None
    canonical_price: Optional[float] = None
    canonical_quantity: Optional[float] = None
    canonical_side: Optional[str] = None

    def identity(self) -> tuple:
        return (self.exchange, self.market_type, self.instrument_key or "", self.stream, self.trade_id)


@dataclass(frozen=True)
class RawTradeEvidenceIssue:
    reason: str
    locator: dict[str, Any]


@dataclass(frozen=True)
class RawTradeEvidenceConversion:
    records: list[DedupEvidenceRecord] = field(default_factory=list)
    invalid_records: list[RawTradeEvidenceIssue] = field(default_factory=list)


@dataclass(frozen=True)
class RawTradeDedupForensicReport:
    conversion: RawTradeEvidenceConversion
    analysis: "DedupEvidenceReport"


@dataclass(frozen=True)
class ObservedStatistic:
    """A numeric result that is always rendered with its evidentiary
    qualifier attached, so it is structurally awkward to quote the bare
    number without it (design doc, section J).

    ``None`` means "no data to compute this from" -- never a fabricated
    zero.
    """
    value: Optional[float]
    sample_size: int
    unit: str = "ms"

    def describe(self) -> str:
        if self.value is None:
            return f"no observation available (sample_size={self.sample_size})"
        return f"{self.value} {self.unit}, observed in this sample of {self.sample_size} records"

    def __repr__(self) -> str:
        return f"ObservedStatistic({self.describe()})"


@dataclass(frozen=True)
class DuplicateDelayReport:
    identity: tuple
    first_local_receive_ts: int
    duplicate_local_receive_ts: int
    delay_ms: int
    classification: str = EXACT_DUPLICATE
    different_fields: tuple[str, ...] = ()
    arrival_order_ambiguous: bool = False
    first_payload_sha256: Optional[str] = None
    duplicate_payload_sha256: Optional[str] = None
    first_connection_id: Optional[str] = None
    duplicate_connection_id: Optional[str] = None
    first_connection_generation: Optional[int] = None
    duplicate_connection_generation: Optional[int] = None
    first_reconnect_marker: Optional[str] = None
    duplicate_reconnect_marker: Optional[str] = None
    reconnect_associated: bool = False   # correlation only -- see design doc section E


@dataclass(frozen=True)
class OrderingReport:
    """Per (exchange, market_type, instrument_key, stream) ID-ordering
    summary, in local-receive order -- never exchange-event order, never
    ID-sorted order (either would assume the answer). ``applicable=False``
    for UUID-shaped identities (Bybit): ordering analysis is not attempted
    there, not silently reported as "always increasing"."""
    exchange: str
    market_type: str
    instrument_key: Optional[str]
    stream: str
    applicable: bool
    increases: int = 0
    equal: int = 0     # exact duplicates by ID, counted here too for completeness
    decreases: int = 0
    non_numeric_ids_skipped: int = 0


@dataclass(frozen=True)
class DedupEvidenceReport:
    total_records: int
    duplicate_count: int
    missing_id_count: int = 0   # records with trade_id=None -- never compared, but reported, not dropped silently
    missing_id_examples: list[dict[str, Any]] = field(default_factory=list)
    exact_duplicate_count: int = 0
    identity_payload_conflict_count: int = 0
    invalid_local_receive_ts_count: int = 0
    invalid_local_receive_ts_examples: list[dict[str, Any]] = field(default_factory=list)
    duplicates: list = field(default_factory=list)          # list[DuplicateDelayReport]
    delay_min: ObservedStatistic = None
    delay_median: ObservedStatistic = None
    delay_p95: ObservedStatistic = None
    delay_p99: ObservedStatistic = None
    delay_max: ObservedStatistic = None
    ordering_by_stream: dict = field(default_factory=dict)  # {(exch,mt,ik,stream): OrderingReport}


def _percentile(sorted_values: list, pct: float) -> float:
    """Nearest-rank percentile -- simple and sufficient for this tool's
    purpose (evidence summarization, not a statistics library)."""
    if not sorted_values:
        raise ValueError("cannot compute a percentile of an empty sequence")
    idx = min(len(sorted_values) - 1, int(round(pct * (len(sorted_values) - 1))))
    return sorted_values[idx]


def _is_numeric_id(trade_id: Optional[str]) -> bool:
    if trade_id is None:
        return False
    try:
        int(trade_id)
        return True
    except (ValueError, TypeError):
        return False


def _payload_sha256(payload: Optional[str]) -> Optional[str]:
    if payload is None:
        return None
    if not isinstance(payload, str):
        return None
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _record_locator(record: DedupEvidenceRecord) -> dict[str, Any]:
    return {
        "exchange": record.exchange,
        "market_type": record.market_type,
        "instrument_key": record.instrument_key,
        "stream": record.stream,
        "trade_id": record.trade_id,
        "exchange_event_ts": record.exchange_event_ts,
        "local_receive_ts": record.local_receive_ts,
        "local_processing_ts": record.local_processing_ts,
        "connection_id": record.connection_id,
        "connection_generation": record.connection_generation,
        "reconnect_marker": record.reconnect_marker,
        "raw_payload_sha256": record.raw_payload_sha256,
    }


def _sort_key(record: DedupEvidenceRecord, local_receive_ts: int) -> tuple:
    return (
        local_receive_ts,
        record.raw_payload_sha256 or "",
        record.connection_id or "",
        record.connection_generation if record.connection_generation is not None else -1,
        record.reconnect_marker or "",
        record.exchange_event_ts if record.exchange_event_ts is not None else -1,
        record.local_processing_ts if record.local_processing_ts is not None else -1,
        str(record.canonical_price),
        str(record.canonical_quantity),
        str(record.canonical_side),
    )


def _valid_local_receive_ts(value: Any) -> tuple[Optional[int], Optional[str]]:
    if isinstance(value, bool):
        return None, "INVALID_LOCAL_RECEIVE_TS_BOOL"
    if value is None:
        return None, "INVALID_LOCAL_RECEIVE_TS_NONE"
    if not isinstance(value, int):
        return None, "INVALID_LOCAL_RECEIVE_TS_NON_INT"
    if value < 0:
        return None, "INVALID_LOCAL_RECEIVE_TS_NEGATIVE"
    return value, None


def _duplicate_classification(first: DedupEvidenceRecord, duplicate: DedupEvidenceRecord) -> tuple[str, tuple[str, ...]]:
    different_fields = tuple(
        name for name, first_value, duplicate_value in (
            ("canonical_price", first.canonical_price, duplicate.canonical_price),
            ("canonical_quantity", first.canonical_quantity, duplicate.canonical_quantity),
            ("canonical_side", first.canonical_side, duplicate.canonical_side),
            ("exchange_event_ts", first.exchange_event_ts, duplicate.exchange_event_ts),
        )
        if first_value != duplicate_value
    )
    if different_fields:
        return IDENTITY_PAYLOAD_CONFLICT, different_fields
    return EXACT_DUPLICATE, ()


def _adapter_for(venue: Any, market_type: Any):
    venue_key = str(venue or "").upper()
    market_type_key = str(market_type or "")
    if venue_key == "BINANCE" and market_type_key == "spot":
        return BinanceSpotAdapter()
    if venue_key == "BINANCE":
        return BinanceAdapter()
    if venue_key == "BYBIT":
        return BybitAdapter()
    if venue_key == "OKX":
        return OKXAdapter()
    return None


def convert_raw_wire_to_dedup_evidence(
    raw_wire_rows: Sequence[Mapping[str, Any]],
    *,
    keep_raw_payload: bool = True,
) -> RawTradeEvidenceConversion:
    """Convert raw wire rows into forensic records without production dedup.

    This path uses each venue adapter's `normalize()` parser directly and does
    not call `normalize_message()`, so `_dedupe_trades()` semantics are never
    involved. Empty/missing/malformed/truncated/unroutable frames are returned
    as invalid evidence issues instead of disappearing silently.
    """
    records: list[DedupEvidenceRecord] = []
    invalid_records: list[RawTradeEvidenceIssue] = []

    for row in raw_wire_rows:
        payload = row.get("payload")
        payload_hash = _payload_sha256(payload if isinstance(payload, str) else None)
        local_receive_ts = row.get("local_receive_ts", row.get("timestamp"))
        locator = {
            "exchange": row.get("venue"),
            "market_type": row.get("market_type"),
            "instrument_key": row.get("instrument_key"),
            "stream": row.get("stream") or row.get("channel"),
            "trade_id": None,
            "exchange_event_ts": row.get("exchange_event_ts"),
            "local_receive_ts": local_receive_ts,
            "local_processing_ts": row.get("local_processing_ts"),
            "connection_id": row.get("connection_id"),
            "connection_generation": row.get("connection_generation"),
            "reconnect_marker": row.get("reconnect_marker"),
            "raw_payload_sha256": payload_hash,
        }
        if payload is None:
            invalid_records.append(RawTradeEvidenceIssue("MISSING_PAYLOAD", locator))
            continue
        if not isinstance(payload, str):
            invalid_records.append(RawTradeEvidenceIssue("MALFORMED_PAYLOAD_TYPE", locator))
            continue
        if payload == "":
            invalid_records.append(RawTradeEvidenceIssue("EMPTY_PAYLOAD", locator))
            continue
        if row.get("truncated"):
            invalid_records.append(RawTradeEvidenceIssue("TRUNCATED_PAYLOAD", locator))
            continue
        try:
            raw_json = json.loads(payload)
        except (TypeError, ValueError):
            invalid_records.append(RawTradeEvidenceIssue("MALFORMED_JSON", locator))
            continue

        adapter = _adapter_for(row.get("venue"), row.get("market_type"))
        if adapter is None:
            invalid_records.append(RawTradeEvidenceIssue("UNSUPPORTED_VENUE_OR_MARKET_TYPE", locator))
            continue

        adapter_local_receive_ts = local_receive_ts if isinstance(local_receive_ts, int) and not isinstance(local_receive_ts, bool) else 0
        try:
            events = adapter.normalize(raw_json, local_receive_ts=adapter_local_receive_ts)
        except Exception:  # noqa: BLE001 - evidence path must classify malformed rows, not crash.
            invalid_records.append(RawTradeEvidenceIssue("MALFORMED_PAYLOAD", locator))
            continue
        if not events:
            invalid_records.append(RawTradeEvidenceIssue("UNROUTABLE_FRAME", locator))
            continue
        trade_events = [event for event in events if isinstance(event, CanonicalTradeEvent)]
        if not trade_events:
            invalid_records.append(RawTradeEvidenceIssue("NON_TRADE_FRAME", locator))
            continue

        for event in trade_events:
            records.append(
                DedupEvidenceRecord(
                    exchange=event.exchange,
                    market_type=event.market_type,
                    instrument_key=event.instrument.key if event.instrument is not None else None,
                    stream=event.stream,
                    trade_id=event.trade_id,
                    exchange_event_ts=event.exchange_event_ts,
                    local_receive_ts=local_receive_ts,
                    local_processing_ts=row.get("local_processing_ts", event.local_process_ts),
                    connection_id=row.get("connection_id"),
                    reconnect_marker=row.get("reconnect_marker"),
                    connection_generation=row.get("connection_generation"),
                    raw_payload_sha256=payload_hash,
                    raw_payload=payload if keep_raw_payload else None,
                    canonical_price=event.price,
                    canonical_quantity=event.quantity,
                    canonical_side=event.side,
                )
            )
    return RawTradeEvidenceConversion(records=records, invalid_records=invalid_records)


def analyze_dedup_evidence_from_raw_wire(
    raw_wire_rows: Sequence[Mapping[str, Any]],
    *,
    keep_raw_payload: bool = True,
) -> RawTradeDedupForensicReport:
    conversion = convert_raw_wire_to_dedup_evidence(raw_wire_rows, keep_raw_payload=keep_raw_payload)
    return RawTradeDedupForensicReport(
        conversion=conversion,
        analysis=analyze_dedup_evidence(conversion.records),
    )


def analyze_dedup_evidence(records: list) -> DedupEvidenceReport:
    """Analyze a sequence of DedupEvidenceRecord for duplicates and ID
    ordering. Pure function; does not mutate or sort its input in place
    (a local copy is sorted for the ordering pass).

    Equal ``local_receive_ts`` values are treated as arrival-order ambiguity:
    delay remains exactly 0 and each duplicate report marks
    ``arrival_order_ambiguous=True``. Ordering ties are resolved using
    deterministic provenance fields so classification does not depend on
    input list order.
    """
    total = len(records)

    # --- Duplicate detection: group by identity, matching _dedupe_trades's
    # own None-id exemption exactly (design doc section C).
    by_identity: dict = {}
    valid_local_receive_ts: dict[int, int] = {}
    missing_id_count = 0
    missing_id_examples: list[dict[str, Any]] = []
    invalid_local_receive_ts_count = 0
    invalid_local_receive_ts_examples: list[dict[str, Any]] = []
    for r in records:
        receive_ts, invalid_reason = _valid_local_receive_ts(r.local_receive_ts)
        if invalid_reason is not None:
            invalid_local_receive_ts_count += 1
            if len(invalid_local_receive_ts_examples) < 10:
                invalid_local_receive_ts_examples.append(
                    {"reason": invalid_reason, **_record_locator(r)}
                )
            continue
        if r.trade_id is None:
            missing_id_count += 1
            if len(missing_id_examples) < 10:
                missing_id_examples.append(_record_locator(r))
            continue
        by_identity.setdefault(r.identity(), []).append(r)
        valid_local_receive_ts[id(r)] = receive_ts

    duplicates: list = []
    exact_duplicate_count = 0
    identity_payload_conflict_count = 0
    for identity, group in by_identity.items():
        if len(group) < 2:
            continue
        ordered = sorted(group, key=lambda r: _sort_key(r, valid_local_receive_ts[id(r)]))
        first = ordered[0]
        first_receive_ts = valid_local_receive_ts[id(first)]
        for dup in ordered[1:]:
            dup_receive_ts = valid_local_receive_ts[id(dup)]
            classification, different_fields = _duplicate_classification(first, dup)
            if classification == EXACT_DUPLICATE:
                exact_duplicate_count += 1
            else:
                identity_payload_conflict_count += 1
            marker_transition = (
                first.reconnect_marker is not None
                and dup.reconnect_marker is not None
                and dup.reconnect_marker != first.reconnect_marker
            )
            generation_transition = (
                first.connection_generation is not None
                and dup.connection_generation is not None
                and dup.connection_generation != first.connection_generation
            )
            reconnect_associated = (
                marker_transition or generation_transition
            )
            duplicates.append(DuplicateDelayReport(
                identity=identity,
                first_local_receive_ts=first_receive_ts,
                duplicate_local_receive_ts=dup_receive_ts,
                delay_ms=dup_receive_ts - first_receive_ts,
                classification=classification,
                different_fields=different_fields,
                arrival_order_ambiguous=dup_receive_ts == first_receive_ts,
                first_payload_sha256=first.raw_payload_sha256,
                duplicate_payload_sha256=dup.raw_payload_sha256,
                first_connection_id=first.connection_id,
                duplicate_connection_id=dup.connection_id,
                first_connection_generation=first.connection_generation,
                duplicate_connection_generation=dup.connection_generation,
                first_reconnect_marker=first.reconnect_marker,
                duplicate_reconnect_marker=dup.reconnect_marker,
                reconnect_associated=reconnect_associated,
            ))

    delays = sorted(d.delay_ms for d in duplicates)
    n = len(delays)

    def stat(value):
        return ObservedStatistic(value=value, sample_size=n)

    delay_min = stat(delays[0] if delays else None)
    delay_median = stat(_percentile(delays, 0.5) if delays else None)
    delay_p95 = stat(_percentile(delays, 0.95) if delays else None)
    delay_p99 = stat(_percentile(delays, 0.99) if delays else None)
    delay_max = stat(delays[-1] if delays else None)

    # --- Ordering: per (exchange, market_type, instrument_key, stream),
    # in local-receive order.
    by_stream: dict = {}
    for r in records:
        receive_ts, invalid_reason = _valid_local_receive_ts(r.local_receive_ts)
        if invalid_reason is not None:
            continue
        key = (r.exchange, r.market_type, r.instrument_key or "", r.stream)
        by_stream.setdefault(key, []).append(r)
        valid_local_receive_ts[id(r)] = receive_ts

    ordering_by_stream: dict = {}
    for key, group in by_stream.items():
        exchange, market_type, instrument_key, stream = key
        ordered = sorted(group, key=lambda r: _sort_key(r, valid_local_receive_ts[id(r)]))
        numeric_group = [r for r in ordered if _is_numeric_id(r.trade_id)]
        non_numeric = len(ordered) - len(numeric_group)

        if exchange == "BYBIT":
            # UUID-shaped identity: ordering analysis deliberately not
            # attempted (design doc section F / task's Bybit special case).
            ordering_by_stream[key] = OrderingReport(
                exchange=exchange, market_type=market_type,
                instrument_key=instrument_key or None, stream=stream,
                applicable=False, non_numeric_ids_skipped=len(ordered),
            )
            continue

        increases = equal = decreases = 0
        for prev, cur in zip(numeric_group, numeric_group[1:]):
            prev_id, cur_id = int(prev.trade_id), int(cur.trade_id)
            if cur_id > prev_id:
                increases += 1
            elif cur_id == prev_id:
                equal += 1
            else:
                decreases += 1

        ordering_by_stream[key] = OrderingReport(
            exchange=exchange, market_type=market_type,
            instrument_key=instrument_key or None, stream=stream,
            applicable=True, increases=increases, equal=equal,
            decreases=decreases, non_numeric_ids_skipped=non_numeric,
        )

    return DedupEvidenceReport(
        total_records=total, duplicate_count=len(duplicates), missing_id_count=missing_id_count,
        missing_id_examples=missing_id_examples,
        exact_duplicate_count=exact_duplicate_count,
        identity_payload_conflict_count=identity_payload_conflict_count,
        invalid_local_receive_ts_count=invalid_local_receive_ts_count,
        invalid_local_receive_ts_examples=invalid_local_receive_ts_examples,
        duplicates=duplicates,
        delay_min=delay_min, delay_median=delay_median, delay_p95=delay_p95,
        delay_p99=delay_p99, delay_max=delay_max, ordering_by_stream=ordering_by_stream,
    )
