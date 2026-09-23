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

from dataclasses import dataclass, field
from typing import Optional


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
    local_receive_ts: int
    local_processing_ts: Optional[int] = None
    connection_id: Optional[str] = None
    reconnect_marker: Optional[str] = None

    def identity(self) -> tuple:
        return (self.exchange, self.market_type, self.instrument_key or "", self.stream, self.trade_id)


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
    reconnect_associated: bool   # correlation only -- see design doc section E


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


def analyze_dedup_evidence(records: list) -> DedupEvidenceReport:
    """Analyze a sequence of DedupEvidenceRecord for duplicates and ID
    ordering. Pure function; does not mutate or sort its input in place
    (a local copy is sorted for the ordering pass).

    Known limitation, documented rather than papered over with more
    machinery: when two or more records of the same identity share the
    exact same ``local_receive_ts``, which one is labeled "first" (vs.
    "duplicate") is decided by Python's stable sort, i.e. by their
    position in the input list -- there is no secondary ordering key in
    the record schema to break the tie any other way. This does not
    affect any reported delay (it is always exactly 0 for a same-timestamp
    pair, regardless of which record is picked as "first"), so no
    statistic in this report is wrong because of it; only forensic
    inspection of *which specific record* is labeled which would be
    input-order-sensitive in this one case.
    """
    total = len(records)

    # --- Duplicate detection: group by identity, matching _dedupe_trades's
    # own None-id exemption exactly (design doc section C).
    by_identity: dict = {}
    missing_id_count = 0
    for r in records:
        if r.trade_id is None:
            missing_id_count += 1
            continue
        by_identity.setdefault(r.identity(), []).append(r)

    duplicates: list = []
    for identity, group in by_identity.items():
        if len(group) < 2:
            continue
        ordered = sorted(group, key=lambda r: r.local_receive_ts)
        first = ordered[0]
        for dup in ordered[1:]:
            reconnect_associated = (
                dup.reconnect_marker is not None
                and dup.reconnect_marker != first.reconnect_marker
            )
            duplicates.append(DuplicateDelayReport(
                identity=identity,
                first_local_receive_ts=first.local_receive_ts,
                duplicate_local_receive_ts=dup.local_receive_ts,
                delay_ms=dup.local_receive_ts - first.local_receive_ts,
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
        key = (r.exchange, r.market_type, r.instrument_key or "", r.stream)
        by_stream.setdefault(key, []).append(r)

    ordering_by_stream: dict = {}
    for key, group in by_stream.items():
        exchange, market_type, instrument_key, stream = key
        ordered = sorted(group, key=lambda r: r.local_receive_ts)
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
        duplicates=duplicates,
        delay_min=delay_min, delay_median=delay_median, delay_p95=delay_p95,
        delay_p99=delay_p99, delay_max=delay_max, ordering_by_stream=ordering_by_stream,
    )
