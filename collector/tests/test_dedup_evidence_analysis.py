"""Tests for dedup_evidence_analysis.py.

Every fixture in this file is SYNTHETIC, constructed to exercise specific
arithmetic paths in the analyzer. None of it is presented as, or should be
read as, evidence about real exchange behavior -- these tests prove the
tool computes correctly on known inputs, which is a precondition for it
being useful once real evidence exists, not a substitute for that evidence.
"""
from __future__ import annotations

import hashlib

from collector.collector.dedup_evidence_analysis import (
    DedupEvidenceRecord,
    EXACT_DUPLICATE,
    IDENTITY_PAYLOAD_CONFLICT,
    analyze_dedup_evidence,
    analyze_dedup_evidence_from_raw_wire,
    convert_raw_wire_to_dedup_evidence,
)


def _rec(exchange, market_type, stream, trade_id, local_receive_ts, *,
         instrument_key="BINANCE|linear_perpetual|BTC-USDT|BTCUSDT",
         exchange_event_ts=None, connection_id=None, reconnect_marker=None,
         connection_generation=None, canonical_price=100.0, canonical_quantity=1.0,
         canonical_side="BUY", raw_payload_sha256=None):
    return DedupEvidenceRecord(
        exchange=exchange, market_type=market_type, instrument_key=instrument_key,
        stream=stream, trade_id=trade_id, exchange_event_ts=exchange_event_ts or local_receive_ts,
        local_receive_ts=local_receive_ts, connection_id=connection_id,
        reconnect_marker=reconnect_marker, connection_generation=connection_generation,
        canonical_price=canonical_price, canonical_quantity=canonical_quantity,
        canonical_side=canonical_side, raw_payload_sha256=raw_payload_sha256,
    )


# ---------------------------------------------------------------------------
# Duplicate detection matches _dedupe_trades's own contract.
# ---------------------------------------------------------------------------


def test_no_duplicates_in_a_clean_set():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "1", 1000),
              _rec("BINANCE", "linear_perpetual", "trades", "2", 1010)]
    report = analyze_dedup_evidence(records)
    assert report.duplicate_count == 0
    assert report.delay_min.value is None


def test_exact_duplicate_is_detected_with_correct_delay():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "1", 1000, exchange_event_ts=900),
              _rec("BINANCE", "linear_perpetual", "trades", "1", 1250, exchange_event_ts=900)]
    report = analyze_dedup_evidence(records)
    assert report.duplicate_count == 1
    assert report.exact_duplicate_count == 1
    assert report.identity_payload_conflict_count == 0
    assert report.duplicates[0].delay_ms == 250
    assert report.duplicates[0].classification == EXACT_DUPLICATE
    assert report.delay_min.value == 250 and report.delay_max.value == 250


def test_missing_trade_id_is_never_deduplicated_against_anything():
    """Mirrors _dedupe_trades's own explicitly flagged catastrophic
    failure mode exactly: None must never collapse together."""
    records = [_rec("BINANCE", "linear_perpetual", "trades", None, 1000),
              _rec("BINANCE", "linear_perpetual", "trades", None, 1010),
              _rec("BINANCE", "linear_perpetual", "trades", None, 1020)]
    report = analyze_dedup_evidence(records)
    assert report.duplicate_count == 0


def test_missing_trade_id_is_counted_not_silently_dropped():
    """Audit finding: the report must surface how many records had no
    trade_id at all -- required by this task's own Definition of Done
    ("missing IDs") -- not merely exclude them from duplicate detection
    with no visible trace."""
    records = [_rec("BINANCE", "linear_perpetual", "trades", None, 1000),
              _rec("BINANCE", "linear_perpetual", "trades", "1", 1010),
              _rec("BINANCE", "linear_perpetual", "trades", None, 1020)]
    result = analyze_dedup_evidence(records)
    assert result.missing_id_count == 2
    assert result.total_records == 3
    assert result.duplicate_count == 0


def test_same_local_receive_ts_duplicate_has_zero_delay_regardless_of_input_order():
    """Audit finding: tie-breaking for which record is 'first' when two
    share an identical local_receive_ts is input-order-dependent (no
    secondary key exists), documented in analyze_dedup_evidence's own
    docstring. This pins the one thing that IS guaranteed regardless of
    that tie-break: delay_ms is always exactly 0 for such a pair, whichever
    record ends up labeled 'first'."""
    a = _rec("BINANCE", "linear_perpetual", "trades", "1", 5000)
    b = _rec("BINANCE", "linear_perpetual", "trades", "1", 5000)
    forward = analyze_dedup_evidence([a, b])
    reversed_input = analyze_dedup_evidence([b, a])
    assert forward.duplicate_count == reversed_input.duplicate_count == 1
    assert forward.duplicates[0].delay_ms == reversed_input.duplicates[0].delay_ms == 0
    assert forward.duplicates[0].arrival_order_ambiguous is True
    assert reversed_input.duplicates[0].arrival_order_ambiguous is True


def test_same_trade_id_different_stream_is_not_a_duplicate():
    """OKX trades vs trades-all: must never be compared against each other."""
    records = [_rec("OKX", "linear_perpetual", "trades", "5", 1000,
                    instrument_key="OKX|linear_perpetual|BTC-USDT|BTC-USDT-SWAP"),
              _rec("OKX", "linear_perpetual", "trades-all", "5", 1010,
                    instrument_key="OKX|linear_perpetual|BTC-USDT|BTC-USDT-SWAP")]
    report = analyze_dedup_evidence(records)
    assert report.duplicate_count == 0


def test_same_trade_id_different_market_type_is_not_a_duplicate():
    """Binance Spot vs USD-M sharing a native symbol must never collide."""
    records = [_rec("BINANCE", "spot", "trades", "1", 1000,
                    instrument_key="BINANCE|spot|BTC-USDT|BTCUSDT"),
              _rec("BINANCE", "linear_perpetual", "trades", "1", 1010,
                    instrument_key="BINANCE|linear_perpetual|BTC-USDT|BTCUSDT")]
    report = analyze_dedup_evidence(records)
    assert report.duplicate_count == 0


def test_three_deliveries_of_the_same_trade_produce_two_duplicate_reports():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "1", 1000),
              _rec("BINANCE", "linear_perpetual", "trades", "1", 1100),
              _rec("BINANCE", "linear_perpetual", "trades", "1", 1300)]
    report = analyze_dedup_evidence(records)
    assert report.duplicate_count == 2
    delays = sorted(d.delay_ms for d in report.duplicates)
    assert delays == [100, 300]   # both measured from the FIRST delivery, not chained


def test_input_order_does_not_affect_delay_calculation():
    """The 'first' delivery is determined by local_receive_ts, not list
    position -- feeding the duplicate before the original in the input
    list must not change the result."""
    records = [_rec("BINANCE", "linear_perpetual", "trades", "1", 1250),
              _rec("BINANCE", "linear_perpetual", "trades", "1", 1000)]
    report = analyze_dedup_evidence(records)
    assert report.duplicates[0].delay_ms == 250
    assert report.duplicates[0].first_local_receive_ts == 1000


# ---------------------------------------------------------------------------
# Statistic labeling: never a bare number.
# ---------------------------------------------------------------------------


def test_observed_statistic_describe_always_carries_the_qualifier():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "1", 1000),
              _rec("BINANCE", "linear_perpetual", "trades", "1", 1017)]
    report = analyze_dedup_evidence(records)
    description = report.delay_max.describe()
    assert "observed in this sample" in description
    assert "17" in description


def test_observed_statistic_with_no_data_says_so_not_zero():
    report = analyze_dedup_evidence([_rec("BINANCE", "linear_perpetual", "trades", "1", 1000)])
    assert report.delay_min.value is None
    assert "no observation available" in report.delay_min.describe()


def test_percentiles_are_computed_over_a_larger_sample():
    base = 1000
    records = [_rec("BINANCE", "linear_perpetual", "trades", "1", base)]
    # 10 duplicates of the same trade at increasing delays 100..1000ms
    for i in range(1, 11):
        records.append(_rec("BINANCE", "linear_perpetual", "trades", "1", base + i * 100))
    report = analyze_dedup_evidence(records)
    assert report.duplicate_count == 10
    assert report.delay_min.value == 100
    assert report.delay_max.value == 1000
    assert report.delay_median.sample_size == 10


# ---------------------------------------------------------------------------
# Reconnect association: correlation only, explicitly not causal.
# ---------------------------------------------------------------------------


def test_duplicate_after_a_different_reconnect_marker_is_flagged_associated():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "1", 1000, reconnect_marker="conn-A"),
              _rec("BINANCE", "linear_perpetual", "trades", "1", 2000, reconnect_marker="conn-B")]
    report = analyze_dedup_evidence(records)
    assert report.duplicates[0].reconnect_associated is True


def test_duplicate_on_the_same_connection_is_not_flagged_associated():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "1", 1000, reconnect_marker="conn-A"),
              _rec("BINANCE", "linear_perpetual", "trades", "1", 1050, reconnect_marker="conn-A")]
    report = analyze_dedup_evidence(records)
    assert report.duplicates[0].reconnect_associated is False


def test_no_reconnect_marker_at_all_is_not_flagged_associated():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "1", 1000),
              _rec("BINANCE", "linear_perpetual", "trades", "1", 1050)]
    report = analyze_dedup_evidence(records)
    assert report.duplicates[0].reconnect_associated is False


def test_duplicate_across_connection_generations_is_associated_even_without_marker():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "1", 1000, connection_generation=1),
               _rec("BINANCE", "linear_perpetual", "trades", "1", 1050, connection_generation=2)]
    report = analyze_dedup_evidence(records)
    assert report.duplicates[0].reconnect_associated is True


# ---------------------------------------------------------------------------
# Hostile forensic payload conflict checks.
# ---------------------------------------------------------------------------


def test_same_identity_different_price_is_identity_payload_conflict():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "dup-1", 1000, canonical_price=100.0, exchange_event_ts=900),
               _rec("BINANCE", "linear_perpetual", "trades", "dup-1", 1100, canonical_price=101.0, exchange_event_ts=900)]
    report = analyze_dedup_evidence(records)
    dup = report.duplicates[0]
    assert dup.classification == IDENTITY_PAYLOAD_CONFLICT
    assert dup.different_fields == ("canonical_price",)


def test_same_identity_different_quantity_side_and_exchange_ts_are_conflict():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "dup-2", 1000,
                    canonical_quantity=1.0, canonical_side="BUY", exchange_event_ts=1000),
               _rec("BINANCE", "linear_perpetual", "trades", "dup-2", 1100,
                    canonical_quantity=2.0, canonical_side="SELL", exchange_event_ts=1001)]
    report = analyze_dedup_evidence(records)
    dup = report.duplicates[0]
    assert dup.classification == IDENTITY_PAYLOAD_CONFLICT
    assert set(dup.different_fields) == {"canonical_quantity", "canonical_side", "exchange_event_ts"}


def test_same_identity_identical_payload_fields_stays_exact_duplicate_with_hash_provenance():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "dup-3", 1000, raw_payload_sha256="a", exchange_event_ts=900),
               _rec("BINANCE", "linear_perpetual", "trades", "dup-3", 1010, raw_payload_sha256="a", exchange_event_ts=900)]
    report = analyze_dedup_evidence(records)
    dup = report.duplicates[0]
    assert dup.classification == EXACT_DUPLICATE
    assert dup.first_payload_sha256 == "a"
    assert dup.duplicate_payload_sha256 == "a"


# ---------------------------------------------------------------------------
# Timestamp hardening / missing-id semantics.
# ---------------------------------------------------------------------------


def test_invalid_local_receive_timestamps_are_reported_not_silently_used():
    records = [
        _rec("BINANCE", "linear_perpetual", "trades", "1", None),
        _rec("BINANCE", "linear_perpetual", "trades", "1", True),
        _rec("BINANCE", "linear_perpetual", "trades", "1", "1000"),
        _rec("BINANCE", "linear_perpetual", "trades", "1", -1),
    ]
    report = analyze_dedup_evidence(records)
    assert report.invalid_local_receive_ts_count == 4
    assert report.duplicate_count == 0


def test_negative_receive_timestamp_is_rejected_not_clamped_into_delay_math():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "n", -5),
               _rec("BINANCE", "linear_perpetual", "trades", "n", 10)]
    report = analyze_dedup_evidence(records)
    assert report.invalid_local_receive_ts_count == 1
    assert report.duplicate_count == 0


def test_empty_trade_id_is_a_real_identity_but_none_is_missing():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "", 1000),
               _rec("BINANCE", "linear_perpetual", "trades", "", 1010),
               _rec("BINANCE", "linear_perpetual", "trades", None, 1020)]
    report = analyze_dedup_evidence(records)
    assert report.duplicate_count == 1
    assert report.missing_id_count == 1


def test_numeric_string_and_uuid_ids_supported_for_identity_grouping():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "12345", 1000),
               _rec("BINANCE", "linear_perpetual", "trades", "12345", 1010),
               _rec("BYBIT", "linear_perpetual", "trades",
                    "20f43950-d8dd-5b31-9112-a178eb6023af", 2000,
                    instrument_key="BYBIT|linear_perpetual|BTC-USDT|BTCUSDT"),
               _rec("BYBIT", "linear_perpetual", "trades",
                    "20f43950-d8dd-5b31-9112-a178eb6023af", 2010,
                    instrument_key="BYBIT|linear_perpetual|BTC-USDT|BTCUSDT")]
    report = analyze_dedup_evidence(records)
    assert report.duplicate_count == 2


# ---------------------------------------------------------------------------
# Raw-wire converter tests: valid + malformed evidence classification.
# ---------------------------------------------------------------------------


def test_raw_converter_hashes_exact_payload_bytes_and_preserves_trade_fields():
    payload = '{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1000,"T":1000,"a":7,"p":"100","q":"0.1","m":false}}'
    conversion = convert_raw_wire_to_dedup_evidence([{
        "venue": "BINANCE",
        "market_type": "linear_perpetual",
        "stream": "btcusdt@aggTrade",
        "connection_id": "c1",
        "local_receive_ts": 1000,
        "payload": payload,
    }])
    assert len(conversion.records) == 1
    record = conversion.records[0]
    assert record.trade_id == "7"
    assert record.canonical_price == 100.0
    assert record.canonical_quantity == 0.1
    assert record.canonical_side == "BUY"
    assert record.raw_payload_sha256 == hashlib.sha256(payload.encode("utf-8")).hexdigest()


def test_raw_converter_classifies_missing_empty_truncated_and_malformed_payloads():
    conversion = convert_raw_wire_to_dedup_evidence([
        {"venue": "BINANCE", "market_type": "linear_perpetual", "local_receive_ts": 1, "payload": None},
        {"venue": "BINANCE", "market_type": "linear_perpetual", "local_receive_ts": 2, "payload": ""},
        {"venue": "BINANCE", "market_type": "linear_perpetual", "local_receive_ts": 3, "payload": "{", "truncated": True},
        {"venue": "BINANCE", "market_type": "linear_perpetual", "local_receive_ts": 4, "payload": "{"},
    ])
    reasons = [issue.reason for issue in conversion.invalid_records]
    assert reasons == ["MISSING_PAYLOAD", "EMPTY_PAYLOAD", "TRUNCATED_PAYLOAD", "MALFORMED_JSON"]


def test_raw_converter_keeps_okx_trades_and_trades_all_isolated():
    trades = '{"arg":{"channel":"trades","instId":"BTC-USDT-SWAP"},"data":[{"ts":"1000","tradeId":"1","px":"100","sz":"0.1","side":"buy","seqId":"1"}]}'
    trades_all = '{"arg":{"channel":"trades-all","instId":"BTC-USDT-SWAP"},"data":[{"ts":"1001","tradeId":"1","px":"100","sz":"0.1","side":"buy","seqId":"1","source":"0"}]}'
    conversion = convert_raw_wire_to_dedup_evidence([
        {"venue": "OKX", "market_type": "linear_perpetual", "local_receive_ts": 1000, "payload": trades},
        {"venue": "OKX", "market_type": "linear_perpetual", "local_receive_ts": 1001, "payload": trades_all},
    ])
    streams = sorted(record.stream for record in conversion.records)
    assert streams == ["trades", "trades-all"]
    report = analyze_dedup_evidence(conversion.records)
    assert report.duplicate_count == 0


def test_raw_forensic_analysis_uses_raw_rows_not_deduplicated_replay_output():
    payload = '{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1000,"T":1000,"a":11,"p":"100","q":"0.1","m":false}}'
    result = analyze_dedup_evidence_from_raw_wire([
        {"venue": "BINANCE", "market_type": "linear_perpetual", "local_receive_ts": 1000, "payload": payload},
        {"venue": "BINANCE", "market_type": "linear_perpetual", "local_receive_ts": 1010, "payload": payload},
    ])
    assert len(result.conversion.records) == 2
    assert result.analysis.duplicate_count == 1


# ---------------------------------------------------------------------------
# Mutation-style invariants (tests fail when common hostile mutations applied).
# ---------------------------------------------------------------------------


def test_mutation_removing_instrument_from_identity_would_be_caught():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "x", 1000,
                    instrument_key="BINANCE|linear_perpetual|BTC-USDT|BTCUSDT"),
               _rec("BINANCE", "linear_perpetual", "trades", "x", 1010,
                    instrument_key="BINANCE|linear_perpetual|BTC-USDT|ETHUSDT")]
    assert analyze_dedup_evidence(records).duplicate_count == 0


def test_mutation_ignoring_payload_comparison_fields_would_be_caught():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "m1", 1000, canonical_price=100, canonical_quantity=1, canonical_side="BUY"),
               _rec("BINANCE", "linear_perpetual", "trades", "m1", 1010, canonical_price=101, canonical_quantity=2, canonical_side="SELL")]
    dup = analyze_dedup_evidence(records).duplicates[0]
    assert dup.classification == IDENTITY_PAYLOAD_CONFLICT
    assert {"canonical_price", "canonical_quantity", "canonical_side"} <= set(dup.different_fields)


def test_mutation_inverting_reconnect_association_would_be_caught():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "m2", 1000, reconnect_marker="same"),
               _rec("BINANCE", "linear_perpetual", "trades", "m2", 1010, reconnect_marker="same")]
    assert analyze_dedup_evidence(records).duplicates[0].reconnect_associated is False

# ---------------------------------------------------------------------------
# Ordering analysis: numeric IDs vs Bybit's UUID exclusion.
# ---------------------------------------------------------------------------


def test_monotonic_numeric_ids_are_reported_as_all_increases():
    records = [_rec("BINANCE", "linear_perpetual", "trades", str(i), 1000 + i * 10)
              for i in range(1, 6)]
    report = analyze_dedup_evidence(records)
    key = ("BINANCE", "linear_perpetual", "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT", "trades")
    ordering = report.ordering_by_stream[key]
    assert ordering.applicable is True
    assert ordering.increases == 4 and ordering.decreases == 0


def test_a_decrease_is_counted_separately_from_increases():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "5", 1000),
              _rec("BINANCE", "linear_perpetual", "trades", "3", 1010),   # decrease
              _rec("BINANCE", "linear_perpetual", "trades", "10", 1020)]  # increase
    report = analyze_dedup_evidence(records)
    key = ("BINANCE", "linear_perpetual", "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT", "trades")
    ordering = report.ordering_by_stream[key]
    assert ordering.increases == 1 and ordering.decreases == 1


def test_bybit_uuid_ids_are_marked_not_applicable_for_ordering():
    """The task's explicit Bybit special case: never convert a UUID into
    an artificial integer, never sort lexicographically and call it
    exchange ordering. This must show up as applicable=False, not as a
    silently-computed (and meaningless) result."""
    records = [_rec("BYBIT", "linear_perpetual", "trades",
                    "20f43950-d8dd-5b31-9112-a178eb6023af", 1000,
                    instrument_key="BYBIT|linear_perpetual|BTC-USDT|BTCUSDT"),
              _rec("BYBIT", "linear_perpetual", "trades",
                    "3a1b2c3d-e4f5-6789-0abc-def123456789", 1010,
                    instrument_key="BYBIT|linear_perpetual|BTC-USDT|BTCUSDT")]
    report = analyze_dedup_evidence(records)
    key = ("BYBIT", "linear_perpetual", "BYBIT|linear_perpetual|BTC-USDT|BTCUSDT", "trades")
    ordering = report.ordering_by_stream[key]
    assert ordering.applicable is False
    assert ordering.increases == 0 and ordering.decreases == 0   # not computed, not guessed


def test_okx_trades_and_trades_all_get_separate_ordering_reports():
    records = [_rec("OKX", "linear_perpetual", "trades", "1", 1000,
                    instrument_key="OKX|linear_perpetual|BTC-USDT|BTC-USDT-SWAP"),
              _rec("OKX", "linear_perpetual", "trades", "2", 1010,
                    instrument_key="OKX|linear_perpetual|BTC-USDT|BTC-USDT-SWAP"),
              _rec("OKX", "linear_perpetual", "trades-all", "1", 1005,
                    instrument_key="OKX|linear_perpetual|BTC-USDT|BTC-USDT-SWAP")]
    report = analyze_dedup_evidence(records)
    trades_key = ("OKX", "linear_perpetual", "OKX|linear_perpetual|BTC-USDT|BTC-USDT-SWAP", "trades")
    trades_all_key = ("OKX", "linear_perpetual", "OKX|linear_perpetual|BTC-USDT|BTC-USDT-SWAP", "trades-all")
    assert trades_key in report.ordering_by_stream
    assert trades_all_key in report.ordering_by_stream
    assert report.ordering_by_stream[trades_key] is not report.ordering_by_stream[trades_all_key]


# ---------------------------------------------------------------------------
# Purity: the analyzer must not mutate its input.
# ---------------------------------------------------------------------------


def test_analyze_does_not_mutate_or_reorder_the_input_list():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "2", 1200),
              _rec("BINANCE", "linear_perpetual", "trades", "1", 1000)]
    original_order = list(records)
    analyze_dedup_evidence(records)
    assert records == original_order   # same objects, same order, untouched
