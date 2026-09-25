"""Tests for dedup_evidence_analysis.py.

Every fixture in this file is SYNTHETIC, constructed to exercise specific
arithmetic paths in the analyzer. None of it is presented as, or should be
read as, evidence about real exchange behavior -- these tests prove the
tool computes correctly on known inputs, which is a precondition for it
being useful once real evidence exists, not a substitute for that evidence.
"""
from __future__ import annotations

import hashlib
import json

import pytest

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


def test_raw_converter_preserves_a_same_frame_duplicate_trade():
    """Critical Issue #2's mandatory test: a SINGLE raw frame containing
    two identical trade entries must produce TWO DedupEvidenceRecord
    objects, not one. This is the one case a fresh-adapter-per-row
    architecture does NOT protect against on its own -- production
    _dedupe_trades operates on the list normalize() returns for one
    message, so if this converter called adapter.normalize() (the
    dedup-active, wrapped method) instead of bypassing it via
    __wrapped__, the second identical trade in this single OKX push
    would be silently suppressed before ever reaching this module.
    Verified empirically in this test, not merely asserted from reading
    functools.wraps's documentation."""
    payload = json.dumps({"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [
        {"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "100", "sz": "1", "side": "buy", "ts": "1000"},
        {"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "100", "sz": "1", "side": "buy", "ts": "1000"},
    ]})
    conversion = convert_raw_wire_to_dedup_evidence([{
        "venue": "OKX", "market_type": "linear_perpetual", "local_receive_ts": 1000, "payload": payload,
    }])
    assert len(conversion.records) == 2, \
        "a same-frame duplicate must survive the forensic converter -- production dedup must be bypassed"
    assert conversion.records[0].trade_id == conversion.records[1].trade_id == "1"
    # And the analyzer, given these two preserved records, correctly
    # reports them as a duplicate pair -- proving the two ends connect.
    report = analyze_dedup_evidence(conversion.records)
    assert report.duplicate_count == 1


def test_raw_converter_preserves_same_frame_duplicates_with_conflicting_payloads():
    """Same-frame duplicate identity with different canonical fields:
    must still both survive conversion, and the analyzer must classify
    the pair as IDENTITY_PAYLOAD_CONFLICT, not silently pick one."""
    payload = json.dumps({"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [
        {"instId": "BTC-USDT-SWAP", "tradeId": "9", "px": "100", "sz": "1", "side": "buy", "ts": "1000"},
        {"instId": "BTC-USDT-SWAP", "tradeId": "9", "px": "101", "sz": "2", "side": "sell", "ts": "1000"},
    ]})
    conversion = convert_raw_wire_to_dedup_evidence([{
        "venue": "OKX", "market_type": "linear_perpetual", "local_receive_ts": 1000, "payload": payload,
    }])
    assert len(conversion.records) == 2
    report = analyze_dedup_evidence(conversion.records)
    assert report.duplicate_count == 1
    assert report.duplicates[0].classification == IDENTITY_PAYLOAD_CONFLICT


def test_raw_converter_preserves_same_frame_distinct_trade_ids():
    """Same-frame, different trade IDs -- both survive, neither is
    treated as a duplicate of the other."""
    payload = json.dumps({"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [
        {"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "100", "sz": "1", "side": "buy", "ts": "1000"},
        {"instId": "BTC-USDT-SWAP", "tradeId": "2", "px": "100", "sz": "1", "side": "buy", "ts": "1000"},
    ]})
    conversion = convert_raw_wire_to_dedup_evidence([{
        "venue": "OKX", "market_type": "linear_perpetual", "local_receive_ts": 1000, "payload": payload,
    }])
    assert len(conversion.records) == 2
    assert analyze_dedup_evidence(conversion.records).duplicate_count == 0


def test_dedup_bypass_helper_produces_more_events_than_production_normalize():
    """Direct proof of the architectural boundary itself: calling the
    real adapter.normalize() (production, dedup-active) on this same
    payload returns fewer events than the forensic bypass path -- the
    exact defect this module exists to avoid, demonstrated by diffing
    against the thing being bypassed rather than only testing the
    bypass path in isolation."""
    from collector.collector.adapters.okx import OKXAdapter
    from collector.collector.dedup_evidence_analysis import _dedup_bypassed_normalize

    raw = {"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [
        {"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "100", "sz": "1", "side": "buy", "ts": "1000"},
        {"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "100", "sz": "1", "side": "buy", "ts": "1000"},
    ]}
    production = OKXAdapter().normalize(dict(raw), local_receive_ts=1000)
    bypassed = _dedup_bypassed_normalize(OKXAdapter(), dict(raw), local_receive_ts=1000)
    assert len(production) == 1, "sanity check: production dedup IS active on this path"
    assert len(bypassed) == 2, "the forensic bypass must not be subject to the same suppression"


def test_dedup_bypass_helper_fails_loudly_without_wrapped_rather_than_falling_back():
    """Critical Issue #1's explicit requirement: a future/mock adapter
    lacking __wrapped__ must raise, never silently fall back to the
    dedup-active adapter.normalize()."""
    import pytest as _pytest
    from collector.collector.dedup_evidence_analysis import DedupBypassUnavailableError, _dedup_bypassed_normalize

    class _FakeAdapterWithoutWrapped:
        def normalize(self, raw, *, local_receive_ts=None):
            return []

    with _pytest.raises(DedupBypassUnavailableError):
        _dedup_bypassed_normalize(_FakeAdapterWithoutWrapped(), {}, local_receive_ts=0)


def test_fabricated_receive_ts_placeholder_never_leaks_into_evidence():
    """Critical Issue #3: when local_receive_ts is invalid, the converter
    must pass a placeholder to the parser (a type it requires) but the
    record's own local_receive_ts must retain the ORIGINAL invalid value,
    never the placeholder -- proven for every invalid-type case, not just
    asserted in a comment."""
    payload = '{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1000,"T":1000,"a":1,"p":"100","q":"0.1","m":false}}'
    for bad_value in (None, True, "not-an-int", -5):
        conversion = convert_raw_wire_to_dedup_evidence([{
            "venue": "BINANCE", "market_type": "linear_perpetual",
            "local_receive_ts": bad_value, "payload": payload,
        }])
        assert len(conversion.records) == 1
        record = conversion.records[0]
        assert record.local_receive_ts == bad_value, \
            f"expected the original invalid value {bad_value!r} preserved, got {record.local_receive_ts!r}"
        assert record.local_receive_ts != 0 or bad_value == 0
        # And it correctly flows through to the analyzer as invalid, not
        # as a silently-accepted timestamp of 0.
        report = analyze_dedup_evidence(conversion.records)
        assert report.invalid_local_receive_ts_count == 1


def test_invalid_timestamp_record_still_participates_in_identity_collision_detection():
    """Critical Issue #4: an invalid-timestamp record must not be invisible
    to identity-collision detection just because its timing can't be used.
    Covers combination 1 (invalid+valid) and 2 (invalid+invalid)."""
    invalid_plus_valid = [
        _rec("BINANCE", "linear_perpetual", "trades", "1", None),    # invalid timestamp
        _rec("BINANCE", "linear_perpetual", "trades", "1", 1000),    # valid, same identity
    ]
    report = analyze_dedup_evidence(invalid_plus_valid)
    assert report.invalid_timestamp_identity_collisions == 1
    assert report.duplicate_count == 0, "cannot be timed (only one valid timestamp exists) -- not a scored duplicate"

    invalid_plus_invalid = [
        _rec("BINANCE", "linear_perpetual", "trades", "2", None),
        _rec("BINANCE", "linear_perpetual", "trades", "2", True),
    ]
    report2 = analyze_dedup_evidence(invalid_plus_invalid)
    assert report2.invalid_timestamp_identity_collisions == 1
    assert report2.duplicate_count == 0


def test_invalid_timestamp_unique_identity_is_not_a_collision():
    """Combination 3: a single invalid-timestamp record with no other
    occurrence of its identity is not a collision of any kind."""
    report = analyze_dedup_evidence([_rec("BINANCE", "linear_perpetual", "trades", "solo", None)])
    assert report.invalid_timestamp_identity_collisions == 0
    assert report.duplicate_count == 0
    assert report.invalid_local_receive_ts_count == 1


def test_valid_and_invalid_records_with_same_id_but_missing_id_are_independent_dimensions():
    """Critical Issue #5: missing_id and invalid_local_receive_ts are
    independent dimensions of one record -- both counted when both apply,
    never hidden behind an early continue that only checks one."""
    both_wrong = _rec("BINANCE", "linear_perpetual", "trades", None, None)
    report = analyze_dedup_evidence([both_wrong])
    assert report.missing_id_count == 1
    assert report.invalid_local_receive_ts_count == 1


def test_three_valid_plus_one_invalid_same_identity_reports_both_dimensions():
    """Combination 8: multiple valid + invalid records sharing an
    identity -- duplicate_count from the valid pair AND
    invalid_timestamp_identity_collisions from the invalid one, not
    mutually exclusive."""
    records = [
        _rec("BINANCE", "linear_perpetual", "trades", "z", 1000),
        _rec("BINANCE", "linear_perpetual", "trades", "z", 1010),
        _rec("BINANCE", "linear_perpetual", "trades", "z", None),
    ]
    report = analyze_dedup_evidence(records)
    assert report.duplicate_count == 1
    assert report.invalid_timestamp_identity_collisions == 1


def test_keep_raw_payload_false_drops_payload_but_keeps_hash():
    payload = '{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1000,"T":1000,"a":3,"p":"100","q":"0.1","m":false}}'
    conversion = convert_raw_wire_to_dedup_evidence(
        [{"venue": "BINANCE", "market_type": "linear_perpetual", "local_receive_ts": 1000, "payload": payload}],
        keep_raw_payload=False,
    )
    record = conversion.records[0]
    assert record.raw_payload is None
    assert record.raw_payload_sha256 == hashlib.sha256(payload.encode("utf-8")).hexdigest()



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


# ---------------------------------------------------------------------------
# Hostile-audit findings, this session (fresh-adapter-per-row invariant;
# invalid-timestamp records must not silently hide a real duplicate).
# ---------------------------------------------------------------------------


def test_fresh_adapter_per_row_means_dedup_state_never_accumulates():
    """Pins the safety mechanism explicitly, beyond the pre-existing
    2-row test: three separate identical frames across three rows must
    all survive as forensic records. If adapter construction were ever
    hoisted out of the per-row loop (a natural-looking performance
    'optimization'), this would immediately regress to 1 record."""
    payload = '{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1000,"T":1000,"a":42,"p":"100","q":"0.1","m":false}}'
    rows = [{"venue": "BINANCE", "market_type": "linear_perpetual", "local_receive_ts": 1000 + i,
            "payload": payload} for i in range(3)]
    conversion = convert_raw_wire_to_dedup_evidence(rows)
    assert len(conversion.records) == 3
    report = analyze_dedup_evidence(conversion.records)
    assert report.duplicate_count == 2   # 2 duplicates of the 1 original, not "already deduped to 1"


def test_invalid_timestamp_record_sharing_an_identity_is_flagged_not_silently_hidden():
    """Hostile-audit finding: excluding an invalid-timestamp record from
    duplicate grouping entirely would let a real duplicate go completely
    unreported if the FIRST delivery happened to have a corrupted
    timestamp. This must surface as invalid_timestamp_identity_collisions,
    never silently absorbed into '1 unique trade, 0 duplicates'."""
    records = [
        _rec("BINANCE", "linear_perpetual", "trades", "x1", None),      # invalid ts, same identity
        _rec("BINANCE", "linear_perpetual", "trades", "x1", 1000),      # valid ts, same identity
    ]
    report = analyze_dedup_evidence(records)
    assert report.invalid_timestamp_identity_collisions == 1
    assert report.invalid_timestamp_identity_collision_examples[0]["trade_id"] == "x1"


def test_invalid_timestamp_record_with_a_unique_identity_is_not_flagged_as_a_collision():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "unique-1", None)]
    report = analyze_dedup_evidence(records)
    assert report.invalid_timestamp_identity_collisions == 0
    assert report.invalid_local_receive_ts_count == 1


# ---------------------------------------------------------------------------
# Additional coverage from a third parallel audit pass on this same PR.
# The dedup-bypass and invalid-timestamp-collision fixes above were already
# correctly implemented and merged (a319571 + 0e2a09d) by the time this pass
# ran; verified empirically rather than re-implemented. These tests add
# coverage those commits did not include: OKX same-frame preservation,
# the DedupBypassUnavailableError propagating through the full public
# convert_raw_wire_to_dedup_evidence() API (not just the internal helper),
# purity/no-mutation, input-order determinism, an exhaustive missing-ID x
# invalid-timestamp independence matrix, and Bybit's specific
# exchange_event_ts-vs-exchange_transaction_ts limitation.
# ---------------------------------------------------------------------------

def test_bybit_ordering_report_is_never_applicable_even_with_sortable_looking_uuids():
    a = _rec("BYBIT", "linear_perpetual", "trades", "00000000-0000-0000-0000-000000000001", 1000,
             instrument_key="BYBIT|linear_perpetual|BTC-USDT|BTCUSDT")
    b = _rec("BYBIT", "linear_perpetual", "trades", "00000000-0000-0000-0000-000000000002", 2000,
             instrument_key="BYBIT|linear_perpetual|BTC-USDT|BTCUSDT")
    report = analyze_dedup_evidence([a, b])
    key = ("BYBIT", "linear_perpetual", "BYBIT|linear_perpetual|BTC-USDT|BTCUSDT", "trades")
    assert report.ordering_by_stream[key].applicable is False


# ---------------------------------------------------------------------------
# PHASE 13: purity -- the analyzer and converter must not mutate inputs.
# ---------------------------------------------------------------------------


def test_bybit_same_frame_shares_one_exchange_event_ts_but_transaction_ts_differs():
    """Documents the real, empirically-found limitation: Bybit's per-trade
    timestamp (`T`) is NOT currently compared for conflict detection --
    only the frame-level `ts` (exchange_event_ts) is. Two same-ID Bybit
    trades with different per-trade `T` but otherwise identical fields are
    therefore classified EXACT_DUPLICATE, not IDENTITY_PAYLOAD_CONFLICT,
    because the field that actually differs (exchange_transaction_ts) is
    not part of the comparison. This is stated, not silently accepted as
    correct -- see the design doc's limitations section."""
    import json

    frame = json.dumps({"topic": "publicTrade.BTCUSDT", "ts": 1, "type": "snapshot", "data": [
        {"T": 100, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "100", "i": "x"},
        {"T": 200, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "100", "i": "x"},
    ]})
    row = {"venue": "BYBIT", "market_type": "linear_perpetual", "payload": frame, "local_receive_ts": 1}
    conversion = convert_raw_wire_to_dedup_evidence([row])
    report = analyze_dedup_evidence(conversion.records)
    # Documents current behavior precisely, including its limitation.
    assert report.exact_duplicate_count == 1
    assert report.identity_payload_conflict_count == 0


def test_bypassed_normalize_does_not_suppress_a_same_adapter_repeat():
    """The actual fix: the bypass path must NOT reproduce the suppression
    the previous test proves the naive path has."""
    from collector.collector.adapters.binance import BinanceAdapter
    from collector.collector.dedup_evidence_analysis import _dedup_bypassed_normalize

    adapter = BinanceAdapter()
    frame = {"stream": "btcusdt@aggTrade",
             "data": {"e": "aggTrade", "E": 1, "T": 1, "a": 42, "p": "100", "q": "1", "m": False}}
    first = _dedup_bypassed_normalize(adapter, frame, local_receive_ts=1)
    second = _dedup_bypassed_normalize(adapter, frame, local_receive_ts=2)
    assert len(first) == 1
    assert len(second) == 1, "bypassed path must preserve the repeat as forensic evidence"


def test_converter_does_not_mutate_the_input_rows():
    import json

    frame = json.dumps({"stream": "btcusdt@aggTrade",
                        "data": {"e": "aggTrade", "E": 1, "T": 1, "a": 1, "p": "100", "q": "1", "m": False}})
    row = {"venue": "BINANCE", "market_type": "linear_perpetual", "payload": frame, "local_receive_ts": 1}
    row_copy = dict(row)
    convert_raw_wire_to_dedup_evidence([row])
    assert row == row_copy


# ---------------------------------------------------------------------------
# PHASE 14: hostile mutation tests. Each mutates the REAL source, runs a
# targeted test, records the failure, then restores byte-identically.
# ---------------------------------------------------------------------------


def test_dedup_bypass_unavailable_error_fires_when_wrapped_is_missing():
    from collector.collector.dedup_evidence_analysis import (
        DedupBypassUnavailableError,
        _dedup_bypassed_normalize,
    )

    class _FakeAdapterWithoutWrapping:
        def normalize(self, raw, *, local_receive_ts=None):
            return []

        def _stamp_instrument(self, events):
            return events

    try:
        _dedup_bypassed_normalize(_FakeAdapterWithoutWrapping(), {}, local_receive_ts=1)
        assert False, "must raise, never silently proceed"
    except DedupBypassUnavailableError:
        pass


def test_dedup_bypass_unavailable_error_is_never_swallowed_by_the_converter():
    """The converter's broad except must not catch DedupBypassUnavailableError
    and turn a real architectural failure into ordinary MALFORMED_PAYLOAD
    evidence -- that would hide the exact failure mode this module exists
    to surface loudly."""
    import json

    from collector.collector import dedup_evidence_analysis as mod

    class _BrokenAdapter:
        def normalize(self, raw, *, local_receive_ts=None):
            return []
        def _stamp_instrument(self, events):
            return events

    original = mod._adapter_for
    mod._adapter_for = lambda venue, market_type: _BrokenAdapter()
    try:
        row = {"venue": "BINANCE", "market_type": "linear_perpetual",
               "payload": json.dumps({"stream": "x", "data": {}}), "local_receive_ts": 1}
        try:
            mod.convert_raw_wire_to_dedup_evidence([row])
            assert False, "DedupBypassUnavailableError must propagate, not be caught"
        except mod.DedupBypassUnavailableError:
            pass
    finally:
        mod._adapter_for = original


def test_duplicate_count_zero_does_not_imply_no_identity_collisions():
    """The exact false inference the task warns against."""
    a = _rec("BINANCE", "linear_perpetual", "trades", "x", 1000)
    b = _rec("BINANCE", "linear_perpetual", "trades", "x", None)
    report = analyze_dedup_evidence([a, b])
    assert report.duplicate_count == 0
    assert report.invalid_timestamp_identity_collisions > 0, (
        "duplicate_count == 0 here must NOT be read as 'no identity collisions'"
    )


# ---------------------------------------------------------------------------
# PHASE 8: equal timestamps are arrival-order-ambiguous, not proof of order.
# ---------------------------------------------------------------------------


def test_invalid_local_receive_ts_does_not_leak_into_duplicate_delay_math():
    """A record with an invalid timestamp must never be silently paired
    against a valid one to compute a fabricated delay."""
    good = _rec("BINANCE", "linear_perpetual", "trades", "same-id", 1000)
    bad = _rec("BINANCE", "linear_perpetual", "trades", "same-id", None)
    report = analyze_dedup_evidence([good, bad])
    assert report.duplicate_count == 0, "an invalid-timestamp member must not enter the timed duplicate path"
    assert report.invalid_timestamp_identity_collisions == 1


# ---------------------------------------------------------------------------
# PHASE 4: missing-ID and invalid-timestamp accounting are independent.
# ---------------------------------------------------------------------------


def test_invalid_local_receive_ts_is_never_replaced_with_zero_sentinel():
    """0 is a VALID millisecond timestamp (the Unix epoch). Silently
    substituting it for an invalid input would be indistinguishable from a
    genuine (if absurd) observation of the epoch -- fabricated evidence."""
    import json

    frame = json.dumps({"stream": "btcusdt@aggTrade",
                        "data": {"e": "aggTrade", "E": 1, "T": 1, "a": 1, "p": "100", "q": "1", "m": False}})
    for bad_value in (None, True, "not-an-int", -5):
        row = {"venue": "BINANCE", "market_type": "linear_perpetual", "payload": frame,
               "local_receive_ts": bad_value}
        conversion = convert_raw_wire_to_dedup_evidence([row])
        assert len(conversion.records) == 1
        # The ORIGINAL invalid value is preserved verbatim in the evidence
        # record, not silently replaced by 0 or any other stand-in.
        assert conversion.records[0].local_receive_ts == bad_value
        report = analyze_dedup_evidence(conversion.records)
        assert report.invalid_local_receive_ts_count == 1


def test_invalid_plus_invalid_same_identity_is_a_collision():
    a = _rec("BINANCE", "linear_perpetual", "trades", "x", None)
    b = _rec("BINANCE", "linear_perpetual", "trades", "x", "bad")
    report = analyze_dedup_evidence([a, b])
    assert report.duplicate_count == 0
    assert report.invalid_timestamp_identity_collisions == 1


def test_invalid_timestamp_missing_id_is_not_an_identity_collision():
    r = _rec("BINANCE", "linear_perpetual", "trades", None, None)
    report = analyze_dedup_evidence([r])
    assert report.invalid_timestamp_identity_collisions == 0  # trade_id=None is never grouped by identity
    assert report.missing_id_count == 1
    assert report.invalid_local_receive_ts_count == 1


def test_missing_id_and_invalid_timestamp_counts_toward_both():
    """A record may legitimately belong to BOTH missing-ID and
    invalid-timestamp evidence -- no early continue may hide one."""
    r = _rec("BINANCE", "linear_perpetual", "trades", None, None)
    report = analyze_dedup_evidence([r])
    assert report.missing_id_count == 1
    assert report.invalid_local_receive_ts_count == 1


def test_missing_id_and_valid_timestamp_counts_toward_missing_id_only():
    r = _rec("BINANCE", "linear_perpetual", "trades", None, 1000)
    report = analyze_dedup_evidence([r])
    assert report.missing_id_count == 1
    assert report.invalid_local_receive_ts_count == 0


def test_missing_id_with_bool_timestamp_counts_toward_both():
    r = _rec("BINANCE", "linear_perpetual", "trades", None, True)
    report = analyze_dedup_evidence([r])
    assert report.missing_id_count == 1
    assert report.invalid_local_receive_ts_count == 1


def test_missing_id_with_negative_timestamp_counts_toward_both():
    r = _rec("BINANCE", "linear_perpetual", "trades", None, -1)
    report = analyze_dedup_evidence([r])
    assert report.missing_id_count == 1
    assert report.invalid_local_receive_ts_count == 1


# ---------------------------------------------------------------------------
# PHASE 5: invalid-timestamp identity collisions are reported independently
# of duplicate_count, and duplicate_count == 0 must never be read as
# "no identity collisions" when this is > 0.
# ---------------------------------------------------------------------------


def test_missing_id_with_string_timestamp_counts_toward_both():
    r = _rec("BINANCE", "linear_perpetual", "trades", None, "bad")
    report = analyze_dedup_evidence([r])
    assert report.missing_id_count == 1
    assert report.invalid_local_receive_ts_count == 1


def test_multiple_valid_plus_one_invalid_same_identity_is_one_collision():
    a = _rec("BINANCE", "linear_perpetual", "trades", "x", 1000)
    b = _rec("BINANCE", "linear_perpetual", "trades", "x", 2000)
    c = _rec("BINANCE", "linear_perpetual", "trades", "x", None)
    report = analyze_dedup_evidence([a, b, c])
    assert report.duplicate_count == 1  # a,b timed pair still computed normally
    assert report.invalid_timestamp_identity_collisions == 1  # collision noted separately


def test_mutation_removing_instrument_from_identity_would_be_caught():
    records = [_rec("BINANCE", "linear_perpetual", "trades", "x", 1000,
                    instrument_key="BINANCE|linear_perpetual|BTC-USDT|BTCUSDT"),
               _rec("BINANCE", "linear_perpetual", "trades", "x", 1010,
                    instrument_key="BINANCE|linear_perpetual|BTC-USDT|ETHUSDT")]
    assert analyze_dedup_evidence(records).duplicate_count == 0


def test_naive_normalize_call_would_suppress_a_same_adapter_repeat():
    """Contrast case: proves _dedupe_trades really does fire on the public
    normalize() path, which is exactly why this module must never call it."""
    import json

    from collector.collector.adapters.binance import BinanceAdapter

    adapter = BinanceAdapter()
    frame = {"stream": "btcusdt@aggTrade",
             "data": {"e": "aggTrade", "E": 1, "T": 1, "a": 42, "p": "100", "q": "1", "m": False}}
    first = adapter.normalize(frame, local_receive_ts=1)
    second = adapter.normalize(frame, local_receive_ts=2)
    assert len(first) == 1
    assert len(second) == 0, "production dedup should suppress the identical repeat here"


def test_okx_single_frame_multiple_trades_all_preserved():
    """OKX trades channel: data is a list of trade objects per instId."""
    import json

    frame = json.dumps({"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [
        {"instId": "BTC-USDT-SWAP", "tradeId": "111", "px": "100", "sz": "1", "side": "buy", "ts": "1000"},
        {"instId": "BTC-USDT-SWAP", "tradeId": "111", "px": "101", "sz": "1", "side": "buy", "ts": "1001"},
    ]})
    row = {"venue": "OKX", "market_type": "linear_perpetual", "payload": frame, "local_receive_ts": 1000}
    conversion = convert_raw_wire_to_dedup_evidence([row])
    assert len(conversion.records) == 2, "OKX same-frame duplicate must also survive the converter"


def test_okx_trades_all_single_frame_multiple_trades_preserved():
    import json

    frame = json.dumps({"arg": {"channel": "trades-all", "instId": "BTC-USDT-SWAP"}, "data": [
        {"instId": "BTC-USDT-SWAP", "tradeId": "222", "px": "100", "sz": "1", "side": "buy", "ts": "1000"},
        {"instId": "BTC-USDT-SWAP", "tradeId": "222", "px": "105", "sz": "1", "side": "buy", "ts": "1001"},
    ]})
    row = {"venue": "OKX", "market_type": "linear_perpetual", "payload": frame, "local_receive_ts": 1000}
    conversion = convert_raw_wire_to_dedup_evidence([row])
    assert len(conversion.records) == 2


# ---------------------------------------------------------------------------
# PHASE 3: invalid local_receive_ts must never leak a fabricated value.
# ---------------------------------------------------------------------------


def test_one_valid_plus_multiple_invalid_same_identity_is_one_collision():
    a = _rec("BINANCE", "linear_perpetual", "trades", "x", 1000)
    b = _rec("BINANCE", "linear_perpetual", "trades", "x", None)
    c = _rec("BINANCE", "linear_perpetual", "trades", "x", "bad")
    report = analyze_dedup_evidence([a, b, c])
    assert report.duplicate_count == 0  # only one valid-timed member -- no timed pair exists
    assert report.invalid_timestamp_identity_collisions == 1


def test_reversed_input_order_does_not_change_classification_or_count():
    a = _rec("BINANCE", "linear_perpetual", "trades", "x", 1000, canonical_price=100.0)
    b = _rec("BINANCE", "linear_perpetual", "trades", "x", 1000, canonical_price=100.0)
    forward = analyze_dedup_evidence([a, b])
    backward = analyze_dedup_evidence([b, a])
    assert forward.duplicate_count == backward.duplicate_count == 1
    assert forward.duplicates[0].arrival_order_ambiguous is True
    assert backward.duplicates[0].arrival_order_ambiguous is True


# ---------------------------------------------------------------------------
# PHASE 11: Bybit ordering is never inferred from UUID lexicographic order.
# ---------------------------------------------------------------------------


def test_single_bybit_frame_with_two_same_id_trades_preserves_both():
    """THE regression pin for the actual bug found in this audit: one raw
    payload, one row, two array elements sharing an identity but with
    different prices. Must survive as 2 records classified as an
    IDENTITY_PAYLOAD_CONFLICT once analyzed -- not silently collapse to 1
    inside the converter."""
    import json

    frame = json.dumps({
        "topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": 1000,
        "data": [
            {"T": 1000, "s": "BTCUSDT", "S": "Buy", "v": "0.001", "p": "100", "L": "PlusTick", "i": "dup-id-123"},
            {"T": 1001, "s": "BTCUSDT", "S": "Buy", "v": "0.002", "p": "101", "L": "PlusTick", "i": "dup-id-123"},
        ],
    })
    row = {"venue": "BYBIT", "market_type": "linear_perpetual", "payload": frame, "local_receive_ts": 1000}
    conversion = convert_raw_wire_to_dedup_evidence([row])
    assert len(conversion.records) == 2, (
        "regression: this exact scenario collapsed to 1 record before the "
        "dedup-bypass fix -- the second trade's price-101 evidence was "
        "silently destroyed"
    )
    report = analyze_dedup_evidence(conversion.records)
    assert report.duplicate_count == 1
    assert report.identity_payload_conflict_count == 1
    assert report.exact_duplicate_count == 0


def test_single_frame_same_id_different_exchange_ts_is_a_conflict():
    """Uses OKX, not Bybit: Bybit's per-element `T` maps to
    exchange_transaction_ts, not exchange_event_ts (that comes from the
    frame-level `ts`, shared by every element in one frame) -- see this
    audit's finding in docs/DEDUP_EVIDENCE_EXPERIMENT_DESIGN.md. OKX's
    per-element `ts` does map to exchange_event_ts, which is what this test
    needs to exercise."""
    import json

    frame = json.dumps({"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [
        {"instId": "BTC-USDT-SWAP", "tradeId": "x", "px": "100", "sz": "1", "side": "buy", "ts": "100"},
        {"instId": "BTC-USDT-SWAP", "tradeId": "x", "px": "100", "sz": "1", "side": "buy", "ts": "200"},
    ]})
    row = {"venue": "OKX", "market_type": "linear_perpetual", "payload": frame, "local_receive_ts": 1}
    conversion = convert_raw_wire_to_dedup_evidence([row])
    report = analyze_dedup_evidence(conversion.records)
    assert report.identity_payload_conflict_count == 1


def test_single_frame_same_id_different_quantity_is_a_conflict():
    import json

    frame = json.dumps({"topic": "publicTrade.BTCUSDT", "ts": 1, "type": "snapshot", "data": [
        {"T": 1, "s": "BTCUSDT", "S": "Buy", "v": "0.001", "p": "100", "i": "x"},
        {"T": 2, "s": "BTCUSDT", "S": "Buy", "v": "0.999", "p": "100", "i": "x"},
    ]})
    row = {"venue": "BYBIT", "market_type": "linear_perpetual", "payload": frame, "local_receive_ts": 1}
    conversion = convert_raw_wire_to_dedup_evidence([row])
    report = analyze_dedup_evidence(conversion.records)
    assert report.identity_payload_conflict_count == 1


def test_single_frame_same_id_different_side_is_a_conflict():
    import json

    frame = json.dumps({"topic": "publicTrade.BTCUSDT", "ts": 1, "type": "snapshot", "data": [
        {"T": 1, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "100", "i": "x"},
        {"T": 2, "s": "BTCUSDT", "S": "Sell", "v": "1", "p": "100", "i": "x"},
    ]})
    row = {"venue": "BYBIT", "market_type": "linear_perpetual", "payload": frame, "local_receive_ts": 1}
    conversion = convert_raw_wire_to_dedup_evidence([row])
    report = analyze_dedup_evidence(conversion.records)
    assert report.identity_payload_conflict_count == 1


def test_single_frame_two_distinct_trade_ids_both_preserved():
    import json

    frame = json.dumps({
        "topic": "publicTrade.BTCUSDT", "ts": 1000, "type": "snapshot",
        "data": [
            {"T": 1000, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "100", "i": "id-1"},
            {"T": 1001, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "100", "i": "id-2"},
        ],
    })
    row = {"venue": "BYBIT", "market_type": "linear_perpetual", "payload": frame, "local_receive_ts": 1000}
    conversion = convert_raw_wire_to_dedup_evidence([row])
    assert len(conversion.records) == 2
    assert {r.trade_id for r in conversion.records} == {"id-1", "id-2"}


def test_valid_plus_invalid_same_identity_is_a_collision_not_a_duplicate():
    valid = _rec("BINANCE", "linear_perpetual", "trades", "x", 1000)
    invalid = _rec("BINANCE", "linear_perpetual", "trades", "x", None)
    report = analyze_dedup_evidence([valid, invalid])
    assert report.duplicate_count == 0
    assert report.invalid_timestamp_identity_collisions == 1


def test_valid_plus_valid_same_identity_is_a_duplicate_not_flagged_as_invalid_collision():
    a = _rec("BINANCE", "linear_perpetual", "trades", "x", 1000)
    b = _rec("BINANCE", "linear_perpetual", "trades", "x", 1500)
    report = analyze_dedup_evidence([a, b])
    assert report.duplicate_count == 1
    assert report.invalid_timestamp_identity_collisions == 0


def test_wrapped_attribute_exists_on_every_supported_adapter():
    """Empirical proof, not an assumption: functools.wraps(impl) sets
    __wrapped__ as a documented side effect, and __init_subclass__ has
    actually replaced normalize on each of these four classes."""
    from collector.collector.adapters.binance import BinanceAdapter
    from collector.collector.adapters.binance_spot import BinanceSpotAdapter
    from collector.collector.adapters.bybit import BybitAdapter
    from collector.collector.adapters.okx import OKXAdapter

    for cls in (BinanceAdapter, BinanceSpotAdapter, BybitAdapter, OKXAdapter):
        adapter = cls()
        unwrapped = getattr(adapter.normalize, "__wrapped__", None)
        assert unwrapped is not None, f"{cls.__name__} has no __wrapped__"
        assert callable(unwrapped)
        # It must actually be a *different* callable from the public
        # attribute -- proving the public one really is a wrapper, not
        # __wrapped__ pointing at itself by coincidence.
        assert unwrapped is not adapter.normalize
