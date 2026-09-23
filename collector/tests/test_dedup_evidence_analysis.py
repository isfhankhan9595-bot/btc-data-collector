"""Tests for dedup_evidence_analysis.py.

Every fixture in this file is SYNTHETIC, constructed to exercise specific
arithmetic paths in the analyzer. None of it is presented as, or should be
read as, evidence about real exchange behavior -- these tests prove the
tool computes correctly on known inputs, which is a precondition for it
being useful once real evidence exists, not a substitute for that evidence.
"""
from __future__ import annotations

from collector.collector.dedup_evidence_analysis import (
    DedupEvidenceRecord,
    analyze_dedup_evidence,
)


def _rec(exchange, market_type, stream, trade_id, local_receive_ts, *,
         instrument_key="BINANCE|linear_perpetual|BTC-USDT|BTCUSDT",
         exchange_event_ts=None, connection_id=None, reconnect_marker=None):
    return DedupEvidenceRecord(
        exchange=exchange, market_type=market_type, instrument_key=instrument_key,
        stream=stream, trade_id=trade_id, exchange_event_ts=exchange_event_ts or local_receive_ts,
        local_receive_ts=local_receive_ts, connection_id=connection_id,
        reconnect_marker=reconnect_marker,
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
    records = [_rec("BINANCE", "linear_perpetual", "trades", "1", 1000),
              _rec("BINANCE", "linear_perpetual", "trades", "1", 1250)]
    report = analyze_dedup_evidence(records)
    assert report.duplicate_count == 1
    assert report.duplicates[0].delay_ms == 250
    assert report.delay_min.value == 250 and report.delay_max.value == 250


def test_missing_trade_id_is_never_deduplicated_against_anything():
    """Mirrors _dedupe_trades's own explicitly flagged catastrophic
    failure mode exactly: None must never collapse together."""
    records = [_rec("BINANCE", "linear_perpetual", "trades", None, 1000),
              _rec("BINANCE", "linear_perpetual", "trades", None, 1010),
              _rec("BINANCE", "linear_perpetual", "trades", None, 1020)]
    report = analyze_dedup_evidence(records)
    assert report.duplicate_count == 0


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
