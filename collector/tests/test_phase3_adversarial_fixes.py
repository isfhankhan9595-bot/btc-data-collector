import math

from collector.collector.validator import Validator


def test_trade_rejects_non_finite_price_and_quantity():
    for field, value in (("price", math.nan), ("price", math.inf), ("quantity", math.nan), ("quantity", math.inf)):
        record = {
            "timestamp": 1_000,
            "exchange_timestamp": 1_000,
            "price": 100.0,
            "quantity": 1.0,
            "trade_id": 1,
        }
        record[field] = value
        validator = Validator()
        assert validator.validate_trade(record) == (False, f"Invalid {'price' if field == 'price' else 'quantity'}")


def test_trade_rejects_non_positive_structural_values_but_keeps_large_dislocation():
    validator = Validator()
    assert validator.validate_trade(
        {
            "timestamp": 1_000,
            "exchange_timestamp": 1_000,
            "price": -100.0,
            "quantity": 1.0,
            "trade_id": 1,
        }
    ) == (False, "Invalid price")

    assert validator.validate_trade(
        {
            "timestamp": 2_000,
            "exchange_timestamp": 2_000,
            "price": 10_000_000.0,
            "quantity": 1.0,
            "trade_id": 2,
        }
    ) == (True, "")


def test_markprice_rejects_non_finite_funding_without_censoring_extremes():
    validator = Validator()
    assert validator.validate_markprice(
        {
            "timestamp": 1_000,
            "exchange_timestamp": 1_000,
            "mark_price": 100.0,
            "funding_rate": math.inf,
            "next_funding_time": 2_000,
        }
    ) == (False, "Invalid funding rate")

    assert validator.validate_markprice(
        {
            "timestamp": 2_000,
            "exchange_timestamp": 2_000,
            "mark_price": 100.0,
            "funding_rate": 0.5,
            "next_funding_time": 3_000,
        }
    ) == (True, "")


def test_large_clock_drift_is_flagged_but_event_is_preserved():
    validator = Validator()
    record = {
        "timestamp": 1_000_000,
        "exchange_timestamp": 970_000,
        "price": 100.0,
        "quantity": 1.0,
        "trade_id": 1,
    }
    assert validator.validate_trade(record) == (True, "")

    events = validator.drain_quality_events()
    assert len(events) == 1
    assert events[0].event_type.value == "CLOCK_ANOMALY"
    assert events[0].gap_size_ms == 30_000
    assert events[0].local_ts == 1_000_000
    assert events[0].reason == "clock_drift"
