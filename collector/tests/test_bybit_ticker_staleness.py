"""Adversarial tests for Bybit ticker staleness (D12).

The defect: each ticker delta was merged into shared state and then the
*merged* dict was tested for mark/index/funding keys, so almost every
message emitted a mark-price event filled with carried-forward values.
Downstream, a funding rate last seen minutes ago was indistinguishable from
one observed now.
"""
from __future__ import annotations

import pytest

from collector.collector.adapters.base import UnhandledReason
from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.canonical import CanonicalMarkPriceEvent, CanonicalOIEvent


def _ticker(ts, data, kind="delta"):
    return {"topic": "tickers.BTCUSDT", "type": kind, "ts": ts, "data": data}


def _snapshot(ts, **fields):
    return _ticker(ts, {"symbol": "BTCUSDT", **fields}, kind="snapshot")


# ---------------------------------------------------------------------------
# The defect
# ---------------------------------------------------------------------------


def test_delta_carrying_no_surfaced_field_emits_no_markprice_event():
    """Previously this republished stale mark/funding values as fresh."""
    adapter = BybitAdapter()
    adapter.normalize(_snapshot(1000, markPrice="100", fundingRate="0.0001",
                                openInterest="500"))

    # A delta about something this adapter does not surface.
    events = adapter.normalize(_ticker(2000, {"volume24h": "123"}))

    assert events == []
    unhandled = adapter.drain_unhandled()
    assert len(unhandled) == 1
    assert unhandled[0].reason is UnhandledReason.EMPTY_DATA
    assert "carried_no_surfaced_field" in (unhandled[0].detail or "")


def test_a_fresh_field_is_not_marked_carried_forward():
    adapter = BybitAdapter()
    adapter.normalize(_snapshot(1000, markPrice="100"))
    event = adapter.normalize(_ticker(2000, {"markPrice": "101"}))[0]

    assert event.mark_price == 101.0
    assert event.is_carried_forward("markPrice") is False
    assert event.age_of("markPrice") == 0


def test_a_carried_forward_field_is_marked_and_aged():
    adapter = BybitAdapter()
    adapter.normalize(_snapshot(1000, markPrice="100", fundingRate="0.0001"))
    # Only markPrice changes; fundingRate is carried forward from t=1000.
    event = adapter.normalize(_ticker(9000, {"markPrice": "105"}))[0]

    assert event.mark_price == 105.0
    assert event.funding_rate == 0.0001
    assert event.is_carried_forward("fundingRate") is True
    assert event.is_carried_forward("markPrice") is False
    assert event.age_of("fundingRate") == 8000
    assert event.age_of("markPrice") == 0


def test_staleness_accumulates_across_many_deltas():
    adapter = BybitAdapter()
    adapter.normalize(_snapshot(0, markPrice="100", fundingRate="0.0001"))
    last = None
    for ts in range(1000, 11000, 1000):
        result = adapter.normalize(_ticker(ts, {"markPrice": str(100 + ts)}))
        if result:
            last = result[0]
    assert last.age_of("fundingRate") == 10_000
    assert last.is_carried_forward("fundingRate") is True


def test_never_observed_field_is_absent_not_zero_age():
    """Absence must not be reported as a fresh observation."""
    adapter = BybitAdapter()
    event = adapter.normalize(_snapshot(1000, markPrice="100"))[0]

    assert event.funding_rate is None
    assert event.age_of("fundingRate") is None
    assert "fundingRate" not in event.carried_forward


# ---------------------------------------------------------------------------
# Snapshot semantics
# ---------------------------------------------------------------------------


def test_snapshot_replaces_state_rather_than_merging():
    adapter = BybitAdapter()
    adapter.normalize(_snapshot(1000, markPrice="100", fundingRate="0.0001"))
    # A later snapshot omits fundingRate; it must not survive.
    event = adapter.normalize(_snapshot(5000, markPrice="200"))[0]

    assert event.mark_price == 200.0
    assert event.funding_rate is None
    assert event.age_of("fundingRate") is None


def test_snapshot_resets_accumulated_ages():
    adapter = BybitAdapter()
    adapter.normalize(_snapshot(0, markPrice="100", fundingRate="0.0001"))
    adapter.normalize(_ticker(9000, {"markPrice": "105"}))
    event = adapter.normalize(_snapshot(10_000, markPrice="110", fundingRate="0.0002"))[0]

    assert event.age_of("fundingRate") == 0
    assert event.carried_forward == ()


# ---------------------------------------------------------------------------
# Open interest is tracked independently of mark price
# ---------------------------------------------------------------------------


def test_open_interest_delta_does_not_emit_a_markprice_event():
    adapter = BybitAdapter()
    adapter.normalize(_snapshot(1000, markPrice="100", openInterest="500"))
    events = adapter.normalize(_ticker(2000, {"openInterest": "600"}))

    assert len(events) == 1
    assert isinstance(events[0], CanonicalOIEvent)
    assert events[0].open_interest == 600.0
    assert events[0].is_carried_forward("openInterest") is False


def test_markprice_delta_does_not_emit_an_oi_event():
    adapter = BybitAdapter()
    adapter.normalize(_snapshot(1000, markPrice="100", openInterest="500"))
    events = adapter.normalize(_ticker(2000, {"markPrice": "101"}))

    assert len(events) == 1
    assert isinstance(events[0], CanonicalMarkPriceEvent)


def test_a_snapshot_carrying_both_emits_both():
    adapter = BybitAdapter()
    events = adapter.normalize(_snapshot(1000, markPrice="100", openInterest="500"))
    assert {type(e) for e in events} == {CanonicalMarkPriceEvent, CanonicalOIEvent}


# ---------------------------------------------------------------------------
# Timestamps and hygiene
# ---------------------------------------------------------------------------


def test_missing_venue_timestamp_does_not_fabricate_an_age():
    adapter = BybitAdapter()
    raw = {"topic": "tickers.BTCUSDT", "type": "snapshot", "data": {"markPrice": "100"}}
    event = adapter.normalize(raw, local_receive_ts=42)[0]

    assert event.exchange_event_ts is None
    assert event.local_receive_ts == 42
    assert event.field_age_ms == ()  # no venue clock, so no age claimed


def test_out_of_order_ticker_never_produces_a_negative_age():
    adapter = BybitAdapter()
    adapter.normalize(_snapshot(10_000, markPrice="100", fundingRate="0.0001"))
    event = adapter.normalize(_ticker(5_000, {"markPrice": "101"}))[0]
    assert event.age_of("fundingRate") == 0


def test_other_bybit_routes_are_unaffected():
    adapter = BybitAdapter()
    trades = adapter.normalize(
        {"topic": "publicTrade.BTCUSDT", "ts": 1,
         "data": [{"i": "7", "p": "100", "v": "1", "S": "Buy", "T": 1}]})
    book = adapter.normalize(
        {"topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": 1,
         "data": {"b": [["100", "1"]], "a": [["101", "1"]], "u": 5, "seq": 9}})
    liq = adapter.normalize(
        {"topic": "allLiquidation.BTCUSDT", "ts": 1,
         "data": [{"S": "Buy", "p": "100", "v": "1", "T": 1}]})
    assert len(trades) == len(book) == len(liq) == 1
    assert adapter.drain_unhandled() == []


def test_control_and_unrouted_frames_still_classified():
    adapter = BybitAdapter()
    adapter.normalize({"op": "subscribe", "success": True})
    adapter.normalize({"topic": "unknown.BTCUSDT", "data": {}})
    reasons = [m.reason for m in adapter.drain_unhandled()]
    assert UnhandledReason.CONTROL_FRAME in reasons
    assert UnhandledReason.NO_ROUTE in reasons


def test_adapters_do_not_share_ticker_state():
    a, b = BybitAdapter(), BybitAdapter()
    a.normalize(_snapshot(1000, markPrice="100"))
    assert b.normalize(_ticker(2000, {"volume24h": "1"})) == []
    assert b._ticker_state == {"symbol": "BTCUSDT", "volume24h": "1"} or b._ticker_state
    # b never saw a markPrice, so it cannot report one.
    assert b._ticker_state.get("markPrice") is None
