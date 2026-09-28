import pytest
from collector.collector.feature_computer import compute_orderbook_features, compute_trades_features, compute_markprice_features, compute_openinterest_features, compute_liquidation_features

def test_compute_orderbook_features():
    msg = {
        "E": 1234567890,
        "b": [[str(100.0 - i), str(1.0)] for i in range(10)],
        "a": [[str(101.0 + i), str(2.0)] for i in range(10)]
    }

    features = compute_orderbook_features(msg)

    assert features["exchange_timestamp"] == 1234567890
    assert features["best_bid"] == 100.0
    assert features["best_ask"] == 101.0
    assert features["mid_price"] == 100.5
    assert features["spread"] == 1.0
    assert abs(features["spread_bps"] - (1.0 / 100.5 * 10000)) < 1e-5
    assert features["total_bid_qty"] == 10.0
    assert features["total_ask_qty"] == 20.0
    assert abs(features["obi"] - (-10.0 / 30.0)) < 1e-5
    # P0-8: with exactly 10 real levels each side, semantics are unchanged --
    # full depth, both level-3 and level-5 OBI are valid observations.
    assert features["bid_depth"] == 10
    assert features["ask_depth"] == 10
    assert features["obi_level_3"] is not None
    assert features["obi_level_5"] is not None

def test_compute_orderbook_features_invalid():
    msg = {"b": [], "a": [["101.0", "2.0"]]}
    features = compute_orderbook_features(msg)
    assert not features


def test_compute_orderbook_features_never_pads_missing_bid_levels():
    """P0-8: with only 5 real bid levels, the output must contain exactly
    5 real bid entries -- never padded to 10 by repeating the last real
    price with a fabricated zero quantity."""
    msg = {
        "E": 1234567890,
        "b": [[str(100.0 - i), str(1.0 + i)] for i in range(5)],
        "a": [[str(101.0 + i), str(2.0)] for i in range(10)],
    }

    features = compute_orderbook_features(msg)

    assert features
    assert features["bids_price"] == [100.0, 99.0, 98.0, 97.0, 96.0]
    assert features["bids_qty"] == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert len(features["bids_price"]) == 5
    assert len(features["bids_qty"]) == 5
    assert features["bid_depth"] == 5
    assert features["ask_depth"] == 10
    # 5 real bid levels + 10 real ask levels satisfies level-5 (>=5 on both
    # sides), so it remains a valid observation here.
    assert features["obi_level_5"] is not None
    assert features["obi_level_3"] is not None


def test_compute_orderbook_features_accepts_single_level_and_computes_obi():
    """P0-8: with exactly 1 real level on each side, the output must
    contain exactly 1 entry per side -- never 10. obi_level_1 remains a
    truthful observation (1 real level is enough for level-1); obi_level_3
    and obi_level_5 must be None, since no fabricated levels 2-10 exist to
    compute them from."""
    msg = {
        "E": 1234567890,
        "b": [["100.0", "3.0"]],
        "a": [["101.0", "1.0"]],
    }

    features = compute_orderbook_features(msg)

    assert features
    assert features["bids_price"] == [100.0]
    assert features["asks_price"] == [101.0]
    assert features["bids_qty"] == [3.0]
    assert features["asks_qty"] == [1.0]
    assert features["bid_depth"] == 1
    assert features["ask_depth"] == 1
    assert features["obi"] == 0.5
    assert features["obi_level_1"] == 0.5
    assert features["obi_level_3"] is None
    assert features["obi_level_5"] is None


def test_compute_orderbook_features_rejects_zero_bids():
    msg = {"b": [], "a": [["101.0", "2.0"] for _ in range(10)]}
    features = compute_orderbook_features(msg)
    assert features == {}


# ---------------------------------------------------------------------------
# P0-8: eliminate fabricated order-book levels -- full adversarial suite.
# ---------------------------------------------------------------------------

def test_p0_8_fewer_than_10_ask_levels_never_fabricated():
    msg = {
        "E": 1,
        "b": [[str(100.0 - i), "1.0"] for i in range(10)],
        "a": [[str(101.0 + i), "1.0"] for i in range(4)],
    }
    features = compute_orderbook_features(msg)
    assert features["asks_price"] == [101.0, 102.0, 103.0, 104.0]
    assert features["ask_depth"] == 4
    assert features["bid_depth"] == 10
    # asks has only 4 real levels -> level-5 is invalid on the ask side too,
    # even though bids alone would have enough.
    assert features["obi_level_5"] is None
    assert features["obi_level_3"] is not None


def test_p0_8_missing_level_3_makes_obi_level_3_none_not_partial():
    """Exactly 2 real levels on each side: obi_level_3 must be None, never
    silently computed over the 2 levels that do exist under the level-3 name."""
    msg = {
        "E": 1,
        "b": [["100.0", "1.0"], ["99.0", "2.0"]],
        "a": [["101.0", "1.0"], ["102.0", "2.0"]],
    }
    features = compute_orderbook_features(msg)
    assert features["bid_depth"] == 2
    assert features["ask_depth"] == 2
    assert features["obi_level_3"] is None
    assert features["obi_level_5"] is None
    # Level-1 remains valid; it only needs 1 real level, which exists.
    assert features["obi_level_1"] is not None


def test_p0_8_missing_level_5_asymmetric_depth():
    """4 real bid levels, 10 real ask levels: level-5 needs 5 on BOTH
    sides, so it must be None even though the ask side alone has enough."""
    msg = {
        "E": 1,
        "b": [[str(100.0 - i), "1.0"] for i in range(4)],
        "a": [[str(101.0 + i), "1.0"] for i in range(10)],
    }
    features = compute_orderbook_features(msg)
    assert features["bid_depth"] == 4
    assert features["ask_depth"] == 10
    assert features["obi_level_5"] is None
    assert features["obi_level_3"] is not None  # 4 >= 3 on both sides


def test_p0_8_duplicate_prices_are_not_reinterpreted():
    """Two input rows at the identical price are passed through faithfully
    as two distinct array entries -- never merged, deduplicated, or used
    to infer a different real depth than what was actually given."""
    msg = {
        "E": 1,
        "b": [["100.0", "1.0"], ["100.0", "2.0"], ["99.0", "1.0"]],
        "a": [["101.0", "1.0"]],
    }
    features = compute_orderbook_features(msg)
    assert features["bids_price"] == [100.0, 100.0, 99.0]
    assert features["bids_qty"] == [1.0, 2.0, 1.0]
    assert features["bid_depth"] == 3


def test_p0_8_zero_quantity_real_level_is_distinct_from_missing_level():
    """A level explicitly present in the input with quantity 0 is a real
    array entry (occupying a real slot, counted in bid_depth) -- NOT the
    same thing as a level that is simply absent from the array."""
    msg = {
        "E": 1,
        "b": [["100.0", "1.0"], ["99.0", "0.0"]],
        "a": [["101.0", "1.0"]],
    }
    features = compute_orderbook_features(msg)
    assert features["bids_price"] == [100.0, 99.0]
    assert features["bids_qty"] == [1.0, 0.0]
    assert features["bid_depth"] == 2  # the zero-qty row is still a real, present level
    assert len(features["bids_price"]) == features["bid_depth"]


def test_p0_8_empty_asks_yields_no_fabricated_book():
    msg = {"E": 1, "b": [["100.0", "1.0"]], "a": []}
    features = compute_orderbook_features(msg)
    assert features == {}


def test_p0_8_malformed_input_yields_no_fabricated_values():
    msg = {"E": 1, "b": [["not_a_number", "1.0"]], "a": [["101.0", "1.0"]]}
    features = compute_orderbook_features(msg)
    assert features == {}


def test_p0_8_best_bid_ask_are_from_real_evidence_only():
    """With only 1 real level, best_bid/best_ask are exactly that one real
    price -- never a fabricated or forward-filled value."""
    msg = {"E": 1, "b": [["100.0", "1.0"]], "a": [["101.0", "1.0"]]}
    features = compute_orderbook_features(msg)
    assert features["best_bid"] == 100.0
    assert features["best_ask"] == 101.0
    assert features["mid_price"] == 100.5


def test_p0_8_more_than_10_real_levels_truncates_without_fabricating():
    """15 real levels on each side: truncated to the top 10 (nearest to the
    touch), and bid_depth/ask_depth reflect that truncation -- 10, not 15,
    since only 10 are carried in the array, and none of those 10 are
    fabricated."""
    msg = {
        "E": 1,
        "b": [[str(100.0 - i), "1.0"] for i in range(15)],
        "a": [[str(101.0 + i), "1.0"] for i in range(15)],
    }
    features = compute_orderbook_features(msg)
    assert len(features["bids_price"]) == 10
    assert features["bid_depth"] == 10
    assert features["bids_price"][-1] == 91.0  # the 10th real level, not fabricated
    assert features["obi_level_5"] is not None

def test_compute_trades_features():
    msg = {
        "E": 1234567890,
        "a": 123,
        "p": "100.0",
        "q": "1.5",
        "m": True # buyer is maker -> seller is taker -> side_sign = -1
    }

    features = compute_trades_features(msg)

    assert features["trade_id"] == 123
    assert features["price"] == 100.0
    assert features["quantity"] == 1.5
    assert features["side_sign"] == -1
    assert features["signed_qty"] == -1.5


def test_compute_trades_features_prefers_trade_time_over_event_time():
    msg = {
        "T": 1234567000,
        "E": 1234567890,
        "a": 123,
        "p": "100.0",
        "q": "1.5",
        "m": True
    }

    features = compute_trades_features(msg)

    assert features["exchange_timestamp"] == 1234567000


def test_compute_trades_features_falls_back_to_event_time_without_trade_time():
    msg = {
        "E": 1234567890,
        "a": 123,
        "p": "100.0",
        "q": "1.5",
        "m": True
    }

    features = compute_trades_features(msg)

    assert features["exchange_timestamp"] == 1234567890


def test_compute_trades_features_falls_back_to_local_timestamp_without_exchange_times(monkeypatch):
    monkeypatch.setattr("collector.collector.feature_computer.time.time", lambda: 1234567.89)
    msg = {
        "a": 123,
        "p": "100.0",
        "q": "1.5",
        "m": True
    }

    features = compute_trades_features(msg)

    assert features["timestamp"] == 1234567890
    assert features["local_timestamp"] == 1234567890
    assert features["exchange_timestamp"] == 1234567890


def test_compute_trades_features_trade_time_latency_includes_dispatch_lag(monkeypatch):
    monkeypatch.setattr("collector.collector.feature_computer.time.time", lambda: 1234568.0)
    trade_time = 1234567000
    event_time = 1234567890
    msg = {
        "T": trade_time,
        "E": event_time,
        "a": 123,
        "p": "100.0",
        "q": "1.5",
        "m": True
    }

    features = compute_trades_features(msg)
    trade_latency = features["local_timestamp"] - features["exchange_timestamp"]
    previous_event_latency = features["local_timestamp"] - event_time

    assert trade_latency > 0
    assert trade_latency == previous_event_latency + (event_time - trade_time)

def test_compute_markprice_features():
    msg = {
        "E": 1234567890,
        "p": "100.0",
        "r": "0.0001",
        "T": 1234567890 + 3600000
    }

    features = compute_markprice_features(msg)

    assert features["mark_price"] == 100.0
    assert features["funding_rate"] == 0.0001
    assert features["funding_rate_bps"] == 1.0
    assert features["hours_to_funding"] == 1.0


def test_compute_openinterest_features_valid_response(monkeypatch):
    monkeypatch.setattr("collector.collector.feature_computer.time.time", lambda: 1234568.0)
    msg = {"openInterest": "123.45", "time": "1234567000", "price": "100.0"}

    features = compute_openinterest_features(msg)

    assert features == {
        "timestamp": 1234568000,
        "exchange_timestamp": 1234567000,
        "local_timestamp": 1234568000,
        "open_interest": 123.45,
    }


def test_compute_openinterest_features_missing_openinterest_returns_empty():
    assert compute_openinterest_features({"time": "1234567000"}) == {}


def test_compute_liquidation_features_buy_force_order(monkeypatch):
    monkeypatch.setattr("collector.collector.feature_computer.time.time", lambda: 1234568.0)
    msg = {
        "o": {
            "S": "BUY",
            "p": "100.0",
            "q": "1.5",
            "T": "1234567000",
            "X": "FILLED",
            "f": "IOC",
        }
    }

    features = compute_liquidation_features(msg)

    assert features == {
        "timestamp": 1234568000,
        "exchange_timestamp": 1234567000,
        "local_timestamp": 1234568000,
        "side": 1,
        "price": 100.0,
        "quantity": 1.5,
        "signed_qty": 1.5,
        "order_status": "FILLED",
        "time_in_force": "IOC",
    }


def test_compute_liquidation_features_sell_force_order(monkeypatch):
    monkeypatch.setattr("collector.collector.feature_computer.time.time", lambda: 1234568.0)
    msg = {
        "o": {
            "S": "SELL",
            "p": "100.0",
            "q": "2.0",
            "T": "1234567000",
            "X": "FILLED",
            "f": "IOC",
        }
    }

    features = compute_liquidation_features(msg)

    assert features["side"] == -1
    assert features["signed_qty"] == -2.0
    assert features["price"] == 100.0
    assert features["quantity"] == 2.0
