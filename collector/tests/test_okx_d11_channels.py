"""D11 implementation tests: parsers for the seven OKX channels covered by
docs/OKX_D11_CHANNEL_SCHEMAS.md.

Fixtures are drawn only from documented OKX examples (funding-rate,
liquidation-orders -- the latter a real captured production frame, both per
the schema doc) or carefully constructed edge cases whose semantics are
directly documented, per the task's testing rule. No fixture claims to be
"real market data" that was not actually captured.
"""
from __future__ import annotations

import copy

import pytest

from collector.collector.adapters.base import UnhandledReason
from collector.collector.adapters.okx import OKXAdapter
from collector.collector.canonical import (
    CanonicalLiquidationEvent,
    CanonicalMarkPriceEvent,
    CanonicalOIEvent,
    CanonicalTradeEvent,
)


def _frame(channel, data):
    return {"arg": {"channel": channel}, "data": data}


# ---------------------------------------------------------------------------
# trades / trades-all -- distinct channels, never conflated
# ---------------------------------------------------------------------------

def test_trades_channel_maps_seqid_to_venue_sequence():
    adapter = OKXAdapter()
    frame = _frame("trades", [{
        "instId": "BTC-USDT-SWAP", "tradeId": "12345", "px": "50000.1",
        "sz": "0.5", "side": "buy", "ts": "1700000000000", "seqId": 42,
    }])
    events = adapter.normalize(frame, local_receive_ts=99)
    assert len(events) == 1
    event = events[0]
    assert isinstance(event, CanonicalTradeEvent)
    assert event.stream == "trades"
    assert event.trade_id == "12345"
    assert event.price == 50000.1
    assert event.quantity == 0.5
    assert event.side == "buy"
    assert event.venue_sequence == 42
    assert event.source is None  # source is trades-all-only
    assert event.exchange_event_ts == 1700000000000
    assert event.local_receive_ts == 99


def test_trades_channel_without_seqid_does_not_error():
    """seqId was only added 2025-07-08; older/edge frames may lack it.
    Absence must not raise and must not be assumed to be a gap."""
    adapter = OKXAdapter()
    frame = _frame("trades", [{
        "instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "50000", "sz": "1",
        "side": "sell", "ts": "1700000000000",
    }])
    events = adapter.normalize(frame, local_receive_ts=1)
    assert events[0].venue_sequence is None


def test_repeated_seqid_is_not_treated_as_a_gap():
    """OKX's own changelog: the same seqId can appear on different updates
    at the same instant. Two trades sharing a seqId must both parse cleanly
    with no gap/error signal -- nothing in this adapter enforces seqId
    uniqueness or monotonicity."""
    adapter = OKXAdapter()
    frame = _frame("trades", [
        {"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "50000", "sz": "1", "side": "buy", "ts": "1700000000000", "seqId": 7},
        {"instId": "BTC-USDT-SWAP", "tradeId": "2", "px": "50001", "sz": "1", "side": "sell", "ts": "1700000000001", "seqId": 7},
    ])
    events = adapter.normalize(frame, local_receive_ts=1)
    assert len(events) == 2
    assert events[0].venue_sequence == events[1].venue_sequence == 7
    assert adapter.unhandled_count == 0


def test_trades_all_is_a_distinct_stream_with_source_field():
    adapter = OKXAdapter()
    frame = _frame("trades-all", [{
        "instId": "BTC-USDT-SWAP", "tradeId": "999", "px": "50000", "sz": "2",
        "side": "sell", "ts": "1700000000000", "source": "1",
    }])
    events = adapter.normalize(frame, local_receive_ts=1)
    event = events[0]
    assert event.stream == "trades-all"
    assert event.source == "1"


def test_trades_all_without_seqid_does_not_assume_presence():
    """Open question #2: seqId is confirmed added to `trades`, not
    confirmed for `trades-all`. Its absence here must be a plain None, not
    an inferred value or an error."""
    adapter = OKXAdapter()
    frame = _frame("trades-all", [{
        "instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "50000", "sz": "1",
        "side": "buy", "ts": "1700000000000", "source": "0",
    }])
    events = adapter.normalize(frame, local_receive_ts=1)
    assert events[0].venue_sequence is None


def test_trades_and_trades_all_are_never_merged_into_one_event_type_ambiguity():
    """A trades event and a trades-all event for the same instrument/price
    must remain distinguishable by `.stream`, not collapse into
    indistinguishable CanonicalTradeEvents."""
    adapter = OKXAdapter()
    t = adapter.normalize(_frame("trades", [{"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "50000", "sz": "1", "side": "buy", "ts": "1"}]), local_receive_ts=1)[0]
    ta = adapter.normalize(_frame("trades-all", [{"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "50000", "sz": "1", "side": "buy", "ts": "1", "source": "0"}]), local_receive_ts=1)[0]
    assert t.stream != ta.stream


# ---------------------------------------------------------------------------
# mark-price / index-tickers -- channel purity
# ---------------------------------------------------------------------------

def test_mark_price_never_populates_index_or_funding():
    adapter = OKXAdapter()
    frame = _frame("mark-price", [{"instType": "SWAP", "instId": "BTC-USDT-SWAP", "markPx": "50123.4", "ts": "1700000000000"}])
    event = adapter.normalize(frame, local_receive_ts=1)[0]
    assert isinstance(event, CanonicalMarkPriceEvent)
    assert event.stream == "mark-price"
    assert event.mark_price == 50123.4
    assert event.index_price is None
    assert event.funding_rate is None


def test_index_tickers_never_populates_mark_price():
    adapter = OKXAdapter()
    frame = _frame("index-tickers", [{"instId": "BTC-USDT", "idxPx": "50100.2", "ts": "1700000000000"}])
    event = adapter.normalize(frame, local_receive_ts=1)[0]
    assert event.stream == "index-tickers"
    assert event.index_price == 50100.2
    assert event.mark_price is None


# ---------------------------------------------------------------------------
# funding-rate -- real production example from docs/OKX_D11_CHANNEL_SCHEMAS.md
# ---------------------------------------------------------------------------

OKX_FUNDING_RATE_EXAMPLE = {
    "formulaType": "noRate", "fundingRate": "0.0001875391284828",
    "fundingTime": "1700726400000", "impactValue": "",
    "instId": "BTC-USD-SWAP", "instType": "SWAP", "interestRate": "",
    "method": "current_period", "maxFundingRate": "0.00375",
    "minFundingRate": "-0.00375", "nextFundingRate": "",
    "nextFundingTime": "1700755200000",
    "premium": "0.0001233824646391",
    "settFundingRate": "0.0001699799259033", "settState": "settled",
    "ts": "1700724675402",
}


def test_funding_rate_real_example_keeps_current_next_settled_distinct():
    adapter = OKXAdapter()
    event = adapter.normalize(_frame("funding-rate", [copy.deepcopy(OKX_FUNDING_RATE_EXAMPLE)]), local_receive_ts=1)[0]
    assert event.funding_rate == pytest.approx(0.0001875391284828)
    assert event.funding_time == 1700726400000
    assert event.next_funding_time == 1700755200000
    assert event.next_funding_rate is None  # documented as often empty; "" -> None, never 0.0
    assert event.sett_funding_rate == pytest.approx(0.0001699799259033)
    assert event.sett_state == "settled"
    assert event.premium == pytest.approx(0.0001233824646391)
    assert event.interest_rate is None  # "" -> None
    assert event.impact_value is None
    assert event.max_funding_rate == pytest.approx(0.00375)
    assert event.min_funding_rate == pytest.approx(-0.00375)
    assert event.formula_type == "noRate"
    assert event.method == "current_period"
    # The three funding values are genuinely different numbers -- proof
    # they were not collapsed onto one field.
    assert len({event.funding_rate, event.sett_funding_rate}) == 2


def test_funding_rate_never_hardcodes_an_interval():
    """No field named interval/8h/funding_interval anywhere on the event --
    the task explicitly forbids assuming 8 hours."""
    adapter = OKXAdapter()
    event = adapter.normalize(_frame("funding-rate", [copy.deepcopy(OKX_FUNDING_RATE_EXAMPLE)]), local_receive_ts=1)[0]
    field_names = {f for f in vars(event)}
    assert not any("interval" in name.lower() for name in field_names)


def test_funding_rate_missing_next_funding_rate_is_none_not_zero():
    adapter = OKXAdapter()
    payload = copy.deepcopy(OKX_FUNDING_RATE_EXAMPLE)
    payload["nextFundingRate"] = ""
    event = adapter.normalize(_frame("funding-rate", [payload]), local_receive_ts=1)[0]
    assert event.next_funding_rate is None
    assert event.next_funding_rate != 0.0


# ---------------------------------------------------------------------------
# open-interest -- three units, none discarded
# ---------------------------------------------------------------------------

def test_open_interest_preserves_all_three_units():
    adapter = OKXAdapter()
    frame = _frame("open-interest", [{
        "instType": "SWAP", "instId": "BTC-USDT-SWAP",
        "oi": "12345", "oiCcy": "1234.5", "oiUsd": "617250000", "ts": "1700000000000",
    }])
    event = adapter.normalize(frame, local_receive_ts=1)[0]
    assert isinstance(event, CanonicalOIEvent)
    assert event.open_interest == 12345.0  # canonical unit: contracts (oi)
    assert event.oi_ccy == 1234.5
    assert event.oi_usd == 617250000.0
    # All three are genuinely distinct values -- not one value copied three ways.
    assert len({event.open_interest, event.oi_ccy, event.oi_usd}) == 3


# ---------------------------------------------------------------------------
# liquidation-orders -- real captured production frame
# ---------------------------------------------------------------------------

OKX_LIQUIDATION_REAL_FRAME = {
    "arg": {"channel": "liquidation-orders", "instType": "SWAP"},
    "data": [{
        "details": [{
            "bkLoss": "0", "bkPx": "1.057", "ccy": "", "posSide": "long",
            "side": "sell", "sz": "768", "ts": "1723892524781",
        }],
        "instFamily": "DYDX-USDT", "instId": "DYDX-USDT-SWAP",
        "instType": "SWAP", "uly": "DYDX-USDT",
    }],
}


def test_liquidation_real_captured_frame_maps_every_field():
    adapter = OKXAdapter()
    event = adapter.normalize(copy.deepcopy(OKX_LIQUIDATION_REAL_FRAME), local_receive_ts=1)[0]
    assert isinstance(event, CanonicalLiquidationEvent)
    assert event.inst_id == "DYDX-USDT-SWAP"
    assert event.inst_family == "DYDX-USDT"
    assert event.uly == "DYDX-USDT"
    assert event.side == "sell"
    assert event.pos_side == "long"
    assert event.price == pytest.approx(1.057)  # bkPx
    assert event.quantity == pytest.approx(768.0)  # sz
    assert event.bk_loss == 0.0
    assert event.ccy == ""  # observed empty; preserved exactly, not None
    assert event.exchange_event_ts == 1723892524781


def test_liquidation_empty_ccy_is_not_coerced_to_none():
    """An explicit empty string and 'field never sent' are different
    observations -- canonical.py's ccy field must distinguish them."""
    adapter = OKXAdapter()
    event = adapter.normalize(copy.deepcopy(OKX_LIQUIDATION_REAL_FRAME), local_receive_ts=1)[0]
    assert event.ccy is not None
    assert event.ccy == ""


def test_liquidation_multiple_details_in_one_push_become_multiple_events():
    adapter = OKXAdapter()
    frame = {
        "arg": {"channel": "liquidation-orders", "instType": "SWAP"},
        "data": [{
            "instId": "BTC-USDT-SWAP", "instFamily": "BTC-USDT", "instType": "SWAP", "uly": "BTC-USDT",
            "details": [
                {"bkPx": "50000", "sz": "1", "side": "sell", "posSide": "long", "ts": "1700000000000", "bkLoss": "0", "ccy": ""},
                {"bkPx": "50001", "sz": "2", "side": "buy", "posSide": "short", "ts": "1700000000005", "bkLoss": "1.5", "ccy": ""},
            ],
        }],
    }
    events = adapter.normalize(frame, local_receive_ts=1)
    assert len(events) == 2
    assert [e.exchange_event_ts for e in events] == [1700000000000, 1700000000005]
    assert [e.quantity for e in events] == [1.0, 2.0]


def test_liquidation_does_not_filter_by_instrument_in_the_adapter():
    """Subscription is instType-scoped, so a push can carry other
    instruments. The task says filtering belongs at the runner/storage
    layer, not silently inside the parser -- confirm the adapter still
    surfaces a non-BTC instrument's liquidation rather than dropping it."""
    adapter = OKXAdapter(inst_id="BTC-USDT-SWAP")
    events = adapter.normalize(copy.deepcopy(OKX_LIQUIDATION_REAL_FRAME), local_receive_ts=1)
    assert len(events) == 1
    assert events[0].inst_id == "DYDX-USDT-SWAP"  # not BTC-USDT-SWAP, not dropped


# ---------------------------------------------------------------------------
# Malformed / missing-field handling -- never fabricate, never crash
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("channel,payload", [
    ("trades", {"instId": "BTC-USDT-SWAP"}),  # missing px/sz/side/ts
    ("trades-all", {"instId": "BTC-USDT-SWAP"}),
    ("mark-price", {"instId": "BTC-USDT-SWAP"}),  # missing markPx
    ("open-interest", {"instId": "BTC-USDT-SWAP"}),  # missing oi
])
def test_missing_required_fields_yield_malformed_not_a_crash(channel, payload):
    adapter = OKXAdapter()
    events = adapter.normalize(_frame(channel, [payload]), local_receive_ts=1)
    assert events == []
    drained = adapter.drain_unhandled()
    assert drained and drained[0].reason is UnhandledReason.MALFORMED_PAYLOAD


def test_liquidation_detail_with_no_optional_fields_does_not_crash():
    """Every field inside `details[]` is read defensively (.get), so an
    empty detail dict must not raise -- it produces an event with mostly
    None fields, which is observable (all-None), not a silent crash."""
    adapter = OKXAdapter()
    frame = {"arg": {"channel": "liquidation-orders"},
             "data": [{"instId": "BTC-USDT-SWAP", "details": [{}]}]}
    events = adapter.normalize(frame, local_receive_ts=1)
    assert len(events) == 1
    event = events[0]
    assert event.side is None and event.price is None and event.quantity is None


def test_index_tickers_missing_idxpx_is_none_not_zero():
    """idxPx uses _num (empty/missing -> None), unlike required trade/OI
    fields -- confirm a missing value never becomes a fabricated 0.0."""
    adapter = OKXAdapter()
    event = adapter.normalize(_frame("index-tickers", [{"instId": "BTC-USDT", "ts": "1700000000000"}]), local_receive_ts=1)[0]
    assert event.index_price is None


def test_unknown_extra_fields_do_not_break_parsing():
    """A field OKX might add later, or a captured-frame artifact, must not
    crash the parser -- it is simply not mapped onto canonical (raw_wire
    remains the lossless copy)."""
    adapter = OKXAdapter()
    frame = _frame("trades", [{
        "instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "1", "sz": "1",
        "side": "buy", "ts": "1", "count": "3", "someNewOkxField": "unexpected",
    }])
    events = adapter.normalize(frame, local_receive_ts=1)
    assert len(events) == 1
    assert events[0].price == 1.0


# ---------------------------------------------------------------------------
# Determinism -- "replay must invoke the same parser" (task's REPLAY
# section). ReplayEngine itself is order-book-only (see replay.py) and is
# not extended to non-orderbook streams in this PR -- see
# docs/EXECUTION_STATUS.md. What this proves instead: adapter.normalize()
# is a pure function of its input, the property that makes "raw frame ->
# same parser -> same canonical result" true whenever it IS wired into a
# replay driver.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("channel,payload", [
    ("trades", {"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "50000", "sz": "1", "side": "buy", "ts": "1", "seqId": 3}),
    ("trades-all", {"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "50000", "sz": "1", "side": "buy", "ts": "1", "source": "0"}),
    ("mark-price", {"instId": "BTC-USDT-SWAP", "markPx": "50000", "ts": "1"}),
    ("index-tickers", {"instId": "BTC-USDT", "idxPx": "50000", "ts": "1"}),
    ("funding-rate", OKX_FUNDING_RATE_EXAMPLE),
    ("open-interest", {"instId": "BTC-USDT-SWAP", "oi": "1", "oiCcy": "1", "oiUsd": "1", "ts": "1"}),
    ("liquidation-orders", OKX_LIQUIDATION_REAL_FRAME["data"][0]),
])
def test_parse_is_deterministic_same_frame_same_result(channel, payload):
    frame = _frame(channel, [copy.deepcopy(payload)])
    a = OKXAdapter().normalize(copy.deepcopy(frame), local_receive_ts=42)
    b = OKXAdapter().normalize(copy.deepcopy(frame), local_receive_ts=42)
    assert a == b
    # Same instance, called twice -- rules out any adapter-instance state
    # (e.g. a cache) silently affecting the second parse.
    adapter = OKXAdapter()
    c = adapter.normalize(copy.deepcopy(frame), local_receive_ts=42)
    d = adapter.normalize(copy.deepcopy(frame), local_receive_ts=42)
    assert c == d == a


# ---------------------------------------------------------------------------
# Declared vs implemented -- pins the D11 completion state itself
# ---------------------------------------------------------------------------

def test_all_declared_channels_are_implemented():
    adapter = OKXAdapter()
    assert adapter.declared_channels() == adapter.implemented_channels()
    assert adapter.unimplemented_channels == frozenset()
    assert adapter.declared_channels() == frozenset({
        "books", "trades", "trades-all", "mark-price", "index-tickers",
        "funding-rate", "open-interest", "liquidation-orders",
    })
