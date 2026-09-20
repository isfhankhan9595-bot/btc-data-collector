"""P5: Binance Spot adapter + sequence semantics.

Fixtures are the official payload shapes documented in
binance_spot.py's module docstring (github.com/binance/binance-spot-api-docs
web-socket-streams.md), not guesses.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from collector.collector.adapters.base import UnhandledReason
from collector.collector.adapters.binance_spot import BinanceSpotAdapter
from collector.collector.canonical import CanonicalOrderBookEvent, CanonicalTradeEvent
from collector.collector.sequence import (
    SpotSequenceComparator,
    SequenceResult,
    binance_spot_snapshot_bridge,
    binance_snapshot_bridge,
)


def _frame(stream, data):
    return {"stream": stream, "data": data}


# ---------------------------------------------------------------------------
# Trade stream
# ---------------------------------------------------------------------------

OFFICIAL_TRADE_EXAMPLE = {
    "e": "trade", "E": 1672515782136, "s": "BNBBTC", "t": 12345,
    "p": "0.001", "q": "100", "T": 1672515782136, "m": True, "M": True,
}


def test_trade_maps_official_example_and_is_explicitly_spot():
    adapter = BinanceSpotAdapter()
    event = adapter.normalize(_frame("bnbbtc@trade", dict(OFFICIAL_TRADE_EXAMPLE)), local_receive_ts=99)[0]
    assert isinstance(event, CanonicalTradeEvent)
    assert event.exchange == "BINANCE"          # P6 identity: exchange stays BINANCE
    assert event.market_type == "spot"           # ...market_type is the differentiator
    assert event.stream == "spot_trades"
    assert event.trade_id == "12345"             # raw `t`, never the aggregate `a`
    assert event.price == 0.001
    assert event.quantity == 100.0
    assert event.side == "SELL"                  # m=true -> buyer is maker -> seller is aggressor
    assert event.exchange_event_ts == 1672515782136   # E
    assert event.exchange_transaction_ts == 1672515782136  # T
    assert event.local_receive_ts == 99


def test_trade_side_semantics_both_directions():
    adapter = BinanceSpotAdapter()
    maker_buyer = dict(OFFICIAL_TRADE_EXAMPLE, m=True)
    taker_buyer = dict(OFFICIAL_TRADE_EXAMPLE, m=False)
    assert adapter.normalize(_frame("bnbbtc@trade", maker_buyer), local_receive_ts=1)[0].side == "SELL"
    assert adapter.normalize(_frame("bnbbtc@trade", taker_buyer), local_receive_ts=1)[0].side == "BUY"


def test_trade_id_is_never_the_aggregate_id():
    """The adapter must read `t` (Trade ID), never `a` (Aggregate trade ID,
    aggTrade-only and not present on the trade stream at all)."""
    adapter = BinanceSpotAdapter()
    payload = dict(OFFICIAL_TRADE_EXAMPLE, t=999)
    event = adapter.normalize(_frame("bnbbtc@trade", payload), local_receive_ts=1)[0]
    assert event.trade_id == "999"


@pytest.mark.parametrize("missing_field", ["p", "q"])
def test_trade_missing_required_field_is_malformed_not_a_crash(missing_field):
    adapter = BinanceSpotAdapter()
    payload = dict(OFFICIAL_TRADE_EXAMPLE)
    del payload[missing_field]
    events = adapter.normalize(_frame("bnbbtc@trade", payload), local_receive_ts=1)
    assert events == []
    drained = adapter.drain_unhandled()
    assert drained and drained[0].reason is UnhandledReason.MALFORMED_PAYLOAD


def test_trade_malformed_numeric_price_is_malformed_not_a_crash():
    adapter = BinanceSpotAdapter()
    payload = dict(OFFICIAL_TRADE_EXAMPLE, p="NOT_A_NUMBER")
    events = adapter.normalize(_frame("bnbbtc@trade", payload), local_receive_ts=1)
    assert events == []
    assert adapter.drain_unhandled()[0].reason is UnhandledReason.MALFORMED_PAYLOAD


# ---------------------------------------------------------------------------
# Diff-depth stream
# ---------------------------------------------------------------------------

OFFICIAL_DEPTH_EXAMPLE = {
    "e": "depthUpdate", "E": 1672515782136, "s": "BNBBTC",
    "U": 157, "u": 160,
    "b": [["0.0024", "10"]], "a": [["0.0026", "100"]],
}


def test_depth_update_maps_official_example_and_is_explicitly_spot():
    adapter = BinanceSpotAdapter()
    event = adapter.normalize(_frame("bnbbtc@depth", dict(OFFICIAL_DEPTH_EXAMPLE)), local_receive_ts=1)[0]
    assert isinstance(event, CanonicalOrderBookEvent)
    assert event.exchange == "BINANCE"
    assert event.market_type == "spot"
    assert event.stream == "spot_orderbook"
    assert event.update_id == 160
    assert event.first_update_id == 157
    assert event.previous_update_id is None  # no `pu` on Spot -- never fabricated
    assert event.bids == ((Decimal("0.0024"), Decimal("10")),)
    assert event.asks == ((Decimal("0.0026"), Decimal("100")),)
    assert event.book_source == "DIFF_DEPTH_RECONSTRUCTED"


@pytest.mark.parametrize("missing_field", ["U", "u"])
def test_depth_update_missing_sequence_id_is_malformed(missing_field):
    adapter = BinanceSpotAdapter()
    payload = dict(OFFICIAL_DEPTH_EXAMPLE)
    del payload[missing_field]
    events = adapter.normalize(_frame("bnbbtc@depth", payload), local_receive_ts=1)
    assert events == []
    assert adapter.drain_unhandled()[0].reason is UnhandledReason.MALFORMED_PAYLOAD


def test_depth_update_non_integer_sequence_id_is_malformed():
    adapter = BinanceSpotAdapter()
    payload = dict(OFFICIAL_DEPTH_EXAMPLE, u="not-an-int")
    events = adapter.normalize(_frame("bnbbtc@depth", payload), local_receive_ts=1)
    assert events == []
    assert adapter.drain_unhandled()[0].reason is UnhandledReason.MALFORMED_PAYLOAD


# ---------------------------------------------------------------------------
# SpotSequenceComparator -- distinct chain rule from USD-M futures
# ---------------------------------------------------------------------------

class _Ev:
    def __init__(self, first_update_id, update_id):
        self.first_update_id = first_update_id
        self.update_id = update_id


def test_spot_comparator_first_event_always_passes():
    assert SpotSequenceComparator().check(_Ev(1, 5), None) == SequenceResult()


def test_spot_comparator_correct_chain_passes():
    previous = _Ev(1, 160)
    current = _Ev(161, 165)  # U == prev.u + 1
    result = SpotSequenceComparator().check(current, previous)
    assert not result.is_gap and not result.is_stale


def test_spot_comparator_broken_chain_is_a_gap():
    previous = _Ev(1, 160)
    current = _Ev(162, 165)  # skipped 161 -- U != prev.u + 1
    result = SpotSequenceComparator().check(current, previous)
    assert result.is_gap
    assert result.reason == "u_chain_broken"


def test_spot_comparator_has_no_stale_duplicate_carve_out():
    """Unlike BinanceSequenceComparator (USD-M), a repeated event is a gap
    here, not silently dropped as stale -- see the comparator's docstring
    for why no such exception is documented for Spot."""
    previous = _Ev(1, 160)
    duplicate = _Ev(1, 160)  # identical to previous -- U != prev.u+1 (161)
    result = SpotSequenceComparator().check(duplicate, previous)
    assert result.is_gap
    assert not result.is_stale


def test_spot_comparator_missing_ids_is_a_gap():
    previous = _Ev(1, 160)
    current = _Ev(None, None)
    result = SpotSequenceComparator().check(current, previous)
    assert result.is_gap
    assert result.reason == "update_id_missing"


# ---------------------------------------------------------------------------
# Snapshot bridge -- the one-token +1 difference from USD-M futures
# ---------------------------------------------------------------------------

def test_spot_bridge_requires_the_plus_one_futures_does_not():
    """last_update_id=160. Spot's own official formula needs U<=161<=u;
    an event with U=161 satisfies Spot's bridge but NOT futures' (which
    needs U<=160<=u) -- this is the off-by-one the two products differ by,
    pinned so a future refactor can't silently merge the two formulas."""
    event = _Ev(first_update_id=161, update_id=165)
    assert binance_spot_snapshot_bridge(event, last_update_id=160) is True
    assert binance_snapshot_bridge(event, last_update_id=160) is False


def test_futures_bridge_accepts_an_event_spot_bridge_rejects():
    """last_update_id=160, update_id=160 exactly (no headroom past it):
    satisfies futures' `u >= lastUpdateId` (160>=160) but not Spot's
    stricter `u >= lastUpdateId+1` (160 >= 161 is false) -- the mirror
    case of the test above, this time the upper bound is what differs."""
    event = _Ev(first_update_id=155, update_id=160)
    assert binance_snapshot_bridge(event, last_update_id=160) is True
    assert binance_spot_snapshot_bridge(event, last_update_id=160) is False


def test_spot_bridge_rejects_non_integer_ids():
    assert binance_spot_snapshot_bridge(_Ev(None, None), 160) is False
    assert binance_spot_snapshot_bridge(_Ev(1, 2), "not-an-int") is False


# ---------------------------------------------------------------------------
# LocalBook integration -- "BINANCE_SPOT" venue, mirroring the existing
# Binance USD-M LocalBook tests (tests/test_adapters_sequence.py) but with
# Spot's own discard/bridge/chain rules, so an accidental merge of the two
# formulas would fail here even if it happened to still pass the futures
# tests.
# ---------------------------------------------------------------------------

def _spot_diff(U, u):
    return CanonicalOrderBookEvent("BINANCE", "spot_orderbook", 1, None, 1,
        market_type="spot", bids=((100.0, 1.0),), asks=((101.0, 1.0),),
        update_id=u, first_update_id=U)


def _spot_snapshot(update):
    return CanonicalOrderBookEvent("BINANCE", "spot_orderbook", None, None, 10,
        market_type="spot", bids=((100.0, 1.0),), asks=((101.0, 1.0),),
        update_id=update, is_snapshot=True)


def test_spot_local_book_uses_binance_spot_venue_key():
    from collector.collector.book_engine import LocalBook
    from collector.collector.sequence import SpotSequenceComparator
    book = LocalBook("BINANCE_SPOT")
    assert isinstance(book.comparator, SpotSequenceComparator)


def test_spot_local_book_snapshot_bridges_with_the_plus_one_discard_rule():
    """last_update_id=10. Spot discards u<=10 (not futures' u<10) -- a diff
    with u==10 exactly must NOT survive as a bridge candidate here, unlike
    the equivalent futures test (test_binance_local_book_snapshot_bridges_
    and_discards_stale_diffs) where u==lastUpdateId does survive."""
    from collector.collector.book_engine import LocalBook
    book = LocalBook("BINANCE_SPOT")
    book.buffer = [_spot_diff(5, 9), _spot_diff(10, 10), _spot_diff(11, 15)]
    assert book.binance_snapshot(10, _spot_snapshot(10))
    assert book.previous.update_id == 15
    assert book.state.state.value == "VALID"


def test_spot_local_book_continues_the_chain_after_bridging():
    from collector.collector.book_engine import LocalBook
    book = LocalBook("BINANCE_SPOT")
    book.buffer = [_spot_diff(11, 15), _spot_diff(16, 20)]  # 16 == 15+1
    assert book.binance_snapshot(10, _spot_snapshot(10))
    assert book.previous.update_id == 20
    assert book.state.state.value == "VALID"


def test_spot_local_book_rejects_a_broken_post_bridge_chain():
    from collector.collector.book_engine import LocalBook
    book = LocalBook("BINANCE_SPOT")
    book.buffer = [_spot_diff(11, 15), _spot_diff(20, 25)]  # 20 != 15+1 -- a gap
    assert not book.binance_snapshot(10, _spot_snapshot(10))
    assert book.last_reason == "u_chain_broken"
    assert book.state.state.value == "SEQUENCE_GAP"


def test_spot_local_book_missing_overlap_is_a_gap_not_a_false_bridge():
    """Buffer starts at U=12 but last_update_id+1=11 is never covered --
    genuinely missing diffs, not bridgeable by any future event."""
    from collector.collector.book_engine import LocalBook
    book = LocalBook("BINANCE_SPOT")
    book.buffer = [_spot_diff(12, 20)]
    assert not book.binance_snapshot(10, _spot_snapshot(10))
    assert book.last_reason == "snapshot_bridge_not_found"


def test_spot_local_book_buffers_events_before_a_valid_book_like_futures():
    """Both Binance products buffer until bridged (book_engine's
    _BUFFER_UNTIL_BRIDGED_VENUES) -- confirm Spot actually gets that
    treatment, not Bybit/OKX's plain snapshot path."""
    from collector.collector.book_engine import LocalBook
    book = LocalBook("BINANCE_SPOT")
    applied = book.apply(_spot_diff(1, 5))
    assert applied is None  # buffered, not applied -- no snapshot yet
    assert len(book.buffer) == 1


# ---------------------------------------------------------------------------
# Storage namespace isolation
# ---------------------------------------------------------------------------

def test_spot_storage_namespace_is_isolated_from_binance_futures():
    from collector.collector.storage_layout import check_stream_namespace, venue_stream

    assert venue_stream("BINANCE_SPOT", "trades") == "spot_trades"
    assert venue_stream("BINANCE", "trades") == "trades"
    assert venue_stream("BINANCE_SPOT", "trades") != venue_stream("BINANCE", "trades")

    # A writer declared as futures must not be allowed onto a spot_ stream,
    # and vice versa -- this is the exact PR #19 collision class the
    # namespace registry exists to prevent, now extended to Spot.
    with pytest.raises(Exception):
        check_stream_namespace("BINANCE", "spot_trades")
    with pytest.raises(Exception):
        check_stream_namespace("BINANCE_SPOT", "trades")
    check_stream_namespace("BINANCE_SPOT", "spot_trades")  # must not raise
    check_stream_namespace("BINANCE", "trades")  # must not raise


# ---------------------------------------------------------------------------
# market_type explicitness -- P6 identity depends on this never silently
# defaulting. Mutation-tested this session: removing
# `market_type=MARKET_TYPE_SPOT` from binance_spot.py's constructions made
# this test fail with "linear_perpetual" instead of "spot" (confirmed via
# pytest -k market_type before restoring the fix). Swapping LocalBook's
# "BINANCE_SPOT" comparator for BinanceSequenceComparator (the futures rule)
# was mutation-tested the same way and failed three of the LocalBook tests
# above, including one on the pu-based reason string ("pu_missing" instead
# of "u_chain_broken") that only a futures comparator would produce.
# ---------------------------------------------------------------------------

def test_market_type_is_never_the_default_for_either_spot_event_type():
    adapter = BinanceSpotAdapter()
    trade = adapter.normalize(_frame("bnbbtc@trade", dict(OFFICIAL_TRADE_EXAMPLE)), local_receive_ts=1)[0]
    book_event = adapter.normalize(_frame("bnbbtc@depth", dict(OFFICIAL_DEPTH_EXAMPLE)), local_receive_ts=1)[0]
    assert trade.market_type == "spot" != CanonicalTradeEvent.__dataclass_fields__["market_type"].default
    assert book_event.market_type == "spot" != CanonicalOrderBookEvent.__dataclass_fields__["market_type"].default


# ---------------------------------------------------------------------------
# P6 causal alignment integration -- proves Spot events are correctly
# distinguished by (exchange, market_type, stream) and follow the same
# local_receive_ts <= observation_ts availability rule the task's section 21
# requires, using P6's actual module rather than a reimplementation.
# ---------------------------------------------------------------------------

def test_spot_and_futures_trades_are_distinct_p6_keys_even_with_same_exchange():
    from collector.pipeline.cross_exchange_alignment import alignment_key
    from collector.collector.canonical import CanonicalTradeEvent

    spot_trade = CanonicalTradeEvent("BINANCE", "spot_trades", 1, 1, 1, market_type="spot")
    futures_trade = CanonicalTradeEvent("BINANCE", "trades", 1, 1, 1)  # default market_type
    assert alignment_key(spot_trade) != alignment_key(futures_trade)
    assert alignment_key(spot_trade)[0] == alignment_key(futures_trade)[0] == "BINANCE"


def test_spot_causal_availability_case_a_not_available():
    """Case A from the task: exchange_event_ts < observation_ts but
    local_receive_ts > observation_ts -- must be NOT available. Exchange
    time never substitutes for receive time."""
    from collector.pipeline.cross_exchange_alignment import causally_align, alignment_key, AlignmentStatus

    adapter = BinanceSpotAdapter()
    late_arriving = adapter.normalize(
        _frame("bnbbtc@trade", dict(OFFICIAL_TRADE_EXAMPLE, E=100)),  # exchange_event_ts=100, "early"
        local_receive_ts=5_000,  # received late
    )[0]
    key = alignment_key(late_arriving)
    result = causally_align([late_arriving], observation_ts=1_000, staleness_ms=10_000, expected_keys=[key])
    assert result[key].status is AlignmentStatus.NEVER_OBSERVED
    assert result[key].event is None


def test_spot_causal_availability_case_b_available():
    """Case B: exchange_event_ts > observation_ts but
    local_receive_ts <= observation_ts -- must BE available."""
    from collector.pipeline.cross_exchange_alignment import causally_align, alignment_key, AlignmentStatus

    adapter = BinanceSpotAdapter()
    early_receive = adapter.normalize(
        _frame("bnbbtc@trade", dict(OFFICIAL_TRADE_EXAMPLE, E=9_999_999)),  # exchange_event_ts far ahead
        local_receive_ts=500,  # but actually received early
    )[0]
    key = alignment_key(early_receive)
    result = causally_align([early_receive], observation_ts=1_000, staleness_ms=10_000, expected_keys=[key])
    assert result[key].status is AlignmentStatus.AVAILABLE
    assert result[key].event is early_receive
