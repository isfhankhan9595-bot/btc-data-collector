"""Market State Engine V0 -- adversarial tests.

The point of this file is not "does the arithmetic work" (that's the easy
part) but "can this engine be tricked into using information it should not
have". Every test in the causal-correctness sections constructs a scenario
where the wrong answer is the *easy* answer to compute, and checks the
engine gives the hard, correct one instead.
"""
from __future__ import annotations

from collector.collector.canonical import (
    CanonicalLiquidationEvent,
    CanonicalMarkPriceEvent,
    CanonicalOIEvent,
    CanonicalOrderBookEvent,
    CanonicalTradeEvent,
    OIUnit,
    OIUnitError,
    assert_comparable_oi,
)
from collector.collector.market_state import MarketStateEngine

BASE = 1_780_000_000_000


def trade(ts, price, qty, side, exchange="BINANCE"):
    return CanonicalTradeEvent(exchange, "trades", ts, None, ts,
                               price=price, quantity=qty, side=side)


def book(ts, bids, asks, exchange="BINANCE"):
    return CanonicalOrderBookEvent(exchange, "orderbook", ts, None, ts,
                                   bids=tuple(bids), asks=tuple(asks))


def liquidation(ts, side, qty, exchange="BINANCE"):
    return CanonicalLiquidationEvent(exchange, "liquidation", ts, None, ts,
                                     side=side, price=100.0, quantity=qty)


def mark(ts, mark_price=None, index_price=None, funding_rate=None, exchange="BINANCE"):
    return CanonicalMarkPriceEvent(exchange, "markprice", ts, None, ts,
                                   mark_price=mark_price, index_price=index_price,
                                   funding_rate=funding_rate)


def oi(ts, value, unit=OIUnit.UNKNOWN, exchange="BINANCE"):
    return CanonicalOIEvent(exchange, "openinterest", ts, None, ts,
                            open_interest=value, unit=unit)


# --------------------------------------------------------------------------
# 1-2. Empty engine, single trade.
# --------------------------------------------------------------------------

def test_empty_engine_reports_nothing_observed_not_zero_as_a_fact():
    engine = MarketStateEngine("BINANCE")
    state = engine.snapshot(BASE)
    assert state.trade_flow.trades_observed is False
    assert state.trade_flow.cvd == 0.0            # arithmetic identity of "no trades", not a claim
    assert state.book.book_available is False
    assert state.liquidation.liquidations_observed is False
    assert state.derivatives.open_interest is None


def test_single_trade_is_reflected():
    engine = MarketStateEngine("BINANCE")
    engine.update(trade(BASE, 50_000.0, 1.5, "buy"))
    state = engine.snapshot(BASE)
    assert state.price.last_trade_price == 50_000.0
    assert state.trade_flow.cumulative_buy_volume == 1.5
    assert state.trade_flow.trade_count == 1


# --------------------------------------------------------------------------
# 3-5. Multiple trades, flow accumulation, CVD.
# --------------------------------------------------------------------------

def test_cvd_accumulates_signed_volume():
    engine = MarketStateEngine("BINANCE")
    engine.update(trade(BASE, 50_000, 2.0, "buy"))
    engine.update(trade(BASE + 1, 50_001, 0.5, "sell"))
    engine.update(trade(BASE + 2, 50_002, 1.0, "buy"))
    state = engine.snapshot(BASE + 10)
    assert state.trade_flow.cumulative_buy_volume == 3.0
    assert state.trade_flow.cumulative_sell_volume == 0.5
    assert state.trade_flow.cvd == 2.5


def test_unrecognised_side_counts_toward_trade_count_never_guessed_into_a_direction():
    engine = MarketStateEngine("BINANCE")
    engine.update(trade(BASE, 50_000, 5.0, side=None))
    state = engine.snapshot(BASE)
    assert state.trade_flow.trade_count == 1
    assert state.trade_flow.cumulative_buy_volume == 0.0
    assert state.trade_flow.cumulative_sell_volume == 0.0


# --------------------------------------------------------------------------
# 6-7. Liquidation accumulation; direction is NOT relabelled long/short.
# --------------------------------------------------------------------------

def test_liquidation_accumulates_by_raw_side_without_long_short_relabeling():
    engine = MarketStateEngine("BINANCE")
    engine.update(liquidation(BASE, "buy", 2.0))
    engine.update(liquidation(BASE + 1, "sell", 1.0))
    state = engine.snapshot(BASE + 10)
    assert state.liquidation.buy_side_quantity == 2.0
    assert state.liquidation.sell_side_quantity == 1.0
    assert state.liquidation.liquidation_count == 2
    # Must not exist under a long/short name -- that mapping is unverified.
    assert not hasattr(state.liquidation, "long_liquidated")
    assert not hasattr(state.liquidation, "short_liquidated")


# --------------------------------------------------------------------------
# 8-11. Order-book state, spread, OBI, zero-denominator.
# --------------------------------------------------------------------------

def test_book_state_computes_mid_spread_and_imbalance():
    engine = MarketStateEngine("BINANCE")
    engine.update(book(BASE, bids=[(100.0, 3.0)], asks=[(101.0, 1.0)]))
    state = engine.snapshot(BASE)
    assert state.book.mid == 100.5
    assert state.book.spread == 1.0
    assert state.book.book_imbalance == 0.5     # (3-1)/(3+1)


def test_book_imbalance_zero_denominator_is_none_not_zero_division_error():
    engine = MarketStateEngine("BINANCE")
    engine.update(book(BASE, bids=[(100.0, 0.0)], asks=[(101.0, 0.0)]))
    state = engine.snapshot(BASE)
    assert state.book.book_imbalance is None
    assert state.book.mid == 100.5   # price side still computable


def test_empty_book_levels_are_available_but_not_priced():
    engine = MarketStateEngine("BINANCE")
    engine.update(book(BASE, bids=[], asks=[]))
    state = engine.snapshot(BASE)
    assert state.book.book_available is True
    assert state.book.best_bid is None


# --------------------------------------------------------------------------
# 12-15. Staleness: book, mark, OI, missing OI.
# --------------------------------------------------------------------------

def test_book_becomes_stale_after_its_threshold_but_not_before():
    engine = MarketStateEngine("BINANCE", book_stale_ms=1_000)
    engine.update(book(BASE, bids=[(100.0, 1.0)], asks=[(101.0, 1.0)]))
    assert engine.snapshot(BASE + 999).book.book_stale is False
    assert engine.snapshot(BASE + 1_001).book.book_stale is True


def test_mark_price_staleness_gates_price_vs_mark():
    engine = MarketStateEngine("BINANCE", mark_stale_ms=1_000)
    engine.update(trade(BASE, 50_010.0, 1.0, "buy"))
    engine.update(mark(BASE, mark_price=50_000.0))
    fresh = engine.snapshot(BASE + 500)
    stale = engine.snapshot(BASE + 5_000)
    assert fresh.price.price_vs_mark == 10.0
    assert stale.price.mark_stale is True
    assert stale.price.price_vs_mark is None    # never computed from stale data


def test_oi_staleness_and_missing_oi_are_distinct():
    engine = MarketStateEngine("BINANCE", oi_stale_ms=1_000)
    never_seen = engine.snapshot(BASE)
    assert never_seen.derivatives.open_interest is None
    assert never_seen.derivatives.oi_stale is True     # "stale" doubles as "never observed" here: both mean don't trust it

    engine.update(oi(BASE, 1000.0))
    fresh = engine.snapshot(BASE + 500)
    gone_stale = engine.snapshot(BASE + 5_000)
    assert fresh.derivatives.oi_stale is False
    assert fresh.derivatives.open_interest == 1000.0
    assert gone_stale.derivatives.oi_stale is True
    assert gone_stale.derivatives.open_interest == 1000.0   # last known value retained, just flagged


# --------------------------------------------------------------------------
# 16-18. OI unit contract: UNKNOWN, cross-venue rejection, same-venue allowed.
# --------------------------------------------------------------------------

def test_unknown_oi_unit_is_never_silently_upgraded():
    engine = MarketStateEngine("BINANCE")
    engine.update(oi(BASE, 1000.0, unit=OIUnit.UNKNOWN))
    state = engine.snapshot(BASE)
    assert state.derivatives.oi_unit is OIUnit.UNKNOWN


def test_cross_venue_oi_comparison_is_rejected_by_the_existing_contract():
    binance_event = oi(BASE, 1000.0, unit=OIUnit.UNKNOWN, exchange="BINANCE")
    bybit_event = oi(BASE, 2000.0, unit=OIUnit.UNKNOWN, exchange="BYBIT")
    import pytest
    with pytest.raises(OIUnitError):
        assert_comparable_oi(binance_event, bybit_event)


def test_same_venue_oi_change_is_computed_even_when_unit_is_unknown():
    """canonical.py's own contract: UNKNOWN-vs-UNKNOWN within one exchange
    is safe, because one venue's stream is one physical quantity regardless
    of whether its name is documented. The engine computes oi_change on
    exactly this basis, not by weakening the cross-venue rule."""
    engine = MarketStateEngine("BINANCE")
    engine.update(oi(BASE, 1000.0, unit=OIUnit.UNKNOWN))
    engine.update(oi(BASE + 1, 1050.0, unit=OIUnit.UNKNOWN))
    state = engine.snapshot(BASE + 10)
    assert state.derivatives.oi_change == 50.0


def test_oi_change_not_computed_across_a_unit_change():
    engine = MarketStateEngine("BINANCE")
    engine.update(oi(BASE, 1000.0, unit=OIUnit.CONTRACTS))
    engine.update(oi(BASE + 1, 500.0, unit=OIUnit.BASE_COIN))
    state = engine.snapshot(BASE + 10)
    assert state.derivatives.oi_change is None


# --------------------------------------------------------------------------
# 19-20. Funding carried-forward age (via MarkPrice's own field_age_ms /
# carried_forward -- exercised through the state engine's funding_age_ms).
# --------------------------------------------------------------------------

def test_funding_age_reflects_time_since_the_observation_not_wall_clock():
    engine = MarketStateEngine("BINANCE")
    engine.update(mark(BASE, funding_rate=0.0001))
    state = engine.snapshot(BASE + 30_000)
    assert state.derivatives.funding_age_ms == 30_000
    assert state.derivatives.funding_rate == 0.0001


# --------------------------------------------------------------------------
# 21-25. Causal ordering, future exchange timestamps, late events.
# --------------------------------------------------------------------------

def test_snapshot_uses_local_receive_ts_not_exchange_event_ts_for_availability():
    """The easy-to-compute wrong answer: use exchange_event_ts to decide
    what's "available". A slow response's exchange timestamp can predate
    when the collector actually received it -- using it would let replay
    (or live) claim information earlier than it was actually known."""
    late_arriving = CanonicalTradeEvent(
        "BINANCE", "trades", exchange_event_ts=BASE - 5_000,   # claims to be old
        exchange_transaction_ts=None, local_receive_ts=BASE + 5_000,  # actually arrived late
        price=50_000.0, quantity=1.0, side="buy")
    engine = MarketStateEngine("BINANCE")
    engine.update(late_arriving)
    # At BASE, the collector had NOT yet received this trade, regardless of
    # its exchange timestamp claiming BASE - 5000.
    assert engine.snapshot(BASE).trade_flow.trades_observed is False
    assert engine.snapshot(BASE + 5_000).trade_flow.trades_observed is True


def test_future_exchange_timestamp_cannot_pull_an_event_into_an_earlier_snapshot():
    future_claimed = CanonicalTradeEvent(
        "BINANCE", "trades", exchange_event_ts=BASE + 999_999,   # implausibly future exchange ts
        exchange_transaction_ts=None, local_receive_ts=BASE,      # but actually received now
        price=1.0, quantity=1.0, side="buy")
    engine = MarketStateEngine("BINANCE")
    engine.update(future_claimed)
    # Available at BASE regardless of the (irrelevant) exchange timestamp.
    assert engine.snapshot(BASE).trade_flow.trades_observed is True


def test_a_previously_returned_snapshot_is_frozen_and_unaffected_by_later_updates():
    """MarketState is a frozen dataclass returned by value: adding a new
    event to the engine after a snapshot was taken must not retroactively
    change the object already handed to a caller, regardless of the new
    event's own timestamp."""
    engine = MarketStateEngine("BINANCE")
    engine.update(trade(BASE, 100.0, 1.0, "buy"))
    earlier_snapshot = engine.snapshot(BASE)
    # A new event, timestamped after this snapshot's observation_ts -- it
    # must never appear in `earlier_snapshot`, no matter when it's added.
    engine.update(trade(BASE + 500, 999.0, 5.0, "sell"))
    assert earlier_snapshot.trade_flow.trade_count == 1
    assert earlier_snapshot.trade_flow.cumulative_sell_volume == 0.0
    # A fresh call for the SAME observation_ts must also still exclude it --
    # the event is genuinely outside that ts's causal window, not merely
    # absent from a stale cached object.
    assert engine.snapshot(BASE).trade_flow.trade_count == 1


def test_inserting_an_event_out_of_order_does_not_change_a_snapshot_that_already_excludes_it():
    engine = MarketStateEngine("BINANCE")
    engine.update(trade(BASE + 100, 100.0, 1.0, "buy"))
    before = engine.snapshot(BASE)
    engine.update(trade(BASE + 50, 200.0, 2.0, "sell"))    # arrives "late" in call order, timestamped between BASE and BASE+100
    after = engine.snapshot(BASE)
    assert before.trade_flow.trade_count == after.trade_flow.trade_count == 0


def test_a_later_snapshot_correctly_includes_an_event_inserted_out_of_call_order():
    engine = MarketStateEngine("BINANCE")
    engine.update(trade(BASE + 100, 100.0, 1.0, "buy"))
    engine.update(trade(BASE + 50, 200.0, 2.0, "sell"))
    state = engine.snapshot(BASE + 200)
    assert state.trade_flow.trade_count == 2
    assert state.trade_flow.cumulative_sell_volume == 2.0


# --------------------------------------------------------------------------
# 26-28. Determinism, replay equality, duplicate handling.
# --------------------------------------------------------------------------

def test_snapshot_is_deterministic_across_repeated_calls():
    engine = MarketStateEngine("BINANCE")
    engine.update(trade(BASE, 100.0, 1.0, "buy"))
    engine.update(book(BASE, bids=[(99.0, 1.0)], asks=[(101.0, 1.0)]))
    a, b = engine.snapshot(BASE + 10), engine.snapshot(BASE + 10)
    assert a.digest() == b.digest()


def test_identical_event_sequences_produce_identical_digests_regardless_of_insertion_order():
    events = [trade(BASE, 100.0, 1.0, "buy"), trade(BASE + 1, 101.0, 2.0, "sell"),
             book(BASE + 2, bids=[(99.0, 1.0)], asks=[(101.0, 1.0)])]
    forward = MarketStateEngine("BINANCE")
    for e in events:
        forward.update(e)
    backward = MarketStateEngine("BINANCE")
    for e in reversed(events):
        backward.update(e)
    assert forward.snapshot(BASE + 100).digest() == backward.snapshot(BASE + 100).digest()


def test_a_duplicate_trade_event_double_counts_at_this_layer_by_design():
    """Deduplication is the ingestion/book layer's job (LocalBook's
    duplicate detection, sequence comparators). This engine consumes
    whatever canonical events it is given and does not re-implement
    dedup -- feeding it the same event object twice is a caller error, not
    a case this layer silently absorbs. Documented explicitly so a future
    reader does not assume this engine deduplicates."""
    engine = MarketStateEngine("BINANCE")
    t = trade(BASE, 100.0, 1.0, "buy")
    engine.update(t)
    engine.update(t)
    state = engine.snapshot(BASE)
    assert state.trade_flow.trade_count == 2   # not deduplicated -- by design, see docstring


# --------------------------------------------------------------------------
# 29-33. Quality/recovery propagation, missing streams, venue isolation.
# --------------------------------------------------------------------------

def test_liquidation_stream_unavailable_is_distinct_from_zero_liquidations_observed():
    engine = MarketStateEngine("BINANCE")
    engine.update(trade(BASE, 100.0, 1.0, "buy"))   # other streams active; liquidations never fed
    state = engine.snapshot(BASE)
    assert state.liquidation.liquidations_observed is False
    assert state.liquidation.liquidation_count == 0
    # Both fields exist so "zero observed" and "never observed" are never
    # collapsed into a single 0 that could mean either.


def test_venue_mismatch_is_rejected_not_silently_mixed():
    engine = MarketStateEngine("BINANCE")
    import pytest
    with pytest.raises(ValueError, match="One engine describes one venue"):
        engine.update(trade(BASE, 100.0, 1.0, "buy", exchange="BYBIT"))


def test_partial_depth_book_source_is_refused_not_silently_accepted():
    """Defense in depth: a depth10-style partial snapshot never reflects the
    true best bid/ask beyond its own shallow window. run_collector.py and
    run_bybit_collector.py already filter it before persisting, but this
    engine must not rely solely on callers remembering that."""
    engine = MarketStateEngine("BINANCE")
    partial = book(BASE, bids=[(100.0, 999.0)], asks=[(101.0, 999.0)])
    partial = CanonicalOrderBookEvent(**{**partial.__dict__, "book_source": "PARTIAL_DEPTH"})
    engine.update(partial)
    state = engine.snapshot(BASE)
    assert state.book.book_available is False   # refused, not accepted as if authoritative


def test_two_engines_for_two_venues_do_not_collapse_into_one_state():
    binance_engine = MarketStateEngine("BINANCE")
    bybit_engine = MarketStateEngine("BYBIT")
    binance_engine.update(trade(BASE, 50_000.0, 1.0, "buy", exchange="BINANCE"))
    bybit_engine.update(trade(BASE, 49_990.0, 1.0, "buy", exchange="BYBIT"))
    b_state = binance_engine.snapshot(BASE)
    y_state = bybit_engine.snapshot(BASE)
    assert b_state.exchange == "BINANCE" and y_state.exchange == "BYBIT"
    assert b_state.price.last_trade_price != y_state.price.last_trade_price
    assert b_state.digest() != y_state.digest()


# --------------------------------------------------------------------------
# 35-36. No wall-clock, no network imports (structural check).
# --------------------------------------------------------------------------

def test_market_state_module_has_no_wall_clock_or_network_imports():
    import ast
    import inspect
    import collector.collector.market_state as module
    source = inspect.getsource(module)
    tree = ast.parse(source)
    forbidden_calls = {"time", "datetime", "socket", "requests", "aiohttp", "websockets"}
    imported_names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_names.add(node.module.split(".")[0])
    # hashlib is fine (pure, deterministic); time/datetime/network libs are not.
    assert not (imported_names & forbidden_calls), (
        f"market_state.py imports forbidden module(s): {imported_names & forbidden_calls}"
    )


# --------------------------------------------------------------------------
# 37-38. Digest sensitivity: changes when meaningful input changes, stable otherwise.
# --------------------------------------------------------------------------

def test_digest_changes_when_a_trade_price_changes():
    a = MarketStateEngine("BINANCE"); a.update(trade(BASE, 100.0, 1.0, "buy"))
    b = MarketStateEngine("BINANCE"); b.update(trade(BASE, 200.0, 1.0, "buy"))
    assert a.snapshot(BASE).digest() != b.snapshot(BASE).digest()


def test_digest_changes_when_a_liquidation_quantity_changes():
    a = MarketStateEngine("BINANCE"); a.update(liquidation(BASE, "buy", 1.0))
    b = MarketStateEngine("BINANCE"); b.update(liquidation(BASE, "buy", 2.0))
    assert a.snapshot(BASE).digest() != b.snapshot(BASE).digest()


def test_digest_changes_when_funding_changes():
    a = MarketStateEngine("BINANCE"); a.update(mark(BASE, funding_rate=0.0001))
    b = MarketStateEngine("BINANCE"); b.update(mark(BASE, funding_rate=0.0002))
    assert a.snapshot(BASE).digest() != b.snapshot(BASE).digest()


def test_digest_changes_when_oi_changes():
    a = MarketStateEngine("BINANCE"); a.update(oi(BASE, 1000.0))
    b = MarketStateEngine("BINANCE"); b.update(oi(BASE, 2000.0))
    assert a.snapshot(BASE).digest() != b.snapshot(BASE).digest()


def test_digest_changes_when_an_orderbook_level_changes():
    a = MarketStateEngine("BINANCE"); a.update(book(BASE, bids=[(100.0, 1.0)], asks=[(101.0, 1.0)]))
    b = MarketStateEngine("BINANCE"); b.update(book(BASE, bids=[(100.0, 5.0)], asks=[(101.0, 1.0)]))
    assert a.snapshot(BASE).digest() != b.snapshot(BASE).digest()


def test_digest_unaffected_by_an_event_never_fed_to_the_engine():
    """Mutation-test the causal cutoff itself: an event that exists but was
    never update()'d (i.e. never available to the collector at all) must
    have zero effect -- there is nothing to "leak" if it was never given."""
    a = MarketStateEngine("BINANCE"); a.update(trade(BASE, 100.0, 1.0, "buy"))
    b = MarketStateEngine("BINANCE"); b.update(trade(BASE, 100.0, 1.0, "buy"))
    b.update(trade(BASE + 999_999, 999.0, 999.0, "sell"))   # far in the future of our observation_ts
    assert a.snapshot(BASE).digest() == b.snapshot(BASE).digest()
