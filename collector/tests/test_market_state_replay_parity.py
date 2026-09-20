"""Market State V0 replay parity.

Drives real recorded frames through the real ``ReplayEngine`` to get real
``adapter.normalize()`` output (``result.non_book_events``), then through
the real ``MarketStateEngine`` -- not a hand-built canonical event and not a
second, test-only calculation path. This is the same discipline
``test_replay.py``/``test_replay_venue.py`` apply to the book engine,
applied here to market state.

Scope, stated plainly: ``ReplayResult.book_updates`` records only
``best_bid``/``best_ask`` as strings (see ``replay.py``'s ``BookUpdate``),
not full bid/ask arrays, so it cannot be turned back into a
``CanonicalOrderBookEvent`` for feeding ``MarketStateEngine``. This file
therefore proves replay parity for the non-book dimensions (trade flow,
liquidations, mark/funding, OI) only. Book-state replay parity is not
claimed here.
"""
from __future__ import annotations

import json

from collector.collector.market_state import MarketStateEngine
from collector.collector.replay import FrameKind, ReplayEngine, ReplayFrame, ReplaySource

BASE = 1_780_000_000_000


def _wire(ts, payload, index):
    return ReplayFrame(timestamp_ms=ts, kind=FrameKind.WIRE, source_index=index,
                       payload=json.dumps(payload))


def _bybit_session():
    return [
        _wire(BASE, {"topic": "publicTrade.BTCUSDT", "ts": BASE,
                    "data": [{"T": BASE, "s": "BTCUSDT", "S": "Buy", "v": "0.5",
                              "p": "50000.0", "i": "t1"}]}, 0),
        _wire(BASE + 10, {"topic": "publicTrade.BTCUSDT", "ts": BASE + 10,
                          "data": [{"T": BASE + 10, "s": "BTCUSDT", "S": "Sell", "v": "0.2",
                                    "p": "49999.0", "i": "t2"}]}, 1),
        _wire(BASE + 20, {"topic": "tickers.BTCUSDT", "type": "snapshot", "ts": BASE + 20,
                          "data": {"symbol": "BTCUSDT", "markPrice": "50000.5",
                                    "indexPrice": "50000.4", "fundingRate": "0.0001",
                                    "nextFundingTime": str(BASE + 3_600_000),
                                    "openInterest": "12345.6"}}, 2),
        _wire(BASE + 30, {"topic": "allLiquidation.BTCUSDT", "ts": BASE + 30,
                          "data": [{"T": BASE + 30, "s": "BTCUSDT", "S": "Sell",
                                    "v": "1.2", "p": "49900"}]}, 3),
    ]


def _run_market_state(observation_ts: int) -> "MarketState":  # noqa: F821
    result = ReplayEngine(venue="BYBIT").run(ReplaySource(_bybit_session()))
    engine = MarketStateEngine("BYBIT")
    for event in result.non_book_events:
        engine.update(event)
    return engine.snapshot(observation_ts)


def test_replaying_the_same_session_twice_produces_identical_market_state():
    a = _run_market_state(BASE + 100)
    b = _run_market_state(BASE + 100)
    assert a.digest() == b.digest()
    assert a.trade_flow.cvd == b.trade_flow.cvd
    assert a.derivatives.funding_rate == b.derivatives.funding_rate


def test_replayed_non_book_events_actually_reached_the_state_engine():
    """Not a vacuous digest-equality check: prove real values landed."""
    state = _run_market_state(BASE + 100)
    assert state.trade_flow.cumulative_buy_volume == 0.5
    assert state.trade_flow.cumulative_sell_volume == 0.2
    assert state.derivatives.funding_rate == 0.0001
    assert state.derivatives.open_interest == 12345.6
    assert state.liquidation.sell_side_quantity == 1.2


def test_market_state_before_the_liquidation_event_excludes_it():
    """Causal cutoff through the full replay -> state pipeline, not just
    the unit-level engine tests."""
    early = _run_market_state(BASE + 25)   # before the liquidation at BASE+30
    assert early.liquidation.liquidations_observed is False
    assert early.trade_flow.trades_observed is True   # trades before +25 still count


def test_mutating_the_replayed_trade_price_changes_the_resulting_digest():
    """The mutated event is the *first* trade; by BASE+100 a second,
    unmutated trade has since become the most recent one, and
    last_trade_price correctly reflects the latest trade, not any trade --
    so the digest at BASE+100 would legitimately be unaffected by this
    particular mutation. Checked instead at BASE+5, the point at which the
    mutated trade genuinely is the most recent price observation, which is
    where a real effect must show up."""
    baseline = _run_market_state(BASE + 5)

    def mutated_session():
        session = _bybit_session()
        session[0] = _wire(BASE, {"topic": "publicTrade.BTCUSDT", "ts": BASE,
                                  "data": [{"T": BASE, "s": "BTCUSDT", "S": "Buy", "v": "0.5",
                                            "p": "999999.0", "i": "t1"}]}, 0)
        return session

    result = ReplayEngine(venue="BYBIT").run(ReplaySource(mutated_session()))
    engine = MarketStateEngine("BYBIT")
    for event in result.non_book_events:
        engine.update(event)
    mutated = engine.snapshot(BASE + 5)

    assert mutated.digest() != baseline.digest()
    assert mutated.price.last_trade_price == 999999.0


def test_mutating_the_replayed_liquidation_quantity_changes_the_digest():
    baseline = _run_market_state(BASE + 100)

    def mutated_session():
        session = _bybit_session()
        session[3] = _wire(BASE + 30, {"topic": "allLiquidation.BTCUSDT", "ts": BASE + 30,
                                       "data": [{"T": BASE + 30, "s": "BTCUSDT", "S": "Sell",
                                                 "v": "99.0", "p": "49900"}]}, 3)
        return session

    result = ReplayEngine(venue="BYBIT").run(ReplaySource(mutated_session()))
    engine = MarketStateEngine("BYBIT")
    for event in result.non_book_events:
        engine.update(event)
    mutated = engine.snapshot(BASE + 100)

    assert mutated.digest() != baseline.digest()
    assert mutated.liquidation.sell_side_quantity == 99.0
