"""Non-order-book canonical events must survive replay, for every venue.

Before this fix, ``ReplayEngine._handle_wire`` did::

    for event in self.adapter.normalize(...):
        if not isinstance(event, CanonicalOrderBookEvent):
            continue

silently dropping every trade, mark/index/funding, open-interest and
liquidation event a venue adapter produced. Replay only ever proved order
books reconstructed correctly; nothing exercised the rest of what the
collector actually records. Separately, ``ReplayEngine`` had no "OKX" entry
in its venue->adapter map at all, so ``ReplayEngine("OKX")`` raised
``ValueError`` regardless of this bug.

Both are fixed together: OKX is registered, and every canonical event type
an adapter yields is preserved in ``ReplayResult.non_book_events`` (order
books still go through ``LocalBook`` as before; nothing about book replay
changes here).
"""
from __future__ import annotations

import json

from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.adapters.okx import OKXAdapter
from collector.collector.canonical import (
    CanonicalLiquidationEvent,
    CanonicalMarkPriceEvent,
    CanonicalOIEvent,
    CanonicalTradeEvent,
)
from collector.collector.replay import FrameKind, ReplayEngine, ReplayFrame, ReplaySource

BASE_TS = 1_780_555_555_000


def _wire(ts, payload, index=0):
    return ReplayFrame(timestamp_ms=ts, kind=FrameKind.WIRE, source_index=index, payload=payload)


# ---------------------------------------------------------------------------
# OKX registration
# ---------------------------------------------------------------------------


def test_replay_engine_selects_okx_adapter():
    engine = ReplayEngine(venue="OKX")
    assert isinstance(engine.adapter, OKXAdapter)


# ---------------------------------------------------------------------------
# Binance non-book events
# ---------------------------------------------------------------------------


def _binance_trade(ts, price, agg_id=1, index=0):
    return _wire(ts, json.dumps({
        "stream": "btcusdt@aggTrade",
        "data": {"e": "aggTrade", "E": ts, "T": ts, "a": agg_id, "p": price, "q": "0.5", "m": False},
    }), index)


def _binance_mark(ts, mark, funding, index=0):
    return _wire(ts, json.dumps({
        "stream": "btcusdt@markPrice@1s",
        "data": {"e": "markPriceUpdate", "E": ts, "p": mark, "i": mark, "r": funding, "T": ts + 1000},
    }), index)


def _binance_liquidation(ts, price, index=0):
    return _wire(ts, json.dumps({
        "stream": "btcusdt@forceOrder",
        "data": {"e": "forceOrder", "E": ts, "o": {"T": ts, "S": "SELL", "p": price, "q": "1.0"}},
    }), index)


def test_binance_trade_survives_replay_as_a_non_book_event():
    result = ReplayEngine(venue="BINANCE").run(ReplaySource([_binance_trade(BASE_TS, "65000.5")]))
    assert result.book_updates == []
    assert len(result.non_book_events) == 1
    event = result.non_book_events[0]
    assert isinstance(event, CanonicalTradeEvent)
    assert event.exchange == "BINANCE" and event.stream == "trades"
    assert event.price == 65000.5 and event.side == "BUY"


def test_binance_mark_price_and_liquidation_survive_replay():
    frames = [
        _binance_mark(BASE_TS, "65000.0", "0.0001", index=0),
        _binance_liquidation(BASE_TS + 10, "64900.0", index=1),
    ]
    result = ReplayEngine(venue="BINANCE").run(ReplaySource(frames))
    assert len(result.non_book_events) == 2
    mark, liq = result.non_book_events
    assert isinstance(mark, CanonicalMarkPriceEvent) and mark.mark_price == 65000.0
    assert isinstance(liq, CanonicalLiquidationEvent) and liq.price == 64900.0 and liq.side == "SELL"


def test_binance_book_and_trade_in_the_same_replay_both_survive():
    """A regression against the isinstance-filter bug specifically: a
    session with both event kinds must not lose one to the other."""
    from tests.test_replay import _depth_frame  # existing Binance book fixture builder
    frames = [
        _depth_frame(BASE_TS, U=100, u=105, pu=None, index=0),
        _binance_trade(BASE_TS + 5, "65000.0", index=1),
    ]
    result = ReplayEngine(venue="BINANCE").run(ReplaySource(frames))
    assert len(result.non_book_events) == 1
    assert isinstance(result.non_book_events[0], CanonicalTradeEvent)
    # The book side is buffered pre-bridge in this fixture (no snapshot), so
    # asserting it didn't error out is the relevant check here, not a
    # specific book_updates count -- that path is covered exhaustively by
    # test_replay.py already. The point is the trade wasn't lost.
    assert result.frames_undecodable == 0


# ---------------------------------------------------------------------------
# Bybit non-book events, including carried-forward ticker provenance
# ---------------------------------------------------------------------------


def _bybit_trade(ts, price, trade_id="t1", index=0):
    return _wire(ts, json.dumps({
        "topic": "publicTrade.BTCUSDT", "ts": ts,
        "data": [{"i": trade_id, "T": ts, "p": price, "v": "0.2", "S": "Buy"}],
    }), index)


def _bybit_ticker(ts, *, is_snapshot, fields, index=0):
    return _wire(ts, json.dumps({
        "topic": "tickers.BTCUSDT", "type": "snapshot" if is_snapshot else "delta",
        "ts": ts, "data": fields,
    }), index)


def _bybit_liquidation(ts, price, index=0):
    return _wire(ts, json.dumps({
        "topic": "allLiquidation.BTCUSDT", "ts": ts,
        "data": [{"T": ts, "S": "Sell", "p": price, "v": "0.3"}],
    }), index)


def test_bybit_trade_and_liquidation_survive_replay():
    frames = [_bybit_trade(BASE_TS, "65000.0", index=0), _bybit_liquidation(BASE_TS + 10, "64800.0", index=1)]
    result = ReplayEngine(venue="BYBIT").run(ReplaySource(frames))
    assert len(result.non_book_events) == 2
    trade, liq = result.non_book_events
    assert isinstance(trade, CanonicalTradeEvent) and trade.price == 65000.0 and trade.side == "Buy"
    assert isinstance(liq, CanonicalLiquidationEvent) and liq.price == 64800.0


def test_bybit_ticker_snapshot_produces_both_markprice_and_oi_events():
    """One raw frame legitimately yields two distinct canonical event types
    here (see BybitAdapter.normalize's ``route == "ticker"`` branch) -- both
    must survive, neither silently dropped in favour of the other."""
    frame = _bybit_ticker(BASE_TS, is_snapshot=True, fields={
        "markPrice": "65000.0", "indexPrice": "64990.0", "fundingRate": "0.0001",
        "nextFundingTime": str(BASE_TS + 3_600_000), "openInterest": "12345.0",
    })
    result = ReplayEngine(venue="BYBIT").run(ReplaySource([frame]))
    types = {type(e) for e in result.non_book_events}
    assert types == {CanonicalMarkPriceEvent, CanonicalOIEvent}


def test_bybit_ticker_carried_forward_provenance_survives_replay_unaltered():
    """The exact concern the task calls out: a later delta that omits a
    field must report it as carried-forward, not as a fresh observation --
    and that must hold identically whether the frames are driven by replay
    or by live ingestion, because both drive the same adapter instance
    method by method, in the same order."""
    snapshot = _bybit_ticker(BASE_TS, is_snapshot=True, fields={
        "markPrice": "65000.0", "indexPrice": "64990.0", "fundingRate": "0.0001",
        "nextFundingTime": str(BASE_TS + 3_600_000),
    }, index=0)
    # Delta only updates markPrice; index/funding must show as carried forward.
    delta = _bybit_ticker(BASE_TS + 1000, is_snapshot=False, fields={"markPrice": "65010.0"}, index=1)
    result = ReplayEngine(venue="BYBIT").run(ReplaySource([snapshot, delta]))
    mark_events = [e for e in result.non_book_events if isinstance(e, CanonicalMarkPriceEvent)]
    assert len(mark_events) == 2
    fresh, carried = mark_events
    assert fresh.carried_forward == ()
    assert "indexPrice" not in fresh.carried_forward  # sanity: nothing bogus on the first event either
    assert set(carried.carried_forward) == {"indexPrice", "fundingRate", "nextFundingTime"}
    assert carried.mark_price == 65010.0
    assert carried.index_price == 64990.0  # the carried value, not fabricated as new


# ---------------------------------------------------------------------------
# OKX non-book events: trades, trades-all (kept distinct), mark-price,
# index-tickers, funding-rate, open-interest, liquidation-orders
# ---------------------------------------------------------------------------


def _okx_frame(ts, channel, data, index=0):
    return _wire(ts, json.dumps({"arg": {"channel": channel, "instId": "BTC-USDT-SWAP"}, "data": data}), index)


def test_okx_trades_and_trades_all_are_never_conflated():
    """Different channels, different stream names, carried through
    distinctly even though both produce CanonicalTradeEvent."""
    frames = [
        _okx_frame(BASE_TS, "trades", [{"ts": str(BASE_TS), "tradeId": "1", "px": "65000.0",
                                        "sz": "1.0", "side": "buy", "seqId": 5}], index=0),
        _okx_frame(BASE_TS + 10, "trades-all", [{"ts": str(BASE_TS + 10), "tradeId": "2", "px": "65001.0",
                                                  "sz": "0.5", "side": "sell", "source": "1"}], index=1),
    ]
    result = ReplayEngine(venue="OKX").run(ReplaySource(frames))
    assert len(result.non_book_events) == 2
    trades, trade_id_by_stream = result.non_book_events, {}
    for event in trades:
        assert isinstance(event, CanonicalTradeEvent)
        trade_id_by_stream[event.stream] = event.trade_id
    assert trade_id_by_stream == {"trades": "1", "trades-all": "2"}
    trades_all_event = next(e for e in trades if e.stream == "trades-all")
    assert trades_all_event.source == "1"  # ELP flag, unique to trades-all


def test_okx_mark_price_and_index_tickers_stay_on_separate_fields():
    frames = [
        _okx_frame(BASE_TS, "mark-price", [{"ts": str(BASE_TS), "markPx": "65000.0"}], index=0),
        _okx_frame(BASE_TS + 10, "index-tickers", [{"ts": str(BASE_TS + 10), "idxPx": "64995.0"}], index=1),
    ]
    result = ReplayEngine(venue="OKX").run(ReplaySource(frames))
    mark, index = result.non_book_events
    assert mark.stream == "mark-price" and mark.mark_price == 65000.0 and mark.index_price is None
    assert index.stream == "index-tickers" and index.index_price == 64995.0 and index.mark_price is None


def test_okx_funding_rate_channel_survives_replay():
    frame = _okx_frame(BASE_TS, "funding-rate", [{
        "ts": str(BASE_TS), "fundingRate": "0.0001", "nextFundingTime": str(BASE_TS + 3_600_000),
        "fundingTime": str(BASE_TS), "settFundingRate": "0.00005", "settState": "settled",
    }])
    result = ReplayEngine(venue="OKX").run(ReplaySource([frame]))
    assert len(result.non_book_events) == 1
    event = result.non_book_events[0]
    assert event.stream == "funding-rate" and event.funding_rate == 0.0001
    assert event.sett_funding_rate == 0.00005 and event.sett_state == "settled"


def test_okx_open_interest_preserves_all_three_units():
    frame = _okx_frame(BASE_TS, "open-interest", [{
        "ts": str(BASE_TS), "oi": "12000", "oiCcy": "1200.5", "oiUsd": "780000000",
    }])
    result = ReplayEngine(venue="OKX").run(ReplaySource([frame]))
    event = result.non_book_events[0]
    assert isinstance(event, CanonicalOIEvent)
    assert (event.open_interest, event.oi_ccy, event.oi_usd) == (12000.0, 1200.5, 780000000.0)


def test_okx_liquidation_orders_preserves_attribution_and_never_filters_by_instrument():
    """The task's explicit invariant: the parser (and therefore replay,
    which drives the same parser) must NOT filter by instId -- a push
    carrying another instrument's liquidation must still produce an event,
    with inst_id intact for a downstream layer to filter on."""
    frame = _okx_frame(BASE_TS, "liquidation-orders", [
        {"instId": "BTC-USDT-SWAP", "instFamily": "BTC-USDT", "uly": "BTC-USDT",
         "details": [{"ts": str(BASE_TS), "side": "sell", "bkPx": "64800.0", "sz": "2.0",
                      "bkLoss": "150.0", "ccy": "", "posSide": "long"}]},
        {"instId": "ETH-USDT-SWAP", "instFamily": "ETH-USDT", "uly": "ETH-USDT",
         "details": [{"ts": str(BASE_TS + 1), "side": "buy", "bkPx": "3000.0", "sz": "5.0"}]},
    ])
    result = ReplayEngine(venue="OKX").run(ReplaySource([frame]))
    assert len(result.non_book_events) == 2  # NOT filtered down to one instrument
    btc_event = next(e for e in result.non_book_events if e.inst_id == "BTC-USDT-SWAP")
    assert btc_event.ccy == ""  # observed empty string, not coerced to None
    assert btc_event.pos_side == "long" and btc_event.bk_loss == 150.0
    eth_event = next(e for e in result.non_book_events if e.inst_id == "ETH-USDT-SWAP")
    assert eth_event.price == 3000.0


# ---------------------------------------------------------------------------
# Digest sensitivity and determinism
# ---------------------------------------------------------------------------


def test_digest_changes_when_a_non_book_trade_price_changes():
    base = [_binance_trade(BASE_TS, "65000.0")]
    changed = [_binance_trade(BASE_TS, "65000.01")]
    digest_a = ReplayEngine(venue="BINANCE").run(ReplaySource(base)).digest
    digest_b = ReplayEngine(venue="BINANCE").run(ReplaySource(changed)).digest
    assert digest_a != digest_b


def test_digest_changes_when_a_trade_disappears():
    with_trade = ReplayEngine(venue="BINANCE").run(ReplaySource([_binance_trade(BASE_TS, "65000.0")])).digest
    without_trade = ReplayEngine(venue="BINANCE").run(ReplaySource([])).digest
    assert with_trade != without_trade


def test_digest_changes_for_okx_funding_oi_and_liquidation_edits():
    def digest_for(funding_rate):
        frame = _okx_frame(BASE_TS, "funding-rate", [{"ts": str(BASE_TS), "fundingRate": funding_rate}])
        return ReplayEngine(venue="OKX").run(ReplaySource([frame])).digest
    assert digest_for("0.0001") != digest_for("0.0002")

    def oi_digest(oi):
        frame = _okx_frame(BASE_TS, "open-interest", [{"ts": str(BASE_TS), "oi": oi}])
        return ReplayEngine(venue="OKX").run(ReplaySource([frame])).digest
    assert oi_digest("12000") != oi_digest("12001")

    def liq_digest(qty):
        frame = _okx_frame(BASE_TS, "liquidation-orders", [
            {"instId": "BTC-USDT-SWAP",
             "details": [{"ts": str(BASE_TS), "side": "sell", "bkPx": "64800.0", "sz": qty}]}])
        return ReplayEngine(venue="OKX").run(ReplaySource([frame])).digest
    assert liq_digest("2.0") != liq_digest("2.1")


def test_replay_is_deterministic_across_two_runs_with_non_book_events():
    frames = [
        _binance_trade(BASE_TS, "65000.0", index=0),
        _binance_mark(BASE_TS + 10, "65001.0", "0.0001", index=1),
        _binance_liquidation(BASE_TS + 20, "64999.0", index=2),
    ]
    digest_1 = ReplayEngine(venue="BINANCE").run(ReplaySource(frames)).digest
    digest_2 = ReplayEngine(venue="BINANCE").run(ReplaySource(frames)).digest
    assert digest_1 == digest_2


def test_replay_digest_is_order_independent_for_ties_but_causal_overall():
    """ReplaySource sorts by (timestamp, kind_rank, source_index) -- feeding
    frames in a different original-list order must not change the result,
    since the total order key, not list position, decides sequencing."""
    frame_a = _binance_trade(BASE_TS, "65000.0", agg_id=1, index=0)
    frame_b = _binance_mark(BASE_TS + 10, "65001.0", "0.0001", index=1)
    forward = ReplayEngine(venue="BINANCE").run(ReplaySource([frame_a, frame_b])).digest
    reversed_input = ReplayEngine(venue="BINANCE").run(ReplaySource([frame_b, frame_a])).digest
    assert forward == reversed_input


def test_okx_malformed_funding_rate_does_not_produce_a_fabricated_event():
    """Missing the one required field (ts) must surface as unhandled, never
    as a CanonicalMarkPriceEvent with a guessed timestamp -- pins the
    adapter's own MALFORMED_PAYLOAD contract as seen through replay."""
    frame = _okx_frame(BASE_TS, "funding-rate", [{"fundingRate": "0.0001"}])  # no "ts"
    result = ReplayEngine(venue="OKX").run(ReplaySource([frame]))
    assert result.non_book_events == []
    assert result.frames_unhandled == 1
