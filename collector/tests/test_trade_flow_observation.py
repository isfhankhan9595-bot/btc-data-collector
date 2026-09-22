"""observe_trade_flow_at (causal CVD): quality-gate tests.

Reuses the real trade-frame builders from test_replay_non_book_events.py
(``_binance_trade``, ``_bybit_trade``, ``_okx_frame``) rather than
duplicating them, and drives the real, unmodified ReplayEngine throughout
-- consistent with every other module built this session.
"""
from __future__ import annotations

import json

import pytest

from collector.collector.instrument import BINANCE_USDM_BTCUSDT, BYBIT_LINEAR_BTCUSDT, OKX_SWAP_BTCUSDT
from collector.collector.replay import FrameKind, ReplayFrame
from collector.collector.trade_flow_observation import observe_trade_flow_at
from collector.pipeline.cross_exchange_alignment import AlignmentStatus
from collector.tests.test_replay_non_book_events import _binance_trade, _bybit_trade, _okx_frame

BASE_TS = 1_781_000_000_000


# ---------------------------------------------------------------------------
# Normal behavior: CVD accumulates signed volume correctly.
# ---------------------------------------------------------------------------


def test_cvd_is_positive_when_buys_exceed_sells():
    frames = [
        _binance_trade(BASE_TS, "65000.0", agg_id=1, index=0),      # BUY (m=False), qty 0.5
        _binance_trade(BASE_TS + 1, "65001.0", agg_id=2, index=1),  # BUY, qty 0.5
    ]
    obs = observe_trade_flow_at(frames, BASE_TS + 1, venue="BINANCE")
    assert obs.status is AlignmentStatus.AVAILABLE
    assert obs.trade_count == 2
    assert obs.buy_volume == pytest.approx(1.0)
    assert obs.sell_volume == pytest.approx(0.0)
    assert obs.cvd == pytest.approx(1.0)


def test_cvd_nets_buys_against_sells():
    buy = _binance_trade(BASE_TS, "65000.0", agg_id=1, index=0)
    sell_payload = json.dumps({"stream": "btcusdt@aggTrade",
                               "data": {"e": "aggTrade", "E": BASE_TS + 1, "T": BASE_TS + 1,
                                        "a": 2, "p": "65000.0", "q": "0.3", "m": True}})  # m=True -> SELL
    sell = ReplayFrame(timestamp_ms=BASE_TS + 1, kind=FrameKind.WIRE, source_index=1, payload=sell_payload)
    obs = observe_trade_flow_at([buy, sell], BASE_TS + 1, venue="BINANCE")
    assert obs.buy_volume == pytest.approx(0.5)
    assert obs.sell_volume == pytest.approx(0.3)
    assert obs.cvd == pytest.approx(0.2)


# ---------------------------------------------------------------------------
# Cross-venue side-casing: Binance ("BUY"/"SELL"), Bybit ("Buy"/"Sell"),
# OKX ("buy"/"sell") must all classify correctly despite different casing.
# ---------------------------------------------------------------------------


def test_bybit_mixed_case_side_is_classified_correctly():
    frame = _bybit_trade(BASE_TS, "65000.0", index=0)   # side="Buy" per that fixture
    obs = observe_trade_flow_at([frame], BASE_TS, venue="BYBIT")
    assert obs.buy_volume and obs.buy_volume > 0
    assert obs.sell_volume == 0


def test_okx_lowercase_side_is_classified_correctly():
    frame = _okx_frame(BASE_TS, "trades", [{"ts": str(BASE_TS), "tradeId": "1", "px": "65000.0",
                                             "sz": "1.0", "side": "sell", "seqId": 1}])
    obs = observe_trade_flow_at([frame], BASE_TS, venue="OKX")
    assert obs.sell_volume == pytest.approx(1.0)
    assert obs.buy_volume == 0


def test_all_three_venues_agree_on_sign_for_equivalent_sides():
    """Not a claim that magnitudes are comparable across venues (they are
    not audited for that here) -- only that BUY-classification itself is
    venue-casing-independent, which is the specific defect this function's
    docstring names."""
    binance = observe_trade_flow_at([_binance_trade(BASE_TS, "1.0", index=0)], BASE_TS, venue="BINANCE")
    bybit = observe_trade_flow_at([_bybit_trade(BASE_TS, "1.0", index=0)], BASE_TS, venue="BYBIT")
    okx = observe_trade_flow_at(
        [_okx_frame(BASE_TS, "trades", [{"ts": str(BASE_TS), "tradeId": "1", "px": "1.0",
                                          "sz": "1.0", "side": "buy", "seqId": 1}])],
        BASE_TS, venue="OKX")
    for obs in (binance, bybit, okx):
        assert obs.buy_volume and obs.buy_volume > 0
        assert obs.sell_volume == 0


# ---------------------------------------------------------------------------
# Gate A (causality): a future trade must not contribute; exact boundary.
# ---------------------------------------------------------------------------


def test_causal_boundary_trade_exactly_at_t_is_included():
    frames = [_binance_trade(BASE_TS, "65000.0", agg_id=1, index=0)]
    obs = observe_trade_flow_at(frames, BASE_TS, venue="BINANCE")
    assert obs.trade_count == 1


def test_causal_boundary_trade_one_ms_after_t_is_excluded():
    frames = [_binance_trade(BASE_TS + 1, "65000.0", agg_id=1, index=0)]
    obs = observe_trade_flow_at(frames, BASE_TS, venue="BINANCE")
    assert obs.status is AlignmentStatus.NEVER_OBSERVED
    assert obs.trade_count == 0


def test_future_trade_does_not_pollute_cvd_of_a_past_observation():
    early = _binance_trade(BASE_TS, "65000.0", agg_id=1, index=0)          # BUY
    late_sell_payload = json.dumps({"stream": "btcusdt@aggTrade",
                                    "data": {"e": "aggTrade", "E": BASE_TS + 10_000, "T": BASE_TS + 10_000,
                                             "a": 2, "p": "1.0", "q": "100.0", "m": True}})
    late = ReplayFrame(timestamp_ms=BASE_TS + 10_000, kind=FrameKind.WIRE, source_index=1,
                       payload=late_sell_payload)
    obs_before = observe_trade_flow_at([early, late], BASE_TS, venue="BINANCE")
    obs_after = observe_trade_flow_at([early, late], BASE_TS + 10_000, venue="BINANCE")
    assert obs_before.cvd == pytest.approx(0.5)     # only the early BUY
    assert obs_after.cvd == pytest.approx(0.5 - 100.0)   # both now visible


# ---------------------------------------------------------------------------
# Gate F (missingness): no trades must never read as a fabricated zero CVD.
# ---------------------------------------------------------------------------


def test_no_causal_frames_is_never_observed_not_zero_cvd():
    frames = [_binance_trade(BASE_TS, "65000.0", agg_id=1, index=0)]
    obs = observe_trade_flow_at(frames, BASE_TS - 1, venue="BINANCE")
    assert obs.status is AlignmentStatus.NEVER_OBSERVED
    assert obs.cvd is None       # not 0.0 -- genuinely unobserved, not "no flow"
    assert obs.buy_volume is None and obs.sell_volume is None


def test_frames_considered_but_no_trade_produced_is_still_never_observed():
    malformed = ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=0,
                            payload="{not valid json", decode_ok=False)
    obs = observe_trade_flow_at([malformed], BASE_TS, venue="BINANCE")
    assert obs.status is AlignmentStatus.NEVER_OBSERVED
    assert obs.frames_considered == 1    # distinct from zero frames
    assert obs.cvd is None


# ---------------------------------------------------------------------------
# Gate E (staleness): stale CVD is retained, not discarded or zeroed.
# ---------------------------------------------------------------------------


def test_staleness_boundary_retains_the_last_known_cvd():
    frames = [_binance_trade(BASE_TS, "65000.0", agg_id=1, index=0)]
    fresh = observe_trade_flow_at(frames, BASE_TS + 4_999, venue="BINANCE", staleness_ms=5_000)
    stale = observe_trade_flow_at(frames, BASE_TS + 5_001, venue="BINANCE", staleness_ms=5_000)
    assert fresh.status is AlignmentStatus.AVAILABLE
    assert stale.status is AlignmentStatus.STALE
    assert stale.cvd == fresh.cvd    # retained, not discarded or zeroed


# ---------------------------------------------------------------------------
# Gate B (identity): correct per-venue instrument, no cross-venue collision.
# ---------------------------------------------------------------------------


def test_binance_observation_carries_binance_usdm_identity():
    obs = observe_trade_flow_at([_binance_trade(BASE_TS, "1.0", index=0)], BASE_TS, venue="BINANCE")
    assert obs.instrument == BINANCE_USDM_BTCUSDT


def test_bybit_and_okx_identities_do_not_collide():
    bybit = observe_trade_flow_at([_bybit_trade(BASE_TS, "1.0", index=0)], BASE_TS, venue="BYBIT")
    okx = observe_trade_flow_at(
        [_okx_frame(BASE_TS, "trades", [{"ts": str(BASE_TS), "tradeId": "1", "px": "1.0",
                                          "sz": "1.0", "side": "buy", "seqId": 1}])],
        BASE_TS, venue="OKX")
    assert bybit.instrument == BYBIT_LINEAR_BTCUSDT
    assert okx.instrument == OKX_SWAP_BTCUSDT
    assert bybit.instrument != okx.instrument


# ---------------------------------------------------------------------------
# Gate G/H (replay/determinism): same input, same result, order-independent.
# ---------------------------------------------------------------------------


def test_replaying_twice_gives_identical_cvd():
    frames = [_binance_trade(BASE_TS, "1.0", agg_id=1, index=0),
             _binance_trade(BASE_TS + 1, "1.0", agg_id=2, index=1)]
    a = observe_trade_flow_at(list(frames), BASE_TS + 1, venue="BINANCE")
    b = observe_trade_flow_at(list(frames), BASE_TS + 1, venue="BINANCE")
    assert a.cvd == b.cvd and a.trade_count == b.trade_count


def test_input_list_order_does_not_affect_the_result():
    frames = [_binance_trade(BASE_TS, "1.0", agg_id=1, index=0),
             _binance_trade(BASE_TS + 1, "1.0", agg_id=2, index=1)]
    forward = observe_trade_flow_at(list(frames), BASE_TS + 1, venue="BINANCE")
    backward = observe_trade_flow_at(list(reversed(frames)), BASE_TS + 1, venue="BINANCE")
    assert forward.cvd == backward.cvd


# ---------------------------------------------------------------------------
# Type validation.
# ---------------------------------------------------------------------------


def test_bool_observation_ts_is_rejected():
    with pytest.raises(TypeError):
        observe_trade_flow_at([_binance_trade(BASE_TS, "1.0", index=0)], True, venue="BINANCE")


def test_float_observation_ts_is_rejected():
    with pytest.raises(TypeError):
        observe_trade_flow_at([_binance_trade(BASE_TS, "1.0", index=0)], float(BASE_TS), venue="BINANCE")


# ---------------------------------------------------------------------------
# Mutation-style leakage test: removing the causal filter must be caught.
# ---------------------------------------------------------------------------


def test_mutation_removing_the_causal_filter_would_be_caught():
    """Directly performs the task's own Mutation B (remove the causal
    filter) against a hand-copied version of the function, and confirms
    the real function's output differs from the mutated one -- proving
    test_future_trade_does_not_pollute_cvd_of_a_past_observation above
    would actually fail if this regression were ever introduced into the
    real module, not merely asserting it would."""
    from collector.collector.canonical import CanonicalTradeEvent
    from collector.collector.replay import ReplayEngine, ReplaySource

    def mutated_no_causal_filter(frames, observation_ts, *, venue):
        # BUG: does not filter frames by timestamp_ms <= observation_ts.
        engine = ReplayEngine(venue=venue)
        engine.run(ReplaySource(list(frames)))
        trades = [e for e in engine.result.non_book_events if isinstance(e, CanonicalTradeEvent)]
        buy = sum(t.quantity for t in trades if (t.side or "").upper() == "BUY")
        sell = sum(t.quantity for t in trades if (t.side or "").upper() == "SELL")
        return buy - sell

    early = _binance_trade(BASE_TS, "65000.0", agg_id=1, index=0)
    late_sell_payload = json.dumps({"stream": "btcusdt@aggTrade",
                                    "data": {"e": "aggTrade", "E": BASE_TS + 10_000, "T": BASE_TS + 10_000,
                                             "a": 2, "p": "1.0", "q": "100.0", "m": True}})
    late = ReplayFrame(timestamp_ms=BASE_TS + 10_000, kind=FrameKind.WIRE, source_index=1,
                       payload=late_sell_payload)
    frames = [early, late]

    real_result = observe_trade_flow_at(frames, BASE_TS, venue="BINANCE").cvd
    mutated_result = mutated_no_causal_filter(frames, BASE_TS, venue="BINANCE")
    assert real_result != mutated_result   # the mutation leaks the future sell; the real function does not
    assert real_result == pytest.approx(0.5)
    assert mutated_result == pytest.approx(0.5 - 100.0)
