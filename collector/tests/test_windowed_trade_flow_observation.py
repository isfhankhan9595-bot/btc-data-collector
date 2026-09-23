"""observe_windowed_trade_flow_at: causal windowed CVD.

Independent audit of the merged cumulative CVD (PR #46) was performed
before writing this file -- see the audit findings recorded directly in
trade_flow_observation.py's docstrings (unknown-side handling, confirmed
by test below; the pre-existing duplicate-trade-message gap, documented
and deliberately not fixed here). No defect was found that blocks
building windowed CVD on top of the existing primitive, so this reuses
_causal_trades / _side_volumes rather than reimplementing replay.

Window contract under test: (window_start, window_end] =
(observation_ts - window_ms, observation_ts]. A trade exactly at
window_start is EXCLUDED; a trade exactly at observation_ts is INCLUDED.
"""
from __future__ import annotations

import json

import pytest

from collector.collector.instrument import BINANCE_USDM_BTCUSDT, BYBIT_LINEAR_BTCUSDT, OKX_SWAP_BTCUSDT
from collector.collector.replay import FrameKind, ReplayFrame
from collector.collector.trade_flow_observation import (
    observe_trade_flow_at,
    observe_windowed_trade_flow_at,
)
from collector.pipeline.cross_exchange_alignment import AlignmentStatus
from collector.tests.test_replay_non_book_events import _binance_trade, _bybit_trade, _okx_frame

T = 1_000_000   # observation_ts for boundary tests, chosen far from 0 so T-W stays positive
W = 60_000      # 1-minute window


def _binance_trade_at(ts, agg_id):
    return _binance_trade(ts, "65000.0", agg_id=agg_id, index=agg_id)


# ---------------------------------------------------------------------------
# Independent audit finding: unknown side is excluded from both sums, never
# guessed into a direction -- confirmed by direct test, not by reading code.
# ---------------------------------------------------------------------------


def test_unknown_side_contributes_to_neither_buy_nor_sell_but_is_still_counted():
    payload = json.dumps({"topic": "publicTrade.BTCUSDT", "ts": T,
                          "data": [{"T": T, "p": "100", "v": "5", "i": "t1"}]})   # no "S" key
    frame = ReplayFrame(timestamp_ms=T, kind=FrameKind.WIRE, source_index=0, payload=payload)
    obs = observe_trade_flow_at([frame], T, venue="BYBIT")
    assert obs.trade_count == 1
    assert obs.buy_volume == 0 and obs.sell_volume == 0   # never guessed as BUY or SELL
    assert obs.cvd == 0


def test_windowed_unknown_side_also_excluded_from_both_sums():
    payload = json.dumps({"topic": "publicTrade.BTCUSDT", "ts": T,
                          "data": [{"T": T, "p": "100", "v": "5", "i": "t1"}]})
    frame = ReplayFrame(timestamp_ms=T, kind=FrameKind.WIRE, source_index=0, payload=payload)
    obs = observe_windowed_trade_flow_at([frame], T, W, venue="BYBIT")
    assert obs.trade_count == 1
    assert obs.buy_volume == 0 and obs.sell_volume == 0


# ---------------------------------------------------------------------------
# Window boundary contract: (T-W, T].
# ---------------------------------------------------------------------------


def test_trade_at_window_start_is_excluded():
    frame = _binance_trade_at(T - W, agg_id=1)
    obs = observe_windowed_trade_flow_at([frame], T, W, venue="BINANCE")
    assert obs.status is AlignmentStatus.NEVER_OBSERVED
    assert obs.trade_count == 0


def test_trade_at_window_start_plus_one_is_included():
    # staleness_ms set >= window_ms deliberately: this test is about
    # boundary INCLUSION, not staleness (a trade this close to window_start
    # is, by construction, close to W old -- STALE under the default 5s
    # threshold is a separate, correctly-triggered concern, not a bug in
    # the boundary logic under test here).
    frame = _binance_trade_at(T - W + 1, agg_id=1)
    obs = observe_windowed_trade_flow_at([frame], T, W, venue="BINANCE", staleness_ms=W)
    assert obs.status is AlignmentStatus.AVAILABLE
    assert obs.trade_count == 1


def test_trade_at_observation_ts_is_included():
    frame = _binance_trade_at(T, agg_id=1)
    obs = observe_windowed_trade_flow_at([frame], T, W, venue="BINANCE")
    assert obs.trade_count == 1


def test_trade_one_ms_after_observation_ts_is_excluded():
    frame = _binance_trade_at(T + 1, agg_id=1)
    obs = observe_windowed_trade_flow_at([frame], T, W, venue="BINANCE")
    assert obs.status is AlignmentStatus.NEVER_OBSERVED
    assert obs.trade_count == 0


def test_window_start_and_end_fields_are_correct():
    obs = observe_windowed_trade_flow_at([_binance_trade_at(T, 1)], T, W, venue="BINANCE")
    assert obs.window_start == T - W
    assert obs.window_end == T
    assert obs.window_ms == W


# ---------------------------------------------------------------------------
# A trade older than the window (but causally known) must not appear in the
# window's tally -- distinguishes windowed from cumulative CVD directly.
# ---------------------------------------------------------------------------


def test_a_trade_older_than_the_window_does_not_appear_in_the_window_but_does_in_cumulative():
    old = _binance_trade_at(T - W - 1_000, agg_id=1)   # well before the window opens
    recent = _binance_trade_at(T, agg_id=2)
    frames = [old, recent]
    windowed = observe_windowed_trade_flow_at(frames, T, W, venue="BINANCE")
    cumulative = observe_trade_flow_at(frames, T, venue="BINANCE")
    assert windowed.trade_count == 1        # only the recent trade
    assert cumulative.trade_count == 2      # both, cumulative has no lower bound


# ---------------------------------------------------------------------------
# Gate A (causality): future-trade adversarial test, exactly as specified.
# ---------------------------------------------------------------------------


def test_future_trade_does_not_affect_a_past_windowed_observation():
    early_buy = _binance_trade_at(T - 1, agg_id=1)   # BUY, qty 0.5
    future_sell_payload = json.dumps({"stream": "btcusdt@aggTrade",
                                      "data": {"e": "aggTrade", "E": T + 1, "T": T + 1,
                                               "a": 2, "p": "1.0", "q": "1000.0", "m": True}})
    future_sell = ReplayFrame(timestamp_ms=T + 1, kind=FrameKind.WIRE, source_index=1,
                              payload=future_sell_payload)
    frames = [early_buy, future_sell]

    at_t = observe_windowed_trade_flow_at(frames, T, W, venue="BINANCE")
    assert at_t.trade_count == 1
    assert at_t.cvd == pytest.approx(0.5)   # the future sell contributes nothing

    at_t_plus_1 = observe_windowed_trade_flow_at(frames, T + 1, W, venue="BINANCE")
    assert at_t_plus_1.trade_count == 2
    assert at_t_plus_1.cvd == pytest.approx(0.5 - 1000.0)   # now visible


def test_exchange_timestamp_cannot_grant_early_window_eligibility():
    """The exchange event timestamp claims a time inside the window; the
    frame's own causal availability (timestamp_ms) is after observation_ts
    -- only the latter may govern eligibility."""
    late_payload = json.dumps({"stream": "btcusdt@aggTrade",
                               "data": {"e": "aggTrade", "E": T - 100, "T": T - 100,
                                        "a": 1, "p": "1.0", "q": "1.0", "m": False}})
    # Frame's causal timestamp_ms is T + 5000 despite the payload's exchange
    # timestamp claiming T - 100 (well inside the window).
    late_frame = ReplayFrame(timestamp_ms=T + 5_000, kind=FrameKind.WIRE, source_index=0,
                             payload=late_payload)
    obs = observe_windowed_trade_flow_at([late_frame], T, W, venue="BINANCE")
    assert obs.status is AlignmentStatus.NEVER_OBSERVED   # not available despite the exchange ts


# ---------------------------------------------------------------------------
# Gate F / Case B, C, D (missingness and completeness distinctions).
# ---------------------------------------------------------------------------


def test_case_a_no_trades_in_a_window_that_has_older_data_is_never_observed_not_zero():
    old = _binance_trade_at(T - W - 1_000, agg_id=1)
    obs = observe_windowed_trade_flow_at([old], T, W, venue="BINANCE")
    assert obs.status is AlignmentStatus.NEVER_OBSERVED
    assert obs.cvd is None   # not 0.0 -- no trades fell in this window, not "zero net flow"


def test_case_d_malformed_frames_only_is_never_observed_not_a_complete_zero_window():
    malformed = ReplayFrame(timestamp_ms=T, kind=FrameKind.WIRE, source_index=0,
                            payload="{not valid json", decode_ok=False)
    obs = observe_windowed_trade_flow_at([malformed], T, W, venue="BINANCE")
    assert obs.status is AlignmentStatus.NEVER_OBSERVED
    assert obs.frames_considered == 1
    assert obs.cvd is None


def test_case_c_stale_last_trade_retains_cvd_marked_stale():
    frame = _binance_trade_at(T - W + 1, agg_id=1)
    fresh = observe_windowed_trade_flow_at([frame], T - W + 1 + 4_999, W, venue="BINANCE", staleness_ms=5_000)
    stale = observe_windowed_trade_flow_at([frame], T - W + 1 + 5_001, W, venue="BINANCE", staleness_ms=5_000)
    assert fresh.status is AlignmentStatus.AVAILABLE
    assert stale.status is AlignmentStatus.STALE
    assert stale.cvd == fresh.cvd   # retained, not discarded


# ---------------------------------------------------------------------------
# Side normalization regression (must not bypass the existing normalization).
# ---------------------------------------------------------------------------


def test_windowed_cvd_normalizes_bybit_title_case_and_okx_lowercase_sides():
    bybit_buy = _bybit_trade(T, "1.0", index=0)          # side="Buy"
    okx_sell = _okx_frame(T, "trades", [{"ts": str(T), "tradeId": "1", "px": "1.0",
                                         "sz": "1.0", "side": "sell", "seqId": 1}])
    bybit_obs = observe_windowed_trade_flow_at([bybit_buy], T, W, venue="BYBIT")
    okx_obs = observe_windowed_trade_flow_at([okx_sell], T, W, venue="OKX")
    assert bybit_obs.buy_volume and bybit_obs.buy_volume > 0 and bybit_obs.sell_volume == 0
    assert okx_obs.sell_volume and okx_obs.sell_volume > 0 and okx_obs.buy_volume == 0


# ---------------------------------------------------------------------------
# Multi-venue: correct identity, no cross-venue collision, independent units.
# ---------------------------------------------------------------------------


def test_three_venues_produce_independent_windowed_observations():
    binance = observe_windowed_trade_flow_at([_binance_trade_at(T, 1)], T, W, venue="BINANCE")
    bybit = observe_windowed_trade_flow_at([_bybit_trade(T, "1.0", index=0)], T, W, venue="BYBIT")
    okx = observe_windowed_trade_flow_at(
        [_okx_frame(T, "trades", [{"ts": str(T), "tradeId": "1", "px": "1.0",
                                   "sz": "1.0", "side": "buy", "seqId": 1}])],
        T, W, venue="OKX")
    assert binance.instrument == BINANCE_USDM_BTCUSDT
    assert bybit.instrument == BYBIT_LINEAR_BTCUSDT
    assert okx.instrument == OKX_SWAP_BTCUSDT
    assert len({binance.instrument, bybit.instrument, okx.instrument}) == 3


# ---------------------------------------------------------------------------
# Determinism.
# ---------------------------------------------------------------------------


def test_windowed_cvd_is_deterministic_across_repeated_calls():
    frames = [_binance_trade_at(T - 100, 1), _binance_trade_at(T, 2)]
    a = observe_windowed_trade_flow_at(list(frames), T, W, venue="BINANCE")
    b = observe_windowed_trade_flow_at(list(frames), T, W, venue="BINANCE")
    assert a.cvd == b.cvd and a.trade_count == b.trade_count


def test_windowed_cvd_is_independent_of_input_list_order():
    frames = [_binance_trade_at(T - 100, 1), _binance_trade_at(T, 2)]
    forward = observe_windowed_trade_flow_at(list(frames), T, W, venue="BINANCE")
    backward = observe_windowed_trade_flow_at(list(reversed(frames)), T, W, venue="BINANCE")
    assert forward.cvd == backward.cvd


# ---------------------------------------------------------------------------
# Type/value validation.
# ---------------------------------------------------------------------------


def test_zero_window_ms_is_rejected():
    with pytest.raises(ValueError):
        observe_windowed_trade_flow_at([_binance_trade_at(T, 1)], T, 0, venue="BINANCE")


def test_negative_window_ms_is_rejected():
    with pytest.raises(ValueError):
        observe_windowed_trade_flow_at([_binance_trade_at(T, 1)], T, -1000, venue="BINANCE")


def test_bool_window_ms_is_rejected():
    with pytest.raises(TypeError):
        observe_windowed_trade_flow_at([_binance_trade_at(T, 1)], T, True, venue="BINANCE")


@pytest.mark.parametrize("window_ms", [30_000, 60_000, 3 * 60_000, 5 * 60_000, 15 * 60_000])
def test_required_windows_all_work(window_ms):
    frame = _binance_trade_at(T, 1)
    obs = observe_windowed_trade_flow_at([frame], T, window_ms, venue="BINANCE")
    assert obs.status is AlignmentStatus.AVAILABLE
    assert obs.window_ms == window_ms


# ---------------------------------------------------------------------------
# Corruption resistance: reuses the existing storage-corruption test pattern
# (no new corruption framework), for the windowed primitive specifically.
# ---------------------------------------------------------------------------


def test_corrupted_canonical_instrument_key_cannot_rewrite_windowed_cvd(tmp_path):
    from collector.run_okx_collector import OKXCollectorApp

    app = OKXCollectorApp(data_dir=str(tmp_path))
    okx_frame = _okx_frame(T, "trades", [{"ts": str(T), "tradeId": "1", "px": "65000.0",
                                          "sz": "1.0", "side": "buy", "seqId": 1}])
    app.raw_wire_writer.write({"timestamp": T, "connection_id": "c1", "payload": okx_frame.payload,
                              "venue": "OKX"})
    app.raw_wire_writer.close()
    # Deliberately wrong canonical row sitting on disk, never read by replay.
    app.trades_writer.write({"timestamp": T, "exchange_timestamp": T, "local_timestamp": T,
                            "trade_id": "1", "price": 65000.0, "quantity": 1.0, "side": "buy",
                            "instrument_key": BYBIT_LINEAR_BTCUSDT.key})
    app.trades_writer.close()

    obs = observe_windowed_trade_flow_at([okx_frame], T, W, venue="OKX")
    assert obs.instrument == OKX_SWAP_BTCUSDT
    assert obs.instrument != BYBIT_LINEAR_BTCUSDT


# ---------------------------------------------------------------------------
# Mutation battery: hand-written broken copies, proven to diverge from the
# real function on adversarial input (not just asserted to).
# ---------------------------------------------------------------------------


def test_mutation_1_removing_all_causal_bounds_would_leak_a_future_trade():
    """Note on this mutation's design: removing ONLY the pre-replay frame
    filter (leaving the window's own `<= observation_ts` post-filter
    intact) does NOT leak anything for trades -- confirmed by trying it
    first and finding it produced the identical result to the real
    function. This is a genuine, worth-recording architectural fact, not
    a bug: trades are stateless-cumulative, so unlike order-book replay
    (where a late frame can corrupt sequence/gap state even if its
    resulting book states are filtered out afterward), moving a trade's
    causal upper-bound check from pre-replay to post-replay is equally
    safe. So the actual leakage-producing mutation has to remove the
    upper bound from BOTH places -- which is what this test does."""
    from collector.collector.canonical import CanonicalTradeEvent
    from collector.collector.replay import ReplayEngine, ReplaySource

    def mutated_no_upper_bound_anywhere(frames, observation_ts, window_ms, *, venue):
        engine = ReplayEngine(venue=venue)
        engine.run(ReplaySource(list(frames)))   # BUG: no pre-replay causal filter
        trades = [e for e in engine.result.non_book_events if isinstance(e, CanonicalTradeEvent)]
        window_start = observation_ts - window_ms
        windowed = [t for t in trades if window_start < t.local_receive_ts]   # BUG: no upper bound at all
        buy = sum(t.quantity for t in windowed if (t.side or "").upper() == "BUY")
        sell = sum(t.quantity for t in windowed if (t.side or "").upper() == "SELL")
        return buy - sell

    early_buy = _binance_trade_at(T - 1, agg_id=1)
    future_sell_payload = json.dumps({"stream": "btcusdt@aggTrade",
                                      "data": {"e": "aggTrade", "E": T + 1, "T": T + 1,
                                               "a": 2, "p": "1.0", "q": "1000.0", "m": True}})
    future_sell = ReplayFrame(timestamp_ms=T + 1, kind=FrameKind.WIRE, source_index=1,
                              payload=future_sell_payload)
    frames = [early_buy, future_sell]

    real = observe_windowed_trade_flow_at(frames, T, W, venue="BINANCE").cvd
    mutated = mutated_no_upper_bound_anywhere(frames, T, W, venue="BINANCE")
    assert real != mutated
    assert real == pytest.approx(0.5)
    assert mutated == pytest.approx(0.5 - 1000.0)   # the future sell leaked in


def test_removing_only_the_pre_replay_filter_does_not_leak_for_stateless_trades():
    """Companion to the test above: documents, with a passing assertion
    rather than a comment, the architectural fact that motivated widening
    the mutation. Not a claim this is true for order-book replay too --
    reconstruct_book_at's own docstring gives the different, stateful
    reason it excludes late frames before replay there."""
    from collector.collector.canonical import CanonicalTradeEvent
    from collector.collector.replay import ReplayEngine, ReplaySource

    def partially_mutated(frames, observation_ts, window_ms, *, venue):
        engine = ReplayEngine(venue=venue)
        engine.run(ReplaySource(list(frames)))   # no pre-replay filter...
        trades = [e for e in engine.result.non_book_events if isinstance(e, CanonicalTradeEvent)]
        window_start = observation_ts - window_ms
        windowed = [t for t in trades if window_start < t.local_receive_ts <= observation_ts]  # ...but upper bound kept
        buy = sum(t.quantity for t in windowed if (t.side or "").upper() == "BUY")
        sell = sum(t.quantity for t in windowed if (t.side or "").upper() == "SELL")
        return buy - sell

    early_buy = _binance_trade_at(T - 1, agg_id=1)
    future_sell_payload = json.dumps({"stream": "btcusdt@aggTrade",
                                      "data": {"e": "aggTrade", "E": T + 1, "T": T + 1,
                                               "a": 2, "p": "1.0", "q": "1000.0", "m": True}})
    future_sell = ReplayFrame(timestamp_ms=T + 1, kind=FrameKind.WIRE, source_index=1,
                              payload=future_sell_payload)
    frames = [early_buy, future_sell]

    real = observe_windowed_trade_flow_at(frames, T, W, venue="BINANCE").cvd
    partially_mutated_result = partially_mutated(frames, T, W, venue="BINANCE")
    assert real == partially_mutated_result == pytest.approx(0.5)   # identical: no leak either way


def test_mutation_2_changing_open_to_closed_lower_bound_would_admit_the_boundary_trade():
    from collector.collector.trade_flow_observation import _causal_trades

    frame = _binance_trade_at(T - W, agg_id=1)   # exactly at window_start

    real = observe_windowed_trade_flow_at([frame], T, W, venue="BINANCE")
    assert real.status is AlignmentStatus.NEVER_OBSERVED   # real: excluded

    # Mutated: >= instead of > for the lower bound.
    trades, _ = _causal_trades([frame], T, "BINANCE")
    window_start = T - W
    mutated_windowed = [t for t in trades if window_start <= t.local_receive_ts <= T]
    assert len(mutated_windowed) == 1   # mutated: incorrectly included


def test_mutation_6_bypassing_side_normalization_would_misclassify_bybit():
    from collector.collector.trade_flow_observation import _causal_trades

    bybit_buy = _bybit_trade(T, "1.0", index=0)   # side="Buy"

    real = observe_windowed_trade_flow_at([bybit_buy], T, W, venue="BYBIT")
    assert real.buy_volume and real.buy_volume > 0   # real: normalized, classified as BUY

    # Mutated: exact-case comparison against uppercase "BUY" only.
    trades, _ = _causal_trades([bybit_buy], T, "BYBIT")
    mutated_buy_volume = sum(t.quantity for t in trades if t.side == "BUY")   # "Buy" != "BUY"
    assert mutated_buy_volume == 0   # mutated: silently misses every Bybit buy
    assert mutated_buy_volume != real.buy_volume
