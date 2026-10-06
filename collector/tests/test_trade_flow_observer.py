"""TradeFlowObserver: one replay, many causal observations -- proven equivalent.

``observe_trade_flow_at`` / ``observe_windowed_trade_flow_at`` replay the whole
causal prefix on every call (``O(K*N)`` adapter operations for ``K``
observations). ``TradeFlowObserver`` replays once. These tests pin that it is
*value-identical* to the module-level reference for every venue, timestamp
and window, that no future frame can reach a historical observation, that the
preconditions it relies on are enforced by falling back to the reference path
(not assumed), and -- deterministically, by counting adapter calls rather than
timing -- that repeated observations no longer repeat the replay.
"""
from __future__ import annotations

import json
import random

import pytest

from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.replay import FrameKind, ReplayFrame
from collector.collector.trade_flow_observation import (
    TradeFlowObserver,
    observe_trade_flow_at,
    observe_windowed_trade_flow_at,
)
from collector.pipeline.cross_exchange_alignment import AlignmentStatus

BASE = 1_781_000_000_000


def _frame(ts, payload, index, **kw):
    return ReplayFrame(timestamp_ms=ts, kind=FrameKind.WIRE, source_index=index, payload=payload, **kw)


def _binance(ts, agg_id, qty="0.5", sell=False, index=0, **kw):
    return _frame(ts, json.dumps({"stream": "btcusdt@aggTrade", "data": {
        "e": "aggTrade", "E": ts, "T": ts, "a": agg_id, "p": "65000.0", "q": qty, "m": sell}}), index, **kw)


def _binance_mark(ts, index):
    return _frame(ts, json.dumps({"stream": "btcusdt@markPrice@1s", "data": {
        "e": "markPriceUpdate", "E": ts, "p": "65000.1", "i": "65000.0", "r": "0.0001", "T": ts + 1000}}), index)


def _bybit(ts, trade_id, qty="0.2", side="Buy", index=0):
    return _frame(ts, json.dumps({"topic": "publicTrade.BTCUSDT", "ts": ts, "data": [
        {"i": trade_id, "T": ts, "p": "65000.0", "v": qty, "S": side}]}), index)


def _okx(ts, trade_id, qty="1.0", side="buy", index=0):
    return _frame(ts, json.dumps({"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [
        {"ts": str(ts), "tradeId": trade_id, "px": "65000.0", "sz": qty, "side": side, "seqId": 1}]}), index)


def _spot(ts, trade_id, qty="0.25", maker=False, index=0):
    return _frame(ts, json.dumps({"stream": "btcusdt@trade", "data": {
        "e": "trade", "E": ts, "s": "BTCUSDT", "t": trade_id, "p": "65000.0", "q": qty,
        "T": ts, "m": maker, "M": True}}), index)


def _dataset(venue, seed, n=90):
    """Repository-shaped wire frames with the awkward cases mixed in: resent
    (duplicate) trades, equal timestamps, an unknown/empty side, non-trade
    frames, an undecodable frame, and a shuffled input order."""
    rnd = random.Random(seed)
    frames, ts, index = [], BASE, 0
    for i in range(n):
        ts += rnd.choice([0, 0, 1, 3, 40, 250, 6_000])      # ties, bursts and quiet gaps
        qty = "%.3f" % (0.001 + rnd.random())
        if venue == "BINANCE":
            if rnd.random() < 0.15:
                frames.append(_binance_mark(ts, index))
            else:
                frames.append(_binance(ts, i, qty, sell=rnd.random() < 0.5, index=index))
        elif venue == "BYBIT":
            side = rnd.choice(["Buy", "Sell", "Buy", "Sell", "", "weird"])
            frames.append(_bybit(ts, f"b{i}", qty, side, index))
        elif venue == "OKX":
            side = rnd.choice(["buy", "sell", "buy", "sell", "", "weird"])
            frames.append(_okx(ts, f"o{i}", qty, side, index))
        else:
            frames.append(_spot(ts, 1_000 + i, qty, maker=rnd.random() < 0.5, index=index))
        index += 1
        if rnd.random() < 0.12:                              # resend: same trade id, later frame
            ts += rnd.choice([0, 2, 90])
            frames.append(_resend(frames[-1], ts, index))
            index += 1
        if rnd.random() < 0.03:
            frames.append(ReplayFrame(timestamp_ms=ts, kind=FrameKind.WIRE, source_index=index,
                                      payload="{not json", decode_ok=False))
            index += 1
    rnd.shuffle(frames)
    return frames


def _resend(original, ts, index):
    return ReplayFrame(timestamp_ms=ts, kind=FrameKind.WIRE, source_index=index, payload=original.payload)


def _query_points(frames):
    stamps = sorted({f.timestamp_ms for f in frames})
    pts = {BASE - 1, stamps[0] - 1, stamps[-1], stamps[-1] + 1, stamps[-1] + 10_000_000}
    for s in stamps[:: max(1, len(stamps) // 12)]:
        pts.update({s - 1, s, s + 1, s + 5_000, s + 5_001})
    return sorted(pts)


WINDOWS = (1, 333, 5_000, 900_000)


def _outcome(fn, *args, **kwargs):
    """Value (as repr) or exception (type, message): equivalence includes failing the same way."""
    try:
        return ("ok", repr(fn(*args, **kwargs)))
    except Exception as exc:  # noqa: BLE001
        return ("raised", type(exc).__name__, str(exc))


def _same(a, b):
    assert a == b
    assert repr(a) == repr(b)        # exact floats, enums and None-ness, not just ==


# ---------------------------------------------------------------------------
# Equivalence with the reference, every venue.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("venue", ["BINANCE", "BYBIT", "OKX", "BINANCE_SPOT"])
@pytest.mark.parametrize("seed", [1, 2, 3])
def test_observer_is_value_identical_to_the_reference_everywhere(venue, seed):
    frames = _dataset(venue, seed)
    observer = TradeFlowObserver(frames, venue=venue)
    assert observer.mode == "indexed", observer.fallback_reason
    checked = 0
    for ts in _query_points(frames):
        _same(observer.observe_trade_flow_at(ts), observe_trade_flow_at(frames, ts, venue=venue))
        for stale in (0, 5_000):
            _same(observer.observe_trade_flow_at(ts, staleness_ms=stale),
                  observe_trade_flow_at(frames, ts, venue=venue, staleness_ms=stale))
        for w in WINDOWS:
            _same(observer.observe_windowed_trade_flow_at(ts, w),
                  observe_windowed_trade_flow_at(frames, ts, w, venue=venue))
            _same(observer.observe_windowed_trade_flow_at(ts, w, staleness_ms=1),
                  observe_windowed_trade_flow_at(frames, ts, w, venue=venue, staleness_ms=1))
            checked += 1
    assert checked > 40


def test_observation_exercised_every_status_so_equivalence_is_not_vacuous():
    frames = _dataset("BINANCE", 5)
    observer = TradeFlowObserver(frames, venue="BINANCE")
    seen = {observer.observe_trade_flow_at(ts).status for ts in _query_points(frames)}
    seen |= {observer.observe_windowed_trade_flow_at(ts, 50).status for ts in _query_points(frames)}
    assert {AlignmentStatus.NEVER_OBSERVED, AlignmentStatus.AVAILABLE, AlignmentStatus.STALE} <= seen


def test_repeated_calls_and_query_order_do_not_change_answers():
    frames = _dataset("OKX", 7)
    observer = TradeFlowObserver(frames, venue="OKX")
    points = _query_points(frames)
    first = [observer.observe_windowed_trade_flow_at(ts, 5_000) for ts in points]
    again = [observer.observe_windowed_trade_flow_at(ts, 5_000) for ts in points]
    backwards = [observer.observe_windowed_trade_flow_at(ts, 5_000) for ts in reversed(points)][::-1]
    assert first == again == backwards


def test_5m_and_15m_horizons_over_one_dataset_match_the_reference():
    frames = _dataset("BINANCE", 11, n=150)
    observer = TradeFlowObserver(frames, venue="BINANCE")
    for ts in _query_points(frames):
        for window_ms in (300_000, 900_000):
            _same(observer.observe_windowed_trade_flow_at(ts, window_ms),
                  observe_windowed_trade_flow_at(frames, ts, window_ms, venue="BINANCE"))


# ---------------------------------------------------------------------------
# Causality: no future frame may reach a historical observation.
# ---------------------------------------------------------------------------


def test_future_frames_never_change_a_historical_observation():
    past = [_binance(BASE + 10 * i, i, "0.5", sell=bool(i % 2), index=i) for i in range(20)]
    cutoff = BASE + 10 * 19
    future = [_binance(cutoff + 1 + i, 1_000 + i, "100.0", index=100 + i) for i in range(20)]
    observer = TradeFlowObserver(past + future, venue="BINANCE")
    for w in (None, 5, 100):
        if w is None:
            got = observer.observe_trade_flow_at(cutoff)
            want = observe_trade_flow_at(past, cutoff, venue="BINANCE")
            assert got.frames_considered == len(past)
        else:
            got = observer.observe_windowed_trade_flow_at(cutoff, w)
            want = observe_windowed_trade_flow_at(past, cutoff, w, venue="BINANCE")
        _same(got, want)
    assert observer.observe_trade_flow_at(cutoff).buy_volume < 100


def test_observation_exactly_at_a_frame_timestamp_includes_it_and_one_ms_earlier_excludes_it():
    frames = [_binance(BASE, 1, index=0), _binance(BASE + 100, 2, index=1)]
    observer = TradeFlowObserver(frames, venue="BINANCE")
    assert observer.observe_trade_flow_at(BASE + 100).trade_count == 2
    assert observer.observe_trade_flow_at(BASE + 99).trade_count == 1
    assert observer.observe_trade_flow_at(BASE + 99).frames_considered == 1
    assert observer.observe_trade_flow_at(BASE - 1).status is AlignmentStatus.NEVER_OBSERVED


def test_window_is_open_below_and_closed_above():
    frames = [_binance(BASE + 100, 1, "1.0", index=0), _binance(BASE + 200, 2, "2.0", index=1),
              _binance(BASE + 300, 3, "4.0", index=2)]
    observer = TradeFlowObserver(frames, venue="BINANCE")
    got = observer.observe_windowed_trade_flow_at(BASE + 300, 200)      # (BASE+100, BASE+300]
    assert got.trade_count == 2 and got.buy_volume == 6.0
    assert got.first_trade_local_receive_ts == BASE + 200
    assert got.last_trade_local_receive_ts == BASE + 300
    _same(got, observe_windowed_trade_flow_at(frames, BASE + 300, 200, venue="BINANCE"))


def test_a_resent_trade_is_counted_once_before_and_after_the_resend():
    original = _binance(BASE, 7, "0.5", index=0)
    frames = [original, _resend(original, BASE + 50, 1), _binance(BASE + 60, 8, "0.5", index=2)]
    observer = TradeFlowObserver(frames, venue="BINANCE")
    for ts in (BASE, BASE + 49, BASE + 50, BASE + 60):
        _same(observer.observe_trade_flow_at(ts), observe_trade_flow_at(frames, ts, venue="BINANCE"))
    assert observer.observe_trade_flow_at(BASE + 50).trade_count == 1
    assert observer.observe_trade_flow_at(BASE + 60).trade_count == 2


def test_unknown_side_counts_toward_trade_count_but_neither_volume():
    frames = [_bybit(BASE, "a", "1.0", "Buy", 0), _bybit(BASE + 1, "b", "2.0", "", 1),
              _bybit(BASE + 2, "c", "4.0", "Sell", 2)]
    got = TradeFlowObserver(frames, venue="BYBIT").observe_trade_flow_at(BASE + 2)
    assert (got.trade_count, got.buy_volume, got.sell_volume, got.cvd) == (3, 1.0, 4.0, -3.0)


def test_observer_snapshots_its_input_so_later_mutation_cannot_change_answers():
    frames = [_binance(BASE + i, i, index=i) for i in range(10)]
    observer = TradeFlowObserver(frames, venue="BINANCE")
    before = observer.observe_trade_flow_at(BASE + 9)
    frames.append(_binance(BASE + 5, 999, "1000.0", index=99))
    frames.clear()
    _same(observer.observe_trade_flow_at(BASE + 9), before)


# ---------------------------------------------------------------------------
# Preconditions are enforced: fall back to the reference, never guess.
# ---------------------------------------------------------------------------


def test_receive_ns_inconsistent_with_timestamp_ms_is_indexed_and_matches_the_reference():
    """Since P0-11's replay-ordering follow-up, ``order_key`` leads with
    ``timestamp_ms`` and uses ``receive_ns`` only within one (ms, kind)
    group, so replay order is timestamp order whatever ``receive_ns`` says.
    Frame A's recorded ns (150 ms) is later than frame B's timestamp (120),
    and a resend shares a trade id: the one-replay and per-prefix results
    must still agree at every T."""
    a = _binance(BASE + 100, 5, index=0, receive_ns=(BASE + 150) * 1_000_000)
    b = _binance(BASE + 120, 5, index=1)
    observer = TradeFlowObserver([a, b], venue="BINANCE")
    assert observer.mode == "indexed"
    for ts in (BASE + 99, BASE + 100, BASE + 110, BASE + 120, BASE + 500):
        _same(observer.observe_trade_flow_at(ts), observe_trade_flow_at([a, b], ts, venue="BINANCE"))
        _same(observer.observe_windowed_trade_flow_at(ts, 50),
              observe_windowed_trade_flow_at([a, b], ts, 50, venue="BINANCE"))
    assert observer.observe_trade_flow_at(BASE + 110).trade_count == 1   # A counted at T=110


def _ns_first_order_key(frame):
    """The pre-follow-up ordering: recorded ns first, ms only as a fallback."""
    ns = frame.receive_ns if frame.receive_ns is not None else frame.timestamp_ms * 1_000_000
    return (ns, 0, frame.source_index)


def test_if_replay_order_ever_stops_being_timestamp_order_the_observer_falls_back(monkeypatch):
    """Defence in depth: the 'causal frames are a prefix of the replay' proof
    needs ``timestamp_ms`` non-decreasing along replay order. Replay
    guarantees it today; this simulates a regression of that guarantee and
    shows the observer notices rather than silently mis-slicing. The shared
    trade id makes naive slicing wrong at T=110 (A is the first occurrence
    there, but not in the full replay)."""
    monkeypatch.setattr(ReplayFrame, "order_key", property(_ns_first_order_key))
    a = _binance(BASE + 100, 5, index=0, receive_ns=(BASE + 150) * 1_000_000)
    b = _binance(BASE + 120, 5, index=1)
    observer = TradeFlowObserver([a, b], venue="BINANCE")
    assert observer.mode == "reference_fallback"
    assert observer.fallback_reason == "frame_timestamps_not_monotone_in_replay_order"
    for ts in (BASE + 99, BASE + 100, BASE + 110, BASE + 120, BASE + 500):
        assert _outcome(observer.observe_trade_flow_at, ts) == \
            _outcome(observe_trade_flow_at, [a, b], ts, venue="BINANCE")
        assert _outcome(observer.observe_windowed_trade_flow_at, ts, 50) == \
            _outcome(observe_windowed_trade_flow_at, [a, b], ts, 50, venue="BINANCE")
    assert observer.observe_trade_flow_at(BASE + 110).trade_count == 1


def test_a_trade_stamped_with_a_different_receive_time_than_its_frame_falls_back(monkeypatch):
    """The observer slices by ``trade.local_receive_ts``; that equals the
    causal frame cut only if each trade carries its own frame's
    ``timestamp_ms``. Every adapter does today, so prove the guard by making
    one adapter violate it."""
    from dataclasses import replace
    original = BinanceAdapter.normalize

    def skewed(self, raw, *, local_receive_ts=None):
        events = original(self, raw, local_receive_ts=local_receive_ts)
        return [replace(e, local_receive_ts=e.local_receive_ts + 1) if hasattr(e, "trade_id") else e
                for e in events] if isinstance(events, list) else events

    monkeypatch.setattr(BinanceAdapter, "normalize", skewed)
    frames = [_binance(BASE + 10 * i, i, index=i) for i in range(6)]
    observer = TradeFlowObserver(frames, venue="BINANCE")
    assert observer.mode == "reference_fallback"
    assert observer.fallback_reason == "trade_receive_ts_differs_from_frame_timestamp"
    for ts in (BASE - 1, BASE + 9, BASE + 10, BASE + 25, BASE + 50):
        assert _outcome(observer.observe_trade_flow_at, ts) == \
            _outcome(observe_trade_flow_at, frames, ts, venue="BINANCE")
        # The reference windowed path asserts its own suffix property here;
        # the observer must reproduce that outcome, not paper over it.
        assert _outcome(observer.observe_windowed_trade_flow_at, ts, 20) == \
            _outcome(observe_windowed_trade_flow_at, frames, ts, 20, venue="BINANCE")


def test_a_replay_exception_in_a_late_frame_only_affects_observations_that_include_it(monkeypatch):
    good = [_binance(BASE + i, i, index=i) for i in range(5)]
    bad = _binance(BASE + 100, 99, index=5)
    frames = good + [bad]
    real = ReplayFrame.order_key

    def exploding(frame):
        if frame.source_index == 5:
            raise ValueError("unreadable frame 5")
        return real.fget(frame)

    monkeypatch.setattr(ReplayFrame, "order_key", property(exploding))
    observer = TradeFlowObserver(frames, venue="BINANCE")
    assert observer.mode == "reference_fallback"
    assert observer.fallback_reason == "build_raised:ValueError"
    _same(observer.observe_trade_flow_at(BASE + 50), observe_trade_flow_at(good, BASE + 50, venue="BINANCE"))
    with pytest.raises(ValueError) as expected:
        observe_trade_flow_at(frames, BASE + 100, venue="BINANCE")
    with pytest.raises(ValueError) as got:
        observer.observe_trade_flow_at(BASE + 100)
    assert str(got.value) == str(expected.value)


def test_unsupported_venue_behaves_exactly_like_the_reference():
    frames = [_binance(BASE, 1, index=0)]
    observer = TradeFlowObserver(frames, venue="NOPE")
    assert observer.mode == "reference_fallback"
    _same(observer.observe_trade_flow_at(BASE - 1), observe_trade_flow_at(frames, BASE - 1, venue="NOPE"))
    with pytest.raises(ValueError, match="does not support venue"):
        observer.observe_trade_flow_at(BASE)


def test_empty_frame_set_is_never_observed():
    observer = TradeFlowObserver([], venue="BINANCE")
    got = observer.observe_trade_flow_at(BASE)
    assert got.status is AlignmentStatus.NEVER_OBSERVED and got.frames_considered == 0
    _same(got, observe_trade_flow_at([], BASE, venue="BINANCE"))


@pytest.mark.parametrize("bad", [True, 1.5, "1", None])
def test_observation_ts_validation_matches_the_reference(bad):
    observer = TradeFlowObserver([_binance(BASE, 1)], venue="BINANCE")
    with pytest.raises(TypeError) as want:
        observe_trade_flow_at([], bad, venue="BINANCE")
    with pytest.raises(TypeError) as got:
        observer.observe_trade_flow_at(bad)
    assert str(got.value) == str(want.value)
    with pytest.raises(TypeError):
        observer.observe_windowed_trade_flow_at(bad, 10)


@pytest.mark.parametrize("bad,exc", [(True, TypeError), (1.5, TypeError), (0, ValueError), (-5, ValueError)])
def test_window_validation_matches_the_reference(bad, exc):
    observer = TradeFlowObserver([_binance(BASE, 1)], venue="BINANCE")
    with pytest.raises(exc) as want:
        observe_windowed_trade_flow_at([], BASE, bad, venue="BINANCE")
    with pytest.raises(exc) as got:
        observer.observe_windowed_trade_flow_at(BASE, bad)
    assert str(got.value) == str(want.value)


# ---------------------------------------------------------------------------
# Scalability, deterministically: count adapter operations, not seconds.
# ---------------------------------------------------------------------------


def test_repeated_observations_replay_the_frames_once_not_once_per_call(monkeypatch):
    calls = {"n": 0}
    original = BinanceAdapter.normalize

    def counting(self, *args, **kwargs):
        calls["n"] += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(BinanceAdapter, "normalize", counting)

    n = 600
    frames = [_binance(BASE + 10 * i, i, index=i) for i in range(n)]
    points = [BASE + 10 * (k * 30 + 29) for k in range(20)]            # 20 observation times
    queries = 20 * 3                                                    # cumulative + 5m + 15m each

    # Reference: every call replays its own causal prefix.
    for ts in points:
        observe_trade_flow_at(frames, ts, venue="BINANCE")
        observe_windowed_trade_flow_at(frames, ts, 300_000, venue="BINANCE")
        observe_windowed_trade_flow_at(frames, ts, 900_000, venue="BINANCE")
    reference_calls = calls["n"]
    assert reference_calls == 3 * sum(k * 30 + 30 for k in range(20))   # sum of causal prefixes

    # Observer: one replay, then zero adapter calls per query.
    calls["n"] = 0
    observer = TradeFlowObserver(frames, venue="BINANCE")
    assert calls["n"] == n
    for ts in points:
        observer.observe_trade_flow_at(ts)
        observer.observe_windowed_trade_flow_at(ts, 300_000)
        observer.observe_windowed_trade_flow_at(ts, 900_000)
    assert calls["n"] == n
    assert reference_calls > 15 * n                                     # the redundancy removed
    assert queries == 60


# ---------------------------------------------------------------------------
# Guard-sufficiency attacks: shapes where both guards hold yet slicing could
# plausibly differ from the per-prefix replay.
# ---------------------------------------------------------------------------


def _bybit_multi(ts, trades, index):
    return _frame(ts, json.dumps({"topic": "publicTrade.BTCUSDT", "ts": ts, "data": [
        {"i": i, "T": ts, "p": "65000.0", "v": v, "S": s} for i, v, s in trades]}), index)


def _okx_multi(ts, trades, index):
    return _frame(ts, json.dumps({"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [
        {"ts": str(ts), "tradeId": i, "px": "65000.0", "sz": v, "side": s, "seqId": 1} for i, v, s in trades]}), index)


def test_multiple_trades_in_one_frame_enter_and_leave_together_including_in_frame_duplicates():
    frames = [
        _bybit_multi(BASE, [("a", "1.0", "Buy"), ("b", "2.0", "Sell"), ("a", "1.0", "Buy")], 0),  # dup inside frame
        _bybit_multi(BASE + 10, [("b", "2.0", "Sell"), ("c", "4.0", "Buy")], 1),                  # b redelivered
        _bybit_multi(BASE + 10, [("d", "8.0", "Sell")], 2),                                       # equal timestamp
    ]
    observer = TradeFlowObserver(frames, venue="BYBIT")
    assert observer.mode == "indexed"
    for ts in (BASE - 1, BASE, BASE + 9, BASE + 10, BASE + 11):
        _same(observer.observe_trade_flow_at(ts), observe_trade_flow_at(frames, ts, venue="BYBIT"))
        for w in (5, 10, 11):
            _same(observer.observe_windowed_trade_flow_at(ts, w),
                  observe_windowed_trade_flow_at(frames, ts, w, venue="BYBIT"))
    assert observer.observe_trade_flow_at(BASE).trade_count == 2
    assert observer.observe_trade_flow_at(BASE + 10).trade_count == 4


def test_okx_multi_trade_frames_and_unidentified_trades_match_the_reference():
    frames = [
        _okx_multi(BASE, [("1", "1.0", "buy"), ("2", "1.0", "sell")], 0),
        _okx_multi(BASE + 5, [("1", "1.0", "buy")], 1),                      # redelivery of 1
        _okx_multi(BASE + 5, [("3", "2.0", "buy")], 2),
    ]
    observer = TradeFlowObserver(frames, venue="OKX")
    for ts in (BASE - 1, BASE, BASE + 4, BASE + 5, BASE + 6):
        _same(observer.observe_trade_flow_at(ts), observe_trade_flow_at(frames, ts, venue="OKX"))
        _same(observer.observe_windowed_trade_flow_at(ts, 5),
              observe_windowed_trade_flow_at(frames, ts, 5, venue="OKX"))


def test_trades_without_an_id_are_never_deduplicated_and_match_the_reference():
    def no_id(ts, index):
        return _frame(ts, json.dumps({"stream": "btcusdt@aggTrade", "data": {
            "e": "aggTrade", "E": ts, "T": ts, "p": "65000.0", "q": "1.0", "m": False}}), index)

    frames = [no_id(BASE, 0), no_id(BASE, 1), no_id(BASE + 3, 2)]
    observer = TradeFlowObserver(frames, venue="BINANCE")
    for ts in (BASE - 1, BASE, BASE + 2, BASE + 3):
        _same(observer.observe_trade_flow_at(ts), observe_trade_flow_at(frames, ts, venue="BINANCE"))
    assert observer.observe_trade_flow_at(BASE).trade_count == 2


def test_input_order_does_not_change_the_observer_result():
    frames = _dataset("BINANCE", 21, n=60)
    expected = TradeFlowObserver(frames, venue="BINANCE")
    for seed in (1, 2, 3):
        shuffled = list(frames)
        random.Random(seed).shuffle(shuffled)
        other = TradeFlowObserver(shuffled, venue="BINANCE")
        for ts in _query_points(frames)[::3]:
            _same(other.observe_windowed_trade_flow_at(ts, 5_000),
                  expected.observe_windowed_trade_flow_at(ts, 5_000))
            _same(other.observe_trade_flow_at(ts), expected.observe_trade_flow_at(ts))
