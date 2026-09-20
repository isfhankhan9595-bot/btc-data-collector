"""Causal cross-exchange alignment: adversarial acceptance tests.

Availability is ``local_receive_ts <= observation_ts``. Exchange timestamps never
decide eligibility, nearest-timestamp matching never happens, and missing or
stale data is never disguised as fresh.
"""
from __future__ import annotations

import ast
import dataclasses
import itertools
import json
from pathlib import Path

import pytest

from collector.collector.canonical import CanonicalTradeEvent
from collector.collector.replay import FrameKind, ReplayEngine, ReplayFrame, ReplaySource
from collector.pipeline import cross_exchange_alignment as module
from collector.pipeline.cross_exchange_alignment import (
    AlignedObservation, AlignmentStatus, alignment_key, causally_align,
)

T = 1_780_000_000_000
PERP = "linear_perpetual"


def trade(exchange="BINANCE", stream="trades", *, recv, ex_ts=None, market_type=PERP,
          price=100.0, quality="VALID", tid="1"):
    return CanonicalTradeEvent(
        exchange, stream, recv if ex_ts is None else ex_ts, None, recv,
        market_type=market_type, quality_state=quality,
        trade_id=tid, price=price, quantity=1.0, side="Buy")


def align(events, ts=T, **kw):
    kw.setdefault("staleness_ms", 1_000)
    return causally_align(events, ts, **kw)


# -- identity isolation ---------------------------------------------------------

def test_three_venues_remain_separate_keys():
    out = align([trade(e, recv=T) for e in ("BINANCE", "BYBIT", "OKX")])
    assert set(out) == {(e, PERP, "trades") for e in ("BINANCE", "BYBIT", "OKX")}


def test_same_exchange_same_stream_keeps_only_the_latest():
    out = align([trade(recv=T - 50, price=1.0), trade(recv=T - 10, price=2.0)])
    assert len(out) == 1 and out[("BINANCE", PERP, "trades")].event.price == 2.0


def test_perp_and_spot_of_one_exchange_and_stream_cannot_collide():
    perp, spot = trade(recv=T - 5, price=1.0), trade(recv=T - 1, price=2.0, market_type="spot")
    out = align([perp, spot])
    assert out[("BINANCE", PERP, "trades")].event is perp
    assert out[("BINANCE", "spot", "trades")].event is spot


def test_different_streams_are_independent():
    out = align([trade(stream="trades", recv=T - 900), trade(stream="markprice", recv=T - 1)])
    assert out[("BINANCE", PERP, "trades")].age_ms == 900
    assert out[("BINANCE", PERP, "markprice")].age_ms == 1


# -- causal timestamp semantics ------------------------------------------------

def test_exchange_timestamp_never_grants_early_availability():
    early_exchange_ts_late_receive = trade(recv=T + 5_000, ex_ts=T - 3_600_000)
    assert align([early_exchange_ts_late_receive]) == {}


def test_future_exchange_timestamp_is_available_if_it_was_received_by_T():
    e = trade(recv=T, ex_ts=T + 3_600_000)
    out = align([e])
    assert out[alignment_key(e)].event is e and out[alignment_key(e)].status is AlignmentStatus.AVAILABLE


@pytest.mark.parametrize("recv,visible", [(T - 1, True), (T, True), (T + 1, False), (T + 5_000, False)])
def test_observation_boundary_is_inclusive_and_one_ms_later_is_excluded(recv, visible):
    assert bool(align([trade(recv=recv)])) is visible


def test_no_nearest_timestamp_behavior_an_older_eligible_event_beats_a_nearer_future_one():
    older, nearer_future = trade(recv=T - 400, price=1.0), trade(recv=T + 1, price=2.0)
    assert align([nearer_future, older])[("BINANCE", PERP, "trades")].event is older


def test_everything_in_the_future_yields_nothing_unless_expected():
    future = [trade(recv=T + 1), trade("BYBIT", recv=T + 9)]
    assert align(future) == {}
    exp = [("BINANCE", PERP, "trades"), ("BYBIT", PERP, "trades")]
    assert {k: v.status for k, v in align(future, expected_keys=exp).items()} == {
        k: AlignmentStatus.NEVER_OBSERVED for k in exp}


# -- ordering / duplicates ------------------------------------------------------

def test_exact_timestamp_tie_is_deterministic_last_input_wins():
    a, b = trade(recv=T - 5, price=1.0), trade(recv=T - 5, price=2.0)
    assert align([a, b])[("BINANCE", PERP, "trades")].event is b
    assert align([a, b]) == align([a, b])
    assert align([b, a])[("BINANCE", PERP, "trades")].event is a     # documented: ties follow input order


def test_out_of_order_input_does_not_change_the_result():
    events = [trade(recv=T - 30, price=1.0), trade(recv=T - 20, price=2.0),
              trade("BYBIT", recv=T - 10, price=3.0), trade("OKX", recv=T - 40, price=4.0)]
    baseline = align(events)
    for perm in itertools.permutations(events):
        assert align(list(perm)) == baseline


# -- missingness ----------------------------------------------------------------

def test_expected_but_never_observed_is_reported_not_fabricated():
    key = ("OKX", PERP, "trades")
    out = align([trade(recv=T)], expected_keys=[key])
    assert out[key] == AlignedObservation(key, None, None, AlignmentStatus.NEVER_OBSERVED)


def test_unrequested_absent_stream_stays_absent():
    out = align([trade(recv=T)])
    assert ("OKX", PERP, "trades") not in out and len(out) == 1


def test_expected_key_that_was_observed_is_not_overwritten_by_never_observed():
    key = ("BINANCE", PERP, "trades")
    assert align([trade(recv=T)], expected_keys=[key])[key].status is AlignmentStatus.AVAILABLE


# -- staleness ------------------------------------------------------------------

def test_stale_observation_is_returned_as_stale_never_dropped_or_relabelled():
    old = trade(recv=T - 5_000)
    obs = align([old], staleness_ms=1_000)[("BINANCE", PERP, "trades")]
    assert obs.status is AlignmentStatus.STALE and obs.event is old and obs.age_ms == 5_000


@pytest.mark.parametrize("age,status", [(999, AlignmentStatus.AVAILABLE), (1_000, AlignmentStatus.AVAILABLE),
                                        (1_001, AlignmentStatus.STALE)])
def test_freshness_boundary(age, status):
    assert align([trade(recv=T - age)], staleness_ms=1_000)[("BINANCE", PERP, "trades")].status is status


def test_staleness_is_required_and_validated():
    with pytest.raises(TypeError):
        causally_align([trade(recv=T)], T)                      # no silent default
    with pytest.raises(ValueError):
        causally_align([trade(recv=T)], T, staleness_ms=-1)
    with pytest.raises(TypeError):
        causally_align([trade(recv=T)], 1.5, staleness_ms=1)   # not epoch-ms int


# -- determinism / mutation -----------------------------------------------------

def test_same_input_same_result_and_input_mutation_changes_it():
    events = [trade(recv=T - 10), trade("BYBIT", recv=T - 20)]
    assert align(events) == align(list(events))
    assert align(events + [trade(recv=T - 1, price=9.0)]) != align(events)
    assert align(events[:1]) != align(events)


def test_three_venues_with_identical_prices_stay_distinct_observations():
    out = align([trade(e, recv=T - 1, price=65_000.0) for e in ("BINANCE", "BYBIT", "OKX")])
    assert len(out) == 3 and {o.event.exchange for o in out.values()} == {"BINANCE", "BYBIT", "OKX"}


def test_result_is_ordered_by_key_regardless_of_input_order():
    out = align([trade("OKX", recv=T), trade("BINANCE", recv=T), trade("BYBIT", recv=T)])
    assert list(out) == sorted(out)


# -- provenance / quality -------------------------------------------------------

def test_original_event_is_returned_untouched_with_exchange_timestamps_verbatim():
    e = trade(recv=T - 7, ex_ts=T + 123_456)
    before = dataclasses.asdict(e)
    obs = align([e])[alignment_key(e)]
    assert obs.event is e and dataclasses.asdict(e) == before
    assert (e.exchange_event_ts, e.local_receive_ts) == (T + 123_456, T - 7)


@pytest.mark.parametrize("quality", ["SEQUENCE_GAP", "RECOVERING", "INVALID"])
def test_availability_does_not_imply_quality(quality):
    obs = align([trade(recv=T - 1, quality=quality)])[("BINANCE", PERP, "trades")]
    assert obs.status is AlignmentStatus.AVAILABLE and obs.event.quality_state == quality


def test_venues_with_different_receive_latency_are_judged_independently():
    exchange_ts = T - 300
    binance = trade("BINANCE", recv=exchange_ts + 250, ex_ts=exchange_ts)   # slow path
    bybit = trade("BYBIT", recv=exchange_ts + 5, ex_ts=exchange_ts)
    at = exchange_ts + 100                                                  # same exchange time, T=+100
    out = causally_align([binance, bybit], at, staleness_ms=1_000)
    assert set(out) == {("BYBIT", PERP, "trades")}                          # Binance not yet received


def test_no_synchronized_or_current_status_exists():
    assert {s.value for s in AlignmentStatus} == {"AVAILABLE", "STALE", "NEVER_OBSERVED"}


# -- purity ---------------------------------------------------------------------

def test_module_is_pure_no_clock_network_or_randomness():
    tree = ast.parse(Path(module.__file__).read_text())
    imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    imported |= {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
    assert not imported & {"time", "datetime", "random", "socket", "requests", "aiohttp", "urllib",
                           "http", "asyncio", "websockets", "os", "secrets"}
    calls = {ast.unparse(n.func) for n in ast.walk(tree) if isinstance(n, ast.Call)}
    assert not {c for c in calls if c.endswith((".now", ".utcnow", "time.time"))}


# -- replay parity --------------------------------------------------------------

def _frame(ts, payload, i=0):
    return ReplayFrame(timestamp_ms=ts, kind=FrameKind.WIRE, source_index=i, payload=json.dumps(payload))


def _replayed_events():
    bybit = ReplayEngine("BYBIT").run(ReplaySource([
        _frame(T - 40, {"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": T - 40, "data": [
            {"T": T - 40, "s": "BTCUSDT", "S": "Buy", "v": "0.01", "p": "65000.5", "i": "b1", "BT": False}]}),
        _frame(T + 10, {"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": T + 10, "data": [
            {"T": T + 10, "s": "BTCUSDT", "S": "Sell", "v": "0.02", "p": "64000.0", "i": "b2", "BT": False}]}, 1),
    ]))
    okx = ReplayEngine("OKX").run(ReplaySource([
        _frame(T - 5, {"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [
            {"instId": "BTC-USDT-SWAP", "tradeId": "o1", "px": "65001", "sz": "1", "side": "buy",
             "ts": str(T - 5)}]}),
    ]))
    return bybit.non_book_events + okx.non_book_events


def test_replay_produced_events_align_deterministically_and_causally():
    events = _replayed_events()
    assert {e.exchange for e in events} == {"BYBIT", "OKX"}
    first = align(events)
    assert first == align(_replayed_events())                    # replay twice -> identical alignment
    assert first == align(list(reversed(events)))
    bybit = first[("BYBIT", PERP, "trades")]
    assert bybit.event.trade_id == "b1" and bybit.age_ms == 40   # the T+10 replay frame is not yet known
    assert first[("OKX", PERP, "trades")].age_ms == 5
