"""Phase E: live == replay identity, across the whole lifecycle.

Defects these pin (each found by tracing, not by the previous tests):

* REST depth snapshots were hand-built at three call sites, outside the adapter's
  identity stamp, so the snapshot row persisted beside diff rows carried a null
  ``instrument_key`` (accidentally lost identity, not a legitimate unidentified).
* Replay called ``normalize_binance_oi`` without ``symbol``: live's OI events were
  identified, replay's were not.
* The Bybit runner comment claimed the adapter attaches no identity; it does.

Identity is compared field by field, never only as a key string.
"""
from __future__ import annotations

import ast
import dataclasses
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from collector import run_bybit_collector
from collector.collector import replay as replay_module
from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.adapters.binance_spot import BinanceSpotAdapter
from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.adapters.okx import OKXAdapter
from collector.collector.binance_oi import normalize_binance_oi
from collector.collector.canonical import (
    CanonicalLiquidationEvent, CanonicalMarkPriceEvent, CanonicalOIEvent, CanonicalOrderBookEvent,
    CanonicalTradeEvent,
)
from collector.collector.instrument import (
    BINANCE_SPOT_BTCUSDT, BINANCE_USDM_BTCUSDT, BYBIT_LINEAR_BTCUSDT, OKX_SWAP_BTCUSDT, InstrumentId,
    InstrumentIdError,
)
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.replay import FrameKind, ReplayEngine, ReplayFrame, ReplaySource
from collector.pipeline.cross_exchange_alignment import UNIDENTIFIED, AlignmentStatus, causally_align
from collector.tests.test_replay import _depth_frame, _snapshot_frame

T = 1_780_000_000_000
BINANCE_OI_BODY = json.dumps({"openInterest": "999.9", "symbol": "BTCUSDT", "time": T - 100})


def same_identity(a, b, label=""):
    """Field-wise, so a difference in any one field is named, never hidden by a key."""
    assert (a is None) == (b is None), f"{label}: {a!r} vs {b!r}"
    if a is None:
        return
    for name in ("exchange", "market_type", "instrument", "native_symbol"):
        assert getattr(a, name) == getattr(b, name), f"{label}: {name} differs ({a.key} vs {b.key})"
    assert a.key == b.key


def wire(ts, payload, index=0):
    return ReplayFrame(timestamp_ms=ts, kind=FrameKind.WIRE, source_index=index,
                       payload=json.dumps(payload) if not isinstance(payload, str) else payload)


# ---- frames: (venue adapter, expected identity, frame) per event type ------------------
BINANCE_FRAMES = {
    CanonicalTradeEvent: {"stream": "btcusdt@aggTrade", "data": {
        "e": "aggTrade", "E": T, "T": T, "a": 1, "p": "65000.0", "q": "0.5", "m": False}},
    CanonicalMarkPriceEvent: {"stream": "btcusdt@markPrice@1s", "data": {
        "e": "markPriceUpdate", "E": T, "p": "65000.0", "i": "65001.0", "r": "0.0001", "T": T + 1000}},
    CanonicalLiquidationEvent: {"stream": "btcusdt@forceOrder", "data": {
        "e": "forceOrder", "E": T, "o": {"T": T, "S": "SELL", "p": "64900.0", "q": "1.0"}}},
}
SPOT_TRADE = {"stream": "btcusdt@trade", "data": {"e": "trade", "E": T, "s": "BTCUSDT", "t": 7, "p": "65001.0",
                                                    "q": "0.25", "T": T, "m": False, "M": True}}
BYBIT_FRAMES = {
    CanonicalTradeEvent: {"topic": "publicTrade.BTCUSDT", "ts": T, "data": [
        {"i": "t1", "T": T, "p": "65002.0", "v": "0.2", "S": "Buy"}]},
    CanonicalLiquidationEvent: {"topic": "allLiquidation.BTCUSDT", "ts": T, "data": [
        {"T": T, "S": "Sell", "p": "64800.0", "v": "0.3"}]},
    CanonicalOrderBookEvent: {"topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": T, "data": {
        "u": 100, "seq": 100, "cts": T, "b": [["100.0", "1.0"]], "a": [["101.0", "1.0"]]}},
}
BYBIT_TICKER = {"topic": "tickers.BTCUSDT", "type": "snapshot", "ts": T, "data": {
    "symbol": "BTCUSDT", "markPrice": "65000.0", "indexPrice": "65001.0", "openInterest": "1234.5",
    "fundingRate": "0.0001", "nextFundingTime": str(T + 1000)}}


def okx(channel, data, inst="BTC-USDT-SWAP"):
    return {"arg": {"channel": channel, "instId": inst}, "data": data}


OKX_FRAMES = {
    "trades": okx("trades", [{"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "65003", "sz": "1", "side": "buy", "ts": str(T)}]),
    "open-interest": okx("open-interest", [{"instId": "BTC-USDT-SWAP", "ts": str(T), "oi": "12000", "oiCcy": "120.5", "oiUsd": "7800000"}]),
    "mark-price": okx("mark-price", [{"instId": "BTC-USDT-SWAP", "markPx": "65000.5", "ts": str(T)}]),
}
OKX_INDEX = okx("index-tickers", [{"instId": "BTC-USDT", "idxPx": "65000.1", "ts": str(T)}], inst="BTC-USDT")


def okx_liquidation(inst_id):
    fam = inst_id.rsplit("-", 1)[0]
    return {"arg": {"channel": "liquidation-orders", "instType": "SWAP"}, "data": [{
        "instType": "SWAP", "instFamily": fam, "uly": fam, "instId": inst_id, "details": [
            {"ts": str(T), "side": "buy", "bkPx": "65000", "sz": "3", "bkLoss": "0", "ccy": "", "posSide": "long"}]}]}


def _live(adapter, frame):
    return adapter.normalize(json.loads(json.dumps(frame)), local_receive_ts=T)


def _replay(venue, frames):
    return ReplayEngine(venue).run(ReplaySource(
        [wire(T + i, f, i) if not isinstance(f, ReplayFrame) else f for i, f in enumerate(frames)]))


# ---------------------------------------------------------------------------
# 1. Bybit (4C): the adapter DOES stamp, the runner cross-checks, comment can't rot
# ---------------------------------------------------------------------------

def test_bybit_adapter_stamps_every_instrument_scoped_event_type_at_runtime():
    frames = [*BYBIT_FRAMES.values(), BYBIT_TICKER]
    events = [e for f in frames for e in _live(BybitAdapter(), f)]
    kinds = {type(e) for e in events}
    assert kinds == {CanonicalTradeEvent, CanonicalLiquidationEvent, CanonicalOrderBookEvent,
                     CanonicalMarkPriceEvent, CanonicalOIEvent}
    for event in events:
        same_identity(event.instrument, BYBIT_LINEAR_BTCUSDT, type(event).__name__)
    assert BybitAdapter.__dict__["normalize"]._stamps_instrument is True     # the wrapper is active


def _close_all(app):
    for value in vars(app).values():
        if isinstance(value, ParquetWriter):
            value.close()


def test_bybit_runner_persists_the_events_own_identity_and_refuses_a_contradiction(tmp_path):
    app = run_bybit_collector.BybitCollectorApp(data_dir=str(tmp_path))
    (trade,) = _live(BybitAdapter(), BYBIT_FRAMES[CanonicalTradeEvent])
    try:
        app._persist_event(trade)
        with pytest.raises(InstrumentIdError):
            app._persist_event(dataclasses.replace(trade, instrument=OKX_SWAP_BTCUSDT))
    finally:
        _close_all(app)
    (segment,) = list((tmp_path / "raw" / "bybit_trades").glob("*.seg"))
    (row,) = pq.read_table(segment).to_pylist()
    assert row["instrument_key"] == trade.instrument.key == BYBIT_LINEAR_BTCUSDT.key


def test_the_bybit_runner_no_longer_claims_the_adapter_attaches_no_identity():
    source = (Path(run_bybit_collector.__file__)).read_text()
    assert "does not itself attach an InstrumentId" not in source
    assert "DOES stamp" in source


# ---------------------------------------------------------------------------
# 2. Every event type, every venue: identity present, correct, and live == replay
# ---------------------------------------------------------------------------

MATRIX = [
    ("BINANCE", BinanceAdapter, BINANCE_USDM_BTCUSDT, list(BINANCE_FRAMES.values())),
    ("BINANCE_SPOT", BinanceSpotAdapter, BINANCE_SPOT_BTCUSDT, [SPOT_TRADE]),
    ("BYBIT", BybitAdapter, BYBIT_LINEAR_BTCUSDT, [BYBIT_FRAMES[CanonicalTradeEvent],
                                                    BYBIT_FRAMES[CanonicalLiquidationEvent], BYBIT_TICKER]),
    ("OKX", OKXAdapter, OKX_SWAP_BTCUSDT, list(OKX_FRAMES.values())),
]


@pytest.mark.parametrize("venue,adapter_cls,expected,frames", MATRIX, ids=[m[0] for m in MATRIX])
def test_live_and_replay_carry_the_identical_full_identity_for_every_non_book_event(venue, adapter_cls, expected, frames):
    live = [e for f in frames for e in _live(adapter_cls(), f)]
    replayed = _replay(venue, frames).non_book_events
    assert live and len(live) == len(replayed), venue
    for l, r in zip(live, replayed):
        same_identity(l.instrument, expected, f"live {type(l).__name__}")
        same_identity(r.instrument, expected, f"replay {type(r).__name__}")
        same_identity(l.instrument, r.instrument, "live vs replay")


def test_binance_oi_live_and_replay_identity_match_field_by_field():
    live = normalize_binance_oi(BINANCE_OI_BODY, response_receive_ts=T, symbol="BTCUSDT")
    rest = [{"purpose": "open_interest", "response_receive_ts": T, "payload": BINANCE_OI_BODY, "ok": True}]
    (replayed,) = ReplayEngine("BINANCE").run(ReplaySource.from_records(rest_rows=rest)).non_book_events
    same_identity(live.instrument, BINANCE_USDM_BTCUSDT, "live OI")
    same_identity(replayed.instrument, BINANCE_USDM_BTCUSDT, "replay OI")


# ---------------------------------------------------------------------------
# 3. OKX: legitimately unidentified is not accidentally lost
# ---------------------------------------------------------------------------

def test_okx_legitimately_unidentified_events_stay_unidentified_in_live_and_replay():
    eth = okx_liquidation("ETH-USDT-SWAP")
    for frame in (OKX_INDEX, eth):
        live = _live(OKXAdapter(), frame)
        replayed = _replay("OKX", [frame]).non_book_events
        assert live and replayed
        assert all(e.instrument is None for e in live + replayed), frame["arg"]["channel"]


def test_okx_instrument_scoped_channels_are_never_unidentified():
    """The other side of the distinction: identity accidentally lost would show here."""
    for channel, frame in OKX_FRAMES.items():
        for event in _live(OKXAdapter(), frame):
            same_identity(event.instrument, OKX_SWAP_BTCUSDT, channel)
    (btc_liq,) = _live(OKXAdapter(), okx_liquidation("BTC-USDT-SWAP"))
    same_identity(btc_liq.instrument, OKX_SWAP_BTCUSDT, "BTC liquidation")


# ---------------------------------------------------------------------------
# 4. REST snapshot path (4H): built by the adapter, identified, same as live
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("adapter_cls,expected,stream,market", [
    (BinanceAdapter, BINANCE_USDM_BTCUSDT, "orderbook", "linear_perpetual"),
    (BinanceSpotAdapter, BINANCE_SPOT_BTCUSDT, "spot_orderbook", "spot")])
def test_snapshot_event_is_identified_and_shaped_like_the_diffs_it_bridges(adapter_cls, expected, stream, market):
    event = adapter_cls().snapshot_event(102, (), (), local_receive_ts=T, local_process_ts=T + 1)
    same_identity(event.instrument, expected)
    assert (event.exchange, event.stream, event.market_type) == ("BINANCE", stream, market)
    assert event.is_snapshot and event.update_id == 102 and (event.local_receive_ts, event.local_process_ts) == (T, T + 1)


def _capture_book_events(engine):
    seen = []
    for name in ("apply", "binance_snapshot"):
        original = getattr(engine.book, name)

        def wrapper(*args, _orig=original, **kwargs):
            seen.extend(a for a in args if isinstance(a, CanonicalOrderBookEvent))
            return _orig(*args, **kwargs)
        setattr(engine.book, name, wrapper)
    return seen


def _spot_diff(ts, U, u, index):
    return wire(ts, {"stream": "btcusdt@depth", "data": {
        "e": "depthUpdate", "E": ts, "s": "BTCUSDT", "U": U, "u": u,
        "b": [["100.0", "1.0"]], "a": [["101.0", "1.0"]]}}, index)


@pytest.mark.parametrize("venue,expected,frames", [
    ("BINANCE", BINANCE_USDM_BTCUSDT, [_depth_frame(T, U=100, u=105, pu=99, index=0), _snapshot_frame(T + 10, 102, index=1)]),
    ("BINANCE_SPOT", BINANCE_SPOT_BTCUSDT, [_spot_diff(T, 100, 105, 0), _snapshot_frame(T + 10, 102, index=1)]),
])
def test_every_book_event_replay_feeds_the_book_engine_is_identified_including_the_snapshot(venue, expected, frames):
    engine = ReplayEngine(venue)
    seen = _capture_book_events(engine)
    engine.run(ReplaySource(frames))
    assert any(e.is_snapshot for e in seen) and any(not e.is_snapshot for e in seen), "session did not exercise both"
    for event in seen:
        same_identity(event.instrument, expected, f"{venue} snapshot={event.is_snapshot}")


def test_spot_and_usdm_snapshots_and_diffs_differ_in_exactly_market_type_and_stream():
    spot = BinanceSpotAdapter().snapshot_event(1, (), (), local_receive_ts=T)
    perp = BinanceAdapter().snapshot_event(1, (), (), local_receive_ts=T)
    assert spot.instrument != perp.instrument
    assert (spot.instrument.exchange, spot.instrument.instrument, spot.instrument.native_symbol) == (
        perp.instrument.exchange, perp.instrument.instrument, perp.instrument.native_symbol)
    assert spot.instrument.key == "BINANCE|spot|BTC-USDT|BTCUSDT"
    assert perp.instrument.key == "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT"


def test_no_production_module_hand_builds_a_canonical_book_event_outside_an_adapter():
    """Structural guard for the root cause: the three former hand-built sites."""
    root = Path(replay_module.__file__).resolve().parents[1]
    for rel in ("collector/replay.py", "run_collector.py", "run_binance_spot_collector.py"):
        tree = ast.parse((root / rel).read_text())
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
                 and getattr(n.func, "id", getattr(n.func, "attr", "")) == "CanonicalOrderBookEvent"]
        assert not calls, f"{rel} hand-builds CanonicalOrderBookEvent (bypasses identity stamping)"


# ---------------------------------------------------------------------------
# 5. Source of truth for replay identity (4I)
# ---------------------------------------------------------------------------

def test_replay_never_reads_or_trusts_a_persisted_canonical_instrument_key():
    """Replay's identity is raw frame + venue adapter. A persisted canonical
    ``instrument_key`` is a derived assertion; a corrupted one cannot become truth
    because replay does not read canonical streams at all."""
    source = Path(replay_module.__file__).read_text()
    assert "instrument_key" not in source
    tree = ast.parse(source)
    read_args = {c.args[0].value for c in ast.walk(tree) if isinstance(c, ast.Call)
                 and getattr(c.func, "id", "") == "read" and c.args and isinstance(c.args[0], ast.Constant)}
    assert read_args == {"raw_wire", "raw_rest"}     # the only streams from_directory reads


# ---------------------------------------------------------------------------
# 6. End to end: raw -> adapter -> replay -> causal alignment (all four venues)
# ---------------------------------------------------------------------------

def _end_to_end_events():
    plan = [
        ("BINANCE", [BINANCE_FRAMES[CanonicalTradeEvent], BINANCE_FRAMES[CanonicalMarkPriceEvent]]),
        ("BINANCE_SPOT", [SPOT_TRADE]),
        ("BYBIT", [BYBIT_FRAMES[CanonicalTradeEvent]]),
        ("OKX", [OKX_FRAMES["trades"], OKX_INDEX]),
    ]
    events = []
    for venue, frames in plan:
        events += _replay(venue, frames).non_book_events
    return events


def test_replay_then_alignment_keeps_identity_excludes_the_future_and_stays_deterministic():
    events = _end_to_end_events()
    at = T + 10
    # every replayed event was received at T + its index; add a strictly-future one and a stale one
    future = dataclasses.replace(events[0], local_receive_ts=at + 1, exchange_event_ts=T - 3_600_000)
    out = causally_align(events + [future], at, staleness_ms=5)
    keys = list(out)

    assert len({k[2] for k in out}) >= 5                                  # 4 instruments + UNIDENTIFIED (OKX index)
    identified = {k[2] for k in out} - {UNIDENTIFIED}
    assert identified == {i.key for i in (BINANCE_USDM_BTCUSDT, BINANCE_SPOT_BTCUSDT, BYBIT_LINEAR_BTCUSDT, OKX_SWAP_BTCUSDT)}
    assert (("OKX", "linear_perpetual", UNIDENTIFIED, "index-tickers")) in out   # legit unidentified stays observable
    binance_trade = out[("BINANCE", "linear_perpetual", BINANCE_USDM_BTCUSDT.key, "trades")]
    assert binance_trade.event.local_receive_ts <= at and binance_trade.event is not future   # future excluded
    for key, obs in out.items():
        same_identity(obs.event.instrument, None if key[2] == UNIDENTIFIED else InstrumentId.from_key(key[2]), key)
        assert (obs.event.exchange, obs.event.market_type, obs.event.stream) == (key[0], key[1], key[3])

    assert out == causally_align(list(reversed(events + [future])), at, staleness_ms=5)      # order-independent
    assert out == causally_align(events + [future], at, staleness_ms=5)                       # deterministic
    assert any(o.status is AlignmentStatus.STALE for o in causally_align(events, T + 10_000, staleness_ms=5).values())
    assert keys == sorted(keys)


def test_replay_runs_are_deterministic_and_identity_is_part_of_their_digest():
    a, b = _replay("BYBIT", [BYBIT_FRAMES[CanonicalTradeEvent]]), _replay("BYBIT", [BYBIT_FRAMES[CanonicalTradeEvent]])
    assert a.digest == b.digest
    a.non_book_events[0] = dataclasses.replace(a.non_book_events[0], instrument=None)
    assert a.digest != b.digest
