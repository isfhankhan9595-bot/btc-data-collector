"""First-class instrument identity: adversarial tests.

Identity is (exchange, market_type, instrument, native_symbol). Every collision
below is one the previous ``(exchange, market_type, stream)`` model could not
express or could not tell apart.
"""
from __future__ import annotations

import dataclasses
import json
from itertools import combinations

import pyarrow.parquet as pq
import pytest

from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.adapters.binance_spot import BinanceSpotAdapter
from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.adapters.okx import OKXAdapter
from collector.collector.canonical import CanonicalTradeEvent
from collector.collector.instrument import (
    BINANCE_SPOT_BTCUSDT, BINANCE_USDM_BTCUSDT, BYBIT_LINEAR_BTCUSDT, OKX_SWAP_BTCUSDT,
    SUPPORTED_INSTRUMENTS, InstrumentId, InstrumentIdError, resolve_instrument, resolve_raw_record,
)
from collector.collector.market_state import MarketStateEngine
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.raw_capture import RAW_WIRE_SCHEMA, RawCapture, RawWireRecord
from collector.collector.replay import ReplayEngine, ReplaySource
from collector.collector.storage_layout import iter_segments, venue_stream
from collector.pipeline.cross_exchange_alignment import UNIDENTIFIED, causally_align

T = 1_780_000_000_000


# ---------------------------------------------------------------------------
# frames (real wire shapes, one trade per venue)
# ---------------------------------------------------------------------------

FRAMES = {
    "BINANCE": (BinanceAdapter, BINANCE_USDM_BTCUSDT, {"stream": "btcusdt@aggTrade", "data": {
        "e": "aggTrade", "E": T, "T": T, "a": 1, "p": "65000.0", "q": "0.5", "m": False}}),
    "BINANCE_SPOT": (BinanceSpotAdapter, BINANCE_SPOT_BTCUSDT, {"stream": "btcusdt@trade", "data": {
        "e": "trade", "E": T, "s": "BTCUSDT", "t": 12345, "p": "65001.0", "q": "0.25", "T": T,
        "m": False, "M": True}}),
    "BYBIT": (BybitAdapter, BYBIT_LINEAR_BTCUSDT, {"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": T,
        "data": [{"T": T, "s": "BTCUSDT", "S": "Buy", "v": "0.01", "p": "65002.5", "i": "b1", "BT": False}]}),
    "OKX": (OKXAdapter, OKX_SWAP_BTCUSDT, {"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [
        {"instId": "BTC-USDT-SWAP", "tradeId": "o1", "px": "65003", "sz": "1", "side": "buy", "ts": str(T)}]}),
}


def _normalize(venue):
    adapter_cls, _, frame = FRAMES[venue]
    return adapter_cls().normalize(json.loads(json.dumps(frame)), local_receive_ts=T)


# ---------------------------------------------------------------------------
# 1. the type: distinctness, equality/hash, strictness
# ---------------------------------------------------------------------------

def test_the_four_supported_instruments_are_pairwise_distinct():
    assert len(set(SUPPORTED_INSTRUMENTS)) == 4
    for a, b in combinations(SUPPORTED_INSTRUMENTS, 2):
        assert a != b and a.key != b.key


def test_spot_and_perpetual_of_one_venue_and_symbol_differ_only_by_market_type():
    assert BINANCE_SPOT_BTCUSDT != BINANCE_USDM_BTCUSDT
    assert (BINANCE_SPOT_BTCUSDT.exchange, BINANCE_SPOT_BTCUSDT.instrument, BINANCE_SPOT_BTCUSDT.native_symbol) == (
        BINANCE_USDM_BTCUSDT.exchange, BINANCE_USDM_BTCUSDT.instrument, BINANCE_USDM_BTCUSDT.native_symbol)


@pytest.mark.parametrize("field,other", [("exchange", "BYBIT"), ("market_type", "spot"),
                                          ("instrument", "ETH-USDT"), ("native_symbol", "XBTUSDT")])
def test_every_field_participates_in_equality_and_hashing(field, other):
    base = BINANCE_USDM_BTCUSDT
    changed = InstrumentId(**{**base.to_dict(), field: other})
    assert changed != base and changed.key != base.key
    assert len({base, changed}) == 2


def test_equal_identities_hash_equal_and_sort_deterministically():
    twin = InstrumentId.from_dict(OKX_SWAP_BTCUSDT.to_dict())
    assert twin == OKX_SWAP_BTCUSDT and hash(twin) == hash(OKX_SWAP_BTCUSDT)
    assert sorted(SUPPORTED_INSTRUMENTS) == sorted(reversed(SUPPORTED_INSTRUMENTS))


def test_venues_share_the_canonical_instrument_but_keep_their_own_native_symbol():
    assert {i.instrument for i in SUPPORTED_INSTRUMENTS} == {"BTC-USDT"}
    assert OKX_SWAP_BTCUSDT.native_symbol == "BTC-USDT-SWAP" != BYBIT_LINEAR_BTCUSDT.native_symbol == "BTCUSDT"


@pytest.mark.parametrize("bad", [
    dict(exchange="binance"),                 # case is never folded
    dict(exchange="BINANCE_SPOT"),            # a storage namespace is not an exchange
    dict(exchange=""), dict(exchange="BIN ANCE"),
    dict(market_type="Spot"), dict(market_type="perpetual"), dict(market_type=""),
    dict(instrument="btc-usdt"), dict(instrument="BTCUSDT"), dict(instrument="BTC-USDT-SWAP"),
    dict(native_symbol=""), dict(native_symbol="BTC USDT"), dict(native_symbol="BTC|USDT"),
    dict(exchange=None), dict(native_symbol=123),
])
def test_construction_is_strict_nothing_is_silently_repaired(bad):
    with pytest.raises(InstrumentIdError):
        InstrumentId(**{**BINANCE_USDM_BTCUSDT.to_dict(), **bad})


def test_native_symbol_is_preserved_verbatim_and_case_sensitive():
    lower = InstrumentId("BINANCE", "spot", "BTC-USDT", "btcusdt")
    assert lower.native_symbol == "btcusdt" and lower != BINANCE_SPOT_BTCUSDT


# ---------------------------------------------------------------------------
# 2. serialization determinism
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ident", SUPPORTED_INSTRUMENTS)
def test_key_and_dict_round_trip_losslessly_and_deterministically(ident):
    assert InstrumentId.from_key(ident.key) == ident
    assert InstrumentId.from_dict(json.loads(json.dumps(ident.to_dict()))) == ident
    assert json.dumps(ident.to_dict(), sort_keys=True) == json.dumps(
        InstrumentId.from_key(ident.key).to_dict(), sort_keys=True)


@pytest.mark.parametrize("bad", ["", "BINANCE|spot|BTC-USDT", "a|b|c|d|e", "BINANCE|spot|btc-usdt|BTCUSDT"])
def test_malformed_keys_are_refused(bad):
    with pytest.raises(InstrumentIdError):
        InstrumentId.from_key(bad)
    with pytest.raises(InstrumentIdError):
        InstrumentId.from_dict({"exchange": "BINANCE"})


# ---------------------------------------------------------------------------
# 3. resolution: display symbol alone is never identity; wrong pairings resolve to nothing
# ---------------------------------------------------------------------------

def test_identity_is_not_derivable_from_a_native_symbol_alone():
    assert BINANCE_SPOT_BTCUSDT.native_symbol == BINANCE_USDM_BTCUSDT.native_symbol == BYBIT_LINEAR_BTCUSDT.native_symbol
    assert len({resolve_instrument(i.exchange, i.market_type, i.native_symbol) for i in SUPPORTED_INSTRUMENTS}) == 4


@pytest.mark.parametrize("triple", [
    ("BYBIT", "linear_perpetual", "BTC-USDT-SWAP"),   # OKX's symbol on Bybit
    ("OKX", "linear_perpetual", "BTCUSDT"),           # Bybit/Binance's symbol on OKX
    ("BINANCE", "linear_perpetual", "BTC-USDT-SWAP"),
    ("BYBIT", "spot", "BTCUSDT"),                      # Bybit spot is not a supported instrument
    ("OKX", "spot", "BTC-USDT-SWAP"),
    ("binance", "spot", "BTCUSDT"),                    # case is not folded
    ("BINANCE", "spot", "btcusdt"),
])
def test_wrong_venue_symbol_pairings_resolve_to_nothing_never_to_a_near_match(triple):
    assert resolve_instrument(*triple) is None


@pytest.mark.parametrize("row,expected", [
    (("BINANCE", "linear_perpetual", "BTCUSDT"), BINANCE_USDM_BTCUSDT),
    (("BINANCE_SPOT", "spot", "BTCUSDT"), BINANCE_SPOT_BTCUSDT),
    (("BYBIT", "linear_perpetual", "BTCUSDT"), BYBIT_LINEAR_BTCUSDT),
    (("OKX", "linear_perpetual", "BTC-USDT-SWAP"), OKX_SWAP_BTCUSDT),
    (("BINANCE_SPOT", "linear_perpetual", "BTCUSDT"), None),   # contradictory row must NOT become the perp
    (("BINANCE", "spot", "BTCUSDT"), BINANCE_SPOT_BTCUSDT),
    (("MADEUP", "spot", "BTCUSDT"), None),
    (("OKX", None, "BTC-USDT-SWAP"), None), ((None, "spot", "BTCUSDT"), None), (("OKX", "linear_perpetual", None), None),
    (("OKX", "linear_perpetual", ""), None),
])
def test_raw_record_columns_resolve_exactly_or_to_none_for_legacy_and_contradictory_rows(row, expected):
    assert resolve_raw_record(*row) == expected


# ---------------------------------------------------------------------------
# 4. adapters stamp identity on real frames
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("venue", list(FRAMES))
def test_every_adapter_stamps_its_instrument_on_real_frames(venue):
    events = _normalize(venue)
    assert events, venue
    assert {e.instrument for e in events} == {FRAMES[venue][1]}


def test_an_event_built_without_identity_is_unidentified_not_defaulted():
    bare = CanonicalTradeEvent("BINANCE", "trades", T, None, T)
    assert bare.instrument is None and bare.market_type == "linear_perpetual"


def test_the_stamping_hook_refuses_an_event_that_contradicts_the_adapter():
    spot_event = CanonicalTradeEvent("BINANCE", "trades", T, None, T, market_type="spot")
    with pytest.raises(InstrumentIdError):
        BinanceAdapter()._stamp_instrument([spot_event])                 # perp adapter, spot event
    wrong_venue = CanonicalTradeEvent("BYBIT", "trades", T, None, T)
    with pytest.raises(InstrumentIdError):
        BinanceAdapter()._stamp_instrument([wrong_venue])


def test_okx_adapter_configured_for_an_unregistered_instrument_is_unidentified_not_faked():
    adapter = OKXAdapter(inst_id="ETH-USDT-SWAP")
    assert adapter.instrument is None
    frame = json.loads(json.dumps(FRAMES["OKX"][2]))
    frame["arg"]["instId"] = frame["data"][0]["instId"] = "ETH-USDT-SWAP"
    events = adapter.normalize(frame, local_receive_ts=T)
    assert events and all(e.instrument is None for e in events)


def _okx_liquidation(inst_id):
    return {"arg": {"channel": "liquidation-orders", "instType": "SWAP"}, "data": [{
        "instType": "SWAP", "instFamily": inst_id.rsplit("-", 1)[0], "uly": inst_id.rsplit("-", 1)[0],
        "instId": inst_id, "details": [{"ts": str(T), "side": "buy", "bkPx": "65000", "sz": "3",
                                        "bkLoss": "0", "ccy": "", "posSide": "long"}]}]}


def test_okx_liquidations_are_identified_only_when_the_row_is_the_adapters_own_instrument():
    """instType-scoped stream: one raw stream, many instruments. Never assume BTC."""
    btc = OKXAdapter().normalize(_okx_liquidation("BTC-USDT-SWAP"), local_receive_ts=T)
    eth = OKXAdapter().normalize(_okx_liquidation("ETH-USDT-SWAP"), local_receive_ts=T)
    assert [e.instrument for e in btc] == [OKX_SWAP_BTCUSDT]
    assert [e.instrument for e in eth] == [None] and eth[0].inst_id == "ETH-USDT-SWAP"


def test_okx_index_tickers_are_not_stamped_with_the_swaps_identity():
    """Keyed by the index pair (BTC-USDT), not the swap: OKX open question #3."""
    frame = {"arg": {"channel": "index-tickers", "instId": "BTC-USDT"},
             "data": [{"instId": "BTC-USDT", "idxPx": "65000.1", "ts": str(T)}]}
    (event,) = OKXAdapter().normalize(frame, local_receive_ts=T)
    assert event.instrument is None and event.index_price == 65000.1


# ---------------------------------------------------------------------------
# 5. alignment: instrument is part of the key
# ---------------------------------------------------------------------------

def _trade(instrument, *, recv=T - 1, exchange=None, market_type=None):
    return CanonicalTradeEvent(exchange or instrument.exchange, "trades", recv, None, recv,
                               market_type=market_type or instrument.market_type, instrument=instrument,
                               trade_id="1", price=1.0, quantity=1.0, side="Buy")


def test_spot_and_perpetual_and_other_venues_never_collide_in_alignment():
    events = [_trade(i) for i in SUPPORTED_INSTRUMENTS]
    out = causally_align(events, T, staleness_ms=1_000)
    assert len(out) == 4
    assert {k[2] for k in out} == {i.key for i in SUPPORTED_INSTRUMENTS}


def test_two_instruments_with_identical_exchange_market_and_stream_do_not_collide():
    """The exact case the 3-tuple key could not separate."""
    btc = InstrumentId("BINANCE", "spot", "BTC-USDT", "BTCUSDT")
    eth = InstrumentId("BINANCE", "spot", "ETH-USDT", "ETHUSDT")
    out = causally_align([_trade(btc), _trade(eth)], T, staleness_ms=1_000)
    assert len(out) == 2


def test_unidentified_events_never_collide_with_identified_ones():
    ident = _trade(BINANCE_USDM_BTCUSDT, recv=T - 5)
    bare = CanonicalTradeEvent("BINANCE", "trades", T - 1, None, T - 1)
    out = causally_align([ident, bare], T, staleness_ms=1_000)
    assert {k[2] for k in out} == {BINANCE_USDM_BTCUSDT.key, UNIDENTIFIED}


def test_alignment_semantics_are_unchanged_receive_time_only_and_stale_is_kept():
    ident = BINANCE_SPOT_BTCUSDT
    late = _trade(ident, recv=T + 1)
    assert causally_align([late], T, staleness_ms=10) == {}
    old = _trade(ident, recv=T - 500)
    (obs,) = causally_align([old], T, staleness_ms=10).values()
    assert obs.status.value == "STALE" and obs.event is old


# ---------------------------------------------------------------------------
# 6. MarketState: one engine, one instrument
# ---------------------------------------------------------------------------

def test_a_bound_engine_refuses_spot_events_even_from_the_same_exchange():
    engine = MarketStateEngine("BINANCE", instrument=BINANCE_USDM_BTCUSDT)
    engine.update(_trade(BINANCE_USDM_BTCUSDT))
    with pytest.raises(ValueError, match="different instruments"):
        engine.update(_trade(BINANCE_SPOT_BTCUSDT))              # same exchange, same symbol, other market
    with pytest.raises(ValueError, match="unidentified"):
        engine.update(CanonicalTradeEvent("BINANCE", "trades", T, None, T))
    assert engine.snapshot(T).instrument == BINANCE_USDM_BTCUSDT


def test_engine_instrument_must_belong_to_its_exchange():
    with pytest.raises(ValueError):
        MarketStateEngine("BYBIT", instrument=BINANCE_USDM_BTCUSDT)


def test_an_unbound_engine_keeps_its_previous_behaviour_and_states_differ_by_instrument():
    unbound = MarketStateEngine("BINANCE")
    unbound.update(CanonicalTradeEvent("BINANCE", "trades", T, None, T, trade_id="1", price=1.0, quantity=1.0, side="Buy"))
    assert unbound.snapshot(T).instrument is None
    spot, perp = (MarketStateEngine("BINANCE", instrument=i) for i in (BINANCE_SPOT_BTCUSDT, BINANCE_USDM_BTCUSDT))
    for engine, ident in ((spot, BINANCE_SPOT_BTCUSDT), (perp, BINANCE_USDM_BTCUSDT)):
        engine.update(_trade(ident))
    assert spot.snapshot(T).digest() != perp.snapshot(T).digest()       # identical observations, different instruments


# ---------------------------------------------------------------------------
# 7. replay and storage preserve identity end to end
# ---------------------------------------------------------------------------

def _record(base, venue, market_type, symbol, frame):
    writer = ParquetWriter(venue_stream(venue, "raw_wire"), RAW_WIRE_SCHEMA, base_dir=str(base), exchange=venue,
                           segment_rows=1000, segment_seconds=3600)
    RawCapture(writer, None).capture_wire(RawWireRecord(
        local_receive_ts=T, payload=json.dumps(frame), venue=venue, connection_id=f"{venue}-1",
        symbol=symbol, market_type=market_type))
    writer.close()


RECORDINGS = [("BINANCE", "linear_perpetual", "BTCUSDT"), ("BINANCE_SPOT", "spot", "BTCUSDT"),
              ("BYBIT", "linear_perpetual", "BTCUSDT"), ("OKX", "linear_perpetual", "BTC-USDT-SWAP")]


def test_identity_survives_storage_and_replay_for_all_four_instruments(tmp_path):
    for venue, market_type, symbol in RECORDINGS:
        _record(tmp_path, venue, market_type, symbol, FRAMES[venue][2])

    for venue, market_type, symbol in RECORDINGS:
        expected = FRAMES[venue][1]
        (segment,) = list(iter_segments(tmp_path, venue_stream(venue, "raw_wire")))
        (row,) = pq.read_table(segment).to_pylist()
        assert (row["venue"], row["market_type"], row["symbol"]) == (venue, market_type, symbol)   # native symbol verbatim
        assert resolve_raw_record(row["venue"], row["market_type"], row["symbol"]) == expected

        live = _normalize(venue)
        replayed = ReplayEngine(venue).run(ReplaySource.from_directory(str(tmp_path), venue=venue))
        assert replayed.non_book_events, venue
        assert [e.instrument for e in replayed.non_book_events] == [expected] * len(replayed.non_book_events)
        assert replayed.non_book_events[0].instrument == live[0].instrument       # replay == live identity


def test_replay_never_assigns_one_venues_identity_to_another_venues_frames(tmp_path):
    for venue, market_type, symbol in RECORDINGS:
        _record(tmp_path, venue, market_type, symbol, FRAMES[venue][2])
    seen = {}
    for venue, *_ in RECORDINGS:
        events = ReplayEngine(venue).run(ReplaySource.from_directory(str(tmp_path), venue=venue)).non_book_events
        seen[venue] = {e.instrument for e in events}
    assert set().union(*seen.values()) == set(SUPPORTED_INSTRUMENTS)
    assert all(len(v) == 1 for v in seen.values())


def test_the_replay_digest_depends_on_instrument_identity(tmp_path):
    _record(tmp_path, "BINANCE_SPOT", "spot", "BTCUSDT", FRAMES["BINANCE_SPOT"][2])
    result = ReplayEngine("BINANCE_SPOT").run(ReplaySource.from_directory(str(tmp_path), venue="BINANCE_SPOT"))
    before = result.digest
    result.non_book_events[0] = dataclasses.replace(result.non_book_events[0], instrument=BINANCE_USDM_BTCUSDT)
    assert result.digest != before, "spot and perpetual observations must not digest identically"


def test_legacy_rows_without_symbol_resolve_to_none_but_replay_uses_the_venue_adapter(tmp_path):
    """Legacy rows lack the identity columns. They are never reinterpreted from
    the row; replay identifies them by the adapter bound to the venue namespace
    (single-instrument by configuration) and resolve_raw_record says 'unknown'."""
    _record(tmp_path, "BYBIT", "linear_perpetual", None, FRAMES["BYBIT"][2])
    (segment,) = list(iter_segments(tmp_path, venue_stream("BYBIT", "raw_wire")))
    (row,) = pq.read_table(segment).to_pylist()
    assert row["symbol"] is None and resolve_raw_record(row["venue"], row["market_type"], row["symbol"]) is None
    replayed = ReplayEngine("BYBIT").run(ReplaySource.from_directory(str(tmp_path), venue="BYBIT"))
    assert {e.instrument for e in replayed.non_book_events} == {BYBIT_LINEAR_BTCUSDT}
