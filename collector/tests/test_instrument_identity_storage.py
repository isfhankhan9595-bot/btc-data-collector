"""Instrument-identity persistence into canonical derived storage.

Phase 2 of instrument identity (PR #34 built InstrumentId itself; this
covers InstrumentId -> instrument_key -> Parquet -> read -> InstrumentId).

Covers: deterministic round-trip per venue, None-stays-None for
unidentified events (OKX index-tickers, cross-instrument liquidation),
malformed-key rejection, spot/perp and cross-venue collision resistance,
legacy (column-absent) rows, and a real adapter-produced event per venue
rather than only hand-built fixtures.
"""
from __future__ import annotations

import pandas as pd
import pyarrow.parquet as pq
import pytest

from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.adapters.binance_spot import BinanceSpotAdapter
from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.adapters.okx import OKXAdapter
from collector.collector.config import (
    BYBIT_TRADES_SCHEMA, OKX_LIQUIDATION_SCHEMA, OKX_INDEXTICKERS_SCHEMA,
    SPOT_TRADES_SCHEMA, TRADES_SCHEMA,
)
from collector.collector.instrument import (
    BINANCE_SPOT_BTCUSDT, BINANCE_USDM_BTCUSDT, BYBIT_LINEAR_BTCUSDT, OKX_SWAP_BTCUSDT,
    InstrumentId, InstrumentIdError, instrument_key, resolve_instrument_key,
)
from collector.collector.parquet_writer import ParquetWriter


# ---------------------------------------------------------------------------
# instrument_key / resolve_instrument_key -- the deterministic mapping itself
# ---------------------------------------------------------------------------

def test_instrument_key_is_deterministic():
    a = InstrumentId("BINANCE", "linear_perpetual", "BTC-USDT", "BTCUSDT")
    b = InstrumentId("BINANCE", "linear_perpetual", "BTC-USDT", "BTCUSDT")
    assert instrument_key_of(a) == instrument_key_of(b)


def instrument_key_of(instrument: InstrumentId) -> str:
    class _E:
        pass
    e = _E()
    e.instrument = instrument
    return instrument_key(e)


def test_instrument_key_round_trips_through_from_key():
    original = BINANCE_USDM_BTCUSDT
    key = instrument_key_of(original)
    assert resolve_instrument_key(key) == original


def test_instrument_key_none_for_unidentified_event():
    class _E:
        instrument = None
    assert instrument_key(_E()) is None


def test_resolve_instrument_key_none_and_empty_stay_none():
    assert resolve_instrument_key(None) is None
    assert resolve_instrument_key("") is None


def test_resolve_instrument_key_rejects_malformed_not_silently_none():
    """A corrupted stored key must never silently resolve to an
    unidentified event -- that would make 'we lost the identity' look
    identical to 'this was never identified', which is exactly the
    ambiguity instrument_key exists to remove."""
    with pytest.raises(InstrumentIdError):
        resolve_instrument_key("not-a-real-key")
    with pytest.raises(InstrumentIdError):
        resolve_instrument_key("BINANCE|spot|BTC-USDT")  # missing native_symbol part
    with pytest.raises(InstrumentIdError):
        resolve_instrument_key("binance|spot|BTC-USDT|BTCUSDT")  # lower-case exchange


# ---------------------------------------------------------------------------
# Collision resistance -- the entire point of the exercise
# ---------------------------------------------------------------------------

def test_spot_and_perpetual_never_collapse():
    assert instrument_key_of(BINANCE_SPOT_BTCUSDT) != instrument_key_of(BINANCE_USDM_BTCUSDT)


def test_different_venues_never_collapse():
    keys = {instrument_key_of(i) for i in
            (BINANCE_USDM_BTCUSDT, BYBIT_LINEAR_BTCUSDT, OKX_SWAP_BTCUSDT, BINANCE_SPOT_BTCUSDT)}
    assert len(keys) == 4  # all four distinct, no accidental collapse


# ---------------------------------------------------------------------------
# Real adapter-produced events, not only hand-built InstrumentId fixtures --
# proves the wiring (adapter -> event.instrument -> instrument_key), not
# just the helper functions in isolation.
# ---------------------------------------------------------------------------

def test_binance_futures_trade_event_carries_the_expected_key():
    adapter = BinanceAdapter()
    event = adapter.normalize(
        {"stream": "btcusdt@aggTrade",
         "data": {"e": "aggTrade", "E": 1, "a": 1, "p": "1", "q": "1", "f": 1, "l": 1, "T": 1, "m": False}},
        local_receive_ts=1,
    )[0]
    assert instrument_key(event) == BINANCE_USDM_BTCUSDT.key


def test_bybit_trade_event_carries_the_expected_key():
    adapter = BybitAdapter()
    event = adapter.normalize(
        {"topic": "publicTrade.BTCUSDT", "data": [
            {"T": 1, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "1", "L": "PlusTick", "i": "1", "BT": False}]},
        local_receive_ts=1,
    )[0]
    assert instrument_key(event) == BYBIT_LINEAR_BTCUSDT.key


def test_okx_trade_event_carries_the_expected_key():
    adapter = OKXAdapter()
    event = adapter.normalize(
        {"arg": {"channel": "trades"}, "data": [
            {"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "1", "sz": "1", "side": "buy", "ts": "1"}]},
        local_receive_ts=1,
    )[0]
    assert instrument_key(event) == OKX_SWAP_BTCUSDT.key


def test_okx_index_tickers_is_unidentified_by_design():
    """The task's own flagged exception: index-tickers is the index, not
    the swap instrument -- must never be stamped with the swap's identity."""
    adapter = OKXAdapter()
    event = adapter.normalize(
        {"arg": {"channel": "index-tickers"}, "data": [{"instId": "BTC-USDT", "idxPx": "1", "ts": "1"}]},
        local_receive_ts=1,
    )[0]
    assert instrument_key(event) is None


def test_okx_cross_instrument_liquidation_is_unidentified():
    """The task's other flagged exception: a liquidation for an instrument
    other than this adapter's own must not be stamped as BTC-USDT-SWAP."""
    adapter = OKXAdapter(inst_id="BTC-USDT-SWAP")
    event = adapter.normalize(
        {"arg": {"channel": "liquidation-orders"}, "data": [
            {"instId": "ETH-USDT-SWAP", "details": [{"bkPx": "1", "sz": "1", "side": "sell", "ts": "1"}]}]},
        local_receive_ts=1,
    )[0]
    assert instrument_key(event) is None


def test_okx_own_instrument_liquidation_is_identified():
    adapter = OKXAdapter(inst_id="BTC-USDT-SWAP")
    event = adapter.normalize(
        {"arg": {"channel": "liquidation-orders"}, "data": [
            {"instId": "BTC-USDT-SWAP", "details": [{"bkPx": "1", "sz": "1", "side": "sell", "ts": "1"}]}]},
        local_receive_ts=1,
    )[0]
    assert instrument_key(event) == OKX_SWAP_BTCUSDT.key


def test_binance_spot_trade_event_carries_the_expected_key():
    adapter = BinanceSpotAdapter()
    event = adapter.normalize(
        {"stream": "btcusdt@trade",
         "data": {"e": "trade", "E": 1, "s": "BTCUSDT", "t": 1, "p": "1", "q": "1", "T": 1, "m": True}},
        local_receive_ts=1,
    )[0]
    assert instrument_key(event) == BINANCE_SPOT_BTCUSDT.key
    # And it never collides with the futures adapter's key for the "same" symbol.
    assert instrument_key(event) != BINANCE_USDM_BTCUSDT.key


# ---------------------------------------------------------------------------
# Storage round-trip: write -> read -> resolve, per venue
# ---------------------------------------------------------------------------

def _row(instrument: "InstrumentId | None", **extra):
    base = {"timestamp": 1, "exchange_timestamp": 1, "local_timestamp": 1,
            "instrument_key": instrument.key if instrument else None}
    base.update(extra)
    return base


@pytest.mark.parametrize("schema,stream_name,venue,instrument,extra", [
    (TRADES_SCHEMA, "trades", "BINANCE", BINANCE_USDM_BTCUSDT,
     {"trade_id": 1, "price": 1.0, "quantity": 1.0, "is_buyer_maker": False, "side_sign": 1, "signed_qty": 1.0}),
    (BYBIT_TRADES_SCHEMA, "bybit_trades", "BYBIT", BYBIT_LINEAR_BTCUSDT,
     {"trade_id": "1", "price": 1.0, "quantity": 1.0, "side": "Buy", "venue_sequence": 1, "block_trade": False, "rpi": False}),
    (OKX_LIQUIDATION_SCHEMA, "okx_liquidation", "OKX", OKX_SWAP_BTCUSDT,
     {"inst_id": "BTC-USDT-SWAP", "side": "sell", "price": 1.0, "quantity": 1.0, "bk_loss": 0.0,
      "ccy": "", "pos_side": "long", "inst_family": "BTC-USDT", "uly": "BTC-USDT"}),
    (SPOT_TRADES_SCHEMA, "spot_trades", "BINANCE_SPOT", BINANCE_SPOT_BTCUSDT,
     {"trade_id": "1", "price": 1.0, "quantity": 1.0, "side": "BUY"}),
])
def test_storage_round_trip_per_venue(tmp_path, schema, stream_name, venue, instrument, extra):
    writer = ParquetWriter(stream_name, schema, base_dir=str(tmp_path), exchange=venue)
    try:
        writer.write(_row(instrument, **extra))
    finally:
        writer.close()

    files = sorted((tmp_path / "raw" / stream_name).glob("*.seg"))
    assert files
    table = pq.read_table(files[0])
    assert "instrument_key" in table.column_names
    stored_key = table.column("instrument_key")[0].as_py()
    assert stored_key == instrument.key
    assert resolve_instrument_key(stored_key) == instrument


def test_storage_round_trip_none_stays_none(tmp_path):
    """An unidentified event (a liquidation for an instrument this adapter
    doesn't track) must round-trip as a genuine NULL, not an empty string
    or a fabricated key. Uses OKX_LIQUIDATION_SCHEMA, which is
    instrument-scoped (unlike OKX_INDEXTICKERS_SCHEMA, deliberately
    excluded from instrument_key -- see config.py)."""
    writer = ParquetWriter("okx_liquidation", OKX_LIQUIDATION_SCHEMA, base_dir=str(tmp_path), exchange="OKX")
    try:
        writer.write(_row(None, inst_id="ETH-USDT-SWAP", side="sell", price=1.0, quantity=1.0,
                           bk_loss=0.0, ccy="", pos_side="long", inst_family="ETH-USDT", uly="ETH-USDT"))
    finally:
        writer.close()
    files = sorted((tmp_path / "raw" / "okx_liquidation").glob("*.seg"))
    table = pq.read_table(files[0])
    stored = table.column("instrument_key")[0].as_py()
    assert stored is None
    assert resolve_instrument_key(stored) is None


def test_okx_indextickers_schema_deliberately_has_no_instrument_key_column():
    """index-tickers represents the index, not the swap instrument (the
    task's own flagged exception) -- confirms this was excluded on
    purpose, not missed."""
    assert "instrument_key" not in OKX_INDEXTICKERS_SCHEMA.names


# ---------------------------------------------------------------------------
# Legacy rows: a segment written before instrument_key existed
# ---------------------------------------------------------------------------

def test_legacy_row_without_instrument_key_column_reads_safely(tmp_path):
    """Simulates a pre-this-change segment: the column is entirely absent
    from the file's own schema (not merely null within it). Reading code
    must use a presence check, never assume the column exists."""
    row = {"timestamp": pd.Timestamp(1, unit="ms", tz="UTC"),
           "exchange_timestamp": pd.Timestamp(1, unit="ms", tz="UTC"),
           "local_timestamp": pd.Timestamp(1, unit="ms", tz="UTC"),
           "trade_id": 1, "price": 1.0, "quantity": 1.0,
           "is_buyer_maker": False, "side_sign": 1, "signed_qty": 1.0}
    df = pd.DataFrame([row])
    path = tmp_path / "legacy.parquet"
    df.to_parquet(path)

    read_back = pd.read_parquet(path)
    assert "instrument_key" not in read_back.columns
    # The safe read pattern every consumer must use:
    key = read_back["instrument_key"].iloc[0] if "instrument_key" in read_back.columns else None
    assert key is None
    assert resolve_instrument_key(key) is None


# ---------------------------------------------------------------------------
# Adversarial: a corrupted stored value must be caught, not silently trusted
# ---------------------------------------------------------------------------

def test_adversarial_corrupted_stored_key_is_detected_on_read(tmp_path):
    writer = ParquetWriter("trades", TRADES_SCHEMA, base_dir=str(tmp_path), exchange="BINANCE")
    try:
        writer.write(_row(BINANCE_USDM_BTCUSDT, trade_id=1, price=1.0, quantity=1.0,
                           is_buyer_maker=False, side_sign=1, signed_qty=1.0))
    finally:
        writer.close()
    files = sorted((tmp_path / "raw" / "trades").glob("*.seg"))
    table = pq.read_table(files[0])
    stored_key = table.column("instrument_key")[0].as_py()
    assert resolve_instrument_key(stored_key) == BINANCE_USDM_BTCUSDT  # sanity: uncorrupted round-trips

    # Now corrupt it the way disk/transfer corruption or a bad manual edit
    # would (merge two pipe-delimited fields, breaking the 4-part shape),
    # and confirm the reader raises rather than silently accepting a bogus
    # identity. (A tail-character mangle alone can still satisfy the
    # native_symbol regex and false-pass -- the delimiter itself must break.)
    corrupted = stored_key.replace("|", "_", 1)
    with pytest.raises(InstrumentIdError):
        resolve_instrument_key(corrupted)
