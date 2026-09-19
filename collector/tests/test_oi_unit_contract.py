"""Open-interest unit contract.

``CanonicalOIEvent.open_interest`` used to be one untyped float that held
different physical quantities per venue (OKX contracts; Bybit documented only as
"size"), with a comment claiming the canonical unit was "contracts" while in
the same sentence calling Bybit's base-currency. Nothing stopped a cross-venue
comparison from silently comparing them.

These tests pin the contract: every OI event states its unit, UNKNOWN is the
safe default, the guard refuses cross-venue comparison it cannot prove, and the
unit is persisted with the data so stored streams cannot be joined blindly.
"""
from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest

from collector import run_bybit_collector, run_okx_collector
from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.adapters.okx import OKXAdapter
from collector.collector.canonical import (
    CanonicalOIEvent, OISource, OIUnit, OIUnitError, assert_comparable_oi, base_coin_oi,
)
from collector.collector.replay import FrameKind, ReplayEngine, ReplayFrame, ReplaySource

TS = 1_780_555_555_000


def _okx_oi(oi="12000", oi_ccy="120.5", oi_usd="7800000"):
    message = {"arg": {"channel": "open-interest", "instId": "BTC-USDT-SWAP"},
               "data": [{"instId": "BTC-USDT-SWAP", "ts": str(TS), "oi": oi,
                         "oiCcy": oi_ccy, "oiUsd": oi_usd}]}
    (event,) = OKXAdapter().normalize(message, local_receive_ts=TS)
    return event


def _bybit_oi(value="12345.6"):
    message = {"topic": "tickers.BTCUSDT", "type": "snapshot", "ts": TS,
               "data": {"symbol": "BTCUSDT", "markPrice": "65000.0", "openInterest": value}}
    events = BybitAdapter().normalize(message, local_receive_ts=TS)
    (event,) = [e for e in events if isinstance(e, CanonicalOIEvent)]
    return event


def _synthetic(exchange, unit, value=1.0):
    return CanonicalOIEvent(exchange, "openinterest", TS, None, TS, open_interest=value,
                            source=OISource.WS_PUSH, unit=unit)


# ---------------------------------------------------------------------------
# Every event states its unit; UNKNOWN is the safe default
# ---------------------------------------------------------------------------

def test_an_event_that_states_no_unit_is_unknown_never_contracts():
    bare = CanonicalOIEvent("SOMEVENUE", "openinterest", TS, None, TS, open_interest=1.0)
    assert bare.unit is OIUnit.UNKNOWN


def test_okx_open_interest_is_contracts_and_keeps_its_other_two_units():
    event = _okx_oi()
    assert event.unit is OIUnit.CONTRACTS
    assert (event.open_interest, event.oi_ccy, event.oi_usd) == (12000.0, 120.5, 7800000.0)


def test_bybit_open_interest_is_unknown_until_its_unit_is_documented():
    """Bybit's ticker field table says only "Open interest size (both sides)".
    The example arithmetic suggests base coin, but an example is not a unit
    statement, and the "both sides" convention is unresolved. If this test ever
    fails because someone set BASE_COIN, they must first cite documentation."""
    assert _bybit_oi().unit is OIUnit.UNKNOWN


# ---------------------------------------------------------------------------
# The guard
# ---------------------------------------------------------------------------

def test_okx_and_bybit_open_interest_are_refused():
    with pytest.raises(OIUnitError, match="different units"):
        assert_comparable_oi(_okx_oi(), _bybit_oi())


def test_same_venue_same_unit_is_comparable_even_when_the_unit_is_unknown():
    """An OI *change* within one venue's stream is one physical quantity by
    construction; blocking it would block the venue's own OI-regime features."""
    assert assert_comparable_oi(_bybit_oi("1"), _bybit_oi("2")) is OIUnit.UNKNOWN
    assert assert_comparable_oi(_okx_oi(), _okx_oi(oi="13000")) is OIUnit.CONTRACTS


def test_unknown_unit_is_refused_across_exchanges_even_if_both_are_unknown():
    with pytest.raises(OIUnitError, match="UNKNOWN"):
        assert_comparable_oi(_synthetic("BINANCE", OIUnit.UNKNOWN), _synthetic("BYBIT", OIUnit.UNKNOWN))


def test_two_exchanges_sharing_a_known_unit_pass():
    assert assert_comparable_oi(
        _synthetic("BYBIT", OIUnit.BASE_COIN), _synthetic("OKX", OIUnit.BASE_COIN)) is OIUnit.BASE_COIN


def test_mixed_known_units_are_refused_even_within_one_exchange():
    with pytest.raises(OIUnitError, match="different units"):
        assert_comparable_oi(_synthetic("OKX", OIUnit.CONTRACTS), _synthetic("OKX", OIUnit.BASE_COIN))


def test_the_error_names_the_exchanges_so_the_offending_pair_is_findable():
    with pytest.raises(OIUnitError) as excinfo:
        assert_comparable_oi(_okx_oi(), _bybit_oi())
    assert "BYBIT" in str(excinfo.value) and "OKX" in str(excinfo.value)


def test_a_single_event_is_a_caller_error_not_a_vacuous_pass():
    with pytest.raises(ValueError):
        assert_comparable_oi(_okx_oi())


# ---------------------------------------------------------------------------
# The only cross-venue-safe accessor
# ---------------------------------------------------------------------------

def test_base_coin_oi_uses_okx_documented_base_currency_figure_not_contracts():
    assert base_coin_oi(_okx_oi(oi="12000", oi_ccy="120.5")) == 120.5     # never 12000.0


def test_base_coin_oi_refuses_rather_than_guesses_for_bybit():
    assert base_coin_oi(_bybit_oi("12345.6")) is None


def test_base_coin_oi_passes_through_a_venue_whose_native_unit_is_base_coin():
    assert base_coin_oi(_synthetic("SOMEVENUE", OIUnit.BASE_COIN, 7.5)) == 7.5


# ---------------------------------------------------------------------------
# Persistence: the unit travels with the stored data
# ---------------------------------------------------------------------------

def _stored_oi_rows(base, stream):
    (segment,) = list((base / "raw" / stream).glob("*.seg"))
    return pq.read_table(segment).to_pylist()


def test_stored_okx_open_interest_carries_its_unit(tmp_path):
    app = run_okx_collector.OKXCollectorApp(data_dir=str(tmp_path))
    try:
        app._persist_event(_okx_oi())
    finally:
        app.oi_writer.close()
        for writer in (app.trades_writer, app.trades_all_writer, app.mark_writer, app.index_writer,
                       app.funding_writer, app.liq_writer, app.raw_wire_writer, app.quality_writer):
            writer.close()
    (row,) = _stored_oi_rows(tmp_path, "okx_openinterest")
    assert row["oi_unit"] == "CONTRACTS"
    assert (row["open_interest"], row["oi_ccy"]) == (12000.0, 120.5)


def test_stored_bybit_open_interest_carries_its_unit_as_unknown(tmp_path):
    app = run_bybit_collector.BybitCollectorApp(data_dir=str(tmp_path))
    try:
        app._persist_event(_bybit_oi("12345.6"))
    finally:
        for writer in (app.oi_writer, app.trades_writer, app.mark_writer, app.liq_writer,
                       app.ob_writer, app.raw_wire_writer, app.quality_writer):
            writer.close()
    (row,) = _stored_oi_rows(tmp_path, "bybit_openinterest")
    assert row["oi_unit"] == "UNKNOWN"
    assert row["open_interest"] == 12345.6


def test_two_venues_stored_oi_columns_are_distinguishable_by_unit_not_by_guesswork(tmp_path):
    """A reader that joins okx_openinterest to bybit_openinterest on
    open_interest can now see the units differ before it compares them."""
    okx = run_okx_collector.OKXCollectorApp(data_dir=str(tmp_path))
    bybit = run_bybit_collector.BybitCollectorApp(data_dir=str(tmp_path))
    try:
        okx._persist_event(_okx_oi())
        bybit._persist_event(_bybit_oi())
    finally:
        for app in (okx, bybit):
            for writer in vars(app).values():
                if hasattr(writer, "close") and hasattr(writer, "stream_dir"):
                    writer.close()
    okx_unit = _stored_oi_rows(tmp_path, "okx_openinterest")[0]["oi_unit"]
    bybit_unit = _stored_oi_rows(tmp_path, "bybit_openinterest")[0]["oi_unit"]
    assert okx_unit != bybit_unit
    assert "UNKNOWN" in {okx_unit, bybit_unit}


# ---------------------------------------------------------------------------
# Replay carries the unit unaltered (same adapter, same canonical event)
# ---------------------------------------------------------------------------

def test_replay_preserves_the_unit_and_the_digest_depends_on_it():
    frame = ReplayFrame(timestamp_ms=TS, kind=FrameKind.WIRE, source_index=0, payload=json.dumps(
        {"arg": {"channel": "open-interest", "instId": "BTC-USDT-SWAP"},
         "data": [{"instId": "BTC-USDT-SWAP", "ts": str(TS), "oi": "12000",
                   "oiCcy": "120.5", "oiUsd": "7800000"}]}))
    result = ReplayEngine(venue="OKX").run(ReplaySource([frame]))
    (event,) = result.non_book_events
    assert event.unit is OIUnit.CONTRACTS

    before = result.digest
    result.non_book_events[0] = CanonicalOIEvent(
        **{**event.__dict__, "unit": OIUnit.BASE_COIN})
    assert result.digest != before, "a changed unit must change the replay digest"
