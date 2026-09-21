"""Canonical instrument identity persistence: Binance USD-M writer wiring.

Phase B of the instrument-identity task (see test_instrument_key_persistence.py's
module docstring, which explicitly scoped Binance USD-M/Spot out of PR #36).
Binance Spot is untouched here -- that is Phase C, separate work.

Two different resolution strategies are exercised, matching two structurally
different paths through run_collector.py -- proven here, not just asserted:

* orderbook / trades / binance_orderbook_raw / binance_trades_raw: these flow
  through ``BinanceAdapter.normalize()``, so they already carry a per-event
  ``InstrumentId`` stamped by the generic ``ExchangeAdapter.__init_subclass__``
  mechanism (adapters/base.py) -- the same mechanism OKX's per-event channels
  use, and it already covers Binance without any change to that module. This
  file proves the *writers* actually thread that stamp through into the
  persisted row, which they did not before this phase.

* markprice / liquidation: these are the "legacy raw-dict handlers" the task
  called out specifically -- they bypass ``BinanceAdapter.normalize()``
  entirely (``run_collector.handle_message`` calls
  ``_handle_markprice``/``_handle_liquidation`` directly with the raw payload
  dict). There is no per-event identity to read here, so the validated
  ``BINANCE_USDM_BTCUSDT`` constant is used instead -- but only after the
  payload's own symbol field (when present) is checked against it, via
  ``CollectorApp._binance_usdm_instrument_key``. A contradiction is rejected,
  never silently stamped.

* openinterest: REST-polled; ``binance_oi.normalize_binance_oi`` resolves the
  identity generically from the response's own ``symbol`` field via
  ``instrument.resolve_instrument``, and raises on a symbol that contradicts
  what was requested.

Mutation-style verification (task section 11) is folded directly into the
contradiction tests below: each constructs the exact wrong input (a
mismatched native symbol, a foreign instrument_key) and asserts the real
code path actually rejects it, not just that the happy path looks right.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import time

import pyarrow.parquet as pq
import pytest

from collector import run_collector as rc
from collector.collector.binance_oi import BinanceOIParseError, normalize_binance_oi
from collector.collector.book_engine import LocalBook
from collector.collector.canonical import CanonicalOrderBookEvent
from collector.collector.instrument import (
    BINANCE_SPOT_BTCUSDT,
    BINANCE_USDM_BTCUSDT,
    BYBIT_LINEAR_BTCUSDT,
    InstrumentId,
    InstrumentIdError,
    resolve_canonical_instrument_key,
)

CollectorApp = rc.CollectorApp


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _app(tmp_path, monkeypatch):
    (tmp_path / "data").mkdir(exist_ok=True)
    monkeypatch.chdir(tmp_path)
    return CollectorApp()


def _bridge_book(app, update_id=10):
    """Bridge the local book with a snapshot, as live does at startup, so
    diffs are applied rather than buffered pending recovery."""
    from decimal import Decimal as _D
    snapshot = CanonicalOrderBookEvent(
        "BINANCE", "orderbook", None, None, 0,
        bids=tuple((_D("100.0") - _D("0.1") * i, _D("1.0")) for i in range(10)),
        asks=tuple((_D("101.0") + _D("0.1") * i, _D("1.0")) for i in range(10)),
        update_id=update_id, is_snapshot=True,
    )
    app.binance_book.snapshot(snapshot)
    app.binance_book.state.recovered()
    return app


def _depth_msg(now):
    return {"stream": "btcusdt@depth@100ms", "data": {
        "E": now, "U": 11, "u": 11, "pu": 10,
        "b": [[str(100.0 - i * 0.1), "1.0"] for i in range(10)],
        "a": [[str(101.0 + i * 0.1), "1.0"] for i in range(10)],
    }}


def _trade_msg(now, trade_id=123):
    return {"stream": "btcusdt@aggTrade", "data": {
        "E": now, "a": trade_id, "p": "100.5", "q": "1.0", "m": False}}


def _mark_msg(now, symbol="BTCUSDT"):
    data = {"E": now, "p": "100.5", "r": "0.0001", "T": now + 3_600_000}
    if symbol is not None:
        data["s"] = symbol
    return {"stream": "btcusdt@markPrice@1s", "data": data}


def _liq_msg(now, symbol="BTCUSDT"):
    o = {"S": "BUY", "p": "100.5", "q": "1.0", "T": now, "X": "FILLED", "f": "IOC"}
    if symbol is not None:
        o["s"] = symbol
    return {"stream": "btcusdt@forceOrder", "data": {"o": o}}


def _drive(app, msgs):
    async def _run():
        for msg in msgs:
            await app.handle_message(msg)
    asyncio.run(_run())


def _close_all(app):
    for name in ("ob_writer", "raw_book_writer", "trades_writer", "raw_trades_writer",
                 "mark_writer", "oi_writer", "liq_writer", "raw_wire_writer",
                 "raw_rest_writer", "quality_writer"):
        writer = getattr(app, name, None)
        if writer is not None:
            try:
                writer.close()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass


# ---------------------------------------------------------------------------
# Per-event resolution (adapter-routed): orderbook, trades, and their raw
# siblings
# ---------------------------------------------------------------------------


def test_orderbook_and_trades_and_raw_siblings_carry_the_resolved_key(tmp_path, monkeypatch):
    app = _bridge_book(_app(tmp_path, monkeypatch))
    now = int(time.time() * 1000)
    try:
        _drive(app, [_depth_msg(now), _trade_msg(now)])

        assert app.ob_writer.buffer, "orderbook writer got no rows"
        assert app.raw_book_writer.buffer, "binance_orderbook_raw writer got no rows"
        assert app.trades_writer.buffer, "trades writer got no rows"
        assert app.raw_trades_writer.buffer, "binance_trades_raw writer got no rows"

        for writer, label in [
            (app.ob_writer, "orderbook"), (app.raw_book_writer, "binance_orderbook_raw"),
            (app.trades_writer, "trades"), (app.raw_trades_writer, "binance_trades_raw"),
        ]:
            for row in writer.buffer:
                assert row["instrument_key"] == BINANCE_USDM_BTCUSDT.key, label
    finally:
        _close_all(app)


def test_instrument_key_round_trips_through_from_key(tmp_path, monkeypatch):
    app = _bridge_book(_app(tmp_path, monkeypatch))
    now = int(time.time() * 1000)
    try:
        _drive(app, [_trade_msg(now)])
        persisted = app.trades_writer.buffer[0]["instrument_key"]
        parsed = InstrumentId.from_key(persisted)
        assert parsed == BINANCE_USDM_BTCUSDT
        assert parsed.exchange == "BINANCE"
        assert parsed.market_type == "linear_perpetual"
        assert parsed.instrument == "BTC-USDT"
        assert parsed.native_symbol == "BTCUSDT"
    finally:
        _close_all(app)


def test_orderbook_parquet_round_trip_preserves_instrument_key(tmp_path, monkeypatch):
    app = _bridge_book(_app(tmp_path, monkeypatch))
    now = int(time.time() * 1000)
    try:
        _drive(app, [_depth_msg(now)])
    finally:
        _close_all(app)

    segments = list(app.ob_writer.stream_dir.glob("*.seg"))
    assert segments, "expected at least one committed orderbook segment"
    table = pq.read_table(segments[0])
    assert table.num_rows == 1
    assert table.column("instrument_key")[0].as_py() == BINANCE_USDM_BTCUSDT.key


# ---------------------------------------------------------------------------
# Legacy raw-dict handlers: markprice, liquidation -- validated constant,
# guarded by an explicit payload-symbol contradiction check
# ---------------------------------------------------------------------------


def test_markprice_and_liquidation_carry_the_validated_constant(tmp_path, monkeypatch):
    app = _bridge_book(_app(tmp_path, monkeypatch))
    now = int(time.time() * 1000)
    try:
        _drive(app, [_mark_msg(now), _liq_msg(now)])
        assert app.mark_writer.buffer, "markprice writer got no rows"
        assert app.liq_writer.buffer, "liquidation writer got no rows"
        assert app.mark_writer.buffer[0]["instrument_key"] == BINANCE_USDM_BTCUSDT.key
        assert app.liq_writer.buffer[0]["instrument_key"] == BINANCE_USDM_BTCUSDT.key
    finally:
        _close_all(app)


def test_markprice_and_liquidation_accept_a_payload_with_no_symbol_field(tmp_path, monkeypatch):
    """Absence of the payload's own symbol field is not a contradiction --
    only an explicit mismatch is. Older/partial payload shapes must not be
    rejected outright."""
    app = _bridge_book(_app(tmp_path, monkeypatch))
    now = int(time.time() * 1000)
    try:
        _drive(app, [_mark_msg(now, symbol=None), _liq_msg(now, symbol=None)])
        assert app.mark_writer.buffer[0]["instrument_key"] == BINANCE_USDM_BTCUSDT.key
        assert app.liq_writer.buffer[0]["instrument_key"] == BINANCE_USDM_BTCUSDT.key
    finally:
        _close_all(app)


@pytest.mark.parametrize("make_msg", [_mark_msg, _liq_msg])
def test_contradictory_payload_symbol_is_rejected_not_silently_stamped(tmp_path, monkeypatch, make_msg):
    """The task's explicit case: a different symbol must never silently
    become BTC-USDT. A markprice/liquidation payload whose own symbol field
    disagrees with the configured instrument must produce NO row at all,
    not a row wrongly labelled BINANCE_USDM_BTCUSDT."""
    app = _bridge_book(_app(tmp_path, monkeypatch))
    now = int(time.time() * 1000)
    stream = "markprice" if make_msg is _mark_msg else "liquidation"
    writer_attr = "mark_writer" if make_msg is _mark_msg else "liq_writer"
    try:
        _drive(app, [make_msg(now, symbol="ETHUSDT")])
        writer = getattr(app, writer_attr)
        assert not writer.buffer, f"{stream} must not have written a row for a contradicting symbol"
        assert app.stream_counters[stream]["rejected"] == 1
        assert app.validation_fail_reasons[stream] == {"symbol_contradicts_configured_instrument": 1}
    finally:
        _close_all(app)


# ---------------------------------------------------------------------------
# Open interest (REST poll): resolved generically from the response body
# ---------------------------------------------------------------------------


def test_openinterest_normalizer_resolves_the_registered_instrument():
    body = json.dumps({"symbol": "BTCUSDT", "openInterest": "1234.5", "time": 1_780_000_000_000})
    event = normalize_binance_oi(body, response_receive_ts=1_780_000_000_100, symbol="BTCUSDT")
    assert event.instrument == BINANCE_USDM_BTCUSDT


def test_openinterest_normalizer_does_not_fabricate_for_an_unregistered_symbol():
    body = json.dumps({"symbol": "ETHUSDT", "openInterest": "1234.5"})
    event = normalize_binance_oi(body, response_receive_ts=1, symbol="ETHUSDT")
    assert event.instrument is None


def test_openinterest_normalizer_rejects_a_response_symbol_that_contradicts_the_request():
    """The task's explicit case, applied to the REST path: the response
    body's own 'symbol' echo must never be silently trusted over what was
    actually requested."""
    body = json.dumps({"symbol": "ETHUSDT", "openInterest": "1234.5"})
    with pytest.raises(BinanceOIParseError):
        normalize_binance_oi(body, response_receive_ts=1, symbol="BTCUSDT")


def test_live_oi_poll_wires_instrument_key_from_the_shared_normalizer():
    """Structural guard, matching test_binance_oi_replayability.py's existing
    check that the live path calls the shared normalizer: the OI writer's
    row must read instrument_key off the normalizer's own event, not
    reimplement resolution inline."""
    source = inspect.getsource(rc.CollectorApp._poll_openinterest)
    assert "normalize_binance_oi" in source
    assert '"instrument_key"' in source
    assert "event.instrument" in source


# ---------------------------------------------------------------------------
# Cross-venue / cross-market-type collision resistance and storage-policy
# parity with the existing resolve_canonical_instrument_key contract
# ---------------------------------------------------------------------------


def test_binance_usdm_does_not_collide_with_spot_or_bybit_despite_shared_native_symbol():
    keys = {BINANCE_USDM_BTCUSDT.key, BINANCE_SPOT_BTCUSDT.key, BYBIT_LINEAR_BTCUSDT.key}
    assert len(keys) == 3
    assert InstrumentId.from_key(BINANCE_USDM_BTCUSDT.key) != InstrumentId.from_key(BINANCE_SPOT_BTCUSDT.key)


def test_binance_spot_identity_is_not_accepted_as_binance_usdm_identity():
    """The task's explicit third contradiction case: Spot's key must never
    resolve as valid inside a USD-M-scoped stream, even though both share
    exchange (BINANCE) and native_symbol (BTCUSDT) -- only market_type
    differs, and that alone must be enough to reject it."""
    row = {"instrument_key": BINANCE_SPOT_BTCUSDT.key}
    with pytest.raises(InstrumentIdError):
        resolve_canonical_instrument_key(row, expected=BINANCE_USDM_BTCUSDT)


@pytest.mark.parametrize("wrong", [BINANCE_SPOT_BTCUSDT, BYBIT_LINEAR_BTCUSDT])
def test_contradictory_instrument_key_in_a_usdm_row_is_rejected(wrong):
    row = {"instrument_key": wrong.key}
    with pytest.raises(InstrumentIdError):
        resolve_canonical_instrument_key(row, expected=BINANCE_USDM_BTCUSDT)


@pytest.mark.parametrize("malformed", [
    "not-a-key-at-all",
    "BINANCE|linear_perpetual|BTC-USDT",                # truncated: 3 parts
    "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT|extra",  # extra delimiter: 5 parts
    "binance|linear_perpetual|BTC-USDT|BTCUSDT",         # wrong case
    "",
])
def test_malformed_instrument_key_raises_never_resolves_to_none(malformed):
    row = {"instrument_key": malformed}
    with pytest.raises(InstrumentIdError):
        resolve_canonical_instrument_key(row, expected=BINANCE_USDM_BTCUSDT)


def test_legacy_row_missing_the_column_resolves_to_none():
    """Storage read-side parity with Bybit/OKX (PR #36): a row written
    before this migration has no column at all, and that must resolve the
    same way as an explicit null -- never as corruption."""
    row = {"price": 100.0}
    assert resolve_canonical_instrument_key(row, expected=BINANCE_USDM_BTCUSDT) is None


def test_explicit_null_resolves_to_none():
    row = {"price": 100.0, "instrument_key": None}
    assert resolve_canonical_instrument_key(row, expected=BINANCE_USDM_BTCUSDT) is None


def test_valid_matching_key_resolves_correctly():
    row = {"instrument_key": BINANCE_USDM_BTCUSDT.key}
    assert resolve_canonical_instrument_key(row, expected=BINANCE_USDM_BTCUSDT) == BINANCE_USDM_BTCUSDT


# ---------------------------------------------------------------------------
# Adapter-level contradiction guard (mutation-style): proves the generic
# ExchangeAdapter._stamp_instrument mechanism actually protects Binance, not
# just OKX/Bybit
# ---------------------------------------------------------------------------


def test_adapter_stamp_instrument_raises_on_exchange_market_type_mismatch():
    """Mirrors adapters/base.py's own guard: constructing a CanonicalEvent
    whose (exchange, market_type) disagrees with the adapter's bound
    instrument must raise, not silently relabel the event as Binance
    USD-M. Exercised directly against the real BinanceAdapter instance
    the collector uses, via its private stamping hook, rather than only
    trusting the docstring."""
    from collector.collector.adapters.binance import BinanceAdapter
    from collector.collector.canonical import CanonicalTradeEvent

    adapter = BinanceAdapter()
    wrong_market_type_event = CanonicalTradeEvent(
        "BINANCE", "trades", 1, None, 1, market_type="spot",
        trade_id="1", price=1.0, quantity=1.0, side="BUY")
    with pytest.raises(InstrumentIdError):
        adapter._stamp_instrument([wrong_market_type_event])
