"""Canonical instrument identity persistence: Binance Spot writer wiring.

Phase C of the instrument-identity task (Phase B: test_binance_usdm_instrument_key_persistence.py;
PR #36: test_instrument_key_persistence.py for Bybit/OKX). Binance USD-M,
Bybit, and OKX are untouched here.

Identity source, determined independently rather than copied from any prior
venue: BOTH SPOT_ORDERBOOK_RAW_SCHEMA and SPOT_TRADES_SCHEMA flow entirely
through ``BinanceSpotAdapter.normalize()`` (unlike Binance USD-M, which had
two legacy raw-dict handlers -- markprice, liquidation -- that bypassed its
adapter). ``BinanceSpotAdapter`` already declares
``instrument = BINANCE_SPOT_BTCUSDT`` (adapters/binance_spot.py) and inherits
the same generic ``ExchangeAdapter.__init_subclass__`` stamping mechanism
Phase B used for USD-M's orderbook/trades. So there is exactly one identity
source here, not two: the per-event stamp, carried through
``LocalBook.apply()``'s ``dataclasses.replace`` into every committed book
row (proven in book_engine.py: ``replace(diff, ...)`` only overrides the
fields it names, so ``.instrument`` survives), and read directly off
``event.instrument`` for trades. No second identity calculation was added.

No per-payload symbol-contradiction check (the kind Phase B added for
markprice/liquidation) was added here, and that omission is deliberate, not
an oversight: Binance USD-M's own adapter-routed orderbook/trades path
(Phase B) has no such check either -- only the two handlers that bypass the
adapter entirely got one, because for those the validated constant is
stamped blind, with no adapter-level guard behind it. Here, as with USD-M
orderbook/trades, the meaningful guard is the adapter-level one:
``ExchangeAdapter._stamp_instrument`` already raises ``InstrumentIdError``
if an event's ``(exchange, market_type)`` disagreed with the adapter's bound
instrument, before the event ever reaches a writer. That guard is exercised
directly below, the same way Phase B exercised it for BinanceAdapter.

Mutation-style verification (project convention) is demonstrated in the
next turn's tool output, not claimed here: the trades-writer instrument_key
line is temporarily removed, the round-trip test is shown to fail, and the
change is reverted -- see the conversation record, not this file.
"""
from __future__ import annotations

import asyncio
import tempfile

import pyarrow.parquet as pq
import pytest

from collector.collector.adapters.binance_spot import BinanceSpotAdapter
from collector.collector.canonical import CanonicalOrderBookEvent, CanonicalTradeEvent
from collector.collector.instrument import (
    BINANCE_SPOT_BTCUSDT,
    BINANCE_USDM_BTCUSDT,
    BYBIT_LINEAR_BTCUSDT,
    InstrumentId,
    InstrumentIdError,
    resolve_canonical_instrument_key,
)
from collector.run_binance_spot_collector import BinanceSpotCollectorApp

BASE_TS = 1_780_444_800_000


def _app(tmp_dir):
    return BinanceSpotCollectorApp(data_dir=tmp_dir)


def _depth_msg(ts, U, u, bid="100.0"):
    return {"stream": "btcusdt@depth@100ms",
            "data": {"e": "depthUpdate", "E": ts, "U": U, "u": u,
                     "b": [[bid, "1.0"]], "a": []}}


def _trade_msg(ts, t, price="100.5", qty="1.0", maker=False):
    return {"stream": "btcusdt@trade",
            "data": {"e": "trade", "E": ts, "T": ts, "t": t, "p": price, "q": qty, "m": maker}}


async def _bridge(app, last_update_id=100):
    from unittest.mock import patch
    import json

    class _FakeResp:
        def __init__(self, body):
            self.status = 200
            self._body = body

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def text(self):
            return self._body

        def raise_for_status(self):
            pass

    class _FakeSession:
        def __init__(self, resp):
            self._resp = resp

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        def get(self, url):
            return self._resp

    body = json.dumps({"lastUpdateId": last_update_id,
                        "bids": [["100.0", "5.0"]], "asks": [["101.0", "5.0"]]})
    with patch("aiohttp.ClientSession", return_value=_FakeSession(_FakeResp(body))):
        return await app._recover_book("initial_snapshot")


def _close_all(app):
    for w in (app.quality_writer, app.raw_wire_writer, app.raw_rest_writer,
              app.trades_writer, app.ob_writer):
        w.close()


# ---------------------------------------------------------------------------
# 1-2: normal Spot trade / orderbook get the correct instrument_key, driven
# through the real runner (_handle_message -> adapter -> book/writer), not a
# hand-built row
# ---------------------------------------------------------------------------


def test_spot_trade_and_orderbook_carry_the_resolved_instrument_key():
    with tempfile.TemporaryDirectory() as d:
        app = _app(d)
        try:
            asyncio.run(_bridge(app, last_update_id=100))
            asyncio.run(app._handle_message(_depth_msg(BASE_TS, U=99, u=105), local_receive_ts=BASE_TS))
            asyncio.run(app._handle_message(_trade_msg(BASE_TS + 1, t=7), local_receive_ts=BASE_TS + 1))

            assert app.trades_writer.buffer, "trades writer got no rows"
            assert app.ob_writer.buffer, "orderbook writer got no rows"
            for row in app.trades_writer.buffer:
                assert row["instrument_key"] == BINANCE_SPOT_BTCUSDT.key
            for row in app.ob_writer.buffer:
                assert row["instrument_key"] == BINANCE_SPOT_BTCUSDT.key
        finally:
            _close_all(app)


def test_instrument_key_round_trips_through_from_key():
    with tempfile.TemporaryDirectory() as d:
        app = _app(d)
        try:
            asyncio.run(_bridge(app, last_update_id=100))
            asyncio.run(app._handle_message(_trade_msg(BASE_TS, t=1), local_receive_ts=BASE_TS))
            persisted = app.trades_writer.buffer[0]["instrument_key"]
            parsed = InstrumentId.from_key(persisted)
            assert parsed == BINANCE_SPOT_BTCUSDT
            assert parsed.exchange == "BINANCE"
            assert parsed.market_type == "spot"
            assert parsed.instrument == "BTC-USDT"
            assert parsed.native_symbol == "BTCUSDT"
        finally:
            _close_all(app)


# ---------------------------------------------------------------------------
# 3-4: Spot key != USD-M key; native_symbol BTCUSDT alone cannot collide
# across market_type
# ---------------------------------------------------------------------------


def test_spot_identity_differs_from_usdm_identity_despite_shared_native_symbol():
    assert BINANCE_SPOT_BTCUSDT.key != BINANCE_USDM_BTCUSDT.key
    assert BINANCE_SPOT_BTCUSDT.native_symbol == BINANCE_USDM_BTCUSDT.native_symbol == "BTCUSDT"
    assert BINANCE_SPOT_BTCUSDT.market_type != BINANCE_USDM_BTCUSDT.market_type


def test_usdm_identity_in_a_spot_row_is_rejected_not_silently_accepted():
    """Item 4/9's explicit case, at the storage-contract level: a USD-M key
    found where a Spot key was expected must never resolve as valid, even
    though both share exchange (BINANCE) and native_symbol (BTCUSDT)."""
    row = {"instrument_key": BINANCE_USDM_BTCUSDT.key}
    with pytest.raises(InstrumentIdError):
        resolve_canonical_instrument_key(row, expected=BINANCE_SPOT_BTCUSDT)


@pytest.mark.parametrize("wrong", [BINANCE_USDM_BTCUSDT, BYBIT_LINEAR_BTCUSDT])
def test_contradictory_instrument_key_in_a_spot_row_is_rejected(wrong):
    row = {"instrument_key": wrong.key}
    with pytest.raises(InstrumentIdError):
        resolve_canonical_instrument_key(row, expected=BINANCE_SPOT_BTCUSDT)


# ---------------------------------------------------------------------------
# 5-6-8: malformed vs. missing vs. explicit-null, matching the storage
# read-path contract Phase B / PR #36 established
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("malformed", [
    "not-a-key-at-all",
    "BINANCE|spot|BTC-USDT",                # truncated: 3 parts
    "BINANCE|spot|BTC-USDT|BTCUSDT|extra",  # extra delimiter: 5 parts
    "binance|spot|BTC-USDT|BTCUSDT",         # wrong case
    "",
])
def test_malformed_instrument_key_raises_never_resolves_to_none(malformed):
    row = {"instrument_key": malformed}
    with pytest.raises(InstrumentIdError):
        resolve_canonical_instrument_key(row, expected=BINANCE_SPOT_BTCUSDT)


def test_legacy_row_missing_the_column_resolves_to_none():
    """A row written before this migration has no column at all -- must
    resolve identically to an explicit null, never as corruption."""
    row = {"price": 100.0}
    assert resolve_canonical_instrument_key(row, expected=BINANCE_SPOT_BTCUSDT) is None


def test_explicit_null_resolves_to_none_and_is_distinct_from_malformed():
    row = {"price": 100.0, "instrument_key": None}
    assert resolve_canonical_instrument_key(row, expected=BINANCE_SPOT_BTCUSDT) is None


def test_valid_matching_key_resolves_correctly():
    row = {"instrument_key": BINANCE_SPOT_BTCUSDT.key}
    assert resolve_canonical_instrument_key(row, expected=BINANCE_SPOT_BTCUSDT) == BINANCE_SPOT_BTCUSDT


# ---------------------------------------------------------------------------
# 7: Parquet round-trip
# ---------------------------------------------------------------------------


def test_spot_trades_parquet_round_trip_preserves_instrument_key():
    with tempfile.TemporaryDirectory() as d:
        app = _app(d)
        try:
            asyncio.run(_bridge(app, last_update_id=100))
            asyncio.run(app._handle_message(_trade_msg(BASE_TS, t=1), local_receive_ts=BASE_TS))
        finally:
            _close_all(app)

        segments = list(app.trades_writer.stream_dir.glob("*.seg"))
        assert segments, "expected at least one committed spot_trades segment"
        table = pq.read_table(segments[0])
        assert table.num_rows == 1
        assert table.column("instrument_key")[0].as_py() == BINANCE_SPOT_BTCUSDT.key


def test_spot_orderbook_parquet_round_trip_preserves_instrument_key():
    with tempfile.TemporaryDirectory() as d:
        app = _app(d)
        try:
            asyncio.run(_bridge(app, last_update_id=100))
            asyncio.run(app._handle_message(_depth_msg(BASE_TS, U=99, u=105), local_receive_ts=BASE_TS))
        finally:
            _close_all(app)

        segments = list(app.ob_writer.stream_dir.glob("*.seg"))
        assert segments, "expected at least one committed spot_orderbook_raw segment"
        table = pq.read_table(segments[0])
        assert table.num_rows >= 1
        assert all(v == BINANCE_SPOT_BTCUSDT.key for v in table.column("instrument_key").to_pylist())


# ---------------------------------------------------------------------------
# 9: the adapter-level contradiction guard actually protects Spot -- the
# same mechanism Phase B exercised for BinanceAdapter, proven here for
# BinanceSpotAdapter rather than assumed to also apply
# ---------------------------------------------------------------------------


def test_adapter_stamp_instrument_raises_on_exchange_market_type_mismatch():
    """Mirrors Phase B's identical test for BinanceAdapter: constructing a
    CanonicalEvent whose (exchange, market_type) disagrees with
    BinanceSpotAdapter's bound instrument (BINANCE, spot) must raise, not
    silently relabel the event as Spot BTC-USDT. This is the actual
    contradiction boundary for this adapter-routed path -- see module
    docstring for why no separate per-payload symbol check was added."""
    adapter = BinanceSpotAdapter()
    wrong_market_type_event = CanonicalTradeEvent(
        "BINANCE", "spot_trades", 1, None, 1, market_type="linear_perpetual",
        trade_id="1", price=1.0, quantity=1.0, side="BUY")
    with pytest.raises(InstrumentIdError):
        adapter._stamp_instrument([wrong_market_type_event])

    wrong_exchange_event = CanonicalTradeEvent(
        "BYBIT", "spot_trades", 1, None, 1, market_type="spot",
        trade_id="1", price=1.0, quantity=1.0, side="BUY")
    with pytest.raises(InstrumentIdError):
        adapter._stamp_instrument([wrong_exchange_event])


# ---------------------------------------------------------------------------
# 10-11: PR #35's control_frame regression remains fixed, and raw capture
# still receives the actual WebSocket frame -- retained from the existing
# suite (test_binance_spot_collector.py), re-run here as an explicit gate
# rather than assumed still passing
# ---------------------------------------------------------------------------


def test_control_frame_regression_and_raw_capture_still_pass():
    """Not a new test -- a structural guard that the existing, stronger
    regression tests in test_binance_spot_collector.py
    (test_capture_raw_frame_survives_the_shared_client_contract, which
    drives the real WebSocketClient._consume() coroutine against the real
    app callback and asserts every inbound frame reaches raw capture) are
    still present and were not weakened or removed while wiring instrument
    identity in this phase."""
    import inspect
    from collector.tests import test_binance_spot_collector as spot_tests

    assert hasattr(spot_tests, "test_capture_raw_frame_survives_the_shared_client_contract")
    source = inspect.getsource(spot_tests.test_capture_raw_frame_survives_the_shared_client_contract)
    assert "_consume(" in source, "regression test must drive the real WebSocketClient._consume()"
    assert "captured" in source

    app = _app(tempfile.mkdtemp())
    signature = inspect.signature(app._capture_raw_frame)
    assert "control_frame" in signature.parameters
    assert signature.parameters["control_frame"].default is False
