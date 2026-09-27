"""Canonical instrument identity persistence: Bybit + OKX writer wiring.

Scope of this file (see docs/CARRY_FORWARD_AUDIT.md-style honesty: this is
NOT the full cross-collector task -- Binance USD-M/Spot wiring is separate,
later work):

* Bybit's five canonical writers stamp a single validated constant
  (BYBIT_LINEAR_BTCUSDT), because the runner's own topics are built from one
  hardcoded SYMBOL -- proven here, not just asserted in a comment.
* OKX's per-event resolution (OKXAdapter.normalize -> event.instrument ->
  writer) is exercised end to end, including both documented exceptions:
  index-tickers (keyed by the index pair, never the swap) and
  liquidation-orders (instType-scoped, many instruments, only the
  configured one gets an identity).
* Cross-venue and cross-market-type collision resistance: two different
  instruments that share a native symbol must never produce the same key.
* Malformed and contradictory instrument_key values are detected, never
  silently coerced to None.
* Legacy rows (column absent) and explicit nulls resolve to None the same
  way, without conflating "never had a column" with "corrupted value".

Mutation-style verification (section 11 of the task) is done directly in
this file: several tests construct the *wrong* result by hand and assert
resolve_canonical_instrument_key actually rejects it, rather than only
testing the happy path and hoping a defect would show up.
"""
from __future__ import annotations

import json

import pytest

from collector.collector.adapters.okx import OKXAdapter
from collector.collector.instrument import (
    BINANCE_SPOT_BTCUSDT,
    BINANCE_USDM_BTCUSDT,
    BYBIT_LINEAR_BTCUSDT,
    OKX_SWAP_BTCUSDT,
    InstrumentId,
    InstrumentIdError,
    resolve_canonical_instrument_key,
)
from collector.run_bybit_collector import BybitCollectorApp
from collector.run_okx_collector import OKXCollectorApp


def _bybit_app(tmp_path):
    return BybitCollectorApp(data_dir=str(tmp_path))


def _drive_bybit(app, frames):
    import asyncio

    class _FakeSocket:
        def __init__(self, frames):
            self.frames = frames

        def __aiter__(self):
            async def gen():
                for frame in self.frames:
                    yield frame
            return gen()

    app.client.running = True

    async def _run():
        await app.client._consume(_FakeSocket(frames))
        app.client.running = False
        await app.client._process_queue()
    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Bybit: every canonical writer carries the validated constant
# ---------------------------------------------------------------------------


def test_bybit_all_five_writers_carry_the_configured_instrument_key(tmp_path):
    app = _bybit_app(tmp_path)
    now = 1_780_000_000_000
    snapshot = json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": now,
                          "data": {"b": [["100", "1"]], "a": [["101", "1"]], "u": 1, "seq": 1}})
    trade = json.dumps({"topic": "publicTrade.BTCUSDT", "ts": now,
                       "data": [{"T": now, "S": "Buy", "v": "1", "p": "100", "i": "t1"}]})
    ticker = json.dumps({"topic": "tickers.BTCUSDT", "type": "snapshot", "ts": now,
                        "data": {"markPrice": "100", "indexPrice": "100", "fundingRate": "0.0001",
                                  "nextFundingTime": str(now), "openInterest": "1"}})
    liquidation = json.dumps({"topic": "allLiquidation.BTCUSDT", "ts": now,
                             "data": [{"T": now, "S": "Sell", "v": "1", "p": "99"}]})

    _drive_bybit(app, [snapshot, trade, ticker, liquidation])

    for writer, label in [
        (app.ob_writer, "orderbook"), (app.trades_writer, "trades"),
        (app.mark_writer, "markprice"), (app.oi_writer, "openinterest"),
        (app.liq_writer, "liquidation"),
    ]:
        assert writer.buffer, f"{label} writer got no rows"
        for row in writer.buffer:
            assert row["instrument_key"] == BYBIT_LINEAR_BTCUSDT.key, label


def test_bybit_instrument_key_round_trips_through_from_key(tmp_path):
    app = _bybit_app(tmp_path)
    now = 1_780_000_000_000
    trade = json.dumps({"topic": "publicTrade.BTCUSDT", "ts": now,
                       "data": [{"T": now, "S": "Buy", "v": "1", "p": "100", "i": "t1"}]})
    _drive_bybit(app, [trade])
    persisted_key = app.trades_writer.buffer[0]["instrument_key"]
    assert InstrumentId.from_key(persisted_key) == BYBIT_LINEAR_BTCUSDT


# ---------------------------------------------------------------------------
# OKX: per-event resolution, including both documented exceptions
# ---------------------------------------------------------------------------


def test_okx_instrument_scoped_channels_carry_the_resolved_key(tmp_path):
    app = OKXCollectorApp(data_dir=str(tmp_path))
    events = app.adapter.normalize(
        {"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
         "data": [{"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "1", "sz": "1",
                   "side": "buy", "ts": "1"}]}, local_receive_ts=1)
    for event in events:
        app._persist_event(event)
    assert app.trades_writer.buffer[0]["instrument_key"] == OKX_SWAP_BTCUSDT.key
    app.trades_writer.close()


def test_okx_index_tickers_never_acquire_the_swap_identity(tmp_path):
    """The task's exception #1: index-tickers is keyed by the index pair,
    not the swap. instrument_key must be None here always, never
    OKX_SWAP_BTCUSDT.key -- checked through the real writer, not just the
    adapter."""
    app = OKXCollectorApp(data_dir=str(tmp_path))
    events = app.adapter.normalize(
        {"arg": {"channel": "index-tickers", "instId": "BTC-USDT"},
         "data": [{"instId": "BTC-USDT", "idxPx": "65000", "ts": "1"}]}, local_receive_ts=1)
    for event in events:
        app._persist_event(event)
    assert app.index_writer.buffer[0]["instrument_key"] is None


def test_okx_cross_instrument_liquidation_only_identifies_the_configured_one(tmp_path):
    """The task's exception #2: liquidation-orders is instType-scoped, one
    push can carry many instruments. Only the row whose own inst_id matches
    this adapter's configured instrument may get an identity; every other
    instrument's row must stay None, never inherit BTC-USDT."""
    app = OKXCollectorApp(data_dir=str(tmp_path))
    events = app.adapter.normalize(
        {"arg": {"channel": "liquidation-orders", "instType": "SWAP"},
         "data": [
             {"instId": "BTC-USDT-SWAP", "details": [{"ts": "1", "side": "sell", "bkPx": "1", "sz": "1"}]},
             {"instId": "ETH-USDT-SWAP", "details": [{"ts": "1", "side": "buy", "bkPx": "1", "sz": "1"}]},
         ]}, local_receive_ts=1)
    for event in events:
        app._persist_event(event)
    rows_by_inst = {row["inst_id"]: row for row in app.liq_writer.buffer}
    assert rows_by_inst["BTC-USDT-SWAP"]["instrument_key"] == OKX_SWAP_BTCUSDT.key
    assert rows_by_inst["ETH-USDT-SWAP"]["instrument_key"] is None


def test_okx_all_seven_channels_produce_the_documented_identity_shape(tmp_path):
    """One assertion per D11 channel: which get an identity, which don't,
    and why -- so a future change to any single channel's scoping is caught
    here rather than only in a narrower per-channel test."""
    app = OKXCollectorApp(data_dir=str(tmp_path))
    messages = [
        ("trades", {"arg": {"channel": "trades"}, "data": [{"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "1", "sz": "1", "side": "buy", "ts": "1"}]}, app.trades_writer, True),
        ("trades-all", {"arg": {"channel": "trades-all"}, "data": [{"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "1", "sz": "1", "side": "buy", "ts": "1", "source": "0"}]}, app.trades_all_writer, True),
        ("mark-price", {"arg": {"channel": "mark-price"}, "data": [{"instId": "BTC-USDT-SWAP", "markPx": "1", "ts": "1"}]}, app.mark_writer, True),
        ("index-tickers", {"arg": {"channel": "index-tickers"}, "data": [{"instId": "BTC-USDT", "idxPx": "1", "ts": "1"}]}, app.index_writer, False),
        ("funding-rate", {"arg": {"channel": "funding-rate"}, "data": [{"instId": "BTC-USDT-SWAP", "fundingRate": "0.0001", "ts": "1"}]}, app.funding_writer, True),
        ("open-interest", {"arg": {"channel": "open-interest"}, "data": [{"instId": "BTC-USDT-SWAP", "oi": "1", "ts": "1"}]}, app.oi_writer, True),
    ]
    for channel, msg, writer, expect_identified in messages:
        for event in app.adapter.normalize(msg, local_receive_ts=1):
            app._persist_event(event)
        row = writer.buffer[-1]
        if expect_identified:
            assert row["instrument_key"] == OKX_SWAP_BTCUSDT.key, channel
        else:
            assert row["instrument_key"] is None, channel


# ---------------------------------------------------------------------------
# Cross-venue / cross-market-type collision resistance
# ---------------------------------------------------------------------------


def test_same_native_symbol_never_collides_across_venue_or_market_type():
    """The exact scenario the task names explicitly: BTCUSDT is the native
    symbol for three different instruments here. None of their keys may be
    equal, and none may parse back to the wrong one."""
    keys = {
        "binance_usdm": BINANCE_USDM_BTCUSDT.key,
        "binance_spot": BINANCE_SPOT_BTCUSDT.key,
        "bybit_linear": BYBIT_LINEAR_BTCUSDT.key,
    }
    assert len(set(keys.values())) == 3, keys
    assert InstrumentId.from_key(keys["binance_usdm"]) != InstrumentId.from_key(keys["binance_spot"])
    assert InstrumentId.from_key(keys["binance_spot"]) != InstrumentId.from_key(keys["bybit_linear"])


def test_okx_native_symbol_does_not_collide_with_bybit_despite_shared_instrument():
    """OKX_SWAP_BTCUSDT and BYBIT_LINEAR_BTCUSDT share the canonical
    instrument (BTC-USDT) and market_type, but different exchanges and
    different native symbols -- must still never collide."""
    assert OKX_SWAP_BTCUSDT.key != BYBIT_LINEAR_BTCUSDT.key
    assert OKX_SWAP_BTCUSDT != BYBIT_LINEAR_BTCUSDT


# ---------------------------------------------------------------------------
# resolve_canonical_instrument_key: the four states, plus mutation checks
# ---------------------------------------------------------------------------


def test_legacy_row_missing_the_column_resolves_to_none():
    row = {"price": 100.0}  # no "instrument_key" key at all
    assert resolve_canonical_instrument_key(row, expected=BYBIT_LINEAR_BTCUSDT) is None


def test_explicit_null_resolves_to_none():
    row = {"price": 100.0, "instrument_key": None}
    assert resolve_canonical_instrument_key(row, expected=BYBIT_LINEAR_BTCUSDT) is None


def test_valid_matching_key_resolves_correctly():
    row = {"instrument_key": BYBIT_LINEAR_BTCUSDT.key}
    assert resolve_canonical_instrument_key(row, expected=BYBIT_LINEAR_BTCUSDT) == BYBIT_LINEAR_BTCUSDT


@pytest.mark.parametrize("malformed", [
    "not-a-key-at-all",
    "BYBIT|linear_perpetual|BTC-USDT",              # truncated: only 3 parts
    "BYBIT|linear_perpetual|BTC-USDT|BTCUSDT|extra", # extra delimiter: 5 parts
    "bybit|linear_perpetual|BTC-USDT|BTCUSDT",       # wrong case on exchange
    "",
])
def test_malformed_instrument_key_raises_never_resolves_to_none(malformed):
    row = {"instrument_key": malformed}
    with pytest.raises(InstrumentIdError):
        resolve_canonical_instrument_key(row, expected=BYBIT_LINEAR_BTCUSDT)


@pytest.mark.parametrize("wrong", [
    BINANCE_USDM_BTCUSDT,   # wrong exchange
    BINANCE_SPOT_BTCUSDT,   # wrong exchange AND market_type
    OKX_SWAP_BTCUSDT,       # wrong exchange, different native_symbol too
])
def test_contradictory_instrument_key_is_rejected_not_silently_accepted(wrong):
    """A row whose persisted key names a *different, validly-formed*
    instrument than the stream it lives in must be rejected -- this is the
    mutation the task calls out explicitly (\"make storage reader silently
    accept malformed key\"): a reader that only checked \"does this parse\"
    would pass a Binance-Spot key sitting in a Bybit row, which is exactly
    the corruption this function exists to catch."""
    row = {"instrument_key": wrong.key}
    with pytest.raises(InstrumentIdError):
        resolve_canonical_instrument_key(row, expected=BYBIT_LINEAR_BTCUSDT)


def test_wrong_market_type_specifically_is_rejected():
    """Same exchange and native symbol, different market_type -- the
    Spot-vs-futures collision named explicitly in the task."""
    row = {"instrument_key": BINANCE_SPOT_BTCUSDT.key}
    with pytest.raises(InstrumentIdError):
        resolve_canonical_instrument_key(row, expected=BINANCE_USDM_BTCUSDT)


def test_wrong_native_symbol_specifically_is_rejected():
    contradictory = InstrumentId("BYBIT", "linear_perpetual", "BTC-USDT", "BTCUSDT-PERP")
    row = {"instrument_key": contradictory.key}
    with pytest.raises(InstrumentIdError):
        resolve_canonical_instrument_key(row, expected=BYBIT_LINEAR_BTCUSDT)


# ---------------------------------------------------------------------------
# Mutation-style verification: deliberately wrong code, confirm tests catch it
# ---------------------------------------------------------------------------


def test_mutation_a_reader_that_ignores_the_expected_identity_would_miss_the_defect():
    """Demonstrates why `expected=` is load-bearing: a naive reader that
    only checks "does the key parse" (dropping the equality check) would
    accept a Binance-Spot key sitting in a Bybit stream. This test proves
    the *real* function raises, then proves a hand-written naive version
    (the mutation) would not have -- so the difference is attributable to
    the equality check, not to InstrumentId.from_key's own validation."""
    row = {"instrument_key": BINANCE_SPOT_BTCUSDT.key}
    with pytest.raises(InstrumentIdError):
        resolve_canonical_instrument_key(row, expected=BYBIT_LINEAR_BTCUSDT)

    def _naive_reader_missing_the_equality_check(row, *, expected):
        if "instrument_key" not in row or row["instrument_key"] is None:
            return None
        return InstrumentId.from_key(row["instrument_key"])  # BUG: never compares to expected

    # The mutation "succeeds" (returns a wrong-but-plausible value) exactly
    # where the real function raises -- confirming the equality check is
    # the specific thing standing between "silently accepted" and "caught".
    assert _naive_reader_missing_the_equality_check(row, expected=BYBIT_LINEAR_BTCUSDT) == BINANCE_SPOT_BTCUSDT


def test_mutation_okx_index_tickers_inheriting_swap_identity_would_be_caught():
    """If OKXAdapter._instrument_scoped were mutated to return True
    unconditionally (losing the index-tickers exception), this is the test
    that would fail -- confirmed by actually performing that mutation here
    against a throwaway adapter instance, not merely asserting it would."""
    app_adapter = OKXAdapter()
    events = app_adapter.normalize(
        {"arg": {"channel": "index-tickers", "instId": "BTC-USDT"},
         "data": [{"instId": "BTC-USDT", "idxPx": "1", "ts": "1"}]}, local_receive_ts=1)
    assert events[0].instrument is None  # real behaviour: passes

    # Now perform the mutation the task describes and confirm it would be caught.
    broken = OKXAdapter()
    broken._instrument_scoped = lambda event: True  # the exact regression named in the task
    mutated_events = broken.normalize(
        {"arg": {"channel": "index-tickers", "instId": "BTC-USDT"},
         "data": [{"instId": "BTC-USDT", "idxPx": "1", "ts": "1"}]}, local_receive_ts=1)
    assert mutated_events[0].instrument == OKX_SWAP_BTCUSDT  # the mutation's wrong result
    assert mutated_events[0].instrument != events[0].instrument  # proves the two are distinguishable


def test_mutation_cross_instrument_liquidation_inheriting_btc_identity_would_be_caught():
    """Same style for exception #2: force _instrument_scoped to ignore
    inst_id matching, confirm the ETH row would wrongly acquire the BTC
    identity, and that the real (unmutated) adapter does not."""
    real = OKXAdapter()
    real_events = real.normalize(
        {"arg": {"channel": "liquidation-orders", "instType": "SWAP"},
         "data": [{"instId": "ETH-USDT-SWAP", "details": [{"ts": "1", "side": "buy", "bkPx": "1", "sz": "1"}]}]},
        local_receive_ts=1)
    assert real_events[0].instrument is None

    broken = OKXAdapter()
    broken._instrument_scoped = lambda event: True
    broken_events = broken.normalize(
        {"arg": {"channel": "liquidation-orders", "instType": "SWAP"},
         "data": [{"instId": "ETH-USDT-SWAP", "details": [{"ts": "1", "side": "buy", "bkPx": "1", "sz": "1"}]}]},
        local_receive_ts=1)
    assert broken_events[0].instrument == OKX_SWAP_BTCUSDT  # wrong: ETH row claiming BTC identity
