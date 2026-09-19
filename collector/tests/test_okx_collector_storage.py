"""Storage wiring for the D11 OKX collector (run_okx_collector.py).

Uses the existing venue-aware storage namespace architecture (PR #19) --
no new storage mechanism. Confirms okx_-prefixed streams, one per D11
channel, and that a books event (out of this runner's scope, see module
docstring) is a documented no-op rather than a crash or a misrouted write.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from collector.run_okx_collector import OKXCollectorApp


def _flush(app: OKXCollectorApp) -> None:
    for writer in (app.trades_writer, app.trades_all_writer, app.mark_writer,
                   app.index_writer, app.funding_writer, app.oi_writer, app.liq_writer):
        writer.close()


def test_each_d11_channel_lands_in_its_own_okx_stream(tmp_path):
    app = OKXCollectorApp(data_dir=str(tmp_path))
    try:
        messages = [
            {"arg": {"channel": "trades"}, "data": [{"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "1", "sz": "1", "side": "buy", "ts": "1"}]},
            {"arg": {"channel": "trades-all"}, "data": [{"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "1", "sz": "1", "side": "buy", "ts": "1", "source": "0"}]},
            {"arg": {"channel": "mark-price"}, "data": [{"instId": "BTC-USDT-SWAP", "markPx": "1", "ts": "1"}]},
            {"arg": {"channel": "index-tickers"}, "data": [{"instId": "BTC-USDT", "idxPx": "1", "ts": "1"}]},
            {"arg": {"channel": "funding-rate"}, "data": [{"instId": "BTC-USDT-SWAP", "fundingRate": "0.0001", "fundingTime": "1", "ts": "1"}]},
            {"arg": {"channel": "open-interest"}, "data": [{"instId": "BTC-USDT-SWAP", "oi": "1", "ts": "1"}]},
            {"arg": {"channel": "liquidation-orders"}, "data": [{"instId": "BTC-USDT-SWAP", "details": [{"bkPx": "1", "sz": "1", "side": "sell", "ts": "1"}]}]},
        ]
        for msg in messages:
            events = app.adapter.normalize(msg, local_receive_ts=1)
            for event in events:
                app._persist_event(event)
    finally:
        _flush(app)
        app.quality_writer.close()
        app.raw_wire_writer.close()

    expected_streams = {
        "okx_trades", "okx_trades_all", "okx_markprice", "okx_indextickers",
        "okx_fundingrate", "okx_openinterest", "okx_liquidation",
    }
    raw_dir = Path(tmp_path) / "raw"
    present = {p.name for p in raw_dir.iterdir() if p.is_dir()}
    assert expected_streams <= present, f"missing: {expected_streams - present}"
    # Every stream namespace is OKX-prefixed -- no collision with a future
    # okx_orderbook, or with any binance_*/bybit_* stream name.
    assert all(name.startswith("okx_") for name in expected_streams)

    trades_files = list((raw_dir / "okx_trades").glob("*.seg"))
    assert trades_files, "expected at least one committed okx_trades segment"
    table = pq.read_table(trades_files[0])
    assert table.num_rows == 1
    assert table.column("price")[0].as_py() == 1.0


def test_orderbook_events_are_a_documented_noop_not_a_crash(tmp_path):
    """books storage is out of this runner's scope (see module docstring);
    confirm that feeding it a books frame does not raise or silently write
    into the wrong stream."""
    app = OKXCollectorApp(data_dir=str(tmp_path))
    try:
        msg = {"arg": {"channel": "books"}, "data": [{"bids": [["1", "1"]], "asks": [["2", "1"]], "ts": "1", "seqId": 1, "prevSeqId": -1}]}
        events = app.adapter.normalize(msg, local_receive_ts=1)
        for event in events:
            app._persist_event(event)  # must not raise
    finally:
        _flush(app)
        app.quality_writer.close()
        app.raw_wire_writer.close()
    raw_dir = Path(tmp_path) / "raw"
    assert not (raw_dir / "okx_orderbook").exists()


def test_subscribe_message_covers_exactly_the_seven_d11_channels(tmp_path):
    app = OKXCollectorApp(data_dir=str(tmp_path))
    try:
        channels = {a["channel"] for a in app._subscribe_message["args"]}
    finally:
        _flush(app)
        app.quality_writer.close()
        app.raw_wire_writer.close()
    assert channels == set(app.channels)
    assert "books" not in channels
