import json
import sys
import time
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock

import pytest

from collector import run_collector as _run_collector
from collector.collector.book_engine import LocalBook
from collector.collector.canonical import CanonicalOrderBookEvent

CollectorApp = _run_collector.CollectorApp


def _valid_depth_msg():
    return {
        "E": int(time.time() * 1000),
        "U": 11,
        "u": 11,
        "pu": 10,
        "b": [[str(100.0 - i * 0.1), "1.0"] for i in range(10)],
        "a": [[str(101.0 + i * 0.1), "1.0"] for i in range(10)],
    }


def _valid_trade_msg(trade_id=123):
    return {"E": int(time.time() * 1000), "a": trade_id, "p": "100.5", "q": "1.0", "m": False}


def _valid_mark_msg():
    now = int(time.time() * 1000)
    return {"E": now, "p": "100.5", "r": "0.0001", "T": now + 3600000}


def _valid_liquidation_msg():
    return {"o": {"S": "BUY", "p": "100.5", "q": "1.0", "T": int(time.time() * 1000), "X": "FILLED", "f": "IOC"}}


def _seed_bridged_book(app, update_id=10):
    """Bridge the book with a snapshot, as live does at startup.

    A fresh LocalBook is RECOVERING and buffers diffs until a snapshot
    bridges it. Tests that exercise diff handling must establish that
    bridge first rather than relying on an unbridged book accepting diffs.
    """
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


def _app_without_init():
    app = CollectorApp.__new__(CollectorApp)
    app.raw_messages_logged = 20
    app.stream_counters = {
        "orderbook": {"received": 0, "computed": 0, "empty_features": 0, "validated": 0, "rejected": 0, "written": 0},
        "trades": {"received": 0, "computed": 0, "empty_features": 0, "validated": 0, "rejected": 0, "written": 0},
        "markprice": {"received": 0, "computed": 0, "empty_features": 0, "validated": 0, "rejected": 0, "written": 0},
        "openinterest": {"received": 0, "computed": 0, "empty_features": 0, "validated": 0, "rejected": 0, "written": 0},
        "liquidation": {"received": 0, "computed": 0, "empty_features": 0, "validated": 0, "rejected": 0, "written": 0},
        "unrouted": {"received": 0},
    }
    app.validation_fail_reasons = {"orderbook": {}, "trades": {}, "markprice": {}, "liquidation": {}}
    app.validator = MagicMock()
    app.validator.validate_orderbook.return_value = (True, "")
    app.validator.validate_trade.return_value = (True, "")
    app.validator.validate_markprice.return_value = (True, "")
    app.validator.validate_liquidation.return_value = (True, "")
    app.validator.check_failure_rate.return_value = False
    app.validator.failures_in_window = 0
    app.gap_detector = MagicMock()
    app.binance_adapter = _run_collector.BinanceAdapter()
    app.binance_book = LocalBook("BINANCE")
    app.binance_book.snapshot(CanonicalOrderBookEvent(
        "BINANCE", "orderbook", 1, None, 1,
        bids=((100.0, 1.0),), asks=((101.0, 1.0),), update_id=10, is_snapshot=True,
    ))
    app._book_snapshot_lock = None
    app.ob_writer = MagicMock()
    app.quality_writer = MagicMock()
    app.trades_writer = MagicMock()
    app.mark_writer = MagicMock()
    app.liq_writer = MagicMock()
    app.health_monitor = MagicMock()
    app.health_monitor.messages_per_minute = {"orderbook": 0, "trades": 0, "markprice": 0, "liquidation": 0}
    return app


def test_route_stream_matches_case_insensitive_required_streams():
    app = CollectorApp.__new__(CollectorApp)
    assert app._route_stream("btcusdt@depth10@100ms") == "orderbook"
    assert app._route_stream("btcusdt@aggTrade") == "trades"
    assert app._route_stream("btcusdt@aggtrade") == "trades"
    assert app._route_stream("btcusdt@markPrice@1s") == "markprice"
    assert app._route_stream("btcusdt@markprice@1s") == "markprice"
    assert app._route_stream("btcusdt@forceOrder") == "liquidation"
    assert app._route_stream("ethusdt@aggtrade") is None


@pytest.mark.asyncio
async def test_handle_message_routes_lowercase_trade_and_markprice_to_health_monitor():
    app = _seed_bridged_book(_app_without_init())
    await app.handle_message({"stream": "btcusdt@depth@100ms", "data": _valid_depth_msg()})
    await app.handle_message({"stream": "btcusdt@aggtrade", "data": _valid_trade_msg()})
    await app.handle_message({"stream": "btcusdt@markprice@1s", "data": _valid_mark_msg()})
    app.health_monitor.record_message.assert_any_call("orderbook", ANY)
    app.health_monitor.record_message.assert_any_call("trades", ANY)
    app.health_monitor.record_message.assert_any_call("markprice", ANY)
    assert app.stream_counters["trades"]["validated"] == 1
    assert app.stream_counters["markprice"]["validated"] == 1
    assert app.stream_counters["unrouted"]["received"] == 0


@pytest.mark.asyncio
async def test_diff_depth_is_processed_from_local_book_with_received_timestamp():
    app = _seed_bridged_book(_app_without_init())
    receive_ts = 123_456
    await app.handle_message({"stream": "btcusdt@depth@100ms", "data": _valid_depth_msg()}, local_receive_ts=receive_ts)
    record = app.ob_writer.write.call_args.args[0]
    assert record["local_timestamp"] == receive_ts
    assert record["bids_price"][0] == 100.0
    assert record["asks_price"][0] == 101.0
    assert app.binance_book.previous.update_id == 11


@pytest.mark.asyncio
async def test_markprice_local_timestamp_is_receive_time_not_processing_time():
    """P0-6: markprice's ``local_timestamp`` must be the frame's actual
    receive time (captured once at ``handle_message`` entry), never a
    fresh ``time.time()`` call taken later inside the handler. Before the
    fix, ``local_timestamp`` silently duplicated ``timestamp`` (processing
    time), so the research assembler had no genuine availability clock for
    markprice at all -- this is the mutation this test must catch."""
    app = _seed_bridged_book(_app_without_init())
    receive_ts = 987_654
    before_call = int(time.time() * 1000)
    await app.handle_message(
        {"stream": "btcusdt@markprice@1s", "data": _valid_mark_msg()}, local_receive_ts=receive_ts,
    )
    record = app.mark_writer.write.call_args.args[0]
    assert record["local_timestamp"] == receive_ts
    # "timestamp" (processing time) is a real, independent wall-clock read
    # taken inside compute_markprice_features -- it must not be forced to
    # equal receive_ts, and (this being a live, later clock read) must be
    # at or after the moment receive_ts was captured for this test.
    assert record["timestamp"] >= before_call
    assert record["local_timestamp"] != record["timestamp"]


@pytest.mark.asyncio
async def test_startup_verification_uses_cumulative_received_counters(monkeypatch):
    app = _app_without_init()
    app.stream_counters["orderbook"]["received"] = 533
    app.stream_counters["trades"]["received"] = 651
    app.stream_counters["markprice"]["received"] = 59
    app.health_monitor.messages_per_minute = {"orderbook": 0, "trades": 0, "markprice": 0, "liquidation": 0}
    monkeypatch.setattr(_run_collector, "STREAM_INACTIVE_STARTUP_SECONDS", 0)
    await app._verify_startup_streams()


def test_trade_validation_rejections_are_grouped_by_reason():
    app = _app_without_init()
    app.validator.validate_trade.return_value = (False, "Timestamp regression")
    app._handle_trades(_valid_trade_msg(), "btcusdt@aggtrade")
    app._handle_trades(_valid_trade_msg(trade_id=124), "btcusdt@aggtrade")
    assert app.stream_counters["trades"]["rejected"] == 2
    assert app.validation_fail_reasons["trades"] == {"Timestamp regression": 2}


def test_handlers_use_exchange_timestamp_for_gap_detection(monkeypatch):
    app = _app_without_init()
    trade_features = {"timestamp": 11_000, "exchange_timestamp": 1_200, "trade_id": 123, "price": 100.5, "quantity": 1.0, "is_buyer_maker": False}
    orderbook_features = {"timestamp": 11_000, "exchange_timestamp": 1_300}
    markprice_features = {"timestamp": 11_000, "exchange_timestamp": 1_400}
    monkeypatch.setattr(_run_collector, "compute_trades_features", lambda _: trade_features)
    monkeypatch.setattr(_run_collector, "compute_orderbook_features", lambda _: orderbook_features)
    monkeypatch.setattr(_run_collector, "compute_markprice_features", lambda _: markprice_features)
    app._handle_trades({}, "btcusdt@aggtrade")
    app._handle_orderbook({}, "btcusdt@depth10@100ms")
    app._handle_markprice({}, "btcusdt@markprice@1s", 11_000)
    app.gap_detector.check_gap.assert_any_call("trades", 1_200)
    app.gap_detector.check_gap.assert_any_call("orderbook", 1_300)
    app.gap_detector.check_gap.assert_any_call("markprice", 1_400)


def test_trade_processing_backlog_local_timestamp_gap_does_not_create_gap(monkeypatch):
    app = _app_without_init()
    app.gap_detector = _run_collector.GapDetector()
    mock_logger = MagicMock()
    mock_alert = MagicMock()
    monkeypatch.setattr(_run_collector, "logger", mock_logger)
    monkeypatch.setattr(_run_collector, "send_telegram_alert", mock_alert)
    monkeypatch.setitem(_run_collector.GapDetector.check_gap.__globals__, "logger", mock_logger)
    monkeypatch.setitem(_run_collector.GapDetector.check_gap.__globals__, "send_telegram_alert", mock_alert)
    feature_records = iter([
        {"timestamp": 1_000, "exchange_timestamp": 1_000, "trade_id": 123, "price": 100.5, "quantity": 1.0, "is_buyer_maker": False},
        {"timestamp": 11_000, "exchange_timestamp": 1_200, "trade_id": 124, "price": 100.5, "quantity": 1.0, "is_buyer_maker": False},
    ])
    monkeypatch.setattr(_run_collector, "compute_trades_features", lambda _: next(feature_records))
    app._handle_trades({}, "btcusdt@aggtrade")
    app._handle_trades({}, "btcusdt@aggtrade")
    assert app.gap_detector.last_seen["trades"] == 1_200
    mock_logger.warning.assert_not_called()
    mock_alert.assert_not_called()


def test_orderbook_reconnect_preserves_validation_context_without_economic_price_censorship():
    app = CollectorApp.__new__(CollectorApp)
    app.validator = _run_collector.Validator()
    app.gap_detector = _run_collector.GapDetector()
    app.validator.last_timestamps = {"orderbook": 1000, "trades": 2000, "markprice": 3000}
    app.gap_detector.last_seen = {"orderbook": 1000, "trades": 2000, "markprice": 3000}
    app.validator.last_trade_id = 12345
    app.validator.last_mid_price = 100.0
    reconnect_handler = app._make_reconnect_handler(_run_collector.BINANCE_PUBLIC_WS_URL)
    reconnect_handler()
    assert app.validator.last_timestamps["orderbook"] == 0
    assert app.gap_detector.last_seen["orderbook"] == 0
    assert app.validator.last_timestamps["trades"] == 2000
    assert app.gap_detector.last_seen["trades"] == 2000
    assert app.validator.last_trade_id == 12345
    assert app.validator.last_mid_price == 100.0
    now = int(time.time() * 1000)
    valid, reason = app.validator.validate_trade({
        "timestamp": now,
        "exchange_timestamp": now,
        "trade_id": 12346,
        "price": 106.0,
        "quantity": 1.0,
    })
    assert valid
    assert reason == ""


def test_market_reconnect_resets_trades_and_markprice_without_orderbook_state():
    app = CollectorApp.__new__(CollectorApp)
    app.validator = _run_collector.Validator()
    app.gap_detector = _run_collector.GapDetector()
    app.validator.last_timestamps = {"orderbook": 1000, "trades": 2000, "markprice": 3000}
    app.gap_detector.last_seen = {"orderbook": 1000, "trades": 2000, "markprice": 3000}
    app.validator.last_trade_id = 12345
    app.validator.last_mid_price = 100.0
    reconnect_handler = app._make_reconnect_handler(_run_collector.BINANCE_MARKET_WS_URL)
    reconnect_handler()
    assert app.validator.last_timestamps == {"orderbook": 1000, "trades": 0, "markprice": 0}
    assert app.gap_detector.last_seen == {"orderbook": 1000, "trades": 0, "markprice": 0}
    assert app.validator.last_trade_id == -1
    assert app.validator.last_mid_price == 100.0


@pytest.mark.asyncio
async def test_poll_openinterest_writes_valid_rest_response(monkeypatch):
    app = _app_without_init()
    app.running = True
    app.stream_counters["openinterest"] = {"received": 0, "computed": 0, "empty_features": 0, "validated": 0, "rejected": 0, "written": 0}
    app.oi_writer = MagicMock()
    captured = []
    app._capture_rest = captured.append
    body = json.dumps({"openInterest": "2.5", "time": str(int(time.time() * 1000)), "price": "100.0"})
    class FakeResponse:
        # Models the aiohttp response surface the collector actually uses:
        # status and text() are required so the REST body can be captured
        # verbatim for replay, not just parsed and discarded.
        status = 200
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): return False
        async def text(self): return body
        async def json(self): return json.loads(body)
        def raise_for_status(self): return None
    class FakeSession:
        def __init__(self, *args, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): return False
        def get(self, url): return FakeResponse()
    class FakeAiohttp:
        ClientSession = FakeSession
        class ClientTimeout:
            def __init__(self, total): self.total = total
    async def stop_after_poll(seconds): app.running = False
    monkeypatch.setitem(sys.modules, "aiohttp", FakeAiohttp)
    monkeypatch.setattr(_run_collector.asyncio, "sleep", stop_after_poll)
    await app._poll_openinterest()
    app.oi_writer.write.assert_called_once()
    app.health_monitor.record_message.assert_any_call("openinterest", ANY)
    assert app.stream_counters["openinterest"]["written"] == 1
    # REST lineage must be captured verbatim, with request and response
    # timestamps kept distinct from the exchange observation time.
    assert captured, "OI poll must record REST lineage"
    record = captured[-1]
    assert record.purpose == "open_interest"
    assert record.ok is True
    assert record.http_status == 200
    assert record.payload == body
    assert record.request_ts <= record.response_receive_ts


@pytest.mark.asyncio
async def test_forceorder_message_routes_to_liquidation_writer_not_trades_writer():
    app = _app_without_init()
    await app.handle_message({"stream": "btcusdt@forceOrder", "data": _valid_liquidation_msg()})
    app.liq_writer.write.assert_called_once()
    app.trades_writer.write.assert_not_called()
    app.health_monitor.record_message.assert_any_call("liquidation", ANY)
    assert app.stream_counters["liquidation"]["written"] == 1
    assert app.stream_counters["trades"]["received"] == 0
    assert app.stream_counters["unrouted"]["received"] == 0


@pytest.mark.asyncio
async def test_forceorder_message_with_real_health_monitor_writes_without_rejection():
    from collector.collector.disk_monitor import DiskMonitor
    from collector.collector.gap_detector import GapDetector
    from collector.collector.health_monitor import HealthMonitor
    from collector.collector.validator import Validator
    app = _app_without_init()
    app.validator = Validator()
    app.gap_detector = GapDetector()
    app.liq_writer = MagicMock()
    ws_ref = SimpleNamespace(connected=True)
    app.health_monitor = HealthMonitor(DiskMonitor(), app.validator, ws_ref)
    await app.handle_message({"stream": "btcusdt@forceOrder", "data": _valid_liquidation_msg()})
    app.liq_writer.write.assert_called_once()
    assert app.stream_counters["liquidation"]["written"] == 1
    assert app.stream_counters["liquidation"]["rejected"] == 0
    assert app.health_monitor.messages_per_minute["liquidation"] == 1


@pytest.mark.asyncio
async def test_unbridged_book_never_reports_valid_and_buffers_diffs():
    """Regression: a book with no snapshot must not claim VALID.

    Previously BookQualityStateMachine started in VALID, so every quality
    event and raw record written before the first bridge was labelled VALID
    despite no snapshot having been applied.
    """
    from collector.collector.quality_events import BookQuality

    app = _app_without_init()
    app.binance_book = LocalBook("BINANCE")  # genuinely fresh: never bridged
    assert app.binance_book.state.state is BookQuality.RECOVERING
    assert app.binance_book.previous is None

    await app.handle_message({"stream": "btcusdt@depth@100ms", "data": _valid_depth_msg()})

    # Buffered, not applied: no authoritative book existed to update.
    app.ob_writer.write.assert_not_called()
    assert app.binance_book.state.state is not BookQuality.VALID
