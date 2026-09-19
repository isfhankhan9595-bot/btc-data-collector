"""Tests for the live Bybit collector.

The most important tests here drive the *real* ``WebSocketClient._consume()``
coroutine against synthetic frames, not a hand-rolled substitute for it.
Two real bugs were found writing this runner precisely because a first pass
called ``BybitCollectorApp._handle_message`` directly and only caught them
once the actual client machinery was exercised:

1. ``on_message`` is awaited by ``_consume`` (``await self.on_message(...)``),
   so a synchronous handler would raise ``TypeError`` on the first frame,
   inside the same fail-open ``try/except`` responsible for this session's
   P0 hotfix. A test that calls the handler directly cannot see this --
   calling a sync function and calling a coroutine function both "work"
   until something actually awaits the result.
2. ``on_open`` is invoked as ``await self.on_open(self._send)`` -- the
   client's bound ``_send`` method, not the client instance. Code written
   against the (wrong) assumption that it receives the client would pass its
   own tests if those tests also assumed a client object, sharing the exact
   blind spot as the bug (the failure mode this project's rules name
   explicitly: a test that encodes the same wrong assumption as the code).
"""
from __future__ import annotations

import asyncio
import json
import time

from collector.collector.quality_events import BookQuality
from collector.run_bybit_collector import BybitCollectorApp, _topics


def _app(tmp_path):
    return BybitCollectorApp(data_dir=str(tmp_path))


class _FakeSocket:
    def __init__(self, frames):
        self.frames = frames

    def __aiter__(self):
        async def gen():
            for frame in self.frames:
                yield frame
        return gen()


def _drive(app, frames):
    """Run frames through the real WebSocketClient._consume() coroutine."""
    app.client.running = True
    asyncio.run(app.client._consume(_FakeSocket(frames)))


def test_topics_use_configured_depth_and_all_four_declared_channels():
    topics = _topics()
    assert len(topics) == 4
    assert any(t.startswith("orderbook.") and t.endswith(".BTCUSDT") for t in topics)
    assert "publicTrade.BTCUSDT" in topics
    assert "tickers.BTCUSDT" in topics
    assert "allLiquidation.BTCUSDT" in topics


def test_on_open_sends_subscribe_over_the_bound_send_method_it_actually_receives(tmp_path):
    """Regression: on_open must accept a send coroutine, not a client object.

    WebSocketClient calls ``await self.on_open(self._send)`` -- confirmed by
    reading the call site. A handler written as ``async def
    _on_open(self, client): await client._send(...)`` raises
    ``AttributeError`` on the first connection, since a bound method has no
    ``._send`` attribute of its own.
    """
    app = _app(tmp_path)
    sent = []

    async def fake_send(payload):
        sent.append(payload)

    asyncio.run(app._on_open(fake_send))

    assert len(sent) == 1
    message = json.loads(sent[0])
    assert message["op"] == "subscribe"
    assert set(message["args"]) == set(_topics())


def test_handle_message_is_a_coroutine():
    """Regression: on_message is awaited by _consume; a sync handler would
    raise TypeError on the very first frame, silently, inside the client's
    fail-open try/except -- indistinguishable from a dropped frame."""
    assert asyncio.iscoroutinefunction(BybitCollectorApp._handle_message)


def test_snapshot_then_delta_through_the_real_consume_path(tmp_path):
    app = _app(tmp_path)
    now = int(time.time() * 1000)
    snapshot = json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": now,
                          "data": {"s": "BTCUSDT",
                                    "b": [["50000", "1.0"], ["49999", "2.0"]],
                                    "a": [["50001", "1.5"], ["50002", "0.5"]],
                                    "u": 1, "seq": 100}})
    delta = json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": now + 10,
                       "data": {"s": "BTCUSDT", "b": [["50000", "3.0"]], "a": [],
                                "u": 2, "seq": 101}})

    _drive(app, [snapshot, delta])

    assert app.book.state.state is BookQuality.VALID
    assert len(app.ob_writer.buffer) == 2
    last = app.ob_writer.buffer[-1]
    assert last["bids_price"][0] == 50000.0
    assert last["bids_qty"][0] == 3.0          # delta applied, absolute quantity
    assert last["is_snapshot"] is False
    assert len(app.raw_wire_writer.buffer) == 2  # every frame captured before parsing


def test_update_id_decrease_is_detected_and_recorded(tmp_path):
    """BybitSequenceComparator flags a *decreasing* update_id as a reset
    signal -- confirmed by reading sequence.py, not assumed. Unlike Binance,
    an *increasing* jump in u is not itself treated as a gap: per official
    Bybit docs, `u` only guarantees non-decrease, and continuity is
    self-healed by a fresh snapshot rather than a client-side bridge chain
    (LocalBook's Binance-only buffering branch does not apply to Bybit at
    all). A first version of this test asserted an increasing jump was a
    gap; that was this test's own wrong assumption, not a property of the
    protocol, and was corrected rather than left in place."""
    app = _app(tmp_path)
    now = int(time.time() * 1000)
    snapshot = json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": now,
                          "data": {"s": "BTCUSDT", "b": [["50000", "1.0"]],
                                    "a": [["50001", "1.0"]], "u": 10, "seq": 100}})
    decreased = json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": now + 10,
                           "data": {"s": "BTCUSDT", "b": [["50000", "9.0"]], "a": [],
                                    "u": 3, "seq": 104}})

    _drive(app, [snapshot, decreased])

    assert app.book.state.state is not BookQuality.VALID
    import pyarrow.parquet as pq
    segment_dir = tmp_path / "raw" / "bybit_quality_events"
    rows = []
    for f in segment_dir.glob("*.seg"):
        rows.extend(pq.read_table(f).to_pylist())
    assert any(r["stream"] == "bybit_orderbook" and r["new_state"] != BookQuality.VALID.value
              for r in rows), "a decreasing update_id must be recorded, not silently absorbed"


def test_trade_ticker_and_liquidation_messages_persist_to_their_own_streams(tmp_path):
    app = _app(tmp_path)
    now = int(time.time() * 1000)
    trade = json.dumps({"topic": "publicTrade.BTCUSDT", "ts": now,
                       "data": [{"T": now, "s": "BTCUSDT", "S": "Buy", "v": "0.01",
                                 "p": "50000.5", "i": "abc123", "BT": False}]})
    ticker = json.dumps({"topic": "tickers.BTCUSDT", "type": "snapshot", "ts": now,
                        "data": {"symbol": "BTCUSDT", "markPrice": "50000.1",
                                  "indexPrice": "50000.2", "fundingRate": "0.0001",
                                  "nextFundingTime": str(now + 3_600_000),
                                  "openInterest": "12345.6"}})
    liquidation = json.dumps({"topic": "allLiquidation.BTCUSDT", "ts": now,
                             "data": [{"T": now, "s": "BTCUSDT", "S": "Sell",
                                        "v": "1.2", "p": "49900"}]})

    _drive(app, [trade, ticker, liquidation])

    assert len(app.trades_writer.buffer) == 1
    assert app.trades_writer.buffer[0]["price"] == 50000.5
    assert len(app.mark_writer.buffer) == 1
    assert app.mark_writer.buffer[0]["funding_rate"] == 0.0001
    assert len(app.oi_writer.buffer) == 1
    assert app.oi_writer.buffer[0]["open_interest"] == 12345.6
    assert len(app.liq_writer.buffer) == 1
    assert app.liq_writer.buffer[0]["quantity"] == 1.2


def test_pong_control_frame_produces_no_events_and_does_not_raise(tmp_path):
    app = _app(tmp_path)
    pong = json.dumps({"success": True, "ret_msg": "pong", "conn_id": "abcd", "op": "ping"})

    _drive(app, [pong])   # must not raise

    assert app.ob_writer.buffer == []
    assert app.trades_writer.buffer == []
    assert len(app.raw_wire_writer.buffer) == 1   # still captured raw, just produced no events


def test_malformed_frame_is_captured_raw_and_does_not_crash_the_consumer(tmp_path):
    app = _app(tmp_path)
    _drive(app, ["{not valid json"])

    assert len(app.raw_wire_writer.buffer) == 1
    assert app.raw_wire_writer.buffer[0]["decode_ok"] is False


def test_capture_raw_frame_accepts_the_shared_client_contract(tmp_path):
    """Same regression class as the P0 hotfix: every on_raw_frame callback
    now receives control_frame=; this one must accept it."""
    app = _app(tmp_path)
    app._capture_raw_frame("{}", local_receive_ts=1, control_frame=True)
    assert len(app.raw_wire_writer.buffer) == 1


def test_keepalive_does_not_wire_an_unrecognisable_reply_expectation(tmp_path):
    """Bybit's pong is JSON with a dynamic conn_id; WebSocketClient's
    reply-tracking only recognises an exact raw-text match against
    control_frames. Wiring `expect=` here would mark every ping as awaiting
    a reply this mechanism can never see arrive, and eventually force a
    spurious reconnect on an otherwise healthy connection."""
    app = _app(tmp_path)
    assert app.client.keepalive is not None
    assert app.client.keepalive.expect is None
    assert json.loads(app.client.keepalive.payload) == {"op": "ping"}


def test_writers_attribute_their_own_events_to_bybit_not_binance(tmp_path):
    import pyarrow.parquet as pq

    app = _app(tmp_path)
    app._persist_quality_event({"stream": "bybit_orderbook", "event_type": "SEQUENCE_GAP",
                                "reason": "test"})
    # bybit_quality_events uses segment_rows=1/segment_seconds=1 (matching the
    # Binance quality writer's own settings), so the row is already flushed to
    # a closed segment rather than sitting in .buffer -- read it back.
    segment_dir = tmp_path / "raw" / "bybit_quality_events"
    files = list(segment_dir.glob("*.seg"))
    assert files, f"no published segment found under {segment_dir}"
    table = pq.read_table(files[0])
    assert table.column("exchange").to_pylist() == ["BYBIT"]
