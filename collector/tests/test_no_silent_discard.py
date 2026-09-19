"""Integration tests for the no-silent-discard contract in the collector.

Before Phase 2 these paths turned received data into nothing with no durable
record:

* ``handle_message`` bare-returned on any frame lacking ``stream``/``data``
* an unrouted stream produced a counter bump and a log line only
* a frame that failed ``json.loads`` produced a log line only, and its text
  was discarded before anything could record it
* REST snapshot and OI bodies were parsed and thrown away, so replay would
  have had to contact the live exchange

Each test below pins one of those.
"""
from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import MagicMock

import pytest

from collector import run_collector as _run_collector
from collector.collector.raw_capture import RawCapture, RawWireRecord
from collector.collector.websocket_client import WebSocketClient

CollectorApp = _run_collector.CollectorApp


def _app():
    """A CollectorApp with only what these tests touch, no real I/O."""
    app = CollectorApp.__new__(CollectorApp)
    app.raw_messages_logged = 99
    app.stream_counters = {
        name: {"received": 0, "computed": 0, "empty_features": 0,
               "validated": 0, "rejected": 0, "written": 0}
        for name in ("orderbook", "trades", "markprice", "openinterest", "liquidation")
    }
    app.stream_counters["unrouted"] = {"received": 0}
    app.stream_counters["malformed_envelope"] = {"received": 0}
    app.stream_counters["adapter_unhandled"] = {"received": 0}
    app.validation_fail_reasons = {}
    app.quality_events = []
    app._persist_quality_event = app.quality_events.append
    app.validator = MagicMock()
    app.validator.check_failure_rate.return_value = False
    app.binance_book = MagicMock()
    app._drain_integrity_quality_events = lambda: None
    app.raw_capture = None
    return app


def _reasons(app):
    return [event.get("reason", "") for event in app.quality_events]


# ---------------------------------------------------------------------------
# handle_message: non-envelope frames
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "frame",
    [
        {"result": None, "id": 1},           # subscription ack
        {"code": -1121, "msg": "Invalid symbol"},  # venue error envelope
        {"stream": "btcusdt@depth@100ms"},   # data missing
        {"data": {"e": "depthUpdate"}},      # stream missing
        {},                                  # empty
    ],
)
async def test_non_envelope_frames_produce_a_durable_record(frame):
    app = _app()
    await app.handle_message(frame, local_receive_ts=42, connection_id="public-1")

    assert app.stream_counters["malformed_envelope"]["received"] == 1
    assert len(app.quality_events) == 1
    event = app.quality_events[0]
    assert "non_envelope_frame" in event["reason"]
    assert event["rows_lost"] == 1
    assert event["connection_id"] == "public-1"
    assert event["local_receive_ts"] == 42


@pytest.mark.asyncio
async def test_non_dict_frame_does_not_raise_and_is_recorded():
    app = _app()
    await app.handle_message(["not", "a", "dict"], local_receive_ts=1)
    assert app.stream_counters["malformed_envelope"]["received"] == 1
    assert app.quality_events


@pytest.mark.asyncio
async def test_unrouted_stream_is_durable_not_log_only():
    app = _app()
    await app.handle_message(
        {"stream": "btcusdt@someNewChannel", "data": {"x": 1}},
        local_receive_ts=7, connection_id="market-3",
    )
    assert app.stream_counters["unrouted"]["received"] == 1
    event = app.quality_events[0]
    assert event["reason"] == "unrouted_stream:btcusdt@someNewChannel"
    assert event["connection_id"] == "market-3"


@pytest.mark.asyncio
async def test_a_valid_frame_still_produces_no_drop_record():
    app = _app()
    app._handle_markprice = MagicMock()
    now = int(time.time() * 1000)
    await app.handle_message(
        {"stream": "btcusdt@markPrice@1s",
         "data": {"E": now, "p": "100.5", "r": "0.0001", "T": now + 3_600_000}},
        local_receive_ts=now,
    )
    assert app.quality_events == []
    assert app.stream_counters["malformed_envelope"]["received"] == 0


# ---------------------------------------------------------------------------
# Raw frame capture wiring
# ---------------------------------------------------------------------------


def test_capture_raw_frame_preserves_the_exact_text_and_native_ids():
    app = _app()
    rows = []
    writer = type("W", (), {"write": lambda self, row: rows.append(row)})()
    app.raw_capture = RawCapture(writer)

    frame = json.dumps({
        "stream": "btcusdt@depth@100ms",
        "data": {"E": 1700, "u": 55, "U": 50, "pu": 49},
    })
    app._capture_raw_frame(
        frame, local_receive_ts=1234, connection_id="public-2",
        connection_generation=2, parsed=json.loads(frame),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["payload"] == frame
    assert row["local_receive_ts"] == 1234
    assert row["connection_id"] == "public-2"
    assert row["channel"] == "orderbook"
    assert (row["update_id"], row["first_update_id"], row["previous_update_id"]) == (55, 50, 49)
    assert row["exchange_event_ts"] == 1700


def test_capture_raw_frame_records_undecodable_text():
    app = _app()
    rows = []
    writer = type("W", (), {"write": lambda self, row: rows.append(row)})()
    app.raw_capture = RawCapture(writer)

    app._capture_raw_frame(
        "{broken", local_receive_ts=9, decode_ok=False,
        decode_error="Expecting property name", parsed=None,
    )
    assert rows[0]["payload"] == "{broken"
    assert rows[0]["decode_ok"] is False
    assert rows[0]["update_id"] is None  # nothing invented from an unparsed frame


def test_capture_raw_frame_is_a_noop_without_capture_configured():
    app = _app()
    app._capture_raw_frame("{}", local_receive_ts=1)  # must not raise


def test_production_capture_raw_frame_survives_every_on_raw_frame_call_shape():
    """Regression: a keyword ``WebSocketClient._consume`` can add must not
    break the real, restricted-signature production callback.

    ``_capture_raw_frame``'s signature is deliberately narrow (no
    ``**kwargs``): it is production code, not a test double, and a narrow
    signature is what lets a reviewer see exactly which fields it consumes.
    That narrowness is also the hazard -- when the OKX capture work
    (`control_frames`/keepalive) needed ``_consume`` to tell every
    ``on_raw_frame`` callback whether a frame was a protocol control frame,
    it added ``control_frame=`` to *every* call, including Binance's. The
    only existing integration-level test for this path used
    ``def on_raw(frame, **kw): ...`` -- strictly more permissive than the
    real method -- so it kept passing while the real callback silently
    raised ``TypeError`` on every single frame, inside the client's
    fail-open ``try/except``. Net effect: raw-wire capture for the live
    Binance path would have gone completely dark, with no exception, no log
    reaching an operator's attention beyond a per-frame warning, and no
    quality event -- exactly the "silent parser failure" this project's
    rules forbid.

    This drives the *actual* ``WebSocketClient._consume`` coroutine, not a
    hand-rolled substitute, so it fails again if any future change to the
    calling contract is not mirrored in ``_capture_raw_frame``.
    """
    app = _app()
    captured = []
    app.raw_capture = MagicMock()
    app.raw_capture.capture_wire.side_effect = lambda record: captured.append(record)

    class _FakeSocket:
        def __init__(self, frames):
            self.frames = frames

        def __aiter__(self):
            async def gen():
                for frame in self.frames:
                    yield frame
            return gen()

    async def on_message(data, ts, connection_id=None):
        pass

    client = WebSocketClient(
        url="wss://fstream.binance.com/public/stream",
        on_message=on_message,
        on_raw_frame=app._capture_raw_frame,
    )
    client.running = True  # _consume gates its loop on this; start() sets it,
                           # but this test drives _consume directly.

    asyncio.run(client._consume(_FakeSocket([
        '{"stream":"btcusdt@depth","data":{"u":1}}',
        "{not valid json",
    ])))

    assert len(captured) == 2, (
        "every inbound frame must reach durable raw capture; "
        f"only {len(captured)} did"
    )
    assert captured[0].decode_ok is True
    assert captured[1].decode_ok is False


def test_adapter_unhandled_becomes_a_durable_quality_event():
    app = _app()
    from collector.collector.adapters.okx import OKXAdapter

    adapter = OKXAdapter()
    adapter.set_unhandled_sink(app._record_adapter_unhandled)
    adapter.normalize({"arg": {"channel": "open-interest"}, "data": [{}]}, local_receive_ts=3)

    assert app.stream_counters["adapter_unhandled"]["received"] == 1
    assert "channel_not_implemented" in app.quality_events[0]["reason"]


# ---------------------------------------------------------------------------
# WebSocket client: capture precedes parsing
# ---------------------------------------------------------------------------


class _FakeSocket:
    def __init__(self, frames):
        self.frames = frames

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __aiter__(self):
        async def gen():
            for frame in self.frames:
                yield frame
        return gen()


def _client_over(frames, **kwargs):
    async def noop(*args, **kw):
        return None

    client = WebSocketClient(url="wss://x", on_message=kwargs.pop("on_message", noop), **kwargs)
    return client


@pytest.mark.asyncio
async def test_malformed_frame_is_captured_before_parsing_and_raises_a_quality_event(monkeypatch):
    captured, quality, delivered = [], [], []

    async def on_message(data, ts):
        delivered.append(data)

    def on_raw(frame, **kw):
        captured.append((frame, kw["decode_ok"], kw["decode_error"]))

    def on_quality(event_type, reason, connection_id=None, stream_group=None):
        quality.append((event_type, reason))

    client = _client_over(None, on_message=on_message, on_raw_frame=on_raw,
                          on_quality_event=on_quality, stream_group="public")

    frames = ['{"stream":"s","data":{}}', "{not json at all", '{"stream":"t","data":{}}']
    monkeypatch.setattr(
        _run_collector.asyncio, "sleep",
        lambda *_: (_ for _ in ()).throw(asyncio.CancelledError()), raising=False,
    )

    def fake_connect(url):
        return _FakeSocket(frames)

    monkeypatch.setattr("collector.collector.websocket_client.websockets.connect", fake_connect)

    async def stop_soon(*_):
        client.running = False

    monkeypatch.setattr("collector.collector.websocket_client.asyncio.sleep", stop_soon)
    await client.start()

    # All three frames captured, including the one that never parsed.
    assert len(captured) == 3
    assert captured[1][0] == "{not json at all"
    assert captured[1][1] is False
    assert captured[1][2] is not None

    # Only the two decodable frames reached the handler.
    assert len(delivered) == 2

    # The malformed frame is durable, not log-only.
    assert any("malformed_frame" in reason for _, reason in quality)
    assert client.malformed_frames == 1


@pytest.mark.asyncio
async def test_handler_without_connection_id_is_still_supported(monkeypatch):
    """Arity is probed once; legacy two-argument handlers keep working."""
    seen = []

    async def legacy_handler(data, ts):
        seen.append((data, ts))

    client = _client_over(None, on_message=legacy_handler)
    assert client._on_message_takes_connection is False

    monkeypatch.setattr(
        "collector.collector.websocket_client.websockets.connect",
        lambda url: _FakeSocket(['{"stream":"s","data":{}}']),
    )

    async def stop_soon(*_):
        client.running = False

    monkeypatch.setattr("collector.collector.websocket_client.asyncio.sleep", stop_soon)
    await client.start()
    assert len(seen) == 1


def test_connection_id_arity_probe():
    async def with_conn(data, ts, connection_id=None):
        return None

    async def with_kwargs(data, ts, **kw):
        return None

    async def without(data, ts):
        return None

    assert WebSocketClient._accepts_connection_id(with_conn) is True
    assert WebSocketClient._accepts_connection_id(with_kwargs) is True
    assert WebSocketClient._accepts_connection_id(without) is False
    assert WebSocketClient._accepts_connection_id(None) is False


def test_collector_handle_message_accepts_connection_id():
    """The real handler must opt into connection lineage."""
    assert WebSocketClient._accepts_connection_id(CollectorApp.handle_message) is True
