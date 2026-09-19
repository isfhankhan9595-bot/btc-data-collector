"""OKX v5 public raw capture, and the venue-protocol hooks it needs.

Protocol assertions in this file come from the OKX v5 API documentation
("WebSocket" overview, "Subscribe", "Notification"), read 2026-09-19. What is
deliberately *not* asserted anywhere here is the meaning of any field inside
``data`` -- those schemas are unverified (D11), and this whole layer exists so
they can be observed rather than guessed.
"""
from __future__ import annotations

import json

import pytest

from collector.collector.adapters.okx import OKXAdapter
from collector.collector.config import RAW_WIRE_SCHEMA
from collector.collector.okx_capture import (
    OKX_KEEPALIVE,
    OKX_PONG_PAYLOAD,
    OKX_PUBLIC_WS_URL,
    FrameKind,
    OKXPublicCapture,
    SubscriptionLedger,
    classify_frame,
    okx_subscribe_message,
)
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.storage_layout import venue_stream
from collector.collector.raw_capture import RawCapture, RawWireRecord
from collector.collector.websocket_client import Keepalive, WebSocketClient
from collector.scripts import okx_schema_report


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeWS:
    """Minimal stand-in for a websockets connection."""

    def __init__(self, frames=(), on_exhausted=None):
        self.frames = list(frames)
        self.sent: list[str] = []
        self.closed = False
        self._on_exhausted = on_exhausted

    async def send(self, payload):
        self.sent.append(payload)

    async def close(self):
        self.closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for frame in self.frames:
            yield frame
        if self._on_exhausted is not None:
            self._on_exhausted()

    @property
    def sent_json(self):
        out = []
        for payload in self.sent:
            try:
                out.append(json.loads(payload))
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
        return out


async def drive(client, frames):
    """Run one connection's worth of frames through the real client loop."""
    from unittest.mock import patch

    ws = FakeWS(frames, on_exhausted=client.stop)

    async def fake_connect(url, **kwargs):
        return ws

    client.running = True
    with patch("collector.collector.websocket_client.websockets.connect", new=fake_connect):
        await client.start()
    return ws


def capture_fixture(channels=("books", "trades"), **kwargs):
    events: list[dict] = []
    captured: list[RawWireRecord] = []

    class Sink:
        wire_captured = 0
        capture_failures = 0

        def capture_wire(self, record):
            captured.append(record)
            Sink.wire_captured += 1
            return True

    capture = OKXPublicCapture(
        channels, raw_capture=Sink(), quality_sink=events.append, **kwargs)
    return capture, events, captured


def reasons(events):
    return [e["reason"] for e in events]


# ---------------------------------------------------------------------------
# Documented subscribe format
# ---------------------------------------------------------------------------

def test_subscribe_message_matches_documented_format():
    message = okx_subscribe_message(["books", "trades"], "BTC-USDT-SWAP", request_id="sub-1")
    assert message == {
        "op": "subscribe",
        "id": "sub-1",
        "args": [
            {"channel": "books", "instId": "BTC-USDT-SWAP"},
            {"channel": "trades", "instId": "BTC-USDT-SWAP"},
        ],
    }
    # ``id`` is optional in the protocol and must be omitted, not sent null.
    assert "id" not in okx_subscribe_message(["books"])


def test_subscribe_message_rejects_empty_channel_list():
    with pytest.raises(ValueError):
        okx_subscribe_message([])


def test_public_endpoint_is_the_documented_one():
    assert OKX_PUBLIC_WS_URL == "wss://ws.okx.com:8443/ws/v5/public"


def test_keepalive_interval_is_below_the_documented_idle_limit():
    """The venue closes a connection idle for >30s; the timer must beat it."""
    assert OKX_KEEPALIVE.interval_s < 30.0
    assert OKX_KEEPALIVE.payload == "ping"
    assert OKX_KEEPALIVE.expect == "pong"


def test_keepalive_rejects_nonpositive_intervals():
    with pytest.raises(ValueError):
        Keepalive(payload="ping", interval_s=0)
    with pytest.raises(ValueError):
        Keepalive(payload="ping", interval_s=5, timeout_s=0)


# ---------------------------------------------------------------------------
# Envelope classification
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("frame,expected", [
    ({"event": "subscribe", "arg": {"channel": "books"}}, FrameKind.SUBSCRIBE_ACK),
    ({"event": "unsubscribe", "arg": {"channel": "books"}}, FrameKind.UNSUBSCRIBE_ACK),
    ({"event": "error", "code": "60012", "msg": "bad"}, FrameKind.ERROR),
    ({"event": "notice", "code": "64008", "msg": "upgrade"}, FrameKind.NOTICE),
    ({"event": "channel-conn-count", "connCount": "2"}, FrameKind.CONN_COUNT),
    ({"event": "channel-conn-count-error", "connCount": "30"}, FrameKind.CONN_COUNT),
    ({"arg": {"channel": "books"}, "data": []}, FrameKind.DATA_PUSH),
    ({"something": "else"}, FrameKind.UNKNOWN),
    ({"event": "brand-new-event-type"}, FrameKind.UNKNOWN),
    ("not-a-mapping", FrameKind.UNKNOWN),
])
def test_frame_classification(frame, expected):
    assert classify_frame(frame) is expected


# ---------------------------------------------------------------------------
# Control frames: pong is healthy protocol, not malformed data
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pong_is_not_reported_as_a_malformed_frame():
    """`pong` is not JSON.

    Without an explicit control-frame set it reaches ``json.loads``, fails,
    and writes a durable ERROR quality event -- one per heartbeat, forever,
    on a perfectly healthy connection. That is a false data-quality signal of
    the same class as D18.
    """
    delivered = []
    client = WebSocketClient(
        "ws://x", on_message=lambda *a, **k: _noop(delivered, a),
        control_frames=frozenset({OKX_PONG_PAYLOAD}))
    quality: list[tuple] = []
    client.on_quality_event = lambda *a: quality.append(a)
    raw: list[dict] = []
    client.on_raw_frame = lambda payload, **kw: raw.append({"payload": payload, **kw})

    await drive(client, [OKX_PONG_PAYLOAD, json.dumps({"arg": {"channel": "books"}, "data": []})])

    assert client.malformed_frames == 0
    assert client.control_frames_received == 1
    assert not any("malformed_frame" in str(q) for q in quality)
    # Still captured: a frame that arrived is evidence, even carrying no data.
    assert [r["control_frame"] for r in raw] == [True, False]
    # And never forwarded to the market-data handler.
    assert len(delivered) == 1


async def _noop(sink, args):
    sink.append(args)


@pytest.mark.asyncio
async def test_genuinely_malformed_frames_are_still_reported():
    """Regression: the control-frame path must not swallow real corruption."""
    client = WebSocketClient(
        "ws://x", on_message=lambda *a, **k: _noop([], a),
        control_frames=frozenset({OKX_PONG_PAYLOAD}))
    quality: list[tuple] = []
    client.on_quality_event = lambda *a: quality.append(a)

    await drive(client, ["{not json", OKX_PONG_PAYLOAD])

    assert client.malformed_frames == 1
    assert any("malformed_frame" in str(q) for q in quality)


# ---------------------------------------------------------------------------
# Subscription lifecycle must be observable
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_subscribe_is_sent_on_open_and_ack_recorded():
    capture, events, _ = capture_fixture(("books", "trades"))
    ws = await drive(capture.client, [
        json.dumps({"event": "subscribe", "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"}}),
        json.dumps({"event": "subscribe", "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}}),
    ])

    assert ws.sent_json[0]["op"] == "subscribe"
    assert {a["channel"] for a in ws.sent_json[0]["args"]} == {"books", "trades"}
    assert capture.ledger.fully_subscribed
    assert capture.ledger.pending == set()
    assert any("okx_subscribed:books" in r for r in reasons(events))


@pytest.mark.asyncio
async def test_rejected_subscription_is_durable_not_silent():
    """A rejected channel produces nothing for the life of the connection.

    If that is only logged, "this channel is dead" and "this market is quiet"
    are indistinguishable from the stored data.
    """
    capture, events, _ = capture_fixture(("books", "liquidation-orders"))
    await drive(capture.client, [
        json.dumps({"event": "subscribe", "arg": {"channel": "books"}}),
        json.dumps({"event": "error", "code": "60018",
                    "arg": {"channel": "liquidation-orders"}, "msg": "wrong url"}),
    ])

    assert capture.ledger.acknowledged == {"books"}
    assert "liquidation-orders" in capture.ledger.rejected
    assert not capture.ledger.fully_subscribed
    assert any("okx_subscribe_error:60018" in r for r in reasons(events))


@pytest.mark.asyncio
async def test_error_without_a_channel_is_still_recorded():
    capture, events, _ = capture_fixture(("books",))
    await drive(capture.client, [json.dumps({"event": "error", "code": "60012", "msg": "bad request"})])
    assert "<unattributed>" in capture.ledger.rejected
    assert any("okx_subscribe_error:60012" in r for r in reasons(events))


def test_reconnect_clears_acknowledgements():
    """Acks belong to a connection, not to the process.

    Carrying them across a reconnect would claim subscription coverage the
    new connection has not been granted yet.
    """
    ledger = SubscriptionLedger()
    ledger.request(["books", "trades"])
    ledger.acknowledge("books")
    ledger.reject("trades", "60018:nope")
    ledger.reset_for_new_connection()

    assert ledger.acknowledged == set()
    assert ledger.rejected == {}
    assert ledger.pending == {"books", "trades"}
    assert ledger.requested == {"books", "trades"}


@pytest.mark.asyncio
async def test_unclassified_envelope_is_recorded_as_a_drop():
    capture, events, _ = capture_fixture(("books",))
    await drive(capture.client, [json.dumps({"mystery": 1, "shape": "new"})])
    dropped = [e for e in events if e["reason"].startswith("okx_unclassified_frame")]
    assert dropped and dropped[0]["rows_lost"] == 1
    assert "keys=mystery,shape" in dropped[0]["reason"]


@pytest.mark.asyncio
async def test_notice_of_impending_disconnect_is_recorded():
    capture, events, _ = capture_fixture(("books",))
    await drive(capture.client, [
        json.dumps({"event": "notice", "code": "64008", "msg": "will close for upgrade"})])
    assert any("okx_notice:64008" in r for r in reasons(events))


# ---------------------------------------------------------------------------
# Raw capture: lineage without interpretation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_data_push_is_captured_with_envelope_lineage_only():
    capture, _, captured = capture_fixture(("books",))
    payload = {"arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
               "action": "update",
               "data": [{"seqId": 7, "prevSeqId": 6, "bids": [["1", "2", "0", "1"]]}]}
    await drive(capture.client, [json.dumps(payload)])

    record = captured[-1]
    assert record.venue == "OKX"
    assert record.channel == "books"          # from arg, a documented envelope field
    assert record.symbol == "BTC-USDT-SWAP"
    assert record.market_type == "linear_perpetual"
    assert record.connection_id is not None
    assert record.decode_ok is True
    # The payload is preserved verbatim, and nothing was read out of `data`.
    assert json.loads(record.payload) == payload
    assert record.update_id is None and record.previous_update_id is None
    assert capture.channel_frame_counts == {"books": 1}


@pytest.mark.asyncio
async def test_every_envelope_kind_is_captured_before_classification():
    """Capture precedes interpretation for control, error and unknown frames."""
    capture, _, captured = capture_fixture(("books",))
    frames = [
        OKX_PONG_PAYLOAD,
        json.dumps({"event": "error", "code": "1", "msg": "x"}),
        json.dumps({"weird": True}),
        "{broken",
        json.dumps({"arg": {"channel": "books"}, "data": [{}]}),
    ]
    await drive(capture.client, frames)
    assert len(captured) == len(frames)
    assert [r.decode_ok for r in captured] == [True, True, True, False, True]
    assert captured[0].channel == "__control__"
    assert captured[3].decode_error is not None


# ---------------------------------------------------------------------------
# The actual unblock mechanism
# ---------------------------------------------------------------------------

def test_capture_subscribes_to_every_declared_channel_regardless_of_parser_status():
    """Capture is unconditional: it subscribes to every channel the adapter
    declares whether or not normalize() implements it, because capture's job
    is recording the wire, not deciding what is parseable. (Originally this
    covered six then-unimplemented D11 channels specifically; all seven are
    now implemented -- see docs/OKX_D11_CHANNEL_SCHEMAS.md -- but the
    capture/parse independence this asserts is unchanged.)
    """
    adapter = OKXAdapter()
    declared = adapter.declared_channels()
    assert declared == adapter.implemented_channels(), (
        "D11 implementation is complete; a channel appearing here as "
        "declared-but-unimplemented would be a regression"
    )

    capture = OKXPublicCapture(sorted(declared))
    args = {a["channel"] for a in capture._subscribe_message["args"]}
    assert args == set(declared)


def test_capture_subscribe_args_are_channel_aware():
    """liquidation-orders subscribes by instType, not instId; index-tickers
    by its own index instId, not the SWAP instId every other channel uses.
    A uniform ``instId``-for-everything builder was the pre-implementation
    bug; this pins the fix (docs/OKX_D11_CHANNEL_SCHEMAS.md §F)."""
    capture = OKXPublicCapture(["books", "index-tickers", "liquidation-orders"])
    by_channel = {a["channel"]: a for a in capture._subscribe_message["args"]}
    assert by_channel["books"] == {"channel": "books", "instId": "BTC-USDT-SWAP"}
    assert by_channel["index-tickers"] == {"channel": "index-tickers", "instId": "BTC-USDT"}
    assert by_channel["liquidation-orders"] == {"channel": "liquidation-orders", "instType": "SWAP"}
    assert "instId" not in by_channel["liquidation-orders"]


def test_oversized_subscribe_request_is_refused_locally():
    with pytest.raises(ValueError, match="64 KB"):
        OKXPublicCapture([f"channel-{i}" for i in range(4000)])


def test_status_reports_whether_anything_is_arriving():
    capture, _, _ = capture_fixture(("books", "trades"))
    status = capture.status()
    assert status["venue"] == "OKX"
    assert status["fully_subscribed"] is False
    assert status["last_frame_ts"] is None
    assert status["subscriptions"]["requested"] == []
    assert set(status["frame_counts"]) == {k.value for k in FrameKind}


# ---------------------------------------------------------------------------
# Keepalive
# ---------------------------------------------------------------------------

def _keepalive_client():
    client = WebSocketClient("ws://x", on_message=None, keepalive=OKX_KEEPALIVE,
                             control_frames=frozenset({OKX_PONG_PAYLOAD}))
    client.running = True
    client._ws = FakeWS()
    return client


@pytest.mark.asyncio
async def test_keepalive_pings_only_after_the_link_goes_quiet(monkeypatch):
    client = _keepalive_client()
    now = [100.0]
    client._monotonic = lambda: now[0]
    client._last_inbound_monotonic = 100.0
    iterations = [0]

    async def fake_sleep(_delay):
        iterations[0] += 1
        now[0] += 5.0
        if iterations[0] >= 6:
            client.running = False

    monkeypatch.setattr("collector.collector.websocket_client.asyncio.sleep", fake_sleep)
    await client._keepalive_loop()

    # Busy link for the first 20s -> no ping; then exactly one, awaiting pong.
    assert client._ws.sent.count("ping") == 1
    assert client._awaiting_reply_since is not None


@pytest.mark.asyncio
async def test_unanswered_ping_closes_the_socket_and_is_recorded(monkeypatch):
    client = _keepalive_client()
    quality: list[tuple] = []
    client.on_quality_event = lambda *a: quality.append(a)
    now = [1000.0]
    client._monotonic = lambda: now[0]
    client._last_inbound_monotonic = 0.0          # long idle: ping immediately
    iterations = [0]

    async def fake_sleep(_delay):
        iterations[0] += 1
        now[0] += 20.0                            # blow past the 10s timeout
        if iterations[0] >= 8:
            client.running = False

    monkeypatch.setattr("collector.collector.websocket_client.asyncio.sleep", fake_sleep)
    await client._keepalive_loop()

    assert client.keepalive_timeouts == 1
    assert client._ws.closed is True, "a silently dead socket must be closed, not kept"
    assert any("keepalive_timeout" in str(q) for q in quality)


@pytest.mark.asyncio
async def test_pong_clears_the_outstanding_ping(monkeypatch):
    client = _keepalive_client()
    now = [500.0]
    client._monotonic = lambda: now[0]
    client._last_inbound_monotonic = 0.0
    iterations = [0]

    async def fake_sleep(_delay):
        iterations[0] += 1
        now[0] += 1.0
        if iterations[0] == 1:
            # The pong arrives via _consume, which clears the waiter.
            client._awaiting_reply_since = None
            client._last_inbound_monotonic = now[0]
        if iterations[0] >= 4:
            client.running = False

    monkeypatch.setattr("collector.collector.websocket_client.asyncio.sleep", fake_sleep)
    await client._keepalive_loop()

    assert client.keepalive_timeouts == 0
    assert client._ws.closed is False


@pytest.mark.asyncio
async def test_binance_style_client_starts_no_keepalive_task():
    """Binance needs no application heartbeat; the default must not add one."""
    delivered = []
    client = WebSocketClient("ws://x", on_message=lambda *a, **k: _noop(delivered, a))
    assert client.keepalive is None
    ws = await drive(client, [json.dumps({"stream": "btcusdt@depth", "data": {}})])
    assert ws.sent == []
    assert len(delivered) == 1


@pytest.mark.asyncio
async def test_failed_subscribe_on_open_is_recorded_and_reconnects():
    quality: list[tuple] = []

    async def exploding_open(_send):
        raise RuntimeError("socket write refused")

    client = WebSocketClient("ws://x", on_message=None, on_open=exploding_open)
    client.on_quality_event = lambda *a: quality.append(a)
    client.running = True

    from unittest.mock import patch

    async def fake_connect(url, **kwargs):
        client.running = False        # one attempt only
        return FakeWS([])

    async def fake_sleep(_d):
        return None

    with patch("collector.collector.websocket_client.websockets.connect", new=fake_connect):
        with patch("collector.collector.websocket_client.asyncio.sleep", new=fake_sleep):
            await client.start()

    assert any("subscribe_failed:RuntimeError" in str(q) for q in quality)


# ---------------------------------------------------------------------------
# Schema report: observation, never interpretation
# ---------------------------------------------------------------------------

def _write_raw_wire(tmp_path, frames):
    writer = ParquetWriter(venue_stream("OKX", "raw_wire"), RAW_WIRE_SCHEMA, base_dir=str(tmp_path),
                           exchange="OKX", segment_rows=1000, segment_seconds=3600)
    capture = RawCapture(writer, None)
    for index, (channel, payload, decode_ok) in enumerate(frames):
        capture.capture_wire(RawWireRecord(
            local_receive_ts=1_780_000_000_000 + index,
            payload=payload, venue="OKX", channel=channel, stream="okx_public",
            symbol="BTC-USDT-SWAP", decode_ok=decode_ok,
            decode_error=None if decode_ok else "boom",
            connection_id="okx_public-1", connection_generation=1))
    writer.close()


def test_schema_report_groups_observed_fields_by_channel(tmp_path):
    _write_raw_wire(tmp_path, [
        ("funding-rate", json.dumps({
            "arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
            "data": [{"instId": "BTC-USDT-SWAP", "fundingRate": "0.0001",
                      "fundingTime": "1700000000000", "method": "next_period"}]}), True),
        ("funding-rate", json.dumps({
            "arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
            "data": [{"instId": "BTC-USDT-SWAP", "fundingRate": "0.0002",
                      "fundingTime": "1700000008000"}]}), True),
        ("open-interest", json.dumps({
            "arg": {"channel": "open-interest", "instId": "BTC-USDT-SWAP"},
            "data": [{"instId": "BTC-USDT-SWAP", "oi": "5000", "oiCcy": "3.2"}]}), True),
        ("__control__", "pong", True),
        ("open-interest", "{broken", False),
        (None, json.dumps({"event": "subscribe", "arg": {"channel": "books"}}), True),
    ])

    report = okx_schema_report.collect(str(tmp_path))
    totals = report["totals"]
    assert totals["okx_rows"] == 6
    assert totals["data_push_frames"] == 3
    assert totals["control_frames"] == 1
    assert totals["decode_failed"] == 1
    assert totals["non_push_frames"] == 1

    funding = report["channels"]["funding-rate"]
    assert funding["frames"] == 2
    assert funding["fields"]["fundingRate"]["present_in"] == 2
    assert funding["fields"]["fundingRate"]["presence_ratio"] == 1.0
    # Partially present fields must not look mandatory.
    assert funding["fields"]["method"]["presence_ratio"] == 0.5
    assert "open-interest" in report["channels"]
    # Envelope keys are already documented and are excluded from the report.
    assert "arg" not in funding["fields"] and "data" not in funding["fields"]


def test_schema_report_flags_numeric_strings(tmp_path):
    """OKX sends numbers as strings; conflating the two causes coercion bugs."""
    _write_raw_wire(tmp_path, [
        ("trades", json.dumps({
            "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
            "data": [{"px": "64000.1", "sz": "3", "side": "buy", "count": 2,
                      "flag": True, "note": None, "levels": [["1", "2"]]}]}), True),
    ])
    fields = okx_schema_report.collect(str(tmp_path))["channels"]["trades"]["fields"]
    assert fields["px"]["types"] == {"str(numeric)": 1}
    assert fields["side"]["types"] == {"str": 1}
    assert fields["count"]["types"] == {"int": 1}
    assert fields["flag"]["types"] == {"bool": 1}
    assert fields["note"]["types"] == {"null": 1}
    # Nested numeric-string detection: order-book levels arrive as lists of
    # decimal strings, and knowing that is what prevents float coercion.
    assert fields["levels"]["types"] == {"list[list[str(numeric)]]": 1}


def test_schema_report_reports_nothing_when_nothing_was_captured(tmp_path):
    _write_raw_wire(tmp_path, [("__control__", "pong", True)])
    report = okx_schema_report.collect(str(tmp_path))
    assert report["channels"] == {}
    assert okx_schema_report.main(["--data-dir", str(tmp_path)]) == 1
    assert "Nothing can be verified" in okx_schema_report.render(report)


def test_schema_report_refuses_to_claim_semantics(tmp_path):
    """The rendered output must say what it does *not* establish.

    A report that lists field names without that caveat invites exactly the
    mistake D11 exists to prevent: writing a parser from names alone and
    guessing units and sign conventions.
    """
    _write_raw_wire(tmp_path, [
        ("funding-rate", json.dumps({
            "arg": {"channel": "funding-rate"},
            "data": [{"fundingRate": "0.0001"}]}), True)])
    rendered = okx_schema_report.render(okx_schema_report.collect(str(tmp_path)))
    assert "Field NAMES above are observed on the wire" in rendered
    assert "MEANINGS" in rendered
    assert "official documentation" in rendered
    assert okx_schema_report.main(["--data-dir", str(tmp_path)]) == 0


def test_okx_adapter_implements_all_declared_channels():
    """Was: adapter refuses six D11 channels with CHANNEL_NOT_IMPLEMENTED.
    Now: all seven wire channels (eight declared names) are implemented --
    see docs/OKX_D11_CHANNEL_SCHEMAS.md. This pins the opposite of the old
    assertion: no declared channel should still raise
    CHANNEL_NOT_IMPLEMENTED, and a well-formed frame for each produces a
    real canonical event, not an empty list."""
    adapter = OKXAdapter()
    fixtures = {
        "books": {"bids": [["50000", "1"]], "asks": [["50001", "1"]], "ts": "1700000000000", "seqId": 1, "prevSeqId": -1},
        "trades": {"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "50000", "sz": "1", "side": "buy", "ts": "1700000000000"},
        "trades-all": {"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "50000", "sz": "1", "side": "buy", "ts": "1700000000000", "source": "0"},
        "mark-price": {"instType": "SWAP", "instId": "BTC-USDT-SWAP", "markPx": "50000", "ts": "1700000000000"},
        "index-tickers": {"instId": "BTC-USDT", "idxPx": "50000", "ts": "1700000000000"},
        "funding-rate": {"instId": "BTC-USDT-SWAP", "fundingRate": "0.0001", "fundingTime": "1700000000000", "ts": "1700000000000"},
        "open-interest": {"instType": "SWAP", "instId": "BTC-USDT-SWAP", "oi": "100", "oiCcy": "1", "oiUsd": "50000", "ts": "1700000000000"},
        "liquidation-orders": {"instId": "BTC-USDT-SWAP", "instType": "SWAP", "instFamily": "BTC-USDT", "uly": "BTC-USDT", "details": [{"bkPx": "50000", "sz": "1", "side": "sell", "posSide": "long", "ts": "1700000000000", "bkLoss": "0", "ccy": ""}]},
    }
    assert set(fixtures) == adapter.declared_channels()
    for channel, payload in fixtures.items():
        events = adapter.normalize(
            {"arg": {"channel": channel}, "data": [payload]}, local_receive_ts=5)
        assert events, f"{channel} produced no events from a well-formed fixture"
    assert adapter.unhandled_count == 0


def test_schema_report_refuses_ambiguous_storage(tmp_path, monkeypatch):
    """An ambiguous storage layout must poison the report, not traceback.

    If both legacy and segmented representations exist for the same hour, the
    observed frame set is not provably the captured frame set, so reporting
    field structure from it would be reporting from unknown data.
    """
    from collector.collector.storage_layout import StorageCollisionError

    def exploding(*args, **kwargs):
        raise StorageCollisionError("legacy and .seg both present for 2026-09-19-19")

    monkeypatch.setattr(okx_schema_report, "iter_segments", exploding)
    report = okx_schema_report.collect(str(tmp_path))
    assert report["channels"] == {}
    assert "legacy and .seg" in report["storage_collision"]
    rendered = okx_schema_report.render(report)
    assert "STORAGE COLLISION" in rendered
    assert "Refusing to report field structure" in rendered
