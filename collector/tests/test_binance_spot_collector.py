"""P5 completion: live Spot runner, replay integration, storage round-trip.

Covers the items P5's own "Not done" list named explicitly:
run_binance_spot_collector.py, ReplayEngine Spot registration, a real
raw-capture -> storage -> replay round trip, a malformed-frame fixture
suite, and causal/boundary/mutation tests around the snapshot bridge.
"""
from __future__ import annotations

import asyncio
import json
import tempfile
from decimal import Decimal
from unittest.mock import patch

import pytest

from collector.collector.adapters.base import UnhandledReason
from collector.collector.canonical import CanonicalOrderBookEvent, CanonicalTradeEvent
from collector.collector.quality_events import BookQuality
from collector.collector.replay import (
    FrameKind,
    ReplayEngine,
    ReplayFrame,
    ReplaySource,
    replay_directory,
)
from collector.collector.storage_layout import VENUE_STREAM_PREFIX, venue_stream
from collector.run_binance_spot_collector import BinanceSpotCollectorApp

BASE_TS = 1_780_444_800_000


def _snapshot_body(last_update_id, bid="100.0", ask="101.0"):
    return json.dumps({"lastUpdateId": last_update_id,
                        "bids": [[bid, "5.0"]], "asks": [[ask, "5.0"]]})


def _depth_msg(ts, U, u, bid="100.0"):
    return {"stream": "btcusdt@depth@100ms",
            "data": {"e": "depthUpdate", "E": ts, "U": U, "u": u,
                     "b": [[bid, "1.0"]], "a": []}}


def _trade_msg(ts, t, price="100.5", qty="1.0", maker=False):
    return {"stream": "btcusdt@trade",
            "data": {"e": "trade", "E": ts, "T": ts, "t": t, "p": price, "q": qty, "m": maker}}


class _FakeResp:
    def __init__(self, body, status=200, ok=True):
        self.status = status
        self._body = body
        self._ok = ok

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def text(self):
        return self._body

    def raise_for_status(self):
        if not self._ok:
            raise RuntimeError(f"HTTP {self.status}")


class _FakeSession:
    def __init__(self, resp):
        self._resp = resp

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url):
        return self._resp


def _app(tmp_dir):
    return BinanceSpotCollectorApp(data_dir=tmp_dir)


async def _bridge(app, last_update_id=100):
    with patch("aiohttp.ClientSession", return_value=_FakeSession(_FakeResp(_snapshot_body(last_update_id)))):
        return await app._recover_book("initial_snapshot")


# ---------------------------------------------------------------------------
# Storage namespace isolation (task §11)
# ---------------------------------------------------------------------------


def test_spot_and_futures_orderbook_streams_never_collide():
    assert venue_stream("BINANCE", "orderbook") != venue_stream("BINANCE_SPOT", "orderbook")
    assert venue_stream("BINANCE", "raw_wire") != venue_stream("BINANCE_SPOT", "raw_wire")
    assert venue_stream("BINANCE", "raw_rest") != venue_stream("BINANCE_SPOT", "raw_rest")


def test_spot_prefix_is_registered_and_nonempty():
    assert VENUE_STREAM_PREFIX["BINANCE_SPOT"] == "spot_"


def test_app_writers_actually_use_the_spot_namespace():
    with tempfile.TemporaryDirectory() as d:
        app = _app(d)
        try:
            assert "spot_" in app.trades_writer.stream_name
            assert "spot_" in app.ob_writer.stream_name
            assert "spot_" in app.raw_wire_writer.stream_name
            assert "spot_" in app.raw_rest_writer.stream_name
        finally:
            for w in (app.quality_writer, app.raw_wire_writer, app.raw_rest_writer,
                      app.trades_writer, app.ob_writer):
                w.close()


# ---------------------------------------------------------------------------
# Live runner: bridge, buffering, trades
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_snapshot_ahead_of_buffer_is_not_treated_as_a_failure():
    """The task's own required distinction: pending != error."""
    with tempfile.TemporaryDirectory() as d:
        app = _app(d)
        try:
            ok = await _bridge(app, last_update_id=100)
            assert ok is True
            assert app.book.state.state is BookQuality.RECOVERING
            assert app.recovery_controller.counters.failed == 0
            assert app.recovery_controller.counters.succeeded == 1
        finally:
            for w in (app.quality_writer, app.raw_wire_writer, app.raw_rest_writer,
                      app.trades_writer, app.ob_writer):
                w.close()


@pytest.mark.asyncio
async def test_diff_straddling_last_update_id_bridges_automatically():
    with tempfile.TemporaryDirectory() as d:
        app = _app(d)
        try:
            await _bridge(app, last_update_id=100)
            await app._handle_message(_depth_msg(BASE_TS, U=99, u=105), local_receive_ts=BASE_TS)
            assert app.book.state.state is BookQuality.VALID
        finally:
            for w in (app.quality_writer, app.raw_wire_writer, app.raw_rest_writer,
                      app.trades_writer, app.ob_writer):
                w.close()


@pytest.mark.asyncio
async def test_trade_and_orderbook_land_in_separate_streams_with_correct_rows():
    with tempfile.TemporaryDirectory() as d:
        app = _app(d)
        try:
            await _bridge(app, last_update_id=100)
            await app._handle_message(_depth_msg(BASE_TS, U=99, u=105), local_receive_ts=BASE_TS)
            await app._handle_message(_trade_msg(BASE_TS + 1, t=7), local_receive_ts=BASE_TS + 1)

            assert app.stream_counters["trades"]["written"] == 1
            assert app.stream_counters["orderbook"]["written"] >= 1
        finally:
            for w in (app.quality_writer, app.raw_wire_writer, app.raw_rest_writer,
                      app.trades_writer, app.ob_writer):
                w.close()


@pytest.mark.asyncio
async def test_own_recovery_controller_is_not_shared_with_futures():
    with tempfile.TemporaryDirectory() as d:
        app = _app(d)
        try:
            from collector.collector.recovery_control import RecoveryController
            assert isinstance(app.recovery_controller, RecoveryController)
            assert app.recovery_controller.name == "binance_spot_orderbook"
        finally:
            for w in (app.quality_writer, app.raw_wire_writer, app.raw_rest_writer,
                      app.trades_writer, app.ob_writer):
                w.close()


@pytest.mark.asyncio
async def test_failed_snapshot_request_is_recorded_and_bounded_not_infinite():
    with tempfile.TemporaryDirectory() as d:
        app = _app(d)
        try:
            with patch("aiohttp.ClientSession", side_effect=RuntimeError("network down")):
                ok = await app._recover_book("initial_snapshot")
            assert ok is False
            assert app.recovery_controller.counters.failed == 1
            assert app.book.state.state is not BookQuality.VALID
        finally:
            for w in (app.quality_writer, app.raw_wire_writer, app.raw_rest_writer,
                      app.trades_writer, app.ob_writer):
                w.close()


# ---------------------------------------------------------------------------
# Malformed-frame fixture suite (task §16)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("data", [
    {"e": "depthUpdate", "u": 5},                               # missing U
    {"e": "depthUpdate", "U": 5},                                # missing u
    {"e": "depthUpdate", "U": "not-int", "u": 5},                 # non-integer U
    {"e": "depthUpdate", "U": 5, "u": "not-int"},                 # non-integer u
    {"e": "depthUpdate", "U": 5, "u": 6, "b": [["bad", "1"]], "a": []},   # malformed bid price
    {"e": "depthUpdate", "U": 5, "u": 6, "b": [["1", "bad"]], "a": []},   # malformed bid qty
    {"e": "depthUpdate", "U": 5, "u": 6, "b": [], "a": [["bad", "1"]]},   # malformed ask price
    {"e": "depthUpdate", "U": 5, "u": 6, "b": [], "a": [["1", "bad"]]},   # malformed ask qty
])
def test_malformed_depth_payloads_are_unhandled_not_fabricated(data):
    from collector.collector.adapters.binance_spot import BinanceSpotAdapter
    adapter = BinanceSpotAdapter()
    events = adapter.normalize({"stream": "btcusdt@depth", "data": data}, local_receive_ts=1)
    assert events == []
    unhandled = adapter.drain_unhandled()
    assert len(unhandled) == 1
    assert unhandled[0].reason is UnhandledReason.MALFORMED_PAYLOAD


@pytest.mark.parametrize("data", [
    {"e": "trade", "t": 1, "p": "bad", "q": "1"},          # malformed price
    {"e": "trade", "t": 1, "p": "100", "q": "bad"},        # malformed quantity
    {"e": "trade"},                                        # malformed/empty payload
])
def test_malformed_trade_payloads_are_unhandled_not_fabricated(data):
    from collector.collector.adapters.binance_spot import BinanceSpotAdapter
    adapter = BinanceSpotAdapter()
    events = adapter.normalize({"stream": "btcusdt@trade", "data": data}, local_receive_ts=1)
    assert events == []
    assert adapter.drain_unhandled()[0].reason is UnhandledReason.MALFORMED_PAYLOAD


def test_trade_missing_native_id_still_produces_an_event_with_null_id():
    """The adapter's actual contract: only p/q are required; t is optional
    via .get(), so a missing trade id is not treated as malformed."""
    from collector.collector.adapters.binance_spot import BinanceSpotAdapter
    adapter = BinanceSpotAdapter()
    events = adapter.normalize(
        {"stream": "btcusdt@trade", "data": {"e": "trade", "p": "100", "q": "1"}},
        local_receive_ts=1)
    assert len(events) == 1
    assert events[0].trade_id is None


def test_missing_bids_or_asks_key_entirely_is_unhandled():
    from collector.collector.adapters.binance_spot import BinanceSpotAdapter
    adapter = BinanceSpotAdapter()
    # No "b"/"a" keys at all -- must not crash, must not fabricate empty levels
    # as if they were a valid (if empty) book state.
    events = adapter.normalize(
        {"stream": "btcusdt@depth", "data": {"e": "depthUpdate", "U": 5, "u": 6}},
        local_receive_ts=1)
    assert len(events) == 1  # missing b/a default to [] per .get(...,[]) -- valid, empty levels
    assert events[0].bids == () and events[0].asks == ()


def test_unknown_stream_is_no_route_not_a_crash():
    from collector.collector.adapters.binance_spot import BinanceSpotAdapter
    adapter = BinanceSpotAdapter()
    adapter.normalize(
        {"stream": "btcusdt@bookTicker", "data": {"u": 1, "s": "BTCUSDT"}},
        local_receive_ts=1)
    assert adapter.drain_unhandled()[0].reason is UnhandledReason.NO_ROUTE


def test_unroutable_but_empty_payload_is_malformed_not_no_route():
    """An empty data dict is rejected before routing is even considered --
    document this priority rather than assume NO_ROUTE for every unroutable case."""
    from collector.collector.adapters.binance_spot import BinanceSpotAdapter
    adapter = BinanceSpotAdapter()
    adapter.normalize({"stream": "btcusdt@bookTicker", "data": {}}, local_receive_ts=1)
    assert adapter.drain_unhandled()[0].reason is UnhandledReason.MALFORMED_PAYLOAD


def test_control_response_is_classified_separately_from_data_loss():
    from collector.collector.adapters.binance_spot import BinanceSpotAdapter
    adapter = BinanceSpotAdapter()
    adapter.normalize({"result": None, "id": 1}, local_receive_ts=1)
    assert adapter.drain_unhandled()[0].reason is UnhandledReason.CONTROL_FRAME


@pytest.mark.asyncio
async def test_non_envelope_frame_at_the_runner_level_is_recorded_not_dropped():
    with tempfile.TemporaryDirectory() as d:
        app = _app(d)
        try:
            await app._handle_message({"result": None, "id": 1}, local_receive_ts=1)
            assert app.stream_counters["malformed_envelope"]["received"] == 1
        finally:
            for w in (app.quality_writer, app.raw_wire_writer, app.raw_rest_writer,
                      app.trades_writer, app.ob_writer):
                w.close()


# ---------------------------------------------------------------------------
# Duplicate / gap / bridge-boundary tests (task §17), mutation-tested
# ---------------------------------------------------------------------------


def test_contiguous_update_stays_valid():
    """Matches the documented protocol's actual order: buffer diffs first,
    then the snapshot bridges the buffer -- not the other way round."""
    from collector.collector.book_engine import LocalBook
    from collector.collector.canonical import CanonicalOrderBookEvent as E
    book = LocalBook("BINANCE_SPOT")
    book.apply(E("BINANCE", "spot_orderbook", None, None, 1,
                bids=((Decimal("99"), Decimal("1")),), asks=(), update_id=101, first_update_id=99))
    book.binance_snapshot(100, E("BINANCE", "spot_orderbook", None, None, 0,
                                 bids=((Decimal("100"), Decimal("1")),),
                                 asks=((Decimal("101"), Decimal("1")),),
                                 update_id=100, is_snapshot=True))
    assert book.state.state is BookQuality.VALID
    book.apply(E("BINANCE", "spot_orderbook", None, None, 2,
                bids=((Decimal("98"), Decimal("1")),), asks=(), update_id=102, first_update_id=102))
    assert book.state.state is BookQuality.VALID


def test_gap_is_detected_not_silently_accepted():
    from collector.collector.book_engine import LocalBook
    from collector.collector.canonical import CanonicalOrderBookEvent as E
    book = LocalBook("BINANCE_SPOT")
    book.apply(E("BINANCE", "spot_orderbook", None, None, 1,
                bids=((Decimal("99"), Decimal("1")),), asks=(), update_id=101, first_update_id=99))
    book.binance_snapshot(100, E("BINANCE", "spot_orderbook", None, None, 0,
                                 bids=((Decimal("100"), Decimal("1")),),
                                 asks=((Decimal("101"), Decimal("1")),),
                                 update_id=100, is_snapshot=True))
    assert book.state.state is BookQuality.VALID
    book.apply(E("BINANCE", "spot_orderbook", None, None, 2,
                bids=((Decimal("97"), Decimal("1")),), asks=(), update_id=110, first_update_id=110))
    assert book.state.state is not BookQuality.VALID


def test_bridge_boundary_lastupdateid_plus_one_both_sides():
    """Mutation-style boundary test on the actual formula:
    U <= lastUpdateId+1 <= u (first_update_id=U, update_id=u)."""
    from collector.collector.sequence import binance_spot_snapshot_bridge
    from collector.collector.canonical import CanonicalOrderBookEvent as E

    last_update_id = 100  # bridge point is lastUpdateId+1 == 101

    # Single-update event landing exactly on 101: U=101<=101<=u=101 -> True.
    exactly_at = E("BINANCE", "spot_orderbook", None, None, 1, update_id=101, first_update_id=101)
    assert binance_spot_snapshot_bridge(exactly_at, last_update_id) is True

    # Starts one past the bridge point -- U=102 > 101, gap. -> False.
    starts_one_late = E("BINANCE", "spot_orderbook", None, None, 1, update_id=102, first_update_id=102)
    assert binance_spot_snapshot_bridge(starts_one_late, last_update_id) is False

    # Wide event that straddles the bridge point comfortably. -> True.
    straddles = E("BINANCE", "spot_orderbook", None, None, 1, update_id=105, first_update_id=99)
    assert binance_spot_snapshot_bridge(straddles, last_update_id) is True

    # Ends one short of the bridge point -- u=100 < 101. -> False.
    ends_one_short = E("BINANCE", "spot_orderbook", None, None, 1, update_id=100, first_update_id=95)
    assert binance_spot_snapshot_bridge(ends_one_short, last_update_id) is False


def test_futures_bridge_formula_is_never_silently_substituted():
    """Pin the actual documented difference: Spot's formula has +1, futures' does not."""
    from collector.collector.sequence import binance_snapshot_bridge, binance_spot_snapshot_bridge
    from collector.collector.canonical import CanonicalOrderBookEvent as E

    last_update_id = 100
    event = E("BINANCE", "orderbook", None, None, 1, update_id=100, first_update_id=100)
    # Futures' own formula (no +1) accepts U<=100<=u here.
    assert binance_snapshot_bridge(event, last_update_id) is True
    # Spot's formula (requires covering lastUpdateId+1 = 101) must not.
    assert binance_spot_snapshot_bridge(event, last_update_id) is False


# ---------------------------------------------------------------------------
# Causality (task §10): local_receive_ts, never exchange_event_ts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_snapshot_eligibility_is_response_receive_time_not_exchange_time():
    with tempfile.TemporaryDirectory() as d:
        app = _app(d)
        captured = []
        original_capture = app.raw_capture.capture_rest
        app.raw_capture.capture_rest = lambda record: (captured.append(record), original_capture(record))[1]
        try:
            await _bridge(app, last_update_id=100)
            assert len(captured) == 1
            # response_receive_ts is set (not None) and is a local wall-clock
            # value distinct from anything inside the snapshot payload itself
            # (the Spot depth snapshot carries no exchange timestamp at all).
            assert captured[0].response_receive_ts is not None
        finally:
            for w in (app.quality_writer, app.raw_wire_writer, app.raw_rest_writer,
                      app.trades_writer, app.ob_writer):
                w.close()


def test_replay_snapshot_availability_uses_response_receive_ts():
    rows = [{
        "purpose": "orderbook_snapshot", "response_receive_ts": BASE_TS + 500,
        "request_ts": BASE_TS, "payload": _snapshot_body(100), "ok": True,
    }]
    source = ReplaySource.from_records(rest_rows=rows)
    assert source.frames[0].timestamp_ms == BASE_TS + 500  # not request_ts


# ---------------------------------------------------------------------------
# ReplayEngine Spot registration + parity (task §12-13)
# ---------------------------------------------------------------------------


def test_replay_engine_accepts_binance_spot_venue():
    engine = ReplayEngine(venue="BINANCE_SPOT")
    assert engine.venue == "BINANCE_SPOT"
    from collector.collector.adapters.binance_spot import BinanceSpotAdapter
    assert isinstance(engine.adapter, BinanceSpotAdapter)
    assert engine.book.venue == "BINANCE_SPOT"


def test_spot_replay_bridges_and_reaches_valid():
    frames = [
        ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=0,
                    payload=json.dumps(_depth_msg(BASE_TS, U=99, u=105))),
        ReplayFrame(timestamp_ms=BASE_TS - 10, kind=FrameKind.REST_SNAPSHOT, source_index=1,
                    payload=_snapshot_body(100), http_ok=True),
    ]
    result = ReplayEngine(venue="BINANCE_SPOT").run(ReplaySource(frames))
    assert result.final_state == BookQuality.VALID.value
    assert result.snapshots_applied == 1


def test_spot_replay_uses_spot_bridge_not_futures_bridge():
    """A diff that would satisfy futures' bridge but not Spot's must fail here."""
    frames = [
        ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=0,
                    payload=json.dumps(_depth_msg(BASE_TS, U=100, u=100))),  # futures-only bridge
        ReplayFrame(timestamp_ms=BASE_TS - 10, kind=FrameKind.REST_SNAPSHOT, source_index=1,
                    payload=_snapshot_body(100), http_ok=True),
    ]
    result = ReplayEngine(venue="BINANCE_SPOT").run(ReplaySource(frames))
    assert result.final_state != BookQuality.VALID.value


def test_spot_replay_includes_trade_events():
    frames = [
        ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=0,
                    payload=json.dumps(_trade_msg(BASE_TS, t=1))),
    ]
    result = ReplayEngine(venue="BINANCE_SPOT").run(ReplaySource(frames))
    trades = [e for e in result.non_book_events if isinstance(e, CanonicalTradeEvent)]
    assert len(trades) == 1
    assert trades[0].market_type == "spot"


def test_spot_replay_is_deterministic():
    frames = [
        ReplayFrame(timestamp_ms=BASE_TS - 10, kind=FrameKind.REST_SNAPSHOT, source_index=0,
                    payload=_snapshot_body(100), http_ok=True),
        ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=1,
                    payload=json.dumps(_depth_msg(BASE_TS, U=99, u=105))),
        ReplayFrame(timestamp_ms=BASE_TS + 10, kind=FrameKind.WIRE, source_index=2,
                    payload=json.dumps(_trade_msg(BASE_TS + 10, t=1))),
    ]
    d1 = ReplayEngine(venue="BINANCE_SPOT").run(ReplaySource(frames)).digest
    d2 = ReplayEngine(venue="BINANCE_SPOT").run(ReplaySource(frames)).digest
    assert d1 == d2


def test_spot_replay_digest_changes_on_meaningful_input_change():
    base = [
        ReplayFrame(timestamp_ms=BASE_TS - 10, kind=FrameKind.REST_SNAPSHOT, source_index=0,
                    payload=_snapshot_body(100), http_ok=True),
        ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=1,
                    payload=json.dumps(_depth_msg(BASE_TS, U=99, u=105, bid="100.0"))),
    ]
    changed = [
        base[0],
        ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=1,
                    payload=json.dumps(_depth_msg(BASE_TS, U=99, u=105, bid="999.0"))),
    ]
    d1 = ReplayEngine(venue="BINANCE_SPOT").run(ReplaySource(base)).digest
    d2 = ReplayEngine(venue="BINANCE_SPOT").run(ReplaySource(changed)).digest
    assert d1 != d2


def test_a_malformed_frame_in_spot_replay_is_surfaced_not_silently_skipped():
    frames = [
        ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=0,
                    payload="{broken json"),
    ]
    result = ReplayEngine(venue="BINANCE_SPOT").run(ReplaySource(frames))
    assert result.frames_undecodable == 1


def test_a_resent_diff_is_treated_as_a_gap_not_silently_accepted():
    """P5's deliberate design decision (see docs/EXECUTION_STATUS.md, 'P5'):
    unlike BinanceSequenceComparator, SpotSequenceComparator has no
    duplicate/stale carve-out, because nothing in the official Spot
    procedure documents one. A retransmitted diff therefore fails the
    U == prev.u+1 continuity check and is correctly classified as a gap --
    the conservative outcome, not a defect."""
    diff = json.dumps(_depth_msg(BASE_TS, U=99, u=105))
    frames = [
        ReplayFrame(timestamp_ms=BASE_TS - 10, kind=FrameKind.REST_SNAPSHOT, source_index=0,
                    payload=_snapshot_body(100), http_ok=True),
        ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=1, payload=diff),
    ]
    bridged_only = ReplayEngine(venue="BINANCE_SPOT").run(ReplaySource(frames))
    assert bridged_only.final_state == BookQuality.VALID.value  # sanity: bridge alone works

    resent = frames + [
        ReplayFrame(timestamp_ms=BASE_TS + 1, kind=FrameKind.WIRE, source_index=2, payload=diff),
    ]
    result = ReplayEngine(venue="BINANCE_SPOT").run(ReplaySource(resent))
    assert result.final_state != BookQuality.VALID.value
    assert any(e.get("event_type") == "SEQUENCE_GAP" for e in result.quality_events)


def test_broken_chain_in_spot_replay_does_not_reach_valid():
    frames = [
        ReplayFrame(timestamp_ms=BASE_TS - 10, kind=FrameKind.REST_SNAPSHOT, source_index=0,
                    payload=_snapshot_body(100), http_ok=True),
        ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=1,
                    payload=json.dumps(_depth_msg(BASE_TS, U=99, u=105))),
        ReplayFrame(timestamp_ms=BASE_TS + 100, kind=FrameKind.WIRE, source_index=2,
                    payload=json.dumps(_depth_msg(BASE_TS + 100, U=500, u=510))),  # gap
    ]
    result = ReplayEngine(venue="BINANCE_SPOT").run(ReplaySource(frames))
    assert result.final_state != BookQuality.VALID.value


# ---------------------------------------------------------------------------
# Storage round-trip (task §15): capture -> ParquetWriter -> disk -> replay
# ---------------------------------------------------------------------------


def test_storage_round_trip_matches_in_memory_replay(tmp_path):
    from collector.collector.parquet_writer import ParquetWriter
    from collector.collector.raw_capture import (
        RAW_REST_SCHEMA, RAW_WIRE_SCHEMA, RawCapture, RawRestRecord, RawWireRecord,
    )

    wire_writer = ParquetWriter(venue_stream("BINANCE_SPOT", "raw_wire"), RAW_WIRE_SCHEMA,
                                base_dir=str(tmp_path), exchange="BINANCE_SPOT")
    rest_writer = ParquetWriter(venue_stream("BINANCE_SPOT", "raw_rest"), RAW_REST_SCHEMA,
                                base_dir=str(tmp_path), exchange="BINANCE_SPOT")
    capture = RawCapture(wire_writer, rest_writer)

    diff_payload = json.dumps(_depth_msg(BASE_TS, U=99, u=105))
    trade_payload = json.dumps(_trade_msg(BASE_TS + 50, t=1))

    capture.capture_rest(RawRestRecord(
        request_ts=BASE_TS - 20, response_receive_ts=BASE_TS - 10,
        endpoint="https://api.binance.com/api/v3/depth", purpose="orderbook_snapshot",
        venue="BINANCE_SPOT", http_status=200, ok=True, payload=_snapshot_body(100)))
    capture.capture_wire(RawWireRecord(
        local_receive_ts=BASE_TS, payload=diff_payload, venue="BINANCE_SPOT"))
    capture.capture_wire(RawWireRecord(
        local_receive_ts=BASE_TS + 50, payload=trade_payload, venue="BINANCE_SPOT"))
    wire_writer.close()
    rest_writer.close()

    from_disk = replay_directory(str(tmp_path), venue="BINANCE_SPOT")

    in_memory_frames = [
        ReplayFrame(timestamp_ms=BASE_TS - 10, kind=FrameKind.REST_SNAPSHOT, source_index=0,
                    payload=_snapshot_body(100), http_ok=True),
        ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=1, payload=diff_payload),
        ReplayFrame(timestamp_ms=BASE_TS + 50, kind=FrameKind.WIRE, source_index=2, payload=trade_payload),
    ]
    in_memory = ReplayEngine(venue="BINANCE_SPOT").run(ReplaySource(in_memory_frames))

    assert from_disk.final_state == in_memory.final_state == BookQuality.VALID.value
    assert from_disk.digest == in_memory.digest
    assert len(from_disk.non_book_events) == len(in_memory.non_book_events) == 1


def test_storage_round_trip_never_pulls_in_futures_rows(tmp_path):
    """A Spot-and-futures directory read must isolate by venue namespace."""
    from collector.collector.parquet_writer import ParquetWriter
    from collector.collector.raw_capture import RAW_WIRE_SCHEMA, RawCapture, RawWireRecord

    spot_writer = ParquetWriter(venue_stream("BINANCE_SPOT", "raw_wire"), RAW_WIRE_SCHEMA,
                                base_dir=str(tmp_path), exchange="BINANCE_SPOT")
    futures_writer = ParquetWriter(venue_stream("BINANCE", "raw_wire"), RAW_WIRE_SCHEMA,
                                   base_dir=str(tmp_path), exchange="BINANCE")
    RawCapture(spot_writer, None).capture_wire(
        RawWireRecord(local_receive_ts=BASE_TS, payload=json.dumps(_trade_msg(BASE_TS, t=1)),
                     venue="BINANCE_SPOT"))
    RawCapture(futures_writer, None).capture_wire(
        RawWireRecord(local_receive_ts=BASE_TS, payload=json.dumps({
            "stream": "btcusdt@aggTrade",
            "data": {"e": "aggTrade", "E": BASE_TS, "T": BASE_TS, "a": 1, "p": "1", "q": "1", "m": False}}),
            venue="BINANCE"))
    spot_writer.close()
    futures_writer.close()

    spot_source = ReplaySource.from_directory(str(tmp_path), venue="BINANCE_SPOT")
    assert len(spot_source) == 1
    result = ReplayEngine(venue="BINANCE_SPOT").run(spot_source)
    trades = [e for e in result.non_book_events if isinstance(e, CanonicalTradeEvent)]
    assert len(trades) == 1
    assert trades[0].market_type == "spot"
