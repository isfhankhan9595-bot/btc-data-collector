"""Adversarial tests for raw wire capture and the no-silent-discard contract.

Governing invariants:

1. A frame that arrives is recorded, even if it does not parse.
2. Capture happens before any lossy transformation.
3. A REST exchange is recorded with its body, so replay never needs the live
   exchange.
4. No code path turns a received message into nothing without leaving a
   durable record.
"""
from __future__ import annotations

import json

import pytest

from collector.collector.adapters.base import (
    UNHANDLED_BUFFER_MAX,
    ExchangeAdapter,
    UnhandledMessage,
    UnhandledReason,
)
from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.adapters.okx import OKXAdapter
from collector.collector.raw_capture import (
    DEFAULT_MAX_PAYLOAD_BYTES,
    RAW_REST_SCHEMA,
    RAW_WIRE_SCHEMA,
    RawCapture,
    RawRestRecord,
    RawWireRecord,
)


class _Writer:
    def __init__(self, fail=False):
        self.rows = []
        self.fail = fail

    def write(self, row):
        if self.fail:
            raise OSError("disk gone")
        self.rows.append(row)


# ---------------------------------------------------------------------------
# Raw wire records
# ---------------------------------------------------------------------------


def test_payload_is_preserved_byte_for_byte():
    frame = '{"stream":"btcusdt@depth@100ms","data":{"u":5,"U":1,"pu":0}}'
    row = RawWireRecord(local_receive_ts=1000, payload=frame, venue="BINANCE").to_row()
    assert row["payload"] == frame
    assert row["payload_bytes"] == len(frame.encode())
    assert row["truncated"] is False


def test_receive_timestamp_is_the_ordering_timestamp_not_capture_time():
    row = RawWireRecord(
        local_receive_ts=1000, payload="{}", venue="BINANCE", local_capture_ts=9999
    ).to_row()
    assert row["timestamp"] == 1000
    assert row["local_receive_ts"] == 1000
    assert row["local_capture_ts"] == 9999


def test_absent_venue_fields_stay_null_and_are_never_fabricated():
    row = RawWireRecord(local_receive_ts=1, payload="{}", venue="BINANCE").to_row()
    for field in ("update_id", "first_update_id", "previous_update_id",
                  "exchange_event_ts", "connection_id", "channel"):
        assert row[field] is None


def test_malformed_frame_is_captured_with_the_decode_error():
    row = RawWireRecord(
        local_receive_ts=5, payload="{not json", venue="BINANCE",
        decode_ok=False, decode_error="Expecting property name",
    ).to_row()
    assert row["decode_ok"] is False
    assert row["payload"] == "{not json"
    assert "Expecting" in row["decode_error"]


def test_oversized_payload_is_truncated_and_flagged_with_true_length():
    big = "x" * 500
    row = RawWireRecord(local_receive_ts=1, payload=big, venue="BINANCE").to_row(
        max_payload_bytes=100
    )
    assert row["truncated"] is True
    assert len(row["payload"]) <= 100
    assert row["payload_bytes"] == 500  # the real size, not the stored size


def test_every_wire_row_matches_the_declared_schema():
    row = RawWireRecord(local_receive_ts=1, payload="{}", venue="BINANCE").to_row()
    assert set(row) == set(RAW_WIRE_SCHEMA.names)


# ---------------------------------------------------------------------------
# REST records
# ---------------------------------------------------------------------------


def test_rest_body_is_preserved_so_replay_need_not_contact_the_exchange():
    body = json.dumps({"lastUpdateId": 42, "bids": [["1", "2"]], "asks": [["3", "4"]]})
    row = RawRestRecord(
        request_ts=10, response_receive_ts=20, endpoint="https://x/depth",
        purpose="orderbook_snapshot", http_status=200, ok=True, payload=body,
    ).to_row()
    assert json.loads(row["payload"])["lastUpdateId"] == 42
    assert row["request_ts"] == 10
    assert row["response_receive_ts"] == 20


def test_failed_rest_exchange_is_recorded_and_ordered_by_request_time():
    row = RawRestRecord(
        request_ts=10, response_receive_ts=None, endpoint="https://x/depth",
        purpose="orderbook_snapshot", ok=False, error="snapshot_timeout",
    ).to_row()
    assert row["ok"] is False
    assert row["error"] == "snapshot_timeout"
    # No response arrived; the request time is used rather than inventing one.
    assert row["timestamp"] == 10
    assert row["response_receive_ts"] is None


def test_request_and_response_timestamps_are_never_collapsed():
    row = RawRestRecord(
        request_ts=100, response_receive_ts=350, endpoint="e", purpose="p",
        ok=True, payload="{}", local_process_ts=400,
    ).to_row()
    assert row["request_ts"] != row["response_receive_ts"] != row["local_process_ts"]


def test_every_rest_row_matches_the_declared_schema():
    row = RawRestRecord(request_ts=1, response_receive_ts=2, endpoint="e", purpose="p").to_row()
    assert set(row) == set(RAW_REST_SCHEMA.names)


def test_unserialisable_request_params_do_not_raise():
    row = RawRestRecord(
        request_ts=1, response_receive_ts=2, endpoint="e", purpose="p",
        request_params={"obj": object()},
    ).to_row()
    assert isinstance(row["request_params"], str)


# ---------------------------------------------------------------------------
# RawCapture: fails open, never silently
# ---------------------------------------------------------------------------


def test_capture_writes_to_the_writer():
    writer = _Writer()
    capture = RawCapture(writer)
    assert capture.capture_wire(RawWireRecord(1, "{}", "BINANCE")) is True
    assert len(writer.rows) == 1
    assert capture.stats()["wire_captured"] == 1


def test_writer_failure_fails_open_and_emits_a_quality_event():
    events = []
    capture = RawCapture(_Writer(fail=True), quality_event_sink=events.append)
    assert capture.capture_wire(RawWireRecord(1, "{}", "BINANCE")) is False
    assert capture.stats()["capture_failures"] == 1
    assert events and "raw_capture_failed" in events[0]["reason"]


def test_capture_failure_never_propagates_even_without_a_sink():
    capture = RawCapture(_Writer(fail=True))
    assert capture.capture_wire(RawWireRecord(1, "{}", "BINANCE")) is False


def test_a_raising_quality_sink_cannot_break_capture():
    def bad_sink(_):
        raise RuntimeError("sink down")

    capture = RawCapture(_Writer(fail=True), quality_event_sink=bad_sink)
    assert capture.capture_wire(RawWireRecord(1, "{}", "BINANCE")) is False


def test_truncation_emits_a_quality_event_because_it_is_partial_data():
    events = []
    capture = RawCapture(_Writer(), quality_event_sink=events.append, max_payload_bytes=10)
    capture.capture_wire(RawWireRecord(1, "y" * 100, "BINANCE"))
    assert any(e["reason"] == "raw_payload_truncated" for e in events)
    assert capture.stats()["truncations"] == 1


def test_disabled_capture_writes_nothing():
    writer = _Writer()
    capture = RawCapture(writer, enabled=False)
    assert capture.capture_wire(RawWireRecord(1, "{}", "BINANCE")) is False
    assert writer.rows == []


# ---------------------------------------------------------------------------
# Adapters: no bare `return []`
# ---------------------------------------------------------------------------


def test_no_adapter_normalize_ends_in_a_bare_return_empty_list():
    """Structural guard against reintroducing the silent-discard pattern."""
    import inspect
    import re

    for adapter_cls in (BinanceAdapter, BybitAdapter, OKXAdapter):
        source = inspect.getsource(adapter_cls.normalize)
        trailing = [
            line for line in source.splitlines()
            if re.match(r"^\s*return \[\]\s*$", line)
        ]
        assert not trailing, f"{adapter_cls.__name__}.normalize has a bare return []"


@pytest.mark.parametrize(
    "adapter_cls, frame",
    [
        (BinanceAdapter, {"stream": "btcusdt@unknownChannel", "data": {"e": "mystery"}}),
        (BybitAdapter, {"topic": "unknown.BTCUSDT", "data": {}}),
        (OKXAdapter, {"arg": {"channel": "unknown-channel"}, "data": []}),
    ],
)
def test_unroutable_frames_are_recorded_not_dropped(adapter_cls, frame):
    adapter = adapter_cls()
    assert adapter.normalize(frame, local_receive_ts=1) == []
    drained = adapter.drain_unhandled()
    assert len(drained) == 1
    assert adapter.unhandled_count == 1


def test_okx_malformed_payload_for_implemented_channels_is_explicitly_classified():
    """D11 follow-up: all eight declared channel names are now implemented
    (docs/OKX_D11_CHANNEL_SCHEMAS.md). An empty/malformed payload for any of
    them must still be observable -- as MALFORMED_PAYLOAD now, not silently
    parsed into a fabricated 0.0-priced event and not (any more)
    CHANNEL_NOT_IMPLEMENTED, since implementation gaps are what that reason
    exists to flag, and there are none left to flag. liquidation-orders is
    the one exception: an empty outer object has a valid shape with zero
    entries in `details[]`, which is EMPTY_DATA (a distinct, equally
    non-silent classification), not a parse failure."""
    adapter = OKXAdapter()
    channels = sorted(adapter.declared_channels())
    assert channels, "expected OKX to declare at least one channel"
    for channel in channels:
        adapter.normalize({"arg": {"channel": channel}, "data": [{}]}, local_receive_ts=1)
    drained = adapter.drain_unhandled()
    assert len(drained) == len(channels)
    by_channel = {m.channel: m.reason for m in drained}
    assert by_channel.pop("liquidation-orders") is UnhandledReason.EMPTY_DATA
    assert all(reason is UnhandledReason.MALFORMED_PAYLOAD for reason in by_channel.values())


def test_declared_channels_minus_unimplemented_is_what_actually_works():
    adapter = OKXAdapter()
    assert adapter.implemented_channels() == adapter.declared_channels()
    assert not adapter.unimplemented_channels


def test_control_frames_are_classified_separately_from_data_loss():
    binance = BinanceAdapter()
    binance.normalize({"result": None, "id": 1}, local_receive_ts=1)
    assert binance.drain_unhandled()[0].reason is UnhandledReason.CONTROL_FRAME

    bybit = BybitAdapter()
    bybit.normalize({"op": "subscribe", "success": True}, local_receive_ts=1)
    assert bybit.drain_unhandled()[0].reason is UnhandledReason.CONTROL_FRAME

    okx = OKXAdapter()
    okx.normalize({"event": "subscribe", "arg": {}}, local_receive_ts=1)
    assert okx.drain_unhandled()[0].reason is UnhandledReason.CONTROL_FRAME


def test_okx_empty_data_array_is_distinguished_from_no_route():
    adapter = OKXAdapter()
    adapter.normalize({"arg": {"channel": "books"}, "data": []}, local_receive_ts=1)
    assert adapter.drain_unhandled()[0].reason is UnhandledReason.EMPTY_DATA


def test_unhandled_sink_receives_messages_immediately():
    seen = []
    adapter = OKXAdapter()
    adapter.set_unhandled_sink(seen.append)
    adapter.normalize({"arg": {"channel": "trades"}, "data": [{}]}, local_receive_ts=1)
    assert len(seen) == 1
    assert isinstance(seen[0], UnhandledMessage)


def test_a_raising_unhandled_sink_cannot_break_normalize():
    adapter = OKXAdapter()
    adapter.set_unhandled_sink(lambda _: (_ for _ in ()).throw(RuntimeError("boom")))
    assert adapter.normalize({"arg": {"channel": "trades"}, "data": [{}]}) == []


def test_unhandled_buffer_is_bounded_and_eviction_is_counted():
    adapter = OKXAdapter()
    for _ in range(UNHANDLED_BUFFER_MAX + 10):
        adapter.normalize({"arg": {"channel": "trades"}, "data": [{}]}, local_receive_ts=1)
    assert len(adapter.drain_unhandled()) == UNHANDLED_BUFFER_MAX
    assert adapter.unhandled_dropped == 10  # evictions are not silent either
    assert adapter.unhandled_count == UNHANDLED_BUFFER_MAX + 10


def test_unhandled_message_converts_to_a_durable_quality_event():
    adapter = OKXAdapter()
    adapter.normalize({"arg": {"channel": "funding-rate"}, "data": [{}]}, local_receive_ts=77)
    event = adapter.drain_unhandled()[0].to_quality_event()
    assert event["exchange"] == "OKX"
    assert event["event_type"] == "DATA_DROP"
    assert "malformed_payload" in event["reason"]
    assert event["rows_lost"] == 1
    assert event["local_receive_ts"] == 77


def test_unhandled_records_keys_only_not_the_payload():
    """Full payloads belong in raw capture; this record stays small."""
    adapter = OKXAdapter()
    adapter.normalize(
        {"arg": {"channel": "trades"}, "data": [{"secret": "x" * 10_000}]},
        local_receive_ts=1,
    )
    message = adapter.drain_unhandled()[0]
    assert message.payload_keys == ("arg", "data")
    assert "x" * 100 not in str(message)


def test_valid_frames_still_produce_events_and_no_unhandled_record():
    adapter = BinanceAdapter()
    events = adapter.normalize(
        {"stream": "btcusdt@aggTrade",
         "data": {"e": "aggTrade", "E": 1, "T": 1, "a": 7, "p": "100", "q": "1", "m": False}},
        local_receive_ts=2,
    )
    assert len(events) == 1
    assert adapter.drain_unhandled() == []


def test_base_adapter_exposes_the_contract():
    assert issubclass(BinanceAdapter, ExchangeAdapter)
    for adapter_cls in (BinanceAdapter, BybitAdapter, OKXAdapter):
        assert adapter_cls().venue != "UNKNOWN"


def test_default_payload_limit_is_generous_enough_for_a_depth1000_snapshot():
    assert DEFAULT_MAX_PAYLOAD_BYTES >= 1_000_000
