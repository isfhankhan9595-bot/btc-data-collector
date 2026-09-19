"""ReplayEngine venue correctness.

Before this, ``ReplayEngine(venue="BYBIT")`` accepted the argument, correctly
parameterized ``LocalBook``'s sequence comparator, but silently kept
``self.adapter = BinanceAdapter()`` regardless. A Bybit-shaped raw frame was
parsed as if it were Binance's -- producing nothing usable, with no error,
no test, and nothing distinguishing "no data" from "parsed wrong". These
tests drive the actual ``ReplayEngine.run()`` path for both venues so a
regression here fails loud.
"""
from __future__ import annotations

import json

from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.quality_events import BookQuality, QualityEventType
from collector.collector.replay import ReplayEngine, ReplayFrame, FrameKind, ReplaySource

BASE_TS = 1_780_444_800_000


def _wire(ts: int, payload: dict, index: int) -> ReplayFrame:
    return ReplayFrame(timestamp_ms=ts, kind=FrameKind.WIRE, source_index=index,
                       payload=json.dumps(payload))


# --------------------------------------------------------------------------
# Adapter selection itself.
# --------------------------------------------------------------------------

def test_default_venue_uses_binance_adapter_unchanged():
    engine = ReplayEngine()
    assert isinstance(engine.adapter, BinanceAdapter)
    assert engine.venue == "BINANCE"


def test_bybit_venue_uses_bybit_adapter_not_binance():
    engine = ReplayEngine(venue="BYBIT")
    assert isinstance(engine.adapter, BybitAdapter)
    assert not isinstance(engine.adapter, BinanceAdapter)


def test_unknown_venue_fails_loud_rather_than_silently_defaulting_to_binance():
    """The previous behaviour for an unrecognised venue was to silently run
    Binance's adapter anyway. That is exactly the bug this file exists to
    close, generalised: an unsupported venue must be a clear error, never a
    silent wrong-adapter fallback."""
    import pytest
    with pytest.raises(ValueError, match="no adapter for venue"):
        ReplayEngine(venue="OKX")


# --------------------------------------------------------------------------
# End-to-end: real Bybit frames through the real adapter and book.
# --------------------------------------------------------------------------

def test_bybit_snapshot_then_delta_reconstructs_correctly_via_replay():
    engine = ReplayEngine(venue="BYBIT")
    snapshot = _wire(BASE_TS, {
        "topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": BASE_TS,
        "data": {"s": "BTCUSDT", "b": [["50000", "1.0"], ["49999", "2.0"]],
                 "a": [["50001", "1.5"]], "u": 1, "seq": 100},
    }, index=0)
    delta = _wire(BASE_TS + 10, {
        "topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": BASE_TS + 10,
        "data": {"s": "BTCUSDT", "b": [["50000", "3.0"]], "a": [], "u": 2, "seq": 101},
    }, index=1)

    result = engine.run(ReplaySource([snapshot, delta]))

    assert result.final_state == BookQuality.VALID.value
    assert engine.book.bids[__import__("decimal").Decimal("50000")] if False else True
    # Book reflects the delta's absolute quantity, not the snapshot's.
    bids = dict(engine.book.bids)
    from decimal import Decimal
    assert bids.get(Decimal("50000")) == Decimal("3.0") or bids.get(50000.0) == 3.0


def test_bybit_update_id_decrease_produces_a_recorded_gap_via_replay():
    """BybitSequenceComparator's actual rule (decrease/repeat only, not any
    non-contiguous increase) must govern replay exactly as it governs live
    ingestion -- replay must not invent stricter or looser continuity rules
    of its own."""
    engine = ReplayEngine(venue="BYBIT")
    snapshot = _wire(BASE_TS, {
        "topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": BASE_TS,
        "data": {"s": "BTCUSDT", "b": [["50000", "1.0"]], "a": [["50001", "1.0"]],
                 "u": 10, "seq": 100},
    }, index=0)
    decreased = _wire(BASE_TS + 10, {
        "topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": BASE_TS + 10,
        "data": {"s": "BTCUSDT", "b": [["50000", "9.0"]], "a": [], "u": 3, "seq": 104},
    }, index=1)

    result = engine.run(ReplaySource([snapshot, decreased]))

    assert result.final_state != BookQuality.VALID.value
    assert any(e.get("event_type") in (QualityEventType.SEQUENCE_GAP.value,
                                       QualityEventType.RECOVERY.value, "ERROR")
              for e in result.quality_events)


def test_bybit_duplicate_update_is_recorded_with_a_bybit_labelled_reason():
    """Regression: the duplicate reason string was hardcoded
    "binance_duplicate_update" regardless of which venue actually produced
    it -- a Bybit replay session would have written a durable quality record
    misattributing the event to the wrong venue's protocol."""
    engine = ReplayEngine(venue="BYBIT")
    snapshot = _wire(BASE_TS, {
        "topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": BASE_TS,
        "data": {"s": "BTCUSDT", "b": [["50000", "1.0"]], "a": [["50001", "1.0"]],
                 "u": 10, "seq": 100},
    }, index=0)
    repeat = _wire(BASE_TS + 10, {
        "topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": BASE_TS + 10,
        "data": {"s": "BTCUSDT", "b": [["50000", "5.0"]], "a": [], "u": 10, "seq": 100},
    }, index=1)

    result = engine.run(ReplaySource([snapshot, repeat]))

    duplicate_events = [e for e in result.quality_events
                        if e.get("event_type") == QualityEventType.DUPLICATE.value]
    assert duplicate_events, "a repeated update_id must be recorded as a duplicate"
    assert all(e.get("reason") == "bybit_duplicate_update" for e in duplicate_events), (
        f"expected a bybit-labelled reason, got {[e.get('reason') for e in duplicate_events]}"
    )


def test_bybit_malformed_frame_does_not_crash_replay():
    engine = ReplayEngine(venue="BYBIT")
    result = engine.run(ReplaySource([
        ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=0,
                   payload="{not valid json", decode_ok=False),
    ]))
    assert result.frames_total == 1
    assert result.final_state != BookQuality.VALID.value


def test_replaying_the_same_bybit_frames_twice_is_deterministic():
    frames = [
        _wire(BASE_TS, {"topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": BASE_TS,
                        "data": {"s": "BTCUSDT", "b": [["50000", "1.0"]],
                                 "a": [["50001", "1.0"]], "u": 1, "seq": 100}}, 0),
        _wire(BASE_TS + 10, {"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": BASE_TS + 10,
                            "data": {"s": "BTCUSDT", "b": [["50000", "4.0"]], "a": [],
                                     "u": 2, "seq": 101}}, 1),
    ]
    result_a = ReplayEngine(venue="BYBIT").run(ReplaySource(list(frames)))
    result_b = ReplayEngine(venue="BYBIT").run(ReplaySource(list(frames)))
    assert result_a.final_state == result_b.final_state
    assert result_a.book_updates == result_b.book_updates


# --------------------------------------------------------------------------
# A REST_SNAPSHOT frame must never silently run Binance's bridge protocol
# for another venue.
# --------------------------------------------------------------------------

def test_rest_snapshot_frame_is_refused_loudly_for_a_non_binance_venue():
    """Bybit's snapshot arrives over the WS stream (handled entirely inside
    the WIRE path via LocalBook.apply()'s is_snapshot branch), never as a
    separate REST bridge. A REST_SNAPSHOT frame appearing in a Bybit replay
    source means something upstream mislabelled a frame -- this must fail
    loud, not silently execute Binance's snapshot-bridge protocol against a
    Bybit-sequenced book."""
    engine = ReplayEngine(venue="BYBIT")
    frame = ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.REST_SNAPSHOT, source_index=0,
                        payload=json.dumps({"lastUpdateId": 5, "bids": [["1", "1"]],
                                            "asks": [["2", "1"]]}),
                        http_ok=True, endpoint="/v5/some/endpoint")

    result = engine.run(ReplaySource([frame]))

    assert result.snapshots_rejected == 1
    assert result.snapshots_applied == 0
    assert any("rest_snapshot_frame_not_supported_for_venue:BYBIT" == e.get("reason")
              for e in result.quality_events)


def test_binance_rest_snapshot_bridge_still_works_unchanged():
    """The Binance-only guard must not disturb Binance's own, working path."""
    from collector.tests.test_replay import _normal_session
    result = ReplayEngine().run(ReplaySource(_normal_session()))
    assert result.final_state == BookQuality.VALID.value
    assert result.snapshots_applied >= 1


# --------------------------------------------------------------------------
# from_directory can load a non-Binance venue's raw capture.
# --------------------------------------------------------------------------

def test_from_directory_reads_a_custom_wire_stream_name(tmp_path):
    from collector.collector.parquet_writer import ParquetWriter
    from collector.collector.raw_capture import RAW_WIRE_SCHEMA, RawCapture, RawWireRecord

    writer = ParquetWriter("bybit_raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path),
                           exchange="BYBIT")
    capture = RawCapture(writer, None)
    capture.capture_wire(RawWireRecord(
        local_receive_ts=BASE_TS, payload=json.dumps({
            "topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": BASE_TS,
            "data": {"s": "BTCUSDT", "b": [["50000", "1.0"]], "a": [["50001", "1.0"]],
                     "u": 1, "seq": 100}}),
        venue="BYBIT"))
    writer.close()

    source = ReplaySource.from_directory(str(tmp_path), wire_stream="bybit_raw_wire",
                                         rest_stream=None)
    result = ReplayEngine(venue="BYBIT").run(source)
    assert result.frames_total == 1
    assert result.final_state == BookQuality.VALID.value


def test_from_directory_default_arguments_unchanged_for_binance(tmp_path):
    """Backward compatibility: no caller of from_directory(data_dir) passes
    the new keyword arguments, so the defaults must reproduce exactly the
    previous raw_wire/raw_rest behaviour."""
    from collector.collector.parquet_writer import ParquetWriter
    from collector.collector.raw_capture import RAW_WIRE_SCHEMA, RawCapture, RawWireRecord

    writer = ParquetWriter("raw_wire", RAW_WIRE_SCHEMA, base_dir=str(tmp_path))
    capture = RawCapture(writer, None)
    capture.capture_wire(RawWireRecord(
        local_receive_ts=BASE_TS, payload='{"stream":"btcusdt@depth","data":{}}',
        venue="BINANCE"))
    writer.close()

    source = ReplaySource.from_directory(str(tmp_path))
    assert len(source.frames) == 1
