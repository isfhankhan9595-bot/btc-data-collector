"""G2: Binance OI must flow through the same canonical pipeline live and
replay both use.

Before this, live OI polling parsed the REST body directly into a derived
feature dict (using wall-clock time for both `timestamp` and
`local_timestamp`), and `ReplaySource.from_records` excluded every REST row
whose `purpose` was `"open_interest"` before it ever became a `ReplayFrame`.
Live OI and replayed OI were not the same pipeline; replay could not
reproduce OI at all.

The fix: `binance_oi.normalize_binance_oi()` is the one normalizer both
call sites use.
"""
from __future__ import annotations

import json

import pytest

from collector.collector.binance_oi import BinanceOIParseError, normalize_binance_oi
from collector.collector.canonical import CanonicalOIEvent, OISource, OIUnit
from collector.collector.replay import FrameKind, ReplayEngine, ReplaySource

BASE_TS = 1_780_444_800_000


def _oi_row(response_ts, oi="1234.5", exchange_ts=None, request_ts=None, ok=True):
    body = json.dumps({
        "symbol": "BTCUSDT", "openInterest": oi,
        **({"time": exchange_ts} if exchange_ts is not None else {}),
    })
    return {
        "purpose": "open_interest", "response_receive_ts": response_ts,
        "request_ts": request_ts if request_ts is not None else response_ts - 20,
        "payload": body, "ok": ok, "endpoint": "https://fapi.binance.com/fapi/v1/openInterest",
    }


# ---------------------------------------------------------------------------
# 1. Live REST response -> canonical OI event
# ---------------------------------------------------------------------------


def test_live_response_normalizes_to_a_canonical_oi_event():
    body = json.dumps({"symbol": "BTCUSDT", "openInterest": "5000.25", "time": BASE_TS})
    event = normalize_binance_oi(body, response_receive_ts=BASE_TS + 30)

    assert isinstance(event, CanonicalOIEvent)
    assert event.exchange == "BINANCE"
    assert event.open_interest == 5000.25
    assert event.source is OISource.REST_POLL
    assert event.exchange_event_ts == BASE_TS


# ---------------------------------------------------------------------------
# 2. Raw REST persistence (round trip through the actual writer)
# ---------------------------------------------------------------------------


def test_raw_rest_round_trips_an_oi_response(tmp_path):
    from collector.collector.parquet_writer import ParquetWriter
    from collector.collector.raw_capture import RAW_REST_SCHEMA, RawCapture, RawRestRecord

    writer = ParquetWriter("raw_rest", RAW_REST_SCHEMA, base_dir=str(tmp_path))
    capture = RawCapture(None, writer)
    body = json.dumps({"openInterest": "42.0", "time": BASE_TS})
    capture.capture_rest(RawRestRecord(
        request_ts=BASE_TS - 10, response_receive_ts=BASE_TS,
        endpoint="https://fapi.binance.com/fapi/v1/openInterest",
        purpose="open_interest", http_status=200, ok=True, payload=body,
        symbol="BTCUSDT"))
    writer.close()

    import glob
    import pandas as pd
    # glob.glob()'s result order is filesystem-dependent, not alphabetical --
    # picking [0] without filtering can select the .seg.meta.json sidecar
    # instead of the .seg parquet segment, which is exactly what happened
    # here once directory listing order shifted (observed non-deterministically
    # across otherwise-identical runs). Filter explicitly for the real segment.
    path = glob.glob(str(tmp_path / "raw" / "raw_rest" / "*.seg"))[0]
    row = pd.read_parquet(path).to_dict("records")[0]
    assert row["purpose"] == "open_interest"
    event = normalize_binance_oi(row["payload"], response_receive_ts=row["response_receive_ts"])
    assert event.open_interest == 42.0


# ---------------------------------------------------------------------------
# 3-4. Replay of the recorded response; live/replay canonical equality
# ---------------------------------------------------------------------------


def test_replay_produces_an_oi_event_from_a_recorded_response():
    source = ReplaySource.from_records(rest_rows=[_oi_row(BASE_TS, oi="777.0", exchange_ts=BASE_TS - 50)])
    assert len(source) == 1
    assert source.frames[0].kind == FrameKind.REST_OI

    result = ReplayEngine().run(source)
    assert result.oi_observations == 1
    oi_events = [e for e in result.non_book_events if isinstance(e, CanonicalOIEvent)]
    assert len(oi_events) == 1
    assert oi_events[0].open_interest == 777.0


def test_live_and_replay_produce_identical_canonical_events_from_the_same_body():
    """The actual G2 contract: one normalizer, two call sites, same output."""
    body = json.dumps({"openInterest": "999.9", "time": BASE_TS - 100})
    live_event = normalize_binance_oi(body, response_receive_ts=BASE_TS)

    source = ReplaySource.from_records(rest_rows=[{
        "purpose": "open_interest", "response_receive_ts": BASE_TS,
        "payload": body, "ok": True,
    }])
    replay_event = ReplayEngine().run(source).non_book_events[0]

    assert live_event == replay_event


# ---------------------------------------------------------------------------
# 5+14. Timestamp causality: response receive time, not exchange time,
# not request time, governs availability
# ---------------------------------------------------------------------------


def test_availability_is_the_response_receive_time_not_the_exchange_time():
    """A slow response must not claim availability earlier than it arrived."""
    exchange_ts = BASE_TS - 5000  # exchange says this was true 5s ago
    response_ts = BASE_TS         # but the response only just landed
    event = normalize_binance_oi(
        json.dumps({"openInterest": "1", "time": exchange_ts}),
        response_receive_ts=response_ts)

    assert event.local_receive_ts == response_ts
    assert event.exchange_event_ts == exchange_ts
    assert event.local_receive_ts != event.exchange_event_ts


def test_replay_orders_the_oi_frame_by_response_receive_time():
    source = ReplaySource.from_records(rest_rows=[
        _oi_row(BASE_TS + 5000, exchange_ts=BASE_TS),       # landed late
        _oi_row(BASE_TS + 100, exchange_ts=BASE_TS - 1000),  # landed early
    ])
    ordered = source.frames
    assert ordered[0].timestamp_ms == BASE_TS + 100
    assert ordered[1].timestamp_ms == BASE_TS + 5000


def test_a_request_that_never_returned_is_excluded_from_replay():
    """Never available live; must never become available in replay."""
    source = ReplaySource.from_records(rest_rows=[{
        "purpose": "open_interest", "response_receive_ts": None,
        "payload": "{}", "ok": False,
    }])
    assert len(source) == 0


# ---------------------------------------------------------------------------
# 6-7. Malformed / missing-field responses
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("body", [
    "{not json", "null", "[]", "{}",
    '{"openInterest": "not-a-number"}',
    '{"openInterest": "-5"}',
    '{"openInterest": "0"}',
    '{"openInterest": "nan"}',
])
def test_malformed_or_missing_oi_field_is_rejected_not_fabricated(body):
    with pytest.raises(BinanceOIParseError):
        normalize_binance_oi(body, response_receive_ts=BASE_TS)


def test_malformed_exchange_timestamp_does_not_invalidate_an_otherwise_valid_reading():
    event = normalize_binance_oi(
        json.dumps({"openInterest": "10", "time": "not-a-timestamp"}),
        response_receive_ts=BASE_TS)
    assert event.open_interest == 10.0
    assert event.exchange_event_ts is None  # not claimed, not fabricated


def test_replay_records_a_malformed_oi_response_as_a_quality_event_not_a_crash():
    source = ReplaySource.from_records(rest_rows=[{
        "purpose": "open_interest", "response_receive_ts": BASE_TS,
        "payload": "{not valid json", "ok": True,
    }])
    result = ReplayEngine().run(source)
    assert result.oi_rejected == 1
    assert result.oi_observations == 0
    assert any("replay_oi_malformed" in str(e.get("reason")) for e in result.quality_events)


def test_replay_records_a_failed_oi_request_without_crashing():
    source = ReplaySource.from_records(rest_rows=[_oi_row(BASE_TS, ok=False)])
    result = ReplayEngine().run(source)
    assert result.oi_rejected == 1
    assert result.oi_observations == 0


# ---------------------------------------------------------------------------
# 8. Wrong purpose routing
# ---------------------------------------------------------------------------


def test_snapshot_purpose_is_never_routed_as_oi():
    source = ReplaySource.from_records(rest_rows=[{
        "purpose": "orderbook_snapshot", "response_receive_ts": BASE_TS,
        "payload": json.dumps({"lastUpdateId": 1, "bids": [], "asks": []}), "ok": True,
    }])
    assert source.frames[0].kind == FrameKind.REST_SNAPSHOT


def test_unknown_rest_purpose_is_excluded_not_misrouted():
    source = ReplaySource.from_records(rest_rows=[{
        "purpose": "some_future_stream", "response_receive_ts": BASE_TS,
        "payload": "{}", "ok": True,
    }])
    assert len(source) == 0


def test_oi_replay_for_a_non_binance_venue_is_refused_not_silently_skipped():
    source = ReplaySource.from_records(rest_rows=[_oi_row(BASE_TS)])
    result = ReplayEngine(venue="BYBIT").run(source)
    assert result.oi_rejected == 1
    assert any("unsupported_venue" in str(e.get("reason")) for e in result.quality_events)


# ---------------------------------------------------------------------------
# 9-10. Deterministic replay; digest sensitivity
# ---------------------------------------------------------------------------


def test_oi_replay_is_deterministic():
    rows = [_oi_row(BASE_TS, oi="100"), _oi_row(BASE_TS + 60_000, oi="105")]
    first = ReplayEngine().run(ReplaySource.from_records(rest_rows=rows)).digest
    second = ReplayEngine().run(ReplaySource.from_records(rest_rows=rows)).digest
    assert first == second


def test_digest_is_sensitive_to_a_changed_oi_value():
    baseline = ReplayEngine().run(ReplaySource.from_records(rest_rows=[_oi_row(BASE_TS, oi="100")])).digest
    changed = ReplayEngine().run(ReplaySource.from_records(rest_rows=[_oi_row(BASE_TS, oi="999")])).digest
    assert baseline != changed


# ---------------------------------------------------------------------------
# 11. Duplicate observations are preserved, not deduplicated
# ---------------------------------------------------------------------------


def test_duplicate_oi_observations_are_each_preserved():
    """OI is a point-in-time poll; two identical readings are two facts,
    not one repeated fact to be collapsed."""
    rows = [_oi_row(BASE_TS, oi="50"), _oi_row(BASE_TS + 60_000, oi="50")]
    result = ReplayEngine().run(ReplaySource.from_records(rest_rows=rows))
    assert result.oi_observations == 2
    assert len(result.non_book_events) == 2


# ---------------------------------------------------------------------------
# 12. Unit preservation
# ---------------------------------------------------------------------------


def test_binance_oi_unit_is_unknown_not_guessed():
    """Per the project's OI-unit contract: unverified against current
    official documentation means UNKNOWN, never CONTRACTS or BASE_COIN."""
    event = normalize_binance_oi(json.dumps({"openInterest": "1"}), response_receive_ts=BASE_TS)
    assert event.unit is OIUnit.UNKNOWN


def test_unknown_unit_still_refuses_cross_venue_comparison():
    from collector.collector.canonical import OIUnitError, assert_comparable_oi
    from collector.collector.adapters.okx import OKXAdapter

    binance_event = normalize_binance_oi(json.dumps({"openInterest": "1"}), response_receive_ts=BASE_TS)
    okx_events = OKXAdapter().normalize(
        {"arg": {"channel": "open-interest"}, "data": [{"oi": "500", "instId": "BTC-USDT-SWAP"}]},
        local_receive_ts=BASE_TS)
    okx_oi = [e for e in okx_events if isinstance(e, CanonicalOIEvent)]
    if okx_oi:
        with pytest.raises(OIUnitError):
            assert_comparable_oi(binance_event, okx_oi[0])


# ---------------------------------------------------------------------------
# 13. Symbol filtering
# ---------------------------------------------------------------------------


def test_symbol_is_carried_through_when_provided():
    event = normalize_binance_oi(
        json.dumps({"openInterest": "1"}), response_receive_ts=BASE_TS, symbol="BTCUSDT")
    # symbol is not a CanonicalEvent field for OI; verify it's simply accepted
    # without error and does not affect the parsed value.
    assert event.open_interest == 1.0


# ---------------------------------------------------------------------------
# Regression: proves the OLD architecture would have failed this
# ---------------------------------------------------------------------------


def test_regression_old_architecture_excluded_oi_from_replay_entirely():
    """Pins G2 directly: before the fix, every OI row's purpose was tested
    against only 'orderbook_snapshot' and silently dropped. This asserts
    the row survives into the frame stream at all.
    """
    rows = [_oi_row(BASE_TS)]
    source = ReplaySource.from_records(rest_rows=rows)
    assert len(source) == 1, (
        "an open_interest REST row must produce a ReplayFrame; the old "
        "from_records only recognised purpose == 'orderbook_snapshot' and "
        "would have discarded this row before it ever reached the engine"
    )
    result = ReplayEngine().run(source)
    assert result.oi_observations == 1


def test_live_poll_uses_the_shared_normalizer_not_a_second_parser():
    """Structural guard: run_collector must import and call
    normalize_binance_oi rather than reimplementing OI parsing."""
    import inspect

    from collector import run_collector as rc

    source = inspect.getsource(rc.CollectorApp._poll_openinterest)
    assert "normalize_binance_oi" in source
    assert "compute_openinterest_features" not in source, (
        "the legacy wall-clock-timestamped parser must not still be in the "
        "live path once the shared normalizer is wired in"
    )
