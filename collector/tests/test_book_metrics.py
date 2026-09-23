"""derive_book_metrics: spread, mid, and microprice from a real
BookObservation. Uses the real, unmodified reconstruct_book_at throughout
(same fixture builders as test_book_observation.py) -- this module adds
zero new causal logic, so its tests are about arithmetic and missingness,
not causality; the causal guarantees are Phase H's, inherited, not
re-tested here.
"""
from __future__ import annotations

import pytest

from collector.collector.book_metrics import derive_book_metrics
from collector.collector.book_observation import reconstruct_book_at
from collector.collector.instrument import BINANCE_USDM_BTCUSDT
from collector.pipeline.cross_exchange_alignment import AlignmentStatus
from collector.tests.test_replay import _depth_frame, _snapshot_frame

T = 1_000_000


def _session():
    return [
        _depth_frame(T, U=100, u=105, pu=99, bid="100.0", ask="101.0", index=0),
        _snapshot_frame(T + 10, last_update_id=102, index=1),
        _depth_frame(T + 20, U=106, u=106, pu=105, bid="100.5", ask="100.9", index=2),
    ]


def test_spread_mid_microprice_from_a_real_reconstructed_book():
    obs = reconstruct_book_at(_session(), T + 20, venue="BINANCE")
    metrics = derive_book_metrics(obs)
    assert metrics.best_bid_price == pytest.approx(100.5)
    assert metrics.best_ask_price == pytest.approx(100.9)
    assert metrics.spread == pytest.approx(0.4)
    assert metrics.mid_price == pytest.approx(100.7)
    assert metrics.spread_bps == pytest.approx(0.4 / 100.7 * 10_000)
    # microprice = (bid*ask_qty + ask*bid_qty) / (bid_qty+ask_qty); both
    # sides here have the fixture's default qty (5.0), so it reduces to
    # the plain mid -- a genuine formula check needs asymmetric sizes,
    # done in the next test.
    assert metrics.microprice == pytest.approx(100.7)
    assert metrics.instrument == BINANCE_USDM_BTCUSDT
    assert metrics.status is AlignmentStatus.AVAILABLE


def test_microprice_weights_toward_the_side_with_more_opposite_size():
    """Real Stoikov-direction check: more resting quantity at the bid
    (support) should pull microprice toward the ask, not the bid --
    verified with genuinely asymmetric sizes, not the fixed 5.0/1.0
    the shared fixture builders hardcode. Binance requires a bridging diff
    before a lone snapshot is ever applied (confirmed while writing this:
    a lone snapshot alone reconstructs to NEVER_OBSERVED), so this builds
    a real bridging diff directly rather than a lone snapshot."""
    import json
    from collector.collector.replay import FrameKind, ReplayFrame

    diff_payload = json.dumps({"stream": "btcusdt@depth@100ms",
        "data": {"e": "depthUpdate", "E": T, "T": T, "U": 100, "u": 105, "pu": 99,
                 "b": [["100.0", "10.0"]], "a": [["101.0", "1.0"]]}})
    diff = ReplayFrame(timestamp_ms=T, kind=FrameKind.WIRE, source_index=0, payload=diff_payload)
    snap_payload = json.dumps({"lastUpdateId": 102, "bids": [["99.0", "1.0"]], "asks": [["102.0", "1.0"]]})
    snap = ReplayFrame(timestamp_ms=T + 5, kind=FrameKind.REST_SNAPSHOT, source_index=1,
                       payload=snap_payload, http_ok=True, endpoint="https://fapi.binance.com/fapi/v1/depth")
    obs = reconstruct_book_at([diff, snap], T + 5, venue="BINANCE")
    metrics = derive_book_metrics(obs)
    assert metrics.best_bid_price == pytest.approx(100.0)
    assert metrics.best_ask_price == pytest.approx(101.0)
    mid = (100.0 + 101.0) / 2.0
    assert metrics.microprice > mid   # heavier bid size (10.0) is support, pulling price toward the ask
    expected = (100.0 * 1.0 + 101.0 * 10.0) / 11.0
    assert metrics.microprice == pytest.approx(expected)


def test_never_observed_book_yields_no_metrics_not_zero():
    obs = reconstruct_book_at([], T, venue="BINANCE")
    metrics = derive_book_metrics(obs)
    assert metrics.status is AlignmentStatus.NEVER_OBSERVED
    assert metrics.spread is None
    assert metrics.mid_price is None
    assert metrics.microprice is None
    assert metrics.best_bid_price is None
    assert metrics.best_ask_price is None


def test_one_sided_book_reports_that_side_but_no_spread():
    """A book with only bids (or only asks) reported -- LocalBook permits
    this once the other side's only level is deleted via qty=0 (confirmed
    reachable this way; a lone one-sided snapshot is rejected outright by
    Binance's bridging requirement, so this establishes a normal two-sided
    book first, then deletes the ask side with a qty=0 diff)."""
    import json
    from collector.collector.replay import FrameKind, ReplayFrame

    diff_payload = json.dumps({"stream": "btcusdt@depth@100ms",
        "data": {"e": "depthUpdate", "E": T, "T": T, "U": 100, "u": 105, "pu": 99,
                 "b": [["100.0", "5.0"]], "a": [["101.0", "5.0"]]}})
    bridge = ReplayFrame(timestamp_ms=T, kind=FrameKind.WIRE, source_index=0, payload=diff_payload)
    snap_payload = json.dumps({"lastUpdateId": 102, "bids": [["100.0", "5.0"]], "asks": [["101.0", "5.0"]]})
    snap = ReplayFrame(timestamp_ms=T + 5, kind=FrameKind.REST_SNAPSHOT, source_index=1,
                       payload=snap_payload, http_ok=True, endpoint="https://fapi.binance.com/fapi/v1/depth")
    delete_ask_payload = json.dumps({"stream": "btcusdt@depth@100ms",
        "data": {"e": "depthUpdate", "E": T + 10, "T": T + 10, "U": 106, "u": 106, "pu": 105,
                 "b": [], "a": [["101.0", "0.0"]]}})  # qty 0 deletes the level
    delete_ask = ReplayFrame(timestamp_ms=T + 10, kind=FrameKind.WIRE, source_index=2,
                             payload=delete_ask_payload)
    obs = reconstruct_book_at([bridge, snap, delete_ask], T + 10, venue="BINANCE")
    metrics = derive_book_metrics(obs)
    assert metrics.best_bid_price == pytest.approx(100.0)
    assert metrics.best_ask_price is None
    assert metrics.spread is None
    assert metrics.mid_price is None
    assert metrics.microprice is None


def test_spread_is_always_strictly_positive_when_both_sides_present():
    """Pins the invariant this module's docstring relies on rather than
    re-validates: LocalBook._validated_maps rejects max(bids) >= min(asks),
    so a real observation with both sides can never yield spread <= 0.
    Not a new check here -- confirms the assumption against real output."""
    obs = reconstruct_book_at(_session(), T + 20, venue="BINANCE")
    metrics = derive_book_metrics(obs)
    assert metrics.spread > 0


def test_stale_book_retains_metrics_marked_stale():
    obs = reconstruct_book_at(_session(), T + 20 + 5_001, venue="BINANCE", staleness_ms=5_000)
    metrics = derive_book_metrics(obs)
    assert metrics.status is AlignmentStatus.STALE
    assert metrics.spread is not None   # retained, not discarded -- established project convention


def test_book_metrics_is_a_pure_function_no_new_causal_surface():
    """Structural check: this module never imports replay/ReplayEngine or
    frame types -- confirms by source inspection that it genuinely cannot
    re-introduce a causality bug, since it has no access to frames at all."""
    import inspect
    from collector.collector import book_metrics
    src = inspect.getsource(book_metrics)
    for forbidden in ("ReplayEngine", "ReplaySource", "ReplayFrame", "FrameKind"):
        assert forbidden not in src
