"""Phase H: causal order-book state reconstruction -- adversarial tests.

Uses real ``test_replay.py`` fixtures (``_depth_frame``, `_snapshot_frame``,
``_normal_session``) and the real, unmodified ``ReplayEngine`` throughout --
per this phase's explicit instruction not to manufacture every event by
hand. Fixtures are imported directly rather than duplicated.
"""
from __future__ import annotations

from decimal import Decimal

from collector.collector.book_observation import reconstruct_book_at
from collector.collector.quality_events import BookQuality
from collector.collector.replay import FrameKind, ReplayFrame
from collector.pipeline.cross_exchange_alignment import AlignmentStatus
from collector.tests.test_replay import (
    BASE_TS,
    _depth_frame,
    _normal_session,
    _snapshot_frame,
)

# --------------------------------------------------------------------------
# Reconstruction: snapshot only, snapshot + deltas, level add/update/delete.
# --------------------------------------------------------------------------

def test_snapshot_only_reconstructs_the_snapshot_levels():
    """Binance requires a bridging diff (verified extensively this session,
    Phase 6) -- a bare snapshot with nothing buffered to bridge against
    never reaches VALID by design, not a bug. Bybit's protocol genuinely
    differs: its snapshot arrives as a WS message and needs no bridge
    (LocalBook.apply()'s generic is_snapshot branch), so it is the correct
    venue for testing this specific property."""
    import json
    snapshot = ReplayFrame(
        timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=0,
        payload=json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": BASE_TS,
                           "data": {"s": "BTCUSDT", "b": [["50000", "1.0"]],
                                     "a": [["50001", "1.0"]], "u": 1, "seq": 100}}))
    obs = reconstruct_book_at([snapshot], BASE_TS, venue="BYBIT")
    assert obs.status is AlignmentStatus.AVAILABLE
    assert obs.bids and obs.asks


def test_a_zero_quantity_delta_deletes_the_level():
    # Correct Binance bridge pattern, mirroring _normal_session(): a diff
    # buffered before the snapshot arrives, satisfying U <= lastUpdateId <= u.
    bridge_diff = _depth_frame(BASE_TS + 10, U=100, u=105, pu=99, bid="100.5", index=0)
    snapshot = _snapshot_frame(BASE_TS + 20, last_update_id=102, index=1)
    delete = ReplayFrame(
        timestamp_ms=BASE_TS + 30, kind=FrameKind.WIRE, source_index=2,
        payload='{"stream":"btcusdt@depth","data":{"U":106,"u":106,"pu":105,'
                '"b":[["100.5","0"]],"a":[]}}')
    frames = [bridge_diff, snapshot, delete]
    at_bridge = reconstruct_book_at(frames, BASE_TS + 20, venue="BINANCE")
    at_delete = reconstruct_book_at(frames, BASE_TS + 30, venue="BINANCE")
    assert Decimal("100.5") in dict(at_bridge.bids)
    assert Decimal("100.5") not in dict(at_delete.bids)


def test_snapshot_plus_deltas_reconstructs_full_session():
    session = _normal_session()
    obs = reconstruct_book_at(session, BASE_TS + 1000, venue="BINANCE")
    assert obs.status is AlignmentStatus.AVAILABLE
    assert obs.quality_state == BookQuality.VALID.value


def test_a_delta_updating_a_level_changes_its_quantity():
    session = _normal_session()
    before = reconstruct_book_at(session, BASE_TS + 20, venue="BINANCE")
    after = reconstruct_book_at(session, BASE_TS + 30, venue="BINANCE")
    # The +30 delta changes the bid price/qty (bid="100.1" per _normal_session).
    assert dict(after.bids) != dict(before.bids)


# --------------------------------------------------------------------------
# Causal boundary: mutation exactly at T, before T, after T.
# --------------------------------------------------------------------------

def test_mutation_exactly_at_observation_ts_is_included():
    session = _normal_session()
    obs = reconstruct_book_at(session, BASE_TS + 30, venue="BINANCE")
    assert obs.last_update_local_receive_ts == BASE_TS + 30


def test_mutation_one_ms_after_observation_ts_is_excluded():
    session = _normal_session()
    obs = reconstruct_book_at(session, BASE_TS + 29, venue="BINANCE")
    assert obs.last_update_local_receive_ts == BASE_TS + 10   # the prior update, not +30


# --------------------------------------------------------------------------
# Future/reverse timestamp traps: exchange time must never override
# local_receive_ts for eligibility.
# --------------------------------------------------------------------------

def test_future_exchange_timestamp_does_not_grant_early_availability():
    """A frame's causal availability comes from ReplayFrame.timestamp_ms,
    itself sourced from local_receive_ts (confirmed by reading
    ReplaySource.from_records), never from any exchange-side timestamp
    embedded in the payload. Construct a frame whose *payload* claims a
    wildly different exchange time than its local_receive_ts-derived
    timestamp_ms, and confirm only timestamp_ms governs eligibility."""
    bridge_diff = _depth_frame(BASE_TS, U=100, u=105, pu=99, index=0)
    snapshot = _snapshot_frame(BASE_TS + 1, last_update_id=102, index=1)
    # This frame's timestamp_ms (its causal availability) is BASE_TS + 5000,
    # regardless of anything the payload might claim about exchange time.
    late_frame = _depth_frame(BASE_TS + 5_000, U=106, u=106, pu=105, bid="99.0", index=2)
    frames = [bridge_diff, snapshot, late_frame]

    at_base = reconstruct_book_at(frames, BASE_TS + 1, venue="BINANCE")
    at_late = reconstruct_book_at(frames, BASE_TS + 5_000, venue="BINANCE")

    assert Decimal("99.0") not in dict(at_base.bids)   # not yet causally available
    assert Decimal("99.0") in dict(at_late.bids)        # available once its timestamp_ms is reached


# --------------------------------------------------------------------------
# Duplicates, gaps, quality-state visibility.
# --------------------------------------------------------------------------

def test_a_duplicate_delta_does_not_double_apply_the_quantity():
    bridge_diff = _depth_frame(BASE_TS, U=100, u=105, pu=99, index=0)
    snapshot = _snapshot_frame(BASE_TS + 1, last_update_id=102, index=1)
    delta = _depth_frame(BASE_TS + 10, U=106, u=106, pu=105, bid="99.5", index=2)
    duplicate = _depth_frame(BASE_TS + 11, U=106, u=106, pu=105, bid="99.5", index=3)
    session = [bridge_diff, snapshot, delta, duplicate]   # same update_id, resent
    obs = reconstruct_book_at(session, BASE_TS + 20, venue="BINANCE")
    assert dict(obs.bids)[Decimal("99.5")] == Decimal("1.0")   # not doubled
    assert obs.quality_state == BookQuality.VALID.value          # duplicate is not a fault


def test_a_sequence_gap_is_visible_as_degraded_quality_not_silently_continued():
    bridge_diff = _depth_frame(BASE_TS, U=100, u=105, pu=99, index=0)
    snapshot = _snapshot_frame(BASE_TS + 1, last_update_id=102, index=1)
    # pu=200 with no bridging update in between: a genuine break.
    broken = _depth_frame(BASE_TS + 10, U=201, u=210, pu=200, bid="200.0", index=2)
    obs = reconstruct_book_at([bridge_diff, snapshot, broken], BASE_TS + 20, venue="BINANCE")
    assert obs.quality_state != BookQuality.VALID.value
    assert obs.status is AlignmentStatus.AVAILABLE   # still reported, not hidden -- degraded, not missing


# --------------------------------------------------------------------------
# NEVER_OBSERVED: no causal frames at all, vs frames-but-no-applied-event.
# --------------------------------------------------------------------------

def test_no_causal_frames_at_all_is_never_observed():
    session = _normal_session()
    obs = reconstruct_book_at(session, BASE_TS - 1, venue="BINANCE")
    assert obs.status is AlignmentStatus.NEVER_OBSERVED
    assert obs.frames_considered == 0
    assert obs.bids == () and obs.asks == ()


def test_only_a_malformed_frame_is_never_observed_but_did_consider_frames():
    malformed = ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=0,
                            payload="{not valid json", decode_ok=False)
    obs = reconstruct_book_at([malformed], BASE_TS, venue="BINANCE")
    assert obs.status is AlignmentStatus.NEVER_OBSERVED
    assert obs.frames_considered == 1   # distinct from zero frames: there WAS provenance


# --------------------------------------------------------------------------
# Staleness.
# --------------------------------------------------------------------------

def test_staleness_boundary_is_respected():
    session = _normal_session()
    # _normal_session()'s last successfully applied update is at BASE_TS+40.
    fresh = reconstruct_book_at(session, BASE_TS + 40 + 4_999, venue="BINANCE", staleness_ms=5_000)
    stale = reconstruct_book_at(session, BASE_TS + 40 + 5_001, venue="BINANCE", staleness_ms=5_000)
    assert fresh.status is AlignmentStatus.AVAILABLE
    assert stale.status is AlignmentStatus.STALE
    # A stale observation still retains the reconstructed book -- never
    # discarded, never zeroed.
    assert stale.bids and stale.asks
    assert stale.quality_state == fresh.quality_state


# --------------------------------------------------------------------------
# Replay/observation equivalence and determinism.
# --------------------------------------------------------------------------

def test_replaying_the_same_session_twice_gives_identical_observation():
    session = _normal_session()
    a = reconstruct_book_at(list(session), BASE_TS + 1000, venue="BINANCE")
    b = reconstruct_book_at(list(session), BASE_TS + 1000, venue="BINANCE")
    assert a.bids == b.bids and a.asks == b.asks and a.quality_state == b.quality_state


def test_frame_input_order_does_not_affect_the_result():
    """ReplaySource sorts by order_key internally; feeding frames in a
    different list order must not change the outcome."""
    session = _normal_session()
    forward = reconstruct_book_at(list(session), BASE_TS + 1000, venue="BINANCE")
    backward = reconstruct_book_at(list(reversed(session)), BASE_TS + 1000, venue="BINANCE")
    assert forward.bids == backward.bids and forward.asks == backward.asks


# --------------------------------------------------------------------------
# Purity: the source frames are never mutated by reconstruction.
# --------------------------------------------------------------------------

def test_source_frames_are_not_mutated_by_reconstruction():
    session = _normal_session()
    payloads_before = [f.payload for f in session]
    reconstruct_book_at(session, BASE_TS + 1000, venue="BINANCE")
    payloads_after = [f.payload for f in session]
    assert payloads_before == payloads_after


# --------------------------------------------------------------------------
# Identity.
# --------------------------------------------------------------------------

def test_reconstructed_observation_carries_the_correct_instrument_identity():
    from collector.collector.instrument import BINANCE_USDM_BTCUSDT
    session = _normal_session()
    obs = reconstruct_book_at(session, BASE_TS + 1000, venue="BINANCE")
    assert obs.instrument == BINANCE_USDM_BTCUSDT


# --------------------------------------------------------------------------
# Type validation.
# --------------------------------------------------------------------------

def test_bool_observation_ts_is_rejected():
    import pytest
    with pytest.raises(TypeError):
        reconstruct_book_at(_normal_session(), True, venue="BINANCE")


def test_float_observation_ts_is_rejected():
    import pytest
    with pytest.raises(TypeError):
        reconstruct_book_at(_normal_session(), float(BASE_TS), venue="BINANCE")


# --------------------------------------------------------------------------
# Purity: no wall clock / network in the module.
# --------------------------------------------------------------------------

def test_module_has_no_wall_clock_or_network_imports():
    import ast
    import inspect
    import collector.collector.book_observation as module
    tree = ast.parse(inspect.getsource(module))
    forbidden = {"time", "datetime", "socket", "requests", "aiohttp", "websockets", "random"}
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert not (imported & forbidden), f"forbidden imports: {imported & forbidden}"
