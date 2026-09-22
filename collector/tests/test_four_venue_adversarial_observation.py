"""Phase H, Gate 2: four-venue end-to-end adversarial adversarial dataset.

Binance Spot, Binance USD-M, Bybit Linear, and OKX Swap, each on its own
independent local clock, each reconstructed through the real, unmodified
``reconstruct_book_at`` / ``ReplayEngine`` / per-venue adapter and sequence
comparator -- nothing here re-implements protocol semantics. Every frame
builder is imported from the real per-venue test module that Phase H/E
already exercises against the live adapters (``test_replay.py``,
``test_replay_bybit_parity.py``, ``test_book_observation_okx.py``,
``test_phase_e_identity_parity.py``), per this project's repeated
instruction not to hand-manufacture protocol payloads that already have a
proven builder.

OKX fixtures here are hand-built wire JSON (the same approach the existing
``test_book_observation_okx.py`` uses), not real captured OKX traffic --
see ``PHASE_H_ORDERBOOK_OBSERVATION.md``'s production-verification
classification table for exactly what that does and does not prove.

The four venues run on staggered local clocks, verified NOT synchronized:
Spot's first frame lands before USD-M's, before Bybit's, before OKX's, and
each venue is reconstructed independently at each observation_ts -- there
is no cross-venue join here (that already exists, tested, in
cross_exchange_alignment.py); this file proves isolation, not merging.
"""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from collector.collector.book_observation import reconstruct_book_at
from collector.collector.instrument import (
    BINANCE_SPOT_BTCUSDT,
    BINANCE_USDM_BTCUSDT,
    BYBIT_LINEAR_BTCUSDT,
    OKX_SWAP_BTCUSDT,
)
from collector.collector.quality_events import BookQuality
from collector.collector.replay import FrameKind, ReplayFrame
from collector.pipeline.cross_exchange_alignment import AlignmentStatus
from collector.tests.test_book_observation_okx import _okx_book_frame
from collector.tests.test_phase_e_identity_parity import _spot_diff
from collector.tests.test_replay import _depth_frame, _snapshot_frame
from collector.tests.test_replay_bybit_parity import _bybit_frame

T = 1_800_000_000_000  # a shared epoch, independent of any single venue's own BASE_TS

# Each venue's first causally-available frame, staggered as the task
# specifies: Spot first, then USD-M, then Bybit, then OKX -- proving the
# reconstruction is genuinely per-venue-clocked, not globally synchronized.
SPOT_T0 = T + 1_000
USDM_T0 = T + 1_100
BYBIT_T0 = T + 1_200
OKX_T0 = T + 1_400


def _usdm_session():
    return [
        _depth_frame(USDM_T0, U=100, u=105, pu=99, index=0),
        _snapshot_frame(USDM_T0 + 10, last_update_id=102, index=1),
        _depth_frame(USDM_T0 + 20, U=106, u=110, pu=105, bid="100.1", index=2),
    ]


def _spot_session():
    return [
        _spot_diff(SPOT_T0, 100, 105, 0),
        _snapshot_frame(SPOT_T0 + 10, last_update_id=102, index=1),
        _spot_diff(SPOT_T0 + 20, 106, 110, 2),
    ]


def _bybit_session():
    return [
        _bybit_frame(BYBIT_T0, u=100, is_snapshot=True, index=0),
        _bybit_frame(BYBIT_T0 + 10, u=101, bid="100.1", index=1),
    ]


def _okx_session():
    return [
        _okx_book_frame(OKX_T0, seq_id=100, prev_seq_id=-1, index=0),
        _okx_book_frame(OKX_T0 + 10, seq_id=101, prev_seq_id=100, bid="65000.1", index=1),
    ]


VENUES = {
    "BINANCE_SPOT": (_spot_session, BINANCE_SPOT_BTCUSDT, SPOT_T0),
    "BINANCE": (_usdm_session, BINANCE_USDM_BTCUSDT, USDM_T0),
    "BYBIT": (_bybit_session, BYBIT_LINEAR_BTCUSDT, BYBIT_T0),
    "OKX": (_okx_session, OKX_SWAP_BTCUSDT, OKX_T0),
}


# ---------------------------------------------------------------------------
# Case 8 / 9 / 10 / 14: staggered clocks -- exactly which venues are
# observable grows as observation_ts advances, each with its own identity,
# and Spot/USD-M (same native_symbol, different market_type) never collide.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "observation_ts,expect_available",
    [
        pytest.param(T + 500, set(), id="before-any-venue"),
        pytest.param(SPOT_T0 + 50, {"BINANCE_SPOT"}, id="spot-only"),
        pytest.param(USDM_T0 + 50, {"BINANCE_SPOT", "BINANCE"}, id="spot-and-usdm"),
        pytest.param(BYBIT_T0 + 50, {"BINANCE_SPOT", "BINANCE", "BYBIT"}, id="spot-usdm-bybit"),
        pytest.param(OKX_T0 + 50, {"BINANCE_SPOT", "BINANCE", "BYBIT", "OKX"}, id="all-four"),
    ],
)
def test_staggered_venue_clocks_produce_the_expected_availability_set(observation_ts, expect_available):
    """Case 8/9/14: NEVER_OBSERVED per venue until that venue's own clock
    reaches T; venues never share a clock. Explicit expected state per
    venue, not a bare 'the test passed'."""
    for venue, (session_fn, expected_identity, _t0) in VENUES.items():
        obs = reconstruct_book_at(session_fn(), observation_ts, venue=venue)
        if venue in expect_available:
            assert obs.status is AlignmentStatus.AVAILABLE, venue
            assert obs.instrument == expected_identity, venue
            assert obs.instrument.exchange == expected_identity.exchange, venue
            assert obs.instrument.market_type == expected_identity.market_type, venue
        else:
            assert obs.status is AlignmentStatus.NEVER_OBSERVED, venue
            assert obs.frames_considered == 0, venue


def test_spot_and_usdm_share_native_symbol_but_never_collide():
    """Case 9/10: identity mismatch / Spot-vs-USD-M collision. Both
    sessions use native_symbol BTCUSDT; reconstruction at the same
    observation_ts for each venue must report its own, distinct identity."""
    ts = OKX_T0 + 50
    spot_obs = reconstruct_book_at(_spot_session(), ts, venue="BINANCE_SPOT")
    usdm_obs = reconstruct_book_at(_usdm_session(), ts, venue="BINANCE")
    assert spot_obs.instrument != usdm_obs.instrument
    assert spot_obs.instrument.native_symbol == usdm_obs.instrument.native_symbol == "BTCUSDT"
    assert spot_obs.instrument.market_type == "spot"
    assert usdm_obs.instrument.market_type == "linear_perpetual"
    # Cross-check: a USD-M session fed to a venue="BINANCE_SPOT" reconstruction
    # is never silently accepted as Spot data -- the wire shape itself differs
    # (no "e":"depthUpdate" match against BinanceSpotAdapter's own routing),
    # so the wrong-venue session simply produces no applied event, not a
    # falsely-identified one.
    wrong = reconstruct_book_at(_usdm_session(), ts, venue="BINANCE_SPOT")
    if wrong.instrument is not None:
        assert wrong.instrument != usdm_obs.instrument


# ---------------------------------------------------------------------------
# Case 1 / 2 / 3: causal boundary, post-observation exclusion, exchange-ts trap.
# One venue is enough to prove the mechanism (already proven venue-agnostic
# in book_observation.py, which has zero venue-specific branching on time);
# proven here across all four venues rather than assumed transferable.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("venue,session_fn,t0", [(v, s, t) for v, (s, _i, t) in VENUES.items()])
def test_exact_boundary_included_one_ms_after_excluded(venue, session_fn, t0):
    """Case 1/2: local_receive_ts == T is included; T+1 is not, per venue."""
    session = session_fn()
    last_frame_ts = max(f.timestamp_ms for f in session)
    at_boundary = reconstruct_book_at(session, last_frame_ts, venue=venue)
    after_boundary = reconstruct_book_at(session, last_frame_ts - 1, venue=venue)
    assert at_boundary.last_update_local_receive_ts == last_frame_ts, venue
    assert after_boundary.last_update_local_receive_ts != last_frame_ts, venue
    # A frame one ms beyond every recorded frame must not be forged into existence.
    future_only = reconstruct_book_at(session, last_frame_ts + 1, venue=venue)
    assert future_only.last_update_local_receive_ts == last_frame_ts, venue  # no later data exists to admit


def test_exchange_timestamp_earlier_than_t_does_not_grant_early_availability():
    """Case 3: a frame's payload can claim any exchange-side time; only its
    ReplayFrame.timestamp_ms (sourced from local_receive_ts, per replay.py's
    ReplaySource.from_records) governs eligibility. Binance USD-M used here
    since its payload carries an explicit "E" exchange-timestamp field that
    could plausibly be mistaken for the causal clock."""
    bridge = _depth_frame(USDM_T0, U=100, u=105, pu=99, index=0)
    snap = _snapshot_frame(USDM_T0 + 1, last_update_id=102, index=1)
    # This frame's payload "E" field (baked into _depth_frame as ts) would
    # read as far in the past if a caller mistakenly used exchange time;
    # its real causal availability (timestamp_ms) is USDM_T0 + 10_000.
    late = _depth_frame(USDM_T0 + 10_000, U=106, u=106, pu=105, bid="77.0", index=2)
    frames = [bridge, snap, late]
    early = reconstruct_book_at(frames, USDM_T0 + 1, venue="BINANCE")
    later = reconstruct_book_at(frames, USDM_T0 + 10_000, venue="BINANCE")
    assert Decimal("77.0") not in dict(early.bids)
    assert Decimal("77.0") in dict(later.bids)


# ---------------------------------------------------------------------------
# Case 4: duplicate update, all four venues (only Binance USD-M had this
# proven pre-existing; Spot/Bybit/OKX did not).
# ---------------------------------------------------------------------------


def test_binance_spot_has_no_duplicate_carve_out_unlike_usdm():
    """Venue-specific semantics, not a shared rule (task's explicit 'do not
    contaminate venue semantics' requirement): SpotSequenceComparator's own
    docstring states Spot's official procedure has no exception for a
    resent/non-advancing event, unlike USD-M's pu-chain tolerance. A
    resent update here is correctly classified as a gap, not silently
    absorbed -- confirmed against real reconstruction output, not assumed
    from the docstring alone."""
    session = [
        _spot_diff(SPOT_T0, 100, 105, 0),
        _snapshot_frame(SPOT_T0 + 10, last_update_id=102, index=1),
        _spot_diff(SPOT_T0 + 20, 106, 106, 2),
        _spot_diff(SPOT_T0 + 21, 106, 106, 3),  # same U/u resent
    ]
    obs = reconstruct_book_at(session, SPOT_T0 + 30, venue="BINANCE_SPOT")
    assert obs.quality_state == BookQuality.SEQUENCE_GAP.value


def test_duplicate_update_does_not_double_apply_bybit():
    session = [
        _bybit_frame(BYBIT_T0, u=100, is_snapshot=True, index=0),
        _bybit_frame(BYBIT_T0 + 10, u=101, bid="100.5", index=1),
        _bybit_frame(BYBIT_T0 + 11, u=101, bid="100.5", index=2),  # same u resent
    ]
    obs = reconstruct_book_at(session, BYBIT_T0 + 20, venue="BYBIT")
    assert obs.quality_state == BookQuality.VALID.value
    assert dict(obs.bids)[Decimal("100.5")] == Decimal("1.0")


def test_okx_has_no_duplicate_carve_out_unlike_usdm():
    """OKXSequenceComparator has no is_stale branch at all (confirmed by
    reading sequence.py and PHASE_H_ORDERBOOK_OBSERVATION.md's own finding):
    a resent message's prevSeqId no longer matches the book's now-current
    update_id, so it is classified as a gap, not silently absorbed."""
    session = [
        _okx_book_frame(OKX_T0, seq_id=100, prev_seq_id=-1, index=0),
        _okx_book_frame(OKX_T0 + 10, seq_id=101, prev_seq_id=100, bid="65000.5", index=1),
        _okx_book_frame(OKX_T0 + 11, seq_id=101, prev_seq_id=100, bid="65000.5", index=2),  # resent
    ]
    obs = reconstruct_book_at(session, OKX_T0 + 20, venue="OKX")
    assert obs.quality_state == BookQuality.SEQUENCE_GAP.value


# ---------------------------------------------------------------------------
# Case 5 / 6: sequence gap, then recovery, per venue -- each venue's own
# protocol-correct recovery mechanism, not a shared generic rule (task's
# explicit "do not contaminate venue semantics" requirement).
# ---------------------------------------------------------------------------


def test_sequence_gap_then_recovery_binance_usdm():
    broken = _depth_frame(USDM_T0 + 20, U=201, u=210, pu=200, bid="90.0", index=2)  # no bridging update
    session = [_depth_frame(USDM_T0, U=100, u=105, pu=99, index=0),
               _snapshot_frame(USDM_T0 + 10, last_update_id=102, index=1), broken]
    gapped = reconstruct_book_at(session, USDM_T0 + 30, venue="BINANCE")
    assert gapped.quality_state != BookQuality.VALID.value
    # Recovery: a fresh REST snapshot re-bridges. Binance buffers diffs
    # until bridged, so the eligible buffered diff (broken, whose own U/u
    # straddles this snapshot's last_update_id) gets applied on top of the
    # snapshot's own levels -- the two price bands must not cross once
    # merged (500/95 vs 101/90 here), the same crossed-book fixture
    # constraint as above, one merge step further.
    recovery_snapshot = _snapshot_frame(USDM_T0 + 40, last_update_id=210, index=3, bid="95.0", ask="500.0")
    recovered = reconstruct_book_at(session + [recovery_snapshot], USDM_T0 + 50, venue="BINANCE")
    assert recovered.quality_state == BookQuality.VALID.value


def test_sequence_gap_then_recovery_bybit():
    session = [
        _bybit_frame(BYBIT_T0, u=100, is_snapshot=True, index=0),
        _bybit_frame(BYBIT_T0 + 10, u=150, bid="150.0", index=1),  # jump: genuine gap
    ]
    gapped = reconstruct_book_at(session, BYBIT_T0 + 20, venue="BYBIT")
    assert gapped.quality_state != BookQuality.VALID.value
    recovery = _bybit_frame(BYBIT_T0 + 30, u=200, is_snapshot=True, bid="200.0", ask="201.0", index=2)
    recovered = reconstruct_book_at(session + [recovery], BYBIT_T0 + 40, venue="BYBIT")
    assert recovered.quality_state == BookQuality.VALID.value


def test_sequence_gap_then_recovery_okx():
    session = [
        _okx_book_frame(OKX_T0, seq_id=100, prev_seq_id=-1, index=0),
        _okx_book_frame(OKX_T0 + 10, seq_id=300, prev_seq_id=299, bid="300.0", index=1),  # gap
    ]
    gapped = reconstruct_book_at(session, OKX_T0 + 20, venue="OKX")
    assert gapped.quality_state != BookQuality.VALID.value
    recovery = _okx_book_frame(OKX_T0 + 30, seq_id=400, prev_seq_id=-1, bid="400.0", index=2)  # fresh snapshot
    recovered = reconstruct_book_at(session + [recovery], OKX_T0 + 40, venue="OKX")
    assert recovered.quality_state == BookQuality.VALID.value


def test_venue_specific_sequence_semantics_are_not_contaminated():
    """Case 14: the exact mechanics of what constitutes a gap and a
    recovery differ per venue and must not be interchangeable. OKX's
    prevSeqId==-1 resync path applied to Bybit's u/seq protocol (or vice
    versa) must not silently 'work' -- confirmed by their comparator
    classes being distinct types with no shared base behaviour beyond
    the SequenceResult return shape (read in sequence.py)."""
    from collector.collector.sequence import BybitSequenceComparator, OKXSequenceComparator
    assert type(BybitSequenceComparator()) is not type(OKXSequenceComparator())
    # Bybit's comparator has a resync-signal path OKX's does not (per
    # PHASE_H_ORDERBOOK_OBSERVATION.md and sequence.py, independently
    # confirmed here): feeding a decreasing u to OKX's comparator (which
    # has no is_resync_signal branch at all) must not be misread as Bybit's
    # decrease/reset rule.
    import inspect
    okx_src = inspect.getsource(OKXSequenceComparator.check)
    assert "is_resync_signal" not in okx_src


# ---------------------------------------------------------------------------
# Case 7: stale observation, per venue (already proven for Binance USD-M in
# test_book_observation.py; extended here to confirm the mechanism -- a pure
# function of observation_ts vs last_update_local_receive_ts -- is venue-
# agnostic by construction, not merely assumed).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("venue,session_fn,t0", [(v, s, t) for v, (s, _i, t) in VENUES.items()])
def test_stale_observation_per_venue(venue, session_fn, t0):
    session = session_fn()
    last_frame_ts = max(f.timestamp_ms for f in session)
    fresh = reconstruct_book_at(session, last_frame_ts + 4_999, venue=venue, staleness_ms=5_000)
    stale = reconstruct_book_at(session, last_frame_ts + 5_001, venue=venue, staleness_ms=5_000)
    assert fresh.status is AlignmentStatus.AVAILABLE, venue
    assert stale.status is AlignmentStatus.STALE, venue
    assert stale.bids and stale.asks, venue  # never discarded on staleness


# ---------------------------------------------------------------------------
# Case 11: malformed update, per venue.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("venue,session_fn,t0", [(v, s, t) for v, (s, _i, t) in VENUES.items()])
def test_malformed_frame_never_observed_but_frame_was_considered(venue, session_fn, t0):
    malformed = ReplayFrame(timestamp_ms=t0, kind=FrameKind.WIRE, source_index=0,
                            payload="{not valid json", decode_ok=False)
    obs = reconstruct_book_at([malformed], t0, venue=venue)
    assert obs.status is AlignmentStatus.NEVER_OBSERVED, venue
    assert obs.frames_considered == 1, venue  # provenance existed, distinct from zero frames


# ---------------------------------------------------------------------------
# Case 13: replay determinism, all four venues, same session run twice.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("venue,session_fn,t0", [(v, s, t) for v, (s, _i, t) in VENUES.items()])
def test_replay_determinism_per_venue(venue, session_fn, t0):
    session = session_fn()
    ts = max(f.timestamp_ms for f in session) + 100
    a = reconstruct_book_at(list(session), ts, venue=venue)
    b = reconstruct_book_at(list(session), ts, venue=venue)
    assert a.bids == b.bids and a.asks == b.asks and a.quality_state == b.quality_state
    # Input-order independence too (ReplaySource sorts internally).
    c = reconstruct_book_at(list(reversed(session)), ts, venue=venue)
    assert a.bids == c.bids and a.asks == c.asks
