"""Phase H, OKX coverage: causal order-book reconstruction for OKX's
seqId/prevSeqId protocol.

Mirrors tests/test_book_observation.py's structure and rigor exactly, for
the venue that file's own commit explicitly left untouched. OKX's protocol
differs from both Binance (no REST snapshot bridge -- the initial snapshot
arrives on the wire itself, ``prevSeqId == -1``, exactly like Bybit) and
Bybit (a simple ``prevSeqId == previous.update_id`` continuity check, no
resync-signal path -- see ``OKXSequenceComparator`` in sequence.py, which
never returns ``is_resync_signal``, unlike Bybit's decrease/reset rule).
Every fixture here constructs real OKX wire JSON and drives it through the
real, unmodified ``OKXAdapter``/``LocalBook``/``ReplayEngine`` via
``reconstruct_book_at`` -- nothing here re-implements OKX protocol
semantics.
"""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from collector.collector.book_observation import reconstruct_book_at
from collector.collector.instrument import OKX_SWAP_BTCUSDT
from collector.collector.quality_events import BookQuality
from collector.collector.replay import FrameKind, ReplayFrame
from collector.pipeline.cross_exchange_alignment import AlignmentStatus

BASE_TS = 1_780_777_777_000


def _okx_book_frame(ts, *, seq_id, prev_seq_id, bid="65000.0", ask="65001.0", index=0):
    payload = json.dumps({
        "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
        "data": [{
            "ts": str(ts), "seqId": seq_id, "prevSeqId": prev_seq_id,
            "bids": [[bid, "1.0"]], "asks": [[ask, "1.0"]],
        }],
    })
    return ReplayFrame(timestamp_ms=ts, kind=FrameKind.WIRE, source_index=index, payload=payload)


def _okx_session():
    """Snapshot (prevSeqId=-1) then two clean deltas -- OKX's equivalent of
    _normal_session()/the Bybit snapshot-only fixture. No REST bridge
    needed: OKX's snapshot is a wire message, like Bybit's."""
    return [
        _okx_book_frame(BASE_TS, seq_id=100, prev_seq_id=-1, index=0),
        _okx_book_frame(BASE_TS + 10, seq_id=101, prev_seq_id=100, bid="65000.1", index=1),
        _okx_book_frame(BASE_TS + 20, seq_id=102, prev_seq_id=101, bid="65000.2", index=2),
    ]


# ---------------------------------------------------------------------------
# Reconstruction: snapshot only, snapshot + deltas.
# ---------------------------------------------------------------------------


def test_okx_snapshot_only_reconstructs_the_snapshot_levels():
    """OKX needs no bridging diff (prevSeqId == -1 is self-sufficient),
    same shape of fact as Bybit, different protocol reason: OKX's own
    snapshot push carries full levels directly, verified via
    OKXSequenceComparator.check's ``current.previous_update_id == -1``
    short-circuit (sequence.py), which never requires a previous event."""
    snapshot = _okx_book_frame(BASE_TS, seq_id=100, prev_seq_id=-1)
    obs = reconstruct_book_at([snapshot], BASE_TS, venue="OKX")
    assert obs.status is AlignmentStatus.AVAILABLE
    assert obs.bids and obs.asks
    assert obs.quality_state == BookQuality.VALID.value


def test_okx_snapshot_plus_deltas_reconstructs_full_session():
    session = _okx_session()
    obs = reconstruct_book_at(session, BASE_TS + 1000, venue="OKX")
    assert obs.status is AlignmentStatus.AVAILABLE
    assert obs.quality_state == BookQuality.VALID.value
    assert dict(obs.bids)[Decimal("65000.2")] == Decimal("1.0")


def test_okx_a_delta_updating_a_level_changes_its_quantity():
    session = _okx_session()
    before = reconstruct_book_at(session, BASE_TS + 5, venue="OKX")
    after = reconstruct_book_at(session, BASE_TS + 10, venue="OKX")
    assert dict(after.bids) != dict(before.bids)


def test_okx_a_zero_quantity_delta_deletes_the_level():
    snapshot = _okx_book_frame(BASE_TS, seq_id=100, prev_seq_id=-1, bid="100.5")
    delete = _okx_book_frame(BASE_TS + 10, seq_id=101, prev_seq_id=100, bid="100.5")
    # Overwrite the delete frame's bid quantity to zero directly in payload.
    delete = ReplayFrame(timestamp_ms=BASE_TS + 10, kind=FrameKind.WIRE, source_index=1,
                         payload=json.dumps({
                             "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
                             "data": [{"ts": str(BASE_TS + 10), "seqId": 101, "prevSeqId": 100,
                                       "bids": [["100.5", "0"]], "asks": []}],
                         }))
    frames = [snapshot, delete]
    at_snapshot = reconstruct_book_at(frames, BASE_TS, venue="OKX")
    at_delete = reconstruct_book_at(frames, BASE_TS + 10, venue="OKX")
    assert Decimal("100.5") in dict(at_snapshot.bids)
    assert Decimal("100.5") not in dict(at_delete.bids)


# ---------------------------------------------------------------------------
# Causal boundary.
# ---------------------------------------------------------------------------


def test_okx_mutation_exactly_at_observation_ts_is_included():
    session = _okx_session()
    obs = reconstruct_book_at(session, BASE_TS + 10, venue="OKX")
    assert obs.last_update_local_receive_ts == BASE_TS + 10


def test_okx_mutation_one_ms_after_observation_ts_is_excluded():
    session = _okx_session()
    obs = reconstruct_book_at(session, BASE_TS + 9, venue="OKX")
    assert obs.last_update_local_receive_ts == BASE_TS   # the snapshot, not the +10 delta


def test_okx_future_exchange_timestamp_does_not_grant_early_availability():
    snapshot = _okx_book_frame(BASE_TS, seq_id=100, prev_seq_id=-1)
    # The payload's own "ts" (exchange timestamp) claims BASE_TS + 999999,
    # but this frame's causal availability (timestamp_ms) is BASE_TS + 5000
    # -- only the latter may govern eligibility.
    late_payload = json.dumps({
        "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
        "data": [{"ts": str(BASE_TS + 999_999), "seqId": 101, "prevSeqId": 100,
                  "bids": [["99.0", "1.0"]], "asks": []}],
    })
    late_frame = ReplayFrame(timestamp_ms=BASE_TS + 5_000, kind=FrameKind.WIRE,
                             source_index=1, payload=late_payload)
    frames = [snapshot, late_frame]
    at_base = reconstruct_book_at(frames, BASE_TS, venue="OKX")
    at_late = reconstruct_book_at(frames, BASE_TS + 5_000, venue="OKX")
    assert Decimal("99.0") not in dict(at_base.bids)
    assert Decimal("99.0") in dict(at_late.bids)


# ---------------------------------------------------------------------------
# Duplicates, gaps.
# ---------------------------------------------------------------------------


def test_okx_a_duplicate_delta_does_not_double_apply_the_quantity():
    """OKXSequenceComparator has no dedicated is_stale carve-out (unlike
    BinanceSequenceComparator) -- an exact resend (same seqId, same
    prevSeqId) passes the prevSeqId==previous.update_id check like any
    other event and gets re-applied. This test pins the ACTUAL behavior:
    since the resent delta sets the same absolute quantity (OKX, like the
    others, uses absolute-quantity semantics, not deltas-of-deltas), a
    resend is idempotent by the nature of the update, not because sequence.py
    detects and skips it specially. If this ever changes, this test is the
    one that should catch it."""
    snapshot = _okx_book_frame(BASE_TS, seq_id=100, prev_seq_id=-1)
    delta = _okx_book_frame(BASE_TS + 10, seq_id=101, prev_seq_id=100, bid="99.5", index=1)
    resend = _okx_book_frame(BASE_TS + 11, seq_id=101, prev_seq_id=100, bid="99.5", index=2)
    obs = reconstruct_book_at([snapshot, delta, resend], BASE_TS + 20, venue="OKX")
    assert dict(obs.bids)[Decimal("99.5")] == Decimal("1.0")   # not doubled or errored


def test_okx_a_sequence_gap_is_visible_as_degraded_quality_not_silently_continued():
    snapshot = _okx_book_frame(BASE_TS, seq_id=100, prev_seq_id=-1)
    # prevSeqId=500 with no bridging event: a genuine, unrecoverable-within-window break.
    broken = _okx_book_frame(BASE_TS + 10, seq_id=501, prev_seq_id=500, bid="1.0", index=1)
    obs = reconstruct_book_at([snapshot, broken], BASE_TS + 20, venue="OKX")
    assert obs.quality_state != BookQuality.VALID.value
    assert obs.status is AlignmentStatus.AVAILABLE   # degraded, not hidden


def test_okx_recovery_after_gap_via_fresh_snapshot_returns_to_valid():
    """OKX has no REST bridge to retry -- recovery here means a fresh wire
    snapshot (prevSeqId == -1 again), the same mechanism Bybit uses."""
    snapshot = _okx_book_frame(BASE_TS, seq_id=100, prev_seq_id=-1)
    broken = _okx_book_frame(BASE_TS + 10, seq_id=501, prev_seq_id=500, bid="1.0", index=1)
    fresh_snapshot = _okx_book_frame(BASE_TS + 20, seq_id=900, prev_seq_id=-1, bid="70000.0", ask="70001.0", index=2)
    obs = reconstruct_book_at([snapshot, broken, fresh_snapshot], BASE_TS + 30, venue="OKX")
    assert obs.quality_state == BookQuality.VALID.value
    assert Decimal("70000.0") in dict(obs.bids)


# ---------------------------------------------------------------------------
# NEVER_OBSERVED.
# ---------------------------------------------------------------------------


def test_okx_no_causal_frames_at_all_is_never_observed():
    session = _okx_session()
    obs = reconstruct_book_at(session, BASE_TS - 1, venue="OKX")
    assert obs.status is AlignmentStatus.NEVER_OBSERVED
    assert obs.frames_considered == 0


def test_okx_only_a_malformed_frame_is_never_observed_but_did_consider_frames():
    malformed = ReplayFrame(timestamp_ms=BASE_TS, kind=FrameKind.WIRE, source_index=0,
                            payload="{not valid json", decode_ok=False)
    obs = reconstruct_book_at([malformed], BASE_TS, venue="OKX")
    assert obs.status is AlignmentStatus.NEVER_OBSERVED
    assert obs.frames_considered == 1


def test_okx_only_a_gap_with_nothing_applied_is_never_observed_but_quality_visible():
    """A delta with prevSeqId=-1 never occurring first (i.e. no snapshot,
    only a broken delta) means nothing is ever applied to the book -- genuinely
    different from zero frames existing at all."""
    broken_only = _okx_book_frame(BASE_TS, seq_id=501, prev_seq_id=500)
    obs = reconstruct_book_at([broken_only], BASE_TS, venue="OKX")
    assert obs.status is AlignmentStatus.NEVER_OBSERVED
    assert obs.frames_considered == 1
    assert obs.quality_state == BookQuality.SEQUENCE_GAP.value


# ---------------------------------------------------------------------------
# Staleness.
# ---------------------------------------------------------------------------


def test_okx_staleness_boundary_is_respected():
    session = _okx_session()   # last applied update at BASE_TS + 20
    fresh = reconstruct_book_at(session, BASE_TS + 20 + 4_999, venue="OKX", staleness_ms=5_000)
    stale = reconstruct_book_at(session, BASE_TS + 20 + 5_001, venue="OKX", staleness_ms=5_000)
    assert fresh.status is AlignmentStatus.AVAILABLE
    assert stale.status is AlignmentStatus.STALE
    assert stale.bids and stale.asks
    assert stale.quality_state == fresh.quality_state


# ---------------------------------------------------------------------------
# Replay/observation equivalence and determinism.
# ---------------------------------------------------------------------------


def test_okx_replaying_the_same_session_twice_gives_identical_observation():
    session = _okx_session()
    a = reconstruct_book_at(list(session), BASE_TS + 1000, venue="OKX")
    b = reconstruct_book_at(list(session), BASE_TS + 1000, venue="OKX")
    assert a.bids == b.bids and a.asks == b.asks and a.quality_state == b.quality_state


def test_okx_frame_input_order_does_not_affect_the_result():
    session = _okx_session()
    forward = reconstruct_book_at(list(session), BASE_TS + 1000, venue="OKX")
    backward = reconstruct_book_at(list(reversed(session)), BASE_TS + 1000, venue="OKX")
    assert forward.bids == backward.bids and forward.asks == backward.asks


# ---------------------------------------------------------------------------
# Identity.
# ---------------------------------------------------------------------------


def test_okx_reconstructed_observation_carries_the_correct_instrument_identity():
    session = _okx_session()
    obs = reconstruct_book_at(session, BASE_TS + 1000, venue="OKX")
    assert obs.instrument == OKX_SWAP_BTCUSDT


# ---------------------------------------------------------------------------
# Cross-venue: OKX and Binance reconstructions never collide or interfere,
# even when driven from independently-numbered frame sequences with the
# same timestamps -- reconstruct_book_at takes a venue explicitly and never
# infers it from frame content.
# ---------------------------------------------------------------------------


def test_okx_and_binance_reconstructions_are_independent():
    from collector.tests.test_replay import _normal_session as _binance_session
    okx_obs = reconstruct_book_at(_okx_session(), BASE_TS + 1000, venue="OKX")
    binance_obs = reconstruct_book_at(_binance_session(), BASE_TS + 1000, venue="BINANCE")
    assert okx_obs.instrument == OKX_SWAP_BTCUSDT
    assert okx_obs.instrument != binance_obs.instrument
    assert okx_obs.exchange == "OKX" and binance_obs.exchange == "BINANCE"
