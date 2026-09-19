"""Bybit venue-aware replay, and live/replay quality-transition parity.

Two defects fixed together here (see docs/EXECUTION_STATUS.md):

1. ``ReplayEngine(venue=...)`` hardcoded ``BinanceAdapter()`` regardless of
   the ``venue`` argument, so ``ReplayEngine(venue="bybit")`` silently fed
   Bybit wire frames through the Binance adapter instead of failing loudly
   or working correctly.

2. Replay's ``_handle_wire`` only recorded a quality transition when the
   *new* state was exactly ``SEQUENCE_GAP``. Bybit's ``is_resync_signal``
   (an ``update_id`` decrease) drives the book straight from ``VALID`` to
   ``RECOVERING`` -- never through ``SEQUENCE_GAP`` -- so that transition
   was silently dropped by replay while live recorded it (see
   ``run_bybit_collector.BybitCollectorApp._apply_orderbook``, which uses
   the broader ``before is not after`` condition). Binance's comparator
   never produces ``is_resync_signal`` (see ``sequence.py``), which is
   exactly why this stayed latent through every existing Binance-only
   replay test.
"""
from __future__ import annotations

import json

import pytest

from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.book_engine import LocalBook
from collector.collector.canonical import CanonicalOrderBookEvent
from collector.collector.quality_events import BookQuality
from collector.collector.replay import FrameKind, ReplayEngine, ReplayFrame, ReplaySource

BASE_TS = 1_780_444_800_000


def _bybit_frame(ts, u, *, seq=None, is_snapshot=False, bid="100.0", ask="101.0", index=0):
    payload = json.dumps({
        "topic": "orderbook.50.BTCUSDT",
        "type": "snapshot" if is_snapshot else "delta",
        "ts": ts,
        "data": {
            "u": u, "seq": seq if seq is not None else u, "cts": ts,
            "b": [[bid, "1.0"]], "a": [[ask, "1.0"]],
        },
    })
    return ReplayFrame(timestamp_ms=ts, kind=FrameKind.WIRE, source_index=index,
                       payload=payload, connection_id="public-1")


def _bybit_session():
    """Snapshot then two clean deltas -- the Bybit equivalent of a healthy run."""
    return [
        _bybit_frame(BASE_TS + 10, u=100, is_snapshot=True, index=0),
        _bybit_frame(BASE_TS + 20, u=101, bid="100.1", index=1),
        _bybit_frame(BASE_TS + 30, u=102, bid="100.2", index=2),
    ]


# ---------------------------------------------------------------------------
# Venue-aware adapter selection
# ---------------------------------------------------------------------------


def test_replay_engine_selects_bybit_adapter_for_bybit_venue():
    engine = ReplayEngine(venue="BYBIT")
    assert isinstance(engine.adapter, BybitAdapter)
    assert not isinstance(engine.adapter, BinanceAdapter)


def test_replay_engine_selects_binance_adapter_by_default():
    engine = ReplayEngine()
    assert isinstance(engine.adapter, BinanceAdapter)


def test_replay_engine_venue_argument_is_case_insensitive():
    engine = ReplayEngine(venue="bybit")
    assert isinstance(engine.adapter, BybitAdapter)
    assert engine.venue == "BYBIT"


def test_replay_engine_rejects_unknown_venue():
    with pytest.raises(ValueError):
        ReplayEngine(venue="DERIBIT")


def test_bybit_replay_actually_advances_the_book():
    """Before the fix this ran BinanceAdapter against Bybit frames instead
    of failing -- silently producing wrong or empty output rather than a
    working Bybit replay."""
    result = ReplayEngine(venue="BYBIT").run(ReplaySource(_bybit_session()))
    assert result.final_state == BookQuality.VALID.value
    assert result.snapshots_applied == 0  # Bybit bridges via the wire, not a REST snapshot frame
    assert len(result.book_updates) == 3
    assert result.book_updates[-1].best_bid == "100.2"


def test_rest_snapshot_frame_is_refused_for_a_non_binance_venue():
    """A REST_SNAPSHOT frame recorded for Bybit would mean the data doesn't
    match Bybit's protocol -- must be refused, not parsed as if it were
    Binance's ``lastUpdateId`` shape."""
    frames = _bybit_session() + [
        ReplayFrame(timestamp_ms=BASE_TS + 40, kind=FrameKind.REST_SNAPSHOT,
                    source_index=3, payload=json.dumps({"lastUpdateId": 1, "bids": [], "asks": []})),
    ]
    result = ReplayEngine(venue="BYBIT").run(ReplaySource(frames))
    assert result.frames_unhandled == 1
    assert any("replay_rest_snapshot_not_supported_for_venue" in str(e.get("reason"))
               for e in result.quality_events)
    # The rest of the (valid) Bybit session must be unaffected.
    assert result.final_state == BookQuality.VALID.value


# ---------------------------------------------------------------------------
# Live/replay quality-transition parity: VALID -> RECOVERING without SEQUENCE_GAP
# ---------------------------------------------------------------------------


def test_bybit_resync_signal_drives_book_to_recovering_directly():
    """Sanity check on the fixture: an update_id decrease is a resync
    signal, not is_gap -- so it must reach RECOVERING without ever passing
    through SEQUENCE_GAP. If this assertion ever fails, the test below is
    no longer exercising the path the fix is for."""
    book = LocalBook("BYBIT")
    snapshot = CanonicalOrderBookEvent(
        "BYBIT", "orderbook", BASE_TS, BASE_TS, BASE_TS,
        bids=((100.0, 1.0),), asks=((101.0, 1.0),), update_id=100, is_snapshot=True,
    )
    assert book.apply(snapshot) is not None
    assert book.state.state is BookQuality.VALID

    decreased = CanonicalOrderBookEvent(
        "BYBIT", "orderbook", BASE_TS + 10, BASE_TS + 10, BASE_TS + 10,
        bids=((100.1, 1.0),), asks=((101.0, 1.0),), update_id=50, is_snapshot=False,
    )
    before = book.state.state
    book.apply(decreased)
    after = book.state.state
    assert before is BookQuality.VALID
    assert after is BookQuality.RECOVERING
    assert after is not BookQuality.SEQUENCE_GAP


def test_replay_records_the_recovering_transition_that_bypasses_sequence_gap():
    """The actual regression test: this must FAIL under the old
    ``after is SEQUENCE_GAP`` check and PASS under the fixed
    ``before is not after`` check, because the transition here is
    VALID -> RECOVERING, never touching SEQUENCE_GAP."""
    frames = [
        _bybit_frame(BASE_TS + 10, u=100, is_snapshot=True, index=0),
        _bybit_frame(BASE_TS + 20, u=101, bid="100.1", index=1),
        # update_id decreases: a Bybit resync signal, not a gap.
        _bybit_frame(BASE_TS + 30, u=50, bid="999.0", index=2),
    ]
    result = ReplayEngine(venue="BYBIT").run(ReplaySource(frames))

    recovering_transitions = [
        e for e in result.quality_events
        if e.get("new_state") == BookQuality.RECOVERING.value
        and e.get("previous_state") == BookQuality.VALID.value
    ]
    assert recovering_transitions, (
        "replay must record VALID -> RECOVERING even when the transition "
        "never passes through SEQUENCE_GAP (Bybit resync signal)"
    )
    # It must not be misreported as a plain sequence gap in the digest-bearing
    # summary field, even though its event_type groups with SEQUENCE_GAP
    # (matching live's own event_type choice for a RECOVERING landing state).
    assert result.final_state != BookQuality.VALID.value


def test_replay_records_recovery_transition_out_of_recovering_too():
    """The parity fix is symmetric: a transition landing on VALID (a
    recovery) must also be recorded, not only ones landing on a bad state.
    Old code recorded neither for Bybit; this proves both directions now
    match live's ``before is not after`` check."""
    frames = [
        _bybit_frame(BASE_TS + 10, u=100, is_snapshot=True, index=0),
        _bybit_frame(BASE_TS + 20, u=50, index=1),   # resync -> RECOVERING
        _bybit_frame(BASE_TS + 30, u=200, is_snapshot=True, index=2),  # fresh snapshot -> VALID
    ]
    result = ReplayEngine(venue="BYBIT").run(ReplaySource(frames))
    assert result.final_state == BookQuality.VALID.value

    recovered_transitions = [
        e for e in result.quality_events
        if e.get("new_state") == BookQuality.VALID.value
        and e.get("previous_state") == BookQuality.RECOVERING.value
    ]
    assert recovered_transitions, "the RECOVERING -> VALID recovery must also be recorded"


def test_replay_bybit_reconstruction_matches_a_direct_book_drive():
    """Same parity contract as the existing Binance test
    (test_replay_reconstruction_matches_a_direct_book_drive), for Bybit."""
    frames = _bybit_session()
    replayed = ReplayEngine(venue="BYBIT").run(ReplaySource(frames))

    adapter = BybitAdapter()
    book = LocalBook("BYBIT")
    direct_bests = []
    for frame in ReplaySource(frames):
        for event in adapter.normalize(json.loads(frame.payload),
                                       local_receive_ts=frame.timestamp_ms):
            applied = book.apply(event)
            if applied is not None:
                direct_bests.append(str(applied.bids[0][0]))

    assert [u.best_bid for u in replayed.book_updates] == direct_bests
    assert book.state.state.value == replayed.final_state


def test_binance_comparator_never_emits_a_resync_signal():
    """Pins the fact the whole bug hinged on: Binance's comparator has no
    path that returns ``is_resync_signal=True`` (a decreasing update_id is
    ``stale_update``, not a resync), which is exactly why the old, narrower
    replay check was never exercised by any Binance test. Exercises the
    comparator directly across every id relationship it distinguishes, so a
    future change that adds a resync path here would fail this test rather
    than silently reopening the Bybit-only blind spot."""
    from collector.collector.sequence import BinanceSequenceComparator

    comparator = BinanceSequenceComparator()
    previous = CanonicalOrderBookEvent(
        "BINANCE", "orderbook", BASE_TS, BASE_TS, BASE_TS,
        update_id=100, first_update_id=95, previous_update_id=90,
    )
    candidates = [
        dict(update_id=50, first_update_id=45, previous_update_id=100),   # id decreased
        dict(update_id=100, first_update_id=95, previous_update_id=90),  # duplicate
        dict(update_id=110, first_update_id=105, previous_update_id=None),  # pu missing
        dict(update_id=110, first_update_id=105, previous_update_id=999),  # pu mismatch
        dict(update_id=110, first_update_id=105, previous_update_id=100),  # clean continuation
    ]
    for kwargs in candidates:
        current = CanonicalOrderBookEvent("BINANCE", "orderbook", BASE_TS, BASE_TS, BASE_TS, **kwargs)
        result = comparator.check(current, previous)
        assert result.is_resync_signal is False, kwargs
