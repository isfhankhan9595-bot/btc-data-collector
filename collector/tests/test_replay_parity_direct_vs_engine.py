"""Phase H, item 17: replay parity, four venues.

This repository has no second order-book implementation to compare replay
against -- ``LocalBook`` is the only reconstruction engine, and
``reconstruct_book_at``/``ReplayEngine`` is the only thing that drives it,
live or historical (confirmed: ``market_state.py`` never instantiates its
own ``LocalBook``). What genuinely differs between "live" and "replay" is
the *call site*: the live collectors (``run_collector.py``,
``run_bybit_collector.py``, ``run_binance_spot_collector.py``) each
construct their own ``LocalBook(venue)`` and drive it directly --
``events = adapter.normalize(raw, local_receive_ts=...)`` then
``book.apply(event)`` per event, one raw message at a time, as data
arrives -- while ``ReplayEngine._handle_wire`` does the same two calls
internally, sourced from ``ReplaySource`` instead of a live socket.

This file proves those two call sites, fed the exact same raw payloads in
the exact same order, produce an identical final ``LocalBook`` state --
not just best bid/ask, but the full book, quality state, sequence state,
and identity. It is the parity the architecture's single-implementation
design predicts, verified rather than assumed.
"""
from __future__ import annotations

import json

from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.adapters.binance_spot import BinanceSpotAdapter
from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.adapters.okx import OKXAdapter
from collector.collector.book_engine import LocalBook
from collector.collector.replay import FrameKind, ReplayEngine, ReplayFrame, ReplaySource
from collector.tests.test_book_observation_okx import _okx_book_frame
from collector.tests.test_phase_e_identity_parity import _spot_diff
from collector.tests.test_replay import _depth_frame, _snapshot_frame
from collector.tests.test_replay_bybit_parity import _bybit_frame

ADAPTERS = {
    "BINANCE": BinanceAdapter,
    "BINANCE_SPOT": BinanceSpotAdapter,
    "BYBIT": BybitAdapter,
    "OKX": OKXAdapter,
}


def _usdm_session():
    return [_depth_frame(1_000, U=100, u=105, pu=99, index=0),
            _snapshot_frame(1_010, last_update_id=102, index=1),
            _depth_frame(1_020, U=106, u=110, pu=105, bid="100.1", index=2)]


def _spot_session():
    return [_spot_diff(1_000, 100, 105, 0),
            _snapshot_frame(1_010, last_update_id=102, index=1),
            _spot_diff(1_020, 106, 110, 2)]


def _bybit_session():
    return [_bybit_frame(1_000, u=100, is_snapshot=True, index=0),
            _bybit_frame(1_010, u=101, bid="100.1", index=1)]


def _okx_session():
    return [_okx_book_frame(1_000, seq_id=100, prev_seq_id=-1, index=0),
            _okx_book_frame(1_010, seq_id=101, prev_seq_id=100, bid="65000.1", index=1)]


SESSIONS = {"BINANCE": _usdm_session, "BINANCE_SPOT": _spot_session,
            "BYBIT": _bybit_session, "OKX": _okx_session}


def _direct_apply(venue, frames):
    """Exactly what the live collectors do, dispatched by frame kind the
    same way ReplayEngine._handle_wire / _handle_snapshot do: WIRE frames
    go through adapter.normalize() + book.apply() (confirmed against
    run_bybit_collector.py / run_collector.py's own call sites); a Binance
    REST_SNAPSHOT frame does not (live's own snapshot fetch never calls
    adapter.normalize() on it either -- run_collector.py builds a
    snapshot_event via adapter.snapshot_event() and calls
    book.binance_snapshot() directly, mirrored here from replay.py's
    _handle_snapshot, which itself was written to call 'the same adapter
    method the live runners call')."""
    import json as _json
    from decimal import Decimal as _D
    adapter = ADAPTERS[venue]()
    book = LocalBook(venue)
    for frame in frames:
        if frame.kind == FrameKind.REST_SNAPSHOT:
            payload = _json.loads(frame.payload)
            last_update_id = int(payload["lastUpdateId"])
            bids = tuple((_D(p), _D(q)) for p, q in payload["bids"])
            asks = tuple((_D(p), _D(q)) for p, q in payload["asks"])
            snapshot_event = adapter.snapshot_event(
                last_update_id, bids, asks,
                local_receive_ts=frame.timestamp_ms, local_process_ts=frame.timestamp_ms)
            book.binance_snapshot(last_update_id, snapshot_event)
            continue
        raw = _json.loads(frame.payload)
        for event in adapter.normalize(raw, local_receive_ts=frame.timestamp_ms):
            book.apply(event)
    return book


def _via_replay_engine(venue, frames):
    engine = ReplayEngine(venue=venue)
    engine.run(ReplaySource(frames))
    return engine.book


def _full_state(book):
    """Everything the task's replay-parity list asks for that ``LocalBook``
    actually carries: bid levels, ask levels (both with quantities, since
    ``bids``/``asks`` are dicts), quality state, and sequence state
    (``previous``, whose ``update_id`` is the sequence position)."""
    return {
        "bids": dict(book.bids),
        "asks": dict(book.asks),
        "quality_state": book.state.state.value,
        "previous_update_id": getattr(book.previous, "update_id", None),
        "previous_instrument": getattr(book.previous, "instrument", None),
        "duplicate_count": book.duplicate_count,
        "stale_count": book.stale_count,
    }


def _wire_frames(session):
    """The session builders return ReplayFrame objects already; the direct
    path needs the same list (payload + timestamp_ms), the replay path
    needs it wrapped as-is (ReplaySource accepts ReplayFrame iterables
    directly, per test_replay.py's own usage)."""
    return session


def test_direct_application_and_replay_engine_produce_identical_book_state():
    for venue, session_fn in SESSIONS.items():
        frames = session_fn()
        direct = _full_state(_direct_apply(venue, frames))
        replayed = _full_state(_via_replay_engine(venue, frames))
        assert direct == replayed, venue


def test_direct_and_replay_agree_through_a_sequence_gap_and_recovery():
    """Parity must hold through a degraded state too, not only the happy
    path -- gap and quality-state transitions are exactly where a
    reimplementation would most plausibly diverge."""
    broken = _depth_frame(1_020, U=201, u=210, pu=200, bid="90.0", index=2)
    session = [_depth_frame(1_000, U=100, u=105, pu=99, index=0),
               _snapshot_frame(1_010, last_update_id=102, index=1), broken]
    direct = _full_state(_direct_apply("BINANCE", session))
    replayed = _full_state(_via_replay_engine("BINANCE", session))
    assert direct["quality_state"] == replayed["quality_state"] == "SEQUENCE_GAP"
    assert direct == replayed


def test_direct_and_replay_agree_on_provenance_via_digest():
    """ReplayResult carries its own content digest (used by
    scripts/replay.py's --verify-determinism); confirm the digest of a
    replay run is stable and that the book state it produces matches the
    direct path's provenance-relevant fields (identity, sequence
    position) exactly, not merely 'both say VALID'."""
    for venue, session_fn in SESSIONS.items():
        frames = session_fn()
        engine = ReplayEngine(venue=venue)
        result = engine.run(ReplaySource(frames))
        direct_book = _direct_apply(venue, frames)
        assert result.final_state == direct_book.state.state.value, venue
        assert getattr(engine.book.previous, "instrument", None) == \
               getattr(direct_book.previous, "instrument", None), venue
        # Same input replayed twice through ReplayEngine must yield the same
        # digest -- the mechanism scripts/replay.py --verify-determinism
        # relies on in production, checked here directly rather than only
        # via a subprocess/CLI test.
        second = ReplayEngine(venue=venue).run(ReplaySource(frames))
        assert second.digest == result.digest, venue
