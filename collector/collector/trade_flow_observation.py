"""Causal trade-flow observation: "what has aggressive order flow done by T?"

Architecture: deliberately the same shape as ``book_observation.py``, for
the same reason -- a thin, causal, read-only wrapper around the existing,
unmodified ``ReplayEngine``, not a new reconstruction path. Frames are
filtered to ``timestamp_ms <= observation_ts`` *before* a ``ReplaySource``
is built, exactly as ``reconstruct_book_at`` does, so "no lookahead" is a
property of the input to replay, not something this module has to enforce
after the fact. See that module's docstring for the fuller architectural
rationale (three options considered, this one chosen); it applies
unchanged here.

Why this is a new module rather than folded into ``book_observation.py``:
a trade-flow observation is stateless-cumulative (every trade up to T
contributes, independent of "the current book"), while a book observation
is inherently stateful reconstruction (``LocalBook``). Sharing the causal
cutoff logic is right; sharing the result shape would conflate two
different kinds of "what do we know at T".

Cross-venue side-casing finding (recorded here because this is the first
module that has to compare ``side`` values, not just persist them): the
three adapters do not agree on ``CanonicalTradeEvent.side`` casing.
Binance's adapter constructs it itself as ``"BUY"``/``"SELL"`` (uppercase,
never taken from the raw payload). Bybit and OKX pass their raw wire
field through verbatim -- Bybit's is ``"Buy"``/``"Sell"``, OKX's is
lowercase ``"buy"``/``"sell"`` (confirmed by reading each adapter's
``_parse_trades``, not assumed). This function normalizes with
``.upper()`` before comparing; nothing upstream is changed, since each
adapter's raw-preserving choice is itself intentional (see OKX's
``trades``/``trades-all`` docstrings) and not this module's business to
alter.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .canonical import CanonicalTradeEvent
from .instrument import InstrumentId
from .replay import ReplayEngine, ReplaySource

from collector.pipeline.cross_exchange_alignment import AlignmentStatus


@dataclass(frozen=True)
class TradeFlowObservation:
    """Causal cumulative trade flow as of observation_ts, or its explicit absence.

    ``cvd`` is signed (buy_volume - sell_volume, in each trade's own
    ``quantity`` unit -- never converted or compared across venues here).
    ``None`` fields mean genuinely unobserved, never a fabricated zero: see
    Gate F in the phase's own quality-gate list -- "no liquidation
    observed" must not read as "liquidation volume = 0" unless the
    observation window is proven complete, and the same principle applies
    here to trades.
    """
    exchange: str
    instrument: Optional[InstrumentId]
    observation_ts: int
    status: AlignmentStatus
    cvd: Optional[float]
    buy_volume: Optional[float]
    sell_volume: Optional[float]
    trade_count: int
    last_trade_local_receive_ts: Optional[int]
    age_ms: Optional[int]
    frames_considered: int


def observe_trade_flow_at(
    frames, observation_ts: int, *, venue: str, staleness_ms: int = 5_000,
) -> TradeFlowObservation:
    """Reconstruct cumulative trade flow at observation_ts from causally
    available frames only.

    Every trade with ``local_receive_ts <= observation_ts`` contributes;
    nothing with a later receive time is even constructed into the
    ``ReplaySource`` this delegates to, for the same reason
    ``reconstruct_book_at`` excludes late frames before replay rather than
    after: exclusion has to happen before the frame can influence anything,
    including sequence/gap state for the trade stream itself, not merely
    before it is counted in the final tally.
    """
    if isinstance(observation_ts, bool) or not isinstance(observation_ts, int):
        raise TypeError(f"observation_ts must be an int (epoch ms), got {observation_ts!r}")

    causal_frames = [f for f in frames if f.timestamp_ms <= observation_ts]
    if not causal_frames:
        return TradeFlowObservation(
            exchange=venue, instrument=None, observation_ts=observation_ts,
            status=AlignmentStatus.NEVER_OBSERVED, cvd=None, buy_volume=None,
            sell_volume=None, trade_count=0, last_trade_local_receive_ts=None,
            age_ms=None, frames_considered=0,
        )

    engine = ReplayEngine(venue=venue)
    engine.run(ReplaySource(causal_frames))
    trades = [e for e in engine.result.non_book_events if isinstance(e, CanonicalTradeEvent)]

    if not trades:
        # Frames existed (malformed, or a different stream entirely) but no
        # trade was ever produced. Distinct from zero frames, same
        # distinction reconstruct_book_at makes for "considered but nothing
        # applied" -- there IS provenance, just nothing to report from it.
        return TradeFlowObservation(
            exchange=venue, instrument=None, observation_ts=observation_ts,
            status=AlignmentStatus.NEVER_OBSERVED, cvd=None, buy_volume=None,
            sell_volume=None, trade_count=0, last_trade_local_receive_ts=None,
            age_ms=None, frames_considered=len(causal_frames),
        )

    buy_volume = sum(t.quantity for t in trades if (t.side or "").upper() == "BUY")
    sell_volume = sum(t.quantity for t in trades if (t.side or "").upper() == "SELL")
    last_trade = trades[-1]   # last in causal/replay order, not max(local_receive_ts):
                              # ReplaySource's own deterministic ordering already IS
                              # the causal-arrival order (see replay.py's order_key),
                              # so re-deriving "last" via max() would be redundant and
                              # would silently stop matching that order if a tie-break
                              # rule ever changed there without this module noticing.
    age_ms = observation_ts - last_trade.local_receive_ts
    status = AlignmentStatus.STALE if age_ms > staleness_ms else AlignmentStatus.AVAILABLE
    return TradeFlowObservation(
        exchange=last_trade.exchange, instrument=last_trade.instrument,
        observation_ts=observation_ts, status=status,
        cvd=buy_volume - sell_volume, buy_volume=buy_volume, sell_volume=sell_volume,
        trade_count=len(trades), last_trade_local_receive_ts=last_trade.local_receive_ts,
        age_ms=age_ms, frames_considered=len(causal_frames),
    )
