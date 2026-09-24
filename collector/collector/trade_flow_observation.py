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

Unknown-side finding (from independent audit before adding windowed CVD):
a trade whose ``side`` is missing, empty, or any spelling other than a
BUY/SELL variant contributes to *neither* buy_volume nor sell_volume --
confirmed by direct test, not just by reading the ``.upper()`` comparison
and assuming. It still counts in ``trade_count``, so ``buy_volume +
sell_volume < trade_count`` is possible and is the caller's signal that
some trade's direction went unrecorded, rather than a fabricated BUY or
SELL guess.
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


def _causal_trades(frames, observation_ts: int, venue: str):
    """Shared primitive for both the cumulative and windowed observations:
    every trade with ``local_receive_ts <= observation_ts``, in causal/
    replay order, plus how many frames were considered. Frames past
    ``observation_ts`` are excluded before ``ReplaySource`` is even built --
    see ``observe_trade_flow_at``'s docstring for why that matters (not
    merely "excluded from the tally", excluded from being able to
    influence anything about the replay at all).

    Factored out so windowed CVD does not reimplement this -- reuse the
    existing causal-trade-extraction logic rather than build a second
    replay path that could silently diverge from this one.
    """
    causal_frames = [f for f in frames if f.timestamp_ms <= observation_ts]
    if not causal_frames:
        return [], 0
    engine = ReplayEngine(venue=venue)
    engine.run(ReplaySource(causal_frames))
    trades = [e for e in engine.result.non_book_events if isinstance(e, CanonicalTradeEvent)]
    return trades, len(causal_frames)


def _side_volumes(trades):
    """BUY/SELL volumes with casing normalized (see module docstring). A
    trade whose side is missing, empty, or any value other than a BUY/SELL
    spelling contributes to *neither* sum -- audited, not accidental: it
    must never be guessed into a direction it wasn't reported to have. It
    still counts toward the caller's own trade_count, so a caller comparing
    ``buy_volume + sell_volume`` against ``trade_count`` can detect this
    case rather than silently losing that trade's volume with no trace."""
    buy_volume = sum(t.quantity for t in trades if (t.side or "").upper() == "BUY")
    sell_volume = sum(t.quantity for t in trades if (t.side or "").upper() == "SELL")
    return buy_volume, sell_volume


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

    **Known, pre-existing limitation, inherited rather than fixed here**
    (report a pipeline correctness hole rather than silently compensate
    for it inside a feature module): the trade pipeline has no
    duplicate-trade-message protection anywhere -- unlike order-book
    diffs, which sequence.py validates for continuity and duplication, a
    resent trade wire message (a real reconnect/replay scenario on every
    venue) would be counted twice, by every consumer of
    ``non_book_events``, this module included. Fixing this belongs in the
    adapter/replay layer, not here, and was not attempted in this pass.
    """
    if isinstance(observation_ts, bool) or not isinstance(observation_ts, int):
        raise TypeError(f"observation_ts must be an int (epoch ms), got {observation_ts!r}")

    trades, frames_considered = _causal_trades(frames, observation_ts, venue)
    if not trades:
        return TradeFlowObservation(
            exchange=venue, instrument=None, observation_ts=observation_ts,
            status=AlignmentStatus.NEVER_OBSERVED, cvd=None, buy_volume=None,
            sell_volume=None, trade_count=0, last_trade_local_receive_ts=None,
            age_ms=None, frames_considered=frames_considered,
        )

    buy_volume, sell_volume = _side_volumes(trades)
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
        age_ms=age_ms, frames_considered=frames_considered,
    )


@dataclass(frozen=True)
class WindowedTradeFlowObservation:
    """Causal trade flow over exactly ``(window_start, window_end]`` --
    i.e. ``(observation_ts - window_ms, observation_ts]``. A trade at
    ``window_start`` itself is EXCLUDED (open lower bound); a trade at
    ``observation_ts`` (``window_end``) is INCLUDED (closed upper bound).
    This is the same convention a causal person would expect from "the
    last W of data as of T": T itself counts, T-W exactly does not (it is
    the instant the window opens, not inside it).

    ``status`` distinguishes what the cumulative observation's two-state
    NEVER_OBSERVED/AVAILABLE/STALE already covers, applied to the window,
    with one addition the cumulative case does not need: an *empty*
    window is not automatically NEVER_OBSERVED.

    Three genuinely different situations collapse to "trade_count == 0"
    if not distinguished, and this dataclass distinguishes them by
    ``status`` (audit finding, corrected here -- an earlier version of
    this module conflated the first two):

    * **NEVER_OBSERVED** (``cvd``/``buy_volume``/``sell_volume`` all
      ``None``): no causal trade evidence exists for this venue at all,
      up to ``observation_ts`` -- not merely none *in this window*. There
      is no basis to say anything about the window, empty or otherwise.
    * **AVAILABLE with a genuine zero** (``cvd == buy_volume ==
      sell_volume == 0.0``, ``trade_count == 0``): causal trade evidence
      for this venue exists, it is simply outside this specific window,
      and the nearest such evidence is recent enough (within
      ``staleness_ms`` of ``observation_ts``) to trust that "nothing fell
      in this window" reflects a genuinely quiet period rather than a
      gap in what was received. This is real information, not an
      unknown, and must not be reported as ``None``.
    * **STALE with a numeric zero** (same fields as AVAILABLE, ``status``
      differs): evidence exists somewhere for this venue, the window is
      empty, but the nearest evidence is *older* than ``staleness_ms`` --
      i.e. nothing at all has been heard from this venue recently, window
      or not. A confident "zero" cannot be claimed here (a silent gap in
      reception would look identical to a quiet market), so the window's
      zero is reported but flagged STALE, exactly mirroring how the
      cumulative observation retains a numeric CVD under STALE rather
      than discarding it.

    A non-empty window's own STALE/AVAILABLE split is unchanged from
    before this distinction was added: it is governed by the age of the
    most recent trade *inside* the window, which -- because
    ``_causal_trades`` returns trades in ascending causal order and the
    window is the suffix ``(observation_ts - window_ms, observation_ts]``
    -- is always also the most recent causally-known trade overall
    whenever the window is non-empty. See
    ``observe_windowed_trade_flow_at``'s body for the one-line proof.

    ``frames_considered`` is always the *cumulative* frame count up to
    ``observation_ts`` (matching what ``_causal_trades`` computed), not a
    window-scoped count -- frames have no lower causal bound the way a
    trade's window membership does.
    """
    exchange: str
    instrument: Optional[InstrumentId]
    observation_ts: int
    window_start: int
    window_end: int
    window_ms: int
    status: AlignmentStatus
    cvd: Optional[float]
    buy_volume: Optional[float]
    sell_volume: Optional[float]
    trade_count: int
    first_trade_local_receive_ts: Optional[int]
    last_trade_local_receive_ts: Optional[int]
    age_ms: Optional[int]
    frames_considered: int


def observe_windowed_trade_flow_at(
    frames, observation_ts: int, window_ms: int, *, venue: str, staleness_ms: int = 5_000,
) -> WindowedTradeFlowObservation:
    """Causal trade flow over the last ``window_ms`` as of ``observation_ts``.

    Reuses ``_causal_trades`` -- the exact same causal-frame-filter-before-
    replay step ``observe_trade_flow_at`` uses -- for the upper bound
    (``local_receive_ts <= observation_ts``), then applies the window's
    lower bound (``local_receive_ts > observation_ts - window_ms``) as a
    plain filter over the resulting, already-causally-safe trade list.
    This is safe to do post-replay, unlike the upper bound: trades are
    stateless-cumulative (no sequence/gap dependency the way order-book
    diffs have), so which of the causally-known trades get *tallied* is
    exactly a counting decision, not a replay-input decision -- nothing
    about excluding an old trade from this window could let a future trade
    leak in, because the upper-bound exclusion already happened first, at
    the frame level, before this function ever sees the trade list.
    """
    if isinstance(observation_ts, bool) or not isinstance(observation_ts, int):
        raise TypeError(f"observation_ts must be an int (epoch ms), got {observation_ts!r}")
    if isinstance(window_ms, bool) or not isinstance(window_ms, int):
        raise TypeError(f"window_ms must be an int (milliseconds), got {window_ms!r}")
    if window_ms <= 0:
        raise ValueError(f"window_ms must be positive, got {window_ms!r}")

    window_start = observation_ts - window_ms
    window_end = observation_ts

    all_causal_trades, frames_considered = _causal_trades(frames, observation_ts, venue)

    if not all_causal_trades:
        # Genuinely NEVER_OBSERVED: no trade evidence exists for this venue
        # at all, up to observation_ts. Nothing to say about the window.
        return WindowedTradeFlowObservation(
            exchange=venue, instrument=None, observation_ts=observation_ts,
            window_start=window_start, window_end=window_end, window_ms=window_ms,
            status=AlignmentStatus.NEVER_OBSERVED, cvd=None, buy_volume=None,
            sell_volume=None, trade_count=0, first_trade_local_receive_ts=None,
            last_trade_local_receive_ts=None, age_ms=None, frames_considered=frames_considered,
        )

    windowed = [t for t in all_causal_trades if window_start < t.local_receive_ts <= window_end]
    # `all_causal_trades` is in ascending causal order (ReplaySource's own
    # deterministic order), and the window is the suffix
    # `(window_start, observation_ts]`; every trade in `all_causal_trades`
    # already satisfies `local_receive_ts <= observation_ts`, so the
    # window filter above keeps exactly a trailing run of it. That means
    # `all_causal_trades[-1]` -- the most recent causally-known trade,
    # whether or not it happens to fall inside this window -- is the
    # right "how fresh is our knowledge of this venue" reference in every
    # case: when the window is non-empty it IS `windowed[-1]` (the suffix
    # includes the last element or the window would be empty), and when
    # the window is empty it is still the best available evidence of when
    # this venue was last actually heard from.
    latest_known = all_causal_trades[-1]
    overall_age_ms = observation_ts - latest_known.local_receive_ts

    if not windowed:
        # Evidence for this venue exists, just not inside this specific
        # window -- NOT the same as never having observed the venue at
        # all (the case handled above). Whether the empty window can be
        # trusted as a genuine, confirmed zero depends on how fresh the
        # nearest evidence is: fresh enough (AVAILABLE) means we can
        # trust "nothing happened in the last W"; not fresh (STALE) means
        # we have not heard from this venue recently at all, so an empty
        # window cannot be distinguished from a silent reception gap --
        # the zero is still reported (never fabricated as `None` when we
        # do have a concrete count of zero), but flagged accordingly,
        # exactly mirroring how the cumulative observation keeps a
        # numeric CVD under STALE rather than discarding it.
        status = AlignmentStatus.STALE if overall_age_ms > staleness_ms else AlignmentStatus.AVAILABLE
        return WindowedTradeFlowObservation(
            exchange=latest_known.exchange, instrument=latest_known.instrument,
            observation_ts=observation_ts, window_start=window_start, window_end=window_end,
            window_ms=window_ms, status=status, cvd=0.0, buy_volume=0.0, sell_volume=0.0,
            trade_count=0, first_trade_local_receive_ts=None, last_trade_local_receive_ts=None,
            age_ms=overall_age_ms, frames_considered=frames_considered,
        )

    buy_volume, sell_volume = _side_volumes(windowed)
    first_trade, last_trade = windowed[0], windowed[-1]
    assert last_trade is latest_known    # the suffix property claimed above, made explicit
    age_ms = observation_ts - last_trade.local_receive_ts
    status = AlignmentStatus.STALE if age_ms > staleness_ms else AlignmentStatus.AVAILABLE
    return WindowedTradeFlowObservation(
        exchange=last_trade.exchange, instrument=last_trade.instrument,
        observation_ts=observation_ts, window_start=window_start, window_end=window_end,
        window_ms=window_ms, status=status, cvd=buy_volume - sell_volume,
        buy_volume=buy_volume, sell_volume=sell_volume, trade_count=len(windowed),
        first_trade_local_receive_ts=first_trade.local_receive_ts,
        last_trade_local_receive_ts=last_trade.local_receive_ts,
        age_ms=age_ms, frames_considered=frames_considered,
    )
