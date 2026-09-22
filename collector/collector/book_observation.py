"""Causal order-book state reconstruction: "what did the book look like at T?"

Architecture decision (recorded here, not just in a commit message)
---------------------------------------------------------------------
Before this module, the repository could answer "what order-book messages
occurred before T" (``ReplayEngine.run()`` processes a whole session) but
not "what did the book look like at T" as a first-class query -- the
narrow ``BookUpdate`` record (``best_bid``/``best_ask`` strings only, no
depth, no instrument identity) meant a caller had no clean way to get full
reconstructed state at an arbitrary point, only a post-hoc digest for
replay-parity comparison. ``pipeline/observation.py`` (Phase G) explicitly
declined to retrofit book state into itself for exactly this reason -- see
its own "Known limitation" docstring.

Three ways to close that gap were considered:

1. **Reinvent state reconstruction here.** Rejected outright: the
   Binance-USD-M-vs-Spot sequence divergence, Bybit's decrease/reset rule,
   and OKX's protocol are already correctly implemented in
   ``LocalBook``/``sequence.py``, extensively verified (this session's own
   Phase 6 closed a real Binance USD-M semantics defect the hard way, by
   reading official docs and fixing five bugs). Re-deriving any of that
   here would either silently diverge from the proven implementation or
   duplicate it -- both are worse than reusing it.
2. **Change what ``ReplayEngine``/``BookUpdate`` record**, to carry full
   depth and identity. Rejected: a real change to a heavily-tested,
   unrelated module's recorded-output shape, for a need this module can
   satisfy without touching it at all (see below).
3. **A thin, causal, read-only wrapper around the existing, unmodified
   ``ReplayEngine``.** Chosen. ``ReplayFrame.timestamp_ms`` is confirmed
   (by reading ``ReplaySource.from_records``) to be sourced from
   ``local_receive_ts`` for WIRE frames and ``response_receive_ts`` for
   REST_SNAPSHOT frames -- i.e. it already *is* the causal availability
   timestamp for every frame kind, wire and REST alike. Filtering the frame
   list to ``timestamp_ms <= observation_ts`` *before* constructing a
   ``ReplaySource`` (which itself sorts by ``order_key``, so filtering does
   not disturb ordering) and then running the ordinary, unmodified
   ``ReplayEngine`` reproduces exactly what live processing would have
   looked like at that moment -- because that is precisely what live
   processing *is*: processing frames in causal order, up to now. No new
   reconstruction logic; only a causal cutoff applied to the input.

What this exposes that ``BookUpdate`` does not
-------------------------------------------------
Full ``bids``/``asks`` depth (from ``LocalBook.bids``/``.asks`` directly,
not the top-of-book strings), the reconstructed quality state, and
instrument identity -- recovered from ``LocalBook.previous`` (the last
successfully applied ``CanonicalOrderBookEvent``, which is confirmed by
reading ``book_engine.py`` to be the one place the book retains a
reference to a full canonical event; ``LocalBook`` itself tracks no
identity internally, by design -- one instance is one instrument by
construction, not by a field it carries).

Staleness and NEVER_OBSERVED reuse ``pipeline.cross_exchange_alignment.AlignmentStatus``
for vocabulary consistency across the codebase, not because this is the
same kind of observation as ``causally_align()``'s (a book is stateful;
``causally_align()`` selects a single latest event per key, which is not
what "the book" means).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .book_engine import LocalBook
from .instrument import InstrumentId
from .replay import ReplayEngine, ReplaySource

# Re-exported for a single, consistent status vocabulary across the
# alignment and book-observation layers. First cross-import between
# collector.collector and collector.pipeline in the codebase -- checked
# for a circular-import risk before adding: cross_exchange_alignment.py
# imports nothing from collector.collector, so this is one-directional.
from collector.pipeline.cross_exchange_alignment import AlignmentStatus


@dataclass(frozen=True)
class BookObservation:
    """Authoritative order-book state as of an observation_ts, or the
    explicit absence of one.

    ``bids``/``asks`` are the full reconstructed depth (price, quantity)
    tuples, exactly as ``LocalBook`` holds them -- not a top-of-book
    summary. Empty tuples with ``status is NEVER_OBSERVED`` mean no
    causally-available data existed at all; empty tuples with a real
    status mean the book itself is genuinely empty at that depth (rare,
    but distinct -- never conflated).
    """
    exchange: str
    market_type: str
    instrument: Optional[InstrumentId]
    observation_ts: int
    status: AlignmentStatus
    quality_state: Optional[str]     # BookQuality value, or None if NEVER_OBSERVED
    bids: tuple
    asks: tuple
    last_update_local_receive_ts: Optional[int]
    age_ms: Optional[int]
    frames_considered: int


def reconstruct_book_at(
    frames, observation_ts: int, *, venue: str, staleness_ms: int = 5_000,
) -> BookObservation:
    """Reconstruct order-book state at observation_ts from causally
    available frames only.

    ``frames`` is any iterable of ``ReplayFrame`` (typically
    ``ReplaySource(...).frames`` from a recorded session, or a live
    session's own accumulated frame list). Frames with
    ``timestamp_ms > observation_ts`` are excluded *before* replay runs --
    they are never constructed into a ``ReplaySource`` at all, so they
    cannot influence sequence/gap/recovery state either, not merely the
    final reported book. This is what makes "no lookahead" a property of
    the input, not something this function has to separately enforce
    inside the replay it delegates to.
    """
    if isinstance(observation_ts, bool) or not isinstance(observation_ts, int):
        raise TypeError(f"observation_ts must be an int (epoch ms), got {observation_ts!r}")

    causal_frames = [f for f in frames if f.timestamp_ms <= observation_ts]
    if not causal_frames:
        return BookObservation(
            exchange=venue, market_type="", instrument=None, observation_ts=observation_ts,
            status=AlignmentStatus.NEVER_OBSERVED, quality_state=None, bids=(), asks=(),
            last_update_local_receive_ts=None, age_ms=None, frames_considered=0,
        )

    engine = ReplayEngine(venue=venue)
    engine.run(ReplaySource(causal_frames))
    book = engine.book

    last_event = book.previous
    if last_event is None:
        # Frames existed and were processed (e.g. only a malformed frame,
        # or diffs buffered awaiting a bridge that never came within the
        # causal window) but no event was ever successfully applied to the
        # book. Genuinely different from "no frames at all": there IS
        # provenance, just no reconstructed state to show for it yet.
        return BookObservation(
            exchange=venue, market_type="", instrument=None, observation_ts=observation_ts,
            status=AlignmentStatus.NEVER_OBSERVED, quality_state=book.state.state.value,
            bids=(), asks=(), last_update_local_receive_ts=None, age_ms=None,
            frames_considered=len(causal_frames),
        )

    age_ms = observation_ts - last_event.local_receive_ts
    status = AlignmentStatus.STALE if age_ms > staleness_ms else AlignmentStatus.AVAILABLE
    return BookObservation(
        exchange=last_event.exchange, market_type=last_event.market_type,
        instrument=last_event.instrument, observation_ts=observation_ts,
        status=status, quality_state=book.state.state.value,
        bids=tuple(sorted(book.bids.items(), reverse=True)),
        asks=tuple(sorted(book.asks.items())),
        last_update_local_receive_ts=last_event.local_receive_ts, age_ms=age_ms,
        frames_considered=len(causal_frames),
    )
