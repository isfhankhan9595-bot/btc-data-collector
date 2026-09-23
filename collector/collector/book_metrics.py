"""Spread and microprice: the smallest composable primitive built on top of
Phase H's already-audited ``reconstruct_book_at``.

Deliberately not a new causal/replay primitive at all -- ``derive_book_metrics``
is a pure function of a ``BookObservation`` (top-of-book price/quantity only),
with no frames, no observation_ts of its own, no replay. Every causal
guarantee it has, it inherits entirely from the ``BookObservation`` it is
given: if that observation is causally correct (Phase H's own extensive
audit), this is too, by construction -- there is no new surface here for a
leak, a gap, or a staleness bug to hide in.

This keeps the "smallest composable primitive that unlocks the next layer"
rule from the task's own instructions literal: nothing here duplicates
book reconstruction, sequence validation, or identity resolution. It is a
thin arithmetic layer, not a feature engine, and does not grow into one --
see the module docstring's own scope note below.

Why bundle spread, mid, and microprice in one function rather than three
modules: they are three views of the exact same two numbers (best bid,
best ask, and their quantities) computed once; splitting them would not
add composability, only duplicate the same missingness/None-handling three
times. This is not the start of a features.py -- OBI, OFI, depth-imbalance
and every other candidate primitive in the task's own list would each need
their own multi-level depth logic and their own missingness contract, and
belong in their own modules when and if they are built, not folded in here.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .book_observation import BookObservation
from .instrument import InstrumentId
from .quality_events import BookQuality
from collector.pipeline.cross_exchange_alignment import AlignmentStatus


@dataclass(frozen=True)
class BookMetrics:
    """Top-of-book derived metrics as of the given ``BookObservation``, or
    their explicit absence. Mirrors that observation's own identity,
    status, and quality_state exactly -- this is a derived view of it, not
    an independent observation with its own causal story.

    ``spread``/``mid_price``/``microprice`` are ``None`` whenever either
    side of the book is empty (including the whole-book-empty
    ``NEVER_OBSERVED`` case): a one-sided book has no meaningful spread,
    and fabricating one from a single side would misrepresent what was
    actually knowable. ``best_bid_price``/``best_bid_qty`` (and the ask
    equivalents) are reported independently whenever that one side is
    present, even if the other is not.
    """
    exchange: str
    instrument: Optional[InstrumentId]
    observation_ts: int
    status: AlignmentStatus
    quality_state: Optional[str]
    best_bid_price: Optional[float]
    best_bid_qty: Optional[float]
    best_ask_price: Optional[float]
    best_ask_qty: Optional[float]
    spread: Optional[float]
    spread_bps: Optional[float]
    mid_price: Optional[float]
    microprice: Optional[float]
    age_ms: Optional[int]


def derive_book_metrics(observation: BookObservation) -> BookMetrics:
    """Derive spread/mid/microprice from an already-reconstructed book.

    ``observation.bids``/``.asks`` are sorted best-first (confirmed by
    reading ``reconstruct_book_at``: bids descending, asks ascending), so
    the top-of-book is simply the first element of each when non-empty --
    no re-sorting, no re-validation of level ordering here.

    Relies on one invariant already enforced upstream, not re-checked here:
    ``LocalBook._validated_maps`` rejects any update where
    ``max(bids) >= min(asks)`` (book_engine.py, confirmed by reading it),
    so whenever both sides are present, best_bid_price is always strictly
    less than best_ask_price and spread is always strictly positive. This
    function does not re-validate that -- it is Phase H's invariant to
    guarantee, not this module's to re-derive.
    """
    best_bid_price = best_bid_qty = None
    best_ask_price = best_ask_qty = None
    if observation.bids:
        best_bid_price, best_bid_qty = observation.bids[0]
        best_bid_price, best_bid_qty = float(best_bid_price), float(best_bid_qty)
    if observation.asks:
        best_ask_price, best_ask_qty = observation.asks[0]
        best_ask_price, best_ask_qty = float(best_ask_price), float(best_ask_qty)

    spread = spread_bps = mid_price = microprice = None
    if best_bid_price is not None and best_ask_price is not None:
        spread = best_ask_price - best_bid_price
        mid_price = (best_bid_price + best_ask_price) / 2.0
        spread_bps = (spread / mid_price) * 10_000.0 if mid_price else None
        total_qty = best_bid_qty + best_ask_qty
        # Standard microprice (Stoikov): each side's price weighted by the
        # OPPOSITE side's quantity -- more size resting at one level acts as
        # support/resistance there, so the fair price is pulled toward the
        # OTHER, thinner side (large bid_qty -> microprice biased toward the
        # ask; large ask_qty -> biased toward the bid). Top-of-book only;
        # not a multi-level imbalance measure (that is a distinct,
        # not-yet-built primitive -- see this module's docstring).
        microprice = ((best_bid_price * best_ask_qty + best_ask_price * best_bid_qty) / total_qty
                      if total_qty > 0 else None)

    return BookMetrics(
        exchange=observation.exchange, instrument=observation.instrument,
        observation_ts=observation.observation_ts, status=observation.status,
        quality_state=observation.quality_state,
        best_bid_price=best_bid_price, best_bid_qty=best_bid_qty,
        best_ask_price=best_ask_price, best_ask_qty=best_ask_qty,
        spread=spread, spread_bps=spread_bps, mid_price=mid_price, microprice=microprice,
        age_ms=observation.age_ms,
    )
