"""Binance USD-M diff-depth semantics, verified against official documentation.

Source of truth for every assertion about the protocol in this file:

    "How to manage a local order book correctly" (USD-M futures)
    https://developers.binance.com/docs/derivatives/usds-margined-futures/
    websocket-market-streams/How-to-manage-a-local-order-book-correctly
    verified 2026-09-19

The documented steps are asserted directly, so that a future change to the
implementation that contradicts the venue fails here rather than silently
producing a plausible-looking book. The remaining tests pin the five defects
found while closing D14; each is named after the behaviour it forbids, not
after the code that used to be wrong.
"""
from __future__ import annotations

import random
from decimal import Decimal

import pytest

from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.book_engine import NON_AUTHORITATIVE_BOOK_SOURCES, LocalBook
from collector.collector.canonical import CanonicalOrderBookEvent
from collector.collector.quality_events import BookQuality
from collector.collector.sequence import (
    BinanceSequenceComparator,
    binance_snapshot_bridge,
)


def diff(u, U=None, pu=None, bids=None, asks=None, source="DIFF_DEPTH_RECONSTRUCTED"):
    """One ``depthUpdate`` event. ``U``/``u``/``pu`` use the venue's names."""
    return CanonicalOrderBookEvent(
        "BINANCE", "orderbook", 1, None, 1,
        bids=(("100", "1"),) if bids is None else bids,
        asks=(("101", "1"),) if asks is None else asks,
        update_id=u, first_update_id=u if U is None else U,
        previous_update_id=pu, book_source=source,
    )


def snapshot(last_update_id, bids=None, asks=None):
    return CanonicalOrderBookEvent(
        "BINANCE", "orderbook", None, None, 10,
        bids=(("100", "1"),) if bids is None else bids,
        asks=(("101", "1"),) if asks is None else asks,
        update_id=last_update_id, is_snapshot=True,
    )


def bridged_book():
    """A book in the state live reaches after a successful startup bridge."""
    book = LocalBook("BINANCE")
    book.buffer = [diff(100, 100, 99)]
    assert book.binance_snapshot(100, snapshot(100))
    assert book.state.state is BookQuality.VALID
    return book


# --------------------------------------------------------------------------
# Documented procedure: steps 4 through 9 asserted directly.
# --------------------------------------------------------------------------

def test_step4_drops_events_strictly_below_last_update_id():
    """Step 4: drop any event where u is *< lastUpdateId*.

    Spot uses ``<=``. Using Spot's rule here would discard the one event
    that is allowed to bridge, so equality must survive.
    """
    book = LocalBook("BINANCE")
    book.buffer = [diff(8, 8), diff(9, 9), diff(10, 9), diff(11, 11, 10)]
    assert book.binance_snapshot(10, snapshot(10))
    # u == lastUpdateId bridged; u < lastUpdateId was dropped; the chain
    # continued to the newest event.
    assert book.previous.update_id == 11
    assert book.state.state is BookQuality.VALID


def test_step5_bridge_is_inclusive_and_has_no_spot_plus_one():
    """Step 5: first processed event has U <= lastUpdateId AND u >= lastUpdateId."""
    assert binance_snapshot_bridge(diff(105, U=100), 105) is True   # u == lastUpdateId
    assert binance_snapshot_bridge(diff(105, U=100), 100) is True   # U == lastUpdateId
    assert binance_snapshot_bridge(diff(105, U=100), 104) is True   # strictly inside
    assert binance_snapshot_bridge(diff(104, U=105), 105) is False  # U > lastUpdateId
    assert binance_snapshot_bridge(diff(99, U=90), 105) is False    # u < lastUpdateId
    # Spot's rule (U <= lastUpdateId+1) would accept this; USD-M must not.
    assert binance_snapshot_bridge(diff(101, U=101), 100) is False


def test_step6_pu_must_equal_previous_u():
    """Step 6: each new event's pu equals the previous event's u."""
    comparator = BinanceSequenceComparator()
    assert comparator.check(diff(11, 11, pu=10), diff(10, 9, pu=8)).is_gap is False
    assert comparator.check(diff(11, 11, pu=9), diff(10, 9, pu=8)).reason == "pu_mismatch"


def test_steps7and8_absolute_quantities_and_zero_removes_level():
    """Step 7: quantities are absolute. Step 8: quantity 0 removes the level."""
    book = bridged_book()
    applied = book.apply(diff(101, 101, 100, bids=(("100", "5"),)))
    assert dict(applied.bids)[Decimal("100")] == Decimal("5")  # absolute, not additive

    applied = book.apply(diff(102, 102, 101, bids=(("100", "0"),)))
    assert Decimal("100") not in dict(applied.bids)


def test_step9_removing_an_absent_level_is_normal_not_an_error():
    """Step 9: removing a price level absent from the local book is normal."""
    book = bridged_book()
    applied = book.apply(diff(101, 101, 100, bids=(("50", "0"),)))
    assert applied is not None
    assert book.state.state is BookQuality.VALID


# --------------------------------------------------------------------------
# D18: a non-advancing event is stale, not a sequence gap.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("repeat_u,expected", [(100, "duplicate_update"), (99, "stale_update")])
def test_non_advancing_event_is_stale_and_does_not_break_the_book(repeat_u, expected):
    """A re-delivered or out-of-order-late event must not trigger recovery.

    Its ``pu`` no longer matches the book's ``u``, so a bare step-6 check
    calls it a gap. That is wrong: step 7 makes every event a statement of
    absolute quantities, so an event that does not advance ``u`` cannot hold
    anything the book is missing. Misclassifying it spent a bounded REST
    recovery slot and wrote a false SEQUENCE_GAP into the research record.
    """
    book = bridged_book()
    applied = book.apply(diff(repeat_u, repeat_u, pu=repeat_u - 1))

    assert applied is None                          # contributed nothing
    assert book.last_reason == expected             # and says why
    assert book.state.state is BookQuality.VALID    # book still trusted
    assert book.stale_count == 1
    assert book.buffer == []                        # not buffered for recovery


def test_genuine_gap_is_still_detected_after_stale_handling():
    """Advancing u with a broken pu remains a gap. Fail-safe is preserved."""
    book = bridged_book()
    assert book.apply(diff(105, 105, pu=99)) is None
    assert book.last_reason == "pu_mismatch"
    assert book.state.state is BookQuality.SEQUENCE_GAP


def test_stale_event_cannot_resurrect_a_removed_price_level():
    """The dangerous form of the old bug: stale data mutating a live book."""
    book = bridged_book()
    book.apply(diff(101, 101, 100, bids=(("100", "0"),)))       # level removed
    book.apply(diff(99, 99, 98, bids=(("100", "9"),)))          # stale re-delivery
    assert Decimal("100") not in book.bids


# --------------------------------------------------------------------------
# D20: unprovable continuity is distinguishable from violated continuity.
# --------------------------------------------------------------------------

def test_missing_pu_and_missing_u_get_their_own_reasons():
    comparator = BinanceSequenceComparator()
    previous = diff(10, 9, pu=8)

    missing_pu = comparator.check(diff(11, 11, pu=None), previous)
    assert missing_pu.is_gap and missing_pu.reason == "pu_missing"

    missing_u = comparator.check(diff(None, 11, pu=10), previous)
    assert missing_u.is_gap and missing_u.reason == "update_id_missing"

    # Still distinct from a real venue-side break.
    assert comparator.check(diff(11, 11, pu=9), previous).reason == "pu_mismatch"


# --------------------------------------------------------------------------
# D21: the bridge predicate must not raise on malformed input.
# --------------------------------------------------------------------------

def test_bridge_predicate_rejects_malformed_ids_without_raising():
    def malformed(u, first_update_id):
        return CanonicalOrderBookEvent(
            "BINANCE", "orderbook", 1, None, 1,
            update_id=u, first_update_id=first_update_id)

    assert binance_snapshot_bridge(malformed(None, None), 100) is False
    assert binance_snapshot_bridge(malformed(100, None), 100) is False   # U absent
    assert binance_snapshot_bridge(malformed(None, 100), 100) is False   # u absent
    assert BinanceAdapter.bridge_accepts(malformed(None, None), 100) is False
    assert binance_snapshot_bridge(diff(100, U=100), None) is False


# --------------------------------------------------------------------------
# D19: partial depth is never authoritative.
# --------------------------------------------------------------------------

def test_partial_depth_never_mutates_or_invalidates_the_book():
    """depth10 is a top-N snapshot, not a diff.

    Applying it through the diff path leaves levels outside the top N frozen
    at stale values while the book still claims VALID. It must also not be
    treated as evidence *against* the diff chain, so quality state is
    untouched either way.
    """
    assert "PARTIAL_DEPTH" in NON_AUTHORITATIVE_BOOK_SOURCES
    book = bridged_book()
    before_bids, before_state = dict(book.bids), book.state.state

    applied = book.apply(diff(101, 101, 100, bids=(("100", "999"),), source="PARTIAL_DEPTH"))

    assert applied is None
    assert dict(book.bids) == before_bids
    assert book.state.state is before_state
    assert book.non_authoritative_count == 1


def test_partial_depth_is_refused_even_on_an_unbridged_book():
    book = LocalBook("BINANCE")
    assert book.apply(diff(5, 5, 4, source="PARTIAL_DEPTH")) is None
    assert book.buffer == []   # must not enter the recovery buffer either
    assert book.state.state is BookQuality.RECOVERING


# --------------------------------------------------------------------------
# D22: a snapshot ahead of the buffer is retained; one behind a hole is not.
# --------------------------------------------------------------------------

def test_snapshot_ahead_of_buffer_is_retained_and_bridges_on_the_next_diff():
    """Step 4 can legally drop every buffered event.

    That is not a failure: the next diff will straddle lastUpdateId. The
    snapshot is kept so the bridge completes from data already in hand
    instead of burning another of the five REST slots per minute.
    """
    book = LocalBook("BINANCE")
    book.buffer = [diff(8, 8, 7), diff(9, 9, 8)]     # all older than the snapshot

    assert book.binance_snapshot(10, snapshot(10)) is False
    assert book.last_reason == "snapshot_ahead_of_buffer"
    assert book.state.state is BookQuality.RECOVERING

    # The straddling diff arrives; live buffers it, then retries.
    book.apply(diff(11, 9, 8))
    assert book.retry_pending_snapshot() is True
    assert book.state.state is BookQuality.VALID
    assert book.previous.update_id == 11
    assert book.pending_snapshot_bridges == 1


def test_snapshot_behind_a_hole_is_discarded_not_retained():
    """The opposite failure: U > lastUpdateId means diffs were lost.

    No future event can repair that, so retaining the snapshot would loop
    forever. A fresh one is genuinely required.
    """
    book = LocalBook("BINANCE")
    book.buffer = [diff(20, 15, 14)]                  # U=15 > lastUpdateId=10

    assert book.binance_snapshot(10, snapshot(10)) is False
    assert book.last_reason == "snapshot_bridge_not_found"
    assert book.retry_pending_snapshot() is False     # nothing retained


def test_retry_is_a_no_op_without_a_retained_snapshot():
    assert LocalBook("BINANCE").retry_pending_snapshot() is False
    assert bridged_book().retry_pending_snapshot() is False


def test_reconnect_drops_a_retained_snapshot():
    """After a reconnect an unknown amount of time has passed; fail safe."""
    book = LocalBook("BINANCE")
    book.buffer = [diff(8, 8, 7)]
    book.binance_snapshot(10, snapshot(10))
    book.invalidate("reconnect")
    assert book.retry_pending_snapshot() is False


def test_successful_bridge_clears_the_retained_snapshot():
    book = LocalBook("BINANCE")
    book.buffer = [diff(8, 8, 7)]
    book.binance_snapshot(10, snapshot(10))
    book.apply(diff(11, 9, 8))
    assert book.retry_pending_snapshot() is True
    assert book.retry_pending_snapshot() is False     # not re-bridgeable


# --------------------------------------------------------------------------
# Adversarial / property: no invalid causal sequence may become VALID.
# --------------------------------------------------------------------------

def test_stale_storm_cannot_move_the_book_out_of_valid():
    """Bounded work, bounded memory, no state change, under repetition."""
    book = bridged_book()
    for _ in range(500):
        book.apply(diff(100, 100, 99))
    assert book.state.state is BookQuality.VALID
    assert book.stale_count == 500
    assert book.buffer == []          # no unbounded growth from stale events


def test_property_no_broken_chain_ever_reports_valid():
    """Fuzz: only a proven bridge or an unbroken pu chain may yield VALID."""
    rng = random.Random(20260919)
    for _ in range(400):
        book = LocalBook("BINANCE")
        u = 1000
        for _ in range(rng.randint(1, 12)):
            choice = rng.random()
            if choice < 0.25:                       # break the chain
                book.apply(diff(u + 5, u + 5, pu=u - 99))
                u += 5
            elif choice < 0.5:                      # stale / duplicate
                book.apply(diff(u - rng.randint(0, 3), u, pu=u - 1))
            else:                                   # well-formed continuation
                book.apply(diff(u + 1, u + 1, pu=u))
                u += 1
        # The book was never bridged by a snapshot, so it can never be VALID
        # no matter what arrives on the wire.
        assert book.state.state is not BookQuality.VALID


# --------------------------------------------------------------------------
# Recovery-budget accounting for the retained snapshot.
# --------------------------------------------------------------------------

def test_retained_snapshot_does_not_consume_the_recovery_budget():
    """A cold start must not spend REST slots on a self-resolving state.

    On a fresh process the buffer is empty until the first diff lands, so the
    startup snapshot very often arrives ahead of everything buffered. If that
    outcome is booked as a recovery failure, backoff escalates and the
    controller walks toward `attempts_exhausted` while nothing is actually
    wrong. The pending bridge must instead complete on the next diff.
    """
    from collector.collector.backoff import ExponentialBackoff
    from collector.collector.recovery_control import RecoveryController

    controller = RecoveryController(
        name="test", min_interval_s=0.0, max_per_window=5, window_s=60.0,
        backoff=ExponentialBackoff(base_delay=1.0, max_delay=60.0, max_attempts=10))

    book = LocalBook("BINANCE")
    for _ in range(6):
        # Repeated cold-start-shaped attempts: snapshot ahead of the buffer.
        book.buffer = [diff(8, 8, 7)]
        assert book.binance_snapshot(10, snapshot(10)) is False
        assert book.last_reason == "snapshot_ahead_of_buffer"
        controller.succeed()          # what run_collector now does

    assert controller.consecutive_failures == 0
    assert controller.request("gap").allowed is True

    # And the bridge still completes from recorded data alone.
    book.apply(diff(11, 9, 8))
    assert book.retry_pending_snapshot() is True
    assert book.state.state is BookQuality.VALID
