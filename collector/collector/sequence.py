"""Exchange-specific order-book continuity comparators.

Binance USD-M semantics here are transcribed from the official procedure,
"How to manage a local order book correctly" (USD-M futures), verified
2026-09-19 against:

    https://developers.binance.com/docs/derivatives/usds-margined-futures/
    websocket-market-streams/How-to-manage-a-local-order-book-correctly

The documented steps, and where each is enforced:

    1. Open a stream to ``<symbol>@depth``.
    2. Buffer the events received. For the same price, the latest received
       update covers the previous one.                  -> LocalBook._buffer_event
    3. Get a depth snapshot from ``/fapi/v1/depth?limit=1000``.
    4. Drop any event where ``u`` is ``< lastUpdateId`` in the snapshot.
       Note the strict ``<``: Spot uses ``<=``.          -> LocalBook.binance_snapshot
    5. The first processed event must have ``U <= lastUpdateId`` AND
       ``u >= lastUpdateId``. Note the absence of Spot's ``+1``.
                                                        -> binance_snapshot_bridge
    6. While listening, each new event's ``pu`` must equal the previous
       event's ``u``, otherwise reinitialise from step 3.
                                                        -> BinanceSequenceComparator
    7. The data in each event is the absolute quantity for a price level.
    8. If the quantity is 0, remove the price level.     -> LocalBook._validated_maps
    9. Receiving an event that removes a price level absent from the local
       book can happen and is normal.                    -> LocalBook._validated_maps

Steps 7 and 8 are the reason a non-advancing event is *stale* rather than a
break in the chain: each event restates absolute quantities for the levels it
names, so an event whose final update id does not advance past the book
carries nothing the book does not already hold. See
:class:`BinanceSequenceComparator`.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class SequenceResult:
    """Classification of one event against the book's previous event.

    The four outcomes are mutually exclusive and deliberately distinct,
    because they demand different responses and are different facts about
    the venue:

    ``is_gap``
        Continuity is broken or unprovable. The book must stop trusting
        itself and recover from a fresh snapshot.
    ``is_resync_signal``
        The venue signalled a sequence reset (Bybit). Not a venue fault.
    ``is_stale``
        The event advances nothing. It is dropped; the book stays valid and
        no recovery is triggered.
    neither
        Normal continuation.
    """

    is_gap: bool = False
    is_resync_signal: bool = False
    reason: str = ""
    is_stale: bool = False


class SequenceComparator:
    def check(self, current: Any, previous: Optional[Any]) -> SequenceResult:
        raise NotImplementedError


class BinanceSequenceComparator(SequenceComparator):
    """USD-M futures diff-depth continuity (documented step 6).

    Four classifications, where the previous three implementations had one:

    ``update_id_missing``
        A gap. ``u`` is absent or not an integer, so continuity cannot be
        evaluated at all. Failing safe is correct, but it is recorded under
        its own reason so a malformed-payload problem is not filed in the
        research record as a venue-side sequence break.

    ``stale_update`` / ``duplicate_update``
        Not a gap. ``u`` did not advance past the book's current ``u``.
        Because each event carries *absolute* quantities for the levels it
        names (step 7), such an event cannot contain information the book is
        missing. Treating it as a gap -- which is what a bare ``pu``
        comparison does, since a re-delivered event's ``pu`` no longer
        matches the book's ``u`` -- forced a REST resync, spent the bounded
        recovery budget, and wrote a spurious ``VALID -> SEQUENCE_GAP``
        transition into the quality record. Downstream research then sees a
        data-quality hole where the venue merely repeated itself.

    ``pu_missing``
        A gap, but distinct from ``pu_mismatch``: the frame has no ``pu``
        field, so continuity is *unprovable* rather than *violated*. The
        two look identical in a quality report otherwise, and they have
        different causes (our parsing vs. the venue's stream).

    ``pu_mismatch``
        A gap. The venue's chain genuinely broke: step 6 failed.
    """

    def check(self, current: Any, previous: Optional[Any]) -> SequenceResult:
        if previous is None:
            return SequenceResult()

        current_update_id = getattr(current, "update_id", None)
        previous_update_id = getattr(previous, "update_id", None)
        if not isinstance(current_update_id, int) or not isinstance(previous_update_id, int):
            return SequenceResult(True, False, "update_id_missing")

        # Steps 2 and 7: a later update covers an earlier one at the same
        # price, and every event states absolute quantities. An event that
        # does not advance ``u`` is therefore redundant, not a break.
        if current_update_id < previous_update_id:
            return SequenceResult(False, False, "stale_update", is_stale=True)
        if current_update_id == previous_update_id:
            return SequenceResult(False, False, "duplicate_update", is_stale=True)

        if current.previous_update_id is None:
            return SequenceResult(True, False, "pu_missing")
        if current.previous_update_id != previous_update_id:
            return SequenceResult(True, False, "pu_mismatch")
        return SequenceResult()


class BybitSequenceComparator(SequenceComparator):
    def check(self, current, previous):
        if previous is None:
            return SequenceResult()
        if (current.update_id is not None and previous.update_id is not None
                and current.update_id < previous.update_id):
            return SequenceResult(False, True, "update_id_decrease_or_reset")
        if current.update_id == previous.update_id:
            return SequenceResult(False, False, "duplicate_update", is_stale=True)
        return SequenceResult()


class OKXSequenceComparator(SequenceComparator):
    def check(self, current, previous):
        if current.previous_update_id == -1:
            return SequenceResult()
        if previous is None:
            return SequenceResult(True, False, "missing_snapshot")
        if current.previous_update_id != previous.update_id:
            return SequenceResult(True, False, "prev_seq_id_mismatch")
        return SequenceResult()


class SpotSequenceComparator(SequenceComparator):
    """Binance **Spot** diff-depth continuity -- distinct from
    :class:`BinanceSequenceComparator` (USD-M futures).

    Verified 2026-09-19 against the official Spot procedure
    ("How to manage a local order book correctly",
    github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md,
    mirrored at developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).
    Spot's ``depthUpdate`` payload has no ``pu`` field at all (confirmed
    absent from the official payload example, unlike futures) -- continuity
    is instead verified arithmetically: *"each new event's U should be equal
    to the previous event's u+1"*. This is why Spot needs its own
    comparator rather than reusing Binance USD-M's ``pu``-based one: there
    is no ``pu`` to compare.

    No stale/duplicate carve-out. ``BinanceSequenceComparator`` treats a
    non-advancing ``u`` as harmless because the official futures procedure
    documents ``pu``-chain verification as the sole continuity check, so a
    retransmitted event with a matching ``pu`` legitimately passes it. Spot's
    official procedure states the ``U == prev.u + 1`` check directly with no
    documented exception for a retransmitted or duplicate event -- so a
    non-advancing event here fails that check like any other break, and is
    treated as a gap rather than assumed harmless. Absolute-quantity
    semantics (steps 7/8, identical to futures) still hold, but nothing in
    the official Spot procedure says a duplicate is exempt from the chain
    check itself, and this comparator does not invent that exemption.
    """

    def check(self, current, previous):
        if previous is None:
            return SequenceResult()
        current_first = getattr(current, "first_update_id", None)
        previous_update = getattr(previous, "update_id", None)
        if not isinstance(current_first, int) or not isinstance(previous_update, int):
            return SequenceResult(True, False, "update_id_missing")
        if current_first != previous_update + 1:
            return SequenceResult(True, False, "u_chain_broken")
        return SequenceResult()


def binance_snapshot_bridge(event: Any, last_update_id: int) -> bool:
    """Documented step 5: ``U <= lastUpdateId AND u >= lastUpdateId``.

    Deliberately no Spot-style ``+1`` on either side; USD-M and Spot differ
    here and conflating them silently shifts the bridge by one event.

    Returns ``False`` rather than raising when the ids are not integers.
    ``LocalBook.binance_snapshot`` validates them before calling, but this is
    also reachable through the public ``BinanceAdapter.bridge_accepts``, and a
    predicate that raises on malformed input turns a data problem into a
    crashed ingest task.
    """
    first_update_id = getattr(event, "first_update_id", None)
    update_id = getattr(event, "update_id", None)
    if not isinstance(first_update_id, int) or not isinstance(update_id, int):
        return False
    if not isinstance(last_update_id, int):
        return False
    return first_update_id <= last_update_id <= update_id


def binance_spot_snapshot_bridge(event: Any, last_update_id: int) -> bool:
    """Spot's documented bridge step: ``U <= lastUpdateId+1 AND u >= lastUpdateId+1``.

    The ``+1`` on both sides is the one-token difference from
    :func:`binance_snapshot_bridge` (USD-M has none) -- confirmed against
    the same official source as :class:`SpotSequenceComparator`. Getting
    this wrong silently shifts which buffered event is treated as the
    bridge by exactly one event, which is exactly the kind of off-by-one
    that looks like a working book until the first genuine gap.
    """
    first_update_id = getattr(event, "first_update_id", None)
    update_id = getattr(event, "update_id", None)
    if not isinstance(first_update_id, int) or not isinstance(update_id, int):
        return False
    if not isinstance(last_update_id, int):
        return False
    return first_update_id <= last_update_id + 1 <= update_id
