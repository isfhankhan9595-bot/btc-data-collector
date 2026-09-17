from decimal import Decimal

import pytest

from collector.collector.book_engine import LocalBook
from collector.collector.canonical import CanonicalOrderBookEvent
from collector.collector.sequence import binance_snapshot_bridge


def _diff(update_id: int, previous_update_id: int | None = None, first_update_id: int | None = None) -> CanonicalOrderBookEvent:
    return CanonicalOrderBookEvent(
        exchange="BINANCE",
        stream="orderbook",
        exchange_event_ts=update_id,
        exchange_transaction_ts=None,
        local_receive_ts=update_id,
        bids=((Decimal("100"), Decimal("1")),),
        asks=((Decimal("101"), Decimal("1")),),
        update_id=update_id,
        first_update_id=update_id if first_update_id is None else first_update_id,
        previous_update_id=previous_update_id,
    )


def _snapshot(update_id: int) -> CanonicalOrderBookEvent:
    return CanonicalOrderBookEvent(
        exchange="BINANCE",
        stream="orderbook",
        exchange_event_ts=update_id,
        exchange_transaction_ts=None,
        local_receive_ts=update_id,
        bids=((Decimal("100"), Decimal("1")),),
        asks=((Decimal("101"), Decimal("1")),),
        update_id=update_id,
        first_update_id=update_id,
        is_snapshot=True,
        book_source="REST_SNAPSHOT",
    )


def test_binance_recovery_buffer_overflow_discards_trigger_and_requires_new_bridge():
    book = LocalBook("BINANCE", max_buffer_events=2)

    assert book.apply(_diff(1)) is None
    assert book.apply(_diff(2, 1)) is None
    assert len(book.buffer) == 2

    # max+1 is the critical boundary: the triggering event is NOT retained.
    assert book.apply(_diff(3, 2)) is None
    assert len(book.buffer) == 0
    assert book.buffer_overflow_count == 1
    assert book.last_reason == "buffer_overflow"
    assert book.buffer_overflowed is True

    # A snapshot cannot manufacture continuity from the discarded chain.
    assert book.binance_snapshot(3, _snapshot(3)) is False
    assert book.state.state.value == "RECOVERING"

    # Events arriving after overflow form a new candidate chain and may bridge
    # a later snapshot. This is the only path back to VALID.
    assert book.apply(_diff(4, 3)) is None
    assert len(book.buffer) == 1
    assert book.binance_snapshot(4, _snapshot(4)) is True
    assert book.state.state.value == "VALID"
    assert book.buffer == []
    assert book.buffer_overflowed is False


def test_binance_buffer_capacity_must_be_positive():
    with pytest.raises(ValueError, match="max_buffer_events"):
        LocalBook("BINANCE", max_buffer_events=0)


def test_binance_snapshot_bridge_uses_usdm_futures_rule():
    # USD-M Futures bridge: U <= lastUpdateId <= u.
    assert binance_snapshot_bridge(_diff(105, first_update_id=100), 105) is True
    assert binance_snapshot_bridge(_diff(105, first_update_id=100), 104) is True
    assert binance_snapshot_bridge(_diff(104, first_update_id=105), 105) is False


def test_binance_snapshot_bridge_does_not_use_spot_plus_one_rule():
    # Under the incorrect Spot-style rule, a first update beginning at 101
    # would be treated as the bridge for snapshot 100. USD-M accepts only when
    # the update range actually covers the snapshot update ID.
    assert binance_snapshot_bridge(_diff(101, first_update_id=101), 100) is False
