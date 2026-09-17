from decimal import Decimal

import pytest

from collector.collector.book_engine import LocalBook
from collector.collector.canonical import CanonicalOrderBookEvent


def _diff(update_id: int, previous_update_id: int | None = None) -> CanonicalOrderBookEvent:
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
        previous_update_id=previous_update_id,
    )


def test_binance_recovery_buffer_is_bounded_and_forces_resync():
    book = LocalBook("BINANCE", max_buffer_events=2)

    assert book.apply(_diff(1)) is None
    assert len(book.buffer) == 1
    assert book.apply(_diff(2, 1)) is None
    assert len(book.buffer) == 2

    # The third event cannot silently extend an unbounded recovery queue. The
    # old untrusted chain is discarded and a fresh snapshot recovery is forced.
    assert book.apply(_diff(3, 2)) is None
    assert len(book.buffer) == 1
    assert book.buffer_overflow_count == 1
    assert book.last_reason == "buffer_overflow"


def test_binance_buffer_capacity_must_be_positive():
    with pytest.raises(ValueError, match="max_buffer_events"):
        LocalBook("BINANCE", max_buffer_events=0)
