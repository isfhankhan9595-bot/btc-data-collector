from unittest.mock import MagicMock

from collector import run_collector as _run_collector
from collector.collector.book_engine import LocalBook
from collector.collector.canonical import CanonicalOrderBookEvent
from collector.collector.quality_events import QualityEventType
from collector.collector.validator import Validator


CollectorApp = _run_collector.CollectorApp


def _app_for_quality_test():
    app = CollectorApp.__new__(CollectorApp)
    app.quality_writer = MagicMock()
    app.binance_book = LocalBook("BINANCE", max_buffer_events=2)
    app.validator = Validator()
    return app


def test_drain_persists_clock_anomaly_and_clears_validator_queue():
    app = _app_for_quality_test()
    valid, reason = app.validator.validate_trade(
        {
            "timestamp": 20_000,
            "exchange_timestamp": 1_000,
            "trade_id": 1,
            "price": 100.0,
            "quantity": 1.0,
        }
    )
    assert valid, reason
    assert len(app.validator.quality_events) == 1

    app._drain_integrity_quality_events()

    assert app.validator.quality_events == []
    rows = [call.args[0] for call in app.quality_writer.write.call_args_list]
    assert len(rows) == 1
    assert rows[0]["event_type"] == QualityEventType.CLOCK_ANOMALY.value
    assert rows[0]["reason"] == "clock_drift"
    assert rows[0]["gap_size_ms"] == 19_000


def test_drain_persists_buffer_overflow_with_rows_lost_and_clears_book_queue():
    app = _app_for_quality_test()

    def event(update_id):
        return CanonicalOrderBookEvent(
            "BINANCE", "orderbook", update_id, update_id - 1, update_id,
            bids=((100.0, 1.0),), asks=((101.0, 1.0),),
            update_id=update_id,
        )

    app.binance_book._buffer_event(event(1))
    app.binance_book._buffer_event(event(2))
    app.binance_book._buffer_event(event(3))

    assert len(app.binance_book.quality_events) == 1
    app._drain_integrity_quality_events()

    assert app.binance_book.quality_events == []
    rows = [call.args[0] for call in app.quality_writer.write.call_args_list]
    assert len(rows) == 1
    assert rows[0]["event_type"] == QualityEventType.BUFFER_OVERFLOW.value
    assert rows[0]["reason"] == "buffer_overflow"
    assert rows[0]["rows_lost"] == "2"
