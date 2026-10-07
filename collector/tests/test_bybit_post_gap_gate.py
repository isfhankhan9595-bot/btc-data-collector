"""Bybit: once the book is untrusted, only a fresh wire snapshot restores it.

Defect (PR #89 follow-up): after a malformed/unprovable event moved the book to
SEQUENCE_GAP/RECOVERING, a later ordinary higher-``u`` delta still passed the
comparator, mutated the book, was returned, and was persisted by
``BybitCollectorApp._apply_orderbook`` -- whose orderbook rows carry no
``quality_state``, so the row looked authoritative.

Preserved Bybit contract: increasing ``u`` jumps allowed, decrease => resync,
equal => duplicate, missing/non-int ``u`` => gap, wire snapshot => recovery.
"""
import json
from dataclasses import replace
from decimal import Decimal

import pyarrow.parquet as pq

from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.book_engine import LocalBook
from collector.collector.quality_events import BookQuality
from collector.tests.test_bybit_collector import _app, _drive
from collector.tests.test_bybit_sequence_contract import _feed, _wire, snap

VALID, GAP, RECOVERING = BookQuality.VALID, BookQuality.SEQUENCE_GAP, BookQuality.RECOVERING


def _book_snapshot(book):
    return (dict(book.bids), dict(book.asks), book.previous)


# ---------------------------------------------------------------- LocalBook

def test_post_gap_higher_u_delta_is_not_applied_or_returned_after_missing_u():
    book, out = _feed(snap(), _wire(101, bid="100.1"), _wire(None, bid="100.2"))
    assert book.state.state is GAP and book.last_reason == "update_id_missing"
    frozen = _book_snapshot(book)

    adapter = BybitAdapter()
    later = adapter.normalize(_wire(102, bid="100.9"), local_receive_ts=5000)[0]
    assert book.apply(later) is None                       # not returned
    assert _book_snapshot(book) == frozen                   # contents unchanged
    assert book.state.state is GAP                          # no flapping
    assert book.last_reason == "update_id_missing"          # original cause kept
    assert book.untrusted_discard_count == 1


def test_post_resync_higher_u_delta_is_not_applied_after_decrease():
    book, _ = _feed(snap(), _wire(101, bid="100.1"), _wire(99, bid="100.9"))
    assert book.state.state is RECOVERING
    frozen = _book_snapshot(book)
    event = BybitAdapter().normalize(_wire(102, bid="100.5"), local_receive_ts=5000)[0]
    assert book.apply(event) is None
    assert _book_snapshot(book) == frozen and book.state.state is RECOVERING


def test_many_post_gap_deltas_never_mutate_or_transition():
    book, _ = _feed(snap(), _wire(None))
    frozen = _book_snapshot(book)
    adapter = BybitAdapter()
    for u in (101, 105, 103, 50, 200):                      # incl. decreases/dups
        assert book.apply(adapter.normalize(_wire(u, bid="100.4"), local_receive_ts=5000)[0]) is None
        assert book.state.state is GAP
    assert _book_snapshot(book) == frozen and book.untrusted_discard_count == 5


def test_delta_before_any_snapshot_is_discarded():
    book, out = _feed(_wire(101, bid="100.1"))
    assert out == [None] and book.bids == {} and book.state.state is RECOVERING


def test_delta_after_invalidate_is_discarded_until_snapshot():
    book, _ = _feed(snap(), _wire(101, bid="100.1"))
    book.invalidate("reconnect")
    frozen = _book_snapshot(book)
    event = BybitAdapter().normalize(_wire(102, bid="100.7"), local_receive_ts=5000)[0]
    assert book.apply(event) is None and _book_snapshot(book) == frozen


def test_fresh_snapshot_restores_valid_and_first_delta_is_accepted():
    book, _ = _feed(snap(), _wire(101, bid="100.1"), _wire(None), _wire(102, bid="100.9"))
    assert book.state.state is GAP and str(max(book.bids)) == "100.1"
    adapter = BybitAdapter()
    restored = book.apply(adapter.normalize(snap(200, bid="200.0", a=[["201.0", "1"]]), local_receive_ts=5000)[0])
    assert restored is not None and book.state.state is VALID
    assert [str(p) for p in book.bids] == ["200.0"]
    first = book.apply(adapter.normalize(_wire(201, bid="200.5", a=[["201.0", "1"]]), local_receive_ts=5000)[0])
    assert first is not None and first.quality_state == "VALID"
    assert str(max(book.bids)) == "200.5"


def test_contract_preserved_jump_still_accepted_while_valid():
    book, out = _feed(snap(), _wire(150, bid="100.5"))
    assert out[1] is not None and book.state.state is VALID and book.untrusted_discard_count == 0


def test_other_venues_do_not_use_the_bybit_gate():
    for venue in ("BINANCE", "BINANCE_SPOT", "OKX"):
        assert LocalBook(venue).untrusted_discard_count == 0
    # Binance still buffers (not discards) while non-VALID.
    from collector.collector.canonical import CanonicalOrderBookEvent
    binance = LocalBook("BINANCE")
    diff = CanonicalOrderBookEvent("BINANCE", "orderbook", 1, None, 1, bids=((Decimal("100"), Decimal("1")),),
                                   asks=((Decimal("101"), Decimal("1")),), update_id=5, first_update_id=5)
    assert binance.apply(diff) is None
    assert len(binance.buffer) == 1 and binance.untrusted_discard_count == 0


# ------------------------------------------------- runner / persistence path

def _frame(typ, u, bid, qty, ts):
    data = {"s": "BTCUSDT", "b": [[bid, qty]], "a": [["50001", "1.0"]], "seq": ts}
    if u is not None:
        data["u"] = u
    return json.dumps({"topic": "orderbook.50.BTCUSDT", "type": typ, "ts": ts, "data": data})


def _quality_rows(tmp_path):
    rows = []
    for f in sorted((tmp_path / "raw" / "bybit_quality_events").glob("*.seg")):
        rows.extend(pq.read_table(f).to_pylist())
    return [r for r in rows if r.get("stream") == "bybit_orderbook"]


def test_runner_never_persists_post_gap_rows_and_records_transitions(tmp_path):
    app = _app(tmp_path)
    _drive(app, [
        _frame("snapshot", 10, "50000", "1.0", 1000),
        _frame("delta", 11, "50000", "2.0", 1010),
        _frame("delta", None, "50000", "3.0", 1020),     # malformed -> GAP
        _frame("delta", 12, "50000", "9.0", 1030),        # post-gap ordinary delta
        _frame("delta", 13, "50000", "8.0", 1040),
    ])
    assert app.book.state.state is GAP
    assert app.book.bids[Decimal("50000")] == Decimal("2.0")            # never mutated
    assert [r["update_id"] for r in app.ob_writer.buffer] == [10, 11]   # no post-gap row
    assert all(r["bids_qty"][0] != 9.0 and r["bids_qty"][0] != 8.0 for r in app.ob_writer.buffer)

    rows = _quality_rows(tmp_path)
    transitions = [(r["event_type"], r["previous_state"], r["new_state"], r["reason"]) for r in rows]
    assert transitions.count(("SEQUENCE_GAP", "VALID", "SEQUENCE_GAP", "update_id_missing")) == 1
    assert len(transitions) == 2        # snapshot (RECOVERING->VALID) + one gap; discards add none


def test_runner_recovers_only_via_snapshot_and_resumes_persisting(tmp_path):
    app = _app(tmp_path)
    _drive(app, [
        _frame("snapshot", 10, "50000", "1.0", 1000),
        _frame("delta", None, "50000", "3.0", 1010),     # -> GAP
        _frame("delta", 12, "50000", "9.0", 1020),        # discarded
        _frame("snapshot", 20, "50000", "5.0", 1030),     # fresh snapshot
        _frame("delta", 21, "50000", "6.0", 1040),        # first valid delta
    ])
    assert app.book.state.state is VALID
    assert [r["update_id"] for r in app.ob_writer.buffer] == [10, 20, 21]
    assert app.ob_writer.buffer[-1]["bids_qty"][0] == 6.0
    types = [(r["event_type"], r["new_state"]) for r in _quality_rows(tmp_path)]
    assert ("SEQUENCE_GAP", "SEQUENCE_GAP") in types and ("RECOVERY", "VALID") in types


def test_persistence_boundary_refuses_untrusted_state_independently(tmp_path):
    """Defence in depth: even if LocalBook.apply() returned an untrusted book,
    _apply_orderbook must not write it (rows carry no quality_state)."""
    app = _app(tmp_path)
    real_apply = app.book.apply

    def leaky_apply(event):
        out = real_apply(event)
        return None if out is None else replace(out, quality_state="SEQUENCE_GAP")
    app.book.apply = leaky_apply
    _drive(app, [_frame("snapshot", 10, "50000", "1.0", 1000)])
    assert app.ob_writer.buffer == []
