"""Bybit order-book sequence contract (real BybitAdapter + LocalBook).

Contract preserved (docs/EXECUTION_STATUS.md, Phase 7): ``u`` is monotonic, an
increasing jump is NOT a gap (no Binance ``pu``/``+1`` chain), a decrease is a
resync signal, an equal ``u`` is a dropped duplicate, and a wire snapshot
recovers. Added fail-closed rule: if monotonicity cannot be evaluated (``u``
missing / non-integer on either event) the book must not stay VALID.

Deltas use non-crossing prices so the crossed-book guard cannot mask the
sequence behaviour (the older ``..._recovery_bybit`` test passes only because
its ``bid=150`` crosses ``ask=101``).
"""
from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.book_engine import LocalBook
from collector.collector.quality_events import BookQuality

VALID, GAP, RECOVERING = (BookQuality.VALID, BookQuality.SEQUENCE_GAP, BookQuality.RECOVERING)


def _wire(u, typ="delta", bid="100.0", **data_over):
    data = {"seq": u, "b": [[bid, "2"]], "a": [["101.0", "1"]]}
    if u is not None:
        data["u"] = u
    data.update(data_over)
    return {"topic": "orderbook.50.BTCUSDT", "type": typ, "ts": 1000, "data": data}


def _feed(*frames):
    book, adapter, applied = LocalBook("BYBIT"), BybitAdapter(), []
    for frame in frames:
        event = adapter.normalize(frame, local_receive_ts=5000)[0]
        applied.append(book.apply(event))
    return book, applied


def snap(u=100, **kw):
    return _wire(u, "snapshot", **kw)


def test_snapshot_then_contiguous_delta_is_valid_and_applied():
    book, applied = _feed(snap(), _wire(101, bid="100.1"))
    assert book.state.state is VALID and applied[1] is not None
    assert max(book.bids) == __import__("decimal").Decimal("100.1")


def test_duplicate_is_dropped_without_state_change_or_double_apply():
    book, applied = _feed(snap(), _wire(101, bid="100.1"), _wire(101, bid="100.9"))
    assert applied[2] is None and book.state.state is VALID
    assert book.last_reason == "duplicate_update" and book.stale_count == 1
    assert str(max(book.bids)) == "100.1"


def test_stale_decrease_is_resync_not_applied():
    book, applied = _feed(snap(), _wire(101, bid="100.1"), _wire(99, bid="100.9"))
    assert applied[2] is None and book.state.state is RECOVERING
    assert book.last_reason == "update_id_decrease_or_reset"
    assert str(max(book.bids)) == "100.1"


def test_out_of_order_after_advance_is_resync():
    book, applied = _feed(snap(), _wire(101, bid="100.1"), _wire(103, bid="100.3"),
                          _wire(102, bid="100.2"))
    assert applied[2] is not None          # 101 -> 103 accepted by contract
    assert applied[3] is None and book.state.state is RECOVERING


def test_increasing_jumps_are_accepted_by_documented_contract():
    """Pins the preserved Bybit semantics: one-step and multi-step jumps are
    NOT gaps (u is monotonic, not consecutive). If this ever changes it must
    be a deliberate, documented contract change, not a Binance copy."""
    for jump in (102, 150):
        book, applied = _feed(snap(), _wire(jump, bid="100.5"))
        assert book.state.state is VALID and applied[1] is not None


def test_snapshot_then_invalid_crossed_delta_is_not_valid():
    book, applied = _feed(snap(), _wire(101, bid="150.0"))
    assert applied[1] is None and book.state.state is GAP
    assert book.last_reason == "invalid_book"


def test_recovery_after_resync_by_wire_snapshot():
    book, applied = _feed(snap(), _wire(101, bid="100.1"), _wire(99), snap(200, bid="200.0",
                          a=[["201.0", "1"]]))
    assert book.state.state is VALID and book.previous.update_id == 200


def test_u_equals_1_restart_snapshot_overwrites_book():
    book, _ = _feed(snap(500), _wire(501, bid="100.1"), snap(1, bid="90.0", a=[["91.0", "1"]]))
    assert book.state.state is VALID and book.previous.update_id == 1
    assert [str(p) for p in book.bids] == ["90.0"]


# ---- fail-closed: monotonicity unprovable --------------------------------

def test_delta_missing_u_is_gap_not_valid():
    book, applied = _feed(snap(), _wire(None, bid="100.1"))
    assert applied[1] is None and book.state.state is GAP
    assert book.last_reason == "update_id_missing"
    assert str(max(book.bids)) == "100.0"      # delta not applied


def test_delta_non_integer_u_does_not_crash_and_is_gap():
    book, applied = _feed(snap(), {**_wire(101, bid="100.1"), "data": {**_wire(101, bid="100.1")["data"], "u": "101"}})
    assert applied[1] is None and book.state.state is GAP
    assert book.last_reason == "update_id_missing"


def test_snapshot_without_u_makes_following_delta_unprovable():
    book, applied = _feed(snap(None), _wire(101, bid="100.1"))
    assert applied[1] is None and book.state.state is GAP


def test_two_missing_ids_are_not_silently_dropped_as_duplicate():
    book, applied = _feed(snap(None), _wire(None, bid="100.1"))
    assert book.state.state is GAP and book.last_reason == "update_id_missing"


def test_gap_from_missing_u_recovers_only_via_snapshot():
    book, _ = _feed(snap(), _wire(None), snap(300, bid="100.2"))
    assert book.state.state is VALID and book.previous.update_id == 300
