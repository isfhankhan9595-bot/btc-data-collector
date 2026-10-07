"""P1: an untrusted OKX order book must not mutate until a fresh snapshot.

OKX ``books`` sequence contract (OKX docs-v5, "Order book channel" ->
"Sequence ID"; what the code in ``OKXSequenceComparator`` implements):

* snapshot: ``prevSeqId == -1``;
* update: ``prevSeqId`` must equal the ``seqId`` of the last message accepted;
* ``seqId`` is generally larger than ``prevSeqId`` but is NOT required to
  increase: an empty keepalive carries ``prevSeqId == seqId`` (== the previous
  ``seqId``), and a maintenance reset delivers ``seqId < prevSeqId`` with the
  linkage still intact;
* there is no "stale/duplicate" class and no REST bridge: a ``prevSeqId`` that
  does not link is a gap, and only a fresh wire snapshot restores authority.

Defect pinned here: after a gap, ``LocalBook.apply`` left ``previous`` at the
last accepted event, so a later delta whose ``prevSeqId`` linked to that stale
``previous`` was applied and returned while ``state`` was still SEQUENCE_GAP. A
re-delivered/duplicate frame followed by the true in-order continuation is a
realistic way to reach it.

Every OKX fixture is real OKX wire JSON through the real ``OKXAdapter`` and
``LocalBook``; nothing re-implements OKX semantics.
"""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from collector.collector.adapters.okx import OKXAdapter
from collector.collector.book_engine import LocalBook
from collector.collector.book_observation import reconstruct_book_at
from collector.collector.canonical import CanonicalOrderBookEvent
from collector.collector.quality_events import BookQuality, QualityEventType
from collector.collector.replay import FrameKind, ReplayEngine, ReplayFrame, ReplaySource
from collector.run_okx_collector import OKXCollectorApp

BASE_TS = 1_780_777_777_000
VALID = BookQuality.VALID.value
GAP = BookQuality.SEQUENCE_GAP.value


def _wire(seq, prev, bid="65000.0", ask="65001.0", *, bids=None, asks=None):
    return {
        "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
        "data": [{
            "ts": str(BASE_TS + seq), "seqId": seq, "prevSeqId": prev,
            "bids": [[bid, "1.0"]] if bids is None else bids,
            "asks": [[ask, "1.0"]] if asks is None else asks,
        }],
    }


class _Session:
    def __init__(self, **book_kwargs):
        self.adapter = OKXAdapter()
        self.book = LocalBook("OKX", **book_kwargs)

    def feed(self, seq, prev, **kw):
        event = self.adapter.normalize(_wire(seq, prev, **kw), local_receive_ts=BASE_TS + seq)[0]
        return self.book.apply(event)

    def bid_prices(self):
        return {str(p) for p in self.book.bids}


def _trusted_session():
    s = _Session()
    assert s.feed(100, -1) is not None
    assert s.feed(101, 100, bid="65000.1") is not None
    assert s.book.state.state is BookQuality.VALID
    return s


# ---------------------------------------------------------------------------
# Core invariant.
# ---------------------------------------------------------------------------


def test_delta_chaining_to_the_stale_previous_after_a_gap_is_discarded():
    s = _trusted_session()
    assert s.feed(105, 104, bid="65000.5") is None            # 104 missed: gap
    assert s.book.state.state is BookQuality.SEQUENCE_GAP
    before = dict(s.book.bids)

    out = s.feed(102, 101, bid="65000.2")                      # links to stale previous
    assert out is None
    assert dict(s.book.bids) == before
    assert s.book.previous.update_id == 101
    assert s.book.state.state is BookQuality.SEQUENCE_GAP
    assert s.book.okx_untrusted_discard_count == 1


def test_in_order_continuation_after_a_duplicate_triggered_gap_is_discarded():
    """The realistic route: an exact re-delivery is a gap for OKX (its
    prevSeqId no longer equals the book's seqId), and the next genuine
    in-order message then chains to ``previous``."""
    s = _trusted_session()
    assert s.feed(101, 100, bid="65000.1") is None             # re-delivered 101
    assert s.book.state.state is BookQuality.SEQUENCE_GAP
    assert s.feed(102, 101, bid="65000.2") is None             # true continuation
    assert s.feed(103, 102, bid="65000.3") is None
    assert s.bid_prices() == {"65000.0", "65000.1"}
    assert s.book.previous.update_id == 101


def test_no_post_gap_delta_is_ever_returned_as_an_applied_book():
    s = _trusted_session()
    s.feed(105, 104)
    for seq, prev in [(106, 105), (102, 101), (103, 102), (104, 103), (200, 199)]:
        out = s.feed(seq, prev, bid=f"{65000 + seq}.0")
        assert out is None, f"post-gap delta {seq}/{prev} was returned as applied"


def test_crossed_book_invalid_delta_then_chaining_delta_is_discarded():
    s = _trusted_session()
    # bid above the best ask: rejected as invalid_book -> gap, previous unchanged
    assert s.feed(102, 101, bid="70000.0") is None
    assert s.book.last_reason == "invalid_book"
    assert s.book.state.state is BookQuality.SEQUENCE_GAP
    # a different 102 that would be perfectly valid on its own must still not apply
    assert s.feed(102, 101, bid="65000.2") is None
    assert s.bid_prices() == {"65000.0", "65000.1"}


def test_fresh_snapshot_restores_authority_and_the_new_chain_applies():
    s = _trusted_session()
    s.feed(105, 104)
    s.feed(102, 101)                                           # discarded
    snap = s.feed(900, -1, bid="70000.0", ask="70001.0")
    assert snap is not None and snap.quality_state == VALID
    assert s.book.state.state is BookQuality.VALID
    assert s.bid_prices() == {"70000.0"}                       # old levels gone
    nxt = s.feed(901, 900, bid="70000.1", ask="70001.0")
    assert nxt is not None and nxt.quality_state == VALID
    assert s.bid_prices() == {"70000.0", "70000.1"}


def test_untrusted_okx_deltas_are_not_buffered_so_overflow_never_fires():
    """OKX has no bridge that consumes a buffer. Without the gate every
    post-gap delta was buffered until BUFFER_OVERFLOW was reported."""
    s = _Session(max_buffer_events=3)
    s.feed(100, -1)
    s.feed(101, 100)
    s.feed(105, 104)                                           # the gap itself
    buffered = len(s.book.buffer)
    for i in range(30):
        s.feed(300 + i, 299 + i)
    assert len(s.book.buffer) == buffered
    assert s.book.buffer_overflow_count == 0
    assert not any(q.event_type is QualityEventType.BUFFER_OVERFLOW
                   for q in s.book.drain_quality_events())


# ---------------------------------------------------------------------------
# The gate must not reject anything the OKX contract says is valid.
# ---------------------------------------------------------------------------


def test_contract_keepalive_with_equal_seqid_and_prevseqid_keeps_the_book_valid():
    s = _trusted_session()
    out = s.feed(101, 101, bids=[], asks=[])                   # no update: prev == seq
    assert out is not None and out.quality_state == VALID
    assert s.book.state.state is BookQuality.VALID


def test_contract_sequence_reset_with_intact_linkage_keeps_the_book_valid():
    s = _Session()
    s.feed(10, -1)
    assert s.feed(15, 10, bid="65000.1") is not None           # normal update
    assert s.feed(15, 15, bids=[], asks=[]) is not None        # no update
    reset = s.feed(3, 15, bid="65000.2")                       # seqId < prevSeqId
    assert reset is not None and reset.quality_state == VALID
    after = s.feed(5, 3, bid="65000.3")                        # prevSeqId == 3
    assert after is not None and after.quality_state == VALID
    assert s.book.state.state is BookQuality.VALID
    assert s.book.okx_untrusted_discard_count == 0


def test_delta_before_any_snapshot_is_still_recorded_as_missing_snapshot_gap():
    """Existing behaviour outside the gate: with no ``previous`` the
    comparator already refuses the delta, and the reason is recorded."""
    s = _Session()
    assert s.feed(501, 500) is None
    assert s.book.last_reason == "missing_snapshot"
    assert s.book.state.state is BookQuality.SEQUENCE_GAP
    assert s.book.previous is None and not s.book.bids and not s.book.asks
    assert s.book.okx_untrusted_discard_count == 0


# ---------------------------------------------------------------------------
# Replay / observation path (the consumers of LocalBook for OKX).
# ---------------------------------------------------------------------------


def _frame(index, seq, prev, **kw):
    return ReplayFrame(
        timestamp_ms=BASE_TS + index, kind=FrameKind.WIRE, source_index=index,
        payload=json.dumps(_wire(seq, prev, **kw)))


def test_replay_never_records_a_book_update_while_the_book_is_untrusted():
    frames = [
        _frame(0, 100, -1),
        _frame(1, 101, 100, bid="65000.1"),
        _frame(2, 101, 100, bid="65000.1"),                    # re-delivery -> gap
        _frame(3, 102, 101, bid="65000.2"),                    # in-order continuation
        _frame(4, 103, 102, bid="65000.3"),
    ]
    engine = ReplayEngine(venue="OKX")
    engine.run(ReplaySource(frames))
    assert [u.update_id for u in engine.result.book_updates] == [100, 101]
    assert all(u.quality_state == VALID for u in engine.result.book_updates)
    assert engine.book.state.state is BookQuality.SEQUENCE_GAP


def test_observation_after_gap_reports_degraded_quality_and_the_pre_gap_book():
    frames = [
        _frame(0, 100, -1),
        _frame(1, 101, 100, bid="65000.1"),
        _frame(2, 101, 100, bid="65000.1"),
        _frame(3, 102, 101, bid="65000.2"),
    ]
    obs = reconstruct_book_at(frames, BASE_TS + 10, venue="OKX")
    assert obs.quality_state == GAP
    assert {p for p, _ in obs.bids} == {Decimal("65000.0"), Decimal("65000.1")}


def test_observation_recovers_only_through_a_fresh_snapshot():
    frames = [
        _frame(0, 100, -1),
        _frame(1, 105, 104),                                   # gap
        _frame(2, 106, 105),                                   # discarded
        _frame(3, 900, -1, bid="70000.0", ask="70001.0"),
        _frame(4, 901, 900, bid="70000.1", ask="70001.0"),
    ]
    obs = reconstruct_book_at(frames, BASE_TS + 10, venue="OKX")
    assert obs.quality_state == VALID
    assert {p for p, _ in obs.bids} == {Decimal("70000.0"), Decimal("70000.1")}


# ---------------------------------------------------------------------------
# Runner level: run_okx_collector cannot persist a reconstructed OKX book.
# ---------------------------------------------------------------------------


async def test_runner_never_writes_a_book_row_through_a_gap_and_post_gap_delta(tmp_path):
    app = OKXCollectorApp(data_dir=str(tmp_path))
    writers = [app.trades_writer, app.trades_all_writer, app.mark_writer, app.index_writer,
               app.funding_writer, app.oi_writer, app.liq_writer]
    writes = []
    for w in writers:
        w.write = lambda row, _w=w: writes.append((_w.stream_name, row))
    try:
        for seq, prev in [(100, -1), (101, 100), (105, 104), (102, 101), (103, 102)]:
            await app._handle_message(_wire(seq, prev), BASE_TS + seq)
    finally:
        app.quality_writer.close()
        app.raw_wire_writer.close()
    assert writes == []
    assert not (tmp_path / "raw" / "okx_orderbook").exists()
    assert not hasattr(app, "book") and not hasattr(app, "ob_writer")


# ---------------------------------------------------------------------------
# Differential: the gate is OKX-scoped and does not touch other venues.
# ---------------------------------------------------------------------------


def _canonical(venue, update_id, prev=None, first=None, *, snapshot=False, bid="100", ask="101"):
    return CanonicalOrderBookEvent(
        venue, "orderbook", BASE_TS + update_id, None, BASE_TS + update_id,
        bids=((Decimal(bid), Decimal("1")),), asks=((Decimal(ask), Decimal("1")),),
        update_id=update_id, first_update_id=first, previous_update_id=prev,
        is_snapshot=snapshot)


@pytest.mark.parametrize("venue", ["BINANCE", "BINANCE_SPOT", "BYBIT"])
def test_non_okx_venues_never_reach_the_okx_gate(venue):
    book = LocalBook(venue)
    book.apply(_canonical(venue, 10, snapshot=(venue == "BYBIT"), first=10))
    book.apply(_canonical(venue, 11, prev=10, first=11, bid="100.5"))
    book.apply(_canonical(venue, 5, prev=99, first=5, bid="100.6"))    # decrease / break
    book.apply(_canonical(venue, 12, prev=11, first=12, bid="100.7"))
    assert book.okx_untrusted_discard_count == 0


def test_okx_gate_does_not_apply_to_a_non_okx_book_with_the_same_wire_shape():
    """Same canonical stream through a BINANCE book is buffered by its own
    (unchanged) un-bridged path, not discarded by the OKX gate."""
    book = LocalBook("BINANCE")
    book.apply(_canonical("BINANCE", 11, prev=10, first=11))
    assert len(book.buffer) == 1
    assert book.okx_untrusted_discard_count == 0
