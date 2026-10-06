"""P1 MarketStateEngine correctness + causality audit -- adversarial tests.

Every test here corresponds to a defect REPRODUCED against unmodified ``main``
(385a6e6) or to an invariant that was verified and is now pinned. Where a
defect was reachable through a real producer (``LocalBook``, the Bybit/OKX
adapters) the test drives that real producer rather than a hand-built event.

Each protected invariant was also mutation-checked (see the PR description):
the production behavior was deliberately broken and the named test failed.
"""
from __future__ import annotations

import ast
import itertools
import random
from decimal import Decimal as D
from pathlib import Path

import pytest

from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.adapters.okx import OKXAdapter
from collector.collector.book_engine import LocalBook
from collector.collector.canonical import (
    CanonicalLiquidationEvent,
    CanonicalMarkPriceEvent,
    CanonicalOIEvent,
    CanonicalOrderBookEvent,
    CanonicalTradeEvent,
    OIUnit,
)
from collector.collector.market_state import MarketStateEngine

T = 1_780_000_000_000
H = 3_600_000


# --------------------------------------------------------------------------
# builders
# --------------------------------------------------------------------------

def bk(ts, bids=((D("100"), D("1")),), asks=((D("101"), D("1")),), *, exchange="BYBIT",
       quality_state="VALID", book_source="DIFF_DEPTH_RECONSTRUCTED", update_id=None,
       exchange_ts=None):
    return CanonicalOrderBookEvent(
        exchange, "orderbook", ts if exchange_ts is None else exchange_ts, None, ts,
        quality_state=quality_state, bids=tuple(bids), asks=tuple(asks),
        update_id=update_id, book_source=book_source)


def tr(ts, price="100", qty="1", side="Buy", *, exchange="BYBIT", seq=None, exchange_ts=None,
       trade_id=None):
    return CanonicalTradeEvent(
        exchange, "trades", ts if exchange_ts is None else exchange_ts, None, ts,
        trade_id=trade_id, price=D(price), quantity=D(qty), side=side, venue_sequence=seq)


def mk(ts, mark=None, index=None, funding=None, *, exchange="BYBIT", carried=()):
    return CanonicalMarkPriceEvent(
        exchange, "markprice", ts, None, ts,
        mark_price=None if mark is None else D(mark),
        index_price=None if index is None else D(index),
        funding_rate=None if funding is None else D(funding), carried_forward=tuple(carried))


def oi(ts, value, unit=OIUnit.UNKNOWN, *, exchange="BYBIT", carried=(), instrument=None,
       market_type="linear_perpetual"):
    return CanonicalOIEvent(
        exchange, "openinterest", ts, None, ts, open_interest=None if value is None else D(value),
        unit=unit, carried_forward=tuple(carried), instrument=instrument, market_type=market_type)


def liq(ts, side="Buy", qty="1", *, exchange="BYBIT"):
    return CanonicalLiquidationEvent(
        exchange, "liquidation", ts, None, ts, side=side, price=D("100"),
        quantity=None if qty is None else D(qty))


def engine(venue="BYBIT", **kw):
    return MarketStateEngine(venue, **kw)


def fed(events, venue="BYBIT", **kw):
    e = engine(venue, **kw)
    for ev in events:
        e.update(ev)
    return e


def assert_book_unavailable(state, *, untrusted=True):
    b = state.book
    assert b.book_available is False
    assert b.book_stale is True
    assert b.book_untrusted is untrusted
    assert (b.best_bid, b.best_ask, b.mid, b.spread, b.spread_bps, b.book_imbalance, b.book_ts) == \
        (None,) * 7


# ==========================================================================
# 1. ORDER-BOOK AUTHORITY
# ==========================================================================

@pytest.mark.parametrize("quality", ["RECOVERING", "SEQUENCE_GAP", "STALE", "UNKNOWN", "", "valid", "GARBAGE"])
def test_non_valid_quality_state_book_cannot_become_authoritative(quality):
    state = fed([bk(T, quality_state=quality)]).snapshot(T)
    assert_book_unavailable(state)


@pytest.mark.parametrize("venue", ["BYBIT", "OKX", "BINANCE"])
def test_localbook_discarded_pre_snapshot_delta_never_becomes_an_authoritative_book(venue):
    """On current main (#89 Bybit, #90 OKX, Binance buffering) ``LocalBook.apply``
    DISCARDS a delta that arrives before any snapshot (returns ``None``) instead
    of emitting a RECOVERING one-level fragment, which is what this engine used
    to present as ``book_available=True``. The discarded update must not be
    reinterpreted as an observation: the engine fed only what the producer
    returned has no book, and it is not flagged untrusted either (nothing was
    observed). Non-VALID events that ARE fed are covered by the parametrized
    tests above and below."""
    lb = LocalBook(venue)
    raw = bk(T, bids=((D("100"), D("1")),), asks=((D("101"), D("1")),), update_id=7, exchange=venue)
    applied = lb.apply(raw)
    assert applied is None, "premise: producer discards the unanchored delta"
    state = engine(venue).snapshot(T + 1)          # nothing was returned, so nothing is fed
    assert_book_unavailable(state, untrusted=False)


@pytest.mark.parametrize("name, bids, asks", [
    ("empty_both", (), ()),
    ("empty_bids_one_sided_delta", (), ((D("100.5"), D("2")),)),
    ("empty_asks_one_sided_delta", ((D("99.5"), D("2")),), ()),
    ("zero_qty_bid_delete_marker", ((D("100"), D("0")),), ((D("101"), D("1")),)),
    ("zero_qty_ask_delete_marker", ((D("100"), D("1")),), ((D("101"), D("0")),)),
    ("negative_qty", ((D("100"), D("-1")),), ((D("101"), D("1")),)),
    ("zero_price", ((D("0"), D("1")),), ((D("101"), D("1")),)),
    ("negative_price", ((D("-5"), D("1")),), ((D("101"), D("1")),)),
    ("crossed", ((D("105"), D("1")),), ((D("100"), D("1")),)),
    ("locked", ((D("100"), D("1")),), ((D("100"), D("1")),)),
    ("bids_unsorted_best_not_first", ((D("90"), D("1")), (D("99"), D("3"))), ((D("101"), D("1")),)),
    ("bids_duplicate_price", ((D("99"), D("1")), (D("99"), D("2"))), ((D("101"), D("1")),)),
    ("asks_unsorted_best_not_first", ((D("99"), D("1")),), ((D("110"), D("1")), (D("101"), D("1")))),
    ("level_is_none", ((D("99"), None),), ((D("101"), D("1")),)),
    ("level_is_text", (("abc", D("1")),), ((D("101"), D("1")),)),
    ("level_not_a_pair", ((D("99"),),), ((D("101"), D("1")),)),
])
def test_raw_or_malformed_book_event_cannot_masquerade_as_reconstructed_state(name, bids, asks):
    """Raw incremental diffs are one-sided, carry qty==0 delete markers, arrive
    in venue order and can cross the book they patch. None of that survives
    reconstruction, so none of it may be exposed as best bid/ask/spread/imbalance."""
    state = fed([bk(T, bids=bids, asks=asks)]).snapshot(T)
    assert_book_unavailable(state)


def test_unlisted_book_source_is_untrusted_allowlist_not_denylist():
    """The engine gated on a denylist (PARTIAL_DEPTH) while run_collector/replay
    use an allowlist; an unknown source used to pass straight through."""
    state = fed([bk(T, book_source="SOME_FUTURE_SOURCE")]).snapshot(T)
    assert_book_unavailable(state)


def test_partial_depth_is_still_dropped_and_does_not_invalidate_a_good_book():
    good, partial = bk(T, update_id=1), bk(T + 10, book_source="PARTIAL_DEPTH",
                                           bids=((D("1"), D("999")),), asks=((D("2"), D("999")),))
    state = fed([good, partial]).snapshot(T + 20)
    assert state.book.book_available is True       # partial push is not evidence about the chain
    assert (state.book.best_bid, state.book.best_ask) == (100.0, 101.0)
    only_partial = fed([partial]).snapshot(T + 20)
    assert only_partial.book.book_available is False and only_partial.book.book_untrusted is False


def test_untrusted_book_is_a_barrier_an_earlier_good_book_does_not_stand_in():
    good = bk(T, update_id=1)
    gap = bk(T + 100, quality_state="SEQUENCE_GAP", update_id=2)
    later_good = bk(T + 300, bids=((D("100.5"), D("1")),), update_id=3)
    e = fed([good, gap, later_good])
    assert e.snapshot(T + 50).book.book_available is True          # before the gap: history intact
    assert_book_unavailable(e.snapshot(T + 200))                   # after the producer said "not valid"
    assert e.snapshot(T + 300).book.best_bid == 100.5              # a new VALID book restores it
    assert e.snapshot(T + 300).book.book_untrusted is False


def test_barrier_is_not_bypassed_by_insertion_order():
    events = [bk(T, update_id=1), bk(T + 100, quality_state="RECOVERING", update_id=2)]
    for order in itertools.permutations(events):
        assert_book_unavailable(fed(order).snapshot(T + 200))


def test_untrusted_and_never_observed_are_distinguishable():
    never = engine().snapshot(T).book
    assert never.book_available is False and never.book_untrusted is False
    assert fed([bk(T, quality_state="RECOVERING")]).snapshot(T).book.book_untrusted is True


def test_out_of_order_canonical_book_does_not_win_by_insertion_position():
    newer, older = bk(T + 10, bids=((D("100.5"), D("1")),), update_id=11), bk(T, update_id=10)
    for order in ((newer, older), (older, newer)):
        s = fed(order).snapshot(T + 20)
        assert s.book.best_bid == 100.5 and s.book.book_ts == T + 10


def test_equal_receive_ts_books_resolve_by_update_id_not_insertion_order():
    """REPRODUCED on main: same-ms book states -- the OLDER update_id could win."""
    # Chosen so a repr()-only tiebreak would pick the OLDER update_id (its repr
    # sorts last): only the venue sequence puts update_id 11 on top.
    v10, v11 = bk(T, bids=((D("100.5"), D("1")),), update_id=10), bk(T, bids=((D("100"), D("1")),), update_id=11)
    for order in ((v10, v11), (v11, v10)):
        s = fed(order).snapshot(T + 1)
        assert s.book.best_bid == 100.0


@pytest.mark.parametrize("venue", ["BYBIT", "OKX", "BINANCE", "BINANCE_SPOT"])
def test_legitimate_localbook_output_is_still_accepted_for_every_venue(venue):
    """The gate must not reject what real producers emit when VALID."""
    lb = LocalBook(venue)
    produced = []
    if venue == "BYBIT":
        snap = CanonicalOrderBookEvent("BYBIT", "orderbook", T, None, T, bids=((D("100"), D("1")),),
                                       asks=((D("101"), D("1")),), update_id=10, is_snapshot=True)
        delta = CanonicalOrderBookEvent("BYBIT", "orderbook", T + 1, None, T + 1,
                                        bids=((D("100.5"), D("2")),), asks=(), update_id=11)
        produced = [lb.apply(snap), lb.apply(delta)]
    elif venue == "OKX":
        snap = CanonicalOrderBookEvent("OKX", "orderbook", T, None, T, bids=((D("100"), D("1")),),
                                       asks=((D("101"), D("1")),), update_id=10, previous_update_id=-1,
                                       is_snapshot=True)
        delta = CanonicalOrderBookEvent("OKX", "orderbook", T + 1, None, T + 1,
                                        bids=((D("100.5"), D("2")),), asks=(), update_id=11,
                                        previous_update_id=10)
        produced = [lb.apply(snap), lb.apply(delta)]
    else:
        stream = "orderbook" if venue == "BINANCE" else "spot_orderbook"
        d1 = CanonicalOrderBookEvent(venue, stream, T, None, T, bids=((D("100"), D("1")),),
                                     asks=((D("101"), D("1")),), update_id=101, first_update_id=99,
                                     previous_update_id=98)
        d2 = CanonicalOrderBookEvent(venue, stream, T + 1, None, T + 1, bids=((D("100.5"), D("2")),),
                                     asks=(), update_id=102, first_update_id=102, previous_update_id=101)
        assert lb.apply(d1) is None and lb.apply(d2) is None            # buffered until bridged
        snapshot = CanonicalOrderBookEvent(venue, stream, None, None, T + 2, bids=((D("99"), D("5")),),
                                           asks=((D("102"), D("5")),), update_id=100, is_snapshot=True)
        assert lb.binance_snapshot(100, snapshot) is True
        produced = [ev for ev, _kind, _gen in lb.committed_recovery_events]
    produced = [p for p in produced if p is not None]
    assert produced and all(p.quality_state == "VALID" for p in produced)
    e = engine(venue)
    for p in produced:
        e.update(p)
    state = e.snapshot(T + 10)
    assert state.book.book_available is True and state.book.book_untrusted is False
    assert state.book.best_bid is not None and state.book.best_ask is not None
    assert state.book.best_bid < state.book.best_ask


@pytest.mark.xfail(strict=True, reason=(
    "KNOWN RESIDUAL GAP, not fixable inside market_state.py. A well-formed, two-sided, ordered, "
    "uncrossed, positive-quantity raw one-level DIFF is field-for-field identical to a one-level "
    "reconstructed book: adapters stamp raw diffs quality_state='VALID' and "
    "book_source='DIFF_DEPTH_RECONSTRUCTED' by default. Closing it needs a provenance field "
    "stamped by LocalBook (book_engine.py; #89/#90 have merged without adding one). Strict xfail: when that "
    "lands this test XPASSes and must be promoted to a plain test."))
def test_well_formed_raw_diff_cannot_masquerade_as_a_reconstructed_book():
    raw_diff = bk(T, bids=((D("10"), D("1")),), asks=((D("20"), D("1")),), update_id=5)
    assert fed([raw_diff]).snapshot(T + 1).book.book_available is False


# ==========================================================================
# 2. CAUSALITY
# ==========================================================================

def _one_of_each(ts, k=0):
    return [tr(ts, "100", "1", "Buy", seq=ts), bk(ts, update_id=ts), mk(ts, "100", "99", "0.0001"),
            oi(ts, 500 + k), liq(ts, "Buy", "1")]


@pytest.mark.parametrize("make_future", [
    lambda ts: tr(ts, "999", "50", "Sell", seq=ts),
    lambda ts: bk(ts, bids=((D("5"), D("1")),), asks=((D("6"), D("1")),), update_id=ts),
    lambda ts: mk(ts, "999", "998", "0.9"),
    lambda ts: oi(ts, 123456),
    lambda ts: liq(ts, "Sell", "77"),
], ids=["trade", "book", "mark", "oi", "liquidation"])
def test_event_received_after_observation_ts_never_changes_that_snapshot(make_future):
    base = fed(_one_of_each(T))
    before = base.snapshot(T + 10).digest()
    base.update(make_future(T + 11))                 # received strictly after the observation
    assert base.snapshot(T + 10).digest() == before
    base.update(make_future(T + 10_000))
    assert base.snapshot(T + 10).digest() == before


def test_late_arriving_event_with_old_exchange_ts_cannot_rewrite_history():
    e = fed(_one_of_each(T))
    before = e.snapshot(T + 100).digest()
    e.update(tr(T + 5_000, "7", "9", "Sell", exchange_ts=T - 1))      # exchange ts predates, receive postdates
    e.update(bk(T + 5_000, bids=((D("1"), D("1")),), asks=((D("2"), D("1")),), exchange_ts=T - 1))
    assert e.snapshot(T + 100).digest() == before


def test_event_received_at_exactly_observation_ts_is_included_for_every_dimension():
    s = fed(_one_of_each(T)).snapshot(T)
    assert s.trade_flow.trades_observed and s.book.book_available and s.price.mark_price == 100.0
    assert s.derivatives.open_interest == 500.0 and s.liquidation.liquidations_observed
    s2 = fed(_one_of_each(T)).snapshot(T - 1)
    assert not s2.trade_flow.trades_observed and not s2.book.book_available
    assert s2.price.mark_price is None and s2.derivatives.open_interest is None
    assert not s2.liquidation.liquidations_observed


def test_future_exchange_timestamp_cannot_pull_an_event_into_an_earlier_snapshot_all_types():
    e = fed([tr(T + 500, exchange_ts=T), bk(T + 500, exchange_ts=T), mk(T + 500, "1"),
             oi(T + 500, 1), liq(T + 500)])
    s = e.snapshot(T + 100)
    assert not s.trade_flow.trades_observed and not s.book.book_available
    assert s.price.mark_price is None and s.derivatives.open_interest is None


def test_market_state_module_never_reads_non_receive_timestamps():
    """AST guard: availability (and ordering) may use local_receive_ts only. A
    field read of exchange_event_ts / exchange_transaction_ts / local_process_ts
    anywhere in the module is a causality hazard."""
    src = (Path(__file__).resolve().parent.parent / "collector" / "market_state.py").read_text()
    banned = {"exchange_event_ts", "exchange_transaction_ts", "local_process_ts"}
    hits = [n.attr for n in ast.walk(ast.parse(src)) if isinstance(n, ast.Attribute) and n.attr in banned]
    hits += [n.value for n in ast.walk(ast.parse(src))
             if isinstance(n, ast.Constant) and isinstance(n.value, str) and n.value in banned]
    assert hits == []


# ==========================================================================
# 3. DETERMINISM
# ==========================================================================

def test_all_insertion_orders_of_tied_mixed_events_give_one_digest():
    """REPRODUCED on main: at equal local_receive_ts the 'latest' trade/mark/OI/book --
    and so the digest -- depended on insertion order, contradicting the module's
    own documented guarantee."""
    events = [tr(T, "100", "1", "Buy", seq=1), tr(T, "105", "2", "Sell", seq=2),
              bk(T, update_id=10), bk(T, bids=((D("100.5"), D("1")),), update_id=11),
              mk(T, "100", "99", "0.0001"), mk(T, "101", "98", "0.0002"),
              oi(T, 500), oi(T, 510), liq(T, "Buy", "1"), liq(T, "Sell", "2")]
    rng = random.Random(20260906)                    # seeded: deterministic test, 400 distinct shuffles
    digests = set()
    for _ in range(400):
        order = events[:]
        rng.shuffle(order)
        digests.add(fed(order).snapshot(T + 1).digest())
    assert len(digests) == 1


@pytest.mark.parametrize("make", [
    lambda i: tr(T, str(100 + i), "1", "Buy"),                    # no seq, no id: pure content tie
    lambda i: mk(T, str(100 + i)),
    lambda i: oi(T, 500 + i),
    lambda i: liq(T, "Buy", str(1 + i)),
], ids=["trade_no_sequence", "mark", "oi", "liquidation"])
def test_events_tied_on_receive_ts_and_sequence_resolve_by_content_not_order(make):
    evs = [make(i) for i in range(4)]
    digests = {fed(order).snapshot(T + 1).digest() for order in itertools.permutations(evs)}
    assert len(digests) == 1


def test_repeated_snapshot_calls_are_identical_and_call_order_independent():
    e = fed(_one_of_each(T) + _one_of_each(T + 50, 1) + _one_of_each(T + 100, 2))
    d_a = [e.snapshot(T + 120).digest() for _ in range(3)]
    assert len(set(d_a)) == 1
    forward = [e.snapshot(t).digest() for t in (T, T + 60, T + 120)]
    reverse = [e.snapshot(t).digest() for t in (T + 120, T + 60, T)][::-1]
    assert forward == reverse
    fresh = fed(_one_of_each(T) + _one_of_each(T + 50, 1) + _one_of_each(T + 100, 2))
    assert fresh.snapshot(T + 60).digest() == e.snapshot(T + 60).digest()


def test_duplicate_calls_with_the_same_event_object_are_deterministic():
    ev = tr(T, seq=1)
    a, b = fed([ev, ev]), fed([ev, ev])
    assert a.snapshot(T).digest() == b.snapshot(T).digest()
    assert a.snapshot(T).trade_flow.trade_count == 2          # documented: dedup is not this layer's job


def test_snapshot_is_independent_of_whether_a_later_snapshot_was_taken_first():
    events = _one_of_each(T) + _one_of_each(T + 1_000, 5)
    e1, e2 = fed(events), fed(events)
    e1.snapshot(T + 2_000)                       # touch the future first
    assert e1.snapshot(T + 10).digest() == e2.snapshot(T + 10).digest()


def test_non_integer_receive_ts_is_refused_at_update_not_at_every_later_snapshot():
    """REPRODUCED on main: a stored non-int local_receive_ts made EVERY later
    snapshot() raise TypeError, poisoning the engine permanently."""
    e = engine()
    e.update(tr(T, seq=1))
    for bad in (None, 1.5, "1780000000000", True):
        with pytest.raises(ValueError, match="local_receive_ts"):
            e.update(CanonicalTradeEvent("BYBIT", "trades", T, None, bad, price=D("1"),
                                         quantity=D("1"), side="Buy"))
    assert e.snapshot(T).trade_flow.trade_count == 1       # engine still healthy


# ==========================================================================
# 4. QUALITY SEMANTICS: absent / stale / invalid / valid
# ==========================================================================

def test_fresh_dimensions_are_explicitly_fresh_and_never_default_to_usable():
    s = engine().snapshot(T)
    assert (s.book.book_available, s.book.book_stale) == (False, True)
    assert (s.price.mark_stale, s.price.index_stale) == (True, True)
    assert (s.derivatives.oi_stale, s.derivatives.funding_stale) == (True, True)
    assert (s.trade_flow.trades_observed, s.liquidation.liquidations_observed) == (False, False)


@pytest.mark.parametrize("dim, kwargs, make, attr", [
    ("book", {"book_stale_ms": 1_000}, lambda: bk(T, update_id=1), lambda s: s.book.book_stale),
    ("mark", {"mark_stale_ms": 1_000}, lambda: mk(T, "100"), lambda s: s.price.mark_stale),
    ("index", {"mark_stale_ms": 1_000}, lambda: mk(T, index="99"), lambda s: s.price.index_stale),
    ("oi", {"oi_stale_ms": 1_000}, lambda: oi(T, 5), lambda s: s.derivatives.oi_stale),
    ("funding", {"funding_stale_ms": 1_000}, lambda: mk(T, funding="0.0001"), lambda s: s.derivatives.funding_stale),
])
def test_staleness_threshold_is_strictly_greater_than_and_explicit(dim, kwargs, make, attr):
    e = fed([make()], **kwargs)
    assert attr(e.snapshot(T + 1_000)) is False          # age == threshold: not stale
    assert attr(e.snapshot(T + 1_001)) is True           # age  > threshold: stale


def test_event_with_no_mark_price_does_not_make_the_mark_look_fresh():
    """REPRODUCED on main: a latest mark event with mark_price=None gave
    mark_price=None AND mark_stale=False ('fresh mark with no price')."""
    s = fed([mk(T, mark=None, index="99", funding="0.0001")]).snapshot(T + 1)
    assert s.price.mark_price is None and s.price.mark_stale is True and s.price.mark_ts is None
    assert s.price.index_price == 99.0 and s.price.index_stale is False


def test_oi_event_with_no_value_does_not_make_oi_look_fresh():
    s = fed([oi(T, None)]).snapshot(T + 1)
    assert s.derivatives.open_interest is None and s.derivatives.oi_stale is True
    assert s.derivatives.oi_ts is None


@pytest.mark.parametrize("bad", [0, -1, -5_000, True, 1.5, None, "60000"])
def test_misconfigured_window_or_thresholds_are_refused_not_silently_empty(bad):
    """REPRODUCED on main: window_ms<=0 reported window_trade_count=0 for a
    market that had trades; non-positive stale thresholds flip every flag."""
    for name in ("trade_flow_window_ms", "book_stale_ms", "mark_stale_ms", "oi_stale_ms", "funding_stale_ms"):
        with pytest.raises(ValueError, match=name):
            MarketStateEngine("BYBIT", **{name: bad})


# ==========================================================================
# 5. CARRIED-FORWARD / CHANNEL-PURE FRESHNESS (real adapters)
# ==========================================================================

def _bybit_ticker(adapter, engine_, typ, ts, data):
    raw = {"topic": "tickers.BTCUSDT", "type": typ, "ts": ts, "data": data}
    for ev in adapter.normalize(raw, local_receive_ts=ts):
        if isinstance(ev, (CanonicalMarkPriceEvent, CanonicalOIEvent)):
            engine_.update(ev)


def test_bybit_carried_forward_funding_is_not_refreshed_by_unrelated_ticker_deltas():
    """REPRODUCED on main: funding last observed at T0; five hourly deltas that
    carried only markPrice left the engine reporting funding_age_ms=1 and
    funding_stale=False five hours later."""
    a, e = BybitAdapter(), engine("BYBIT")
    _bybit_ticker(a, e, "snapshot", T, {"markPrice": "100", "indexPrice": "99", "fundingRate": "0.0001",
                                        "nextFundingTime": str(T + 8 * H)})
    for k in range(1, 6):
        _bybit_ticker(a, e, "delta", T + k * H, {"markPrice": str(100 + k)})
    s = e.snapshot(T + 5 * H + 1)
    assert s.derivatives.funding_rate == 0.0001
    assert s.derivatives.funding_ts == T
    assert s.derivatives.funding_age_ms == 5 * H + 1
    assert s.derivatives.funding_stale is True
    assert s.price.mark_price == 105.0 and s.price.mark_stale is False      # genuinely observed just now
    assert s.price.index_price == 99.0 and s.price.index_stale is True      # observed only at T0


def test_bybit_carried_forward_mark_price_is_not_refreshed_by_a_funding_only_delta():
    a, e = BybitAdapter(), engine("BYBIT")
    _bybit_ticker(a, e, "snapshot", T, {"markPrice": "100", "indexPrice": "99", "fundingRate": "0.0001",
                                        "nextFundingTime": str(T + 8 * H)})
    _bybit_ticker(a, e, "delta", T + 60_000, {"fundingRate": "0.0002"})
    s = e.snapshot(T + 60_001)
    assert s.price.mark_price == 100.0 and s.price.mark_ts == T and s.price.mark_stale is True
    assert s.price.price_vs_mark is None                  # never derived from a stale mark
    assert s.derivatives.funding_rate == 0.0002 and s.derivatives.funding_stale is False


def test_okx_channel_pure_events_do_not_erase_each_others_fields():
    """REPRODUCED on main: mark-price, funding-rate and index-tickers arrive as
    separate events; the engine read all three fields from whichever came last,
    yielding mark_price=None, funding=None and mark_stale=False."""
    o, e = OKXAdapter(), engine("OKX")

    def feed(channel, payload, ts):
        raw = {"arg": {"channel": channel, "instId": "BTC-USDT-SWAP"}, "data": [payload]}
        for ev in o.normalize(raw, local_receive_ts=ts):
            if isinstance(ev, CanonicalMarkPriceEvent):
                e.update(ev)

    feed("mark-price", {"instId": "BTC-USDT-SWAP", "markPx": "100.5", "ts": str(T)}, T)
    feed("funding-rate", {"instId": "BTC-USDT-SWAP", "fundingRate": "0.0001",
                          "nextFundingTime": str(T + 8 * H), "fundingTime": str(T + H), "ts": str(T + 1)}, T + 1)
    feed("index-tickers", {"instId": "BTC-USDT", "idxPx": "100.4", "ts": str(T + 2)}, T + 2)
    s = e.snapshot(T + 3)
    assert (s.price.mark_price, s.price.index_price, s.derivatives.funding_rate) == (100.5, 100.4, 0.0001)
    assert (s.price.mark_ts, s.price.index_ts, s.derivatives.funding_ts) == (T, T + 2, T + 1)
    assert not s.price.mark_stale and not s.price.index_stale and not s.derivatives.funding_stale


def test_carried_forward_oi_is_not_a_new_observation_and_makes_no_spurious_change():
    events = [oi(T, 500), oi(T + 10, 500, carried=("openInterest",)), oi(T + 20, 500, carried=("open_interest",))]
    s = fed(events).snapshot(T + 25)
    assert s.derivatives.open_interest == 500.0 and s.derivatives.oi_ts == T
    assert s.derivatives.oi_change is None                # one real observation: nothing to difference


def test_oi_change_is_between_the_two_latest_genuine_observations():
    events = [oi(T, 500), oi(T + 10, 530), oi(T + 20, 530, carried=("openInterest",))]
    s = fed(events).snapshot(T + 25)
    assert s.derivatives.open_interest == 530.0 and s.derivatives.oi_change == 30.0


# ==========================================================================
# 6. DERIVATIVES / OI UNITS
# ==========================================================================

def test_oi_unit_mismatch_never_produces_an_oi_change():
    s = fed([oi(T, 500, OIUnit.CONTRACTS), oi(T + 10, 52, OIUnit.BASE_COIN)]).snapshot(T + 20)
    assert s.derivatives.oi_change is None
    assert s.derivatives.open_interest == 52.0 and s.derivatives.oi_unit is OIUnit.BASE_COIN


def test_unit_flip_flop_only_compares_adjacent_observations_of_the_same_unit():
    s = fed([oi(T, 500, OIUnit.CONTRACTS), oi(T + 10, 52, OIUnit.BASE_COIN),
             oi(T + 20, 54, OIUnit.BASE_COIN)]).snapshot(T + 30)
    assert s.derivatives.oi_change == 2.0


def test_unknown_unit_oi_change_is_allowed_within_one_instrument_stream():
    s = fed([oi(T, 500), oi(T + 10, 510)]).snapshot(T + 20)
    assert s.derivatives.oi_unit is OIUnit.UNKNOWN and s.derivatives.oi_change == 10.0


def test_oi_change_refuses_to_difference_different_instruments_in_an_unbound_engine():
    """An unbound engine accepts several instruments of one exchange; 'same
    exchange' alone does not make two OI readings the same physical quantity."""
    from dataclasses import replace
    from collector.collector.instrument import BYBIT_LINEAR_BTCUSDT

    eth = replace(BYBIT_LINEAR_BTCUSDT, instrument="ETH-USDT", native_symbol="ETHUSDT")
    same = fed([oi(T, 500, instrument=BYBIT_LINEAR_BTCUSDT), oi(T + 10, 510, instrument=BYBIT_LINEAR_BTCUSDT)])
    assert same.snapshot(T + 20).derivatives.oi_change == 10.0
    mixed = fed([oi(T, 500, instrument=BYBIT_LINEAR_BTCUSDT), oi(T + 10, 510, instrument=eth)])
    assert mixed.snapshot(T + 20).derivatives.oi_change is None


def test_oi_change_refuses_to_difference_different_market_types():
    s = fed([oi(T, 500, market_type="spot"), oi(T + 10, 510, market_type="linear_perpetual")]).snapshot(T + 20)
    assert s.derivatives.oi_change is None


# ==========================================================================
# 7. TRADE FLOW
# ==========================================================================

def test_trailing_window_boundary_lower_bound_excluded_upper_included():
    """(observation_ts - window_ms, observation_ts] -- same contract as
    docs/WINDOWED_TRADE_FLOW_OBSERVATION.md."""
    w = 60_000
    obs = T + 10 * w
    e = fed([tr(obs - w, "100", "1", "Buy", seq=1),          # exactly on the lower bound: OUT
             tr(obs - w + 1, "100", "2", "Buy", seq=2),      # one ms inside: IN
             tr(obs, "100", "4", "Sell", seq=3),             # exactly at observation_ts: IN
             tr(obs + 1, "100", "8", "Sell", seq=4)],        # after: not yet known
            trade_flow_window_ms=w)
    f = e.snapshot(obs).trade_flow
    assert f.window_trade_count == 2
    assert (f.window_buy_volume, f.window_sell_volume, f.window_net_volume) == (2.0, 4.0, -2.0)
    assert f.trade_count == 3 and (f.cumulative_buy_volume, f.cumulative_sell_volume) == (3.0, 4.0)
    assert f.window_ms == w


def test_window_ms_is_honoured_and_reported():
    e = fed([tr(T, qty="1", seq=1), tr(T + 500, qty="1", seq=2)], trade_flow_window_ms=100)
    f = e.snapshot(T + 550).trade_flow
    assert f.window_ms == 100 and f.window_trade_count == 1 and f.trade_count == 2


@pytest.mark.parametrize("side", [None, "", "unknown", "BUYY", "?", 1, 0, b"buy", "buy "])
def test_unknown_trade_side_is_counted_but_never_guessed(side):
    f = fed([tr(T, qty="5", side=side, seq=1)]).snapshot(T).trade_flow
    assert f.trade_count == 1 and f.window_trade_count == 1
    assert (f.cumulative_buy_volume, f.cumulative_sell_volume, f.cvd) == (0.0, 0.0, 0.0)
    assert (f.window_buy_volume, f.window_sell_volume) == (0.0, 0.0)
    assert f.unknown_side_trade_count == 1


@pytest.mark.parametrize("side, expect_buy", [("Buy", True), ("BUY", True), ("buy", True), ("b", True),
                                              ("B", True), ("Sell", False), ("SELL", False), ("s", False)])
def test_recognised_side_spellings_map_to_the_right_direction(side, expect_buy):
    f = fed([tr(T, qty="3", side=side, seq=1)]).snapshot(T).trade_flow
    assert (f.cumulative_buy_volume, f.cumulative_sell_volume) == ((3.0, 0.0) if expect_buy else (0.0, 3.0))
    assert f.unknown_side_trade_count == 0


def test_trade_direction_is_not_inverted_end_to_end_through_the_real_binance_adapter():
    """Binance aggTrade ``m`` = buyer is the maker, i.e. the AGGRESSOR SOLD."""
    a, e = BinanceAdapter(), engine("BINANCE")
    for i, (maker, qty) in enumerate(((True, "2"), (False, "5"))):
        raw = {"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "E": T + i, "a": 10 + i, "p": "100",
                                                      "q": qty, "T": T + i, "m": maker}}
        for ev in a.normalize(raw, local_receive_ts=T + i):
            e.update(ev)
    f = e.snapshot(T + 5).trade_flow
    assert (f.cumulative_sell_volume, f.cumulative_buy_volume, f.cvd) == (2.0, 5.0, 3.0)


def test_cumulative_trade_count_includes_every_eligible_trade_exactly_once():
    trades = [tr(T + i, qty="1", seq=i) for i in range(50)]
    e = fed(trades)
    assert e.snapshot(T + 49).trade_flow.trade_count == 50
    assert e.snapshot(T + 24).trade_flow.trade_count == 25


# ==========================================================================
# 8. LIQUIDATIONS / DIGEST COVERAGE
# ==========================================================================

def test_liquidation_without_quantity_is_counted_but_flagged_never_silently_zero():
    """REPRODUCED on main: the OKX adapter can emit quantity=None (sz absent);
    the engine summed it as 0.0 with no signal that the sums were lower bounds."""
    s = fed([liq(T, "Buy", "2"), liq(T + 1, "Sell", None), liq(T + 2, "Sell", "3")]).snapshot(T + 5)
    q = s.liquidation
    assert q.liquidation_count == 3 and q.unquantified_liquidation_count == 1
    assert (q.buy_side_quantity, q.sell_side_quantity) == (2.0, 3.0)


def test_zero_liquidations_observed_is_distinct_from_unavailable():
    assert fed([liq(T + 100)]).snapshot(T).liquidation.liquidations_observed is False
    assert fed([liq(T)]).snapshot(T).liquidation.liquidations_observed is True


def test_digest_distinguishes_every_new_or_previously_omitted_field():
    base = fed([tr(T, seq=1), bk(T, update_id=1), mk(T, "100", "99", "0.0001"), oi(T, 500), liq(T)])
    ref = base.snapshot(T + 1).digest()
    variants = {
        "unknown_side": [tr(T, seq=1, side=None), bk(T, update_id=1), mk(T, "100", "99", "0.0001"), oi(T, 500), liq(T)],
        "untrusted_book": [tr(T, seq=1), bk(T, update_id=1, quality_state="RECOVERING"),
                           mk(T, "100", "99", "0.0001"), oi(T, 500), liq(T)],
        "unquantified_liq": [tr(T, seq=1), bk(T, update_id=1), mk(T, "100", "99", "0.0001"), oi(T, 500), liq(T, qty=None)],
        "index_only_ts": [tr(T, seq=1), bk(T, update_id=1), mk(T, "100", None, "0.0001"), oi(T, 500), liq(T)],
        "oi_ts": [tr(T, seq=1), bk(T, update_id=1), mk(T, "100", "99", "0.0001"), oi(T - 7, 500), liq(T)],
        "funding_ts": [tr(T, seq=1), bk(T, update_id=1), mk(T, "100", "99", "0.0001", carried=("fundingRate",)),
                       oi(T, 500), liq(T)],
    }
    seen = {ref}
    for name, evs in variants.items():
        d = fed(evs).snapshot(T + 1).digest()
        assert d not in seen, name
        seen.add(d)


def test_digest_isolates_untrusted_book_from_never_observed_book():
    """The two states differ ONLY in ``book_untrusted``; the digest must tell them apart."""
    never = engine().snapshot(T)
    untrusted = fed([bk(T, quality_state="RECOVERING")]).snapshot(T)
    assert never.book == untrusted.book.__class__() and untrusted.book.book_untrusted is True
    assert never.digest() != untrusted.digest()


def test_digest_isolates_index_observation_time():
    """Same index value, same staleness verdict, different observation time."""
    a = fed([mk(T, "100", "99")]).snapshot(T + 1)
    b = fed([mk(T - 7, "100", "99"), mk(T, "100", None)]).snapshot(T + 1)
    assert (a.price.index_price, a.price.index_stale) == (b.price.index_price, b.price.index_stale)
    assert a.price.index_ts != b.price.index_ts
    assert a.digest() != b.digest()
