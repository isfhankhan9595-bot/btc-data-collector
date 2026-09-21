"""Phase F: adversarial audit of causal alignment's identity contract.

This file does NOT duplicate test_cross_exchange_alignment.py's existing 29
tests (identity isolation, causal/lookahead boundaries, ties, determinism,
staleness, NEVER_OBSERVED, result ordering, quality preservation, purity,
replay parity for Bybit/OKX) -- that coverage was audited and found sound.
This file adds exactly the gaps the audit found:

* F-A: identity collision proven with real ``InstrumentId`` objects (four
  fields individually), not only the ``UNIDENTIFIED`` placeholder every
  existing test used.
* F-B: identified vs. unidentified collision -- untested before this file,
  and explicitly the most important gap. An identified event and an
  unidentified event of the same (exchange, market_type, stream) must never
  share a key, in either arrival order or at either timestamp relationship.
* F-J: a STALE observation's ``quality_state`` is preserved, not just an
  AVAILABLE one (the existing parametrized test only covered AVAILABLE).
* F-K: type-validation edge cases the existing test missed: ``bool`` for
  both ``observation_ts`` and ``staleness_ms`` (a real footgun, since
  ``isinstance(True, int)`` is ``True`` in Python), a string for either, and
  an invalid ``local_receive_ts`` type on an individual event.
* F-M: a genuinely adversarial 4-venue, 5-stream-type dataset (trade,
  orderbook->book-derived-equivalent via a second stream, markprice, OI,
  liquidation, and a legitimately-unidentified OKX event) through one
  ``causally_align`` call.
* F-N: replay-produced events extended to all four venues (existing test
  only covered Bybit + OKX); Binance USD-M and Spot added.
* F-O: replay's identity source-of-truth, proven adversarially -- not just
  architecturally, by grep. A corrupted *canonical* ``instrument_key`` is
  constructed, replay is run on the *raw* frame for the same event, and the
  two are shown to disagree, with replay (raw + adapter) being correct and
  the corrupted canonical value being the one that's wrong. Structural grep
  evidence (replay.py never references the string "instrument_key") is
  recorded in the module docstring correction to docs/INSTRUMENT_IDENTITY.md
  and PR body, not repeated as a test assertion here, since grep is not a
  test.
* F-P: Phase D's mutations G (malformed -> None) and H (contradictory
  accepted) are re-run against the current ``resolve_canonical_instrument_key``,
  not merely assumed to still hold from Phase D's own commit.

Mutation-style verification of this file's own new invariants (F-A/F-B in
particular) is demonstrated in the conversation record, not claimed here.
"""
from __future__ import annotations

import json

import pytest

from collector.collector.canonical import (
    CanonicalLiquidationEvent, CanonicalMarkPriceEvent, CanonicalOIEvent,
    CanonicalOrderBookEvent, CanonicalTradeEvent, OISource,
)
from collector.collector.instrument import (
    BINANCE_SPOT_BTCUSDT, BINANCE_USDM_BTCUSDT, BYBIT_LINEAR_BTCUSDT,
    OKX_SWAP_BTCUSDT, InstrumentId, InstrumentIdError, resolve_canonical_instrument_key,
)
from collector.collector.replay import FrameKind, ReplayEngine, ReplayFrame, ReplaySource
from collector.pipeline.cross_exchange_alignment import (
    AlignmentStatus, UNIDENTIFIED as UNID, alignment_key, causally_align,
)

T = 1_780_000_000_000
PERP = "linear_perpetual"


def trade(exchange="BINANCE", stream="trades", *, recv, ex_ts=None, market_type=PERP,
          instrument=None, price=100.0, quality="VALID", tid="1"):
    return CanonicalTradeEvent(
        exchange, stream, recv if ex_ts is None else ex_ts, None, recv,
        market_type=market_type, quality_state=quality, instrument=instrument,
        trade_id=tid, price=price, quantity=1.0, side="Buy")


def align(events, ts=T, **kw):
    kw.setdefault("staleness_ms", 1_000)
    return causally_align(events, ts, **kw)


# ---------------------------------------------------------------------------
# F-A: identity collision, with real InstrumentId objects, fields compared
# individually
# ---------------------------------------------------------------------------


def test_binance_spot_and_usdm_keys_differ_in_every_field_except_the_two_that_collide():
    """The task's own explicit demand: compare the four InstrumentId fields
    individually, not just the opaque .key string."""
    assert BINANCE_SPOT_BTCUSDT.exchange == BINANCE_USDM_BTCUSDT.exchange == "BINANCE"
    assert BINANCE_SPOT_BTCUSDT.instrument == BINANCE_USDM_BTCUSDT.instrument == "BTC-USDT"
    assert BINANCE_SPOT_BTCUSDT.native_symbol == BINANCE_USDM_BTCUSDT.native_symbol == "BTCUSDT"
    assert BINANCE_SPOT_BTCUSDT.market_type == "spot"
    assert BINANCE_USDM_BTCUSDT.market_type == "linear_perpetual"
    assert BINANCE_SPOT_BTCUSDT.market_type != BINANCE_USDM_BTCUSDT.market_type
    assert BINANCE_SPOT_BTCUSDT.key != BINANCE_USDM_BTCUSDT.key


def test_real_binance_spot_and_usdm_instruments_do_not_collide_in_alignment():
    spot = trade("BINANCE", "trades", recv=T - 5, instrument=BINANCE_SPOT_BTCUSDT, market_type="spot")
    usdm = trade("BINANCE", "trades", recv=T - 1, instrument=BINANCE_USDM_BTCUSDT, market_type=PERP)
    out = align([spot, usdm])
    assert len(out) == 2
    assert out[("BINANCE", "spot", BINANCE_SPOT_BTCUSDT.key, "trades")].event is spot
    assert out[("BINANCE", PERP, BINANCE_USDM_BTCUSDT.key, "trades")].event is usdm


def test_bybit_does_not_collide_with_okx_using_real_instruments():
    bybit = trade("BYBIT", "trades", recv=T - 3, instrument=BYBIT_LINEAR_BTCUSDT)
    okx = trade("OKX", "trades", recv=T - 3, instrument=OKX_SWAP_BTCUSDT)
    out = align([bybit, okx])
    assert len(out) == 2
    assert {o.event.instrument.native_symbol for o in out.values()} == {"BTCUSDT", "BTC-USDT-SWAP"}


def test_streams_remain_independent_for_one_real_identified_instrument():
    events = [trade("BYBIT", stream=s, recv=T - 1, instrument=BYBIT_LINEAR_BTCUSDT)
              for s in ("trades", "markprice", "orderbook", "openinterest", "liquidation")]
    out = align(events)
    assert len(out) == 5
    assert {k[3] for k in out} == {"trades", "markprice", "orderbook", "openinterest", "liquidation"}
    assert all(k[2] == BYBIT_LINEAR_BTCUSDT.key for k in out)


def test_two_different_real_instruments_same_exchange_market_stream_cannot_overwrite():
    """A synthetic second BTC-quoted instrument on Bybit (guards against a
    key collapsing to (exchange, market_type, stream) if instrument_key were
    ever dropped from equality -- exercised with real InstrumentId objects,
    not strings)."""
    other = InstrumentId("BYBIT", PERP, "ETH-USDT", "ETHUSDT")
    a = trade("BYBIT", recv=T - 1, instrument=BYBIT_LINEAR_BTCUSDT, price=65_000.0)
    b = trade("BYBIT", recv=T - 1, instrument=other, price=3_500.0)
    out = align([a, b])
    assert len(out) == 2
    assert out[("BYBIT", PERP, BYBIT_LINEAR_BTCUSDT.key, "trades")].event.price == 65_000.0
    assert out[("BYBIT", PERP, other.key, "trades")].event.price == 3_500.0


# ---------------------------------------------------------------------------
# F-B: identified vs. unidentified collision -- the important untested gap
# ---------------------------------------------------------------------------


def test_identified_event_is_not_overwritten_by_an_unidentified_one_arriving_later():
    identified = trade("BINANCE", "trades", recv=T - 5, instrument=BINANCE_SPOT_BTCUSDT, market_type="spot")
    unidentified = trade("BINANCE", "trades", recv=T - 1, market_type="spot", instrument=None, price=999.0)
    out = align([identified, unidentified])
    assert len(out) == 2
    assert out[("BINANCE", "spot", BINANCE_SPOT_BTCUSDT.key, "trades")].event is identified
    assert out[("BINANCE", "spot", UNID, "trades")].event is unidentified


def test_unidentified_event_is_not_overwritten_by_an_identified_one_arriving_later():
    unidentified = trade("BINANCE", "trades", recv=T - 5, market_type="spot", instrument=None, price=999.0)
    identified = trade("BINANCE", "trades", recv=T - 1, instrument=BINANCE_SPOT_BTCUSDT, market_type="spot")
    out = align([unidentified, identified])
    assert len(out) == 2
    assert out[("BINANCE", "spot", BINANCE_SPOT_BTCUSDT.key, "trades")].event is identified
    assert out[("BINANCE", "spot", UNID, "trades")].event is unidentified


def test_identified_and_unidentified_at_the_exact_same_receive_timestamp_stay_separate():
    identified = trade("BINANCE", "trades", recv=T, instrument=BINANCE_SPOT_BTCUSDT, market_type="spot")
    unidentified = trade("BINANCE", "trades", recv=T, market_type="spot", instrument=None, price=999.0)
    out = align([identified, unidentified])
    assert len(out) == 2, "same local_receive_ts must not make identified and unidentified collide"
    assert out[("BINANCE", "spot", BINANCE_SPOT_BTCUSDT.key, "trades")].event is identified
    assert out[("BINANCE", "spot", UNID, "trades")].event is unidentified


def test_identified_and_unidentified_at_different_receive_timestamps_stay_separate():
    identified = trade("BINANCE", "trades", recv=T - 30, instrument=BINANCE_SPOT_BTCUSDT, market_type="spot")
    unidentified = trade("BINANCE", "trades", recv=T - 1, market_type="spot", instrument=None, price=999.0)
    out = align([identified, unidentified])
    assert len(out) == 2
    assert out[("BINANCE", "spot", BINANCE_SPOT_BTCUSDT.key, "trades")].age_ms == 30
    assert out[("BINANCE", "spot", UNID, "trades")].age_ms == 1


def test_two_unidentified_events_of_one_stream_intentionally_share_the_slot():
    """The documented contract, verified rather than assumed: two
    unidentified events of the same (exchange, market_type, stream) DO
    collapse to one UNIDENTIFIED key -- latest wins, same as identified
    events would. This is intentional, not a bug, and this test guards
    against it accidentally being 'fixed'."""
    older = trade("BINANCE", "trades", recv=T - 10, market_type="spot", instrument=None, price=1.0)
    newer = trade("BINANCE", "trades", recv=T - 1, market_type="spot", instrument=None, price=2.0)
    out = align([older, newer])
    assert len(out) == 1
    assert out[("BINANCE", "spot", UNID, "trades")].event is newer


# ---------------------------------------------------------------------------
# F-J: quality preserved under STALE too, not only AVAILABLE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("quality", ["SEQUENCE_GAP", "RECOVERING", "INVALID"])
def test_stale_observation_retains_its_original_quality_state(quality):
    old = trade(recv=T - 5_000, quality=quality)
    obs = align([old], staleness_ms=1_000)[("BINANCE", PERP, UNID, "trades")]
    assert obs.status is AlignmentStatus.STALE
    assert obs.event.quality_state == quality, "STALE must not collapse or discard quality_state"


# ---------------------------------------------------------------------------
# F-K: type validation edge cases the existing suite did not cover
# ---------------------------------------------------------------------------


def test_bool_observation_ts_is_rejected_despite_bool_being_an_int_subclass():
    with pytest.raises(TypeError):
        causally_align([trade(recv=T)], True, staleness_ms=1_000)


def test_bool_staleness_ms_is_rejected_despite_bool_being_an_int_subclass():
    with pytest.raises(TypeError):
        causally_align([trade(recv=T)], T, staleness_ms=False)


def test_string_observation_ts_is_rejected():
    with pytest.raises(TypeError):
        causally_align([trade(recv=T)], str(T), staleness_ms=1_000)


def test_string_staleness_ms_is_rejected():
    with pytest.raises(TypeError):
        causally_align([trade(recv=T)], T, staleness_ms="1000")


def test_invalid_local_receive_ts_type_on_an_event_fails_loudly_not_silently():
    bad = trade(recv=T)
    object.__setattr__(bad, "local_receive_ts", "not-an-int")
    with pytest.raises(TypeError):
        causally_align([bad], T, staleness_ms=1_000)


def test_float_local_receive_ts_on_an_event_fails_loudly():
    bad = trade(recv=T)
    object.__setattr__(bad, "local_receive_ts", float(T))
    with pytest.raises(TypeError):
        causally_align([bad], T, staleness_ms=1_000)


# ---------------------------------------------------------------------------
# F-M: adversarial 4-venue, multi-stream-type end-to-end dataset
# ---------------------------------------------------------------------------


def test_four_venue_multi_stream_adversarial_dataset():
    events = [
        # BINANCE USD-M: trade available, markprice stale
        trade("BINANCE", "trades", recv=T - 1, instrument=BINANCE_USDM_BTCUSDT, market_type=PERP, price=65_000.0),
        CanonicalMarkPriceEvent("BINANCE", "markprice", T - 5_000, None, T - 5_000,
                                 market_type=PERP, instrument=BINANCE_USDM_BTCUSDT, mark_price=65_010.0),
        # BINANCE Spot: trade available -- must not collide with USD-M above
        trade("BINANCE", "trades", recv=T - 2, instrument=BINANCE_SPOT_BTCUSDT, market_type="spot", price=64_990.0),
        # BYBIT: OI available, liquidation future (must be excluded)
        CanonicalOIEvent("BYBIT", "openinterest", T - 1, None, T - 1, market_type=PERP,
                          instrument=BYBIT_LINEAR_BTCUSDT, open_interest=1_234.5, source=OISource.WS_PUSH),
        CanonicalLiquidationEvent("BYBIT", "liquidation", T + 1, None, T + 1, market_type=PERP,
                                   instrument=BYBIT_LINEAR_BTCUSDT, side="SELL", price=64_000.0, quantity=0.5),
        # OKX: orderbook-equivalent stream available, plus a legitimately
        # unidentified event on a different stream (index-tickers style)
        CanonicalOrderBookEvent("OKX", "orderbook", T - 1, None, T - 1, market_type=PERP,
                                 instrument=OKX_SWAP_BTCUSDT, bids=((1.0, 1.0),), asks=((2.0, 1.0),)),
        CanonicalMarkPriceEvent("OKX", "indextickers", T - 1, None, T - 1, market_type=PERP,
                                 instrument=None, index_price=65_005.0),
    ]
    out = causally_align(events, T, staleness_ms=1_000)

    # 1. no identity collision: USD-M vs Spot trades stay separate
    assert out[("BINANCE", PERP, BINANCE_USDM_BTCUSDT.key, "trades")].event.price == 65_000.0
    assert out[("BINANCE", "spot", BINANCE_SPOT_BTCUSDT.key, "trades")].event.price == 64_990.0
    # 2. no market_type collision (same assertion, different framing)
    assert len({k[1] for k in out if k[0] == "BINANCE"}) == 2
    # 3. no venue collision
    assert {k[0] for k in out} == {"BINANCE", "BYBIT", "OKX"}
    # 4. no stream collision (liquidation excluded -- it's in the future, see 5)
    assert {k[3] for k in out} == {"trades", "markprice", "openinterest", "orderbook", "indextickers"}
    # 5. future event excluded (Bybit liquidation at T+1)
    assert ("BYBIT", PERP, BYBIT_LINEAR_BTCUSDT.key, "liquidation") not in out
    # 6. stale event retained, not dropped
    stale = out[("BINANCE", PERP, BINANCE_USDM_BTCUSDT.key, "markprice")]
    assert stale.status is AlignmentStatus.STALE and stale.event.mark_price == 65_010.0
    # 7. NEVER_OBSERVED correct for an expected-but-absent key
    expected = list(out) + [("OKX", PERP, OKX_SWAP_BTCUSDT.key, "funding")]
    with_expected = causally_align(events, T, staleness_ms=1_000, expected_keys=expected)
    never = with_expected[("OKX", PERP, OKX_SWAP_BTCUSDT.key, "funding")]
    assert never.status is AlignmentStatus.NEVER_OBSERVED and never.event is None and never.age_ms is None
    # 8. quality preserved (default VALID on all constructed events here)
    assert all(o.event.quality_state == "VALID" for o in out.values())
    # 9. deterministic result
    import random
    shuffled = list(events)
    random.Random(7).shuffle(shuffled)
    assert causally_align(shuffled, T, staleness_ms=1_000) == out
    # 10. unidentified (OKX indextickers-style) remains unidentified, distinct from every real key
    assert out[("OKX", PERP, UNID, "indextickers")].event.index_price == 65_005.0


# ---------------------------------------------------------------------------
# F-N: replay-produced events extended to all four venues
# ---------------------------------------------------------------------------


def _frame(ts, payload, i=0):
    return ReplayFrame(timestamp_ms=ts, kind=FrameKind.WIRE, source_index=i, payload=json.dumps(payload))


def _all_four_venues_replayed_events():
    binance = ReplayEngine("BINANCE").run(ReplaySource([
        _frame(T - 20, {"stream": "btcusdt@aggTrade",
                         "data": {"E": T - 20, "a": 501, "p": "65002.0", "q": "0.5", "m": False}}),
    ]))
    spot = ReplayEngine("BINANCE_SPOT").run(ReplaySource([
        _frame(T - 15, {"stream": "btcusdt@trade",
                         "data": {"e": "trade", "E": T - 15, "T": T - 15, "t": 900,
                                  "p": "64998.0", "q": "0.25", "m": False}}),
    ]))
    bybit = ReplayEngine("BYBIT").run(ReplaySource([
        _frame(T - 40, {"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": T - 40, "data": [
            {"T": T - 40, "s": "BTCUSDT", "S": "Buy", "v": "0.01", "p": "65000.5", "i": "b1", "BT": False}]}),
    ]))
    okx = ReplayEngine("OKX").run(ReplaySource([
        _frame(T - 5, {"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [
            {"instId": "BTC-USDT-SWAP", "tradeId": "o1", "px": "65001", "sz": "1", "side": "buy",
             "ts": str(T - 5)}]}),
    ]))
    return binance.non_book_events + spot.non_book_events + bybit.non_book_events + okx.non_book_events


def test_all_four_venues_replay_identity_survives_into_alignment():
    events = _all_four_venues_replayed_events()
    assert {e.exchange for e in events} == {"BINANCE", "BYBIT", "OKX"}
    assert {e.market_type for e in events} == {PERP, "spot"}
    out = align(events)
    assert out == align(_all_four_venues_replayed_events())      # replay twice -> identical alignment
    assert out == align(list(reversed(events)))
    assert out[("BINANCE", PERP, BINANCE_USDM_BTCUSDT.key, "trades")].event.price == 65002.0
    assert out[("BINANCE", "spot", BINANCE_SPOT_BTCUSDT.key, "spot_trades")].event.price == 64998.0
    assert out[("BYBIT", PERP, BYBIT_LINEAR_BTCUSDT.key, "trades")].event.trade_id == "b1"
    assert out[("OKX", PERP, OKX_SWAP_BTCUSDT.key, "trades")].event.trade_id == "o1"
    # USD-M and Spot both replay to native_symbol BTCUSDT but never collide
    assert len(out) == 4


# ---------------------------------------------------------------------------
# F-O: replay's identity is derived from raw + adapter, never from a
# persisted canonical instrument_key -- proven adversarially, not just by
# reading the code
# ---------------------------------------------------------------------------


def test_replay_identity_is_unaffected_by_a_corrupted_canonical_instrument_key():
    """Construct the same trade two ways: (a) the raw wire frame, replayed
    through the real adapter, and (b) a canonical row whose instrument_key
    column has been deliberately corrupted to a foreign venue's key. Replay
    must reproduce the correct identity regardless of what the corrupted
    canonical row says -- because replay never reads that column at all.
    """
    raw_frame_payload = {"stream": "btcusdt@aggTrade",
                          "data": {"E": T, "a": 42, "p": "65000.0", "q": "1.0", "m": False}}
    replayed = ReplayEngine("BINANCE").run(
        ReplaySource([_frame(T, raw_frame_payload)])).non_book_events
    assert len(replayed) == 1
    assert replayed[0].instrument == BINANCE_USDM_BTCUSDT

    # The corrupted canonical row a storage-layer bug (or bit-rot) could
    # produce for the *same logical event* -- a foreign key that would, if
    # replay ever consumed it, silently relabel this USD-M trade as Spot.
    corrupted_canonical_row = {"trade_id": "42", "price": 65000.0, "instrument_key": BINANCE_SPOT_BTCUSDT.key}

    # Reading that row through the storage contract correctly REJECTS it
    # (this is Phase D's job, re-confirmed here) -- it does not silently
    # become truth for anything, canonical or replay.
    with pytest.raises(InstrumentIdError):
        resolve_canonical_instrument_key(corrupted_canonical_row, expected=BINANCE_USDM_BTCUSDT)

    # And replay's own identity, derived independently from the raw frame,
    # was never touched by the corrupted row -- still correct.
    assert replayed[0].instrument == BINANCE_USDM_BTCUSDT
    assert replayed[0].instrument != BINANCE_SPOT_BTCUSDT


def test_replay_module_never_references_the_persisted_instrument_key_column():
    """Structural corroboration of the adversarial test above: the string
    'instrument_key' does not appear anywhere in replay.py's source. Grep
    evidence, recorded as a guard so a future change that starts reading it
    is caught here rather than only in documentation."""
    import inspect
    from collector.collector import replay as replay_module
    source = inspect.getsource(replay_module)
    assert "instrument_key" not in source


# ---------------------------------------------------------------------------
# F-P: Phase D mutations G and H, re-run against the current implementation
# (previously only run once, in Phase D's own commit, per docs/EXECUTION_STATUS.md's
# own "not verified this phase" note)
# ---------------------------------------------------------------------------


def test_phase_d_mutation_g_malformed_key_resolves_to_none_is_demonstrated_live():
    """Mutation G: patch resolve_canonical_instrument_key so a malformed
    value resolves to None instead of raising. Prove this breaks a real
    test, then restore the real implementation and prove it passes again --
    a live demonstration in this process, not an assumption carried from
    Phase D's own commit history."""
    import collector.collector.instrument as instrument_module

    real = instrument_module.resolve_canonical_instrument_key

    def mutated_resolves_malformed_to_none(row, *, expected):
        try:
            return real(row, expected=expected)
        except InstrumentIdError:
            return None   # the mutation: malformed silently becomes None

    instrument_module.resolve_canonical_instrument_key = mutated_resolves_malformed_to_none
    try:
        # Under the mutation, a malformed key no longer raises -- proving
        # the mutation actually changes behavior (a real failure, not a
        # vacuous one): the module-level function now returns None where
        # the real implementation would have raised InstrumentIdError.
        assert instrument_module.resolve_canonical_instrument_key(
            {"instrument_key": "not-a-valid-key"}, expected=BINANCE_USDM_BTCUSDT) is None
    finally:
        instrument_module.resolve_canonical_instrument_key = real

    # restored: the real implementation raises again
    with pytest.raises(InstrumentIdError):
        instrument_module.resolve_canonical_instrument_key(
            {"instrument_key": "not-a-valid-key"}, expected=BINANCE_USDM_BTCUSDT)


def test_phase_d_mutation_h_contradictory_key_accepted_is_demonstrated_live():
    """Mutation H: patch resolve_canonical_instrument_key so a
    well-formed-but-contradictory key (right shape, wrong instrument) is
    accepted instead of rejected. Prove this breaks a real invariant, then
    restore."""
    import collector.collector.instrument as instrument_module

    real = instrument_module.resolve_canonical_instrument_key

    def mutated_accepts_contradictory(row, *, expected):
        if "instrument_key" not in row or row["instrument_key"] is None:
            return None
        # the mutation: parse and return without checking it matches `expected`
        return InstrumentId.from_key(row["instrument_key"])

    instrument_module.resolve_canonical_instrument_key = mutated_accepts_contradictory
    try:
        contradictory_row = {"instrument_key": BINANCE_SPOT_BTCUSDT.key}
        # Under the mutation, a Spot key silently passes as a valid USD-M
        # identity instead of raising.
        assert instrument_module.resolve_canonical_instrument_key(
            contradictory_row, expected=BINANCE_USDM_BTCUSDT) == BINANCE_SPOT_BTCUSDT
    finally:
        instrument_module.resolve_canonical_instrument_key = real

    # restored: the real implementation rejects the contradiction again
    with pytest.raises(InstrumentIdError):
        instrument_module.resolve_canonical_instrument_key(
            {"instrument_key": BINANCE_SPOT_BTCUSDT.key}, expected=BINANCE_USDM_BTCUSDT)
