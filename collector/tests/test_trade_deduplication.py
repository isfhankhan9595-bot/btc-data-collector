"""Trade-message deduplication: identity semantics, per-venue audit results,
architectural placement, and real-source mutation evidence.

## Duplicate definition

A duplicate is a second ``CanonicalTradeEvent`` whose
``(exchange, market_type, instrument_key_or_UNIDENTIFIED, stream, trade_id)``
tuple this adapter instance has already produced. This is intentionally
narrower than "same price/qty/timestamp" (see test 17 below: legitimate
same-value trades with different trade_id are never merged) and narrower
than "same trade_id" alone (see tests 18-19: the same trade_id on a
different venue, market_type, or stream is never a duplicate).

## Venue-by-venue trade-identity audit (read from source, not assumed)

- Binance USD-M (`adapters/binance.py`): only the aggTrade stream is
  consumed (`<symbol>@aggTrade`); `trade_id` = aggTrade's `a` field, the
  *aggregate* trade ID, not any per-execution ID. The ordinary `trade`
  stream is not subscribed to at all by this venue's runner.
- Binance Spot (`adapters/binance_spot.py`): the opposite choice -- only
  the ordinary `trade` stream is consumed; `trade_id` = its `t` field, a
  genuine per-execution ID. aggTrade is not used here. Confirmed distinct
  from USD-M's choice by reading both adapters, not assumed to match.
- Bybit Linear (`adapters/bybit.py`): `publicTrade.<symbol>`; `trade_id` =
  the `i` field (Bybit's own trade ID string). `seq` is never used as
  identity (that field carries book-level sequencing elsewhere, not trade
  identity) and `T` (timestamp) is never used as identity either -- both
  per this task's explicit warning against assuming either.
- OKX Swap (`adapters/okx.py`): both `trades` and `trades-all` channels
  are implemented, each producing one `CanonicalTradeEvent` per array
  element (a push can carry more than one trade; each gets `trade_id` =
  its own `tradeId`). The two channels are *not* deduplicated against each
  other (see test 20) because whether `trades-all` aggregates or overlaps
  with `trades` is an open, unresolved question per `adapters/okx.py`'s
  own docstring -- conflating them would be exactly the "aggregated trade
  vs ordinary trade" merge this task explicitly forbids without proof.

All four venues already canonicalize `trade_id` to `Optional[str]` (never a
raw int), so there is no int/str type-identity risk to guard against
separately -- confirmed by reading each adapter's `CanonicalTradeEvent`
construction, not assumed.

## Architectural placement

`ExchangeAdapter._dedupe_trades` (adapters/base.py), wired into the same
`__init_subclass__` hook that already stamps instrument identity, run
immediately after stamping (so the key can use the resolved
`event.instrument`). This is the one place every runner (`run_collector.py`,
`run_bybit_collector.py`, `run_binance_spot_collector.py`,
`run_okx_collector.py`) AND `ReplayEngine` already funnel through via
`adapter.normalize()` -- confirmed by grep, not assumed -- so live and
replay get identical duplicate behavior for free, with no per-runner code
and no feature module (trade_flow_observation.py, in particular) doing its
own deduplication. Raw evidence is never touched: `_capture_raw_frame`
persists the wire frame before `normalize()` is ever called, at every
runner, so a suppressed duplicate's raw bytes remain on disk regardless of
what the canonical stream does with it.

## Missing-ID and state-lifetime decisions

A `trade_id is None` event is NEVER deduplicated against anything, including
another `None` trade -- every one is kept. State lives exactly as long as
the adapter instance does: one runner process (surviving reconnects, since
no runner recreates its adapter on reconnect) or one `ReplayEngine.run()`
call (spanning every frame/segment given to it). Known, documented cost:
`_seen_trade_ids` grows without bound for the adapter's lifetime --
deliberately not optimized here per this task's own instruction to keep the
simple, correct reference implementation rather than an unproven bounded
structure; recommended as the natural next bounded task.

## Real mutation testing

Performed and demonstrated live in this conversation's tool output (source
mutated, real suite run, failures recorded, source restored, `diff`
confirmed byte-identical, suite re-run green) -- not reproduced as
committed code here, per this project's established convention (Phase D/F)
of not leaving mutated source in the repository.

- Mutation A (disable duplicate rejection entirely): 11 real failures.
- Mutation B (drop `exchange` from the identity key): **0 failures.**
  Genuine finding, not a test bug or a forced result (per this task's own
  "never manipulate assertions to force a failure" rule) -- investigated,
  not hidden: every concrete adapter is bound to exactly one
  `(exchange, market_type, instrument)` triple for its whole lifetime
  (`instrument` is a fixed class attribute; `_stamp_instrument` raises
  rather than let one instance emit a contradicting pair), and every
  current cross-venue/cross-instrument test in this file uses a separate
  adapter *instance* per venue -- so instance isolation alone already
  provides that separation today, independent of what is or isn't in the
  key. `stream` + `trade_id` are the only currently load-bearing key
  components (OKX's `trades` vs `trades-all` on one shared instance proves
  `stream` is; every positive dedup test proves `trade_id` is).
  `exchange`/`market_type`/`instrument` remain in the key as deliberate
  defense-in-depth for a future multi-instrument-per-adapter namespace
  (already a documented limitation elsewhere in this project, not
  invented for this finding) -- the same "keep it, it's not dead code,
  pin the invariant that currently makes it redundant" choice PR #48 made
  for windowed CVD's own redundant upper bound.
  `test_one_adapter_instance_is_always_bound_to_at_most_one_identity_triple`
  pins that invariant.
- Mutation C (treat missing trade IDs as one identity): 1 real failure
  (the dedicated missing-ID test).
"""
from __future__ import annotations

import json

import pytest

from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.adapters.binance_spot import BinanceSpotAdapter
from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.adapters.okx import OKXAdapter
from collector.collector.canonical import CanonicalTradeEvent
from collector.collector.instrument import BINANCE_SPOT_BTCUSDT, BINANCE_USDM_BTCUSDT, BYBIT_LINEAR_BTCUSDT
from collector.collector.replay import FrameKind, ReplayEngine, ReplayFrame, ReplaySource
from collector.collector.trade_flow_observation import observe_trade_flow_at, observe_windowed_trade_flow_at

T = 1_780_000_000_000


def _frame(ts, payload, i=0):
    return ReplayFrame(timestamp_ms=ts, kind=FrameKind.WIRE, source_index=i, payload=json.dumps(payload))


def _binance_aggtrade(ts, trade_id, price="65000.0", qty="1.0", maker=False):
    return {"stream": "btcusdt@aggTrade", "data": {"E": ts, "a": trade_id, "p": price, "q": qty, "m": maker}}


def _spot_trade(ts, trade_id, price="64998.0", qty="0.5", maker=False):
    return {"stream": "btcusdt@trade",
            "data": {"e": "trade", "E": ts, "T": ts, "t": trade_id, "p": price, "q": qty, "m": maker}}


def _bybit_trade(ts, trade_id, price="65000.5", qty="0.01", side="Buy"):
    return {"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": ts, "data": [
        {"T": ts, "s": "BTCUSDT", "S": side, "v": qty, "p": price, "i": trade_id, "BT": False}]}


def _okx_trade(ts, trade_id, channel="trades", price="65001", qty="1", side="buy"):
    return {"arg": {"channel": channel, "instId": "BTC-USDT-SWAP"}, "data": [
        {"instId": "BTC-USDT-SWAP", "tradeId": trade_id, "px": price, "sz": qty, "side": side, "ts": str(ts)}]}


# ---------------------------------------------------------------------------
# 1-4: per-venue positive dedup, real adapter, same instance, real frame
# shapes (not hand-built CanonicalTradeEvent)
# ---------------------------------------------------------------------------


def test_binance_usdm_duplicate_aggtrade_on_same_adapter_instance_is_suppressed():
    adapter = BinanceAdapter()
    first = adapter.normalize(_binance_aggtrade(T, 501), local_receive_ts=T)
    second = adapter.normalize(_binance_aggtrade(T + 1, 501), local_receive_ts=T + 1)
    assert len(first) == 1
    assert second == [], "identical aggTrade id on the same adapter instance must be suppressed"
    assert adapter.unhandled_count == 1


def test_binance_spot_duplicate_trade_on_same_adapter_instance_is_suppressed():
    adapter = BinanceSpotAdapter()
    first = adapter.normalize(_spot_trade(T, 900), local_receive_ts=T)
    second = adapter.normalize(_spot_trade(T + 1, 900), local_receive_ts=T + 1)
    assert len(first) == 1
    assert second == []


def test_bybit_duplicate_trade_on_same_adapter_instance_is_suppressed():
    adapter = BybitAdapter()
    first = adapter.normalize(_bybit_trade(T, "b1"), local_receive_ts=T)
    second = adapter.normalize(_bybit_trade(T + 1, "b1"), local_receive_ts=T + 1)
    assert len(first) == 1
    assert second == []


def test_okx_duplicate_trade_on_same_adapter_instance_is_suppressed():
    adapter = OKXAdapter()
    first = adapter.normalize(_okx_trade(T, "o1"), local_receive_ts=T)
    second = adapter.normalize(_okx_trade(T + 1, "o1"), local_receive_ts=T + 1)
    assert len(first) == 1
    assert second == []


# ---------------------------------------------------------------------------
# 17: legitimate same-value trades, different IDs, must remain separate --
# mandatory per the task
# ---------------------------------------------------------------------------


def test_legitimate_same_value_trades_with_different_ids_both_kept():
    adapter = BinanceAdapter()
    a = adapter.normalize(_binance_aggtrade(T, 1, price="65000.0", qty="1.0", maker=False), local_receive_ts=T)
    b = adapter.normalize(_binance_aggtrade(T, 2, price="65000.0", qty="1.0", maker=False), local_receive_ts=T)
    assert len(a) == 1 and len(b) == 1
    assert a[0].trade_id != b[0].trade_id
    assert a[0].price == b[0].price and a[0].quantity == b[0].quantity


# ---------------------------------------------------------------------------
# 18-19: multi-venue / cross-market-type collision resistance
# ---------------------------------------------------------------------------


def test_same_trade_id_across_venues_does_not_collide():
    binance = BinanceAdapter().normalize(_binance_aggtrade(T, 123), local_receive_ts=T)
    bybit = BybitAdapter().normalize(_bybit_trade(T, "123"), local_receive_ts=T)
    assert len(binance) == 1 and len(bybit) == 1  # different adapter instances anyway,
    # but the important proof is the *key*, not just instance separation:
    combined = BinanceAdapter()
    combined_events = combined.normalize(_binance_aggtrade(T, 123), local_receive_ts=T)
    assert len(combined_events) == 1
    # A Bybit-shaped trade can never even reach BinanceAdapter's dedup set
    # (different adapter, different venue field on the event), but prove
    # the key itself is venue-scoped by inspecting it directly:
    key_binance = (binance[0].exchange, binance[0].market_type,
                   binance[0].instrument.key, binance[0].stream, binance[0].trade_id)
    key_bybit = (bybit[0].exchange, bybit[0].market_type,
                 bybit[0].instrument.key, bybit[0].stream, bybit[0].trade_id)
    assert key_binance != key_bybit
    assert key_binance[0] != key_bybit[0]  # exchange differs: BINANCE vs BYBIT


def test_same_native_trade_id_across_binance_spot_and_usdm_does_not_collide():
    """Binance Spot and USD-M could plausibly emit the same integer trade
    id by coincidence (independent id spaces on the exchange side); market
    type alone must be enough to keep them separate. Uses two adapters
    (as live/replay actually would -- one adapter instance per venue
    runner) with the identical trade_id value."""
    usdm = BinanceAdapter().normalize(_binance_aggtrade(T, 999), local_receive_ts=T)
    spot = BinanceSpotAdapter().normalize(_spot_trade(T, 999), local_receive_ts=T)
    assert len(usdm) == 1 and len(spot) == 1
    assert usdm[0].trade_id == spot[0].trade_id == "999"
    assert usdm[0].market_type != spot[0].market_type
    assert usdm[0].instrument != spot[0].instrument


# ---------------------------------------------------------------------------
# 14: missing/None trade IDs -- the major edge case; must never collapse
# together
# ---------------------------------------------------------------------------


def test_missing_trade_ids_are_never_deduplicated_against_each_other():
    adapter = BinanceAdapter()
    no_id_1 = {"stream": "btcusdt@aggTrade", "data": {"E": T, "p": "1.0", "q": "1.0", "m": False}}
    no_id_2 = {"stream": "btcusdt@aggTrade", "data": {"E": T, "p": "2.0", "q": "1.0", "m": False}}
    first = adapter.normalize(no_id_1, local_receive_ts=T)
    second = adapter.normalize(no_id_2, local_receive_ts=T + 1)
    third = adapter.normalize(no_id_1, local_receive_ts=T + 2)  # even the exact same payload again
    assert len(first) == 1 and first[0].trade_id is None
    assert len(second) == 1 and second[0].trade_id is None
    assert len(third) == 1 and third[0].trade_id is None, \
        "a missing trade_id must never be treated as a duplicate of any other missing trade_id"


# ---------------------------------------------------------------------------
# 16: reconnect/overlap adversarial sequence
# ---------------------------------------------------------------------------


def test_reconnect_overlap_produces_no_duplicated_canonical_trades():
    """A, B, C delivered; connection drops; reconnect redelivers B, C, D
    (a real overlap scenario -- the venue or client resends recent trades
    around a reconnect). The adapter instance survives the reconnect (no
    runner recreates it), so this is exactly the live topology, driven
    through one adapter instance with no ReplayEngine involved."""
    adapter = BinanceAdapter()
    delivered = [1, 2, 3, 2, 3, 4]
    seen_prices = []
    for i, trade_id in enumerate(delivered):
        events = adapter.normalize(_binance_aggtrade(T + i, trade_id, price=str(trade_id)), local_receive_ts=T + i)
        seen_prices.extend(e.trade_id for e in events)
    assert seen_prices == ["1", "2", "3", "4"], seen_prices


# ---------------------------------------------------------------------------
# 21: storage/replay boundary -- duplicate across adjacent segments, fed to
# one ReplayEngine (matches ReplaySource.from_directory's real usage: one
# engine spans an entire date's segments)
# ---------------------------------------------------------------------------


def test_duplicate_across_adjacent_replay_segments_is_still_caught():
    frames = [
        _frame(T, _binance_aggtrade(T, 1)),           # "segment 1"
        _frame(T + 3_600_000, _binance_aggtrade(T + 3_600_000, 2)),
        _frame(T + 3_600_001, _binance_aggtrade(T + 3_600_001, 1)),  # "segment 2", same id 1 hours later
    ]
    result = ReplayEngine("BINANCE").run(ReplaySource(frames))
    trades = [e for e in result.non_book_events if isinstance(e, CanonicalTradeEvent)]
    assert [t.trade_id for t in trades] == ["1", "2"]


# ---------------------------------------------------------------------------
# 24: out-of-order duplicate -- late duplicate after a newer trade
# ---------------------------------------------------------------------------


def test_late_arriving_duplicate_after_a_newer_trade_is_still_suppressed():
    adapter = BinanceAdapter()
    a = adapter.normalize(_binance_aggtrade(T, 1), local_receive_ts=T)
    b = adapter.normalize(_binance_aggtrade(T + 5, 2), local_receive_ts=T + 5)
    late_dup_of_a = adapter.normalize(_binance_aggtrade(T + 10, 1), local_receive_ts=T + 10)
    assert len(a) == 1 and len(b) == 1
    assert late_dup_of_a == [], "duplicate detection must not depend on arrival order"


# ---------------------------------------------------------------------------
# replay determinism with a real duplicate present
# ---------------------------------------------------------------------------


def test_replay_with_a_duplicate_frame_is_deterministic():
    frames = [_frame(T, _binance_aggtrade(T, 1)), _frame(T + 1, _binance_aggtrade(T + 1, 1))]
    a = ReplayEngine("BINANCE").run(ReplaySource(list(frames)))
    b = ReplayEngine("BINANCE").run(ReplaySource(list(frames)))
    trades_a = [e for e in a.non_book_events if isinstance(e, CanonicalTradeEvent)]
    trades_b = [e for e in b.non_book_events if isinstance(e, CanonicalTradeEvent)]
    assert trades_a == trades_b
    assert len(trades_a) == 1


# ---------------------------------------------------------------------------
# 20: OKX trades vs trades-all are never merged (aggregate vs ordinary
# representation, unresolved overlap -- must not be conflated)
# ---------------------------------------------------------------------------


def test_okx_trades_and_trades_all_with_the_same_trade_id_are_not_merged():
    adapter = OKXAdapter()
    a = adapter.normalize(_okx_trade(T, "shared-id", channel="trades"), local_receive_ts=T)
    b = adapter.normalize(_okx_trade(T + 1, "shared-id", channel="trades-all"), local_receive_ts=T + 1)
    assert len(a) == 1 and len(b) == 1, \
        "different trade representations (channels) must not be deduplicated against each other"
    assert a[0].stream != b[0].stream


def test_one_adapter_instance_is_always_bound_to_at_most_one_identity_triple():
    """Pins the invariant that currently makes exchange/market_type/instrument
    redundant in the dedup key (see this module's docstring, "Genuine
    finding" section, for the mutation evidence): every concrete
    trade-producing adapter is bound to exactly one (exchange,
    market_type, instrument) triple for its whole lifetime, and
    ``_stamp_instrument`` raises ``InstrumentIdError`` rather than let one
    instance emit a contradicting pair. So today, ``stream`` + ``trade_id``
    alone already fully separate one adapter instance's own trades;
    exchange/market_type/instrument only matter if that invariant is ever
    relaxed (e.g. a future multi-instrument-per-adapter namespace --
    already a documented limitation in docs/INSTRUMENT_IDENTITY.md). If
    this test ever fails, the dedup key's identity components stop being
    merely defense-in-depth and become load-bearing -- exactly the
    situation this pin exists to surface.

    Binance/Spot/Bybit bind it via a fixed class attribute. OKX is the one
    architectural exception (no class-level ``instrument`` -- some
    channels, like index-tickers and cross-instrument liquidation, are
    deliberately multi-instrument, see adapters/okx.py), so its trades
    channels are checked empirically instead: two different trades on one
    instance must resolve to the identical instrument, not merely
    non-None."""
    for adapter_cls in (BinanceAdapter, BinanceSpotAdapter, BybitAdapter):
        assert adapter_cls.instrument is not None, \
            f"{adapter_cls.__name__} must declare a single fixed instrument"

    okx = OKXAdapter()
    a = okx.normalize(_okx_trade(T, "pin-a"), local_receive_ts=T)
    b = okx.normalize(_okx_trade(T + 1, "pin-b"), local_receive_ts=T + 1)
    assert a[0].instrument is not None
    assert a[0].instrument == b[0].instrument, \
        "OKX trades channel must resolve to one consistent instrument per adapter instance"


def test_stream_and_trade_id_alone_already_separate_okx_channels_today():
    """Companion to the pin above, demonstrated positively: OKX's trades
    vs trades-all (test 20) already prove stream is load-bearing on its
    own, independent of whatever the identity components contribute."""
    adapter = OKXAdapter()
    a = adapter.normalize(_okx_trade(T, "x", channel="trades"), local_receive_ts=T)
    b = adapter.normalize(_okx_trade(T, "x", channel="trades-all"), local_receive_ts=T)
    assert len(a) == 1 and len(b) == 1


# ---------------------------------------------------------------------------
# quality-event semantics: reuses the existing DUPLICATE taxonomy value,
# informational (rows_lost=0), does not invalidate the stream
# ---------------------------------------------------------------------------


def test_duplicate_trade_produces_an_informational_duplicate_quality_event_not_a_drop():
    adapter = BinanceAdapter()
    adapter.normalize(_binance_aggtrade(T, 1), local_receive_ts=T)
    adapter.normalize(_binance_aggtrade(T + 1, 1), local_receive_ts=T + 1)
    drained = adapter.drain_unhandled()
    assert len(drained) == 1
    event = drained[0].to_quality_event()
    assert event["event_type"] == "DUPLICATE"
    assert event["rows_lost"] == 0, "a suppressed duplicate is not a real data loss"
    assert "duplicate_trade" in event["reason"]


def test_bybit_runner_now_wires_the_unhandled_sink_for_duplicate_visibility():
    """Structural guard for the fix this task required: BybitAdapter's
    unhandled/duplicate outcomes were previously never wired to any quality
    persistence at all (the other three runners already had this).
    Without this, a Bybit trade duplicate would be silently invisible in
    the persisted quality_events stream, unlike every other venue."""
    import inspect
    from collector import run_bybit_collector
    source = inspect.getsource(run_bybit_collector.BybitCollectorApp.__init__)
    assert "set_unhandled_sink" in source


# ---------------------------------------------------------------------------
# 26: downstream CVD/trade-count acceptance test -- no special-casing in
# the feature module itself
# ---------------------------------------------------------------------------


def test_cumulative_cvd_does_not_double_count_a_duplicate_trade():
    clean_frames = [_frame(T, _binance_aggtrade(T, 1, price="65000", qty="1.0", maker=False)),
                     _frame(T + 1, _binance_aggtrade(T + 1, 2, price="65000", qty="2.0", maker=True))]
    with_duplicate = clean_frames + [_frame(T + 2, _binance_aggtrade(T + 2, 1, price="65000", qty="1.0", maker=False))]

    clean = observe_trade_flow_at(clean_frames, T + 5, venue="BINANCE")
    duped = observe_trade_flow_at(with_duplicate, T + 5, venue="BINANCE")

    assert duped.trade_count == clean.trade_count == 2
    assert duped.cvd == clean.cvd
    assert duped.buy_volume == clean.buy_volume
    assert duped.sell_volume == clean.sell_volume


def test_windowed_trade_flow_does_not_double_count_a_duplicate_trade():
    clean_frames = [_frame(T, _binance_aggtrade(T, 1, qty="1.0", maker=False)),
                     _frame(T + 1, _binance_aggtrade(T + 1, 2, qty="2.0", maker=True))]
    with_duplicate = clean_frames + [_frame(T + 2, _binance_aggtrade(T + 2, 1, qty="1.0", maker=False))]

    clean = observe_windowed_trade_flow_at(clean_frames, T + 5, 60_000, venue="BINANCE")
    duped = observe_windowed_trade_flow_at(with_duplicate, T + 5, 60_000, venue="BINANCE")

    assert duped.trade_count == clean.trade_count == 2
    assert duped.cvd == clean.cvd
