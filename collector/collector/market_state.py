"""Market State Engine V0 — descriptive, causal, venue-local market state.

This answers "what market conditions currently exist", from validated
canonical events, using only what the collector had actually received by
a given moment. It is not a signal engine: it produces no BUY/SELL output,
no confidence score, no prediction. See docs/MARKET_STATE_V0.md.

Causal contract
----------------
``MarketStateEngine.snapshot(observation_ts)`` is a **pure function** of
(every event given to ``update()`` so far, ``observation_ts``): it recomputes
state from scratch each call rather than mutating a running state in place.
This is a deliberate simplification over an incremental design, made because
it makes every one of the required guarantees structural rather than
merely tested-for:

* A snapshot uses only events with ``local_receive_ts <= observation_ts``
  (never ``exchange_event_ts``, which is not proof the collector had the
  data -- a slow response can carry an exchange timestamp far earlier than
  when it actually arrived).
* Calling ``update()`` with a **late-arriving** event can never change the
  answer to an earlier ``snapshot(observation_ts)`` call, because that
  event's own ``local_receive_ts`` still governs which snapshots it is
  eligible for -- there is no mutable "current state" for it to overwrite.
* Replaying the identical sequence of ``update()`` calls, in any order,
  followed by the same ``snapshot(observation_ts)`` call, produces an
  identical result -- the function only reads local_receive_ts-filtered
  events, order of insertion does not matter.
* No wall-clock reads, no network access, no randomness, no pandas: every
  input is an explicit argument.

Venue-local, not cross-exchange
--------------------------------
One engine instance describes one exchange. Cross-exchange derived state
is deliberately out of scope for V0 (see docs/MARKET_STATE_V0.md,
"Non-goals") -- `cross_exchange_alignment.py` is a separate module (P6,
`tests/test_cross_exchange_alignment.py`, 33 tests) keyed on
`(exchange, market_type, instrument_key, stream)` identity with causal
`local_receive_ts <= observation_ts` availability. This engine does not
consume it: building derived cross-venue state directly into a venue-local
engine would still risk the "silently collapse two venues into one"
failure this project's rules forbid, regardless of how well-tested the
alignment primitive itself now is.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .book_engine import NON_AUTHORITATIVE_BOOK_SOURCES
from .instrument import InstrumentId
from .canonical import (
    CanonicalLiquidationEvent,
    CanonicalMarkPriceEvent,
    CanonicalOIEvent,
    CanonicalOrderBookEvent,
    CanonicalTradeEvent,
    OIUnit,
)

#: A dimension is "stale" once its most recent contributing event is older
#: than this many ms relative to observation_ts. Deliberately generous and
#: venue/field-specific rather than one global number -- a book update every
#: 100ms and a funding push every 8h cannot share a staleness definition.
DEFAULT_BOOK_STALE_MS = 5_000
DEFAULT_MARK_STALE_MS = 10_000
DEFAULT_OI_STALE_MS = 120_000
DEFAULT_FUNDING_STALE_MS = 3_600_000
DEFAULT_TRADE_FLOW_WINDOW_MS = 60_000


@dataclass(frozen=True)
class PriceState:
    last_trade_price: Optional[float] = None
    last_trade_ts: Optional[int] = None
    mark_price: Optional[float] = None
    index_price: Optional[float] = None
    mark_ts: Optional[int] = None
    mark_stale: bool = True          # True until proven fresh -- never defaults to "fresh"
    price_vs_mark: Optional[float] = None   # last_trade_price - mark_price, only when both exist


@dataclass(frozen=True)
class BookState:
    """From the authoritative reconstructed book only.

    A ``depth10``-style partial snapshot is never authoritative (see
    ``book_engine.NON_AUTHORITATIVE_BOOK_SOURCES``) and this engine takes no
    steps of its own to filter one out -- callers must pass only events that
    have already cleared that check (i.e. events a `LocalBook` actually
    applied), exactly as `run_bybit_collector.py` and `run_collector.py`
    already do before persisting a book row.
    """
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    mid: Optional[float] = None
    spread: Optional[float] = None
    spread_bps: Optional[float] = None
    #: (bid_qty - ask_qty) / (bid_qty + ask_qty) at best level, in [-1, 1].
    #: Deliberately named book_imbalance, not "OFI": this is a static
    #: snapshot ratio of resting size, not order-FLOW (which requires
    #: observing book *changes* over time, not one snapshot).
    book_imbalance: Optional[float] = None
    book_ts: Optional[int] = None
    book_available: bool = False
    book_stale: bool = True


@dataclass(frozen=True)
class TradeFlowState:
    #: Cumulative since the engine's first observed trade -- unambiguous and
    #: simple; not "since observation_ts minus some window", which would
    #: need its own separate, clearly-labelled fields (below).
    cumulative_buy_volume: float = 0.0
    cumulative_sell_volume: float = 0.0
    cvd: float = 0.0                 # cumulative_buy_volume - cumulative_sell_volume
    trade_count: int = 0
    #: The same three quantities, but only over the trailing window ending
    #: at observation_ts -- kept as distinctly-named fields so a reader can
    #: never confuse a windowed number for the cumulative one.
    window_ms: int = DEFAULT_TRADE_FLOW_WINDOW_MS
    window_buy_volume: float = 0.0
    window_sell_volume: float = 0.0
    window_net_volume: float = 0.0
    window_trade_count: int = 0
    last_trade_ts: Optional[int] = None
    trades_observed: bool = False    # False: no trade has ever been seen yet


@dataclass(frozen=True)
class LiquidationState:
    """Direction is deliberately NOT labelled long/short in V0.

    A forced BUY liquidation order generally closes a short position, and a
    forced SELL generally closes a long -- but this mapping has not been
    individually re-verified against each adapter's actual `side` semantics
    in this pass (Binance/Bybit/OKX may not encode it identically), and the
    project's own rule is explicit: "do not invert semantics without
    checking each adapter." Rather than assert an unverified mapping, this
    state tracks the raw venue-reported side only.
    """
    liquidation_count: int = 0
    buy_side_quantity: float = 0.0
    sell_side_quantity: float = 0.0
    last_liquidation_ts: Optional[int] = None
    liquidations_observed: bool = False


@dataclass(frozen=True)
class DerivativesState:
    open_interest: Optional[float] = None
    oi_unit: OIUnit = OIUnit.UNKNOWN
    oi_ts: Optional[int] = None
    #: Change vs the previous OI observation from THIS engine (same
    #: exchange, same unit by construction -- see canonical.py's OIUnit
    #: contract: same-venue UNKNOWN-vs-UNKNOWN over time is safe, since one
    #: venue's stream is one physical quantity regardless of whether that
    #: quantity's name is documented).
    oi_change: Optional[float] = None
    oi_stale: bool = True
    funding_rate: Optional[float] = None
    funding_ts: Optional[int] = None
    funding_age_ms: Optional[int] = None
    funding_stale: bool = True


@dataclass(frozen=True)
class MarketState:
    exchange: str
    observation_ts: int
    price: PriceState = field(default_factory=PriceState)
    book: BookState = field(default_factory=BookState)
    trade_flow: TradeFlowState = field(default_factory=TradeFlowState)
    liquidation: LiquidationState = field(default_factory=LiquidationState)
    derivatives: DerivativesState = field(default_factory=DerivativesState)
    #: The instrument this state describes (``None`` if the engine was not bound to one).
    instrument: Optional[InstrumentId] = None

    def digest(self) -> str:
        """A stable string for replay-parity comparison.

        Deliberately not Python's ``hash()`` (salted per-process for
        strings, so it is not stable across runs/processes -- exactly the
        kind of nondeterminism this engine must not exhibit) and not
        ``repr()`` of the dataclass directly (field insertion order in a
        nested dataclass repr is stable within one Python version but is an
        implementation detail, not a documented contract). Uses a fixed,
        explicit tuple of every field instead.
        """
        import hashlib
        parts = (
            self.exchange, self.instrument.key if self.instrument is not None else None,
            self.observation_ts,
            self.price.last_trade_price, self.price.last_trade_ts,
            self.price.mark_price, self.price.index_price, self.price.mark_ts,
            self.price.mark_stale, self.price.price_vs_mark,
            self.book.best_bid, self.book.best_ask, self.book.mid,
            self.book.spread, self.book.spread_bps, self.book.book_imbalance,
            self.book.book_ts, self.book.book_available, self.book.book_stale,
            self.trade_flow.cumulative_buy_volume, self.trade_flow.cumulative_sell_volume,
            self.trade_flow.cvd, self.trade_flow.trade_count,
            self.trade_flow.window_buy_volume, self.trade_flow.window_sell_volume,
            self.trade_flow.window_net_volume, self.trade_flow.window_trade_count,
            self.trade_flow.trades_observed,
            self.liquidation.liquidation_count, self.liquidation.buy_side_quantity,
            self.liquidation.sell_side_quantity, self.liquidation.liquidations_observed,
            self.derivatives.open_interest, self.derivatives.oi_unit.value,
            self.derivatives.oi_change, self.derivatives.oi_stale,
            self.derivatives.funding_rate, self.derivatives.funding_age_ms,
            self.derivatives.funding_stale,
        )
        return hashlib.sha256(repr(parts).encode()).hexdigest()


class MarketStateEngine:
    """Accumulates canonical events for one venue; ``snapshot()`` is pure.

    Events are stored append-only, grouped by type, sorted by
    ``local_receive_ts`` lazily inside ``snapshot()`` rather than at insert
    time -- ``update()`` never needs to know what order events will later be
    queried in, which is what makes "a late event cannot rewrite an earlier
    snapshot" true by construction rather than by a check this class has to
    remember to perform.
    """

    def __init__(self, exchange: str, *, instrument: Optional[InstrumentId] = None,
                trade_flow_window_ms: int = DEFAULT_TRADE_FLOW_WINDOW_MS,
                book_stale_ms: int = DEFAULT_BOOK_STALE_MS,
                mark_stale_ms: int = DEFAULT_MARK_STALE_MS,
                oi_stale_ms: int = DEFAULT_OI_STALE_MS,
                funding_stale_ms: int = DEFAULT_FUNDING_STALE_MS) -> None:
        self.exchange = exchange
        if instrument is not None and instrument.exchange != exchange:
            raise ValueError(f"instrument {instrument.key} does not belong to exchange {exchange!r}")
        self.instrument = instrument
        self._trade_flow_window_ms = trade_flow_window_ms
        self._book_stale_ms = book_stale_ms
        self._mark_stale_ms = mark_stale_ms
        self._oi_stale_ms = oi_stale_ms
        self._funding_stale_ms = funding_stale_ms
        self._trades: list[CanonicalTradeEvent] = []
        self._books: list[CanonicalOrderBookEvent] = []
        self._liquidations: list[CanonicalLiquidationEvent] = []
        self._marks: list[CanonicalMarkPriceEvent] = []
        self._ois: list[CanonicalOIEvent] = []

    def update(self, event) -> None:
        if event.exchange != self.exchange:
            raise ValueError(
                f"MarketStateEngine({self.exchange!r}) received an event from "
                f"{event.exchange!r}. One engine describes one venue; mixing "
                f"venues into one instance is exactly the collapse this "
                f"project's multi-venue rules forbid. Use a separate engine "
                f"per exchange.")
        if self.instrument is not None and event.instrument != self.instrument:
            raise ValueError(
                f"MarketStateEngine bound to {self.instrument.key} received an event for "
                f"{event.instrument.key if event.instrument is not None else 'an unidentified instrument'}. "
                f"Spot, perpetual and other venues' BTCUSDT are different instruments; use one engine each.")
        if isinstance(event, CanonicalTradeEvent):
            self._trades.append(event)
        elif isinstance(event, CanonicalOrderBookEvent):
            if event.book_source in NON_AUTHORITATIVE_BOOK_SOURCES:
                # Defense in depth, not just a documented caller obligation:
                # run_collector.py and run_bybit_collector.py already filter
                # depth10-style partial snapshots before this point, but a
                # future third caller might not remember to. A partial
                # snapshot never reflects the true best bid/ask beyond its
                # own shallow window, so silently accepting one here would
                # let book_engine.LocalBook's own hard-won invariant
                # (NON_AUTHORITATIVE_BOOK_SOURCES) be bypassed one layer up.
                return
            self._books.append(event)
        elif isinstance(event, CanonicalLiquidationEvent):
            self._liquidations.append(event)
        elif isinstance(event, CanonicalMarkPriceEvent):
            self._marks.append(event)
        elif isinstance(event, CanonicalOIEvent):
            self._ois.append(event)
        # An event type this engine does not model is silently ignored here,
        # not an error: V0 deliberately does not consume every canonical
        # event type (see docs/MARKET_STATE_V0.md, "Non-goals").

    @staticmethod
    def _available_at(events: list, observation_ts: int) -> list:
        """Every event with local_receive_ts <= observation_ts, oldest first.

        This is the single causal gate: nothing below this call is allowed
        to read exchange_event_ts to decide availability.
        """
        return sorted(
            (e for e in events if e.local_receive_ts <= observation_ts),
            key=lambda e: e.local_receive_ts)

    def snapshot(self, observation_ts: int) -> MarketState:
        return MarketState(
            exchange=self.exchange, instrument=self.instrument, observation_ts=observation_ts,
            price=self._price_state(observation_ts),
            book=self._book_state(observation_ts),
            trade_flow=self._trade_flow_state(observation_ts),
            liquidation=self._liquidation_state(observation_ts),
            derivatives=self._derivatives_state(observation_ts),
        )

    # -- per-dimension pure computations ------------------------------

    def _price_state(self, observation_ts: int) -> PriceState:
        trades = self._available_at(self._trades, observation_ts)
        marks = self._available_at(self._marks, observation_ts)
        last_trade = trades[-1] if trades else None
        last_mark = marks[-1] if marks else None
        mark_stale = True
        mark_price = index_price = mark_ts = None
        if last_mark is not None:
            mark_ts = last_mark.local_receive_ts
            mark_stale = (observation_ts - mark_ts) > self._mark_stale_ms
            mark_price, index_price = last_mark.mark_price, last_mark.index_price
        price_vs_mark = None
        if last_trade is not None and mark_price is not None and not mark_stale:
            price_vs_mark = last_trade.price - mark_price
        return PriceState(
            last_trade_price=last_trade.price if last_trade else None,
            last_trade_ts=last_trade.local_receive_ts if last_trade else None,
            mark_price=mark_price, index_price=index_price, mark_ts=mark_ts,
            mark_stale=mark_stale, price_vs_mark=price_vs_mark,
        )

    def _book_state(self, observation_ts: int) -> BookState:
        books = self._available_at(self._books, observation_ts)
        if not books:
            return BookState()
        latest = books[-1]
        book_ts = latest.local_receive_ts
        stale = (observation_ts - book_ts) > self._book_stale_ms
        if not latest.bids or not latest.asks:
            return BookState(book_ts=book_ts, book_available=True, book_stale=stale)
        best_bid_price, best_bid_qty = latest.bids[0]
        best_ask_price, best_ask_qty = latest.asks[0]
        best_bid, best_ask = float(best_bid_price), float(best_ask_price)
        bid_qty, ask_qty = float(best_bid_qty), float(best_ask_qty)
        mid = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
        spread_bps = (spread / mid * 10_000) if mid else None
        denom = bid_qty + ask_qty
        imbalance = ((bid_qty - ask_qty) / denom) if denom else None  # zero-denominator: None, never 0/0
        return BookState(
            best_bid=best_bid, best_ask=best_ask, mid=mid, spread=spread,
            spread_bps=spread_bps, book_imbalance=imbalance, book_ts=book_ts,
            book_available=True, book_stale=stale,
        )

    def _trade_flow_state(self, observation_ts: int) -> TradeFlowState:
        trades = self._available_at(self._trades, observation_ts)
        if not trades:
            return TradeFlowState(window_ms=self._trade_flow_window_ms)
        buy = sell = 0.0
        for t in trades:
            side = (t.side or "").lower()
            if side in ("buy", "b"):
                buy += t.quantity
            elif side in ("sell", "s"):
                sell += t.quantity
            # An unrecognised/absent side contributes to trade_count only,
            # never silently guessed into buy or sell.
        window_start = observation_ts - self._trade_flow_window_ms
        windowed = [t for t in trades if t.local_receive_ts > window_start]
        w_buy = sum(t.quantity for t in windowed if (t.side or "").lower() in ("buy", "b"))
        w_sell = sum(t.quantity for t in windowed if (t.side or "").lower() in ("sell", "s"))
        return TradeFlowState(
            cumulative_buy_volume=buy, cumulative_sell_volume=sell, cvd=buy - sell,
            trade_count=len(trades), window_ms=self._trade_flow_window_ms,
            window_buy_volume=w_buy, window_sell_volume=w_sell,
            window_net_volume=w_buy - w_sell, window_trade_count=len(windowed),
            last_trade_ts=trades[-1].local_receive_ts, trades_observed=True,
        )

    def _liquidation_state(self, observation_ts: int) -> LiquidationState:
        liqs = self._available_at(self._liquidations, observation_ts)
        if not liqs:
            return LiquidationState()
        buy_qty = sum(l.quantity or 0.0 for l in liqs if (l.side or "").lower() in ("buy", "b"))
        sell_qty = sum(l.quantity or 0.0 for l in liqs if (l.side or "").lower() in ("sell", "s"))
        return LiquidationState(
            liquidation_count=len(liqs), buy_side_quantity=buy_qty,
            sell_side_quantity=sell_qty, last_liquidation_ts=liqs[-1].local_receive_ts,
            liquidations_observed=True,
        )

    def _derivatives_state(self, observation_ts: int) -> DerivativesState:
        ois = self._available_at(self._ois, observation_ts)
        marks = self._available_at(self._marks, observation_ts)
        oi_val = oi_ts = oi_change = None
        oi_unit = OIUnit.UNKNOWN
        oi_stale = True
        if ois:
            latest_oi = ois[-1]
            oi_val, oi_unit, oi_ts = latest_oi.open_interest, latest_oi.unit, latest_oi.local_receive_ts
            oi_stale = (observation_ts - oi_ts) > self._oi_stale_ms
            if len(ois) >= 2 and ois[-2].unit == oi_unit and ois[-2].open_interest is not None and oi_val is not None:
                # Same engine, same venue, same unit by construction -- the
                # one case canonical.py's OIUnit contract calls safe even
                # when the unit itself is UNKNOWN.
                oi_change = oi_val - ois[-2].open_interest
        funding_rate = funding_ts = funding_age = None
        funding_stale = True
        if marks:
            latest_mark = marks[-1]
            if latest_mark.funding_rate is not None:
                funding_rate = latest_mark.funding_rate
                funding_ts = latest_mark.local_receive_ts
                funding_age = observation_ts - funding_ts
                funding_stale = funding_age > self._funding_stale_ms
        return DerivativesState(
            open_interest=oi_val, oi_unit=oi_unit, oi_ts=oi_ts, oi_change=oi_change,
            oi_stale=oi_stale, funding_rate=funding_rate, funding_ts=funding_ts,
            funding_age_ms=funding_age, funding_stale=funding_stale,
        )
