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

from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
from typing import Optional

from .book_engine import NON_AUTHORITATIVE_BOOK_SOURCES
from .instrument import InstrumentId
from .numeric import dec, to_float
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

#: The only ``book_source`` / ``quality_state`` pair that may describe an
#: authoritative reconstructed book. ``run_collector.py`` and ``replay.py``
#: already gate on ``== "DIFF_DEPTH_RECONSTRUCTED"`` (an allowlist); the
#: engine used to gate on a denylist (``NON_AUTHORITATIVE_BOOK_SOURCES``),
#: which fails open for any source it has never heard of.
AUTHORITATIVE_BOOK_SOURCE = "DIFF_DEPTH_RECONSTRUCTED"
VALID_QUALITY_STATE = "VALID"

#: Canonical field -> every spelling an event may use in ``carried_forward``.
#: Adapters record *venue-native* names there (Bybit: ``"fundingRate"``), while
#: the canonical attribute is ``funding_rate``; either spelling means "this
#: value was NOT observed by this event". Fail closed: any hit counts.
_CARRIED_ALIASES = {
    "mark_price": ("mark_price", "markPrice"),
    "index_price": ("index_price", "indexPrice"),
    "funding_rate": ("funding_rate", "fundingRate"),
    "open_interest": ("open_interest", "openInterest"),
}


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _seq_hint(event) -> int:
    """A venue-intrinsic ordering hint used ONLY to break receive-time ties.

    Never used to decide availability (that is ``local_receive_ts`` alone), so
    it cannot pull a future event into an earlier snapshot. ``-1`` when the
    event carries no usable sequence.
    """
    if isinstance(event, CanonicalOrderBookEvent):
        v = event.update_id
    elif isinstance(event, CanonicalTradeEvent):
        v = event.venue_sequence
        if v is None and isinstance(event.trade_id, str) and event.trade_id.isdigit():
            v = int(event.trade_id)
    else:
        v = None
    return v if _is_int(v) else -1


class _OrderedEvents:
    """Events held in a deterministic TOTAL order, maintained at insertion.

    Order: ``local_receive_ts``, then the venue sequence hint, then -- only
    among events still tied on both -- ``repr`` of the event. The resulting
    order is a function of the *set* of events, never of the order ``add()``
    was called in, so a snapshot cannot depend on call order. Without this,
    events sharing a receive millisecond (routine; see
    docs/P0_11_TIMESTAMP_RESOLUTION.md, "ties stay ties") were resolved by
    insertion order, so which event was "latest" -- and the digest -- changed
    with the order ``update()`` was called in.

    Keeping the list ordered at insert time also lets ``upto()`` take the
    causal cut with a binary search instead of filtering and sorting the
    whole history on every snapshot.
    """
    __slots__ = ("keys", "events")

    def __init__(self) -> None:
        self.keys: list[tuple[int, int]] = []
        self.events: list = []

    def add(self, event) -> None:
        key = (event.local_receive_ts, _seq_hint(event))
        if not self.keys or key > self.keys[-1]:
            # Strictly newer than everything held (the normal live case):
            # append, no search. Same position the general path would pick.
            self.keys.append(key)
            self.events.append(event)
            return
        lo, hi = bisect_left(self.keys, key), bisect_right(self.keys, key)
        # Tied on (receive time, sequence): order by repr (binary search over
        # the tie run only, so repr is evaluated O(log run) times, and only
        # when a tie actually exists).
        pos = lo if lo == hi else bisect_right(self.events, repr(event), lo, hi, key=repr)
        self.keys.insert(pos, key)
        self.events.insert(pos, event)

    def upto(self, observation_ts: int) -> list:
        """Every event with ``local_receive_ts <= observation_ts``, oldest first.

        The single causal gate: availability is decided by ``local_receive_ts``
        alone, never ``exchange_event_ts``.
        """
        return self.events[:bisect_right(self.keys, (observation_ts, float("inf")))]


@dataclass(frozen=True)
class PriceState:
    last_trade_price: Optional[float] = None
    last_trade_ts: Optional[int] = None
    mark_price: Optional[float] = None
    index_price: Optional[float] = None
    #: ``local_receive_ts`` of the event that actually OBSERVED ``mark_price``
    #: (never of a later event that merely carried it forward).
    mark_ts: Optional[int] = None
    mark_stale: bool = True          # True until proven fresh -- never defaults to "fresh"
    price_vs_mark: Optional[float] = None   # last_trade_price - mark_price, only when both exist
    #: Same contract as ``mark_ts``/``mark_stale`` but for ``index_price``, which
    #: is observed on its own cadence (OKX ``index-tickers``) and previously had
    #: no freshness signal at all. Uses the mark staleness threshold.
    index_ts: Optional[int] = None
    index_stale: bool = True


@dataclass(frozen=True)
class BookState:
    """From the authoritative reconstructed book only.

    ``book_available`` is True only when the latest causally-available book
    event passed every check in ``MarketStateEngine._book_untrusted_reason``
    (authoritative source, ``quality_state == "VALID"``, both sides present,
    positive finite prices/quantities, strictly ordered levels, uncrossed).
    A latest event that fails any check is an explicit *barrier*:
    ``book_available=False``, ``book_stale=True``, ``book_untrusted=True`` and
    no prices -- never an earlier book carried across it, never a fabricated
    one. ``book_untrusted`` separates "the producer told us the book is not
    trustworthy" from "no book was ever observed" (both unavailable).
    A ``depth10``-style partial snapshot is not evidence about the book at
    all and is dropped at ``update()`` (see
    ``book_engine.NON_AUTHORITATIVE_BOOK_SOURCES``).
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
    book_untrusted: bool = False


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
    #: Trades whose side was absent/unrecognised. They are counted in
    #: ``trade_count`` but contribute to neither buy nor sell volume, so
    #: ``cvd`` is a lower-information figure whenever this is non-zero.
    unknown_side_trade_count: int = 0


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
    #: Liquidations that reported no quantity. They are counted in
    #: ``liquidation_count`` but add nothing to the side sums, which are then
    #: lower bounds -- missing is never silently zero.
    unquantified_liquidation_count: int = 0


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
            self.price.index_ts, self.price.index_stale,
            self.book.best_bid, self.book.best_ask, self.book.mid,
            self.book.spread, self.book.spread_bps, self.book.book_imbalance,
            self.book.book_ts, self.book.book_available, self.book.book_stale,
            self.book.book_untrusted,
            self.trade_flow.cumulative_buy_volume, self.trade_flow.cumulative_sell_volume,
            self.trade_flow.cvd, self.trade_flow.trade_count,
            self.trade_flow.window_buy_volume, self.trade_flow.window_sell_volume,
            self.trade_flow.window_net_volume, self.trade_flow.window_trade_count,
            self.trade_flow.trades_observed, self.trade_flow.unknown_side_trade_count,
            self.trade_flow.window_ms, self.trade_flow.last_trade_ts,
            self.liquidation.liquidation_count, self.liquidation.buy_side_quantity,
            self.liquidation.sell_side_quantity, self.liquidation.liquidations_observed,
            self.liquidation.unquantified_liquidation_count,
            self.liquidation.last_liquidation_ts,
            self.derivatives.open_interest, self.derivatives.oi_unit.value,
            self.derivatives.oi_change, self.derivatives.oi_stale,
            self.derivatives.funding_rate, self.derivatives.funding_age_ms,
            self.derivatives.funding_stale,
            self.derivatives.oi_ts, self.derivatives.funding_ts,
        )
        return hashlib.sha256(repr(parts).encode()).hexdigest()


class MarketStateEngine:
    """Accumulates canonical events for one venue; ``snapshot()`` is pure.

    Events are stored append-only, grouped by type, and ordered lazily inside
    ``snapshot()`` by a deterministic total order (see ``_order_events``)
    rather than at insert time -- ``update()`` never needs to know what order
    events will later be queried in, which is what makes "a late event cannot
    rewrite an earlier snapshot" true by construction rather than by a check
    this class has to remember to perform.
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
        # A non-positive window silently reports "0 trades in window" for a
        # market that has trades, and a non-positive threshold silently marks
        # everything stale/fresh: refuse the misconfiguration instead.
        for name, value in (("trade_flow_window_ms", trade_flow_window_ms),
                            ("book_stale_ms", book_stale_ms), ("mark_stale_ms", mark_stale_ms),
                            ("oi_stale_ms", oi_stale_ms), ("funding_stale_ms", funding_stale_ms)):
            if not _is_int(value) or value <= 0:
                raise ValueError(f"{name} must be a positive int, got {value!r}")
        self.instrument = instrument
        self._trade_flow_window_ms = trade_flow_window_ms
        self._book_stale_ms = book_stale_ms
        self._mark_stale_ms = mark_stale_ms
        self._oi_stale_ms = oi_stale_ms
        self._funding_stale_ms = funding_stale_ms
        self._trades = _OrderedEvents()
        self._books = _OrderedEvents()
        self._liquidations = _OrderedEvents()
        self._marks = _OrderedEvents()
        self._ois = _OrderedEvents()

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
        if not _is_int(getattr(event, "local_receive_ts", None)):
            # Refuse at the door. A non-int receive stamp would otherwise be
            # stored happily and then raise TypeError inside every later
            # snapshot() call, poisoning the engine permanently.
            raise ValueError(
                f"{type(event).__name__}.local_receive_ts must be an int, got "
                f"{getattr(event, 'local_receive_ts', None)!r}; availability is decided "
                f"by this field alone.")
        if isinstance(event, CanonicalTradeEvent):
            self._trades.add(event)
        elif isinstance(event, CanonicalOrderBookEvent):
            if event.book_source in NON_AUTHORITATIVE_BOOK_SOURCES:
                # A partial-depth push is not evidence about the reconstructed
                # book (LocalBook.apply drops it the same way): it neither
                # contributes a book nor invalidates one.
                return
            # Everything else is stored, including events that will NOT be
            # trusted: an untrusted event must still be able to supersede an
            # earlier good book (see _book_state), otherwise a producer-declared
            # RECOVERING/GAP would leave the previous book looking current.
            self._books.add(event)
        elif isinstance(event, CanonicalLiquidationEvent):
            self._liquidations.add(event)
        elif isinstance(event, CanonicalMarkPriceEvent):
            self._marks.add(event)
        elif isinstance(event, CanonicalOIEvent):
            self._ois.add(event)
        # An event type this engine does not model is silently ignored here,
        # not an error: V0 deliberately does not consume every canonical
        # event type (see docs/MARKET_STATE_V0.md, "Non-goals").

    def snapshot(self, observation_ts: int) -> MarketState:
        # The causal cut is taken once per event list and shared by every
        # dimension that reads it.
        trades = self._trades.upto(observation_ts)
        marks = self._marks.upto(observation_ts)
        books = self._books.upto(observation_ts)
        liqs = self._liquidations.upto(observation_ts)
        ois = self._ois.upto(observation_ts)
        return MarketState(
            exchange=self.exchange, instrument=self.instrument, observation_ts=observation_ts,
            price=self._price_state(observation_ts, trades, marks),
            book=self._book_state(observation_ts, books),
            trade_flow=self._trade_flow_state(observation_ts, trades),
            liquidation=self._liquidation_state(liqs),
            derivatives=self._derivatives_state(observation_ts, ois, marks),
        )

    # -- per-field observation selection --------------------------------

    @staticmethod
    def _last_observed(events: list, field_name: str, count: int) -> list:
        """The newest ``count`` events (newest first) that genuinely OBSERVED ``field_name``.

        An event whose value is ``None``, or which lists the field in
        ``carried_forward``, did not observe it: its ``local_receive_ts`` says
        when a message arrived, not when this value was last true. Counting it
        made a funding rate last seen hours ago look fresh on every Bybit
        ticker delta, and made OKX's channel-pure events (mark / index /
        funding arrive separately) erase each other's fields.
        """
        aliases = _CARRIED_ALIASES[field_name]
        found = []
        for e in reversed(events):
            if getattr(e, field_name) is None or any(e.is_carried_forward(a) for a in aliases):
                continue
            found.append(e)
            if len(found) == count:
                break
        return found

    # -- per-dimension pure computations ------------------------------

    def _price_state(self, observation_ts: int, trades: list, marks: list) -> PriceState:
        last_trade = trades[-1] if trades else None
        mark_obs = self._last_observed(marks, "mark_price", 1)
        index_obs = self._last_observed(marks, "index_price", 1)
        mark_price = mark_ts = index_price = index_ts = None
        mark_stale = index_stale = True
        if mark_obs:
            ev = mark_obs[0]
            mark_ts, mark_price = ev.local_receive_ts, to_float(ev.mark_price)
            mark_stale = (observation_ts - mark_ts) > self._mark_stale_ms
        if index_obs:
            ev = index_obs[0]
            index_ts, index_price = ev.local_receive_ts, to_float(ev.index_price)
            index_stale = (observation_ts - index_ts) > self._mark_stale_ms
        price_vs_mark = None
        if last_trade is not None and mark_price is not None and not mark_stale:
            price_vs_mark = to_float(last_trade.price) - mark_price
        return PriceState(
            last_trade_price=to_float(last_trade.price) if last_trade else None,
            last_trade_ts=last_trade.local_receive_ts if last_trade else None,
            mark_price=mark_price, index_price=index_price, mark_ts=mark_ts,
            mark_stale=mark_stale, price_vs_mark=price_vs_mark,
            index_ts=index_ts, index_stale=index_stale,
        )

    @staticmethod
    def _book_untrusted_reason(event: CanonicalOrderBookEvent) -> Optional[str]:
        """Why ``event`` may not be presented as an authoritative book (None = trusted).

        Fail closed. Everything here is provable from the event alone: it
        catches non-VALID producer state, partial/one-sided fragments, delete
        markers (``qty == 0`` never survives reconstruction), unsorted levels
        (``bids[0]`` would not be the best bid) and crossed/locked books.

        What it CANNOT prove: that a well-formed, two-sided, ordered, VALID
        event is a *reconstructed full book* rather than a raw one-level
        diff -- the two are field-for-field identical today (adapters stamp
        raw diffs ``quality_state="VALID"`` and ``book_source=
        "DIFF_DEPTH_RECONSTRUCTED"`` by default). Closing that needs a
        provenance field stamped by ``LocalBook``; see docs/MARKET_STATE_V0.md.
        """
        if event.book_source != AUTHORITATIVE_BOOK_SOURCE:
            return f"book_source={event.book_source!r}"
        if event.quality_state != VALID_QUALITY_STATE:
            return f"quality_state={event.quality_state!r}"
        if not event.bids or not event.asks:
            return "empty_side"
        try:
            bids = [(dec(p), dec(q)) for p, q in event.bids]
            asks = [(dec(p), dec(q)) for p, q in event.asks]
        except (TypeError, ValueError, ArithmeticError):
            return "malformed_level"
        for levels in (bids, asks):
            if any(p is None or q is None or p <= 0 or q <= 0 for p, q in levels):
                return "non_positive_or_missing_level"
        if any(bids[i][0] <= bids[i + 1][0] for i in range(len(bids) - 1)):
            return "bids_not_strictly_descending"
        if any(asks[i][0] >= asks[i + 1][0] for i in range(len(asks) - 1)):
            return "asks_not_strictly_ascending"
        if bids[0][0] >= asks[0][0]:
            return "crossed_or_locked"
        return None

    def _book_state(self, observation_ts: int, books: list) -> BookState:
        if not books:
            return BookState()
        latest = books[-1]
        if self._book_untrusted_reason(latest) is not None:
            # Barrier: the newest thing the producer told us about the book
            # is not a trustworthy book, so no older book may stand in for it.
            return BookState(book_available=False, book_stale=True, book_untrusted=True)
        book_ts = latest.local_receive_ts
        stale = (observation_ts - book_ts) > self._book_stale_ms
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

    @staticmethod
    def _side(raw_side) -> Optional[str]:
        side = (raw_side or "").lower() if isinstance(raw_side, str) or raw_side is None else ""
        if side in ("buy", "b"):
            return "buy"
        if side in ("sell", "s"):
            return "sell"
        return None  # absent/unrecognised: never guessed into a direction

    def _trade_flow_state(self, observation_ts: int, trades: list) -> TradeFlowState:
        if not trades:
            return TradeFlowState(window_ms=self._trade_flow_window_ms)
        # Window is (observation_ts - window_ms, observation_ts]: the lower
        # bound is EXCLUDED, matching docs/WINDOWED_TRADE_FLOW_OBSERVATION.md.
        window_start = observation_ts - self._trade_flow_window_ms
        buy = sell = w_buy = w_sell = 0.0
        unknown = windowed = 0
        for t in trades:
            side = self._side(t.side)
            in_window = t.local_receive_ts > window_start
            windowed += in_window
            if side is None:
                unknown += 1
                continue
            qty = float(t.quantity)
            if side == "buy":
                buy += qty
                w_buy += qty if in_window else 0.0
            else:
                sell += qty
                w_sell += qty if in_window else 0.0
        return TradeFlowState(
            cumulative_buy_volume=buy, cumulative_sell_volume=sell, cvd=buy - sell,
            trade_count=len(trades), window_ms=self._trade_flow_window_ms,
            window_buy_volume=w_buy, window_sell_volume=w_sell,
            window_net_volume=w_buy - w_sell, window_trade_count=windowed,
            last_trade_ts=trades[-1].local_receive_ts, trades_observed=True,
            unknown_side_trade_count=unknown,
        )

    def _liquidation_state(self, liqs: list) -> LiquidationState:
        if not liqs:
            return LiquidationState()
        buy_qty = sell_qty = 0.0
        unquantified = 0
        for l in liqs:
            if l.quantity is None:
                unquantified += 1
                continue
            side = self._side(l.side)
            if side == "buy":
                buy_qty += float(l.quantity)
            elif side == "sell":
                sell_qty += float(l.quantity)
        return LiquidationState(
            liquidation_count=len(liqs), buy_side_quantity=buy_qty,
            sell_side_quantity=sell_qty, last_liquidation_ts=liqs[-1].local_receive_ts,
            liquidations_observed=True, unquantified_liquidation_count=unquantified,
        )

    def _derivatives_state(self, observation_ts: int, ois: list, marks: list) -> DerivativesState:
        oi_val = oi_ts = oi_change = None
        oi_unit = OIUnit.UNKNOWN
        oi_stale = True
        oi_obs = self._last_observed(ois, "open_interest", 2)   # newest first
        if oi_obs:
            latest_oi = oi_obs[0]
            oi_val, oi_unit, oi_ts = to_float(latest_oi.open_interest), latest_oi.unit, latest_oi.local_receive_ts
            oi_stale = (observation_ts - oi_ts) > self._oi_stale_ms
            if len(oi_obs) >= 2:
                prev = oi_obs[1]
                # Between two genuine observations only (a carried-forward
                # repeat would report a spurious change of 0), and only when
                # they are provably the same physical quantity: same unit AND
                # same instrument AND same market type. Same-exchange alone is
                # not enough -- an unbound engine accepts several instruments.
                if (prev.unit == latest_oi.unit and prev.instrument == latest_oi.instrument
                        and prev.market_type == latest_oi.market_type):
                    oi_change = oi_val - to_float(prev.open_interest)
        funding_rate = funding_ts = funding_age = None
        funding_stale = True
        funding_obs = self._last_observed(marks, "funding_rate", 1)
        if funding_obs:
            latest = funding_obs[0]
            funding_rate = to_float(latest.funding_rate)
            funding_ts = latest.local_receive_ts
            funding_age = observation_ts - funding_ts
            funding_stale = funding_age > self._funding_stale_ms
        return DerivativesState(
            open_interest=oi_val, oi_unit=oi_unit, oi_ts=oi_ts, oi_change=oi_change,
            oi_stale=oi_stale, funding_rate=funding_rate, funding_ts=funding_ts,
            funding_age_ms=funding_age, funding_stale=funding_stale,
        )
