"""OKX v5 public-channel adapter.

D11 status
----------
All seven declared channel names (``books``, ``trades``, ``trades-all``,
``mark-price``, ``index-tickers``, ``funding-rate``, ``open-interest``,
``liquidation-orders`` -- eight names, seven of them wire channels plus the
one duplicate the schema doc explicitly does not conflate) are implemented.
Field-level schemas are sourced from ``docs/OKX_D11_CHANNEL_SCHEMAS.md``,
itself sourced from OKX's official ``docs-v5`` documentation and, for
``liquidation-orders``, one confirmed real captured production frame. That
document also records five semantic questions official documentation does
not resolve (trades aggregation vs trades-all, seqId presence on
trades-all, index-tickers instId convention, OI canonical unit, and
liquidation ``ccy`` field behaviour for BTC-USDT-SWAP specifically) --
this module does not guess answers to them; see the per-channel comments
below for exactly how each is handled without assuming a resolution.

Raw-vs-canonical split
-----------------------
Canonical events here intentionally do not carry every field OKX pushes
(e.g. ``trades``' unconfirmed ``count`` aggregation field, or `funding-rate`'s
``impactValue``/``maxFundingRate``/``minFundingRate`` band). This matches
every other adapter in this codebase (Binance, Bybit): the raw frame,
captured losslessly by ``raw_capture``/``okx_capture`` before this module
ever sees it, is the source of truth for fields with no canonical slot --
canonical events are a derived, intentionally narrower view, not a second
copy of the wire. Fields that *were* given new canonical slots in
``canonical.py`` (the funding current/next/settled distinction, OI's three
units, liquidation's ``bkLoss``/``ccy``/``posSide``) are the ones the task
explicitly called out as risking silent conflation if merged into an
existing field.
"""
from __future__ import annotations
import time
from typing import Any, Optional
from .base import ExchangeAdapter, UnhandledReason
from ..canonical import (
    CanonicalLiquidationEvent,
    CanonicalMarkPriceEvent,
    CanonicalOIEvent,
    CanonicalOrderBookEvent,
    CanonicalTradeEvent,
    OISource,
    OIUnit,
)
from ..sequence import OKXSequenceComparator


def _num(value: Any) -> Optional[float]:
    """Best-effort numeric-string -> float. Empty/None/unparseable -> None.

    Never raises: several OKX fields (``nextFundingRate``, ``impactValue``,
    liquidation ``ccy``-adjacent numerics) are documented as sometimes an
    empty string rather than absent. An empty string is "not currently
    published", not zero -- returning ``None`` preserves that; returning
    ``0.0`` would fabricate a value OKX never sent.
    """
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


from ..instrument import MARKET_LINEAR_PERPETUAL, resolve_instrument

class OKXAdapter(ExchangeAdapter):
    venue = "OKX"
    channel_event_types = {
        "books": ("CanonicalOrderBookEvent",),
        "trades": ("CanonicalTradeEvent",),
        "trades-all": ("CanonicalTradeEvent",),
        "mark-price": ("CanonicalMarkPriceEvent",),
        "index-tickers": ("CanonicalMarkPriceEvent",),
        "open-interest": ("CanonicalOIEvent",),
        "funding-rate": ("CanonicalMarkPriceEvent",),
        "liquidation-orders": ("CanonicalLiquidationEvent",),
    }
    sequence_comparator = OKXSequenceComparator()

    def connect(self):
        return None

    def subscribe_message(self, streams):
        """Build the documented subscribe request, per-channel argument shape.

        Not every OKX channel subscribes by ``instId``:
        ``liquidation-orders`` is documented as scoped by ``instType``
        (confirmed against two independent sources in
        docs/OKX_D11_CHANNEL_SCHEMAS.md, §F) -- an ``instId`` argument there
        would not be the documented shape. Every other channel here uses
        the instrument's own ``instId``, except ``index-tickers``, whose
        instId is the *index's* pair (open question #3 in the schema doc);
        that is exposed as ``index_inst_id`` below, isolated as
        configuration rather than silently assumed equal to the swap's
        instId, per the task's explicit instruction not to pretend an
        unverified identifier is verified.
        """
        args = []
        for stream in streams:
            if stream == "liquidation-orders":
                args.append({"channel": stream, "instType": "SWAP"})
            elif stream == "index-tickers":
                args.append({"channel": stream, "instId": self.index_inst_id})
            else:
                args.append({"channel": stream, "instId": self.inst_id})
        return {"op": "subscribe", "args": args}

    def __init__(self, inst_id: str = "BTC-USDT-SWAP", index_inst_id: str = "BTC-USDT") -> None:
        super().__init__()
        self.inst_id = inst_id
        #: ``None`` if ``inst_id`` is not a registered instrument: unidentified,
        #: never a guessed identity.
        self.instrument = resolve_instrument("OKX", MARKET_LINEAR_PERPETUAL, inst_id)
        #: See subscribe_message docstring: unverified against a live
        #: connection (open question #3). Overridable, not hardcoded deep
        #: in normalize(), so a wrong guess is one constructor argument
        #: away from fixing, not a code change.
        self.index_inst_id = index_inst_id

    def route_message(self, raw):
        return raw.get("arg", {}).get("channel")

    def _instrument_scoped(self, event) -> bool:
        # `index-tickers` is keyed by the index pair (BTC-USDT), not the swap:
        # OKX open question #3, so it is not stamped with the swap's identity.
        if event.stream == "index-tickers":
            return False
        # `liquidation-orders` is instType-scoped: one stream, many instruments.
        # Only rows whose own instId is this adapter's instrument are identified.
        if event.stream == "liquidation-orders":
            return getattr(event, "inst_id", None) == self.inst_id
        return True

    def normalize(self, raw, *, local_receive_ts: Optional[int] = None):
        now = int(time.time() * 1000) if local_receive_ts is None else local_receive_ts
        channel = self.route_message(raw)

        if channel is None:
            reason = UnhandledReason.CONTROL_FRAME if raw.get("event") else UnhandledReason.NO_ROUTE
            return self.unhandled(reason, raw, detail=str(raw.get("event")) if raw.get("event") else None,
                                  local_receive_ts=now)
        if channel not in self.channel_event_types:
            return self.unhandled(UnhandledReason.NO_ROUTE, raw, channel=channel, local_receive_ts=now)
        if not raw.get("data"):
            return self.unhandled(UnhandledReason.EMPTY_DATA, raw, channel=channel, local_receive_ts=now)

        data = raw["data"]
        handler = {
            "books": self._parse_books,
            "trades": self._parse_trades,
            "trades-all": self._parse_trades_all,
            "mark-price": self._parse_mark_price,
            "index-tickers": self._parse_index_tickers,
            "funding-rate": self._parse_funding_rate,
            "open-interest": self._parse_open_interest,
            "liquidation-orders": self._parse_liquidation_orders,
        }[channel]
        try:
            events = handler(data, now)
        except (KeyError, TypeError, ValueError):
            # A required field was missing or unparseable. Emitting a
            # canonical event with a fabricated 0.0/None price for a trade
            # or liquidation would be strictly worse than dropping the
            # frame observably -- the whole point of D11's caution.
            return self.unhandled(UnhandledReason.MALFORMED_PAYLOAD, raw, channel=channel, local_receive_ts=now)
        if not events:
            return self.unhandled(UnhandledReason.EMPTY_DATA, raw, channel=channel, local_receive_ts=now)
        return events

    # -- per-channel parsers -----------------------------------------------

    def _parse_books(self, data, now):
        return [CanonicalOrderBookEvent(
            "OKX", "orderbook", int(d["ts"]), None, now,
            bids=tuple((float(x[0]), float(x[1])) for x in d.get("bids", [])),
            asks=tuple((float(x[0]), float(x[1])) for x in d.get("asks", [])),
            update_id=d.get("seqId"), previous_update_id=d.get("prevSeqId"),
            is_snapshot=d.get("prevSeqId") == -1,
        ) for d in data]

    def _parse_trades(self, data, now):
        """`trades` channel. seqId (added 2025-07-08) may repeat for
        same-instant updates per OKX's own changelog note -- never treated
        as a gap here; it is stored as-is (venue_sequence), not compared or
        validated for monotonicity. Whether this channel aggregates
        multiple fills per push relative to trades-all is open question #1
        -- unresolved here, not assumed either way; each element of `data`
        becomes exactly one event regardless, since that is true whether or
        not the underlying fill count is >1.
        """
        return [CanonicalTradeEvent(
            "OKX", "trades", _int(d["ts"]), None, now,
            trade_id=d.get("tradeId"), price=float(d["px"]), quantity=float(d["sz"]),
            side=d.get("side"), venue_sequence=_int(d.get("seqId")),
        ) for d in data]

    def _parse_trades_all(self, data, now):
        """`trades-all` ("All trades channel"). Distinct from `trades`
        (open question #1/#2: aggregation and seqId presence are NOT
        assumed to match `trades` -- seqId is read defensively via .get,
        so its absence in real frames simply yields None, not an error, and
        its presence is not assumed to mean the same thing as on `trades`).
        `source` (ELP flag) is unique to this channel among the two and is
        preserved on the canonical event's `source` field.
        """
        return [CanonicalTradeEvent(
            "OKX", "trades-all", _int(d["ts"]), None, now,
            trade_id=d.get("tradeId"), price=float(d["px"]), quantity=float(d["sz"]),
            side=d.get("side"), venue_sequence=_int(d.get("seqId")), source=d.get("source"),
        ) for d in data]

    def _parse_mark_price(self, data, now):
        """`mark-price` carries only markPx -- never populate index_price
        or funding_rate here from another channel's cached value; those
        fields stay None on this event, exactly as `Q7`/liquidation-rule
        style channel-purity requires."""
        return [CanonicalMarkPriceEvent(
            "OKX", "mark-price", _int(d["ts"]), None, now,
            mark_price=float(d["markPx"]),
        ) for d in data]

    def _parse_index_tickers(self, data, now):
        """Index price only -- never confused with mark price (separate
        canonical field, separate channel)."""
        return [CanonicalMarkPriceEvent(
            "OKX", "index-tickers", _int(d["ts"]), None, now,
            index_price=_num(d.get("idxPx")),
        ) for d in data]

    def _parse_funding_rate(self, data, now):
        """Current/next/settled funding are three distinct observations
        (see canonical.py's CanonicalMarkPriceEvent docstring) and are never
        merged. Interval is never computed or assumed here (task rule:
        "without assuming an 8-hour interval") -- fundingTime and
        nextFundingTime are stored as given; any interval calculation is a
        downstream feature-layer concern, not ingestion's. A missing
        nextFundingRate is left None, never inferred."""
        return [CanonicalMarkPriceEvent(
            "OKX", "funding-rate", _int(d["ts"]), None, now,
            funding_rate=_num(d.get("fundingRate")),
            next_funding_time=_int(d.get("nextFundingTime")),
            funding_time=_int(d.get("fundingTime")),
            next_funding_rate=_num(d.get("nextFundingRate")),
            sett_funding_rate=_num(d.get("settFundingRate")),
            sett_state=d.get("settState") or None,
            premium=_num(d.get("premium")),
            interest_rate=_num(d.get("interestRate")),
            max_funding_rate=_num(d.get("maxFundingRate")),
            min_funding_rate=_num(d.get("minFundingRate")),
            formula_type=d.get("formulaType") or None,
            method=d.get("method") or None,
            impact_value=_num(d.get("impactValue")),
        ) for d in data]

    def _parse_open_interest(self, data, now):
        """Canonical `open_interest` = `oi` (contracts) -- see canonical.py
        for why. `oiCcy`/`oiUsd` are preserved alongside, never discarded,
        per the explicit instruction not to mix the three units under one
        unnamed field."""
        return [CanonicalOIEvent(
            "OKX", "open-interest", _int(d["ts"]), None, now,
            open_interest=float(d["oi"]), oi_ccy=_num(d.get("oiCcy")),
            oi_usd=_num(d.get("oiUsd")), source=OISource.WS_PUSH,
            # OKX documents `oi` as "Open interest, in contracts"
            # (docs/OKX_D11_CHANNEL_SCHEMAS.md) and `oiCcy` as base currency.
            unit=OIUnit.CONTRACTS,
        ) for d in data]

    def _parse_liquidation_orders(self, data, now):
        """Subscription is scoped by instType, not instId (confirmed in
        docs/OKX_D11_CHANNEL_SCHEMAS.md §F), so `data` can contain
        liquidations for instruments other than this adapter's own
        `inst_id`. This method does NOT filter by instrument -- per the
        task's instruction, filtering belongs "at the appropriate layer"
        (the runner/storage boundary, which has the config for which
        instId to keep), not silently inside the parser, where a filtered-
        out record would look identical to one that was never received.
        Every outer object's `details[]` becomes one event per detail
        (multiple positions can be liquidated in one push). `ccy` is
        preserved exactly as sent -- including an empty string -- per
        canonical.py's note: it is never coerced to None, since an
        explicit empty string is a different observation from a field
        that was never sent at all (this channel's `ccy` is technically
        optional in the schema, so `.get` still returns None if truly
        absent; only an observed `""` is kept as `""`)."""
        events = []
        for outer in data:
            inst_id = outer.get("instId")
            inst_family = outer.get("instFamily")
            uly = outer.get("uly")
            for detail in outer.get("details", []):
                events.append(CanonicalLiquidationEvent(
                    "OKX", "liquidation-orders", _int(detail.get("ts")), None, now,
                    side=detail.get("side"), price=_num(detail.get("bkPx")),
                    quantity=_num(detail.get("sz")), bk_loss=_num(detail.get("bkLoss")),
                    ccy=detail.get("ccy"), pos_side=detail.get("posSide"),
                    inst_family=inst_family, uly=uly, inst_id=inst_id,
                ))
        return events
