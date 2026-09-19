"""Bybit v5 linear-perpetual adapter.

Ticker staleness (D12)
----------------------

Bybit's ``tickers`` topic sends a full ``snapshot`` and then ``delta``
messages carrying only the fields that changed. The previous implementation
merged each delta into a shared ``_ticker_state`` and then tested the
*merged* dict for the presence of mark/index/funding keys. Because the merge
happened first, that test was true on essentially every message, so each
delta emitted a mark-price event populated with values carried forward from
earlier messages -- indistinguishable, downstream, from freshly observed
ones. A funding rate last seen minutes ago looked exactly like a funding
rate observed now.

Two changes fix that:

* An event is emitted only when **this message** actually carried at least
  one of the relevant fields. A delta about something else no longer
  manufactures a mark-price observation.
* Every emitted event states which of its fields were carried forward and
  how old each one is, via ``carried_forward`` and ``field_age_ms``. A field
  never observed is absent from ``field_age_ms`` rather than being reported
  as zero-age.

A ``snapshot`` replaces the cached state rather than merging into it, which
matches the treatment the orderbook topic already gives ``type`` and is the
documented snapshot/delta model. It is flagged in docs as an assumption
pending the D14-style documentation review.
"""
from __future__ import annotations

import time
from typing import Optional

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
from ..sequence import BybitSequenceComparator

#: Ticker fields this adapter surfaces, grouped by the event they feed.
MARKPRICE_FIELDS = ("markPrice", "indexPrice", "fundingRate", "nextFundingTime")
OI_FIELDS = ("openInterest",)


class BybitAdapter(ExchangeAdapter):
    venue = "BYBIT"
    channel_event_types = {
        "orderbook.{depth}.BTCUSDT": ("CanonicalOrderBookEvent",),
        "publicTrade.BTCUSDT": ("CanonicalTradeEvent",),
        "tickers.BTCUSDT": ("CanonicalMarkPriceEvent", "CanonicalOIEvent"),
        "allLiquidation.BTCUSDT": ("CanonicalLiquidationEvent",),
    }
    sequence_comparator = BybitSequenceComparator()

    def __init__(self):
        super().__init__()
        self._ticker_state: dict = {}
        #: When each ticker field was last actually carried by a message.
        self._ticker_field_ts: dict[str, int] = {}

    def connect(self):
        return None

    def subscribe_message(self, streams):
        return {"op": "subscribe", "args": list(streams)}

    def route_message(self, raw):
        topic = raw.get("topic", "")
        if topic.startswith("orderbook."):
            return "orderbook"
        if topic.startswith("publicTrade."):
            return "trades"
        if topic.startswith("tickers."):
            return "ticker"
        if topic.startswith("allLiquidation."):
            return "liquidation"
        return None

    # -- ticker provenance -------------------------------------------------

    def _observe_ticker(self, delta: dict, ts: Optional[int], is_snapshot: bool) -> set[str]:
        """Merge a ticker message and return the fields it actually carried."""
        if is_snapshot:
            # A snapshot is the authoritative full state; merging would keep
            # fields the venue no longer reports.
            self._ticker_state = {}
            self._ticker_field_ts = {}
        present = {key for key in (*MARKPRICE_FIELDS, *OI_FIELDS) if key in delta}
        self._ticker_state.update(delta)
        if ts is not None:
            for key in present:
                self._ticker_field_ts[key] = ts
        return present

    def _provenance(self, fields, present: set[str], ts: Optional[int]):
        """Return ``(carried_forward, field_age_ms)`` for the given fields."""
        carried = []
        ages = []
        for key in fields:
            if self._ticker_state.get(key) is None:
                continue  # never observed: not reported, not fabricated
            if key not in present:
                carried.append(key)
            seen_at = self._ticker_field_ts.get(key)
            if ts is not None and seen_at is not None:
                ages.append((key, max(ts - seen_at, 0)))
        return tuple(carried), tuple(ages)

    @staticmethod
    def _as_float(value):
        return None if value is None else float(value)

    # -- normalise ---------------------------------------------------------

    def normalize(self, raw, *, local_receive_ts: Optional[int] = None):
        now = int(time.time() * 1000) if local_receive_ts is None else local_receive_ts
        data = raw.get("data", {})
        route = self.route_message(raw)
        ts = raw.get("ts")

        if route == "orderbook":
            return [CanonicalOrderBookEvent(
                "BYBIT", "orderbook", ts, data.get("cts"), now,
                bids=tuple((float(p), float(q)) for p, q in data.get("b", [])),
                asks=tuple((float(p), float(q)) for p, q in data.get("a", [])),
                update_id=data.get("u"), sequence=data.get("seq"),
                is_snapshot=raw.get("type") == "snapshot")]

        if route == "trades":
            return [CanonicalTradeEvent(
                "BYBIT", "trades", ts, x.get("T"), now,
                trade_id=str(x["i"]) if x.get("i") is not None else None,
                price=float(x["p"]), quantity=float(x["v"]), side=x.get("S"),
                venue_sequence=x.get("seq"), block_trade=x.get("BT"),
                rpi=x.get("RPI")) for x in data]

        if route == "ticker":
            is_snapshot = raw.get("type") == "snapshot"
            present = self._observe_ticker(data, ts, is_snapshot)
            if not present:
                # This delta changed nothing this adapter surfaces. Emitting a
                # mark-price event here would republish stale values as fresh.
                return self.unhandled(
                    UnhandledReason.EMPTY_DATA, raw, channel="ticker",
                    detail="ticker_delta_carried_no_surfaced_field",
                    local_receive_ts=now)

            state = self._ticker_state
            events = []
            if present & set(MARKPRICE_FIELDS):
                carried, ages = self._provenance(MARKPRICE_FIELDS, present, ts)
                events.append(CanonicalMarkPriceEvent(
                    "BYBIT", "markprice", ts, None, now,
                    mark_price=self._as_float(state.get("markPrice")),
                    index_price=self._as_float(state.get("indexPrice")),
                    funding_rate=self._as_float(state.get("fundingRate")),
                    next_funding_time=(int(state["nextFundingTime"])
                                       if state.get("nextFundingTime") is not None else None),
                    carried_forward=carried, field_age_ms=ages))
            if present & set(OI_FIELDS):
                carried, ages = self._provenance(OI_FIELDS, present, ts)
                events.append(CanonicalOIEvent(
                    "BYBIT", "openinterest", ts, None, now,
                    open_interest=self._as_float(state.get("openInterest")),
                    source=OISource.WS_PUSH,
                    # Deliberately UNKNOWN. Bybit's ticker field table says only
                    # "Open interest size (both sides)" -- no unit. The example
                    # (openInterestValue == openInterest * markPrice) suggests
                    # base coin, but an example is not documentation, and the
                    # "both sides" counting convention versus `singleOpenInterest`
                    # is unresolved. Do not promote this without a documented
                    # unit and convention.
                    unit=OIUnit.UNKNOWN,
                    carried_forward=carried, field_age_ms=ages))
            return events

        if route == "liquidation":
            return [CanonicalLiquidationEvent(
                "BYBIT", "liquidation", x.get("T", ts), None, now,
                side=x.get("S"), price=float(x["p"]), quantity=float(x["v"]))
                for x in data]

        if raw.get("op") or raw.get("success") is not None:
            return self.unhandled(UnhandledReason.CONTROL_FRAME, raw, local_receive_ts=now)
        return self.unhandled(UnhandledReason.NO_ROUTE, raw,
                              channel=raw.get("topic"), local_receive_ts=now)
