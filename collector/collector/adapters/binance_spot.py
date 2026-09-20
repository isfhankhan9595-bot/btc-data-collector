"""Binance **Spot** public-channel adapter (P5).

Spot is a distinct API surface from the USD-M futures venue
``BinanceAdapter`` already implements -- separate official documentation
(github.com/binance/binance-spot-api-docs), separate WebSocket base
(``wss://stream.binance.com:9443``), and, critically, different order-book
sequence semantics (see ``sequence.SpotSequenceComparator`` and
``sequence.binance_spot_snapshot_bridge`` for the exact differences and
their official sourcing, verified 2026-09-19).

Identity split
--------------
Canonical events here carry ``exchange="BINANCE"`` (not a separate exchange
name) with ``market_type="spot"`` explicit on every event -- this is the
identity P6's cross-exchange alignment is keyed on
(``(exchange, market_type, instrument_key, stream)``, see
``pipeline/cross_exchange_alignment.py``), so Spot and USD-M futures are
correctly distinguishable there without inventing a second "exchange".
``self.venue = "BINANCE_SPOT"`` below is a *different*, storage/book-engine
-internal key (see ``book_engine.LocalBook`` and
``storage_layout.VENUE_STREAM_PREFIX``) so Spot's sequence comparator,
snapshot bridge, and storage segments can never be shared with USD-M
futures'. These two identities serve different consumers and are not meant
to be the same value -- see canonical.py's CanonicalEvent docstring context
and storage_layout.py's VENUE_STREAM_PREFIX comment.

Scope of this module (P5, this pass)
-------------------------------------
Implements the ``trade`` stream (raw per-execution tape, confirmed by the
official payload's ``t``: "Trade ID" -- not an aggregate) and the
``depth``/``depth@100ms`` diff stream. Deliberately NOT implemented:
``aggTrade`` -- the task's open question ("whether aggTrade is required...
whether trade, aggTrade, or both should be canonicalized") is left open
rather than guessed; ``trade`` alone already gives the raw individual-fill
tape this collector wants, so aggTrade is additive, not a prerequisite.
Live collector wiring (``run_binance_spot_collector.py``) and non-orderbook
replay integration are also not part of this module -- see
docs/EXECUTION_STATUS.md, "P5" for exactly what's done vs. carried forward.

Official sources
-----------------
Trade stream payload and Diff. Depth Stream payload/procedure verified
2026-09-19 against
https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
(mirrored at
https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).
"""
from __future__ import annotations
import time
from decimal import Decimal
from typing import Optional
from .base import ExchangeAdapter, UnhandledReason
from ..canonical import CanonicalOrderBookEvent, CanonicalTradeEvent
from ..sequence import SpotSequenceComparator, binance_spot_snapshot_bridge

MARKET_TYPE_SPOT = "spot"


from ..instrument import BINANCE_SPOT_BTCUSDT

class BinanceSpotAdapter(ExchangeAdapter):
    #: Book-engine/storage-namespace key -- see module docstring. Canonical
    #: events' own `.exchange` field is "BINANCE", set explicitly below, not
    #: derived from this attribute.
    venue = "BINANCE_SPOT"
    instrument = BINANCE_SPOT_BTCUSDT
    channel_event_types = {
        "<symbol>@depth": ("CanonicalOrderBookEvent",),
        "<symbol>@depth@100ms": ("CanonicalOrderBookEvent",),
        "<symbol>@trade": ("CanonicalTradeEvent",),
    }
    sequence_comparator = SpotSequenceComparator()

    def connect(self):
        return None

    def subscribe_message(self, streams):
        return {"method": "SUBSCRIBE", "params": list(streams), "id": 1}

    def route_message(self, raw):
        stream = raw.get("stream", "").lower()
        for token, route in (("@depth", "orderbook"), ("@trade", "trades")):
            if token in stream:
                return route
        return None

    def normalize(self, raw, *, local_receive_ts: Optional[int] = None):
        d = raw.get("data", raw)
        now = int(time.time() * 1000) if local_receive_ts is None else local_receive_ts
        route = self.route_message(raw) or (d.get("e", "") if isinstance(d, dict) else "")

        if route == "orderbook" or (isinstance(d, dict) and d.get("e") == "depthUpdate"):
            try:
                bids = tuple((Decimal(p), Decimal(q)) for p, q in d.get("b", []))
                asks = tuple((Decimal(p), Decimal(q)) for p, q in d.get("a", []))
                update_id = d["u"]
                first_update_id = d["U"]
                if not isinstance(update_id, int) or not isinstance(first_update_id, int):
                    raise TypeError("U/u must be integers")
            except (KeyError, TypeError, ValueError, ArithmeticError):
                return self.unhandled(UnhandledReason.MALFORMED_PAYLOAD, raw, local_receive_ts=now)
            return [CanonicalOrderBookEvent(
                "BINANCE", "spot_orderbook", d.get("E"), None, now,
                market_type=MARKET_TYPE_SPOT,
                bids=bids, asks=asks, update_id=update_id, first_update_id=first_update_id,
                # No `pu` on Spot's depthUpdate (confirmed absent from the
                # official payload, unlike futures) -- continuity is
                # verified via SpotSequenceComparator's U == prev.u+1 check
                # instead, so this is never populated for Spot, not a
                # missing-field bug.
                previous_update_id=None,
                is_snapshot=False, book_source="DIFF_DEPTH_RECONSTRUCTED",
            )]

        if route == "trades" or (isinstance(d, dict) and d.get("e") == "trade"):
            try:
                price = float(d["p"])
                quantity = float(d["q"])
            except (KeyError, TypeError, ValueError):
                return self.unhandled(UnhandledReason.MALFORMED_PAYLOAD, raw, local_receive_ts=now)
            return [CanonicalTradeEvent(
                "BINANCE", "spot_trades", d.get("E"), d.get("T"), now,
                market_type=MARKET_TYPE_SPOT,
                # `t`: official "Trade ID" -- the raw per-execution ID, never
                # the aggregate `a` id aggTrade would carry (see module
                # docstring: trade and aggTrade are never conflated here).
                trade_id=str(d["t"]) if d.get("t") is not None else None,
                price=price, quantity=quantity,
                # "m": "Is the buyer the market maker?" -- identical wording
                # to USD-M futures' same-named field, which BinanceAdapter
                # already maps m=true -> SELL (seller is the aggressor).
                # Reusing that convention here, not inventing a new one, and
                # not assuming without the docs actually saying so: verified
                # against the current official Spot trade-stream payload
                # (module docstring), which uses the same description.
                side="SELL" if d.get("m") else "BUY",
            )]

        if not isinstance(d, dict) or not d:
            return self.unhandled(UnhandledReason.MALFORMED_PAYLOAD, raw, local_receive_ts=now)
        if any(key in raw for key in ("result", "id", "code", "msg")) and "data" not in raw:
            return self.unhandled(UnhandledReason.CONTROL_FRAME, raw, local_receive_ts=now)
        return self.unhandled(
            UnhandledReason.NO_ROUTE, raw, channel=raw.get("stream"),
            detail=str(d.get("e")) if isinstance(d, dict) and d.get("e") else None,
            local_receive_ts=now,
        )

    @staticmethod
    def bridge_accepts(event, last_update_id):
        return binance_spot_snapshot_bridge(event, last_update_id)
