from __future__ import annotations
import time
from typing import Optional
from .base import ExchangeAdapter, UnhandledReason
from ..canonical import CanonicalOrderBookEvent
from ..sequence import OKXSequenceComparator
class OKXAdapter(ExchangeAdapter):
    venue="OKX"
    # D11: declared but not implemented by normalize(). Named here so the
    # gap is explicit and testable rather than hidden behind an early return.
    unimplemented_channels=frozenset({"trades","mark-price","index-tickers","open-interest","funding-rate","liquidation-orders"})
    channel_event_types={"books":("CanonicalOrderBookEvent",),"trades":("CanonicalTradeEvent",),"mark-price":("CanonicalMarkPriceEvent",),"index-tickers":("CanonicalMarkPriceEvent",),"open-interest":("CanonicalOIEvent",),"funding-rate":("CanonicalMarkPriceEvent",),"liquidation-orders":("CanonicalLiquidationEvent",)}
    sequence_comparator=OKXSequenceComparator()
    def connect(self): return None
    def subscribe_message(self,streams): return {"op":"subscribe","args":[{"channel":s,"instId":"BTC-USDT-SWAP"} for s in streams]}
    def route_message(self,raw): return raw.get("arg",{}).get("channel")
    def normalize(self,raw,*,local_receive_ts:Optional[int]=None):
        now=int(time.time()*1000) if local_receive_ts is None else local_receive_ts
        channel=self.route_message(raw)
        if channel!="books":
            if channel is None:
                reason=UnhandledReason.CONTROL_FRAME if raw.get("event") else UnhandledReason.NO_ROUTE
                return self.unhandled(reason, raw, detail=str(raw.get("event")) if raw.get("event") else None, local_receive_ts=now)
            reason=(UnhandledReason.CHANNEL_NOT_IMPLEMENTED if channel in self.unimplemented_channels
                    else UnhandledReason.NO_ROUTE)
            return self.unhandled(reason, raw, channel=channel, local_receive_ts=now)
        if not raw.get("data"):
            return self.unhandled(UnhandledReason.EMPTY_DATA, raw, channel=channel, local_receive_ts=now)
        return [CanonicalOrderBookEvent("OKX","orderbook",int(d["ts"]),None,now,bids=tuple((float(x[0]),float(x[1])) for x in d.get("bids",[])),asks=tuple((float(x[0]),float(x[1])) for x in d.get("asks",[])),update_id=d.get("seqId"),previous_update_id=d.get("prevSeqId"),is_snapshot=d.get("prevSeqId")==-1) for d in raw.get("data",[])]
