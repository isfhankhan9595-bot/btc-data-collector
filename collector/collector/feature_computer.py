import time

from .numeric import dec
from typing import Dict, Any, Optional

def _required(msg: Dict[str, Any], key: str) -> Any:
    """A venue numeric field that MUST be present. A missing value is a
    malformed message (raises ValueError -> callers return {}), never a
    fabricated 0.0 that would look like a real price/rate/quantity."""
    if key not in msg or msg[key] is None:
        raise ValueError(f"missing required numeric field {key!r}")
    return msg[key]


def compute_orderbook_features(msg: Dict[str, Any]) -> Dict[str, Any]:
    bids = msg.get("b", [])
    asks = msg.get("a", [])

    if len(bids) < 1 or len(asks) < 1:
        return {}

    try:
        # P0-9: parse EXACTLY (Decimal from the venue string, never via
        # float). The evidence arrays below stay Decimal; float64 only ever
        # appears as an explicitly derived value (best_bid, obi, ...).
        bids_price = [dec(b[0]) for b in bids]
        bids_qty = [dec(b[1]) for b in bids]
        asks_price = [dec(a[0]) for a in asks]
        asks_qty = [dec(a[1]) for a in asks]
    except (ValueError, TypeError, IndexError):
        return {}

    # P0-8: NEVER fabricate a missing price level. Truncate to the top 10
    # REAL levels when more are available; never pad past however many are
    # actually present -- a missing level is represented by the array
    # simply being shorter, never by repeating the last real price with a
    # synthetic zero quantity (that made a nonexistent level indistinguishable
    # from a real one at the same price with genuinely zero size). bid_depth/
    # ask_depth make the true observed depth explicit and queryable without
    # relying on callers to notice array length -- see ORDERBOOK_SCHEMA v1.2.
    bids_price = bids_price[:10]
    bids_qty = bids_qty[:10]
    asks_price = asks_price[:10]
    asks_qty = asks_qty[:10]
    bid_depth = len(bids_price)
    ask_depth = len(asks_price)

    # Exact evidence (Decimal) is kept for the persisted arrays; the float
    # mirrors below exist ONLY for the derived analytics that follow
    # (float(Decimal(text)) is identical to float(text), so those values are
    # unchanged from before P0-9).
    evidence_bids_price, evidence_bids_qty = bids_price, bids_qty
    evidence_asks_price, evidence_asks_qty = asks_price, asks_qty
    bids_price = [float(x) for x in evidence_bids_price]
    bids_qty = [float(x) for x in evidence_bids_qty]
    asks_price = [float(x) for x in evidence_asks_price]
    asks_qty = [float(x) for x in evidence_asks_qty]

    best_bid = bids_price[0]
    best_ask = asks_price[0]
    mid_price = (best_bid + best_ask) / 2.0

    bid_qty_0 = bids_qty[0]
    ask_qty_0 = asks_qty[0]

    if bid_qty_0 + ask_qty_0 == 0:
        return {}

    micro_price = (best_bid * ask_qty_0 + best_ask * bid_qty_0) / (bid_qty_0 + ask_qty_0)
    spread = best_ask - best_bid
    if mid_price == 0:
        return {}
    spread_bps = (spread / mid_price) * 10000.0

    total_bid_qty = sum(bids_qty)
    total_ask_qty = sum(asks_qty)

    if total_bid_qty + total_ask_qty == 0:
        return {}
    # Aggregate OBI over however many real levels are actually present (up
    # to the top-10 truncation above) -- semantically valid as a running
    # aggregate, and bid_depth/ask_depth make clear it is not always a
    # full-10-level figure.
    obi = (total_bid_qty - total_ask_qty) / (total_bid_qty + total_ask_qty)

    tbq_1 = bids_qty[0]
    taq_1 = asks_qty[0]
    obi_level_1 = (tbq_1 - taq_1) / (tbq_1 + taq_1) if tbq_1 + taq_1 > 0 else 0.0

    # P0-8: a level-N OBI is only a truthful level-N observation when at
    # least N real levels were actually observed on BOTH sides. Fewer real
    # levels than the metric name claims -> None (missing/invalid), never
    # silently computed over whatever partial depth exists under that name.
    if bid_depth >= 3 and ask_depth >= 3:
        tbq_3 = sum(bids_qty[:3])
        taq_3 = sum(asks_qty[:3])
        obi_level_3 = (tbq_3 - taq_3) / (tbq_3 + taq_3) if tbq_3 + taq_3 > 0 else 0.0
    else:
        obi_level_3 = None

    if bid_depth >= 5 and ask_depth >= 5:
        tbq_5 = sum(bids_qty[:5])
        taq_5 = sum(asks_qty[:5])
        obi_level_5 = (tbq_5 - taq_5) / (tbq_5 + taq_5) if tbq_5 + taq_5 > 0 else 0.0
    else:
        obi_level_5 = None

    timestamp = int(time.time() * 1000)
    exchange_timestamp = msg.get("E", timestamp)

    return {
        "timestamp": timestamp,
        "exchange_timestamp": exchange_timestamp,
        "local_timestamp": timestamp,
        "bids_price": evidence_bids_price,
        "bids_qty": evidence_bids_qty,
        "asks_price": evidence_asks_price,
        "asks_qty": evidence_asks_qty,
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": mid_price,
        "micro_price": micro_price,
        "spread": spread,
        "spread_bps": spread_bps,
        "total_bid_qty": total_bid_qty,
        "total_ask_qty": total_ask_qty,
        "obi": obi,
        "obi_level_1": obi_level_1,
        "obi_level_3": obi_level_3,
        "obi_level_5": obi_level_5
    }

def compute_trades_features(msg: Dict[str, Any]) -> Dict[str, Any]:
    try:
        trade_id = int(msg.get("a", -1))  # exact integer; never via float
        price = dec(_required(msg, "p"))
        quantity = dec(_required(msg, "q"))
        is_buyer_maker = bool(msg.get("m", False))
    except (ValueError, TypeError):
        return {}

    side_sign = -1 if is_buyer_maker else 1
    signed_qty = float(quantity) * side_sign  # derived analytic (float64)

    timestamp = int(time.time() * 1000)
    exchange_timestamp = int(msg.get("T", msg.get("E", timestamp)))

    return {
        "timestamp": timestamp,
        "exchange_timestamp": exchange_timestamp,
        "local_timestamp": timestamp,
        "trade_id": trade_id,
        "price": price,
        "quantity": quantity,
        "is_buyer_maker": is_buyer_maker,
        "side_sign": side_sign,
        "signed_qty": signed_qty
    }

def compute_markprice_features(msg: Dict[str, Any]) -> Dict[str, Any]:
    try:
        mark_price = dec(_required(msg, "p"))
        funding_rate = dec(_required(msg, "r"))
        next_funding_time = int(msg.get("T", 0))
    except (ValueError, TypeError):
        return {}

    funding_rate_bps = float(funding_rate) * 10000.0  # derived analytic
    exchange_timestamp = int(msg.get("E", 0))

    hours_to_funding = (next_funding_time - exchange_timestamp) / 3600000.0

    timestamp = int(time.time() * 1000)

    return {
        "timestamp": timestamp,
        "exchange_timestamp": exchange_timestamp,
        "local_timestamp": timestamp,
        "mark_price": mark_price,
        "funding_rate": funding_rate,
        "next_funding_time": next_funding_time,
        "funding_rate_bps": funding_rate_bps,
        "hours_to_funding": hours_to_funding
    }

def compute_openinterest_features(data: dict) -> dict:
    try:
        oi = dec(_required(data, "openInterest"))
        exchange_ts = int(data.get("time", int(time.time() * 1000)))
    except (ValueError, TypeError):
        return {}
    if oi <= 0:
        return {}
    ts = int(time.time() * 1000)
    return {
        "timestamp": ts,
        "exchange_timestamp": exchange_ts,
        "local_timestamp": ts,
        "open_interest": oi,
    }


def compute_liquidation_features(msg: dict) -> dict:
    try:
        o = msg.get("o", {})
        side_str = o.get("S", "")
        # BUY side in forceOrder = short position liquidated (forced buy)
        side = 1 if side_str == "BUY" else -1
        price = dec(_required(o, "p"))
        quantity = dec(_required(o, "q"))
        ts = int(time.time() * 1000)
        exchange_ts = int(o.get("T", ts))
    except (ValueError, TypeError):
        return {}
    if price <= 0 or quantity <= 0:
        return {}
    return {
        "timestamp": ts,
        "exchange_timestamp": exchange_ts,
        "local_timestamp": ts,
        "side": side,
        "price": price,
        "quantity": quantity,
        "signed_qty": float(quantity) * side,
        "order_status": str(o.get("X", "")),
        "time_in_force": str(o.get("f", "")),
    }
