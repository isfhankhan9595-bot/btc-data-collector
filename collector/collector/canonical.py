"""Venue-neutral market-data events; absent venue fields remain ``None``."""
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional
from .instrument import InstrumentId
from decimal import Decimal

class OISource(str, Enum): REST_POLL = "REST_POLL"; WS_PUSH = "WS_PUSH"


class OIUnit(str, Enum):
    """The physical unit of a ``CanonicalOIEvent.open_interest`` value.

    ``UNKNOWN`` is a first-class, *safe* value: it says the venue's documentation
    does not state the unit, so nothing may compare this value with another
    venue's. It is never a synonym for "probably contracts".
    """

    CONTRACTS = "CONTRACTS"
    BASE_COIN = "BASE_COIN"
    QUOTE_USD = "QUOTE_USD"
    UNKNOWN = "UNKNOWN"


class OIUnitError(ValueError):
    """Open-interest values whose units cannot be proven comparable."""

@dataclass(frozen=True)
class CanonicalEvent:
    exchange: str; stream: str; exchange_event_ts: Optional[int]; exchange_transaction_ts: Optional[int]
    local_receive_ts: int; local_process_ts: Optional[int] = None; market_type: str = "linear_perpetual"; quality_state: str = "VALID"
    #: Which instrument this observation is about (see instrument.py). ``None`` means
    #: unidentified -- legacy data, or an event that is not scoped to one registered
    #: instrument -- and must never be read as a default instrument.
    instrument: Optional[InstrumentId] = None

@dataclass(frozen=True)
class CanonicalOrderBookEvent(CanonicalEvent):
    bids: tuple[tuple[Decimal, Decimal], ...] = (); asks: tuple[tuple[Decimal, Decimal], ...] = ()
    update_id: Optional[int] = None; first_update_id: Optional[int] = None; previous_update_id: Optional[int] = None
    sequence: Optional[int] = None; is_snapshot: bool = False; book_source: str = "DIFF_DEPTH_RECONSTRUCTED"

@dataclass(frozen=True)
class CanonicalTradeEvent(CanonicalEvent):
    trade_id: Optional[str] = None; price: float = 0.0; quantity: float = 0.0; side: Optional[str] = None; nq: Optional[float] = None
    venue_sequence: Optional[int] = None; block_trade: Optional[bool] = None; rpi: Optional[bool] = None
    #: Venue-native order-source flag (e.g. OKX's `trades-all.source`:
    #: "0" normal, "1" Enhanced Liquidity Program). No cross-venue meaning
    #: is implied; kept as the raw string so it is never silently discarded,
    #: not remapped onto block_trade/rpi, which mean something else.
    source: Optional[str] = None

@dataclass(frozen=True)
class CanonicalOIEvent(CanonicalEvent):
    #: The venue's primary/native open-interest size, **in the unit named by
    #: ``unit``**. There is no venue-neutral canonical unit: OKX's ``oi`` is
    #: contracts, Bybit's ``openInterest`` is documented only as "size" (unit
    #: not stated in text), so the same field holds different physical
    #: quantities per venue. Never compare across events without
    #: :func:`assert_comparable_oi`; use :func:`base_coin_oi` for a value that
    #: is safe to compare across venues.
    open_interest: Optional[float] = None; source: OISource = OISource.WS_PUSH
    #: OKX pushes three simultaneous OI representations (contracts/coin/USD).
    #: Only one can be the canonical `open_interest`; the other two are kept
    #: here rather than discarded. Both None for venues that push only one
    #: unit (Bybit, Binance).
    oi_ccy: Optional[float] = None; oi_usd: Optional[float] = None
    #: Fields carried forward from earlier messages rather than present in this one.
    carried_forward: tuple[str, ...] = ()
    #: Age in ms of each field at this event, as ``(field, age_ms)``. A field
    #: absent here was never observed; it is not assumed to be zero-age.
    field_age_ms: tuple[tuple[str, int], ...] = ()
    #: Unit of ``open_interest``. Defaults to UNKNOWN so an adapter that does
    #: not state its unit can never be silently treated as comparable.
    unit: OIUnit = OIUnit.UNKNOWN

    def is_carried_forward(self, field: str) -> bool:
        return field in self.carried_forward

    def age_of(self, field: str) -> Optional[int]:
        return dict(self.field_age_ms).get(field)
def assert_comparable_oi(*events: "CanonicalOIEvent") -> OIUnit:
    """Raise :class:`OIUnitError` unless every event's ``open_interest`` is
    provably the same physical quantity; return the shared unit.

    * Different units are never comparable.
    * An UNKNOWN unit is comparable only with itself **within one exchange**
      (one venue's stream is one quantity by construction, so an OI *change*
      over time is safe). Across exchanges UNKNOWN is refused: nobody has
      established that the two quantities match.
    * Two exchanges sharing one KNOWN unit pass. Instrument (linear vs
      inverse, contract size) is not modelled here and remains the caller's
      responsibility.
    """
    if len(events) < 2:
        raise ValueError("assert_comparable_oi needs at least two events")
    units = {event.unit for event in events}
    exchanges = sorted({event.exchange for event in events})
    if len(units) > 1:
        raise OIUnitError(
            f"open interest is in different units {sorted(u.value for u in units)} "
            f"across {exchanges}; refusing to compare")
    (unit,) = units
    if unit is OIUnit.UNKNOWN and len(exchanges) > 1:
        raise OIUnitError(
            f"open-interest unit is UNKNOWN for {exchanges}; refusing to compare "
            f"across exchanges until each venue's unit is documented")
    return unit


def base_coin_oi(event: "CanonicalOIEvent") -> Optional[float]:
    """Open interest in the base coin, or ``None`` if it cannot be proven.

    The only cross-venue-safe accessor today: a venue that pushes a
    base-currency figure alongside its native one (OKX ``oiCcy``) provides it;
    a venue whose native unit is BASE_COIN provides ``open_interest``; anything
    else returns ``None`` rather than a guess.
    """
    if event.unit is OIUnit.BASE_COIN:
        return event.open_interest
    return event.oi_ccy


@dataclass(frozen=True)
class CanonicalMarkPriceEvent(CanonicalEvent):
    mark_price: Optional[float] = None; index_price: Optional[float] = None; funding_rate: Optional[float] = None; next_funding_time: Optional[int] = None
    #: Fields carried forward from earlier messages rather than present in this one.
    carried_forward: tuple[str, ...] = ()
    #: Age in ms of each field at this event, as ``(field, age_ms)``.
    field_age_ms: tuple[tuple[str, int], ...] = ()
    #: OKX funding-rate channel fields with no existing slot above. These are
    #: distinct observations, not restatements of `funding_rate`/
    #: `next_funding_time`: `funding_time` is when the *current* `funding_rate`
    #: settles; `sett_funding_rate`/`sett_state` describe the *last already
    #: settled* rate, a third, separate value. Forcing all three into the two
    #: existing fields would silently conflate current/next/settled periods.
    #: All None for venues that don't push them (Binance, Bybit).
    funding_time: Optional[int] = None
    next_funding_rate: Optional[float] = None
    sett_funding_rate: Optional[float] = None
    sett_state: Optional[str] = None
    premium: Optional[float] = None
    interest_rate: Optional[float] = None
    max_funding_rate: Optional[float] = None
    min_funding_rate: Optional[float] = None
    formula_type: Optional[str] = None
    method: Optional[str] = None
    impact_value: Optional[float] = None

    def is_carried_forward(self, field: str) -> bool:
        return field in self.carried_forward

    def age_of(self, field: str) -> Optional[int]:
        return dict(self.field_age_ms).get(field)
@dataclass(frozen=True)
class CanonicalLiquidationEvent(CanonicalEvent):
    side: Optional[str] = None; price: Optional[float] = None; quantity: Optional[float] = None
    #: OKX liquidation-orders fields with no existing slot. `price` above is
    #: populated from `bkPx` (bankruptcy/execution price) for OKX.
    #: `ccy` is preserved exactly as pushed, including an empty string --
    #: an empty string and "never sent" are not the same observation, so
    #: this is never coerced to None. `pos_side`/`inst_family`/`uly` are
    #: OKX-native attribution fields not present on Binance/Bybit.
    bk_loss: Optional[float] = None
    ccy: Optional[str] = None
    pos_side: Optional[str] = None
    inst_family: Optional[str] = None
    uly: Optional[str] = None
    #: The instrument this liquidation belongs to. Unlike every other OKX
    #: channel here, `liquidation-orders` subscribes by `instType`, not
    #: `instId` -- one push can carry liquidations for instruments other
    #: than this collector's own. Without this field, "filter by
    #: instrument at the appropriate layer" (the task's own instruction)
    #: would have nothing to filter on downstream of the adapter.
    inst_id: Optional[str] = None
