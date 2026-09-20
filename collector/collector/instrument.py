"""First-class instrument identity.

Three things are kept apart on purpose:

* **IDENTITY** -- *which* instrument: this module (:class:`InstrumentId`).
* **METADATA** -- economics of the instrument (tick size, contract value,
  margin currency). Deliberately absent: it changes over time and belongs in a
  separate, versioned source, never inside a key.
* **OBSERVATION** -- a market event about the instrument (a trade, a book
  update). Carries an identity; is not part of it.

An identity is ``(exchange, market_type, instrument, native_symbol)``, all four
part of equality and hashing:

``exchange``       canonical venue, e.g. ``BINANCE`` (not a storage namespace:
                   Binance Spot is ``BINANCE`` + ``spot``, never ``BINANCE_SPOT``).
``market_type``    the repository's existing vocabulary: ``spot`` or
                   ``linear_perpetual``.
``instrument``     canonical ``BASE-QUOTE`` identifier, e.g. ``BTC-USDT``,
                   shared across venues so cross-venue grouping is explicit.
``native_symbol``  the venue's own symbol, preserved verbatim and case-sensitive
                   (``BTCUSDT``, ``BTC-USDT-SWAP``).

Construction is strict: nothing is case-folded or repaired, because a silent
fix is how ``btcusdt`` and ``BTCUSDT`` become two instruments (or worse, one).
There is no UNKNOWN identity: an event whose instrument is not known carries
``None``, and readers must treat that as "unidentified", never as a default.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

__all__ = [
    "InstrumentId", "InstrumentIdError", "MARKET_SPOT", "MARKET_LINEAR_PERPETUAL",
    "KNOWN_MARKET_TYPES", "BINANCE_SPOT_BTCUSDT", "BINANCE_USDM_BTCUSDT",
    "BYBIT_LINEAR_BTCUSDT", "OKX_SWAP_BTCUSDT", "SUPPORTED_INSTRUMENTS",
    "resolve_instrument", "resolve_raw_record", "resolve_canonical_instrument_key",
]

MARKET_SPOT = "spot"
MARKET_LINEAR_PERPETUAL = "linear_perpetual"
#: The repository's existing market-type vocabulary; extending it is a
#: deliberate, reviewed change to this set, not a free-form string.
KNOWN_MARKET_TYPES = frozenset({MARKET_SPOT, MARKET_LINEAR_PERPETUAL})

_EXCHANGE = re.compile(r"^[A-Z][A-Z0-9]*$")
_INSTRUMENT = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")
_NATIVE = re.compile(r"^[A-Za-z0-9._-]+$")


class InstrumentIdError(ValueError):
    """An instrument identity that is malformed or contradicts its context."""


@dataclass(frozen=True, order=True)
class InstrumentId:
    exchange: str
    market_type: str
    instrument: str
    native_symbol: str

    def __post_init__(self) -> None:
        for name in ("exchange", "market_type", "instrument", "native_symbol"):
            if not isinstance(getattr(self, name), str):
                raise InstrumentIdError(f"{name} must be a str, got {getattr(self, name)!r}")
        if not _EXCHANGE.match(self.exchange):
            raise InstrumentIdError(
                f"exchange {self.exchange!r} must be an upper-case venue such as 'BINANCE' "
                f"(storage namespaces like 'BINANCE_SPOT' are not exchanges)")
        if self.market_type not in KNOWN_MARKET_TYPES:
            raise InstrumentIdError(
                f"market_type {self.market_type!r} is not in {sorted(KNOWN_MARKET_TYPES)}")
        if not _INSTRUMENT.match(self.instrument):
            raise InstrumentIdError(
                f"instrument {self.instrument!r} must be an upper-case BASE-QUOTE id such as 'BTC-USDT'")
        if not _NATIVE.match(self.native_symbol):
            raise InstrumentIdError(
                f"native_symbol {self.native_symbol!r} must be the venue's own non-empty symbol, verbatim")

    @property
    def key(self) -> str:
        """Deterministic, reversible string form: ``EXCHANGE|market_type|BASE-QUOTE|native``."""
        return "|".join((self.exchange, self.market_type, self.instrument, self.native_symbol))

    @classmethod
    def from_key(cls, key: str) -> "InstrumentId":
        parts = key.split("|")
        if len(parts) != 4:
            raise InstrumentIdError(f"not an instrument key: {key!r}")
        return cls(*parts)

    def to_dict(self) -> dict[str, str]:
        return {"exchange": self.exchange, "market_type": self.market_type,
                "instrument": self.instrument, "native_symbol": self.native_symbol}

    @classmethod
    def from_dict(cls, data: dict) -> "InstrumentId":
        missing = {"exchange", "market_type", "instrument", "native_symbol"} - set(data)
        if missing:
            raise InstrumentIdError(f"instrument dict missing {sorted(missing)}")
        return cls(data["exchange"], data["market_type"], data["instrument"], data["native_symbol"])


def resolve_canonical_instrument_key(row: dict, *, expected: InstrumentId) -> Optional[InstrumentId]:
    """Resolve a canonical Parquet row's ``instrument_key`` column.

    ``expected`` is the identity the row's own stream is architecturally
    scoped to (e.g. ``BYBIT_LINEAR_BTCUSDT`` for every row a Bybit runner
    writes) -- this function checks the persisted value agrees with it,
    rather than trusting either side alone.

    Four states, deliberately not collapsed into two:

    * column absent entirely (a legacy row, written before this column
      existed) -> ``None``. Missing is not malformed.
    * column present but explicitly ``null`` -> ``None``. Same resolved
      value as the legacy case (both genuinely have no persisted identity),
      but reached without raising -- an explicit null is not corruption.
    * column present with a value that parses and matches ``expected`` ->
      the parsed :class:`InstrumentId`.
    * column present with a value that fails to parse, or parses but
      disagrees with ``expected`` -> raises :class:`InstrumentIdError`.
      Never silently coerced to ``None``: a corrupted or contradictory
      identity is a data-integrity fault, not an absent one, and collapsing
      the two would let a Binance-Spot row silently pass as a USD-M row (or
      similar) instead of failing loudly.
    """
    if "instrument_key" not in row:
        return None
    raw = row["instrument_key"]
    if raw is None:
        return None
    parsed = InstrumentId.from_key(raw)
    if parsed != expected:
        raise InstrumentIdError(
            f"instrument_key {raw!r} contradicts this stream's own identity {expected.key!r}")
    return parsed


BINANCE_SPOT_BTCUSDT = InstrumentId("BINANCE", MARKET_SPOT, "BTC-USDT", "BTCUSDT")
BINANCE_USDM_BTCUSDT = InstrumentId("BINANCE", MARKET_LINEAR_PERPETUAL, "BTC-USDT", "BTCUSDT")
BYBIT_LINEAR_BTCUSDT = InstrumentId("BYBIT", MARKET_LINEAR_PERPETUAL, "BTC-USDT", "BTCUSDT")
OKX_SWAP_BTCUSDT = InstrumentId("OKX", MARKET_LINEAR_PERPETUAL, "BTC-USDT", "BTC-USDT-SWAP")

SUPPORTED_INSTRUMENTS: tuple[InstrumentId, ...] = (
    BINANCE_SPOT_BTCUSDT, BINANCE_USDM_BTCUSDT, BYBIT_LINEAR_BTCUSDT, OKX_SWAP_BTCUSDT)

_BY_TRIPLE = {(i.exchange, i.market_type, i.native_symbol): i for i in SUPPORTED_INSTRUMENTS}

#: Raw-record ``venue`` values (which double as storage namespaces) -> the
#: canonical exchange, plus the market type that venue value *requires*.
_RAW_VENUE = {
    "BINANCE": ("BINANCE", None),
    "BINANCE_SPOT": ("BINANCE", MARKET_SPOT),
    "BYBIT": ("BYBIT", None),
    "OKX": ("OKX", None),
}


def resolve_instrument(exchange: str, market_type: str, native_symbol: str) -> Optional[InstrumentId]:
    """The registered identity for exactly this triple, else ``None``.

    Exact match only (native symbols are case-sensitive). ``None`` means
    "not a supported instrument"; it is never coerced into a near match.
    """
    return _BY_TRIPLE.get((exchange, market_type, native_symbol))


def resolve_raw_record(venue: Optional[str], market_type: Optional[str],
                       symbol: Optional[str]) -> Optional[InstrumentId]:
    """Rebuild an identity from a stored raw-wire / raw-REST row's columns.

    Returns ``None`` for legacy rows that lack a column, for an unregistered
    combination, and for a contradictory one (venue ``BINANCE_SPOT`` with a
    perpetual ``market_type`` must not resolve to the Binance perpetual).
    """
    if not venue or not market_type or not symbol:
        return None
    mapped = _RAW_VENUE.get(venue)
    if mapped is None:
        return None
    exchange, required_market = mapped
    if required_market is not None and market_type != required_market:
        return None
    return resolve_instrument(exchange, market_type, symbol)
