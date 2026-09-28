"""Exact venue-decimal handling (P0-9).

Exchange prices, quantities, rates and open interest are DECIMAL evidence
transmitted as text. Passing them through binary ``float`` silently changes
what they mean once they exceed ~15 significant digits (and makes distinct
decimals compare equal). This module is the one place that defines the
contract:

* :func:`dec` parses a venue value EXACTLY (string/int/Decimal -> Decimal),
  never via ``float``. Malformed input raises ``ValueError`` -- the same
  exception ``float("abc")`` raised, so existing handlers keep working -- and
  a missing value is ``None``, never a fabricated zero.
* :func:`exact_text` renders a Decimal without exponent notation, preserving
  every digit (including trailing zeros) so ``dec(exact_text(x)) == x`` and
  the text is byte-stable across live, storage and replay.
* :func:`to_float` is the EXPLICIT boundary into approximate arithmetic.
  Derived analytics (imbalance ratios, VWAP, ...) may use float64; exact
  evidence must never be produced from it.

Integer identifiers (trade ids, update ids, timestamps) are not decimals and
must never be routed through here or through ``float``.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Optional


def dec(value: Any) -> Optional[Decimal]:
    """Parse a venue numeric value exactly. ``None`` -> ``None``."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"boolean is not a numeric value: {value!r}")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, int):
        result = Decimal(value)
    elif isinstance(value, float):
        # A JSON *number* (not a string) reached us as a binary float: any
        # precision beyond the shortest round-trip repr was already gone
        # before this code saw it. Use that repr rather than the float's
        # exact binary expansion, which would invent digits the venue never
        # sent. Venue strings (all four supported venues send numerics as
        # strings) never take this branch.
        result = Decimal(repr(value))
    elif isinstance(value, str):
        try:
            result = Decimal(value.strip())
        except InvalidOperation as exc:
            raise ValueError(f"malformed decimal string: {value!r}") from exc
    else:
        raise TypeError(f"unsupported numeric type: {type(value).__name__}")
    if not result.is_finite():
        raise ValueError(f"non-finite numeric value: {value!r}")
    return result


def exact_text(value: Optional[Decimal]) -> Optional[str]:
    """Exponent-free, digit-preserving text of ``value`` (``None`` -> ``None``)."""
    if value is None:
        return None
    return format(value, "f")


def to_float(value: Any) -> Optional[float]:
    """The explicit exact -> approximate boundary for derived analytics."""
    if value is None:
        return None
    return float(value)


EXACT_SUFFIX = "_exact"


def _exact_of(base: Any) -> Any:
    """Exact text for a Decimal (or a non-empty sequence of Decimals).

    Anything that is not exact evidence -- a float, an int that was never a
    venue decimal, ``None`` -- yields ``None``: exactness is never
    (re)constructed from a binary float, because a float has already lost
    the information the companion column exists to preserve.
    """
    if isinstance(base, Decimal):
        return exact_text(base)
    if isinstance(base, (list, tuple)) and base and all(isinstance(x, Decimal) for x in base):
        return [exact_text(x) for x in base]
    return None


def column_value(name: str, record: dict) -> Any:
    """The one exact -> persisted conversion boundary (used by ParquetWriter).

    * ``<field>_exact`` columns: an explicitly supplied value wins; otherwise
      the exact text of ``record[<field>]`` when that is a Decimal (or a list
      of Decimals). Never derived from a float.
    * every other column: ``Decimal`` values (and lists of them) become
      ``float`` -- the documented DERIVED float64 representation of the
      exact value; non-Decimal values pass through untouched.
    """
    value = record.get(name)
    if name.endswith(EXACT_SUFFIX):
        if value is not None:
            return value
        return _exact_of(record.get(name[: -len(EXACT_SUFFIX)]))
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (list, tuple)) and value and any(isinstance(x, Decimal) for x in value):
        return [float(x) if isinstance(x, Decimal) else x for x in value]
    return value
