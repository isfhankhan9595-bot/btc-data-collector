"""The single Binance open-interest normalizer used by both live and replay.

Before this module, live OI ingestion (`run_collector._poll_openinterest`)
called `compute_openinterest_features()`, which parsed the REST body
directly into a derived-feature dict and used **wall-clock time** for both
`timestamp` and `local_timestamp` -- discarding the request/response
timestamps raw capture already had. `ReplayEngine` had no Binance-OI path
at all: `ReplaySource.from_records` keeps only rows whose REST `purpose` is
`"orderbook_snapshot"`, so every recorded OI observation (`purpose ==
"open_interest"`) was silently excluded from replay's frame stream before
the engine ever saw it. Live OI and replayed OI were not the same pipeline,
and replay could not reproduce OI at all (G2).

This module is that one pipeline. Both call sites -- the live poll loop and
:class:`~collector.collector.replay.ReplayEngine` -- parse the identical
recorded/live REST body through :func:`normalize_binance_oi`, producing a
:class:`~collector.collector.canonical.CanonicalOIEvent`. There is no
second, replay-specific interpretation of the response.

Causality
---------

The event's `local_receive_ts` is the RESPONSE's receive timestamp, not the
request timestamp and not the exchange-reported `time` field. A slow
response is available to the collector (and therefore to replay) only once
it has actually arrived -- using the exchange timestamp alone would let a
response that took 3 seconds to return claim availability 3 seconds earlier
than it was actually known, which is exactly the kind of latency-erasing
lookahead the project's causal-availability rule forbids.

Unit
----

Binance USD-M's `/fapi/v1/openInterest` response does not state the
physical unit of `openInterest` in a way this project has verified against
current official documentation. Per the project's OI-unit contract
(`OIUnit`), an unstated unit is `OIUnit.UNKNOWN` -- never guessed as
`CONTRACTS` or `BASE_COIN` to satisfy the type. `assert_comparable_oi()`
already refuses to compare an `UNKNOWN`-unit event against another venue;
this module does not weaken that guard.
"""
from __future__ import annotations

import json
from typing import Optional

from .canonical import CanonicalOIEvent, OISource, OIUnit
from .instrument import MARKET_LINEAR_PERPETUAL, resolve_instrument

__all__ = ["normalize_binance_oi", "BinanceOIParseError"]


class BinanceOIParseError(ValueError):
    """The REST body could not be turned into a CanonicalOIEvent.

    Raised rather than returning ``None`` so a malformed response is a
    distinguishable, loggable failure -- the same discipline the adapter
    layer's ``UnhandledReason`` applies to unroutable websocket frames.
    """


def normalize_binance_oi(
    body: str,
    *,
    response_receive_ts: int,
    local_process_ts: Optional[int] = None,
    symbol: Optional[str] = None,
) -> CanonicalOIEvent:
    """Parse one Binance `/fapi/v1/openInterest` response body.

    ``response_receive_ts`` becomes the event's ``local_receive_ts`` --
    the causal availability point both live and replay must agree on.
    Raises :class:`BinanceOIParseError` on anything unparseable, missing,
    or non-positive; it never fabricates a value.
    """
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, TypeError) as exc:
        raise BinanceOIParseError(f"invalid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise BinanceOIParseError(f"expected a JSON object, got {type(data).__name__}")

    raw_oi = data.get("openInterest")
    if raw_oi is None:
        raise BinanceOIParseError("response has no 'openInterest' field")
    try:
        open_interest = float(raw_oi)
    except (TypeError, ValueError) as exc:
        raise BinanceOIParseError(f"non-numeric openInterest: {raw_oi!r}") from exc
    if open_interest <= 0 or open_interest != open_interest:  # NaN check
        raise BinanceOIParseError(f"non-positive or NaN openInterest: {open_interest!r}")

    # The response's own 'symbol' echo, checked against what was actually
    # requested -- never trusted blindly and never silently accepted when it
    # disagrees. Absent (older/alternate response shapes) is not the same as
    # wrong: only an explicit mismatch is a contradiction.
    response_symbol = data.get("symbol")
    if symbol is not None and response_symbol is not None and response_symbol != symbol:
        raise BinanceOIParseError(
            f"response symbol {response_symbol!r} contradicts requested symbol {symbol!r}")

    instrument = None
    if symbol is not None:
        # None (not raised) for an unregistered symbol: an unsupported
        # instrument is unidentified, not an error -- this poller has never
        # supported anything but the configured BTCUSDT triple in practice,
        # but the identity is resolved generically rather than hardcoded.
        instrument = resolve_instrument("BINANCE", MARKET_LINEAR_PERPETUAL, symbol)

    exchange_event_ts: Optional[int] = None
    raw_time = data.get("time")
    if raw_time is not None:
        try:
            exchange_event_ts = int(raw_time)
        except (TypeError, ValueError):
            # A malformed exchange timestamp does not invalidate a
            # structurally valid OI reading; it is simply not claimed.
            exchange_event_ts = None

    return CanonicalOIEvent(
        exchange="BINANCE",
        stream="openinterest",
        exchange_event_ts=exchange_event_ts,
        exchange_transaction_ts=None,
        local_receive_ts=response_receive_ts,
        local_process_ts=local_process_ts,
        open_interest=open_interest,
        source=OISource.REST_POLL,
        # Not verified against current official documentation: kept UNKNOWN,
        # never guessed. See module docstring and docs/BINANCE_OI.md.
        unit=OIUnit.UNKNOWN,
        instrument=instrument,
    )
