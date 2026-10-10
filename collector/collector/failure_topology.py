"""F5 failure topology: which storage failure means what.

Two boundaries (frozen design, see docs/F5_FATAL_STORAGE_TOPOLOGY.md):

BOUNDARY 1 -- RAW EVIDENCE (irrecoverable)
    ``raw_wire`` / ``raw_rest`` writer failure  => TERMINATE the process.

BOUNDARY 2 -- DERIVED OUTPUT (rebuildable from preserved raw evidence)
    trades / orderbook / markprice / OI-canonical / liquidation writer failure
    => ISOLATE that route; raw capture and every other route keep running.

QUALITY CHANNEL
    ``quality_events`` writer failure => DEGRADE the channel (telemetry), never
    terminate while the raw-evidence boundary is intact.

Anything else that arrives as a typed ``FatalStorageError`` and is not in the
table below is TERMINATE: default-deny. A new writer must be added here on
purpose; it can never be silently treated as "probably derived".

This module is a leaf (imports only ``storage_errors``): no I/O, no logging,
no event loop, so the mapping is trivially unit-testable.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, Mapping, Optional, Tuple

from .storage_errors import FatalStorageError, WriterFailureSnapshot

__all__ = [
    "VERDICT_TERMINATE", "VERDICT_ISOLATE", "VERDICT_DEGRADE_QUALITY",
    "STORAGE_STREAM_VERDICTS", "FORCED_TERMINAL_ORIGINS", "EXIT_FATAL_STORAGE",
    "FailureRecord", "classify_stream", "classify_fatal",
]

VERDICT_TERMINATE = "terminate"
VERDICT_ISOLATE = "isolate_route"
VERDICT_DEGRADE_QUALITY = "degrade_quality"

#: Process exit status of a terminal (raw-evidence) storage failure, shared by
#: every entry point that implements the termination contract. Any non-zero
#: value makes systemd's ``Restart=always`` treat the exit as a failure.
EXIT_FATAL_STORAGE = 70

#: ``FatalStorageError.stream`` (the writer's storage stream) -> (verdict, route).
#: ``route`` names the application route to isolate; ``None`` when no route.
STORAGE_STREAM_VERDICTS: Dict[str, Tuple[str, Optional[str]]] = {
    # Raw evidence: the historical frame / REST response cannot be causally
    # reconstructed later (a REST answer reflects the exchange at request time).
    "raw_wire": (VERDICT_TERMINATE, None),
    "raw_rest": (VERDICT_TERMINATE, None),
    # Derived, rebuildable from raw_wire / raw_rest.
    "orderbook": (VERDICT_ISOLATE, "orderbook"),
    "binance_orderbook_raw": (VERDICT_ISOLATE, "orderbook"),
    "trades": (VERDICT_ISOLATE, "trades"),            # canonical trades + dedup (DedupStateError)
    "binance_trades_raw": (VERDICT_ISOLATE, "trades"),  # dedup recovery anchor
    "markprice": (VERDICT_ISOLATE, "markprice"),
    "openinterest": (VERDICT_ISOLATE, "openinterest"),  # canonical OI; raw_rest keeps capturing
    "liquidation": (VERDICT_ISOLATE, "liquidation"),
    # Telemetry.
    "quality_events": (VERDICT_DEGRADE_QUALITY, None),
}

#: Origins at which the application is AT a raw-evidence boundary by
#: construction (the raw-frame callback, the raw REST capture call). A typed
#: fatal there is terminal whatever stream name it carries: the boundary itself
#: is what failed. (An unrelated stream name must not talk the app into
#: continuing past a raw-evidence failure.)
#:
#: ``run_task`` is the supervisor's last line of defence: a typed fatal that
#: escaped the application task all the way to the supervisor was never
#: classified by anyone, so it is terminal by default-deny.
FORCED_TERMINAL_ORIGINS = frozenset({"raw_frame", "raw_rest", "run_task"})


def classify_stream(stream: Optional[str], *,
                    table: Optional[Mapping[str, Tuple[str, Optional[str]]]] = None
                    ) -> Tuple[str, Optional[str]]:
    """``(verdict, route)`` for a storage stream. Unknown / missing => TERMINATE.

    ``table`` lets an entry point supply the stream -> verdict mapping for ITS
    OWN writers (venue-prefixed stream names differ per runner: ``spot_trades``,
    ``bybit_orderbook``, ``okx_trades`` ...). It replaces the default table, it
    is never merged with it, and a stream missing from it is still TERMINATE:
    default-deny holds for every table."""
    entries = STORAGE_STREAM_VERDICTS if table is None else table
    if isinstance(stream, str):
        entry = entries.get(stream)
        if entry is not None:
            return entry
    return (VERDICT_TERMINATE, None)


def classify_fatal(exc: FatalStorageError, *, origin: Optional[str] = None,
                   table: Optional[Mapping[str, Tuple[str, Optional[str]]]] = None
                   ) -> Tuple[str, Optional[str]]:
    """Classify a typed fatal. Only ``FatalStorageError`` is accepted: this is
    the single place that turns an exception into a verdict, and it refuses to
    classify anything by ``RuntimeError``-ness, message text or class name."""
    if not isinstance(exc, FatalStorageError):
        raise TypeError(f"classify_fatal requires a FatalStorageError, got {type(exc).__name__}")
    if origin in FORCED_TERMINAL_ORIGINS:
        return (VERDICT_TERMINATE, None)
    return classify_stream(exc.stream, table=table)


@dataclass(frozen=True)
class FailureRecord:
    """One latched failure: enough to say what failed, where, how far it got,
    and when it was first seen. Immutable: a record is evidence, not state."""

    stream: Optional[str]
    component: Optional[str]
    stage: Optional[str]
    durability: Optional[str]
    verdict: str
    route: Optional[str]
    origin: str
    first_observed_ts: int
    error: str

    @property
    def key(self) -> str:
        """Identity of the failed component (one record per component)."""
        return f"{self.component}:{self.stream}"

    def as_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_exception(cls, exc: FatalStorageError, *, origin: str, now_ms: int,
                       table: Optional[Mapping[str, Tuple[str, Optional[str]]]] = None) -> "FailureRecord":
        verdict, route = classify_fatal(exc, origin=origin, table=table)
        cause = exc.__cause__
        error = f"{type(exc).__name__}: {exc}"
        if cause is not None:
            error += f" (cause {type(cause).__name__}: {cause})"
        return cls(stream=exc.stream, component=exc.component, stage=exc.stage,
                   durability=exc.durability, verdict=verdict, route=route,
                   origin=origin, first_observed_ts=now_ms, error=error)

    @classmethod
    def from_snapshot(cls, snapshot: WriterFailureSnapshot, *, origin: str, now_ms: int) -> "FailureRecord":
        verdict, route = classify_stream(snapshot.stream)
        return cls(stream=snapshot.stream, component=snapshot.component, stage=snapshot.stage,
                   durability=snapshot.durability, verdict=verdict, route=route,
                   origin=origin, first_observed_ts=now_ms, error=snapshot.error)
