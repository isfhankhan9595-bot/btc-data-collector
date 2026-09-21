"""The causal observation layer: the missing consumer of ``causally_align()``.

Architecture decision (recorded here, not just in a commit message)
---------------------------------------------------------------------

Before this module, ``pipeline/cross_exchange_alignment.py`` had no production
or replay caller: ``causally_align()`` existed, was heavily tested (Phase F,
mutation-tested), and was reachable only from its own test file. Two existing
consumers were considered and rejected as the place to put this:

* **``MarketStateEngine``** (``collector/collector/market_state.py``) is
  explicitly documented as venue-local and explicitly does *not* consume
  cross-exchange alignment, by deliberate design predating this phase. It
  reimplements the same ``local_receive_ts <= observation_ts`` rule in its
  own ``_available_at()`` -- correctly, and *not* a duplicate worth
  consolidating, because its data model (flat per-venue event lists feeding
  richly-derived per-venue state: best bid/ask, trade flow, ...) is a
  different shape from ``causally_align()``'s identity-keyed cross-venue
  latest-event model. Bolting cross-exchange multiplexing onto a venue-local
  engine was exactly the "silently collapse two venues into one" risk that
  engine's own docstring names and refuses to take.
* **Extending ``causally_align()`` itself** into a stateful API was rejected:
  it is pure (no clock, no network, no global state) and that purity is what
  makes it trivially testable and mutation-tested. Every consumer need here
  -- accumulate events over time, then ask for a snapshot -- is satisfiable
  by a thin wrapper *around* the pure function, so the function stays pure
  and the statefulness lives in exactly one place: this module.

So: ``causally_align()`` remains the pure primitive. This module is the
smallest correct consumer, added because none existed.

What this module is
--------------------

:class:`CausalObservationBuilder` accumulates canonical events (from live
ingestion or from ``ReplayEngine.non_book_events``) and produces a
:class:`ObservationSnapshot` -- a self-describing wrapper around
``causally_align()``'s result, carrying the ``observation_ts`` and
``staleness_ms`` the snapshot was built with, since the bare
``dict[AlignmentKey, AlignedObservation]`` result on its own does not
record what question was asked.

Nothing here reinterprets ``AlignedObservation``: AVAILABLE, STALE and
NEVER_OBSERVED pass through untouched; the underlying event's
``quality_state`` is never inspected or collapsed. This module adds a
place to accumulate events over time and a self-describing return type,
nothing else.

Reference-implementation-first (deliberate, not an oversight)
---------------------------------------------------------------

``snapshot()`` re-runs ``causally_align()`` over the *entire* accumulated
event buffer on every call: O(N) per call, not streaming/incremental. This
is the correct-first choice the project's own engineering principle calls
for ("establish a correct reference implementation, then benchmark, then
optimize only if required"). A streaming/indexed variant is future work if
profiling shows this matters; introducing one now, unmeasured, would be
exactly the premature optimization the phase's own instructions warn
against, and would need its own differential tests against this reference
before it could be trusted.

Known limitation, stated rather than hidden
----------------------------------------------

Order-book state is **not** covered by :meth:`CausalObservationBuilder.from_replay_result`.
``ReplayResult.book_updates`` holds ``BookUpdate`` records -- a derived,
post-reconstruction shape (best bid/ask strings, quality state, recovery
generation) -- not ``CanonicalOrderBookEvent`` objects with the
``exchange``/``market_type``/``instrument``/``stream``/``local_receive_ts``
attributes ``causally_align()`` requires. Retrofitting that would mean
either changing what ``ReplayEngine`` records (a real change to a
heavily-tested, unrelated module) or reconstructing synthetic canonical
events from ``BookUpdate`` (inventing data). Neither is "the smallest
correct architecture" this phase asks for. Order-book state remains
queryable directly via ``ReplayResult.book_updates`` or a live
``LocalBook`` instance; only non-order-book canonical events (trades,
funding/mark-price, open interest, liquidations) flow through this layer
for now. This is a scope boundary, not a silent gap: it is asserted by
``test_book_state_is_explicitly_out_of_scope`` in the test file.

Purity and mutation
--------------------

:meth:`CausalObservationBuilder.snapshot` never mutates an event it was
given, and never mutates its own buffer as a side effect of taking a
snapshot (only ``append``/``extend`` grow the buffer). Calling ``update()``
with a late-arriving event cannot change the result of an earlier
``snapshot(observation_ts)`` call for the same reason ``causally_align()``
itself has this property: eligibility is a pure function of the event's own
``local_receive_ts`` against the requested ``observation_ts``, not of
insertion order.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from .cross_exchange_alignment import AlignedObservation, AlignmentKey, causally_align

__all__ = ["ObservationSnapshot", "CausalObservationBuilder"]


@dataclass(frozen=True)
class ObservationSnapshot:
    """A self-describing ``causally_align()`` result.

    Carries the question (``observation_ts``, ``staleness_ms``) alongside
    the answer, since the bare alignment dict does not record what it was
    computed for. ``observations`` is exactly ``causally_align()``'s return
    value -- nothing here reshapes or reinterprets it.
    """

    observation_ts: int
    staleness_ms: int
    observations: dict[AlignmentKey, AlignedObservation]

    def get(self, key: AlignmentKey) -> Optional[AlignedObservation]:
        """Look up one key. Returns ``None`` for a key nobody asked for and
        nothing observed -- distinct from :class:`AlignedObservation` with
        status ``NEVER_OBSERVED``, which means the key *was* in
        ``expected_keys`` when the snapshot was built. Absence here is not a
        claim about availability at all; it means the question was never
        posed for this key.
        """
        return self.observations.get(key)

    def __len__(self) -> int:
        return len(self.observations)

    def __iter__(self):
        return iter(self.observations.items())


class CausalObservationBuilder:
    """Accumulates canonical events; produces :class:`ObservationSnapshot`.

    The only stateful/mutable component in this module, by design --
    ``causally_align()`` itself stays pure. Works identically whether fed
    from a live event stream (``append`` as events arrive) or from a
    replayed one (``extend`` with a full recorded sequence, or
    :meth:`from_replay_result`); the causal-availability contract makes no
    distinction between the two, which is exactly the live/replay parity
    this project requires.
    """

    def __init__(self, events: Optional[Iterable[Any]] = None) -> None:
        self._events: list[Any] = list(events) if events is not None else []

    def append(self, event: Any) -> None:
        self._events.append(event)

    def extend(self, events: Iterable[Any]) -> None:
        self._events.extend(events)

    def __len__(self) -> int:
        return len(self._events)

    def snapshot(
        self,
        observation_ts: int,
        *,
        staleness_ms: int,
        expected_keys: Optional[Iterable[AlignmentKey]] = None,
    ) -> ObservationSnapshot:
        """Build a snapshot from every event accumulated so far.

        Delegates entirely to ``causally_align()``; see that function's
        docstring for the exact availability, identity, staleness and
        tie-breaking rules. This method adds no additional logic beyond
        wrapping the result with the question that produced it.
        """
        observations = causally_align(
            self._events, observation_ts,
            staleness_ms=staleness_ms, expected_keys=expected_keys,
        )
        return ObservationSnapshot(observation_ts, staleness_ms, observations)

    @classmethod
    def from_replay_result(cls, replay_result: Any) -> "CausalObservationBuilder":
        """Seed a builder from a completed replay.

        Uses ``replay_result.non_book_events`` only -- see this module's
        docstring, "Known limitation", for why order-book state is not
        included. This is the replay half of live/replay parity: the exact
        same event objects a live collector would have produced (trades,
        funding, OI, liquidations) flow through the identical
        ``causally_align()`` call either way.
        """
        return cls(replay_result.non_book_events)
