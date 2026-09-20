# Causal cross-exchange alignment

`collector/pipeline/cross_exchange_alignment.py`. A foundation primitive: it has
**no production callers** (verified by repo-wide search) and is deliberately not
wired into `MarketStateEngine` yet.

```python
causally_align(events, observation_ts, *, staleness_ms, expected_keys=None)
    -> dict[(exchange, market_type, stream), AlignedObservation]
```

| Rule | Behaviour |
|---|---|
| Availability | `event.local_receive_ts <= observation_ts` (inclusive). Exchange timestamps never affect eligibility and are preserved verbatim. |
| Identity | `(exchange, market_type, stream)`. Perp and spot of one venue cannot collide. |
| Selection | latest eligible event per key. No nearest-timestamp matching, no interpolation. |
| Statuses | `AVAILABLE` (age <= `staleness_ms`), `STALE` (returned, never dropped), `NEVER_OBSERVED` (only for keys in `expected_keys` with no eligible event *as of* `observation_ts`). No "synchronized"/"current" state exists. |
| Missingness | an unrequested, unobserved key is absent from the result. Missing is not zero, stale, or synthetic. |
| Quality | the original event is returned untouched; `AVAILABLE` says nothing about `quality_state`. |
| Ties | same key and same `local_receive_ts`: the later event in the input wins. Everything else is independent of input order. |
| `staleness_ms` | required, no default: usability of old data is the caller's decision. |

**Limitation (deliberate).** Canonical events have no instrument/symbol field.
The collector is single-instrument (BTCUSDT) by configuration, so the key is
sufficient today. When a second instrument is collected, canonical identity must
gain an instrument before cross-instrument alignment is safe. Also: no adapter
sets `market_type` yet (all default to `linear_perpetual`), so a future spot
adapter **must** set it explicitly for the identity guarantee to mean anything.

Tests: `tests/test_cross_exchange_alignment.py` (33), including replay parity
through the real `ReplayEngine`.
