# Bybit ticker staleness (D12)

Implemented in `collector/collector/adapters/bybit.py`, with field-level
provenance carried on `CanonicalMarkPriceEvent` and `CanonicalOIEvent`.

## The defect

Bybit's `tickers` topic sends a full `snapshot` and then `delta` messages
carrying only changed fields. The previous adapter did this:

```python
self._ticker_state.update(d); d = self._ticker_state
if any(k in d for k in ("markPrice", "indexPrice", "fundingRate", ...)):
    events.append(CanonicalMarkPriceEvent(...))
```

The merge happened **before** the presence test, so the test consulted the
*merged* state, not the message. Once any mark price had ever been seen, the
condition was true on essentially every subsequent delta — including deltas
about unrelated fields like `volume24h`. Each one emitted a mark-price event
filled with carried-forward values.

Downstream there was no way to tell them apart. A funding rate last observed
minutes ago looked exactly like one observed now.

## The fix

**Emit only on genuine observation.** The set of surfaced fields actually
present in *this message* is computed before merging. A delta that carries
none of them emits no event and is classified `EMPTY_DATA` with the detail
`ticker_delta_carried_no_surfaced_field`, so it is visible rather than
silently dropped.

**State provenance on every event.** Two fields are added:

| Field | Meaning |
|---|---|
| `carried_forward` | which of this event's values came from an earlier message |
| `field_age_ms` | `(field, age_ms)` per field, measured against the venue clock |

with helpers `is_carried_forward(field)` and `age_of(field)`.

A field that has **never** been observed is absent from `field_age_ms`
entirely — it is not reported as zero-age, because absence and freshness are
different claims. Out-of-order ticker timestamps clamp to `0` rather than
producing a negative age.

When the venue sends no `ts`, no age is claimed at all: `field_age_ms` is
empty rather than being computed against local time, which would silently
mix two clocks.

**Snapshots replace, deltas merge.** A `snapshot` clears the cached state
and the per-field timestamps rather than merging into them; otherwise a
field the venue has stopped reporting would survive forever and keep aging.

## Known limitations

- Mark, index and funding share one event. A delta carrying only
  `indexPrice` still emits a `CanonicalMarkPriceEvent` whose `mark_price` is
  carried forward — correctly labelled, but the canonical model does not yet
  split these into separate streams.
- The snapshot/delta treatment follows the same model the orderbook topic
  already uses. It is consistent and conservative, but like D14 for Binance
  it has **not** been re-verified against current official Bybit v5
  documentation. That verification is still outstanding.
- Bybit has no live websocket client in `run_collector`, so this adapter is
  exercised by tests and replay fixtures only — no production data flows
  through it yet.
