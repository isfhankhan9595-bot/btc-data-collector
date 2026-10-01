# P0-11 — timestamp resolution and clock domains

Scope is deliberately narrow: **only the local receive stamp on the raw-wire
path** carried sub-millisecond information that was being discarded. Nothing
else was converted.

## Clock domains (never interchangeable)

| Domain | Source | Persisted as | Meaning |
|---|---|---|---|
| Exchange | venue message | `exchange_event_ts` (ms) + `exchange_event_ts_precision="ms"` | source evidence; every supported venue stamps in **ms** |
| Local wall | `time.time_ns()` | `local_receive_ns` (int64 epoch ns) | when the collector received the frame |
| Local monotonic | `time.monotonic_ns()` | `receive_mono_ns` (int64) | differences within one process run only |

`ns_from_ms` is a representation change, not a measurement: a value in an
ns-typed column is not thereby a nanosecond measurement. That is why
precision is stored next to the value (`local_receive_precision`,
`exchange_event_ts_precision`).

## Receive boundary
One `ReceiveStamp` is read once, before decode, raw capture and queueing, and
travels through the ingest queue unchanged. The legacy ms value is derived
from the same clock read, so ms and ns cannot disagree (a mismatch is
refused, not reconciled). No wall<->monotonic offset exists anywhere:
`wall - monotonic` is not stable and nothing here needs it.

## Schema `raw_wire` 1.0 -> 1.1 (additive, nullable)
New: `local_receive_ns`, `receive_mono_ns`, `local_receive_precision`,
`exchange_event_ts_precision`. int64 rather than `timestamp[ns]` so pandas
cannot round-trip through float64 (an epoch-ns is ~1.75e18 > 2**53).
Legacy 1.0 files lack the columns and read as `ms` precision — never as zero.

## Replay
Uses only the recorded stamps; it reads no clock. Orders by recorded ns,
legacy ms rows at the start of their ms. Ties stay ties (no synthetic
offset); secondary keys are the pre-existing kind rank and recorded order.
The ns columns are re-read exactly from Arrow (pandas turns an int64 column
containing a NULL into float64, losing the low digits).

## Not changed (still millisecond) — stated, not assumed
Canonical events, causal alignment, dataset assembly, REST response stamps,
and every other Parquet stream keep millisecond timestamps. Compaction does
not process `raw_wire` (asserted by a test). Feature availability semantics
are untouched, so this change cannot alter causal eligibility.

## Measured / not verified
* MEASURED (sandbox, not EC2): `capture_receive_stamp()` ≈ 0.85 µs/frame vs
  0.13 µs for the legacy expression (+0.73 µs; dominated by dataclass
  validation, not the clock reads).
* NOT VERIFIED: EC2 clock resolution/cost; behaviour under a stepping wall
  clock (monotonic delta is the intra-run tool); whether sub-ms ordering
  changes any downstream research result.
