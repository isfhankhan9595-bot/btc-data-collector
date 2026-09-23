# Trade-message deduplication

Recommended by PR #48's own audit as the next bounded task ("the
highest-priority item per this task's own hierarchy... a real gap
affecting every consumer of `non_book_events` today"). This phase audited
the actual gap, designed the deduplication contract from first principles,
and implemented it at the one shared layer that gives live and replay
identical behavior for free.

## Audit of PR #48 (performed before any code was written)

Independently verified against the real repository: PR #48 merged
(`dbf95c1`, matching `main` HEAD exactly), baseline **1193 passed**,
compileall clean, diff-check clean.

`book_metrics.py`'s microprice formula was checked against the standard
Stoikov convention and confirmed correct in both directions (large resting
quantity at one side pulls the fair price toward the *other*, thinner
side — verified against the actual weighting in the code, not just the
comment). `test_trade_flow_mutation_audit.py` was checked for the
"real mutation, not reimplementation" standard this project requires and
found to genuinely follow it — it pins the invariant the mutation evidence
depends on, rather than re-simulating the mutation as committed code.
`BybitAdapter`'s instrument-stamping claim was independently re-verified
at runtime (not trusted from the doc): `BybitAdapter().normalize(...)`
does carry `instrument=BYBIT_LINEAR_BTCUSDT`, confirming PR #48's own
audit of PR #47 was accurate. No defects found in PR #48 itself.

## Duplicate definition

A duplicate is a second `CanonicalTradeEvent` whose
`(exchange, market_type, instrument_key_or_unidentified, stream, trade_id)`
tuple one adapter instance has already produced. Deliberately **not**
"same price/qty/timestamp" (legitimate same-value trades with different
IDs must never merge) and **not** "same trade_id alone" (the same ID on a
different venue, market type, or trade *representation* must never
collide).

## Venue-by-venue trade identity (read from source)

| Venue | Stream | `trade_id` source | Representation |
|---|---|---|---|
| Binance USD-M | `<symbol>@aggTrade` | `a` (aggregate trade ID) | only aggTrade is consumed; ordinary `trade` stream is not subscribed |
| Binance Spot | `<symbol>@trade` | `t` (per-execution ID) | the opposite choice from USD-M; aggTrade is not used |
| Bybit Linear | `publicTrade.<symbol>` | `i` (Bybit's own trade ID) | `seq` and `T` are never treated as identity |
| OKX Swap | `trades` **and** `trades-all` | `tradeId`, per array element | two distinct channels, each producing its own events; a push can carry more than one trade |

All four already canonicalize `trade_id` to `Optional[str]` — confirmed by
reading each adapter's `CanonicalTradeEvent` construction, not assumed —
so there is no int/str identity risk to guard against.

OKX's `trades` and `trades-all` are **not** deduplicated against each
other: whether `trades-all` aggregates or overlaps with `trades` is an
open, unresolved question per `adapters/okx.py`'s own docstring, and
conflating them would be exactly the "aggregate vs ordinary trade" merge
this task forbids without proof. The dedup key's `stream` component keeps
them permanently separate regardless of how that open question is
eventually resolved.

## Architectural placement

`ExchangeAdapter._dedupe_trades` (`collector/collector/adapters/base.py`),
wired into the same `__init_subclass__` hook that already stamps
instrument identity, run immediately after stamping. This is the one
place every runner (`run_collector.py`, `run_bybit_collector.py`,
`run_binance_spot_collector.py`, `run_okx_collector.py`) **and**
`ReplayEngine` already call through `adapter.normalize()` — confirmed by
grep, not assumed — so live and replay get identical duplicate behavior
automatically, with zero per-runner code and zero deduplication logic
inside any feature module. `trade_flow_observation.py` (cumulative and
windowed CVD) needed no change at all: the canonical trade stream it
consumes is simply correct by the time it gets there now.

Raw evidence is untouched: `_capture_raw_frame` persists the wire frame
before `normalize()` is ever called, at every runner, so a suppressed
duplicate's raw bytes remain on disk exactly as received, regardless of
what the canonical stream does with it.

## Missing-ID behavior

A `trade_id is None` event is **never** deduplicated against anything,
including another `None` trade — every one is kept unconditionally. This
was the task's own explicitly flagged catastrophic failure mode
("`None` in seen_ids => duplicate" collapsing every unidentified trade
together) and is guarded by a dedicated test
(`test_missing_trade_ids_are_never_deduplicated_against_each_other`), not
merely asserted in prose.

## State lifetime

Exactly as long as the adapter instance: one runner process (surviving
reconnects, since no runner recreates its adapter on reconnect) or one
`ReplayEngine.run()` call (spanning every frame and segment given to it —
`ReplaySource.from_directory`'s real usage already spans a whole date's
segments through one engine). This was a property to discover, not
design: no runner or replay code needed to change for this to hold.

**Known, documented cost:** `_seen_trade_ids` grows without bound for the
adapter's lifetime. Deliberately not optimized here, per this task's own
instruction to keep the simple, correct reference implementation over an
unproven bounded structure — flagged below as the natural next task.

## Quality-event semantics

Reuses the existing `QualityEventType.DUPLICATE` value (already used for
order-book-level duplicate updates) rather than inventing a new taxonomy
entry. `rows_lost=0`: a suppressed duplicate is not a real data loss, so
it is deliberately distinguished from `DATA_DROP` in
`UnhandledMessage.to_quality_event()`. It does not touch `BookQuality` or
invalidate the stream — informational only, matching the existing
book-level `DUPLICATE` event's own behavior.

**Fixed as a direct prerequisite, not scope creep:** `run_bybit_collector.py`
never wired `set_unhandled_sink` at all (the other three runners already
had it), so a Bybit trade duplicate would have been silently invisible in
the persisted `quality_events` stream while every other venue correctly
recorded it. One-line addition, matching the other three runners' existing
pattern exactly.

## Real mutation testing (evidence, not simulation)

Performed live against the actual source in this session: file edited,
targeted suite run, failures recorded, file restored, `diff` confirmed
byte-identical, full suite re-run green.

| Mutation | Result |
|---|---|
| A: disable duplicate rejection entirely | 11 real failures |
| B: drop `exchange` from the identity key | **0 failures — see below** |
| C: treat missing trade IDs as one identity | 1 real failure |

**Mutation B is a genuine finding, investigated rather than hidden or
forced** (per this task's own "never manipulate assertions to force a
failure" rule): every concrete adapter is bound to exactly one
`(exchange, market_type, instrument)` triple for its whole lifetime, and
every current cross-venue test in this phase uses a *separate adapter
instance* per venue — so instance isolation alone already provides that
separation today, independent of what the key contains.
`stream` + `trade_id` are the only currently load-bearing components
(proven positively: OKX's `trades` vs `trades-all` on one shared instance
requires `stream`; every basic dedup test requires `trade_id`).
`exchange`/`market_type`/`instrument` remain in the key as deliberate
defense-in-depth for a future multi-instrument-per-adapter namespace
(already a documented limitation in `docs/INSTRUMENT_IDENTITY.md`, not
invented for this finding) — the same choice PR #48 made for windowed
CVD's own currently-redundant upper bound.
`test_one_adapter_instance_is_always_bound_to_at_most_one_identity_triple`
pins the invariant that currently makes this safe.

## Downstream verification

`test_cumulative_cvd_does_not_double_count_a_duplicate_trade` and
`test_windowed_trade_flow_does_not_double_count_a_duplicate_trade`
directly compare a clean frame set against the same set plus one literal
duplicate wire frame, through the real `observe_trade_flow_at`/
`observe_windowed_trade_flow_at` functions, unmodified — `trade_count`,
`cvd`, `buy_volume`, and `sell_volume` are identical in both cases.
Neither function required any code change for this to hold.

## Test count

1212 passed (full suite; 1193 baseline + 19 new
`test_trade_deduplication.py` tests). compileall clean. `git diff --check`
clean.

## Recommended next bounded task

**Bound `_seen_trade_ids`'s memory growth.** The current reference
implementation is an unbounded `set`, correct but unsafe for a very
long-running live process ingesting millions of trades. Needs its own
evidence-based design: whether venue trade IDs are reliably monotonic per
symbol (not verified here — "NOT VERIFIED" per this task's own
verification-honesty requirement) would determine whether a bounded
high-water-mark structure is safe, versus an LRU or time-windowed
eviction policy if not. Deliberately not attempted in this session as its
own bounded task, per the same one-architectural-change-at-a-time
discipline this task itself required.
