# Phase H: Causal Order-Book State Reconstruction

## The question this answers

"What was the order-book state at local receive time T?" — not "what
messages occurred before T?" These are different: the second only needs
event replay; the first needs state reconstruction using only causally
available information.

## Outcome: B (small, targeted addition — not a redesign)

Before this phase, the repository could answer the second question
(`ReplayEngine.run()` processes a whole session) but not the first as a
clean, first-class query. Three approaches were considered:

1. **Reinvent state reconstruction.** Rejected: `LocalBook`/`sequence.py`
   already correctly implement Binance USD-M's snapshot-bridge model,
   Binance Spot's separate rules, and Bybit's decrease/reset rule —
   extensively verified in earlier sessions (Phase 6 closed five real
   Binance USD-M defects by reading official docs). Re-deriving any of
   this would risk silent divergence from proven code.
2. **Change what `ReplayEngine`/`BookUpdate` record.** Rejected: a real
   change to a heavily-tested, unrelated module's output shape, for a need
   satisfiable without touching it.
3. **A thin, causal, read-only wrapper around the existing, unmodified
   `ReplayEngine`.** Chosen — see `collector/collector/book_observation.py`.

## How it works

`ReplayFrame.timestamp_ms` is confirmed (by reading
`ReplaySource.from_records`) to already be the causal availability
timestamp for every frame kind: `local_receive_ts` for WIRE frames,
`response_receive_ts` for REST_SNAPSHOT frames. Filtering the frame list to
`timestamp_ms <= observation_ts` **before** constructing a `ReplaySource`
(which itself sorts by `order_key`, so filtering never disturbs ordering)
and running the ordinary `ReplayEngine` reproduces exactly what live
processing would have looked like at that moment — because that is
precisely what live processing *is*.

No new reconstruction logic is added anywhere; only a causal cutoff is
applied to the input. `reconstruct_book_at()` returns a `BookObservation`
with full `bids`/`asks` depth (from `LocalBook.bids`/`.asks` directly, not
`BookUpdate`'s narrow best-bid/ask strings), the reconstructed quality
state, and instrument identity (recovered from `LocalBook.previous`, the
last successfully applied canonical event — confirmed by reading
`book_engine.py` to be the only place a full event, with its `.instrument`
field, is retained).

## Verified findings, not assumptions

- A **crossed book is correctly rejected** as `invalid_book` by
  `_validated_maps`'s `max(bids) >= min(asks)` check — discovered while
  writing adversarial tests with careless bid/ask values, not by reading
  the code first. This is existing, unmodified behavior; the test data was
  wrong, not the implementation.
- The causal boundary is inclusive at exactly `local_receive_ts ==
  observation_ts` and exclusive one millisecond later, verified against
  real reconstruction output (not just unit-level logic).
- A genuine sequence gap remains **visible** (`quality_state !=
  BookQuality.VALID.value`, `status is AVAILABLE`) rather than hidden —
  degraded data is reported, not disappeared.
- `NEVER_OBSERVED` is split into two causes: zero causal frames at all
  (`frames_considered == 0`) versus frames existing but none successfully
  applied, e.g. only a malformed frame (`frames_considered > 0`,
  `book.previous is None`) — genuinely different facts about what happened,
  not collapsed into one.

## Mutation testing performed

Twelve categories total, matching the original Phase H specification's
list. The first four were done in an earlier session; this session
verified that claim against the actual merged test table (it read "five
mutations" in prose but the table itself only ever listed four — treated
as four, not five, since the table is the artifact that can be checked and
the fifth was never identifiable in either the merged commit or this
session's own review) and then ran the remaining eight, mapped to concrete
code locations before writing any new test, each applied to the real
source, confirmed to break real tests, then restored and reverified byte-identical.

**Already present (4, from the original merged commit):**

| Mutation | Tests broken |
|---|---|
| `<=` → `<` in the causal filter | 5 |
| Causal filter removed entirely (all frames unconditionally included) | 6 |
| Staleness dropped (`STALE` always reported as `AVAILABLE`) | 1 |
| `NEVER_OBSERVED` (no frames) turned into a fake `AVAILABLE` empty book | 1 |

**Run this session (8, mapped to source location → expected failure →
actual failure count → test names before running, not after):**

| # | Mutation | Location | Failures | Representative test names |
|---|---|---|---|---|
| 1 | Exchange timestamp substitution: source `timestamp_ms` from `exchange_event_ts` instead of `local_receive_ts` | `replay.py`, `ReplaySource.from_records` | 2 | `test_book_observation.py::test_future_exchange_timestamp_does_not_grant_early_availability`, `test_book_observation_okx.py::test_okx_future_exchange_timestamp_does_not_grant_early_availability` |
| 2 | Post-observation event admission via a second path | *(no second path exists — see below)* | — | — |
| 3 | Skip gap validation: bypass `self.comparator.check(...)` entirely | `book_engine.py`, `LocalBook.apply` | 6 | `test_book_observation_okx.py::test_okx_a_sequence_gap_is_visible_as_degraded_quality_not_silently_continued`, `test_replay.py::test_sequence_gap_is_detected_and_does_not_become_valid`, `test_replay.py::test_numerically_increasing_ids_cannot_repair_a_broken_chain`, +3 more |
| 4 | Preserve VALID after gap: `BookQualityStateMachine.gap()` made a no-op | `quality_events.py` | 8 | across `test_book_observation.py`, `test_book_observation_okx.py`, `test_storage_replay_corruption_resistance.py` |
| 5 | Apply delta twice: stale/duplicate branch falls through to `_apply(event)` instead of returning | `book_engine.py`, `LocalBook.apply` | 1 | `test_book_observation.py::test_a_duplicate_delta_does_not_double_apply_the_quantity` |
| 6 | Collapse Spot/USD-M: `_BY_TRIPLE` keyed by `(exchange, native_symbol)` only, dropping `market_type` | `instrument.py` | 4 | `test_instrument_identity.py` (multiple), `test_storage_identity_read_path.py::test_same_native_symbol_never_collides_across_venues_and_markets` |
| 7 | Remove instrument identity from key: `InstrumentId.key` drops `market_type` from the joined string | `instrument.py` | 6 | `test_instrument_identity.py`, `test_storage_identity_read_path.py` (multiple) |
| 8 | Hide quality state: `reconstruct_book_at` hardcodes `quality_state="VALID"` | `book_observation.py` | 3 | `test_book_observation.py::test_a_sequence_gap_is_visible_as_degraded_quality_not_silently_continued`, `test_book_observation_okx.py` equivalents |

Mutation #2 has no entry in the failures column because there is no second
admission path to mutate: `book_engine.py` (confirmed by grep, zero
matches) never references any timestamp field at all — causality is
enforced exclusively by `book_observation.py`'s single pre-filter line,
already mutation #1 of the original four. This is recorded as an
architectural finding (single enforcement point, verified by source
inspection) plus a positive regression test
(`test_four_venue_adversarial_observation.py::test_exact_boundary_included_one_ms_after_excluded`,
which feeds the *full* frame list including future frames and confirms the
result is identical to feeding only the causally-filtered subset), rather
than a second, redundant mutation of the same line.

All eight mutations were restored to the original source (`cmp` confirmed
byte-identical) and the full suite reverified green before any new test
was written.

## Checksum: an architectural placement decision, deliberately deferred

OKX's `books`/`books-l2-tbt`/`books50-l2-tbt` channels include a real
`checksum` field (CRC32 over the top 25 bid/ask levels — sourced externally,
see the classification table below; not implemented or read anywhere in
this repository). The question this session answered is not "should we add
it" but "where would it belong, and does that boundary already exist
cleanly enough to add it now."

Reading `book_engine.py`, `sequence.py` and `canonical.py` end to end shows
three distinct validation layers already in place:

1. **Raw validation** — `raw_capture.py` / adapters capture bytes
   losslessly, uninterpreted.
2. **Sequence validation** — `sequence.py`'s per-venue comparators
   (`OKXSequenceComparator` etc.), pure functions comparing update
   identifiers only, called from `LocalBook.apply` before any level is
   touched.
3. **Book-state validation** — `LocalBook._validated_maps`, applied *after*
   a sequence-valid update has been merged into the in-memory book (crossed
   book, non-negative quantity, etc.).

A checksum is fundamentally layer 3, not layer 2: it is a property of the
*reconstructed* book state, not of a single message's sequence identifier.
The user's own framing is exactly what the code confirms — a stream can
have perfectly valid `seqId`/`prevSeqId` continuity and still diverge from
the exchange's true book state (a subtle level-merge bug, a decimal
precision mismatch, a silently-dropped message that still happens to
satisfy sequence continuity). Sequence validation cannot catch that class
of fault by construction; only a book-state check can.

**Decision: not implemented this phase.** Reasons, all independently
sufficient:

- `CanonicalOrderBookEvent` has no `checksum` field at all today — adding
  one is a real schema change, not a validation tweak, and OKX is the only
  venue that would ever populate it.
- `LocalBook` is venue-generic; a checksum validator is inherently
  venue-specific (OKX's own 25-level/CRC32 algorithm), which would need the
  same kind of per-venue injection pattern `comparator` already uses —
  new architecture, not a bug fix.
- Production does not subscribe to OKX's `books` channel at all (Gate 1
  finding below), so there is no live traffic to validate a checksum
  implementation against; writing untested-against-reality validation logic
  for a channel nothing currently captures is exactly the "sounds safer so
  add it" scope creep this project's own instructions warn against.

**Required future work, recorded so it is not silently lost:** add an
optional `checksum` field to `CanonicalOrderBookEvent` (OKX-populated only,
`None` elsewhere); add a venue-keyed checksum validator (mirroring how
`comparator` is selected per venue in `book_engine.py`) that runs in the
book-state layer, after `_validated_maps`, comparing the reconstructed
top-25 levels against the message's declared checksum; add a
`CHECKSUM_MISMATCH` quality-event type distinct from `SEQUENCE_GAP`, since
the two faults are not the same thing and conflating them would hide which
kind of corruption occurred; and — a genuine prerequisite, not just
"nice to have" — enable OKX `books` subscription in a production collector
first, so the validator is ever exercised against real traffic before it is
trusted.

## Gate 1 — OKX protocol audit and production-verification classification

Read in full: `okx.py`, `okx_capture.py`, `run_okx_collector.py`,
`run_okx_capture.py`, `sequence.py`'s `OKXSequenceComparator`, and
`OKX_D11_CHANNEL_SCHEMAS.md`; checked against official OKX documentation
and two independent production trading libraries (nautilustrader,
gocryptotrader) rather than trusted from memory.

**Critical finding, not previously documented anywhere in this repository:**
`run_okx_collector.py` — the actual production OKX collector — explicitly
excludes the `books` channel by design; its own module docstring states
the six D11 channels are the entirety of its scope. The only code that ever
subscribes to `books` is `run_okx_capture.py`, a standalone research-only
script (`--duration 600`, manual runs), never a durable production
collector. **OKX order-book reconstruction has never been exercised
against real captured OKX traffic** — every OKX book test in this
repository, before and after this session, constructs hand-built wire JSON.
That is a legitimate way to test protocol logic in isolation, and this
session's four-venue dataset continues doing exactly that on the user's
explicit instruction (blocking Phase H on live OKX capture would turn a
deterministic correctness/replay phase into a deployment/venue-validation
phase) — but it means the following table's second half cannot honestly be
marked "verified."

| Area | OKX status |
|---|---|
| Wire/schema parsing | Fixture-tested |
| `books` sequence semantics | Fixture-tested |
| `seqId`/`prevSeqId` handling | Fixture-tested |
| Causal observation | Fixture-tested |
| Identity | Fixture-tested |
| Replay/storage corruption resistance | Fixture-tested |
| Official depth contract | Externally verified — `books`: 400 levels, incremental, 100ms; `books-l2-tbt`: 400 levels, tick-by-tick, 10ms, login+VIP5 required; `books50-l2-tbt`: 50 levels, VIP4+; `books5`: 5-level snapshot, 100ms |
| Checksum field existence | Externally verified — real field on `books`/`books-l2-tbt`/`books50-l2-tbt` push messages, CRC32 over top-25 bid/ask levels |
| Checksum validation by collector | Not implemented (see decision above) |
| Real OKX `books` traffic | Not verified |
| Production collector subscribing to `books` | Not enabled |
| Real delivered depth | Not verified — `_parse_books` applies no truncation of its own, but since production never subscribes, actual delivered depth is unconfirmed against anything OKX has sent |
| `books-l2-tbt` | Not implemented / not supported — zero references anywhere in source |

The controlling distinction: **"the implementation correctly handles the
OKX protocol semantics we have modeled" is not the same claim as "we have
proven the implementation against real OKX order-book traffic."** Every
item in this document's "verified" language for OKX means the former only.

## Tests

`test_book_observation.py` — 19 tests, unchanged from the earlier session.
`test_book_observation_okx.py` — 18 tests, unchanged. Both built on real
fixtures and the real, unmodified `ReplayEngine` throughout.

**New this session:**

- `test_four_venue_adversarial_observation.py` — 30 tests. All four venues
  (Binance Spot, Binance USD-M, Bybit Linear, OKX Swap), each on its own
  independent local clock (staggered, not synchronized), reconstructed
  through the real per-venue fixture builders already proven in
  `test_replay.py`, `test_replay_bybit_parity.py`,
  `test_book_observation_okx.py` and `test_phase_e_identity_parity.py` —
  no protocol payload here is hand-invented where a proven builder already
  exists. Covers the task's full 14-case adversarial list: exact-boundary
  inclusion/exclusion, exchange-timestamp substitution, duplicate updates
  (found, while building this, that Binance Spot and OKX have **no**
  duplicate/stale carve-out unlike Binance USD-M — a resent message is
  correctly classified as a gap on both, confirmed against real
  reconstruction output, not assumed from a docstring), sequence gap +
  venue-specific recovery (three different real mechanisms: Binance's
  REST-snapshot re-bridge, Bybit's/OKX's fresh-snapshot-on-the-wire),
  staleness, `NEVER_OBSERVED`, identity mismatch, Spot-vs-USD-M
  non-collision, malformed frames, and replay determinism (including
  input-order independence) per venue.
- `test_storage_corruption_matrix_extended.py` — 8 tests. Extends PR #44's
  single-field, single-venue case with: two static source-inspection
  checks proving the *general* mechanism (canonical Parquet, every column
  of it, is never read by this path — not just `instrument_key`); dynamic
  corruption of `QUALITY_EVENTS_SCHEMA`'s real `exchange`,
  `local_receive_ts`, `update_id` and `quality_state` columns (the generic
  `ORDERBOOK_SCHEMA` has none of these — see the file's own header note for
  the schema-reality correction made while building it); a corrupted
  canonical book row (bids/asks); and a second venue (Bybit) to confirm the
  property is not OKX-specific. One case is an explicitly-labeled
  hypothetical, since OKX has no order-book canonical schema in production
  at all.
- `test_replay_parity_direct_vs_engine.py` — 3 tests. This repository has
  only one reconstruction implementation (`LocalBook`), driven from two
  call sites: the live collectors' own direct `adapter.normalize()` +
  `book.apply()` loop, and `ReplayEngine`'s identical internal dispatch.
  Proves those two call sites, fed the same raw payloads, produce identical
  final state — not just best bid/ask, but full bid/ask levels, quality
  state, sequence position, and identity — through both the happy path and
  a genuine gap+recovery, across all four venues. (Building this surfaced
  one real harness bug worth recording: Binance's REST snapshot is *not*
  applied via `adapter.normalize()` at all, live or in replay — both
  call `LocalBook.binance_snapshot()` directly with an event built by
  `adapter.snapshot_event()`. Fixed in the harness, not the source.)

**Full suite: 1136 passed** (1095 + 41). `compileall` clean, `git diff
--check` clean.

## Lightweight performance note

Per this phase's explicit "no optimization rewrite" scope: a single
`reconstruct_book_at` call over a synthetic 5,000-event single-symbol
Binance USD-M chain (snapshot bridge + 5,000 incremental diffs) completed
in ~0.10s (~51,000 events/sec) on this session's container. This used
one price level per message; real multi-level order-book diffs would
differ, and this was not benchmarked further — documented, not optimized,
per the phase's own scope boundary.

## Production-verification limitations

- **OKX**: see the classification table above in full. In short: protocol
  logic is fixture-tested and correct against the modeled semantics; real
  `books` traffic has never been captured or replayed, because production
  does not subscribe to that channel; checksum validation and
  `books-l2-tbt` remain unimplemented, by deliberate, recorded decision for
  the former.
- **Binance Spot, Binance USD-M, Bybit**: reconstruction logic is
  fixture-tested via the same real per-venue builders used throughout this
  project; this session did not attempt live-capture verification for any
  venue, consistent with every earlier phase's status in this document.
- **No live exchange session backs any of this work.** Verified against
  real fixtures and the real, unmodified reconstruction code path.

## Phase H completion statement

Phase H is complete for the implemented offline/reference architecture. The
repository can causally reconstruct bounded-depth order-book observations
from durable raw data using venue-specific sequence semantics, identity,
quality state, staleness, provenance, and deterministic replay, for all
four supported venues (Binance Spot, Binance USD-M, Bybit Linear, OKX
Swap), proven by: the causal-boundary and staleness mechanism (4 + 4
mutations, all caught); venue-specific sequence/gap/recovery semantics
proven distinct rather than shared; a 14-case adversarial dataset across
all four venues; an extended corruption matrix proving canonical-Parquet
corruption cannot rewrite raw-derived truth, across two venues and six
distinct fields; and replay parity between the live call pattern and
`ReplayEngine`, through both the happy path and gap/recovery, on all four
venues.

**Production-verification limitation:** OKX `books` is not currently
subscribed by the production collector, and no real OKX `books` capture
has been replay-validated. OKX checksum validation and `books-l2-tbt`
support remain unimplemented/unverified. Therefore this is not a claim of
end-to-end production validation of the OKX order-book leg — every other
venue's protocol logic is equally fixture-tested only, but their
production collectors *do* capture the relevant channels live, which OKX's
does not.

## Not attempted (updated)

- OKX `books-l2-tbt` channel support.
- OKX checksum validation (deliberately deferred; see decision above).
- Enabling OKX `books` subscription in a production collector (a
  prerequisite for checksum validation to mean anything, and for closing
  the production-verification gap above) — explicitly out of scope for an
  offline/replay-correctness phase, per the user's own framing.
- Live-captured replay validation for any venue, OKX included — no venue's
  reconstruction has been run against a real captured exchange session in
  this project to date.
