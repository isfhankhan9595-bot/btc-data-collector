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

Five mutations, each applied to the actual source, confirmed to break
tests, then restored and reverified green:

| Mutation | Tests broken |
|---|---|
| `<=` → `<` in the causal filter | 5 |
| Causal filter removed entirely (all frames unconditionally included) | 6 |
| Staleness dropped (`STALE` always reported as `AVAILABLE`) | 1 |
| `NEVER_OBSERVED` (no frames) turned into a fake `AVAILABLE` empty book | 1 |

## Tests

`test_book_observation.py` — **19 tests**, built on real fixtures from
`test_replay.py` (`_depth_frame`, `_snapshot_frame`, `_normal_session`) and
the real, unmodified `ReplayEngine` throughout, per this phase's own
instruction not to manufacture every event by hand. Covers: snapshot-only
reconstruction (Bybit, since Binance genuinely requires a bridging diff by
protocol — not tested there because it isn't true there), snapshot+delta
reconstruction, level add/update/delete, the exact causal boundary,
future-timestamp-in-payload rejection, duplicate non-double-application,
visible sequence-gap degradation, both `NEVER_OBSERVED` causes, the
staleness boundary, replay-twice determinism, input-order independence,
source-frame immutability, correct instrument identity, type validation,
and an AST-based no-wall-clock/network structural check.

**Full suite: 1075 passed** (1056 + 19). `compileall` clean, `git diff
--check` clean.

## Not done — the bulk of the full Phase H specification remains

This phase's own scoping document describes an enormous surface. Status
after this session's continuation:

**Closed this session:**
- OKX order-book reconstruction (18 tests, `tests/test_book_observation_okx.py`):
  snapshot-only, snapshot+deltas, level update/delete, causal boundary,
  future-exchange-timestamp rejection, duplicate non-double-application
  (OKX has no dedicated is_stale carve-out unlike Binance — pinned as the
  actual, not assumed, behavior), sequence-gap visibility, recovery via a
  fresh wire snapshot, both NEVER_OBSERVED causes, staleness boundary,
  replay-twice determinism, input-order independence, instrument identity,
  and OKX/Binance reconstructions proven independent. One real fixture bug
  was hit and root-caused during this work (a crossed-book test snapshot —
  the exact class of gotcha this document already flagged from the
  original Binance/Bybit work), not a defect in the reconstruction code.
- Storage→replay→alignment corruption resistance
  (`tests/test_storage_replay_corruption_resistance.py`, 2 tests): a real
  OKX raw-wire session is persisted through the actual collector's writer,
  a canonical row with a deliberately wrong `instrument_key` (a Bybit
  identity) is written alongside it on disk, and `reconstruct_book_at` is
  proven to reconstruct the correct OKX identity and book state regardless
  — the corrupted row is never read, confirmed both dynamically (the
  reconstruction result is compared against an uncorrupted control run) and
  statically (neither `replay.py` nor `book_observation.py` references
  `instrument_key` anywhere in source).

**Still not attempted:**
- The full four-venue (Binance Spot/USD-M, Bybit, OKX) end-to-end
  adversarial dataset with mixed staleness/missing-venue/gap/recovery
  scenarios exercised together in one test.
- Mutations 3/4/5/6/9/10/11/12 (sequence validation skipping, double-apply,
  VALID-after-gap, Spot/USD-M identity collapse, cross-venue collision,
  contradictory-identity acceptance, ordering-depends-on-input, replay
  consuming canonical identity as truth) — mutations 1/2/7/8 not yet
  independently pinned either, only the 5 from the original Phase H commit.
- OKX's `books-l2-tbt` channel (distinct from `books`) and checksum
  semantics remain unaudited.
- Full-depth vs. bounded-depth terminology audit: this session did not
  verify what depth OKX's `books` channel actually delivers in production;
  the existing D11 raw-capture work should be consulted before claiming a
  specific depth guarantee.

No live exchange session backs any of this; verified against real fixtures
and the real reconstruction code path, consistent with every other phase's
live-verification status in this project.
