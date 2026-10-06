# Deterministic replay

Implemented in `collector/collector/replay.py`; CLI at `collector/scripts/replay.py`.

Status vocabulary used in this document (and in `DATA_SUFFICIENCY.md`):

| Term | Meaning |
|---|---|
| IMPLEMENTED | code exists and is wired |
| TESTED | a committed test exercises it with synthetic / fixture frames |
| REPLAY-VERIFIED | a committed test drives recorded-format frames through `ReplayEngine` and asserts the canonical output |
| LIVE-VERIFIED | evidence from a real venue session exists. **No venue has this in this repository.** |

## What replay means here

Replay drives the **same objects** the live collectors drive. There is no
replay-only parser and no replay-only reconstruction path; a second
implementation would be free to diverge from production and would mask exactly
the bugs replay exists to catch.

```
recorded frame -> <Venue>Adapter.normalize() -+-> CanonicalOrderBookEvent -> LocalBook.apply() -> quality state
                                              |
                                              +-> every other canonical event -> ReplayResult.non_book_events
```

Only the clock and the source differ. `ReplayEngine(venue)` selects the adapter
from a registry: **BINANCE, BYBIT, OKX**. An unregistered venue raises
`ValueError`; it is never silently routed to another venue's adapter.

## Two replay paths

### Order-book replay

Book events are applied through `LocalBook`, whose sequence comparator is
venue-specific (`BinanceSequenceComparator`, `BybitSequenceComparator`,
`OKXSequenceComparator`). Quality transitions are recorded from the states the
replay actually observed, matching what the live runners record. Partial-depth
events (`book_source != "DIFF_DEPTH_RECONSTRUCTED"`) are refused as
non-authoritative, as live does.

| Venue | Status |
|---|---|
| Binance | IMPLEMENTED, TESTED, REPLAY-VERIFIED (snapshot bridge, gaps, recovery, repeated diffs) |
| Bybit | IMPLEMENTED, TESTED, REPLAY-VERIFIED (wire snapshot, resync signal, recovery transitions) |
| OKX | Correct on the cases now covered by `tests/test_p1_replay_integrity.py` (snapshot, update, level delete, `prevSeqId` mismatch, sequence reset, missing sequence ids, update with no snapshot, malformed level; price text preserved). Fixture-tested only; a malformed delta is reported unhandled while the book stays VALID until the next message exposes the sequence break (in-flight PR #90 addresses fail-closed behaviour). **No live OKX book collection exists**: `run_okx_collector` deliberately excludes `books`, so recorded OKX book frames exist only if `run_okx_capture` was used. OKX levels are parsed as `float` while the canonical dataclass declares `Decimal`; the precision consequence has not been analysed. |

### Non-order-book event replay

Before PR #23, `ReplayEngine` discarded every canonical event that was not an
order-book event, so replay proved books reconstruct and nothing else. Now
every such event is preserved, as the frozen dataclass instance the adapter
yielded, in `ReplayResult.non_book_events`. They never touch `LocalBook`.

REPLAY-VERIFIED by `tests/test_replay_non_book_events.py`:

| Venue | Events replayed | Notes |
|---|---|---|
| Binance | trades (`aggTrade`), mark price, liquidation, open interest (REST poll) | mark price value is asserted; the funding field it carries is not separately asserted by a replay test; OI is routed through the same `normalize_binance_oi()` live uses -- see below |
| Bybit | trades, liquidation, ticker -> mark price **and** OI | carried-forward field provenance from a partial ticker survives replay unaltered (tested directly) |
| OKX | `trades`, `trades-all`, `mark-price`, `index-tickers`, `funding-rate`, `open-interest`, `liquidation-orders` | `trades` and `trades-all` are never conflated; mark and index stay on separate fields; OI preserves all three units (contracts / coin / USD); liquidation attribution is preserved and **never filtered by instrument** |

**Binance open interest IS replayable.** Binance OI is a REST poll, recorded
as a `raw_rest` row with `purpose="open_interest"`.
`ReplaySource.from_records` routes it into a `FrameKind.REST_OI` frame
(ordered by `response_receive_ts`, never by the exchange-reported time), and
`ReplayEngine._handle_rest_oi()` sends the recorded response body through
`binance_oi.normalize_binance_oi()` -- **the exact same normalizer**
`run_collector.py`'s live poll loop calls, producing an identical
`CanonicalOIEvent` (`local_receive_ts = response_receive_ts`, never the
exchange-reported time). See `tests/test_binance_oi_replayability.py`
(`test_live_and_replay_produce_identical_canonical_events_from_the_same_body`,
`test_availability_is_the_response_receive_time_not_the_exchange_time`).
This produces canonical-layer `CanonicalOIEvent` objects for replay
consumers (e.g. cross-exchange alignment); there is currently no pipeline
stage that writes replayed OI back into the flattened `OPENINTEREST_SCHEMA`
parquet `pipeline/dataset_assembler.py` reads, so that assembler only aligns
whichever OI segments were actually collected live (see
`docs/RESEARCH_DATASET_TIME_CONTRACT.md`).

Replay does **not** compute features over these events, and it does not assess
their quality: each event carries whatever `quality_state` its adapter assigned
(default `VALID`).

## Causality

Recorded websocket frames and recorded REST snapshot responses merge into
**one** time-ordered stream:

- a websocket frame becomes available at its `local_receive_ts`
- a REST snapshot becomes available at its `response_receive_ts`

This mirrors live, where a snapshot requested during a gap arrives
asynchronously and can only bridge once it has landed. A snapshot is never
visible before the moment it arrived in the recorded run, so replay cannot
repair a gap with information from the future. A snapshot request with no
response is excluded entirely: it bridged nothing live, so it bridges nothing
here -- and the exclusion is counted in `ReplayResult.dropped_rest_rows`.

**Timestamps and what they establish**

| Stamp | Used for ordering / availability? | Notes |
|---|---|---|
| `local_receive_ts` / `local_receive_ns` (wire) | yes -- the only causal clock | ns is recorded for P0-11+ rows; legacy rows are ms only |
| `response_receive_ts` (REST) | yes | **whole milliseconds only**; there is no REST ns stamp |
| `exchange_event_ts`, `request_ts`, `local_capture_ts`, `local_process_ts` | never | source evidence / diagnostics, not availability |
| `receive_mono_ns` | never | intra-run delta only; not an epoch value |

**Same-millisecond ordering is a convention, not evidence.** Because REST rows
carry whole milliseconds, a REST response and a WIRE frame in the same
millisecond may have arrived in either order. Replay sorts the WIRE frame
first (`kind_rank`), which is deterministic but not observed; the two possible
true orders produce different outputs (pinned by
`test_the_two_physically_possible_orders_give_different_outputs`). Replay does
not hide this: every REST row that shares a millisecond with a WIRE frame, and
every ns-less WIRE frame sharing a millisecond with ns-bearing ones, is counted
in `ReplayResult.unresolved_order_ties` and makes the result non-pristine.
Nothing is fabricated to resolve it.

**Availability of a bridged book.** A `BookUpdate` stamped `RECOVERY_BRIDGE` /
`RECOVERY_INCREMENTAL` has `timestamp_ms` equal to the diff's own receive time
but only became available when the bridging snapshot landed.
`BookUpdate.available_ts_ms` carries that recorded time; consumers joining on
availability must use it, not `timestamp_ms`.

REST snapshots are a **Binance-only** mechanism. For any other venue a
`REST_SNAPSHOT` frame is counted as unhandled and recorded as
`replay_rest_snapshot_not_supported_for_venue:<VENUE>`; it is never parsed as
Binance's `lastUpdateId` format.

## Determinism

Frames carry a total order key
`(timestamp_ms, kind_rank, ns_tiebreak, source_index)`, so ties never depend on
filesystem iteration, dict ordering or sort stability. Within one millisecond a
wire frame sorts before a REST row (see the causality caveat above: a
convention, **not** an observed order). Ordering is independent of the order
frames are supplied in (tested).

Replay is deterministic. That is a weaker claim than causally correct: it is
causally faithful only where the recorded stamps distinguish the frames.

### What `digest` proves, and what it does not

`ReplayResult.digest` is an **output-state digest**. It is a SHA-256 over, in
order:

1. every book update (`BookUpdate.digest_tuple()`),
2. every non-book canonical event (its type name plus its full field set), so a
   trade and a liquidation with coincidentally equal fields never collide,
3. every quality event, by `(event_type, reason, quality_state)`.

Tests show the digest changes for a changed or removed trade, changed OI,
changed funding rate and changed liquidation quantity, and is identical across
two runs. **Not covered by the digest:** the frame counters (`frames_total`,
`frames_unhandled`, ...), skipped/dropped source rows, unresolved ordering
ties, and the `previous_state` / `new_state` / lineage keys of quality events.
Equal digests therefore prove equal *output*, not equal or clean *inputs*: a
replay that skipped foreign-venue rows, dropped REST rows or relied on
same-millisecond ties has the same digest as a pristine one when the book it
built is the same. The digest value is unchanged by the P1 integrity work
(pinned in `tests/test_p1_replay_integrity.py`).

Two further concepts sit beside it on `ReplayResult`:

| Field | Question it answers |
|---|---|
| `digest` | what output state was produced |
| `input_fingerprint` | exactly which recorded evidence (frame kinds, recorded stamps, lineage, success flags, payload hashes, skip accounting) was consumed |
| `integrity_issues()` / `is_pristine` | whether the replay was a clean, fully evidenced reconstruction; lists `frames_undecodable`, `frames_unhandled`, `frames_truncated`, `snapshots_rejected`, `oi_rejected`, `skipped_rows`, `dropped_rest_rows`, `unresolved_order_ties`, `empty_replay` |

Replay-originated quality events also carry `replay_ts_ms` (the recorded
availability time of the frame being handled, never the wall clock),
`replay_source_index`, `replay_frame_kind` and, when recorded,
`replay_connection_id`. These do not enter the digest.

```bash
python -m collector.scripts.replay 2026-06-03 --data-dir data --venue BYBIT --verify-determinism
```

`--venue` defaults to `BINANCE`.

## Reading recorded data

`ReplaySource.from_directory(data_dir, date, venue)` (and
`replay_directory(..., venue=...)`) resolves the venue's own stream directories
(see `STORAGE_NAMESPACES.md`) and keeps a row only if its own `venue` column
matches the requested venue. Excluded rows are counted in
`ReplaySource.skipped_rows`, copied onto `ReplayResult.skipped_rows` (so
`replay_directory()` callers see them too), and logged. For OKX this includes the legacy
unprefixed `raw_wire`, where captures made before storage namespacing share a
directory with Binance's frames.

## No network

The module imports no HTTP client and performs no I/O beyond reading recorded
segments. Enforced by an AST-based test over the module's own imports
(`test_replay_module_imports_no_network_client`), not a substring scan.

## What replay refuses to do

| Situation | Behaviour |
|---|---|
| frame was undecodable when recorded | stays undecodable; counted, never re-parsed |
| adapter cannot produce a valid event (malformed / missing required field) | `frames_unhandled` and a quality event; **no event is fabricated** (tested for OKX funding-rate) |
| snapshot request failed | recorded as an attempt; bridges nothing |
| snapshot never returned (a stored null reads back as `NaT`/`NaN`, not `None`) | excluded from the stream entirely and counted in `dropped_rest_rows`; it no longer aborts the replay |
| REST row with a purpose replay does not drive | excluded and counted in `dropped_rest_rows` |
| recorded `truncated` payload (capture clipped it; live saw the whole frame) | refused as `replay_truncated_frame`, counted in `frames_truncated` and `frames_undecodable` |
| snapshot malformed or empty | rejected, counted |
| `depth10` partial | refused as non-authoritative, as live does |
| broken chain, later tidy increments | stays non-VALID; increments cannot repair it |
| REST snapshot frame for a non-Binance venue | unhandled + quality event |
| no recorded frames at all | CLI exits 1: an empty replay is not a clean replay |

## Known limitations

- **No live verification.** Every replay test uses synthetic or fixture frames.
  No venue's real wire traffic has been replayed, because none has been
  captured in an environment with exchange access (see `DATA_SUFFICIENCY.md`).
- A full live-vs-replay parity harness over a captured production session does
  not exist. Parity is demonstrated by driving the same adapter and book
  engine over the same frames.
- OKX order-book replay is fixture-tested but has no live collector (above).
- REST stamps have whole-millisecond resolution; same-millisecond WIRE-vs-REST order cannot be established and is reported, not resolved (see Causality).
- `input_fingerprint` identifies the evidence a replay consumed; it does not prove that evidence is complete (a missing capture is not detectable from the capture itself).
- Non-book events are preserved, not featurised or quality-gated.
- Bybit and OKX sequence semantics are exercised against documented protocol
  behaviour, not against real sequence-reset or gap events from the venues.
- Binance sequence semantics are documented in `BINANCE_USDM_SEMANTICS.md`
  (D14 closed): a re-delivered or non-advancing diff is classified stale or
  duplicate rather than as a gap. The earlier note that duplicate handling was
  an unverified assumption no longer applies.
