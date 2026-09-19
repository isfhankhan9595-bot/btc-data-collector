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
| OKX | The adapter parses `books` and replay applies it through `LocalBook`, but this is **not covered by a committed test** (an ad-hoc synthetic snapshot + 2 updates reached VALID, which is evidence of nothing more than that). **No live OKX book collection exists**: `run_okx_collector` deliberately excludes `books`, so recorded OKX book frames exist only if `run_okx_capture` was used. OKX levels are parsed as `float` while the canonical dataclass declares `Decimal`; the precision consequence has not been analysed. |

### Non-order-book event replay

Before PR #23, `ReplayEngine` discarded every canonical event that was not an
order-book event, so replay proved books reconstruct and nothing else. Now
every such event is preserved, as the frozen dataclass instance the adapter
yielded, in `ReplayResult.non_book_events`. They never touch `LocalBook`.

REPLAY-VERIFIED by `tests/test_replay_non_book_events.py`:

| Venue | Events replayed | Notes |
|---|---|---|
| Binance | trades (`aggTrade`), mark price, liquidation | mark price value is asserted; the funding field it carries is not separately asserted by a replay test |
| Bybit | trades, liquidation, ticker -> mark price **and** OI | carried-forward field provenance from a partial ticker survives replay unaltered (tested directly) |
| OKX | `trades`, `trades-all`, `mark-price`, `index-tickers`, `funding-rate`, `open-interest`, `liquidation-orders` | `trades` and `trades-all` are never conflated; mark and index stay on separate fields; OI preserves all three units (contracts / coin / USD); liquidation attribution is preserved and **never filtered by instrument** |

**Not replayed: Binance open interest.** Binance OI is a REST poll, recorded as
a `raw_rest` row with `purpose="open_interest"`. `ReplaySource.from_records`
consumes only `purpose == "orderbook_snapshot"` REST rows
(`test_non_snapshot_rest_purposes_do_not_drive_the_book`), and OI is not routed
through `BinanceAdapter.normalize()`. There is therefore no shared live/replay
path for Binance OI, and it never appears in `non_book_events`.

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
here.

REST snapshots are a **Binance-only** mechanism. For any other venue a
`REST_SNAPSHOT` frame is counted as unhandled and recorded as
`replay_rest_snapshot_not_supported_for_venue:<VENUE>`; it is never parsed as
Binance's `lastUpdateId` format.

## Determinism

Frames carry a total order key `(timestamp, kind_rank, source_index)`, so ties
never depend on filesystem iteration, dict ordering or sort stability. Within
one millisecond a wire frame sorts before a snapshot, matching live, where the
diff was already in the socket buffer when the HTTP response completed.
Ordering is independent of the order frames are supplied in (tested).

`ReplayResult.digest` is a SHA-256 over, in order:

1. every book update (`BookUpdate.digest_tuple()`),
2. every non-book canonical event (its type name plus its full field set), so a
   trade and a liquidation with coincidentally equal fields never collide,
3. every quality event, by `(event_type, reason, quality_state)`.

Tests show the digest changes for a changed or removed trade, changed OI,
changed funding rate and changed liquidation quantity, and is identical across
two runs. **Not covered by the digest:** the frame counters (`frames_total`,
`frames_unhandled`, ...), `ReplaySource.skipped_rows`, and the `previous_state`
/ `new_state` fields of quality events.

```bash
python -m collector.scripts.replay 2026-06-03 --data-dir data --venue BYBIT --verify-determinism
```

`--venue` defaults to `BINANCE`.

## Reading recorded data

`ReplaySource.from_directory(data_dir, date, venue)` (and
`replay_directory(..., venue=...)`) resolves the venue's own stream directories
(see `STORAGE_NAMESPACES.md`) and keeps a row only if its own `venue` column
matches the requested venue. Excluded rows are counted in
`ReplaySource.skipped_rows` and logged. For OKX this includes the legacy
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
| snapshot never returned | excluded from the stream entirely |
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
- Binance OI is not replayable (above).
- OKX order-book replay is untested and has no live collector (above).
- Non-book events are preserved, not featurised or quality-gated.
- Bybit and OKX sequence semantics are exercised against documented protocol
  behaviour, not against real sequence-reset or gap events from the venues.
- Binance sequence semantics are documented in `BINANCE_USDM_SEMANTICS.md`
  (D14 closed): a re-delivered or non-advancing diff is classified stale or
  duplicate rather than as a gap. The earlier note that duplicate handling was
  an unverified assumption no longer applies.
