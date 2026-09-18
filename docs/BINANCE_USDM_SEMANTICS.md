# Binance USD-M diff-depth semantics (D14 closure)

## Source

All protocol claims below are transcribed from the official procedure, read at
implementation time rather than from memory:

> **How to manage a local order book correctly** — USD-M futures
> <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly>
> Verified **2026-09-19**.

D14 recorded that `binance_snapshot_bridge` and the USD-M sequence rules were
"implemented from assumption and have not been re-verified against current
official Binance documentation". They are now verified. The rules were
**correct**; five defects were found in the code *around* them.

## The documented procedure, and where each step lives

| Step | Documented rule | Enforced in | Verified |
|---|---|---|---|
| 1 | Open a stream to `<symbol>@depth` | `config.BINANCE_PUBLIC_WS_URL` (`btcusdt@depth@100ms`) | yes |
| 2 | Buffer events; for the same price the latest update covers the previous | `LocalBook._buffer_event`, `_validated_maps` | yes |
| 3 | Get a snapshot from `/fapi/v1/depth?limit=1000` | `run_collector.BINANCE_DEPTH_SNAPSHOT_URL` | yes |
| 4 | Drop any event where `u` **<** `lastUpdateId` | `LocalBook._attempt_bridge` | yes |
| 5 | First processed event has `U <= lastUpdateId` **AND** `u >= lastUpdateId` | `sequence.binance_snapshot_bridge` | yes |
| 6 | Each new event's `pu` equals the previous event's `u` | `sequence.BinanceSequenceComparator` | yes |
| 7 | Data in each event is the **absolute** quantity for a price level | `LocalBook._validated_maps` | yes |
| 8 | If the quantity is 0, remove the price level | `LocalBook._validated_maps` | yes |
| 9 | Removing a level absent from the local book is normal | `LocalBook._validated_maps` (`pop(price, None)`) | yes |

### Two places USD-M differs from Spot

Both were already correct and are now pinned by tests that fail if either is
changed to the Spot form:

- **Step 4** uses strict `<`. Spot uses `<=`. Spot's rule would discard the
  one event permitted to bridge.
- **Step 5** has no `+1`. Spot uses `U <= lastUpdateId+1 AND u >= lastUpdateId+1`.
  Spot's rule shifts the bridge by one event.

## Defects found and closed

### D18 — a re-delivered diff was misreported as a sequence gap

`BinanceSequenceComparator` evaluated only step 6. A re-delivered or late
event has a `pu` that no longer matches the book's `u`, so it was classified
as a gap.

That is **safe but wrong**. Step 7 makes every event a statement of absolute
quantities for the levels it names, so an event whose `u` does not advance
past the book cannot contain anything the book is missing. The consequences of
the misclassification were real:

- a `VALID → SEQUENCE_GAP` transition written into the durable quality record
  for a hole the venue never created, which is precisely the kind of false
  data-quality signal downstream research must be able to trust;
- a REST resync out of the bounded budget (5 per 60s) for no reason.

Now classified in four ways, not one: `stale_update` and `duplicate_update`
(not gaps — dropped, book stays `VALID`, no recovery), versus `pu_mismatch`
and `pu_missing` (gaps). Duplicates remain **observable**: `stale_count`
increments and the runner emits a `DUPLICATE` quality event. Nothing is
silently accepted.

### D19 — `depth10` partial depth could reach the diff path

`route_message` matches `"@depth" in stream`, which also matches
`btcusdt@depth10@100ms`. Partial depth is a periodic **top-N snapshot**, not a
diff: it never names levels that have left the top N, so applying it through
the diff path freezes stale depth below the visible window while the book
still reports `VALID`.

`run_collector` and `replay` already refused it at their routing layers, but
`LocalBook` owns book authority and must not depend on a caller for that
guarantee. `LocalBook.apply` now refuses any `PARTIAL_DEPTH` event: no
mutation, and no quality-state change either, since a partial push is not
evidence about the diff chain in either direction.

Only latent today — `config` subscribes `@depth@100ms` alone — but the adapter
declares `depth10` as a supported channel.

### D20 — unprovable continuity was indistinguishable from violated continuity

A frame with no `pu` field reported `pu_mismatch`, identical to a genuine
venue-side break. Different causes (our parsing vs. the venue's stream) that
need different investigation. Now `pu_missing` and `update_id_missing`, both
still gaps — failing safe is unchanged, only the recorded reason is truthful.

### D21 — the bridge predicate raised on malformed ids

`binance_snapshot_bridge` compared ids directly, so a `None` id raised
`TypeError`. `LocalBook` validates before calling, but the predicate is also
public via `BinanceAdapter.bridge_accepts`. A predicate that raises turns a
data problem into a crashed ingest task. It now returns `False`.

### D22 — a valid snapshot was discarded when it arrived ahead of the buffer

A snapshot can fail to bridge for two causally opposite reasons, and both were
collapsed into one discard:

| Situation | Meaning | Correct response |
|---|---|---|
| Every buffered diff has `u < lastUpdateId` (step 4 drops them all) | Snapshot is **ahead** of the buffer. Nothing is wrong; the next diff will straddle `lastUpdateId`. | **Retain** it |
| Earliest surviving diff has `U > lastUpdateId` | Snapshot is **behind a hole** — the diffs between it and the buffer were never received. No future event repairs this. | Discard; fetch a newer one |

Discarding the first case meant fetching another snapshot, which was equally
likely to land ahead of the buffer, looping until a diff happened to straddle
— spending the bounded recovery budget on a state that resolves itself within
one 100ms diff, and holding the book un-bridged for up to a minute.

Such a snapshot is now retained and re-attempted by
`LocalBook.retry_pending_snapshot()` on the next buffered diff. **This cannot
create a false `VALID`**: the retry runs the identical step 4 + step 5 proof
against real recorded snapshot bytes. Staleness is bounded by the bridge rule
itself — a diff arriving much later has `U >> lastUpdateId` and so fails step
5, at which point the snapshot is discarded.

Retention is bounded to one snapshot, and is dropped on `invalidate()`
(reconnect) and on any successful bridge.

**Accounting.** On a cold start the buffer is empty until the first diff
lands, so the startup snapshot commonly arrives ahead of it. Booking that as a
recovery failure would escalate backoff toward `attempts_exhausted` during
normal operation, so `run_collector` treats `snapshot_ahead_of_buffer` as a
deferred success: the REST call did succeed, and the bridge completes from
data already in hand.

**Live/replay parity.** `replay.py` performs the same retry at the same point,
or the same recorded bytes would produce a different book.

## Tests

`collector/tests/test_binance_usdm_semantics.py` — 24 tests.

- Steps 4–9 asserted directly against the documented text, including both
  USD-M/Spot divergences.
- One test per defect, named for the behaviour it forbids.
- Adversarial: a stale event must not resurrect a removed price level; a
  500-event stale storm must not change quality state or grow the buffer.
- Property/fuzz: over 400 random sequences mixing breaks, staleness and
  well-formed continuations, an unbridged book never reports `VALID`.
- Recovery-budget accounting for the retained snapshot.

`tests/test_replay.py::test_replayed_repeat_of_an_applied_diff_is_not_silently_accepted`
asserted the pre-D18 contract — that a resent diff *should* become a gap
because that "cannot produce a falsely VALID book". It was rewritten, not
weakened: the duplicate must still be observable in the quality record, but it
is now reported as a duplicate, the book stays trusted, and idempotence is
asserted by comparing book updates against the same session without the
repeat.

## Not claimed

- No live session was run against Binance in this work; conformance is
  asserted against the documented procedure and recorded fixtures.
- Steps 1 and 3 are verified as URLs/config, not by a live connection.
