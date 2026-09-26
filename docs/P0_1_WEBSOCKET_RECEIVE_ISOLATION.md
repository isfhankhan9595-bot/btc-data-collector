# P0-1: WebSocket Receive/Processing Isolation

## Repository-state finding (before any code was written)

The task document claimed a previous session had already implemented
P0-1 (bounded queue, maxsize=2000, single worker, 1251 tests passing) on
some other branch. This was checked exhaustively before trusting it:

```
git log --all --oneline --grep="P0-1"           -> no results
git log --all --oneline --grep="bounded queue|processing worker|backpressure" -i -> no results
git log --all --oneline -- collector/collector/websocket_client.py -> last touched by
    69f347d "Phase 7: OKX v5 public raw capture" (an old, unrelated commit)
```

**P0-1 did not exist anywhere in this repository** -- not on any of the
~60 branches, not in any commit message, not in the file itself, which
still had the plain inline structure the task doc described as the
problem (`async for msg in ws: ... await self.on_message(...)`). This was
a previous session's inaccurate self-report, not a misplaced branch. Per
the task's own instruction ("you must discover it rather than recreate it
blindly" -- read as: if genuinely absent, build it, don't skip it), P0-1
was implemented fresh this session.

## A. Root cause

`WebSocketClient._consume`'s read loop directly awaited `on_message` for
every frame, inline, before reading the next one. `run_collector.py`'s
`handle_message` does real processing work, including `await`ed calls
that can involve actual I/O (`_handle_binance_orderbook`'s REST
snapshot-bridge fetch, confirmed by reading it). A slow processing step
therefore stalled the *read* loop itself, not just downstream book state.

## B. Why it matters

Raw capture (`on_raw_frame`) already happens before `on_message` is ever
reached, synchronously, on the fast path -- confirmed by reading
`_consume` directly. So the risk was never "raw bytes lost"; it was:
stalling the read loop risks the exchange (or the underlying TCP/OS
buffers) seeing this client as a slow reader, which can trigger
disconnection or rate-limiting server-side, and serially delays every
message queued behind the slow one -- a real availability/latency risk
for a live collector, independent of raw-capture durability.

## C. Before

```
async for msg in ws:
    decode
    raw-capture (fast, synchronous)
    await on_message(...)   <- next read blocked until this returns
```

## D. After

```
async for msg in ws:
    decode
    raw-capture (fast, synchronous, unchanged)
    processing_queue.put_nowait(item)   <- never blocks; next read proceeds immediately
                                            (QueueFull -> drop-newest, counted, DATA_DROP
                                             quality event -- raw copy still safe)

separate worker task (lives for the whole start() call, across reconnects):
    while True:
        item = await processing_queue.get()
        await on_message(*item)         <- all the slow work happens here, decoupled
```

`processing_queue_maxsize=2000`: not copied from the earlier (inaccurate)
report -- independently justified. A single-symbol, single-venue
collector's combined orderbook+trade traffic realistically peaks in the
low hundreds of messages/sec even during high volatility; the slowest
realistic per-message stall (a REST snapshot-bridge fetch) resolves in
low single-digit seconds even under a poor network. 2000 buffers
comfortably past that worst case at a materially higher sustained rate
than observed traffic, without holding unbounded memory.

**Overflow policy: drop-newest, never block.** Blocking `_consume` on a
full queue would silently reintroduce the exact coupling this fix
removes. The dropped item was already raw-captured before the drop
decision, so it remains replayable from raw storage even though it never
entered live processing -- an explicit, bounded, observable degradation
(counted in `processing_queue_overflow`, reported as a `DATA_DROP`
quality event via the existing P0-2 WAL-backed path), not a silent one.

**Ordering is preserved exactly**: one `asyncio.Queue` (FIFO), one
worker, processed strictly in arrival order -- proven directly
(`test_processing_order_is_preserved`, 20 messages).

**Graceful shutdown**: the worker runs for the entire `start()` call
(survives reconnects, so nothing queued right before a disconnect is
lost); `start()`'s `finally` block sends a shutdown sentinel and awaits
the worker to fully drain before `start()` itself returns, so a caller
that awaits `start()` after calling `stop()` observes every
already-accepted message actually processed.

## E. Files changed

- `collector/collector/websocket_client.py`: `_processing_queue`,
  `_processing_worker`, `processing_queue_overflow`, the
  `_WORKER_SHUTDOWN` sentinel, and the `try/finally` wrapping `start()`'s
  reconnect loop. `_consume`'s dispatch point now enqueues instead of
  awaiting `on_message` directly. Raw capture, control-frame handling,
  decode-error handling, and the reconnect/backoff logic are all
  unchanged -- confirmed by keeping every line before the dispatch point
  untouched.
- `collector/tests/test_bybit_collector.py`,
  `collector/tests/test_instrument_key_persistence.py`: their `_drive`/
  `_drive_bybit` helpers previously assumed `_consume` returning meant
  processing had completed (true under the old inline architecture).
  Fixed to start the worker and `await processing_queue.join()` before
  returning -- a test-harness update to match the new real architecture,
  not a weakening of what the tests assert.

## F. Tests

`collector/tests/test_websocket_client.py`, 5 new tests:
`test_consume_does_not_block_on_a_slow_on_message` (the core property --
`_consume` returns in <0.1s despite a 0.2s-sleeping handler),
`test_processing_order_is_preserved`, `test_raw_capture_happens_before_
enqueue_regardless_of_processing_backlog`, `test_processing_queue_
overflow_drops_newest_without_blocking_or_raising`,
`test_graceful_stop_drains_every_already_queued_message` (exercises the
real `start()` reconnect-loop lifecycle end to end, not just the queue
primitive in isolation).

**Mutation testing** (real source mutated, restored, `cmp` confirmed
byte-identical both times):

| Mutation | Result |
|---|---|
| `put_nowait` -> blocking `put` (reintroduces receive/processing coupling) | Real deadlock: `test_websocket_client.py` hung past a 25s `timeout` wrapper (exit code 124) -- the overflow test's never-draining consumer combined with a blocking enqueue on a full queue never returns. Strong evidence the non-blocking property is load-bearing, via the most severe possible failure mode. |
| `start()`'s drain `finally` body replaced with `pass` (worker abandoned mid-drain) | Clean assertion failure: `test_graceful_stop_drains_every_already_queued_message` -- `processed == []` instead of `[0..9]`. |

**Baseline** (measured on the true current branch state before any P0-1
change, after P0-2 was already present): 1267 passed. **Final**: **1272
passed** (1267 + 5). `compileall`: clean. `git diff --check`: clean.

## G. Data integrity

Nothing about causality, identity, or the existing durability model
changes. What can now happen that couldn't before: under sustained
processing overload, a *live-processing* message can be dropped (never a
raw-capture one) -- explicit, bounded, counted, and reported as a quality
event, replayable from raw storage. What cannot happen: silent loss with
no trace, reordering (strict FIFO, one worker), or fabrication.

## H. Crash semantics

Unchanged from before this fix at the process level (a hard kill still
loses whatever was only in the in-memory processing queue at that
instant, same as before P0-1 existed at all -- P0-1 does not claim to
add durability to in-flight processing, only to stop the receive loop
from blocking on it). P0-2's WAL remains the durability layer for quality
events specifically, unaffected and unweakened by this change.

## I. Research impact

None on causality/replay/reproducibility: raw capture, the sole source of
truth for replay, is unaffected by any part of this change -- it happens
before the queue, unconditionally. Live book-state completeness under
sustained overload is the one thing that can now degrade (a dropped
live-processing message means live state isn't updated for it), but the
raw record needed to reconstruct that state later via replay is untouched.

## J. Remaining limitations

- `processing_queue_overflow` is a lifetime counter, not reset per
  connection/reconnect -- a design choice (total dropped messages across
  the client's life is arguably the more useful number), not yet
  reconsidered against an alternative.
- No live-traffic measurement of `processing_queue_maxsize=2000`'s actual
  adequacy exists; the justification above is reasoned from known
  message-rate and stall-duration bounds, not measured production load.
- P0-3 through P0-12 untouched, as instructed.

## K. Git state

Branch: `p0-1-receive-processing-isolation` (based on `origin/main` @
`790df62`, the same commit P0-2 branched from). Commits and push status:
see the final report in-conversation.
