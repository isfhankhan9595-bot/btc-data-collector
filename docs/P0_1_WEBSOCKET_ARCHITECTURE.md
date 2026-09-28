# P0-1: WebSocket Receive/Processing Architecture (post-correction)

Two rounds: the original P0-1 (PR #60, merged) decoupled the socket receive
loop from downstream processing via a bounded queue and a worker. A
post-merge hostile audit of that merged code (this document) found that
the split, as merged, had moved raw-frame capture into the worker —
reopening a real data-loss window from the opposite direction. This
document describes the corrected, final architecture.

## Architecture

```
WebSocket frame
  -> _consume() [receive loop]
       - capture local_receive_ts
       - classify control frames
       - JSON decode (pure CPU, no I/O)
       - on_raw_frame()  <-- durable raw capture happens HERE, before enqueue
       - enqueue IngestItem (bounded queue, retried with a shutdown check)
  -> _process_queue() [single worker, FIFO, persists across reconnects]
       -> _process_item(): malformed-frame quality event, or on_message()
            (adapter -> sequence validation -> book reconstruction -> canonical events)
```

**What moved and why.** The originally merged version put JSON decode,
raw capture, *and* `on_message` all in the worker, with `_consume` doing
nothing but building an `IngestItem` and enqueueing it. That reintroduced
exactly the failure mode P0-1 was meant to close, from the other
direction: a frame that had been received and timestamped existed only
in an in-memory `asyncio.Queue` — up to `ingest_queue_maxsize` of them at
once — until the worker got around to it. A crash or `SIGKILL` in that
window lost raw evidence for every frame still queued, with nothing in
the architecture able to detect or report it. That is a direct violation
of this project's central invariant: raw exchange data is authoritative
and durable before anything else happens to it.

The fix moves JSON decoding and raw-frame capture back into `_consume`,
before the frame is ever enqueued — the same ordering the pre-P0-1
architecture had for those two steps. Only `on_message` (the actually
unbounded-latency, per-venue adapter/book-reconstruction work) stays in
the worker, which is what P0-1 was actually for.

**The accepted trade-off.** `ParquetWriter.write()` (what `on_raw_frame`
typically calls) is a cheap in-memory buffer append on the common path,
but does real, synchronous file I/O at a segment-rollover boundary
(open/close/rename). Moving raw capture back into the receive loop
reintroduces that blocking risk — but it is bounded and infrequent (once
per rollover threshold, not once per message), a materially smaller risk
than the data-loss window it replaces. "Data truth comes before
performance" is the explicit reason for this choice, not an afterthought.

## Guarantees

**Raw-data durability.** A frame that has entered `_consume`'s loop body
is raw-captured before it is enqueued, before its fate depends on queue
capacity, worker availability, or a later crash. A crash or `SIGKILL` at
any point after `on_raw_frame` returns loses nothing beyond whatever the
raw writer's own durability contract already allows (its own buffering/
flush semantics, unchanged by this work) — not an additional in-memory-
only window this class introduces.

**Bounded queue, actually enforced.** `ingest_queue_maxsize <= 0` now
raises `ValueError` at construction. `asyncio.Queue` treats a
non-positive `maxsize` as *unbounded* — the opposite of every "bounded
queue" claim previously made in this file's own comments, with nothing
previously enforcing it.

**Backpressure never drops a frame, and never hangs shutdown.** A full
queue makes `_consume` poll (`put_nowait` + short sleep) rather than
block indefinitely. If `stop()` is called while a frame is stuck behind a
full queue with no worker draining it, `_consume` returns promptly
(bounded by the poll interval) rather than waiting forever — the frame's
raw evidence is already durable by that point (`on_raw_frame` already
ran), so what is abandoned is only its `on_message` processing in *this*
process run, counted explicitly via `frames_abandoned_at_shutdown` and
reported through a quality event, never silent.

**Ordering.** A single long-lived worker drains the queue FIFO,
persisting across reconnects — unchanged by this correction.

**Metrics** (all directly inspectable):
`frames_received`, `frames_enqueued`, `frames_processed`,
`processing_errors`, `queue_high_watermark`, `queue_backpressure_events`,
`frames_abandoned_at_shutdown`. None of these describe a state that
cannot actually occur; `frames_abandoned_at_shutdown` was added by this
correction specifically so shutdown-time degradation is observable rather
than invisible.

## What survives which failure

| Event | Raw evidence | on_message processing (this run) |
|---|---|---|
| Crash/SIGKILL after `on_raw_frame` returns, before enqueue | Survives (raw writer's own durability contract) | Lost |
| Crash/SIGKILL while item sits in the queue | Survives | Lost |
| Worker exception on one item | Survives | That item lost; worker keeps running, subsequent items unaffected |
| `stop()` while a frame is blocked behind a full queue | Survives | Abandoned, counted (`frames_abandoned_at_shutdown`) |
| `stop()` with queue non-empty but not full | Survives | Drained by the worker (bounded 30s `queue.join()` wait) before the worker is cancelled |
| Raw writer itself fails (disk full, etc.) | **Not proven safe by this correction** — see Known limitations | Frame is still enqueued and processed; `on_raw_frame`'s own exception handling here only logs a warning and continues |

## Known limitations (not claimed fixed by this correction)

- **Raw-capture failure handling (audited in the final P0-1 pass).** There
  are two distinct layers, and they fail differently:
  - *Writer failure* (`OSError`, disk full, serialization error) is caught
    inside `RawCapture.capture_wire`, which increments its own
    `capture_failures`, returns `False`, and emits a durable `DATA_DROP`
    quality event (`raw_capture_failed:wire:<ExcType>`, `rows_lost: 1`) **per
    lost frame**. Covered by `test_raw_capture.py::
    test_writer_failure_fails_open_and_emits_a_quality_event`. Ingestion
    continues; raw completeness is broken and *stated in the data*, not
    silently.
  - *Callback failure* (a bug in a runner's `_capture_raw_frame`, before it
    reaches `RawCapture`) was previously a log line only. It is now counted
    (`raw_capture_callback_failures`) and reported as one `ERROR` quality
    event per failure streak (not per frame), with a recovery log when it
    clears. A frame is never dropped from processing because of either.
  - **Judgement, not proof:** failing *open* is kept deliberately (stopping
    would lose every frame, not only the raw copy). It means a consumer must
    treat any `raw_capture` `DATA_DROP` or `raw_capture_callback_failed`
    event as a raw-completeness break for that window. There is no separate
    persistent "degraded" flag beyond those events. INFERENCE: sufficient,
    since the events are durable and per-loss; NOT VERIFIED against a real
    disk-full condition.
  - **Not covered:** a failure at a *periodic or shutdown flush* outside
    `capture_wire`'s `try` (e.g. `ParquetWriter.close()`), which this pass did
    not trace.
- **No dedicated crash-window integration test** exercises an actual
  process kill; the guarantees above are argued from reading the code
  paths involved (this file, plus `ParquetWriter`'s buffering/flush
  behavior), not demonstrated by literally killing a process mid-frame in
  a test. This is stated as `UNVERIFIED` rather than implied tested.
- **Queue capacity (default 2000) is not empirically validated** against
  real message rates or real processing latency on the production
  collector. `UNVERIFIED` — this requires measurement on the actual AWS
  deployment, not something derivable from this offline environment.
- **Disk-full behavior specifically** was not simulated: the writer-failure
  path is tested with an injected `OSError`, not a full filesystem.

## Live verification status

**OFFLINE VERIFIED only.** No live exchange connection was made in either
the original P0-1 work or this correction. Every guarantee above is
argued from the source and demonstrated by tests against synthetic
fixtures, not observed against real exchange traffic.

## Final audit additions

- **Drain timeout is now accounted for.** When shutdown's bounded drain
  (`shutdown_drain_timeout_s`, default 30 s) gives up, the frames still queued
  are added to `frames_abandoned_at_shutdown` and an `ERROR` quality event
  `ingest_queue_drain_timeout:<n>` is emitted; previously this was a log line
  only, unlike the sibling QueueFull-at-shutdown path. Raw capture precedes the
  enqueue, so these frames lose *processing* in this run, not raw evidence.
  `qsize()` excludes an item the worker is mid-way through.
- **Test additions** pin invariants that mutation testing showed were
  unguarded: the arrival timestamp (never processing time) reaches
  `on_message` on *both* dispatch paths; one worker and FIFO order survive a
  real reconnect through `start()`; the `BACKPRESSURE` quality event is emitted
  once per stall; callback failures and drain timeouts are counted.
- **Still true:** backpressure blocks the receive loop rather than dropping
  frames; a slow `on_raw_frame` (e.g. a Parquet rollover) blocks receive
  directly; the default queue size of 2000 is unvalidated against real traffic.
