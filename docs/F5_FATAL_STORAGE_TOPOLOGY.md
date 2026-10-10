# F5 — Fatal storage topology / fail-closed routing

Scope: **every entry point that uses the shared `WebSocketClient`** now has an explicit, supervised
failure policy: `collector.run_collector` (Binance USD-M, the only runner with a systemd unit),
`run_binance_spot_collector`, `run_bybit_collector`, `run_okx_collector` and `run_okx_capture`
(capture-only). They implement the *same contract* with *different mechanisms* (see *Standalone
runners*). The fail-closed raw-evidence behaviour is therefore **not USD-M-only any more, and it is
not described as universal either**: it is enumerated per entry point in the table below.

| Entry point | raw writer fatal | derived writer fatal | supervised application task |
|---|---|---|---|
| `run_collector` (USD-M) | terminate, exit 70 | isolate route | yes (`_failure_supervisor_loop`) |
| `run_binance_spot_collector` | terminate, exit 70 | isolate route | yes (`supervise_standalone_runner`) |
| `run_bybit_collector` | terminate, exit 70 | isolate route | yes (`supervise_standalone_runner`) |
| `run_okx_collector` | terminate, exit 70 | isolate route | yes (`supervise_standalone_runner`) |
| `run_okx_capture` (capture-only) | terminate, exit 70 | n/a (no derived writer) | yes (`OKXCaptureApp.run`) |

## The problem F5 closes

A `ParquetWriter` that latches FAILED refuses every later write, but before F5 nothing above it
treated that as more than an ordinary exception:

* `RawCapture` deliberately **failed open**: a dead `raw_wire` / `raw_rest` writer became a
  `DATA_DROP` quality event per frame while ingestion carried on with no durable raw evidence.
* The websocket worker counted any exception as a processing error and reconnected/kept going.
* `_handle_liquidation` and `_poll_openinterest` swallowed a dead writer with a blanket
  `except Exception` and kept calling it.
* A writer that latched while quiet was never noticed (the exception only appears on the *next*
  write).

## Topology (frozen)

| Failure | Verdict |
|---|---|
| `raw_wire` writer, `raw_rest` writer | **TERMINATE** — controlled, non-zero exit |
| trades (canonical), raw trade anchor (`binance_trades_raw`), dedup SQLite (`DedupStateError`) | isolate **trades** |
| `orderbook`, `binance_orderbook_raw` | isolate **orderbook** |
| `markprice` | isolate **markprice** |
| canonical `openinterest` | isolate **openinterest**; `raw_rest` keeps capturing |
| `liquidation` | isolate **liquidation** |
| publication marker / dedup-hook failure | the owning writer's row above |
| `quality_events` writer | **degrade** the quality channel; process keeps running |
| ordinary exception (not `FatalStorageError`) | continue — unchanged P0-1 isolation |
| typed fatal from a stream not in this table | **TERMINATE** (default-deny) |

The table is `failure_topology.STORAGE_STREAM_VERDICTS`; a new writer must be added there on
purpose.

### Why `raw_rest` is a raw-evidence boundary

A REST answer reflects the exchange *at request time*: its content and availability cannot be
causally reconstructed later, and replay must never contact the live exchange. A REST response is
authoritative only if it was durably captured, so a lost `raw_rest` writer terminates, and a
response that was not captured is never fed to the canonical OI writer. By contrast every derived
route (trades, book, mark, OI, liquidation) can be regenerated from preserved `raw_wire` /
`raw_rest`, so losing one only isolates that route.

## Mechanism

* **Typed exception** — `storage_errors.FatalStorageError(RuntimeError)` with `stream`,
  `component`, `stage`, `durability`. It is a leaf module. It deliberately does **not** inherit
  `OSError`. `DedupStateError` is a subclass (message-only constructor and name unchanged).
  Classification is `isinstance(exc, FatalStorageError)` only: never `RuntimeError`, never message
  text (`"FAILED"`), never a class-name string.
* **Writer** — `ParquetWriter` raises `FatalStorageError ... from exc` at the existing FAILED /
  publication-gate boundaries. F1 ordering is untouched (fsync → rename → dir fsync → marker →
  hook). Refusal after an explicit `close()` stays a plain `RuntimeError`. `failure_snapshot()` is
  a read-only view of the latch.
* **Raw capture** — `RawCapture(fail_closed_on_fatal_storage=True)` (set by the USD-M
  collector and, in this PR, by Spot, Bybit, OKX and OKX capture-only) re-raises a typed fatal from the raw writers *without consulting the quality sink*.
  Capture still precedes parsing; payload, truncation and "never fabricate" are unchanged.
* **Websocket client** — `except FatalStorageError` precedes the generic handlers at the raw-frame
  callback, the worker and the connection loop, and hands the error to an optional `on_fatal`
  hook. Ordinary exceptions keep the P0-1 behaviour (counted, worker stays alive). A raw-frame
  fatal is terminal for the client: `enter_discard_mode()` stops ingestion and reconnects while the
  worker keeps draining and calls `task_done()` exactly once per item, so `queue.join()` completes
  and a producer never blocks. The worker never calls `shutdown()`/`sys.exit`.
* **Application** — `CollectorApp` keeps `failed_components`, `isolated_routes`,
  `terminal_failure`, `quality_degraded` (each `FailureRecord`: stream, component, stage,
  durability, verdict, route, origin, first-observed ts). The first failure per component wins;
  repeats are only counted. An isolated route's handler/writer is never invoked again
  (`route_short_circuits` counts frames; no per-frame event). Reconnects do not touch any latch;
  only a process restart (F1/P0-4 startup reconciliation) re-opens a stream.
* **Supervisor** — `_failure_supervisor_loop` (1 s) reads each writer's `failure_snapshot()` so a
  failure that latched while the writer was quiet is still found; on a terminal failure it starts
  the existing `_async_shutdown` **from the supervisor task**. It never repairs a writer.
* **Exit** — `main()` returns `EXIT_FATAL_STORAGE` (70) when a terminal failure latched and the
  entry point does a single `sys.exit(main())`. No `os._exit`.
* **Reporting (recursion-safe)** — synchronous structured log → the quality WAL directly (its seq is
  held in-flight so a later checkpoint cannot cover it; the next start replays it) → one operator
  alert. It never calls `quality_writer` or `_persist_quality_event`. Once the quality channel is
  degraded, `_persist_quality_event` stops calling the failed writer. An event is counted in
  `_quality_events_wal_only` **only when its WAL record is established**; see
  "Quality-channel failure boundaries" below for the other two outcomes.
* **Quality-channel failure boundaries (remediation Part 2A)** — a failing *quality* writer degrades
  telemetry; it never terminates healthy market-data processing and is never mistaken for a raw or
  derived failure. Three boundaries are handled differently on purpose:
  * `_persist_quality_event` **keeps its contract: it raises** when the quality writer fails (the
    persistence loop, startup replay, overflow and shutdown paths depend on that to hold the
    checkpoint back). It latches the channel degraded, blocks the checkpoint and reports once.
  * `_emit_quality_event` is the market-data-path variant (handlers, order-book recovery, drains,
    OI polling, startup corruption report). It contains **only** a typed fatal that `failure_topology`
    maps to the quality channel (the event is already in the WAL). Ordinary exceptions (P0-1) and a
    typed fatal of any other stream still propagate. Order-book recovery therefore always finishes
    its lifecycle (`controller.succeed()/fail()`), and an OI error handler cannot crash the task.
  * `ParquetWriter._deliver_to_sink` stops a quality-channel fatal raised by the *sink* from
    surfacing as the **emitting writer's** failure (the unguarded migration / crashed-segment
    `DATA_DROP` paths inside `write()`). Without it, a quality failure carried through a `raw_wire`
    write was classified at the raw-frame boundary, which is TERMINATE by construction. Its classification
    uses the **default (USD-M) stream table**, i.e. it recognises `quality_events` only. Venue runners whose
    quality sink re-raises must contain their own quality stream themselves: Bybit does so in
    `_writer_quality_sink`, which delegates to `_emit_quality_event` (see "Final fix, Part 2 (Finding B)" below): the
    runner's own table (`StandaloneFailurePolicy.is_quality_channel_failure`) decides, the degrade is latched once and
    every other typed fatal is re-raised. Spot, OKX and OKX capture-only do the same
    inside their own `_persist_quality_event` (final fix, Finding A): a typed fatal that the runner's OWN table
    maps to its quality stream is latched once through `StandaloneFailurePolicy.on_fatal(origin="quality_writer")`
    (one structured log + one operator alert) and contained, so nothing escapes their sinks; any other typed
    fatal is re-raised; an ordinary exception keeps the log-and-continue path (a log line per event, as on base).
    Before this, the typed fatal was caught by the blanket `except Exception`, never latched, and logged once
    per event with no alert and a healthy-looking quality state.
  * **No WAL on these runners.** Spot, OKX and OKX capture-only keep no durable quality WAL, so a degraded
    quality channel means the events are *lost*, not retained. Once latched, the failed writer is never called
    again; every later event (and the one that tripped the latch) only increments `quality_events_lost`.
    `quality_degraded` (the latched `FailureRecord`) and `quality_channel_status()` expose this on the runner,
    and OKX capture-only adds it to the status it returns and prints. Raw capture, market-data routes and the
    process exit status are unaffected by a quality-only failure; a later raw-evidence fatal still exits 70.
  * `StandaloneFailurePolicy._report` lets a *quality* record nest inside another failure's report (it only logs
    and alerts; the runner's reporter is never called for it, so nothing reaches the failed quality writer).
    On a full disk the derived writers of Spot/OKX have no quality sink, so the first write to the quality
    writer is the one made by the derived failure's own reporter; without this the quality latch made there
    was recorded silently. A nested non-quality report is still suppressed.
  * Genuine raw (`raw_wire`/`raw_rest`) fatals still terminate; derived-writer fatals still isolate
    their route; a failed quality writer is never called again and never resurrected in-process.
* **Degraded-channel accounting (remediation Parts 2A/2B)** — for each event that reaches the degraded
  channel, **including the event whose own write tripped the quality writer** (Part 2B: it was
  previously left out of the count although its WAL record was already durable): `_quality_events_wal_only` = WAL record established (replayed on next start);
  `_quality_events_wal_unconfirmed` = the append failed *after* assigning an id (a record may exist; its
  seq pins the checkpoint, but it is not claimed retained); `_quality_events_unrecorded` = no WAL
  record and no writer: the event is **lost**, reported as such (bounded log: first, then powers of two;
  one operator alert) and never through the failed writer. No path advances a checkpoint over an event
  whose durability is unproven. The counters are a lower bound of events whose only durable copy is the
  WAL: rows already buffered in the failed segment before the trip are held by their in-flight seqs
  (checkpoint blocked) but are not added to `_quality_events_wal_only`. An ordinary (non-typed) quality
  writer exception is not degraded-channel accounting. These counters exist in the Binance USD-M
  `CollectorApp` and (final fix, Part 2) in `BybitCollectorApp`; Spot, OKX and OKX capture-only keep no WAL and
  expose the single `quality_events_lost` counter instead.
* **Shutdown** — unchanged order. A failed writer's `close()` raising does not stop the other
  writers from closing (`_close_writer_reporting_failure`).

## Standalone runners (Spot, Bybit, OKX, OKX capture)

**The defect this closes.** These runners share the `WebSocketClient` with USD-M but had no
supervised path for a typed fatal. A derived trades-writer ENOSPC made the client enter terminal
discard mode, its task ended with `FatalStorageError`, and `_main` never awaited or observed that
task: the process stayed alive collecting nothing. On the pre-F5 base the same failure was
isolated by the ordinary worker-error path and raw capture continued, so this was a regression,
not a pre-existing gap.

**Decision: all four are brought under the raw-evidence termination contract in this PR.**
At the previously reviewed head (114e02d) only USD-M built `RawCapture(fail_closed_on_fatal_storage=True)`;
Spot and OKX capture-only failed open on a raw-writer fatal (reproduced by the review), and Bybit and the
OKX collector were no different. A capture-only process writes *nothing but* raw frames, so a dead raw
writer means it captures nothing while looking healthy; Spot's raw REST snapshot is not causally
reconstructable. All four now set the flag, which is what routes a raw-writer fatal to the termination
path below instead of the fail-open `DATA_DROP` path.

**Mechanism** (`collector/standalone_failure_policy.py`):

* `build_stream_table(raw=, derived=, quality=, dedup_route=)` builds each runner's
  stream -> verdict table **from its own live writers' `stream_name`**, so Binance USD-M names are
  never copied onto `spot_*` / `bybit_*` / `okx_*` streams. A typed fatal from a stream that is not
  in the runner's table is TERMINATE (default-deny); a USD-M name on a venue that does not write it is
  *not* silently mapped.
* `StandaloneFailurePolicy.on_fatal(exc, origin)` classifies by `isinstance(exc, FatalStorageError)`
  plus the failed writer's own `stream`, latches (`failed_components`, `isolated_routes`,
  `terminal_failure`, `quality_degraded`) and, for TERMINATE, puts the client in discard mode and sets
  `terminal_event`. It only **latches**; it never shuts anything down, so it is safe to call from the
  websocket worker.
* **Handlers do not catch typed fatals.** This preserves the committed P0-4 contract (a handler
  raises `DedupStateError` / the writer's fatal; nothing is admitted or indexed). The fatal
  propagates to the client's worker boundary, which calls `on_fatal(exc, "worker")`. The handler only
  `tag_route`s the exception (`f5_route`) before re-raising, so a failure naming a generic stream
  (`DedupStateError` -> `trades`) is still attributed to the right route, including OKX's two trade
  routes (`trades`, `trades_all`). This differs from USD-M, whose `handle_message` classifies in place.
* `supervise_standalone_runner(app, stop)` is each runner's `_main`: it **always observes** the
  application task, waits for a signal, a latched terminal failure, or the task ending, then shuts
  down *from the main task* (never from a worker), bounds the wait for the cancelled task
  (`SHUTDOWN_TASK_TIMEOUT_S`), retrieves its outcome so it can never be an unobserved background task,
  and returns the exit status. A typed fatal that escaped the task unclassified is terminal
  (origin `run_task`, default-deny).
* **Exit status:** `70` (`EXIT_FATAL_STORAGE`) when a terminal failure latched; `1`
  (`EXIT_RUN_TASK_CRASHED`) when the task died of an ordinary exception; `0` for a signalled stop.
  `__main__` does `raise SystemExit(asyncio.run(_main(...)))`; no `os._exit`.
* **OKX capture-only** does not use the supervisor; `OKXCaptureApp.run` waits on
  *task | duration | terminal_event*, so a raw failure no longer runs on silently to the end of
  `--duration` / `--forever`, and `main()` returns the exit status.
* Spot recovery (`_run_recovery` / `_capture_rest`) is outside the client worker, so it classifies its
  own fatals. A response whose raw REST capture was lost is never used to feed the book.

**Isolating a route in a standalone runner** short-circuits later frames of that route before
`normalize()` (which runs trade dedup), counted in `route_short_circuits`. A message that carries
several events drops its remaining events when one of them raises, exactly as an uncaught handler error
did before; for these venues one message feeds one route.

## F-2: exception-safe terminal shutdown (`run_collector`)

Reproduced against the real USD-M lifecycle: raw storage fails terminally, the first quality-writer
operation during shutdown also fails, `_async_shutdown` raised before `shutdown()`, all ten writers
stayed unclosed, the health monitor stayed active and the process did not exit 70.

* **Quality reporting is never a precondition of shutdown.** `_async_shutdown` and `start()`'s
  `finally` run `shutdown()` and the task cancellation in a `finally`; each preceding step is guarded.
* `_drain_integrity_quality_events_safely()` (used on the shutdown paths only) never raises and one
  failing event does not forfeit the next. Every event is already WAL-protected before it reaches the
  writer, so a failed persist only blocks the checkpoint and the next start replays it (F1/WAL ordering
  is unchanged: drain -> close `quality_writer` -> only then close the WAL).
* `shutdown()` is exception-safe and **retryable**. `_closed` means "shutdown has begun" (the supervisor
  loop reads it); cleanup completion is `_shutdown_done`, set only after a full pass. Each writer's close
  is attempted at most once (`_writer_close_results`; an attempt that was *interrupted* is not recorded
  and is retried). A failure in one step or one writer never stops the next.
* `main()` returns `EXIT_FATAL_STORAGE` even if teardown raised after a terminal failure latched; an
  unrelated crash keeps its traceback.
* No `os._exit`. The shutdown runs in the supervisor's (or signal handler's) task, never in a worker,
  so cancelling `self.tasks` cannot cancel the running coroutine.

## Behaviour changes worth knowing

* `CollectorApp.handle_message` now *classifies* a typed fatal instead of letting it propagate:
  the first failing frame isolates the route and later frames are short-circuited, so six existing
  runner tests that expected a raise were converted (USD-M only; the standalone runners still raise
  out of their handlers and are classified at the client's worker boundary). The
  safety assertions in them (trade not admitted, nothing indexed, buffer empty) are unchanged.
* Isolating *trades* stops the raw trade anchor too (route-wide), consistent with "trades, anchor
  or dedup failure ⇒ isolate trades". `raw_wire` still holds every frame.
* `_poll_openinterest` no longer writes a second, contradictory `ok=False` `raw_rest` row for a
  response that was already captured `ok=True`.
* `mark`, `oi`, `liquidation` writers now have the same non-recursive quality sink as the other
  derived writers. `quality_writer` still has none.

## Replay qualification

F5 requires `RAW EVIDENCE → production replay path → authoritative canonical observation`, without
contacting the exchange. It does **not** claim, and nothing here verifies, exact reproduction of
the feature-Parquet outputs.

## Part 2B: client lifecycle and exact queue accounting

* `WebSocketClient.start()` refuses a client that is already latched in discard mode: it connects to nothing,
  enqueues nothing and runs only the shutdown tail (a latched fatal with no classifier is still raised). A
  connection that completes *after* the latch landed is dropped before `on_reconnect`, the CONNECT quality
  event and the subscribe. A fresh client starts exactly as before.
* The full-queue wait re-checks `running` after its sleep, immediately before the next `put_nowait`. Before, a
  producer parked on a full queue could enqueue one item after the worker had drained and exited (for discard
  mode and for an ordinary `stop()`), leaving `join()` to wait out the drain timeout. Such a frame is now counted
  in `frames_abandoned_at_shutdown` and reported; `task_done()` stays exactly once per queued item.
* Idle secondary client (observed, not changed): a client blocked in `recv()` on a quiet socket does not see the
  latch until a frame arrives, on base as well. The terminal contract does not depend on it: the supervisor cancels
  the client task and the worker exits by itself. Covered by a test.

## Final fix, Part 2 (Finding B): Bybit quality-channel containment

**Defect.** `BybitCollectorApp._apply_orderbook` and `_record_adapter_unhandled` called `_persist_quality_event`
directly from the market-data path, and `_persist_quality_event` re-raises when the quality writer is FAILED.
Reproduced on the unfixed head (snapshot, then a delta with a decreasing update id, quality writer failing):

* `_apply_orderbook`: the snapshot's RECOVERING→VALID transition event raised before `ob_writer.write`, so the
  **canonical order-book row was lost** and the client counted a worker fatal for every book transition; the failed
  quality writer was called again once per transition (`bybit_quality_checkpoint_blocked` each time).
* `_record_adapter_unhandled`: the adapter invokes its sink under `except Exception: pass`
  (`ExchangeAdapter.unhandled`), so the typed quality fatal was **swallowed silently**: never latched, never alerted,
  the dead writer called again for every duplicate-trade / unrouted frame. (The frame itself was not aborted here.)

**Fix (`run_bybit_collector.py` only).**

* `_emit_quality_event` is the market-data-path variant (same contract as USD-M's). It contains **only** a typed
  fatal that Bybit's *own* failure policy maps to *its* quality channel. Any other typed fatal, and every ordinary
  exception, propagates unchanged. Used by `_apply_orderbook`, `_record_adapter_unhandled`, `_on_client_quality_event`
  and `_writer_quality_sink`. `_persist_quality_event` keeps its raise-on-failure contract (startup replay, shutdown
  reports and the failure reporter rely on it).
* `_persist_quality_event` latches the channel once on the first failing write/publish
  (`StandaloneFailurePolicy.on_fatal(origin="quality_writer")`: one structured log, one operator alert) and, once
  degraded, **never calls the failed writer again**: the event is WAL-protected first, then only accounted.
  Only a fatal the runner's own table maps to the quality stream is latched here, so an unrelated typed fatal is
  neither swallowed nor relabelled.
* `_record_adapter_unhandled` hands a typed fatal that is *not* the quality channel's to the failure policy
  (`origin="adapter_unhandled"`) before re-raising, because the adapter's catch-all would otherwise drop it.
* While degraded, the per-event `bybit_quality_event_wal_append_failed_in_persist` error log is replaced by the
  bounded accounting log (a full disk typically breaks the WAL and Parquet together).

**Degraded-channel counters** (`quality_channel_status()`, also `quality_degraded`), disjoint, counted for every
event that reaches the degraded channel including the one whose write tripped the latch:

| counter | meaning |
|---|---|
| `quality_events_wal_only` | WAL append returned: the record is established and the next start replays it into Parquet |
| `quality_events_wal_unconfirmed` | append raised after assigning an id (e.g. fsync failed): a record *may* exist; neither "retained" nor "lost" |
| `quality_events_lost` | no WAL record and no usable writer: gone. Bounded log (first, then powers of two) + one alert |

Verified against reality, not just against each other: the WAL on disk holds exactly the `wal_only` events,
none of them is checkpointed, and a restart on a healthy disk publishes each exactly once.

**Unchanged by design.** Raw-evidence fatal → exit 70 (also while quality is degraded); real order-book writer
fatal → isolate the `orderbook` route; unknown typed fatal → terminate; ordinary exception → P0-1 (a plain exception
from the quality writer on the book path still drops that frame's row; the adapter still swallows ordinary
exceptions from its sink).

**Remaining limitations.**

* While degraded, every event is still appended to the quality WAL and its seq kept in memory
  (`_quality_wal_inflight`). WAL size rotation only runs from the segment-published hook, which never fires for a
  dead writer, so a long degradation with a high event rate grows the WAL until restart. Same as USD-M.
* Events already buffered in the failed segment before the trip are covered by their in-flight seqs but are not
  counted in `quality_events_wal_only` (the counter is a lower bound).
* The counters are in-process: they restart at 0 with the process; replayed events are not re-counted.
* `quality_channel_status()` is exposed on the runner object; Bybit has no status printout or health endpoint
  wired to it yet.
* Not exercised against a real full disk or under systemd (fault injection at `os.replace` / WAL `fsync`).

## Not changed

F1 publication semantics, the P0-1 queue design, replay and the systemd unit.

## Not verified

* Live behaviour under a real disk-full / EIO condition and under systemd (`Restart=always` with
  the start limit) is **not tested**; tests inject faults at `os.replace` and check a real
  subprocess exit status of 70.
* The 1 s supervisor interval bounds detection latency for a quiet-writer failure; it is not
  measured under load.
* If the quality WAL itself is unwritable, a failure report falls back to the log line and the
  alert only.
