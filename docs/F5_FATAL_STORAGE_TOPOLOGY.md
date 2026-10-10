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
  degraded, `_persist_quality_event` stops calling the failed writer (WAL copy only; counted in
  `_quality_events_wal_only`).
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

## Not changed

F1 publication semantics, the P0-1 queue design, replay, the systemd unit, and `WebSocketClient`
itself (including the fact that `start()` sets `running = True` unconditionally: calling it again on a
client that is already in discard mode would reconnect. Nothing does: the application starts it once and
the supervisor cancels, never restarts, the task. This is observed, not changed, and not covered by a
guard test).

## Not verified

* Live behaviour under a real disk-full / EIO condition and under systemd (`Restart=always` with
  the start limit) is **not tested**; tests inject faults at `os.replace` and check a real
  subprocess exit status of 70.
* The 1 s supervisor interval bounds detection latency for a quiet-writer failure; it is not
  measured under load.
* If the quality WAL itself is unwritable, a failure report falls back to the log line and the
  alert only.
