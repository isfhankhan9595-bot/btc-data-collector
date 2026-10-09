# F5 — Fatal storage topology / fail-closed routing

Scope: `collector.run_collector` (Binance USD-M), the only runner with a systemd unit. The typed
exception, the `ParquetWriter` latch and `RawCapture`'s opt-in flag are shared code; the other
venue runners (Bybit, OKX, Binance Spot) are **not** rewired and behave as before (see
*Not changed*).

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
* **Raw capture** — `RawCapture(fail_closed_on_fatal_storage=True)` (set only by the USD-M
  collector) re-raises a typed fatal from the raw writers *without consulting the quality sink*.
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

## Behaviour changes worth knowing

* `CollectorApp.handle_message` now *classifies* a typed fatal instead of letting it propagate:
  the first failing frame isolates the route and later frames are short-circuited, so six existing
  runner tests that expected a raise were converted (USD-M only; other runners still raise). The
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

Bybit/OKX/Binance Spot runners (their `RawCapture` stays fail-open; their writers raise the typed
fatal, which their own handlers treat as an ordinary error as before), F1 publication semantics,
the P0-1 queue design, replay, and the systemd unit.

## Not verified

* Live behaviour under a real disk-full / EIO condition and under systemd (`Restart=always` with
  the start limit) is **not tested**; tests inject faults at `os.replace` and check a real
  subprocess exit status of 70.
* The 1 s supervisor interval bounds detection latency for a quiet-writer failure; it is not
  measured under load.
* If the quality WAL itself is unwritable, a failure report falls back to the log line and the
  alert only.
