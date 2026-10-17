# Deployment (systemd on EC2)

Unit: `collector/btc-collector.service`. The only deployment file in the repo.

## Verification status

| Claim | Status |
|---|---|
| Unit syntax and directive validity | Checked offline (`systemd-analyze verify` on a path-remapped copy, plus static tests) |
| Paths/command match the repo's runner contract (`python -m collector.run_collector` from repo root) | Checked offline against source |
| EC2 user is `ec2-user`, repo at `/home/ec2-user/btc-data-collector`, venv at `.venv` | **NOT VERIFIED** -- assumption from project context; no live host was accessible |
| Service actually starts, restarts, and stops cleanly on EC2 | **NOT VERIFIED** |

## Install (on the host, after checking `id` and the clone path)

```
cd /home/ec2-user/btc-data-collector
python3 -m venv .venv && .venv/bin/pip install -r collector/requirements.txt
sudo install -m 0600 -o root -g root /dev/null /etc/btc-collector.env   # add TELEGRAM_BOT_TOKEN=... / TELEGRAM_CHAT_ID=... if wanted
sudo cp collector/btc-collector.service /etc/systemd/system/
sudo systemd-analyze verify /etc/systemd/system/btc-collector.service
sudo systemctl daemon-reload && sudo systemctl enable --now btc-collector
```

If the user or path differs, edit `User`, `Group`, `WorkingDirectory` and `ExecStart` together.
Use `sudo chown -R ec2-user:ec2-user` on the repo so `data/` and `logs/` are writable.

Operate: `systemctl status|stop|restart btc-collector`, `journalctl -u btc-collector -f`.

## Secrets

Not in the unit. `EnvironmentFile=/etc/btc-collector.env` (root-owned, 0600) supplies
`TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. The previous unit set
`TELEGRAM_BOT_TOKEN=YOUR_BOT_TOKEN`, which the notifier would have read as a real token.

## Failure behaviour

- Missing/unreadable env file, bad WorkingDirectory/ExecStart/user: start fails immediately.
- Repeated failure: after 5 starts in 300 s the unit enters `failed` (no infinite crash loop).
- Stop: SIGTERM, up to 60 s (must exceed the 30 s websocket queue drain).
- **F5 terminal storage failure:** if the `raw_wire` or `raw_rest` writer fails (raw evidence is
  irrecoverable), the collector stops ingesting, runs the normal shutdown (closing the healthy
  writers) and exits with status **70**. Under the existing `Restart=always` that is a failure exit
  and is restarted after `RestartSec`; F1/P0-4 startup reconciliation then recovers the stream. It
  counts toward `StartLimitBurst` like any other failed start. A *derived* writer failure does not
  exit: only that route is isolated and the operator is alerted. The unit file is unchanged;
  behaviour under a real systemd/disk fault is not verified live. See `F5_FATAL_STORAGE_TOPOLOGY.md`.
- **F5-QA-04 disk pressure:** both WAL-backed runners monitor free space continuously (defaults warn at
  7 GiB, critical at 4 GiB, controlled stop with exit status **1** below 2 GiB; configurable via
  `COLLECTOR_DISK_*`). **UNKNOWN fails closed:** ingestion does not start until one valid measurement
  exists, and storage that cannot be measured for `COLLECTOR_DISK_UNKNOWN_GRACE_S` (default 60 s,
  consecutive) ends in the controlled shutdown with exit status **69** (distinct from the disk emergency
  1; **70** keeps priority: 70 > 1 > 69). There is no hard WAL size cap.
- **Restart policy for exit 69:** the unit sets `RestartPreventExitStatus=69` (only 69) next to the
  unchanged `Restart=always` / `RestartSec=5` / `StartLimitBurst=5` per 300 s. A persistently unmeasurable
  disk would otherwise cycle about every 65 s (60 s grace + 5 s), too slowly for the start limit to ever
  trip. After exit 69 the unit stays **`failed`** for an operator: fix the monitor / filesystem problem,
  then `sudo systemctl restart btc-collector` (add `systemctl reset-failed btc-collector` only if status
  shows `start-limit-hit`). Exit 1 and 70 still restart as before. This behaviour follows systemd's
  documentation and `systemd-analyze verify`; it is **not verified on a real host**.
- **Bybit** has no repository-managed unit: after either disk stop it stays stopped unless an external
  supervisor is configured, and what such a supervisor would do is UNKNOWN. See
  `F5_QA_04_STORAGE_PRESSURE.md`.

## Runner coverage (repository evidence, P0-12 finalization)

Only `collector.run_collector` (Binance USD-M) has a systemd unit. `run_bybit_collector.py`,
`run_okx_collector.py` and `run_binance_spot_collector.py` exist in the repository but have
no deployment artifact. This is scope, not a limitation of this unit: nothing in the repo
indicates one unit should cover every runner, and no second unit was added speculatively.
Add one per runner, following this file's pattern, if/when those need deployment.
All of them now exit `70` on a terminal (raw-evidence) storage failure and `1` if the application
task crashed of an ordinary exception, so any supervisor that restarts on non-zero will treat both
as failures; see `F5_FATAL_STORAGE_TOPOLOGY.md`.

## Shutdown contract (traced from source and exercised with fake sockets -- NOT VERIFIED LIVE)

```
SIGTERM -> CollectorApp._async_shutdown()
  -> running = False; recovery latch set (no new REST producer can start)
  -> cancel every REST producer (startup recovery, recovery task, OI poller); wait at most
       WS_FALLBACK_SLACK_S (1 s) for the recovery task. Whether any producer ENDED is judged
       later from its task state by shutdown(), never from a wait having returned
  -> drain integrity quality events, await the quality-queue task
  -> _stop_ws_clients(): client.stop() for each client, then wait (NOT cancel) for each
       client's start() tail: receive loop ended -> ingest queue drained -> worker retired
  -> shutdown(): every writer closed, quality writer last, then the WAL
```

If a client is not idle after its own bound, the runner hard-cancels the tasks, waits once
more, and then **fails closed**: the exit status is non-zero, the market-data writers, the
quality writer, the quality queue and the WAL are left untouched (the WAL is the recovery
source), and each raw writer is closed only when its own producers are stopped:
`raw_wire_writer` <- receive loop only (ingress stopped); `raw_rest_writer` <- recovery task
and OI poller (stopped, and none can start once shutdown began).

### A REST producer that ignores cancellation

Traced from source and exercised with a coroutine that swallows every `CancelledError`, including
in a child process running the real `main()` under a real SIGTERM
(`tests/test_ws_shutdown_runner_exit.py`, `tests/test_ws_shutdown_late_producer.py`,
`tests/test_ws_shutdown_startup_recovery.py`) -- NOT VERIFIED LIVE.

What the application controls:

- No REST producer starts once shutdown began. The latch is set before anything is cancelled, and
  the startup recovery, `_schedule_recovery` and the OI poller start are all gated by it.
- Every tracked producer (startup recovery, recovery task, OI poller) is sent a cancel.
- A producer that is still running keeps exactly the storage it can reach open
  (`REST_PRODUCER_REACH`): raw REST capture, reconstructed book rows (recovery) or `oi_writer`
  (poller), and the quality writer + WAL. Everything else is finalised. Nothing is ever closed
  beneath a live producer, including by a later `shutdown()` call.
- A non-zero status (`rest_producer_not_stopped`, 1) is latched in `CollectorApp.exit_code`.
- Every exit from `start()` after the startup preflight reaches the same finalization: a normal
  return, an exception from the startup recovery or any task (it is re-raised afterwards, so
  `main()` still fails with its traceback), and the caller cancelling `start()` (finalization runs,
  then the cancellation propagates). The wait for a recovery that ignores a cancel of `start()` is
  bounded by `WS_FALLBACK_SLACK_S`.
- If a producer that outlived `shutdown()` ends later, a done-callback retries `shutdown()`: it
  drains the quality queue, closes the remaining writers once each, then `quality_writer`, then the
  WAL. A late close is not a clean shutdown: the latched status stays non-zero.
- The quality persistence loop is waited for within fixed bounds, never without a deadline
  (`_stop_quality_consumer`, used by `start()`'s finalization and by `_async_shutdown`): up to
  `QUALITY_DRAIN_BOUND_S` (1 s) for it to hand every queued event to the quality writer, up to
  `QUALITY_CONSUMER_STOP_BOUND_S` (0.5 s) for it to leave on its own, then it is told to leave and
  cancelled (`WS_FALLBACK_SLACK_S`, 1 s). A timeout is logged (`quality_queue_drain_incomplete`), never
  read as a completed drain, and loses no event: every queued event already has a WAL record that holds
  the checkpoint back, `shutdown()` drains what is left synchronously once no websocket client can still
  enqueue, and anything not drained stays in the WAL for the next start. A loop that still has not ended
  is a user of the quality writer like a REST producer: `shutdown()` keeps that writer and the WAL open,
  closes everything else, latches `quality_consumer_not_stopped` (status 1) and the late retry finalizes
  both once the loop ends. The loop's own exception is logged and never replaces the caller's
  cancellation or the original exception of `start()`.

What it does not control:

- Cancellation is a request. A coroutine that swallows it cannot be stopped by the application.
- While such a task exists the process does not return the latched status. With a startup
  recovery under SIGTERM, `start()` stays parked in `_run_startup_recovery`; with the OI poller,
  `start()` returns after its own bounded waits, but `asyncio.run` then waits for the task in its
  teardown. Either way `main()` returns only once the task ends.
- A producer that never ends is ended only by process-level termination: systemd's SIGKILL at
  `TimeoutStopSec`, which reports the kill, not the latched status. Its reachable writers and the
  quality WAL are then never finalised by this process (the WAL stays the recovery source for
  quality events). Nothing in the code guarantees that shutdown completes within 60 s; the bounds in
  the next table cover the websocket part only.
- The late retry only runs while the event loop does. If the process is killed first, nothing runs.
  It does not retire a websocket worker: it cannot make a worker that ignores cancellation end. It
  only reacts when one ends. Each client's `start()` task and live ingest worker carry the same
  done-callback, which retries `shutdown()` only once every websocket client is idle
  (`lifecycle_idle`), every REST producer has ended and the quality loop has ended. A late close after
  `ws_client_worker_not_retired` keeps that failure latched (non-zero status): it is not a clean shutdown.

### Stop budget vs `TimeoutStopSec=60`

| Step | Bound | Source |
|---|---|---|
| opening handshake in progress when stop lands | 0 (cancelled by `stop()`) | `_connect_or_stop` |
| close handshake + subscribe/quality-event slack | 10 + 2 s | websockets `close_timeout` default (pinned by a test), `CONNECTION_PHASE_SLACK_S` |
| ingest-queue drain | 30 s | `shutdown_drain_timeout_s` |
| worker retirement | 5 s | `worker_retire_timeout_s` |
| **client graceful bound** (`shutdown_deadline_s`) | **47 s** | sum of the above |
| hard-cancel fallback, only if the graceful bound was exceeded | +6 s | `worker_retire_timeout_s + WS_FALLBACK_SLACK_S` |
| **bounded websocket part, worst case** | **53 s** | |
| left for everything else | **7 s** (`FINALIZATION_RESERVE_S`) | |

The "everything else" is NOT bounded by code and has not been measured: finalization (flush,
fsync, rename, directory fsync) of about ten Parquet writers, the WAL close and interpreter
teardown. The one wait inside it that has a code bound is the stop of the quality persistence
loop: at most `QUALITY_DRAIN_BOUND_S` + `QUALITY_CONSUMER_STOP_BOUND_S` + `WS_FALLBACK_SLACK_S`
= 2.5 s, reached only when the loop is stalled, dead or outpaced (normally milliseconds), and
chosen, not measured on the host. It is spent BEFORE the websocket stop, so it comes out of the
7 s reserve. A full-queue drain through a slow `fsync` can exceed `QUALITY_DRAIN_BOUND_S`
(1000 queued events took about 2.3 s on the development sandbox, two segment publishes; that is
not host data): the drain then finishes in `shutdown()`'s synchronous pass instead, with the same
total work. 7 s is the arithmetic remainder, not a demonstrated
sufficient margin. A test fails if any client timeout is raised without raising
`TimeoutStopSec`. Measure writer finalization on the real host before relying on it; if it
can exceed ~7 s, lower `shutdown_drain_timeout_s` (30 s -> 20 s buys 10 s) rather than
raising `TimeoutStopSec`.

The standalone runners (Spot, Bybit, OKX) have no systemd unit in this repository, so no
stop budget is defined for them. Their wait for the client is the same 47 s bound.

### Quality queue: verified limitations

Reproduced with `tests/test_quality_queue_shutdown_bounds.py` (real `start()` / `_async_shutdown()` /
`shutdown()`, only the network edge and the websocket clients faked) -- not verified on a live host,
against a live exchange, under power loss, or within the 60 s stop budget.

- Only websocket-client events use the queue (`_websocket_quality_event`). Events from REST producers
  and handlers are persisted synchronously and never queued.
- `task_done()` means the loop handled the event (handed it to the quality writer), not that it is in a
  published segment: publication happens at 500 rows, 30 s idle or writer close, and the WAL checkpoint
  advances only then. "Drained" is therefore not "durable".
- A websocket worker that is still alive when `shutdown()` runs (`ws_client_worker_not_retired`) leaves the
  queue undrained and the quality writer and WAL open. The events are WAL-retained and replayed at the
  next start. If that worker ends later while the process is still running, the late retry (see above)
  finalises the held writers and then the WAL; if it never ends, they stay open until process-level
  termination, which cannot guarantee that buffered Parquet segments are finalised. Not changed or
  tested here: the standalone Spot/Bybit/OKX runners, which have their own `shutdown()` and no
  late-finalization callback for a worker that ends after it returned.
- A quality-writer failure that the loop survives (`quality_checkpoint_blocked`) does not change the exit
  status: the events stay in the WAL (replayed, possibly as duplicate rows with the same
  `quality_event_id`). The operator signal is the alert and `quality_channel_status()`.
- If the loop is cancelled or dies, the `quality_queue_overflow:total=...` summary row it writes on exit is
  not written. The overflowed events themselves were each persisted directly (or WAL-retained); only the
  aggregate record is skipped.
- Nothing can bound a loop that blocks the event loop itself (a synchronous call that never returns):
  every deadline here is an event-loop timer.

## Telegram credentials

`telegram_bot.py` reads `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` from the environment only;
there is no source-code default. Telegram stays optional: unset or malformed values disable it
and the collector runs normally. An explicit request (`load_telegram_config(validate=True)`,
`send_test_telegram_alert()`) raises `TelegramConfigError` instead.

**ROTATION REQUIRED.** A bot token and chat id were previously committed to `telegram_bot.py`
and remain in Git history. Treat that token as compromised: revoke it via BotFather, issue a new
one, and supply it only through `/etc/btc-collector.env` on the host. Never commit it.
