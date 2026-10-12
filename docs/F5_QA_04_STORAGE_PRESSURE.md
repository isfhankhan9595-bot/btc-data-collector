# F5-QA-04 — quality-WAL growth and continuous storage-pressure monitoring

Scope: the two WAL-backed runners only — Binance USD-M Futures (`run_collector.py`) and Bybit
(`run_bybit_collector.py`). Spot, OKX and OKX capture-only are unchanged. WAL format, schema,
event IDs, sequencing, checkpoint semantics and at-least-once delivery are unchanged.

## Problem (Phase A evidence, test results — not production rates)

`QualityEventWAL.max_bytes` (default 10,000,000) is a **per-file rotation threshold**, not a total
bound. Rotation runs only after a *successful* checkpoint. When a checkpoint write fails after the
segment was published, `_block_quality_checkpoint()` latches checkpointing off, the rotation path is
never reached again, and events keep being appended to the same WAL file. Before this change the
only free-space control, `DiskMonitor.check_disk_space()` (7/4/2 GiB), ran once in the Futures
constructor (where its shutdown callback hit a half-built app: `AttributeError: 'CollectorApp'
object has no attribute 'ws_clients'`) and Bybit had no disk monitoring at all.

Reproduction (real runners, real WAL and `ParquetWriter`, scratch dirs, `max_bytes=800`, checkpoint
`os.replace` failure injected after a successful publish):

| scenario | runner | events | WAL files | WAL bytes | rotations / WAL unlinks |
|---|---|---|---|---|---|
| healthy | Futures / Bybit | 67 | 1 / 1 | 396 / 633 | 13 / 13 and 16 / 16 |
| blocked | Futures / Bybit | 67 | 1 / 1 | 13,247 / 14,116 | 1 / 0 (rotation never reached again) |
| blocked | Futures / Bybit | 401 | 1 / 1 | 80,283 / 85,492 | 1 / 0 |

Growth was linear at roughly 200–213 bytes per record in these tests. After a restart the latch
cleared, the WAL replayed (duplicate Parquet rows, identical `quality_event_id`s — the documented
at-least-once behaviour) and the checkpoint caught up. These numbers are small-probe measurements
and are **not** a basis for a size limit.

## Policy

**There is no hard WAL size cap.** Nothing in this change deletes, truncates, rewrites or moves an
uncheckpointed WAL file, and the WAL is *not* bounded by `max_bytes` (`status()` reports
`wal_total_size_bounded: false`). A retention/cap policy would need its own explicit
data-preservation design.

What bounds the damage is a continuous free-space control for both runners, on the filesystem(s)
holding each runner's configured data directory and quality-WAL directory (worst free space wins):

| state | free space (defaults) | action |
|---|---|---|
| OK | >= 7 GiB | none |
| WARNING | < 7 GiB | one alert on entry; collection continues |
| CRITICAL | < 4 GiB | one alert on entry; collection continues |
| EMERGENCY | < 2 GiB | one alert, then the runner's **controlled shutdown**, exit status **1** |
| UNKNOWN | measurement failed | explicit monitor-error state; never treated as healthy; retried every interval; alert at most once per 900 s per process; **fails closed**: no ingestion until one valid measurement exists, and a run of consecutive failures lasting `COLLECTOR_DISK_UNKNOWN_GRACE_S` (60 s) triggers the controlled shutdown, exit status **69** (see below) |

* First sample: only the single highest applicable severity alerts.
* Hysteresis (default 0.5 GiB): a state is left only when free space rises that far above the
  threshold that defined it. Recovery is reported in `status()` (`last_transition`) and logged, not
  notified. Re-escalation after a genuine recovery alerts again.
* The poll runs every 15 s in a worker thread (`asyncio.to_thread`), never on the ingestion hot path.
  Alert, logger and callback failures cannot change state or stop future polls. An emergency callback
  that fails is retried each interval.
* Alerts include free/total GiB, the monitored path, quality-WAL bytes and file count (or "unavailable"),
  and the checkpoint latch state and reason.

### Configuration (environment; GiB = 1024^3 bytes)

| variable | default | meaning |
|---|---|---|
| `COLLECTOR_DISK_WARNING_GIB` | 7 | WARNING below this free space |
| `COLLECTOR_DISK_CRITICAL_GIB` | 4 | CRITICAL below this |
| `COLLECTOR_DISK_EMERGENCY_GIB` | 2 | EMERGENCY (controlled stop) below this |
| `COLLECTOR_DISK_CHECK_INTERVAL_S` | 15 | seconds between polls |
| `COLLECTOR_DISK_HYSTERESIS_GIB` | 0.5 | recovery margin above a threshold |
| `COLLECTOR_DISK_ERROR_ALERT_INTERVAL_S` | 900 | minimum seconds between monitor-error alerts |
| `COLLECTOR_DISK_UNKNOWN_GRACE_S` | 60 | seconds of CONSECUTIVE measurement failure tolerated (startup and runtime) before the controlled stop; finite and > 0 |

Invalid values are rejected at startup (`DiskPressureConfigError`), never coerced: warning > critical
> emergency >= 0, interval > 0, 0 <= hysteresis < the smallest gap between thresholds, grace > 0 (finite). Size the
thresholds for the host's disk; the defaults preserve the previous absolute policy and the capacity of
any particular host is not assumed.

## Shutdown and restart behaviour

* **Startup gate (fail closed).** After the app is fully constructed (so `shutdown()` is safe) and
  before any websocket client starts, `establish_measurement()` polls until ONE valid measurement
  exists. Each failed probe is UNKNOWN (never OK), is retried every `interval_s` and is bounded by a
  monotonic deadline of `COLLECTOR_DISK_UNKNOWN_GRACE_S`. A valid reading is classified normally:
  WARNING / CRITICAL start ingestion (with their one alert); EMERGENCY runs the normal shutdown and
  exits with status 1. If no valid reading is obtained inside the grace period, the runner runs the
  same normal shutdown without starting ingestion and exits with status **69**. Startup retries reuse
  the one monitor object: no extra monitor, task, client or shutdown, and the error alert stays
  rate-limited. Consequence: startup can be delayed by up to the grace period when the probe is
  failing.
* **Runtime measurement failure.** A run of consecutive failures (timer starts at the first failure,
  monotonic clock) shorter than the grace period never stops collection: status shows UNKNOWN, the
  last successful classification is kept only as `last_classified_severity`, the first failure alerts
  (then at most once per 900 s), and a success resets the timer. When the run reaches the grace
  period the monitor requests the controlled shutdown exactly once (Futures: `_async_shutdown`;
  Bybit: the supervisor stop event) and the runner latches "monitor unavailable" separately from
  "disk emergency". A success after that request does not cancel it (no flapping out of a committed
  stop). A real low-space reading is independent of monitor availability and follows the severity
  policy; WARNING / CRITICAL never stop collection.
* **Runtime EMERGENCY.** Futures: a dedicated task schedules `_async_shutdown` (the same path as a
  SIGTERM / fatal-storage stop). Bybit: the monitor sets the supervisor's own stop event; the
  unchanged `supervise_standalone_runner` performs the controlled shutdown and `_run_supervised`
  maps the result to the disk status. `DiskMonitor` no longer calls `sys.exit` or any shutdown callback.
* **Exit statuses** (existing: 0 clean, 1 run task crashed, 70 fatal raw-evidence storage):

  | status | meaning | note |
  |---|---|---|
  | 70 | fatal raw-evidence storage failure | highest priority, unchanged |
  | 1 | confirmed disk-pressure EMERGENCY | also numerically the standalone "run task crashed" status; logs / alerts / `shutdown_reason: disk_emergency` distinguish them |
  | 69 | storage could not be measured for the grace period | new; sysexits `EX_UNAVAILABLE`, unused elsewhere in the repo; `shutdown_reason: disk_monitor_unavailable`; NOT a confirmed low-disk emergency |

  Precedence when several are latched: 70 > 1 > 69. Futures `main()` returns `app.exit_code`; Bybit's
  `_run_supervised` returns the supervisor's non-zero status unchanged and maps a clean stop to
  `app.exit_code`. No `sys.exit` / `os._exit` is called from the monitor.
* **Futures under systemd (template unchanged).** `Restart=always`, `RestartSec=5`,
  `StartLimitBurst=5` per 300 s restart any exit, including 1 and 69. Under a persistent EMERGENCY each
  restart fails at once at the startup gate, so the start limit is reached quickly (inference from the
  unit settings, **not verified on a real host**). Under a persistently failing probe each cycle takes
  about grace + RestartSec = 65 s, so only about 4–5 starts fall inside any 300 s window and the start
  limit may never trip: by inference the unit would keep cycling roughly every 65 s, never ingesting,
  and each new process alerts again (the 900 s alert rate limit is per process). This is a known
  residual risk, not a verified behaviour. A restart with free space above the threshold and a working
  probe replays the uncheckpointed quality events (at-least-once) and, if the earlier checkpoint fault
  was transient, catches the checkpoint up.
* **Bybit.** There is no systemd unit for this runner. After an emergency stop it stays stopped unless
  an external supervisor is configured.

## Diagnostics

`storage_pressure_status()` on each runner (read-only, separate from `quality_channel_status()` and
its lost / unconfirmed / WAL-only counters): `severity` (UNKNOWN while the latest measurement is
failing), `last_classified_severity` (the last successful classification, diagnostics only),
`free_bytes` / `total_bytes` (`None`, never 0, when the measurement failed), `quality_wal_bytes`,
`quality_wal_file_count` (`None` when unreadable), `quality_checkpoint_blocked` + reason,
`last_monitor_error`, `consecutive_monitor_errors`, `monitor_failure_duration_s` (monotonic; `None`
when not failing), `monitor_error_alerts`, `monitor_unavailable_latched`, `shutdown_reason`
(`disk_emergency` / `disk_monitor_unavailable` / `None`), `last_transition`, `alerts_sent`, thresholds,
`wal_total_size_bounded: false`.

## Not validated

Real ENOSPC, real power loss and a live systemd unit were not exercised; tests inject the free-space
reading (including probe failures and a fake monotonic clock) and checkpoint `os.replace` failures on
scratch directories. The restart cadence under systemd for exit status 69 is inferred, not observed.
There is still no hard WAL cap.
