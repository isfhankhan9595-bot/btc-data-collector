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
| UNKNOWN | measurement failed | explicit monitor-error state; alert at most once per 900 s; retried every interval; never treated as healthy; never stops collection by itself |

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

Invalid values are rejected at startup (`DiskPressureConfigError`), never coerced: warning > critical
> emergency >= 0, interval > 0, 0 <= hysteresis < the smallest gap between thresholds. Size the
thresholds for the host's disk; the defaults preserve the previous absolute policy and the capacity of
any particular host is not assumed.

## Shutdown and restart behaviour

* **Startup preflight.** After the app is fully constructed (so `shutdown()` is safe) and before any
  websocket client starts, one sample is taken. At EMERGENCY the runner alerts once, runs its normal
  shutdown (writers closed, quality writer last, WAL closed last) and exits with status 1. A failed
  measurement does not block startup.
* **Runtime EMERGENCY.** Futures: a dedicated task schedules `_async_shutdown` (the same path as a
  SIGTERM / fatal-storage stop). Bybit: the monitor sets the supervisor's own stop event; the
  unchanged `supervise_standalone_runner` performs the controlled shutdown and `_run_supervised`
  maps the result to status 1. `DiskMonitor` no longer calls `sys.exit` or any shutdown callback.
* **Exit statuses.** 1 = disk-pressure emergency (also numerically the standalone "run task crashed"
  status; the log line and alert distinguish them). 70 = fatal raw-evidence storage failure and keeps
  priority if both occur. 0 = clean.
* **Futures under systemd (template unchanged).** `Restart=always`, `RestartSec=5`,
  `StartLimitBurst=5` per 300 s restart any exit, including 1. While free space stays below the
  emergency threshold each restart fails at the startup preflight, so the unit stops restarting after
  the start limit is reached (by inference from the unit settings; **not verified on a real host**).
  A restart with free space above the threshold replays the uncheckpointed quality events
  (at-least-once) and, if the earlier checkpoint fault was transient, catches the checkpoint up.
* **Bybit.** There is no systemd unit for this runner. After an emergency stop it stays stopped unless
  an external supervisor is configured.

## Diagnostics

`storage_pressure_status()` on each runner (read-only, separate from `quality_channel_status()` and
its lost / unconfirmed / WAL-only counters): `severity`, `free_bytes` / `total_bytes` (`None`, never
0, when the measurement failed), `quality_wal_bytes`, `quality_wal_file_count` (`None` when
unreadable), `quality_checkpoint_blocked` + reason, `last_monitor_error`, `last_transition`,
`alerts_sent`, thresholds, `wal_total_size_bounded: false`.

## Not validated

Real ENOSPC, real power loss and a live systemd unit were not exercised; tests inject the free-space
reading and checkpoint `os.replace` failures on scratch directories.
