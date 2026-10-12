# F5-QA-04 — quality-WAL growth and continuous storage-pressure monitoring

Scope: the two WAL-backed runners only — Binance USD-M Futures (`run_collector.py`) and Bybit
(`run_bybit_collector.py`). Spot, OKX and OKX capture-only are unchanged. The WAL row shape,
sequence/checkpoint contract and at-least-once replay semantics remain intact; the WAL now owns and
protects its `seq` / `quality_event_id` fields against caller override. New cap-refused events are
force-published to Parquet without WAL IDs rather than being misrepresented as WAL-retained.

## Problem (Phase A evidence, test results — not production rates)

`QualityEventWAL.max_bytes` (default 10,000,000) is a **per-file rotation threshold**, not a total
bound. Rotation runs only after a *successful* checkpoint. When a checkpoint write fails after the
segment was published, `_block_quality_checkpoint()` latches checkpointing off, the rotation path is
never reached again, and events keep being appended to the same WAL file. Before this change the
only free-space control, `DiskMonitor.check_disk_space()` (7/4/2 GiB), ran once in the Futures
constructor (where its shutdown callback hit a half-built app: `AttributeError: 'CollectorApp'
object has no attribute 'ws_clients'`) and Bybit had no disk monitoring at all.

Original uncapped reproduction (real runners, real WAL and `ParquetWriter`, scratch dirs,
`max_bytes=800`, checkpoint `os.replace` failure injected after a successful publish):

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

**A hard aggregate WAL admission cap is now implemented.** The default limit is 100,000,000 bytes
(decimal 100 MB), configured by `COLLECTOR_QUALITY_WAL_MAX_TOTAL_BYTES` as a positive base-10
integer. The value is intentionally independent of the per-file rotation threshold (`max_bytes`).
Every append sums all `.wal` file sizes in the WAL namespace and measures the candidate's UTF-8
JSONL bytes including newline. If it cannot prove the candidate will fit, the append is refused
before writing; admission latches closed until restart. A startup WAL already at/over the limit or
an unmeasurable aggregate also latches admission closed. Invalid capacity configuration fails
startup; it is never coerced.

**No WAL evidence is deleted, truncated, rewritten, or moved to meet the limit.** Existing
uncheckpointed records stay intact. Rejected events do not receive a WAL ID and do not enter the
WAL-backed queue. Both runners use the existing direct fallback to synchronously write and
force-publish quality Parquet. Those rows have a null `quality_event_id`; they count as durably
published only when that publication succeeds. If the WAL is capped and the quality Parquet writer
has failed too, existing lost/unconfirmed accounting applies: no durability is invented. The quality
channel reports `quality_wal_capacity_exhausted`, reason and configured limit; storage status
reports aggregate bytes, file count, limit and `wal_total_size_bounded` only when the observed
aggregate is at or below that limit.

The cap protects one correctly single-writer WAL directory, not total dataset or Parquet storage and
not total inode count. Sustained cap mode can cause immediate quality-Parquet publication and thus
many small segments; continuous free-space monitoring remains required. The independent disk
pressure policy is unchanged:

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
* **Futures under systemd.** The unit keeps `Restart=always`, `RestartSec=5`, `StartLimitIntervalSec=300`,
  `StartLimitBurst=5` and adds **`RestartPreventExitStatus=69`** (only 69).
  * *Why (arithmetic on systemd's documented start-limit semantics, not measured on a host):* containment
    needs a 6th start inside the 300 s window, i.e. a restart cycle <= 300 / 5 = 60 s. A confirmed
    emergency (exit 1) exits within seconds, so its cycle is about 7 s and the 6th start is refused at
    roughly 35 s: the start limit contains it, unchanged. A persistently unmeasurable disk (exit 69)
    spends the whole 60 s grace before exiting, so its cycle is at least 60 + 5 = 65 s (startup and
    shutdown only lengthen it): starts land at 0, 65, 130, 195, 260, 325 s and the 6th falls outside the
    window, so the limit never trips (fixed and sliding window models agree). Without the directive the
    unit would cycle forever without ingesting and re-alert on every process. The 60 s grace was not
    changed to hide this.
  * *Behaviour (by systemd's documentation; **not verified on a real host**):* after exit 69 the unit is
    left **`failed`** (`status=69` in `systemctl status`) and is not restarted automatically. Exit 1
    (confirmed disk emergency) and 70 (fatal storage) are not prevented and restart as before, still
    protected by the start limit. Every other exit still restarts.
  * *Operator action:* fix the monitor / filesystem problem (probe errors are in the log and the alert's
    `last error`), then run `sudo systemctl restart btc-collector`. A restart replays any uncheckpointed
    quality events (at-least-once); nothing was deleted. If `systemctl status` ever reports
    `start-limit-hit` (a different failure), `sudo systemctl reset-failed btc-collector` is additionally
    required (standard systemd behaviour, also unverified for this unit).
  * A restart with free space above the threshold and a working probe resumes normally and, if the earlier
    checkpoint fault was transient, catches the checkpoint up.
* **Bybit.** There is no systemd unit for this runner. After an emergency stop it stays stopped unless
  an external supervisor is configured.

## Diagnostics

`storage_pressure_status()` on each runner (read-only, separate from `quality_channel_status()` and
its lost / unconfirmed / WAL-only counters): `severity` (UNKNOWN while the latest measurement is
failing), `last_classified_severity`, `free_bytes` / `total_bytes` (`None`, never 0, when measurement
failed), `quality_wal_bytes`, `quality_wal_file_count` (`None` when unreadable),
`quality_wal_max_bytes`, `wal_total_size_bounded` (true only when measured bytes are at/below the
known cap), `quality_checkpoint_blocked` + reason, `last_monitor_error`,
`consecutive_monitor_errors`, `monitor_failure_duration_s`, `monitor_error_alerts`,
`monitor_unavailable_latched`, `shutdown_reason`, `last_transition`, `alerts_sent`, thresholds.
The quality-channel status separately reports capacity-latched state and reason.

## Not validated

Real ENOSPC, real power loss and a live systemd unit were not exercised; tests inject the free-space
reading (including probe failures and a fake monotonic clock) and checkpoint `os.replace` failures on
scratch directories. The systemd behaviour for exit statuses 1 and 69 (start-limit containment, the `failed` state, restart-prevention) is derived from the unit text, `systemd-analyze verify` and arithmetic, not observed on a live host. Bybit has no repository-managed unit; whether any external supervisor restarts it, and how, is UNKNOWN.
A per-file rotation threshold and an aggregate admission cap both exist now; the cap is not a total
dataset-space/inode bound and does not make direct Parquet fallback immune to ENOSPC.
