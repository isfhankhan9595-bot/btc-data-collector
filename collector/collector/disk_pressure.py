"""F5-QA-04: continuous, read-only storage-pressure monitoring for the WAL-backed runners.

Why this exists
---------------
``QualityEventWAL.max_bytes`` is a per-file rotation threshold, never a total-size bound.
While a quality checkpoint is latched blocked, WAL files are neither rotated nor deleted
and the quality WAL can keep growing; ``DiskMonitor.check_disk_space`` used to run once, in
the Futures constructor only, and Bybit had no disk monitoring at all. This module is the
continuous free-space control for BOTH WAL-backed runners (Binance USD-M Futures and Bybit).

What it does NOT do
-------------------
* It never deletes, truncates, rewrites or moves a WAL file or any other evidence. There is
  NO hard WAL size cap and nothing here claims the WAL is bounded; ``status()`` says so
  explicitly (``wal_total_size_bounded: False``). A retention policy would be a separate,
  explicit data-preservation design.
* It never terminates a market-data route at WARNING / CRITICAL. Only EMERGENCY asks the
  runner for its own controlled shutdown, and this module never calls ``sys.exit`` or
  touches a writer: the runner's callback decides how to stop.
* It never treats a failed measurement as healthy or as zero bytes.

Severity state machine
----------------------
Free space is classified against three configurable thresholds (defaults preserve the
pre-existing 7 / 4 / 2 GiB policy)::

    free >= warning                  -> OK
    critical  <= free < warning      -> WARNING
    emergency <= free < critical     -> CRITICAL
    free < emergency                 -> EMERGENCY

* One notification per ENTRY into a more severe state (escalation alerts for the new
  severity). Staying in a state never alerts again, however many polls observe it.
* The first sample alerts only for the single highest applicable severity.
* Recovery uses hysteresis: a state is left only when free space rises ``hysteresis_gib``
  ABOVE the threshold that defined it, so noise around a threshold cannot alert-storm.
  Recovery is logged and visible in ``status()`` (``last_transition``); it is not a
  notification.
* A failed measurement is an explicit UNKNOWN / monitor-error state: it keeps the last
  classification for hysteresis, is reported as ``severity: UNKNOWN``, alerts at most once
  per ``error_alert_interval_s`` and is retried on the next poll.

Configuration (environment, GiB = 1024**3 bytes, seconds for intervals)
------------------------------------------------------------------------
``COLLECTOR_DISK_WARNING_GIB`` (7), ``COLLECTOR_DISK_CRITICAL_GIB`` (4),
``COLLECTOR_DISK_EMERGENCY_GIB`` (2), ``COLLECTOR_DISK_CHECK_INTERVAL_S`` (15),
``COLLECTOR_DISK_HYSTERESIS_GIB`` (0.5), ``COLLECTOR_DISK_ERROR_ALERT_INTERVAL_S`` (900).
Invalid values are REJECTED at startup (``DiskPressureConfigError``), never coerced:
``warning > critical > emergency >= 0``, ``interval > 0``, ``0 <= hysteresis < min(gap)``.
"""
from __future__ import annotations

import asyncio
import enum
import math
import os
import shutil
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple

from .notifications import send_alert
from .utils import logger

__all__ = [
    "GIB", "EXIT_DISK_PRESSURE", "Severity", "DiskPressureConfig", "DiskPressureConfigError",
    "PollResult", "StoragePressureMonitor", "classify", "wal_dir_stats",
    "ENV_WARNING_GIB", "ENV_CRITICAL_GIB", "ENV_EMERGENCY_GIB", "ENV_INTERVAL_S",
    "ENV_HYSTERESIS_GIB", "ENV_ERROR_ALERT_INTERVAL_S",
]

GIB = 1024 ** 3

#: Process exit status of a disk-pressure EMERGENCY stop. Deliberately NOT 70, which stays
#: reserved for the fatal raw-evidence storage failure (``failure_topology.EXIT_FATAL_STORAGE``).
#: Note it numerically equals the standalone "run task crashed" status; the log line and
#: alert distinguish the two.
EXIT_DISK_PRESSURE = 1

ENV_WARNING_GIB = "COLLECTOR_DISK_WARNING_GIB"
ENV_CRITICAL_GIB = "COLLECTOR_DISK_CRITICAL_GIB"
ENV_EMERGENCY_GIB = "COLLECTOR_DISK_EMERGENCY_GIB"
ENV_INTERVAL_S = "COLLECTOR_DISK_CHECK_INTERVAL_S"
ENV_HYSTERESIS_GIB = "COLLECTOR_DISK_HYSTERESIS_GIB"
ENV_ERROR_ALERT_INTERVAL_S = "COLLECTOR_DISK_ERROR_ALERT_INTERVAL_S"


class DiskPressureConfigError(ValueError):
    """The storage-pressure configuration is invalid. Startup must fail, not coerce."""


class Severity(str, enum.Enum):
    OK = "OK"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    EMERGENCY = "EMERGENCY"
    UNKNOWN = "UNKNOWN"


_RANK = {Severity.OK: 0, Severity.WARNING: 1, Severity.CRITICAL: 2, Severity.EMERGENCY: 3}


def _finite(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DiskPressureConfigError(f"{name} must be a number, got {value!r}")
    value = float(value)
    if not math.isfinite(value):
        raise DiskPressureConfigError(f"{name} must be finite, got {value!r}")
    return value


@dataclass(frozen=True)
class DiskPressureConfig:
    warning_gib: float = 7.0
    critical_gib: float = 4.0
    emergency_gib: float = 2.0
    interval_s: float = 15.0
    hysteresis_gib: float = 0.5
    error_alert_interval_s: float = 900.0

    def __post_init__(self) -> None:
        warning = _finite(ENV_WARNING_GIB, self.warning_gib)
        critical = _finite(ENV_CRITICAL_GIB, self.critical_gib)
        emergency = _finite(ENV_EMERGENCY_GIB, self.emergency_gib)
        interval = _finite(ENV_INTERVAL_S, self.interval_s)
        hysteresis = _finite(ENV_HYSTERESIS_GIB, self.hysteresis_gib)
        error_interval = _finite(ENV_ERROR_ALERT_INTERVAL_S, self.error_alert_interval_s)
        if not (warning > critical > emergency >= 0):
            raise DiskPressureConfigError(
                "thresholds must satisfy warning > critical > emergency >= 0 (GiB); got "
                f"warning={warning}, critical={critical}, emergency={emergency}")
        if interval <= 0:
            raise DiskPressureConfigError(f"{ENV_INTERVAL_S} must be > 0 seconds, got {interval}")
        if error_interval <= 0:
            raise DiskPressureConfigError(
                f"{ENV_ERROR_ALERT_INTERVAL_S} must be > 0 seconds, got {error_interval}")
        smallest_gap = min(warning - critical, critical - emergency)
        if hysteresis < 0 or hysteresis >= smallest_gap:
            raise DiskPressureConfigError(
                f"{ENV_HYSTERESIS_GIB} must satisfy 0 <= hysteresis < the smallest gap between "
                f"adjacent thresholds ({smallest_gap} GiB), got {hysteresis}")

    @classmethod
    def from_env(cls, environ: Optional[Mapping[str, str]] = None) -> "DiskPressureConfig":
        """Build from the environment; a present-but-invalid value is an error."""
        env = os.environ if environ is None else environ
        names = {"warning_gib": ENV_WARNING_GIB, "critical_gib": ENV_CRITICAL_GIB,
                 "emergency_gib": ENV_EMERGENCY_GIB, "interval_s": ENV_INTERVAL_S,
                 "hysteresis_gib": ENV_HYSTERESIS_GIB,
                 "error_alert_interval_s": ENV_ERROR_ALERT_INTERVAL_S}
        values = {}
        for field_name, var in names.items():
            if var not in env:
                continue
            raw = env[var]
            try:
                values[field_name] = float(raw)
            except (TypeError, ValueError):
                raise DiskPressureConfigError(f"{var} is not a number: {raw!r}") from None
        return cls(**values)

    def as_dict(self) -> dict:
        return {"warning_gib": self.warning_gib, "critical_gib": self.critical_gib,
                "emergency_gib": self.emergency_gib, "interval_s": self.interval_s,
                "hysteresis_gib": self.hysteresis_gib,
                "error_alert_interval_s": self.error_alert_interval_s}


def classify(free_bytes: int, config: DiskPressureConfig, margin_gib: float = 0.0) -> Severity:
    """Severity for ``free_bytes``. ``margin_gib`` raises every threshold (hysteresis)."""
    if free_bytes < (config.emergency_gib + margin_gib) * GIB:
        return Severity.EMERGENCY
    if free_bytes < (config.critical_gib + margin_gib) * GIB:
        return Severity.CRITICAL
    if free_bytes < (config.warning_gib + margin_gib) * GIB:
        return Severity.WARNING
    return Severity.OK


def wal_dir_stats(wal_dir: Any) -> Tuple[int, int]:
    """(total bytes, file count) of the ``*.wal`` files in ``wal_dir``. Read-only.

    Raises on any failure: the caller must report "unknown", never a fabricated zero."""
    total = 0
    count = 0
    with os.scandir(wal_dir) as entries:          # raises if the directory is missing / unreadable
        for entry in entries:
            if entry.name.endswith(".wal") and entry.is_file():
                total += entry.stat().st_size
                count += 1
    return total, count


@dataclass(frozen=True)
class PollResult:
    severity: Severity                 # UNKNOWN when the measurement failed
    measurement_ok: bool
    emergency_entered: bool            # True exactly once, on the poll that entered EMERGENCY
    free_bytes: Optional[int] = None
    total_bytes: Optional[int] = None
    error: Optional[str] = None


@dataclass
class _State:
    level: Optional[Severity] = None          # last SUCCESSFUL classification (hysteresis memory)
    measurement_ok: bool = False
    sampled: bool = False
    free_bytes: Optional[int] = None
    total_bytes: Optional[int] = None
    monitored_path: Optional[str] = None
    last_error: Optional[str] = None
    consecutive_errors: int = 0
    last_error_alert_at: Optional[float] = None
    last_transition: Optional[dict] = None
    emergency_latched: bool = False
    emergency_handled: bool = False
    polls: int = 0
    alerts_sent: dict = field(default_factory=lambda: {s.value: 0 for s in (
        Severity.WARNING, Severity.CRITICAL, Severity.EMERGENCY)})
    wal_bytes: Optional[int] = None
    wal_file_count: Optional[int] = None
    wal_error: Optional[str] = None


_GUIDANCE = {
    Severity.WARNING: "Free space is falling: free disk before it reaches the critical threshold.",
    Severity.CRITICAL: ("Free space is critically low: free disk now. If the quality checkpoint is "
                        "blocked the quality WAL keeps growing; nothing deletes it automatically."),
    Severity.EMERGENCY: ("Collection is being stopped through the controlled shutdown (exit status 1); "
                         "raw evidence and the quality WAL are preserved. Free disk before restarting; "
                         "a restart replays uncheckpointed quality events (at-least-once)."),
}


class StoragePressureMonitor:
    """Read-only free-space + quality-WAL observer for ONE runner.

    ``paths`` returns the directories whose filesystems matter (the runner's configured data
    directory and its quality-WAL directory); the worst (smallest free) is used. ``wal_dir``
    and ``checkpoint_state`` feed diagnostics only and can never change a severity.
    """

    def __init__(self, *, label: str, config: DiskPressureConfig,
                 paths: Callable[[], Sequence[Any]],
                 wal_dir: Optional[Callable[[], Any]] = None,
                 checkpoint_state: Optional[Callable[[], Tuple[bool, Optional[str]]]] = None,
                 notify: Optional[Callable[[str], Any]] = None,
                 usage_fn: Callable[[str], Any] = shutil.disk_usage,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.label = label
        self.config = config
        self._paths = paths
        self._wal_dir = wal_dir
        self._checkpoint_state = checkpoint_state
        self._notify = notify
        self._usage = usage_fn
        self._clock = clock
        self._state = _State()

    # ------------------------------------------------------------------ polling
    def poll(self) -> PollResult:
        """One synchronous sample + state update. Never raises; safe to run in a thread."""
        try:
            return self._poll()
        except Exception as exc:  # noqa: BLE001 - an unexpected bug is a monitor error, not a crash
            return self._record_error(exc)

    def _poll(self) -> PollResult:
        state = self._state
        state.polls += 1
        try:
            free, total, path = self._measure()
        except Exception as exc:  # noqa: BLE001 - a failed measurement is a state, not a crash
            return self._record_error(exc)
        state.measurement_ok = True
        state.sampled = True
        state.free_bytes, state.total_bytes, state.monitored_path = free, total, path
        state.consecutive_errors = 0
        previous = state.level
        new = self._next_level(previous, free)
        state.level = new
        self._collect_diagnostics()
        entered = False
        if previous is None:
            escalated = new != Severity.OK            # first sample: highest applicable only
        else:
            escalated = _RANK[new] > _RANK[previous]
        if previous is not None and new != previous:
            state.last_transition = {"from": previous.value, "to": new.value,
                                     "direction": "escalation" if escalated else "recovery",
                                     "free_bytes": free}
            if not escalated:
                self._log("info", "storage_pressure_recovered", severity=new.value,
                          previous=previous.value, free_bytes=free)
        elif previous is None and new != Severity.OK:
            state.last_transition = {"from": None, "to": new.value, "direction": "escalation",
                                     "free_bytes": free}
        if escalated:
            self._alert_severity(new, free, total, path)
        if new == Severity.EMERGENCY and not state.emergency_latched:
            state.emergency_latched = True
            entered = True
        return PollResult(severity=new, measurement_ok=True, emergency_entered=entered,
                          free_bytes=free, total_bytes=total)

    def _next_level(self, previous: Optional[Severity], free: int) -> Severity:
        raw = classify(free, self.config)
        if previous is None or _RANK[raw] > _RANK[previous]:
            return raw
        if _RANK[raw] == _RANK[previous]:
            return previous
        sticky = classify(free, self.config, margin_gib=self.config.hysteresis_gib)
        return sticky if _RANK[sticky] < _RANK[previous] else previous

    def _measure(self) -> Tuple[int, int, str]:
        paths = list(self._paths())
        if not paths:
            raise RuntimeError("no storage path to monitor")
        worst: Optional[Tuple[int, int, str]] = None
        for path in paths:
            usage = self._usage(str(path))
            total, free = int(usage[0]), int(usage[2])
            if free < 0 or total < 0:
                raise RuntimeError(f"implausible disk usage for {path}: total={total} free={free}")
            if worst is None or free < worst[0]:
                worst = (free, total, str(path))
        assert worst is not None
        return worst

    def _collect_diagnostics(self) -> None:
        state = self._state
        if self._wal_dir is None:
            return
        try:
            state.wal_bytes, state.wal_file_count = wal_dir_stats(self._wal_dir())
            state.wal_error = None
        except Exception as exc:  # noqa: BLE001 - unknown, never zero
            state.wal_bytes = state.wal_file_count = None
            state.wal_error = f"{type(exc).__name__}: {exc}"

    # ------------------------------------------------------------------ alerting
    def _record_error(self, exc: BaseException) -> PollResult:
        state = self._state
        state.measurement_ok = False
        state.last_error = f"{type(exc).__name__}: {exc}"
        state.consecutive_errors += 1
        now = self._clock()
        due = (state.last_error_alert_at is None
               or now - state.last_error_alert_at >= self.config.error_alert_interval_s)
        self._log("error", "storage_pressure_monitor_error", error=state.last_error,
                  consecutive=state.consecutive_errors)
        if due:
            state.last_error_alert_at = now          # marked BEFORE sending: a failing backend cannot re-send
            self._send(f"DISK PRESSURE MONITOR ERROR [{self.label}]: free space could not be measured "
                       f"({state.last_error}). Disk state is UNKNOWN, not healthy; the monitor keeps "
                       f"retrying every {self.config.interval_s:g}s. This alert repeats at most every "
                       f"{self.config.error_alert_interval_s:g}s.")
        return PollResult(severity=Severity.UNKNOWN, measurement_ok=False, emergency_entered=False,
                          error=state.last_error)

    def _alert_severity(self, severity: Severity, free: int, total: int, path: str) -> None:
        state = self._state
        state.alerts_sent[severity.value] += 1       # counted before sending
        threshold = {Severity.WARNING: self.config.warning_gib,
                     Severity.CRITICAL: self.config.critical_gib,
                     Severity.EMERGENCY: self.config.emergency_gib}[severity]
        if state.wal_bytes is None:
            wal = f"quality WAL size unavailable ({state.wal_error or 'not monitored'})"
        else:
            wal = f"quality WAL {state.wal_bytes} bytes in {state.wal_file_count} file(s)"
        blocked, reason = self._checkpoint()
        self._log("error" if severity != Severity.WARNING else "warning", "storage_pressure",
                  severity=severity.value, free_bytes=free, total_bytes=total, path=path,
                  wal_bytes=state.wal_bytes, wal_files=state.wal_file_count)
        self._send(f"DISK PRESSURE {severity.value} [{self.label}]: {free / GIB:.2f} GiB free of "
                   f"{total / GIB:.2f} GiB (threshold {threshold:g} GiB) on {path}. {wal}; "
                   f"quality checkpoint blocked={blocked} reason={reason}. {_GUIDANCE[severity]}")

    def _checkpoint(self) -> Tuple[Optional[bool], Optional[str]]:
        if self._checkpoint_state is None:
            return None, None
        try:
            blocked, reason = self._checkpoint_state()
            return bool(blocked), reason
        except Exception:  # noqa: BLE001 - diagnostics only
            return None, None

    def _send(self, message: str) -> None:
        try:
            (self._notify or send_alert)(message)
        except Exception as exc:  # noqa: BLE001 - alerting fails open, state already updated
            self._log("error", "storage_pressure_alert_failed", error=f"{type(exc).__name__}: {exc}")

    def _log(self, level: str, event: str, **fields: Any) -> None:
        try:
            getattr(logger, level)(event, label=self.label, **fields)
        except Exception:  # noqa: BLE001 - logging is never a precondition
            pass

    # ------------------------------------------------------------------ diagnostics
    @property
    def severity(self) -> Severity:
        state = self._state
        if not state.sampled or not state.measurement_ok or state.level is None:
            return Severity.UNKNOWN
        return state.level

    def status(self) -> dict:
        """Read-only operator view. Failed measurements are None / UNKNOWN, never zero."""
        state = self._state
        ok = state.measurement_ok
        blocked, reason = self._checkpoint()
        return {
            "monitoring": True,
            "severity": self.severity.value,
            "last_classified_severity": None if state.level is None else state.level.value,
            "measurement_ok": ok,
            "sampled": state.sampled,
            "free_bytes": state.free_bytes if ok else None,
            "total_bytes": state.total_bytes if ok else None,
            "monitored_path": state.monitored_path,
            "quality_wal_bytes": state.wal_bytes,
            "quality_wal_file_count": state.wal_file_count,
            "quality_wal_error": state.wal_error,
            "wal_total_size_bounded": False,   # no hard cap exists; max_bytes is a per-file rotation threshold
            "quality_checkpoint_blocked": blocked,
            "quality_checkpoint_block_reason": reason,
            "last_monitor_error": state.last_error,
            "consecutive_monitor_errors": state.consecutive_errors,
            "last_transition": None if state.last_transition is None else dict(state.last_transition),
            "emergency_latched": state.emergency_latched,
            "alerts_sent": dict(state.alerts_sent),
            "polls": state.polls,
            "thresholds": self.config.as_dict(),
        }

    # ------------------------------------------------------------------ lifecycle
    async def run(self, on_emergency: Callable[[PollResult], Any]) -> None:
        """Poll every ``interval_s`` OFF the event-loop thread until EMERGENCY is handled.

        ``on_emergency`` is the runner's request for its own controlled shutdown. It is
        retried every interval until it returns without raising, so a failing callback can
        never leave the monitor silently disabled. No exception ends this loop except
        cancellation."""
        state = self._state
        while True:
            result: Optional[PollResult] = None
            try:
                result = await asyncio.to_thread(self.poll)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - poll() does not raise; belt and braces
                self._log("error", "storage_pressure_poll_failed", error=f"{type(exc).__name__}: {exc}")
            if state.emergency_latched and not state.emergency_handled:
                try:
                    on_emergency(result or PollResult(Severity.EMERGENCY, True, True))
                    state.emergency_handled = True
                    return
                except Exception as exc:  # noqa: BLE001 - retried next interval
                    self._log("error", "storage_pressure_emergency_callback_failed",
                              error=f"{type(exc).__name__}: {exc}")
            await asyncio.sleep(self.config.interval_s)
