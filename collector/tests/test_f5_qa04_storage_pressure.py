"""F5-QA-04: continuous storage-pressure monitoring for the WAL-backed runners (Futures + Bybit).

Original Phase A finding (fixed below; regression test retains the injected failure)
-----------------------------------------------------------------------------------
``QualityEventWAL.max_bytes`` is a per-file rotation threshold. After a checkpoint failure latches
checkpointing off, ``maybe_rotate_for_size`` is no longer reached. The aggregate-cap fix now
refuses additional WAL appends before writing beyond the configured byte limit and routes those
events through synchronous, force-published Parquet persistence. The continuous free-space monitor
still protects against other disk growth.

Policy under test
------------------
A hard aggregate WAL cap is tested separately from configurable 7/4/2 GiB free-space thresholds
checked every 15 s off the hot path. No cap path deletes or truncates evidence. Once capped,
events are synchronously force-published to Parquet and are not falsely labelled with WAL IDs.
Free-space measurement failure is UNKNOWN; EMERGENCY uses each runner's controlled lifecycle with
exit status 1 (70 stays reserved for fatal storage); startup preflight; read-only status.

Everything below runs on scratch directories with the real runners / WAL / ``ParquetWriter``. The
free-space reading is injected (``usage_fn``); NO disk is filled and none of this is evidence of real
ENOSPC, power-loss or systemd behaviour.
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

import collector.run_bybit_collector as rbc
import collector.run_collector as rc
from collector.collector import notifications
from collector.collector import disk_pressure as dp
from collector.collector.disk_pressure import (
    GIB, DiskPressureConfig, DiskPressureConfigError, Severity, StoragePressureMonitor, classify,
)
from collector.collector.failure_topology import EXIT_FATAL_STORAGE
from collector.collector.quality_wal import QualityEventWAL
from collector.tests.test_f5_qa03_checkpoint_latch_observability import Rig
from collector.tests._bybit_quality_fixtures import one_row_bybit_quality_segments

DEFAULT = DiskPressureConfig()


# ------------------------------------------------------------------ helpers
class FakeDisk:
    """Injected ``shutil.disk_usage``: a controllable free-space reading (never a real disk)."""

    def __init__(self, free_gib: float = 100.0, total_gib: float = 500.0):
        self.free_gib, self.total_gib, self.fail = free_gib, total_gib, None
        self.paths: list[str] = []

    def __call__(self, path):
        self.paths.append(str(path))
        if self.fail is not None:
            raise self.fail
        free = int(self.free_gib * GIB)
        total = int(self.total_gib * GIB)
        return (total, total - free, free)


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture(autouse=True)
def alerts():
    sent: list[str] = []
    notifications.set_notifier(notifications.CallableNotifier(lambda message: sent.append(message) or True))
    yield sent
    notifications.set_notifier(None)


@pytest.fixture(autouse=True)
def _fast_interval(monkeypatch):
    for var in (dp.ENV_WARNING_GIB, dp.ENV_CRITICAL_GIB, dp.ENV_EMERGENCY_GIB, dp.ENV_HYSTERESIS_GIB,
                dp.ENV_ERROR_ALERT_INTERVAL_S):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv(dp.ENV_INTERVAL_S, "0.02")


def _unit(disk=None, *, config=DEFAULT, clock=None, notify=None, tmp_path=None, wal_dir=None, ckpt=None):
    sent: list[str] = []
    monitor = StoragePressureMonitor(
        label="TEST", config=config, paths=lambda: [tmp_path or "/x"], usage_fn=disk or FakeDisk(),
        clock=clock or Clock(), notify=notify or sent.append,
        wal_dir=(lambda: wal_dir) if wal_dir else None, checkpoint_state=ckpt)
    return monitor, sent


def _pressure_alerts(sent):
    return [m for m in sent if m.startswith("DISK PRESSURE ") and "MONITOR" not in m.split("]")[0]
            and "MONITOR ERROR" not in m and "MONITOR UNAVAILABLE" not in m]


# ======================= configuration =======================
def test_defaults_preserve_the_existing_7_4_2_policy_and_15s_cadence():
    cfg = DiskPressureConfig.from_env({})
    assert (cfg.warning_gib, cfg.critical_gib, cfg.emergency_gib, cfg.interval_s) == (7.0, 4.0, 2.0, 15.0)
    for free, expected in [(7.0, Severity.OK), (6.99, Severity.WARNING), (4.0, Severity.WARNING),
                           (3.99, Severity.CRITICAL), (2.0, Severity.CRITICAL), (1.99, Severity.EMERGENCY),
                           (0, Severity.EMERGENCY)]:
        assert classify(int(free * GIB), cfg) is expected, free


@pytest.mark.parametrize("kwargs", [
    dict(warning_gib=4, critical_gib=4), dict(warning_gib=3, critical_gib=4),
    dict(critical_gib=2, emergency_gib=2), dict(emergency_gib=-1), dict(interval_s=0),
    dict(interval_s=-5), dict(hysteresis_gib=-0.1), dict(hysteresis_gib=2.0),   # >= smallest gap
    dict(warning_gib=float("nan")), dict(warning_gib=float("inf")), dict(warning_gib=True),
    dict(error_alert_interval_s=0),
])
def test_invalid_threshold_settings_are_rejected(kwargs):
    with pytest.raises(DiskPressureConfigError):
        DiskPressureConfig(**kwargs)


def test_environment_configuration_names_and_rejection():
    cfg = DiskPressureConfig.from_env({dp.ENV_WARNING_GIB: "70", dp.ENV_CRITICAL_GIB: "40",
                                       dp.ENV_EMERGENCY_GIB: "20", dp.ENV_INTERVAL_S: "5",
                                       dp.ENV_HYSTERESIS_GIB: "1"})
    assert (cfg.warning_gib, cfg.critical_gib, cfg.emergency_gib, cfg.interval_s, cfg.hysteresis_gib) == \
        (70.0, 40.0, 20.0, 5.0, 1.0)
    for bad in ({dp.ENV_WARNING_GIB: "abc"}, {dp.ENV_WARNING_GIB: ""}, {dp.ENV_CRITICAL_GIB: "9"},
                {dp.ENV_EMERGENCY_GIB: "5"}):
        with pytest.raises(DiskPressureConfigError):
            DiskPressureConfig.from_env(bad)


def test_runner_construction_fails_on_inconsistent_environment(tmp_path, monkeypatch):
    monkeypatch.setenv(dp.ENV_CRITICAL_GIB, "9")           # critical > warning
    with pytest.raises(DiskPressureConfigError):
        rbc.BybitCollectorApp(data_dir=str(tmp_path))


# ======================= state machine =======================
def test_first_sample_below_every_threshold_sends_exactly_one_alert_for_the_highest_severity():
    monitor, sent = _unit(FakeDisk(1.0))
    result = monitor.poll()
    assert result.severity is Severity.EMERGENCY and result.emergency_entered
    assert len(_pressure_alerts(sent)) == 1 and "EMERGENCY" in sent[0]
    for _ in range(20):                       # sustained condition: no alert per iteration
        again = monitor.poll()
        assert not again.emergency_entered
    assert len(sent) == 1


def test_escalation_alerts_once_per_new_severity_and_never_per_poll():
    disk = FakeDisk(50)
    monitor, sent = _unit(disk)
    assert monitor.poll().severity is Severity.OK and sent == []
    for free, sev in [(6.0, "WARNING"), (3.0, "CRITICAL"), (1.0, "EMERGENCY")]:
        disk.free_gib = free
        for _ in range(5):
            monitor.poll()
        assert sev in sent[-1]
    assert len(sent) == 3 and monitor.status()["alerts_sent"] == {"WARNING": 1, "CRITICAL": 1, "EMERGENCY": 1}


def test_hysteresis_prevents_alert_storms_and_rearms_only_after_real_recovery():
    disk = FakeDisk(3.9)
    monitor, sent = _unit(disk)
    monitor.poll()                              # first sample: CRITICAL, one alert
    assert len(sent) == 1
    for free in (4.1, 3.9, 4.2, 3.95, 4.3, 3.9):   # jitter around the 4 GiB threshold
        disk.free_gib = free
        assert monitor.poll().severity is Severity.CRITICAL
    assert len(sent) == 1
    disk.free_gib = 4.6                          # >= 4 + 0.5 hysteresis: recovered to WARNING
    assert monitor.poll().severity is Severity.WARNING
    status = monitor.status()
    assert status["severity"] == "WARNING" and status["last_transition"]["direction"] == "recovery"
    assert len(sent) == 1                        # recovery is diagnostics, not a notification
    disk.free_gib = 3.9                          # genuine re-escalation alerts again, once
    monitor.poll(); monitor.poll()
    assert len(sent) == 2 and "CRITICAL" in sent[-1]
    disk.free_gib = 20
    assert monitor.poll().severity is Severity.OK
    assert monitor.status()["last_transition"]["to"] == "OK"


def test_hysteresis_is_bounded_and_documented_in_status():
    status = _unit()[0].status()
    assert status["thresholds"]["hysteresis_gib"] == 0.5
    assert status["thresholds"]["hysteresis_gib"] < min(7 - 4, 4 - 2)


def test_measurement_failure_is_unknown_not_healthy_and_never_zero_bytes():
    disk, clock = FakeDisk(50), Clock()
    monitor, sent = _unit(disk, clock=clock)
    assert monitor.poll().severity is Severity.OK
    disk.fail = OSError(5, "I/O error")
    for _ in range(10):
        result = monitor.poll()
        assert result.severity is Severity.UNKNOWN and not result.measurement_ok
    status = monitor.status()
    assert status["severity"] == "UNKNOWN" and status["measurement_ok"] is False
    assert status["free_bytes"] is None and status["total_bytes"] is None      # never 0
    assert "I/O error" in status["last_monitor_error"] and status["consecutive_monitor_errors"] == 10
    assert len([m for m in sent if "MONITOR ERROR" in m]) == 1                  # rate limited
    clock.now += DEFAULT.error_alert_interval_s + 1
    monitor.poll()
    assert len([m for m in sent if "MONITOR ERROR" in m]) == 2
    disk.fail = None                                                            # retried and recovered
    assert monitor.poll().severity is Severity.OK
    assert monitor.status()["measurement_ok"] is True and monitor.status()["free_bytes"] is not None


def test_error_state_does_not_corrupt_hysteresis_memory():
    disk = FakeDisk(3.9)
    monitor, sent = _unit(disk)
    monitor.poll()
    disk.fail = OSError("boom")
    monitor.poll()
    disk.fail = None
    disk.free_gib = 4.1
    assert monitor.poll().severity is Severity.CRITICAL      # still sticky across the error
    assert len(_pressure_alerts(sent)) == 1


def test_failing_notifier_and_logger_cannot_undo_state_or_disable_future_checks(monkeypatch):
    calls = []

    def exploding_notify(message):
        calls.append(message)
        raise RuntimeError("notifier exploded")

    class BoomLogger:
        def __getattr__(self, name):
            def fail(*a, **k):
                raise RuntimeError("logger exploded")
            return fail

    monkeypatch.setattr(dp, "logger", BoomLogger())
    disk = FakeDisk(6.0)
    monitor, _ = _unit(disk, notify=exploding_notify)
    assert monitor.poll().severity is Severity.WARNING            # state advanced despite both failing
    assert monitor.status()["severity"] == "WARNING" and len(calls) == 1
    disk.free_gib = 3.0
    assert monitor.poll().severity is Severity.CRITICAL           # future checks still run
    assert len(calls) == 2
    disk.fail = OSError("x")
    assert monitor.poll().severity is Severity.UNKNOWN            # error path also survives both


def test_unexpected_exception_inside_poll_is_a_monitor_error_not_a_crash():
    monitor = StoragePressureMonitor(label="T", config=DEFAULT, paths=lambda: (_ for _ in ()).throw(KeyError("k")),
                                     notify=lambda m: True)
    assert monitor.poll().severity is Severity.UNKNOWN
    assert monitor.status()["severity"] == "UNKNOWN"


def test_a_bug_after_a_successful_measurement_is_still_a_monitor_error_not_a_crash(monkeypatch):
    monitor, sent = _unit(FakeDisk(50))

    def broken(*a, **k):
        raise ZeroDivisionError("bug in diagnostics")

    monkeypatch.setattr(monitor, "_collect_diagnostics", broken)
    result = monitor.poll()                                  # must not raise into the polling thread
    assert result.severity is Severity.UNKNOWN and "ZeroDivisionError" in monitor.status()["last_monitor_error"]
    assert any("MONITOR ERROR" in m for m in sent)


def test_status_is_unknown_before_the_first_sample_and_never_claims_a_bound():
    status = _unit()[0].status()
    assert status["severity"] == "UNKNOWN" and status["sampled"] is False and status["free_bytes"] is None
    assert status["wal_total_size_bounded"] is False


def test_wal_stats_failure_is_reported_as_unavailable_not_zero(tmp_path):
    monitor, sent = _unit(FakeDisk(3.0), tmp_path=tmp_path, wal_dir=tmp_path / "missing" / "wal")
    monitor.poll()
    status = monitor.status()
    assert status["quality_wal_bytes"] is None and status["quality_wal_file_count"] is None
    assert status["quality_wal_error"] and "unavailable" in sent[0]


def test_worst_filesystem_across_monitored_paths_wins():
    disks = {"/data": (100 * GIB, 50 * GIB), "/wal": (10 * GIB, 1 * GIB)}

    def usage(path):
        total, free = disks[path]
        return (total, total - free, free)

    monitor = StoragePressureMonitor(label="T", config=DEFAULT, paths=lambda: ["/data", "/wal"],
                                     usage_fn=usage, notify=lambda m: True)
    assert monitor.poll().severity is Severity.EMERGENCY
    assert monitor.status()["monitored_path"] == "/wal"


def test_monitor_source_is_read_only_and_never_exits_the_process():
    import ast
    forbidden = {"unlink", "remove", "rmtree", "truncate", "rename", "replace", "_exit", "exit", "open",
                 "write", "write_text", "write_bytes", "touch", "mkdir", "makedirs", "chmod"}
    tree = ast.parse(Path(dp.__file__).read_text())
    used = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | \
           {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    assert not (used & forbidden), used & forbidden
    disk_monitor_tree = ast.parse(Path(rc.__file__).parent.joinpath("collector", "disk_monitor.py").read_text())
    assert not {n.attr for n in ast.walk(disk_monitor_tree) if isinstance(n, ast.Attribute)} & {"exit", "_exit"}


# ======================= periodic task lifecycle =======================
def test_periodic_task_polls_at_cadence_survives_errors_and_stops_on_cancel():
    async def scenario():
        disk = FakeDisk(50)
        monitor, sent = _unit(disk, config=DiskPressureConfig(interval_s=0.01))
        task = asyncio.create_task(monitor.run(lambda r: pytest.fail("no emergency expected")))
        await asyncio.sleep(0.15)
        polls_ok = monitor.status()["polls"]
        assert polls_ok >= 5                                   # repeated, not startup-only
        disk.fail = OSError("flaky")
        await asyncio.sleep(0.1)
        assert monitor.status()["severity"] == "UNKNOWN" and not task.done()
        disk.fail = None
        await asyncio.sleep(0.1)
        assert monitor.status()["severity"] == "OK" and not task.done()   # error did not disable checks
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert sent == [] or all("MONITOR ERROR" in m for m in sent)
    asyncio.run(scenario())


def test_emergency_callback_is_retried_until_it_succeeds():
    async def scenario():
        monitor, sent = _unit(FakeDisk(1.0), config=DiskPressureConfig(interval_s=0.01))
        attempts = []

        def callback(result):
            attempts.append(result)
            if len(attempts) < 3:
                raise RuntimeError("shutdown channel not ready")

        await asyncio.wait_for(monitor.run(callback), 5)
        assert len(attempts) == 3 and len(_pressure_alerts(sent)) == 1
    asyncio.run(scenario())


# ======================= runner rigs =======================
@pytest.fixture(params=["futures", "bybit"])
def rig(request, tmp_path, monkeypatch):
    r = Rig(request.param, tmp_path, monkeypatch)
    if request.param == "futures":
        r.app._init_storage_pressure()                # the production wiring, on the minimal app
    r.disk = FakeDisk(50)
    r.app.storage_pressure._usage = r.disk
    yield r
    r.release()


def _wal_dir_bytes_and_count(rig):
    files = rig.wal_files()
    return sum(f.stat().st_size for f in files), len(files)


def test_runner_monitors_its_own_configured_data_and_wal_directories(rig):
    rig.app.storage_pressure.poll()
    seen = {Path(p) for p in rig.disk.paths}
    assert seen == {rig.root, rig.wal_dir}                    # the runner's own dirs, nothing hard-coded


def test_phase_a_healthy_rotation_vs_blocked_growth_beyond_max_bytes(rig):
    """Injected checkpoint failure must no longer allow aggregate WAL growth past its hard cap."""
    wal = rig.app._quality_wal
    wal.max_bytes = 800
    wal.max_total_bytes = 12_000
    calls = {"rotate": 0}
    real = QualityEventWAL.maybe_rotate_for_size

    def counting(self):
        calls["rotate"] += 1
        return real(self)

    QualityEventWAL.maybe_rotate_for_size = counting
    try:
        rig.emit_many("healthy", 30)
        healthy_bytes, healthy_files = _wal_dir_bytes_and_count(rig)
        assert calls["rotate"] >= 30 and healthy_bytes < 5 * 800        # healthy: rotation + deletion bound it
        assert healthy_bytes < wal.max_total_bytes
        rotations_before = calls["rotate"]
        rig.armed = True
        rig.emit_many("blocked", 60)
        assert rig.status()["quality_checkpoint_blocked"] is True
        assert calls["rotate"] == rotations_before        # latched: rotation is never reached again
    finally:
        QualityEventWAL.maybe_rotate_for_size = real
    blocked_bytes, blocked_files = _wal_dir_bytes_and_count(rig)
    assert blocked_bytes <= wal.max_total_bytes                          # hard cap spans every WAL file
    assert blocked_files <= 2
    assert wal.capacity_exhausted is True
    reasons = {row["reason"] for row in rig.rows()}
    assert all(f"blocked{i}" in reasons for i in range(60))              # rejected WAL events still reach Parquet
    assert rig.status()["quality_events_lost"] == 0
    rig.app.storage_pressure.poll()
    status = rig.app.storage_pressure_status()
    assert status["quality_wal_bytes"] == blocked_bytes and status["quality_wal_file_count"] == blocked_files
    assert status["quality_wal_max_bytes"] == wal.max_total_bytes
    assert status["quality_checkpoint_blocked"] is True
    assert status["quality_checkpoint_block_reason"] == "checkpoint_write_failed:OSError"
    assert status["wal_total_size_bounded"] is True


def test_disk_pressure_status_is_separate_from_quality_channel_counters(rig):
    rig.emit("a")
    rig.armed = True
    rig.emit_many("b", 5)
    before = rig.status()
    rig.disk.free_gib = 1.0
    rig.app.storage_pressure.poll()
    after = rig.status()
    for key in ("quality_events_lost", "quality_events_wal_only", "quality_events_wal_unconfirmed",
                "quality_degraded", "quality_failure"):
        assert after[key] == before[key], key                    # disk pressure never touches them
    assert after["quality_events_lost"] == 0
    assert "severity" not in after and rig.app.storage_pressure_status()["severity"] == "EMERGENCY"


def test_warning_and_critical_never_terminate_market_data_routes(rig):
    for free, sev in [(6.0, "WARNING"), (3.0, "CRITICAL")]:
        rig.disk.free_gib = free
        assert rig.app.storage_pressure.poll().severity.value == sev
        rig.assert_market_data_untouched()
        assert getattr(rig.app, "_disk_emergency", None) is None


def test_exit_status_70_keeps_priority_over_a_disk_emergency_and_1_is_the_disk_status(rig):
    app = rig.app
    assert app.exit_code == 0
    app._disk_emergency = {"severity": "EMERGENCY"}
    assert app.exit_code == 1
    rig.policy().terminal_failure = object()
    assert app.exit_code == EXIT_FATAL_STORAGE == 70


# ======================= real Futures app: preflight + runtime lifecycle =======================
def _real_futures(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    app = rc.CollectorApp()
    app.quality_writer.segment_rows = 1        # publish (and so checkpoint) every event, as in QA-03
    return app


class _StubWS:
    connected = True

    def __init__(self, events=None):
        self.started = False
        self.events = events

    async def start(self):
        self.started = True
        if self.events is not None:
            self.events.append("ingest:start")
        await asyncio.sleep(3600)

    async def wait_connected(self, timeout_seconds=0):
        return True

    def stop(self):
        pass

    def enter_discard_mode(self, why):
        pass


def _stub_futures_network(app, events=None):
    app.ws_clients = [_StubWS(events)]

    async def noop(*a, **k):
        return None

    async def forever(*a, **k):
        await asyncio.sleep(3600)

    app._recover_binance_book = noop
    app._poll_openinterest = forever
    app._verify_startup_streams = noop
    app.health_monitor.start = forever


def test_real_futures_monitor_targets_its_configured_data_and_wal_directories(tmp_path, monkeypatch):
    app = _real_futures(tmp_path, monkeypatch)
    try:
        monitored = {Path(p) for p in app.storage_pressure._paths()}
        assert monitored == {Path(app.quality_writer.base_dir), app._quality_wal.wal_dir}
        assert Path(app.disk_monitor.data_dir) == Path(app.quality_writer.base_dir)   # health probe too
    finally:
        app.shutdown()


def test_futures_constructor_no_longer_checks_disk_or_calls_shutdown(tmp_path, monkeypatch, alerts):
    monkeypatch.setattr("collector.collector.disk_monitor.shutil.disk_usage",
                        lambda p: (500 * GIB, 499 * GIB, 1 * GIB))
    app = _real_futures(tmp_path, monkeypatch)             # used to raise AttributeError at < 2 GiB
    try:
        assert not getattr(app, "_shutdown_done", False)
        assert alerts == []                                # nothing is alerted at construction
    finally:
        app.shutdown()


def test_futures_startup_at_emergency_pressure_exits_through_controlled_shutdown(tmp_path, monkeypatch, alerts):
    app = _real_futures(tmp_path, monkeypatch)
    _stub_futures_network(app)
    app.storage_pressure._usage = FakeDisk(1.0)
    wal_dir = app._quality_wal.wal_dir
    asyncio.run(asyncio.wait_for(app.start(), 20))
    assert app.exit_code == 1 and rc.main(app_factory=lambda: app) == 1
    assert app._shutdown_done is True and app._quality_wal._closed is True      # writers + WAL closed in order
    assert app.ws_clients[0].started is False and app.tasks == []                # ingestion never began
    assert len(_pressure_alerts(alerts)) == 1 and "EMERGENCY" in _pressure_alerts(alerts)[0]
    assert wal_dir.exists()


def test_futures_runtime_emergency_stops_collection_via_the_real_start_lifecycle(tmp_path, monkeypatch, alerts):
    app = _real_futures(tmp_path, monkeypatch)
    _stub_futures_network(app)
    disk = FakeDisk(50)
    app.storage_pressure._usage = disk

    async def scenario():
        runner = asyncio.create_task(app.start())
        await asyncio.sleep(0.3)                       # healthy: running, periodic task alive
        assert not runner.done() and app.storage_pressure.status()["polls"] >= 3
        assert app.exit_code == 0 and app.running
        disk.free_gib = 3.0                            # CRITICAL: warned, still collecting
        await asyncio.sleep(0.2)
        assert not runner.done() and app.running and app.exit_code == 0
        disk.free_gib = 1.0                            # EMERGENCY: controlled stop
        await asyncio.wait_for(runner, 30)
    asyncio.run(scenario())
    assert app.exit_code == 1 and app._shutdown_done is True and app._quality_wal._closed is True
    assert app.terminal_failure is None and not app.isolated_routes
    sev = [m.split("[")[0].strip() for m in _pressure_alerts(alerts)]
    assert sev == ["DISK PRESSURE CRITICAL", "DISK PRESSURE EMERGENCY"]


def test_futures_emergency_preserves_every_uncheckpointed_wal_record_and_replays_on_restart(
        tmp_path, monkeypatch):
    app = _real_futures(tmp_path, monkeypatch)
    wal_dir = app._quality_wal.wal_dir
    real = os.replace

    def replace(src, dst, *a, **k):
        if str(src).endswith("checkpoint.json.tmp") and armed["on"]:
            raise OSError(28, "No space left on device (injected checkpoint replace)")
        return real(src, dst, *a, **k)

    armed = {"on": False}
    monkeypatch.setattr(os, "replace", replace)
    app._websocket_quality_event(rc.QualityEventType.DISCONNECT, "healthy")
    app._persist_quality_event(app._quality_queue.get_nowait())
    armed["on"] = True
    for i in range(12):
        app._websocket_quality_event(rc.QualityEventType.DISCONNECT, f"blocked{i}")
        app._persist_quality_event(app._quality_queue.get_nowait())
    assert app.quality_channel_status()["quality_checkpoint_blocked"] is True
    ids_before = {json.loads(line)["quality_event_id"] for f in wal_dir.glob("*.wal")
                  for line in f.read_text().splitlines() if line.strip()}
    pending_before = {r["quality_event_id"] for r in QualityEventWAL.recover(wal_dir)}
    assert len(pending_before) >= 12
    app.storage_pressure._usage = FakeDisk(1.0)
    _stub_futures_network(app)
    asyncio.run(asyncio.wait_for(app.start(), 20))                     # startup preflight -> controlled stop
    ids_after = {json.loads(line)["quality_event_id"] for f in wal_dir.glob("*.wal")
                 for line in f.read_text().splitlines() if line.strip()}
    assert ids_before <= ids_after                                      # nothing deleted
    assert pending_before <= {r["quality_event_id"] for r in QualityEventWAL.recover(wal_dir)}
    status = app.quality_channel_status()
    assert status["quality_events_lost"] == 0                           # no false loss from pressure
    monkeypatch.setattr(os, "replace", real)
    restarted = _real_futures(tmp_path, monkeypatch)                   # restart: real recovery
    try:
        replayed = {r["quality_event_id"] for r in QualityEventWAL.recover(restarted._quality_wal.wal_dir)}
        assert replayed == set()                                        # replayed, then checkpoint caught up
        assert restarted._quality_wal.checkpointed_seq >= max(
            int(i.rsplit("-", 1)[1]) for i in pending_before)
    finally:
        restarted.shutdown()


# ======================= real Bybit app: preflight + runtime lifecycle =======================
class _StubBybitClient:
    def __init__(self, events=None):
        self.started = False
        self.stopped = False
        self.events = events

    async def start(self):
        self.started = True
        if self.events is not None:
            self.events.append("ingest:start")
        await asyncio.sleep(3600)

    def stop(self):
        self.stopped = True

    def enter_discard_mode(self, why):
        pass


def _real_bybit(tmp_path):
    with one_row_bybit_quality_segments():      # P0-3: Bybit batches in production; one-row set BEFORE construction
        app = rbc.BybitCollectorApp(data_dir=str(tmp_path))
    app.client = _StubBybitClient()
    return app


def test_bybit_startup_at_emergency_pressure_exits_1_without_starting_the_client(tmp_path, alerts):
    app = _real_bybit(tmp_path)
    app.storage_pressure._usage = FakeDisk(1.0)

    async def go():
        return await rbc._run_supervised(app, asyncio.Event())
    assert asyncio.run(asyncio.wait_for(go(), 20)) == 1
    assert app.client.started is False and app.client.stopped is True
    assert app._quality_wal._closed is True and app.exit_code == 1
    assert len(_pressure_alerts(alerts)) == 1


def test_bybit_runtime_emergency_stops_through_the_supervisor_with_exit_1(tmp_path, alerts):
    app = _real_bybit(tmp_path)
    disk = FakeDisk(50)
    app.storage_pressure._usage = disk

    async def scenario():
        stop = asyncio.Event()
        runner = asyncio.create_task(rbc._run_supervised(app, stop))
        await asyncio.sleep(0.3)
        assert not runner.done() and app.client.started and app.storage_pressure.status()["polls"] >= 3
        disk.free_gib = 6.0                                    # WARNING: no stop
        await asyncio.sleep(0.15)
        assert not runner.done() and app.exit_code == 0
        disk.free_gib = 1.0
        return await asyncio.wait_for(runner, 30)
    assert asyncio.run(scenario()) == 1
    assert app._quality_wal._closed is True and app.client.stopped is True
    assert app.failure_policy.terminal_failure is None and not app.failure_policy.isolated_routes
    assert [m.split("[")[0].strip() for m in _pressure_alerts(alerts)] == \
        ["DISK PRESSURE WARNING", "DISK PRESSURE EMERGENCY"]


def test_bybit_emergency_with_blocked_checkpoint_preserves_wal_and_recovers_on_restart(tmp_path, monkeypatch):
    app = _real_bybit(tmp_path)
    wal_dir = app._quality_wal.wal_dir
    real = os.replace
    armed = {"on": False}

    def replace(src, dst, *a, **k):
        if armed["on"] and str(src).endswith("checkpoint.json.tmp"):
            raise OSError(28, "No space left on device (injected checkpoint replace)")
        return real(src, dst, *a, **k)

    monkeypatch.setattr(os, "replace", replace)
    app._on_client_quality_event("DISCONNECT", "healthy", "bybit-1", "bybit")
    armed["on"] = True
    for i in range(12):
        app._on_client_quality_event("DISCONNECT", f"blocked{i}", "bybit-1", "bybit")
    pending_before = {r["quality_event_id"] for r in QualityEventWAL.recover(wal_dir)}
    assert len(pending_before) >= 12
    app.storage_pressure._usage = FakeDisk(1.0)

    async def go():
        return await rbc._run_supervised(app, asyncio.Event())
    assert asyncio.run(asyncio.wait_for(go(), 20)) == 1
    assert pending_before <= {r["quality_event_id"] for r in QualityEventWAL.recover(wal_dir)}
    assert app.quality_channel_status()["quality_events_lost"] == 0
    monkeypatch.setattr(os, "replace", real)
    restarted = rbc.BybitCollectorApp(data_dir=str(tmp_path))
    try:
        assert restarted._quality_wal.checkpointed_seq >= max(int(i.rsplit("-", 1)[1]) for i in pending_before)
        assert restarted.quality_channel_status()["quality_checkpoint_blocked"] is False
    finally:
        asyncio.run(restarted.shutdown())


def test_bybit_probe_failing_at_startup_is_retried_and_ingestion_waits_for_a_valid_measurement(tmp_path):
    """Replaces the pre-follow-up expectation that a failing probe could run collection forever: a probe
    that fails at startup is UNKNOWN, retried, and ingestion starts only after a valid measurement."""
    app = _real_bybit(tmp_path)
    disk = FakeDisk(50)
    disk.fail = OSError(5, "I/O error")
    app.storage_pressure._usage = disk

    async def scenario():
        stop = asyncio.Event()
        runner = asyncio.create_task(rbc._run_supervised(app, stop))
        await asyncio.sleep(0.3)
        assert not runner.done() and app.exit_code == 0
        assert app.client.started is False                      # no ingestion while UNKNOWN
        assert app.storage_pressure_status()["severity"] == "UNKNOWN"
        disk.fail = None
        await asyncio.sleep(0.2)
        assert app.client.started is True and app.storage_pressure_status()["severity"] == "OK"
        stop.set()
        return await asyncio.wait_for(runner, 30)
    assert asyncio.run(scenario()) == 0


# ======================= F5-QA-04 follow-up: UNKNOWN fails closed =======================
from collector.collector.disk_pressure import EXIT_DISK_MONITOR_UNAVAILABLE, EXIT_DISK_PRESSURE  # noqa: E402
from collector.collector.quality_wal import CHECKPOINT_FILENAME  # noqa: E402


class ScriptedDisk(FakeDisk):
    """Free-space reading driven by a script of floats (GiB free) / exceptions, then ``fail`` / ``free_gib``.

    Failing calls advance the injected monotonic clock by ``step`` seconds, so startup-grace tests are
    deterministic and take no real time. ``events`` records ordering against ingestion start."""

    def __init__(self, script=(), *, free_gib=50.0, clock=None, step=0.0, events=None):
        super().__init__(free_gib)
        self.script, self.clock, self.step = list(script), clock, step
        self.events = events if events is not None else []
        self.calls = 0

    def __call__(self, path):
        self.calls += 1
        item = self.script.pop(0) if self.script else (self.fail if self.fail is not None else self.free_gib)
        if isinstance(item, BaseException):
            self.events.append("measure:fail")
            if self.clock is not None:
                self.clock.now += self.step
            raise item
        self.events.append("measure:ok")
        free = int(item * GIB)
        total = int(self.total_gib * GIB)
        return (total, total - free, free)


@pytest.fixture(params=["futures", "bybit"])
def kind(request):
    return request.param


class Harness:
    """Drives the REAL runner lifecycle (Futures ``start()`` / Bybit ``_run_supervised``) on scratch dirs."""

    def __init__(self, kind, tmp_path, monkeypatch, *, disk=None, clock=None, grace="60"):
        monkeypatch.setenv(dp.ENV_UNKNOWN_GRACE_S, str(grace))
        self.kind, self.clock = kind, clock or Clock()
        self.events: list = []
        self.disk = disk if disk is not None else ScriptedDisk(clock=self.clock, events=self.events)
        self.disk.events = self.events
        self.shutdown_calls = 0
        self.shutdown_requests = 0
        if kind == "futures":
            self.app = _real_futures(tmp_path, monkeypatch)
            _stub_futures_network(self.app, self.events)
        else:
            with one_row_bybit_quality_segments():      # P0-3: set BEFORE construction
                self.app = rbc.BybitCollectorApp(data_dir=str(tmp_path))
            self.app.client = _StubBybitClient(self.events)
        self.app.storage_pressure._usage = self.disk
        self.app.storage_pressure._clock = self.clock
        self._wrap()
        self.task = None
        self.stop = None

    def _wrap(self):
        app = self.app
        real_shutdown = app.shutdown
        if self.kind == "futures":
            def shutdown(*a, **k):
                self.shutdown_calls += 1
                return real_shutdown(*a, **k)
            real_async = app._async_shutdown

            async def async_shutdown(*a, **k):
                self.shutdown_requests += 1
                return await real_async(*a, **k)
            app._async_shutdown = async_shutdown
        else:
            async def shutdown(*a, **k):
                self.shutdown_calls += 1
                self.shutdown_requests += 1
                return await real_shutdown(*a, **k)
        app.shutdown = shutdown

    def launch(self):
        if self.kind == "futures":
            self.task = asyncio.create_task(self.app.start())
        else:
            self.stop = asyncio.Event()
            self.task = asyncio.create_task(rbc._run_supervised(self.app, self.stop))

    @property
    def ingest_started(self):
        return (self.app.ws_clients[0].started if self.kind == "futures" else self.app.client.started)

    @property
    def done(self):
        return self.task.done()

    def status(self):
        return self.app.storage_pressure_status()

    def graceful_stop(self):
        if self.kind == "futures":
            self.app._graceful = asyncio.create_task(self.app._async_shutdown(0))
        else:
            self.stop.set()

    async def wait_exit(self, timeout=30):
        result = await asyncio.wait_for(self.task, timeout)
        return self.app.exit_code if self.kind == "futures" else result

    @property
    def wal_closed(self):
        return self.app._quality_wal._closed

    def leftover_tasks(self):
        current = asyncio.current_task()
        return [t for t in asyncio.all_tasks() if t is not current and not t.done()]

    def finalized(self):
        if self.kind == "futures":
            assert self.app._shutdown_done is True
            assert all(t.done() for t in self.app.tasks)
        else:
            assert self.app.client.stopped is True
            assert self.app._disk_task is None or self.app._disk_task.done()
        assert self.wal_closed is True


def _monitor_tasks():
    return [t for t in asyncio.all_tasks() if t.get_coro().__qualname__.endswith("_storage_pressure_loop")]


def _count(alerts, text):
    return len([m for m in alerts if text in m])


# ---- configuration ----
def test_unknown_grace_default_validation_and_environment():
    assert DiskPressureConfig().unknown_grace_s == 60.0 == DiskPressureConfig.from_env({}).unknown_grace_s
    assert DiskPressureConfig.from_env({dp.ENV_UNKNOWN_GRACE_S: "120"}).unknown_grace_s == 120.0
    for bad in (0, -1, float("nan"), float("inf"), True):
        with pytest.raises(DiskPressureConfigError):
            DiskPressureConfig(unknown_grace_s=bad)
    for bad in ("", "abc", "0", "-5", "nan"):
        with pytest.raises(DiskPressureConfigError):
            DiskPressureConfig.from_env({dp.ENV_UNKNOWN_GRACE_S: bad})
    assert DiskPressureConfig().as_dict()["unknown_grace_s"] == 60.0


def test_runner_construction_rejects_invalid_unknown_grace(tmp_path, monkeypatch):
    monkeypatch.setenv(dp.ENV_UNKNOWN_GRACE_S, "0")
    with pytest.raises(DiskPressureConfigError):
        rbc.BybitCollectorApp(data_dir=str(tmp_path))


def test_exit_statuses_are_distinct_and_documented():
    assert (EXIT_DISK_PRESSURE, EXIT_DISK_MONITOR_UNAVAILABLE, EXIT_FATAL_STORAGE) == (1, 69, 70)
    assert len({EXIT_DISK_PRESSURE, EXIT_DISK_MONITOR_UNAVAILABLE, EXIT_FATAL_STORAGE, 0}) == 4


# ---- state machine (unit, injected monotonic clock) ----
def _grace_unit(grace=60.0, **kw):
    clock, disk = Clock(), FakeDisk(50)
    monitor, sent = _unit(disk, clock=clock, config=DiskPressureConfig(unknown_grace_s=grace, **kw))
    return monitor, disk, clock, sent


def test_default_clock_is_monotonic_and_wall_clock_jumps_do_not_matter(monkeypatch):
    import time
    monitor = StoragePressureMonitor(label="T", config=DEFAULT, paths=lambda: ["/x"], notify=lambda m: True)
    assert monitor._clock is time.monotonic
    clock = Clock()
    monitor, disk, _, _ = _grace_unit()
    monitor._clock = clock
    disk.fail = OSError("x")
    monkeypatch.setattr(time, "time", lambda: 10 ** 12)                # absurd wall-clock jump
    assert monitor.poll().unavailable_entered is False


def test_unavailable_latches_exactly_once_after_the_grace_period_not_before():
    monitor, disk, clock, sent = _grace_unit(60.0)
    monitor.poll()
    disk.fail = OSError(5, "I/O error")
    results = []
    for _ in range(4):
        results.append(monitor.poll())
        clock.now += 20                       # failures at +0, +20, +40, +60
    assert [r.unavailable_entered for r in results] == [False, False, False, True]
    for _ in range(5):
        assert monitor.poll().unavailable_entered is False             # one-shot
    status = monitor.status()
    assert status["monitor_unavailable_latched"] is True and status["shutdown_reason"] == "disk_monitor_unavailable"
    assert _count(sent, "MONITOR UNAVAILABLE") == 1 and "NOT a confirmed low-disk" in sent[-1]


def test_a_success_before_the_grace_expires_resets_the_failure_run():
    monitor, disk, clock, sent = _grace_unit(60.0)
    monitor.poll()
    disk.fail = OSError("x")
    monitor.poll(); clock.now += 50; monitor.poll()
    assert monitor.status()["monitor_failure_duration_s"] == 50.0
    disk.fail = None
    assert monitor.poll().measurement_ok
    assert monitor.status()["monitor_failure_duration_s"] is None and monitor.status()["consecutive_monitor_errors"] == 0
    disk.fail = OSError("x")
    monitor.poll(); clock.now += 50
    assert monitor.poll().unavailable_entered is False               # 50 s into a NEW run, not 100 s
    clock.now += 10
    assert monitor.poll().unavailable_entered is True


def test_unknown_never_exposes_the_last_classification_as_the_current_severity():
    monitor, disk, clock, _ = _grace_unit()
    disk.free_gib = 6.0
    assert monitor.poll().severity is Severity.WARNING
    disk.fail = OSError("x")
    monitor.poll()
    status = monitor.status()
    assert status["severity"] == "UNKNOWN" and status["last_classified_severity"] == "WARNING"
    assert status["free_bytes"] is None and status["total_bytes"] is None and status["measurement_ok"] is False


def test_low_space_readings_never_trigger_monitor_unavailable():
    monitor, disk, clock, _ = _grace_unit(60.0)
    for free in (6.0, 3.0, 6.0, 50.0):
        disk.free_gib = free
        clock.now += 1000
        result = monitor.poll()
        assert result.measurement_ok and not result.unavailable_entered
    assert monitor.status()["monitor_unavailable_latched"] is False


def test_establish_measurement_retries_then_succeeds_without_duplicate_alerts():
    async def scenario():
        clock, events = Clock(), []
        disk = ScriptedDisk([OSError("boom"), OSError("boom")], free_gib=50, clock=clock, step=10, events=events)
        monitor, sent = _unit(disk, clock=clock, config=DiskPressureConfig(interval_s=0.01))
        result = await asyncio.wait_for(monitor.establish_measurement(), 5)
        assert result.measurement_ok and result.severity is Severity.OK and disk.calls == 3
        assert _count(sent, "MONITOR ERROR") == 1                      # rate limited across the retries
        assert monitor.status()["alerts_sent"] == {"WARNING": 0, "CRITICAL": 0, "EMERGENCY": 0}
    asyncio.run(scenario())


def test_establish_measurement_is_bounded_by_the_grace_and_does_not_spin():
    async def scenario():
        clock, events = Clock(), []
        disk = ScriptedDisk([], clock=clock, step=20, events=events)
        disk.fail = OSError("dead probe")
        monitor, sent = _unit(disk, clock=clock, config=DiskPressureConfig(interval_s=0.01, unknown_grace_s=60))
        result = await asyncio.wait_for(monitor.establish_measurement(), 5)
        assert not result.measurement_ok and result.unavailable_entered and disk.calls == 4
        assert _count(sent, "MONITOR UNAVAILABLE") == 1 and _count(sent, "MONITOR ERROR") == 1
    asyncio.run(scenario())


def test_unavailable_callback_is_retried_until_it_succeeds_and_accepted_once():
    async def scenario():
        clock = Clock()
        disk = ScriptedDisk([], clock=clock, step=100)
        disk.fail = OSError("dead")
        monitor, sent = _unit(disk, clock=clock, config=DiskPressureConfig(interval_s=0.01, unknown_grace_s=60))
        attempts = []

        def on_unavailable(result):
            attempts.append(result)
            if len(attempts) < 3:
                raise RuntimeError("stop channel not ready")

        await asyncio.wait_for(monitor.run(lambda r: pytest.fail("not an emergency"), on_unavailable), 5)
        assert len(attempts) == 3 and _count(sent, "MONITOR UNAVAILABLE") == 1
    asyncio.run(scenario())


def test_establish_measurement_spaces_retries_by_the_interval_and_never_hot_loops():
    """With a frozen clock the grace can never expire, so only the interval spacing bounds the probe rate."""
    async def scenario():
        clock = Clock()                                   # frozen: the grace deadline never arrives
        disk = ScriptedDisk([], clock=clock, step=0)
        disk.fail = OSError("dead probe")
        monitor, _ = _unit(disk, clock=clock, config=DiskPressureConfig(interval_s=0.05, unknown_grace_s=60))
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(monitor.establish_measurement(), 0.4)
        assert 3 <= disk.calls <= 12, disk.calls          # ~0.4 s / 0.05 s; a hot loop would be thousands
    asyncio.run(scenario())


def test_futures_runtime_stop_reasons_share_one_shutdown_even_if_both_fire(tmp_path, monkeypatch, alerts):
    h = Harness("futures", tmp_path, monkeypatch)

    async def scenario():
        h.launch()
        await asyncio.sleep(0.25)
        assert h.ingest_started and h.shutdown_requests == 0
        h.app._on_disk_emergency(None)                    # both stop reasons, repeated, back to back
        h.app._on_disk_monitor_unavailable(None)
        h.app._on_disk_emergency(None)
        assert await h.wait_exit() == 1                   # confirmed emergency outranks unavailable
        assert h.shutdown_requests == 1                   # one controlled shutdown, not three
        h.finalized()
        assert h.leftover_tasks() == []
    asyncio.run(scenario())


# ---- startup (real lifecycles, both runners) ----
def test_startup_first_probe_fails_then_succeeds_ingestion_starts_only_after_the_valid_measurement(
        kind, tmp_path, monkeypatch, alerts):
    clock = Clock()
    h = Harness(kind, tmp_path, monkeypatch, clock=clock,
                disk=ScriptedDisk([OSError(5, "I/O error")], free_gib=50, clock=clock, step=20))

    async def scenario():
        h.launch()
        for _ in range(100):
            if h.ingest_started:
                break
            await asyncio.sleep(0.02)
        assert h.ingest_started
        ev = h.events
        assert ev.index("measure:fail") < ev.index("measure:ok") < ev.index("ingest:start")
        assert ev.count("ingest:start") == 1
        await asyncio.sleep(0.15)                                       # runtime loop is running ...
        assert len(_monitor_tasks()) == 1                               # ... exactly one monitor task
        assert h.shutdown_requests == 0 and h.shutdown_calls == 0 and not h.done
        status = h.status()
        assert status["severity"] == "OK" and status["monitor_error_alerts"] == 1
        assert status["alerts_sent"] == {"WARNING": 0, "CRITICAL": 0, "EMERGENCY": 0}
        h.graceful_stop()
        assert await h.wait_exit() == 0
        h.finalized()
        assert h.leftover_tasks() == []
    asyncio.run(scenario())
    assert _count(alerts, "MONITOR ERROR") == 1


def test_startup_probe_never_recovers_controlled_shutdown_no_ingestion_exit_69(kind, tmp_path, monkeypatch, alerts):
    clock = Clock()
    disk = ScriptedDisk([], clock=clock, step=20)
    disk.fail = OSError(5, "I/O error")
    h = Harness(kind, tmp_path, monkeypatch, clock=clock, disk=disk)

    async def scenario():
        h.launch()
        assert await h.wait_exit() == EXIT_DISK_MONITOR_UNAVAILABLE == 69
        assert h.ingest_started is False and "ingest:start" not in h.events
        assert disk.calls == 4                                          # bounded: failures at +0, +20, +40, +60
        assert h.shutdown_calls == 1                                    # exactly one shutdown action
        assert len(_monitor_tasks()) == 0                               # the periodic task was never started
        h.finalized()
        assert h.leftover_tasks() == []
    asyncio.run(scenario())
    status = h.status()
    assert status["shutdown_reason"] == "disk_monitor_unavailable" and status["severity"] == "UNKNOWN"
    assert status["free_bytes"] is None and "I/O error" in status["last_monitor_error"]
    assert h.app._disk_emergency is None                                # NOT recorded as a disk emergency
    assert _count(alerts, "MONITOR UNAVAILABLE") == 1 and _count(alerts, "MONITOR ERROR") == 1
    assert _pressure_alerts(alerts) == []                               # no fabricated severity alert
    if kind == "futures":
        assert rc.main(app_factory=lambda: h.app) == 69                 # main() maps the latch to the status


def test_startup_probe_fails_then_reports_emergency_exit_1_not_69_and_no_ingestion(kind, tmp_path, monkeypatch, alerts):
    clock = Clock()
    h = Harness(kind, tmp_path, monkeypatch, clock=clock,
                disk=ScriptedDisk([OSError("boom"), 1.0], clock=clock, step=20))

    async def scenario():
        h.launch()
        assert await h.wait_exit() == EXIT_DISK_PRESSURE == 1
        assert h.ingest_started is False and h.shutdown_calls == 1
        h.finalized()
    asyncio.run(scenario())
    assert h.app._disk_monitor_unavailable is None and h.status()["shutdown_reason"] == "disk_emergency"
    assert _count(alerts, "EMERGENCY") == 1 and _count(alerts, "MONITOR UNAVAILABLE") == 0


@pytest.mark.parametrize("free,label", [(6.0, "WARNING"), (3.0, "CRITICAL")])
def test_startup_warning_or_critical_first_reading_proceeds_and_is_not_confused_with_unknown(
        kind, tmp_path, monkeypatch, alerts, free, label):
    h = Harness(kind, tmp_path, monkeypatch, disk=ScriptedDisk([free], free_gib=free))

    async def scenario():
        h.launch()
        for _ in range(100):
            if h.ingest_started:
                break
            await asyncio.sleep(0.02)
        assert h.ingest_started and not h.done
        status = h.status()
        assert status["severity"] == label and status["measurement_ok"] is True
        assert status["monitor_error_alerts"] == 0 and status["monitor_unavailable_latched"] is False
        h.graceful_stop()
        assert await h.wait_exit() == 0
    asyncio.run(scenario())
    assert [m.split("[")[0].strip() for m in _pressure_alerts(alerts)] == [f"DISK PRESSURE {label}"]   # once


# ---- runtime (real lifecycles, both runners) ----
def test_runtime_transient_error_is_unknown_rate_limited_and_never_stops_collection(kind, tmp_path, monkeypatch, alerts):
    h = Harness(kind, tmp_path, monkeypatch)

    async def scenario():
        h.launch()
        await asyncio.sleep(0.25)
        assert h.ingest_started and h.status()["severity"] == "OK"
        h.disk.fail = OSError(5, "I/O error")
        await asyncio.sleep(0.2)                                        # ~10 failing polls, clock frozen
        status = h.status()
        assert status["severity"] == "UNKNOWN" and status["free_bytes"] is None and status["consecutive_monitor_errors"] >= 3
        assert status["monitor_failure_duration_s"] == 0.0 and status["shutdown_reason"] is None
        assert not h.done and h.app.exit_code == 0
        assert _count(alerts, "MONITOR ERROR") == 1                     # rate limited, not one per poll
        h.disk.fail = None                                              # recovers inside the grace period
        await asyncio.sleep(0.2)
        status = h.status()
        assert status["severity"] == "OK" and status["monitor_failure_duration_s"] is None
        assert status["consecutive_monitor_errors"] == 0 and "I/O error" in status["last_monitor_error"]
        assert not h.done and h.shutdown_requests == 0
        h.graceful_stop()
        assert await h.wait_exit() == 0
        h.finalized()
    asyncio.run(scenario())


def test_runtime_persistent_failure_stops_exactly_once_with_status_69_and_cleans_up(kind, tmp_path, monkeypatch, alerts):
    h = Harness(kind, tmp_path, monkeypatch)

    async def scenario():
        h.launch()
        await asyncio.sleep(0.25)
        assert h.ingest_started
        h.disk.free_gib = 6.0
        await asyncio.sleep(0.1)                                        # WARNING classification remembered
        h.disk.fail = OSError(5, "I/O error")
        await asyncio.sleep(0.1)
        h.clock.now += 25
        await asyncio.sleep(0.1)
        status = h.status()                                             # mid-failure diagnostics
        assert status["severity"] == "UNKNOWN" and status["last_classified_severity"] == "WARNING"
        assert status["monitor_failure_duration_s"] == 25.0 and status["free_bytes"] is None
        assert status["shutdown_reason"] is None and not h.done
        h.clock.now += 40                                               # 65 s of consecutive failures >= 60 s grace
        assert await h.wait_exit() == 69
        assert h.shutdown_requests == 1                                 # exactly once
        h.finalized()
        assert h.leftover_tasks() == []
    asyncio.run(scenario())
    status = h.status()
    assert status["shutdown_reason"] == "disk_monitor_unavailable" and status["free_bytes"] is None
    assert h.app._disk_emergency is None and h.app._disk_monitor_unavailable is not None
    assert _count(alerts, "MONITOR UNAVAILABLE") == 1 and _pressure_alerts(alerts)[-1].startswith("DISK PRESSURE WARNING")
    assert h.app.quality_channel_status()["quality_events_lost"] == 0


def test_runtime_intermittent_success_resets_the_consecutive_failure_grace_period(kind, tmp_path, monkeypatch):
    h = Harness(kind, tmp_path, monkeypatch)

    async def scenario():
        h.launch()
        await asyncio.sleep(0.25)
        h.disk.fail = OSError("flaky")
        await asyncio.sleep(0.1)
        h.clock.now += 50
        await asyncio.sleep(0.1)
        assert not h.done and h.status()["monitor_failure_duration_s"] == 50.0
        h.disk.fail = None                                              # success resets the run
        await asyncio.sleep(0.1)
        assert h.status()["monitor_failure_duration_s"] is None
        h.disk.fail = OSError("flaky")
        await asyncio.sleep(0.1)
        h.clock.now += 50                                               # 100 s total, but only 50 s consecutive
        await asyncio.sleep(0.15)
        assert not h.done and h.shutdown_requests == 0
        h.clock.now += 20                                               # now 70 s consecutive
        assert await h.wait_exit() == 69 and h.shutdown_requests == 1
    asyncio.run(scenario())


def test_runtime_real_emergency_during_unknown_uses_the_emergency_path_status_1(kind, tmp_path, monkeypatch, alerts):
    h = Harness(kind, tmp_path, monkeypatch)

    async def scenario():
        h.launch()
        await asyncio.sleep(0.25)
        h.disk.fail = OSError("flaky")
        await asyncio.sleep(0.1)
        h.clock.now += 30
        await asyncio.sleep(0.1)
        h.disk.fail = None
        h.disk.free_gib = 1.0                                           # a real measurement: confirmed emergency
        assert await h.wait_exit() == 1
        assert h.shutdown_requests == 1
        h.finalized()
    asyncio.run(scenario())
    assert h.status()["shutdown_reason"] == "disk_emergency" and h.app._disk_monitor_unavailable is None
    assert _count(alerts, "MONITOR UNAVAILABLE") == 0 and _count(alerts, "EMERGENCY") == 1


def test_runtime_warning_and_critical_readings_never_trigger_monitor_unavailable(kind, tmp_path, monkeypatch, alerts):
    h = Harness(kind, tmp_path, monkeypatch)

    async def scenario():
        h.launch()
        await asyncio.sleep(0.25)
        for free in (6.0, 3.0):
            h.disk.fail = OSError("flaky")
            await asyncio.sleep(0.08)
            h.clock.now += 1000                                         # far beyond any grace if the run had not ended
            h.disk.fail = None
            h.disk.free_gib = free
            await asyncio.sleep(0.15)
            assert not h.done and h.app.exit_code == 0
            assert h.status()["monitor_unavailable_latched"] is False
        assert h.status()["severity"] == "CRITICAL" and h.ingest_started
        h.graceful_stop()
        assert await h.wait_exit() == 0
    asyncio.run(scenario())


def test_runtime_throwing_logger_and_notifier_cannot_stop_the_monitor_or_corrupt_state(kind, tmp_path, monkeypatch):
    class BoomLogger:
        def __getattr__(self, name):
            def fail(*a, **k):
                raise RuntimeError("logger exploded")
            return fail

    def exploding(message):
        raise RuntimeError("notifier exploded")

    h = Harness(kind, tmp_path, monkeypatch)
    monkeypatch.setattr(dp, "logger", BoomLogger())
    notifications.set_notifier(notifications.CallableNotifier(exploding))

    async def scenario():
        h.launch()
        await asyncio.sleep(0.25)
        h.disk.fail = OSError(5, "I/O error")
        await asyncio.sleep(0.15)
        assert h.status()["severity"] == "UNKNOWN" and not h.done          # polling continues despite both failing
        h.clock.now += 100
        assert await h.wait_exit() == 69 and h.shutdown_requests == 1
        h.finalized()
    asyncio.run(scenario())
    assert h.status()["shutdown_reason"] == "disk_monitor_unavailable"


def test_fatal_storage_status_70_has_priority_over_disk_emergency_and_monitor_unavailable(kind, tmp_path, monkeypatch):
    h = Harness(kind, tmp_path, monkeypatch)
    app = h.app
    app._disk_emergency = {"severity": "EMERGENCY"}
    app._disk_monitor_unavailable = {"severity": "UNKNOWN"}
    assert app.exit_code == 1                                           # confirmed emergency outranks unavailable
    app._disk_emergency = None
    assert app.exit_code == 69
    app._disk_emergency = {"severity": "EMERGENCY"}
    (app if kind == "futures" else app.failure_policy).terminal_failure = object()
    assert app.exit_code == EXIT_FATAL_STORAGE == 70
    app._disk_emergency = None
    assert app.exit_code == 70
    app.storage_pressure  # still constructed; nothing was torn down
    asyncio.run(_close(h))


async def _close(h):
    if h.kind == "bybit":
        await h.app.shutdown()
    else:
        h.app.shutdown()


def test_bybit_supervisor_mapping_keeps_70_and_maps_disk_stops(tmp_path, monkeypatch):
    h = Harness("bybit", tmp_path, monkeypatch)
    h.app._disk_monitor_unavailable = {"severity": "UNKNOWN"}

    async def supervisor_70(app, stop, **kw):
        return 70

    async def supervisor_0(app, stop, **kw):
        return 0

    async def supervisor_crash(app, stop, **kw):
        return 1

    async def scenario():
        monkeypatch.setattr(rbc, "supervise_standalone_runner", supervisor_70)
        assert await rbc._run_supervised(h.app, asyncio.Event()) == 70          # 70 never overwritten
        monkeypatch.setattr(rbc, "supervise_standalone_runner", supervisor_0)
        assert await rbc._run_supervised(h.app, asyncio.Event()) == 69          # clean stop + latch -> 69
        monkeypatch.setattr(rbc, "supervise_standalone_runner", supervisor_crash)
        assert await rbc._run_supervised(h.app, asyncio.Event()) == 1           # a crash keeps its own status
        await h.app.shutdown()
    asyncio.run(scenario())


# ---- WAL / data truth ----
def _emit_blocked(kind, app, n):
    for i in range(n):
        if kind == "futures":
            app._websocket_quality_event(rc.QualityEventType.DISCONNECT, f"blocked{i}")
            app._persist_quality_event(app._quality_queue.get_nowait())
            app._quality_queue.task_done()          # a real consumer acknowledges; shutdown joins the queue
        else:
            app._on_client_quality_event("DISCONNECT", f"blocked{i}", "bybit-1", "bybit")


def _wal_ids(wal_dir):
    return {json.loads(line)["quality_event_id"] for f in wal_dir.glob("*.wal")
            for line in f.read_text().splitlines() if line.strip()}


def _disk_checkpoint(wal_dir):
    path = wal_dir / CHECKPOINT_FILENAME
    return json.loads(path.read_text())["checkpointed_seq"] if path.exists() else -1


@pytest.mark.parametrize("when", ["startup", "runtime"])
def test_monitor_unavailable_shutdown_preserves_uncheckpointed_wal_and_restart_recovers(
        kind, when, tmp_path, monkeypatch):
    clock = Clock()
    disk = ScriptedDisk(clock=clock, step=20)
    h = Harness(kind, tmp_path, monkeypatch, clock=clock, disk=disk)
    app = h.app
    wal_dir = app._quality_wal.wal_dir
    real_replace, armed = os.replace, {"on": False}

    def replace(src, dst, *a, **k):
        if armed["on"] and str(src).endswith("checkpoint.json.tmp"):
            raise OSError(28, "No space left on device (injected checkpoint replace)")
        return real_replace(src, dst, *a, **k)

    monkeypatch.setattr(os, "replace", replace)
    _emit_blocked(kind, app, 1)                                         # healthy event: checkpoint advances
    armed["on"] = True
    _emit_blocked(kind, app, 12)
    assert app.quality_channel_status()["quality_checkpoint_blocked"] is True
    ids_before, pending_before = _wal_ids(wal_dir), {r["quality_event_id"] for r in QualityEventWAL.recover(wal_dir)}
    ckpt_before, status_before = _disk_checkpoint(wal_dir), app.quality_channel_status()
    bytes_before = sum(f.stat().st_size for f in wal_dir.glob("*.wal"))
    assert len(pending_before) >= 12

    async def scenario():
        if when == "startup":
            disk.fail = OSError(5, "I/O error")
            h.launch()
        else:
            h.launch()
            await asyncio.sleep(0.25)
            assert h.ingest_started
            disk.fail = OSError(5, "I/O error")
            await asyncio.sleep(0.1)
            clock.now += 100
        assert await h.wait_exit() == 69
        h.finalized()
    asyncio.run(scenario())

    assert ids_before <= _wal_ids(wal_dir)                              # nothing deleted
    assert pending_before <= {r["quality_event_id"] for r in QualityEventWAL.recover(wal_dir)}
    assert sum(f.stat().st_size for f in wal_dir.glob("*.wal")) >= bytes_before   # nothing truncated
    assert _disk_checkpoint(wal_dir) == ckpt_before                     # no fabricated checkpoint success
    status = app.quality_channel_status()
    for key in ("quality_events_lost", "quality_events_wal_only", "quality_events_wal_unconfirmed"):
        assert status[key] == status_before[key], key                   # stopping is not loss
    assert status["quality_events_lost"] == 0

    monkeypatch.setattr(os, "replace", real_replace)                    # transient fault is gone: restart
    monkeypatch.setenv(dp.ENV_UNKNOWN_GRACE_S, "60")
    if kind == "futures":
        restarted = _real_futures(tmp_path, monkeypatch)
    else:
        restarted = rbc.BybitCollectorApp(data_dir=str(tmp_path))
    try:
        top = max(int(i.rsplit("-", 1)[1]) for i in pending_before)
        assert restarted._quality_wal.checkpointed_seq >= top           # existing at-least-once recovery
        assert restarted.quality_channel_status()["quality_checkpoint_blocked"] is False
        assert restarted.quality_channel_status()["quality_events_lost"] == 0
        assert QualityEventWAL.recover(restarted._quality_wal.wal_dir) == []
    finally:
        if kind == "futures":
            restarted.shutdown()
        else:
            asyncio.run(restarted.shutdown())
