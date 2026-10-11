"""F5-QA-04: continuous storage-pressure monitoring for the WAL-backed runners (Futures + Bybit).

Phase A finding (kept as characterization tests below, see ``test_phase_a_*``)
-----------------------------------------------------------------------------
``QualityEventWAL.max_bytes`` is a per-file rotation threshold, not a total-size bound. After a
checkpoint failure latches checkpointing off, ``maybe_rotate_for_size`` is never reached again and
the WAL grows without bound; the old ``DiskMonitor`` ran once in the Futures constructor only and
Bybit had no disk monitoring at all.

Policy under test (approved decisions)
--------------------------------------
No hard WAL cap and nothing here deletes evidence; configurable 7/4/2 GiB thresholds checked every
15 s off the hot path; one alert per severity entry with hysteresis; measurement failure is an
explicit UNKNOWN state; EMERGENCY stops collection through each runner's controlled lifecycle with
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
    return [m for m in sent if m.startswith("DISK PRESSURE ") and "MONITOR ERROR" not in m]


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
    """Phase A evidence retained: rotation is a per-file threshold, not a total-size bound."""
    wal = rig.app._quality_wal
    wal.max_bytes = 800
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
        rotations_before = calls["rotate"]
        rig.armed = True
        rig.emit_many("blocked", 60)
        assert rig.status()["quality_checkpoint_blocked"] is True
        assert calls["rotate"] == rotations_before        # latched: rotation is never reached again
    finally:
        QualityEventWAL.maybe_rotate_for_size = real
    blocked_bytes, blocked_files = _wal_dir_bytes_and_count(rig)
    assert blocked_bytes > 10 * 800                                      # far beyond max_bytes
    assert blocked_files <= 2                                            # rotation did not even split it
    rig.app.storage_pressure.poll()
    status = rig.app.storage_pressure_status()
    assert status["quality_wal_bytes"] == blocked_bytes and status["quality_wal_file_count"] == blocked_files
    assert status["quality_checkpoint_blocked"] is True
    assert status["quality_checkpoint_block_reason"] == "checkpoint_write_failed:OSError"
    assert status["wal_total_size_bounded"] is False


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

    def __init__(self):
        self.started = False

    async def start(self):
        self.started = True
        await asyncio.sleep(3600)

    async def wait_connected(self, timeout_seconds=0):
        return True

    def stop(self):
        pass

    def enter_discard_mode(self, why):
        pass


def _stub_futures_network(app):
    app.ws_clients = [_StubWS()]

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
    def __init__(self):
        self.started = False
        self.stopped = False

    async def start(self):
        self.started = True
        await asyncio.sleep(3600)

    def stop(self):
        self.stopped = True

    def enter_discard_mode(self, why):
        pass


def _real_bybit(tmp_path):
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


def test_runtime_monitor_error_in_a_runner_reports_unknown_and_never_stops_collection(tmp_path):
    app = _real_bybit(tmp_path)
    disk = FakeDisk(50)
    disk.fail = OSError(5, "I/O error")
    app.storage_pressure._usage = disk

    async def scenario():
        stop = asyncio.Event()
        runner = asyncio.create_task(rbc._run_supervised(app, stop))
        await asyncio.sleep(0.3)
        assert not runner.done() and app.exit_code == 0
        assert app.storage_pressure_status()["severity"] == "UNKNOWN"
        disk.fail = None
        await asyncio.sleep(0.15)
        assert app.storage_pressure_status()["severity"] == "OK"
        stop.set()
        return await asyncio.wait_for(runner, 30)
    assert asyncio.run(scenario()) == 0
