"""P0-12: the systemd unit must match the repo's real runtime contract.

Static + offline only. Nothing here starts the service; live EC2 behaviour is
NOT VERIFIED (see docs/DEPLOYMENT.md).
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import re
import shutil
import subprocess
import sys

import pytest

PKG = pathlib.Path(__file__).resolve().parents[1]          # <repo>/collector
REPO = PKG.parent
UNIT = PKG / "btc-collector.service"
ASSUMED_USER = "ec2-user"                                   # NOT VERIFIED on a live host


def parse(text: str = None) -> dict:
    """Minimal systemd parser: section -> key -> [values] (repeats allowed)."""
    out, section = {}, None
    for raw in (text if text is not None else UNIT.read_text()).splitlines():
        line = raw.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            out.setdefault(section, {})
            continue
        key, _, value = line.partition("=")
        out[section].setdefault(key.strip(), []).append(value.strip())
    return out


def one(u, section, key):
    vals = u[section][key]
    assert len(vals) == 1, (key, vals)
    return vals[0]


def _all_service_files():
    return sorted(p for p in REPO.rglob("*.service") if ".git" not in p.parts)


def test_expected_service_file_exists():
    services = _all_service_files()
    assert UNIT in services


@pytest.mark.parametrize("unit_path", _all_service_files())
def test_every_tracked_service_file_meets_the_deployment_contract(unit_path):
    """Contract test, not a 'there can only be one unit' assumption: nothing
    in the repository says one systemd unit must cover every runner (see
    docs/DEPLOYMENT.md's Runner coverage section -- only run_collector has
    one today). If a second unit is added later (e.g. for
    run_bybit_collector), it must satisfy this same contract; this test is
    written to run over every *.service file the repo tracks, not just the
    one that exists right now."""
    u = parse(unit_path.read_text())
    assert "Environment" not in u.get("Service", {})
    for bad in ("YOUR_", "/path/to", "BOT_TOKEN="):
        assert bad not in unit_path.read_text()
    assert one(u, "Service", "EnvironmentFile").startswith("/")
    assert not one(u, "Service", "EnvironmentFile").startswith("-")
    assert one(u, "Service", "Restart") == "always"
    assert int(one(u, "Unit", "StartLimitBurst")) >= 1


def test_user_group_are_not_the_wrong_default():
    u = parse()
    assert one(u, "Service", "User") == ASSUMED_USER
    assert one(u, "Service", "Group") == ASSUMED_USER


def test_working_directory_is_this_repo_not_the_trading_bot_or_a_placeholder():
    wd = one(parse(), "Service", "WorkingDirectory")
    assert wd.startswith("/") and "/path/to" not in wd
    assert pathlib.PurePosixPath(wd).name == REPO.name == "btc-data-collector"


def test_execstart_uses_repo_venv_python_and_module_form():
    u = parse()
    wd = one(u, "Service", "WorkingDirectory")
    argv = one(u, "Service", "ExecStart").split()
    assert argv[0] == f"{wd}/.venv/bin/python"          # not /usr/bin/python3
    assert argv[1:] == ["-m", "collector.run_collector"]


def test_execstart_module_exists_is_importable_and_has_main_guard():
    module = "collector.run_collector"
    assert (REPO / "collector" / "run_collector.py").is_file()
    assert 'if __name__ == "__main__"' in (PKG / "run_collector.py").read_text()
    # resolvable from the repo root, exactly as WorkingDirectory makes it
    old = os.getcwd()
    os.chdir(REPO)
    try:
        sys.path.insert(0, str(REPO))
        assert importlib.util.find_spec(module) is not None
    finally:
        sys.path.remove(str(REPO))
        os.chdir(old)


def test_data_and_logs_dirs_are_cwd_relative_so_workdir_must_be_writable_repo_root():
    cfg = (PKG / "collector" / "config.py").read_text()
    assert re.search(r'^DATA_DIR = "data"$', cfg, re.M)
    assert re.search(r'^LOGS_DIR = "logs"$', cfg, re.M)


def test_no_secret_or_placeholder_in_unit_and_env_file_is_required():
    text = UNIT.read_text()
    u = parse()
    assert "Environment" not in u["Service"]            # no inline Environment=
    for bad in ("YOUR_", "/path/to", "BOT_TOKEN=", "AAH"):
        assert bad not in text
    env = one(u, "Service", "EnvironmentFile")
    assert env.startswith("/") and not env.startswith("-")   # missing file => loud failure


def test_bad_deployment_fails_loudly_start_limit_and_restart_policy():
    u = parse()
    assert one(u, "Service", "Restart") == "always"
    assert int(one(u, "Service", "RestartSec")) >= 1
    assert int(one(u, "Unit", "StartLimitBurst")) >= 1
    assert int(one(u, "Unit", "StartLimitIntervalSec")) > int(one(u, "Service", "RestartSec"))


def test_stop_timeout_outlasts_the_p0_1_websocket_drain():
    ws_source = (PKG / "collector" / "websocket_client.py").read_text()
    # The drain timeout is now a named attribute (shutdown_drain_timeout_s),
    # not an inline literal passed straight to wait_for -- matches the
    # repository's current shape, re-verified against source rather than
    # assumed unchanged.
    drain_attr = re.search(r"self\.shutdown_drain_timeout_s = (\d+(?:\.\d+)?)", ws_source)
    assert drain_attr, "P0-1 drain timeout attribute not found; shutdown contract changed"
    assert "timeout=self.shutdown_drain_timeout_s" in ws_source, (
        "drain wait no longer uses the shutdown_drain_timeout_s attribute; re-check this test"
    )
    u = parse()
    assert int(one(u, "Service", "TimeoutStopSec")) > float(drain_attr.group(1))
    assert one(u, "Service", "KillSignal") == "SIGTERM"
    # the runner really handles SIGTERM itself
    assert "signal.SIGTERM" in (PKG / "run_collector.py").read_text()


def test_network_ordering_waits_for_online():
    u = parse()
    assert one(u, "Unit", "After") == "network-online.target"
    assert one(u, "Unit", "Wants") == "network-online.target"


def test_requirements_file_referenced_by_docs_exists():
    assert (PKG / "requirements.txt").is_file()
    assert "collector/requirements.txt" in (REPO / "docs" / "DEPLOYMENT.md").read_text()


@pytest.mark.skipif(shutil.which("systemd-analyze") is None, reason="systemd-analyze unavailable")
def test_systemd_analyze_verify_on_path_remapped_copy(tmp_path):
    """Syntax/directive validity via the real systemd parser. Paths are
    remapped into a temp tree because the EC2 layout does not exist here."""
    u = parse()
    wd = one(u, "Service", "WorkingDirectory")
    fake_repo = tmp_path / "btc-data-collector"
    py = fake_repo / ".venv" / "bin" / "python"
    py.parent.mkdir(parents=True)
    py.write_text("#!/bin/sh\nexit 0\n")
    py.chmod(0o755)
    env = tmp_path / "collector.env"
    env.write_text("")
    text = UNIT.read_text().replace(wd, str(fake_repo)).replace(
        "/etc/btc-collector.env", str(env))
    unit = tmp_path / "btc-collector.service"
    unit.write_text(text)
    r = subprocess.run(["systemd-analyze", "verify", str(unit)],
                       capture_output=True, text=True)
    # a missing ec2-user in the sandbox is an environment fact, not a unit defect
    problems = [l for l in (r.stdout + r.stderr).splitlines()
                if l.strip() and "ec2-user" not in l and "network-online" not in l]
    assert problems == [], problems


# ---------------------------------------------------------------------------
# F5-QA-04: exit status 69 (disk monitor unavailable) must not restart-loop.
#
# EVIDENCE LEVELS. Static: the unit text and the parser (systemd-analyze verify above). Arithmetic:
# the start-limit calculation below, on systemd's documented StartLimit semantics. NOT VERIFIED on a
# real host: no live systemd was available, so the failed-state / restart behaviour is inferred.
# ---------------------------------------------------------------------------
def _exit_status_tokens(u, key):
    return {tok for val in u["Service"].get(key, []) for tok in val.split()}


def test_unit_prevents_restart_for_exactly_exit_69():
    from collector.collector.disk_pressure import EXIT_DISK_MONITOR_UNAVAILABLE
    u = parse()
    assert one(u, "Service", "RestartPreventExitStatus") == "69"
    assert EXIT_DISK_MONITOR_UNAVAILABLE == 69          # the unit and the application agree on the status


def test_restart_and_start_limit_settings_are_unchanged():
    u = parse()
    assert one(u, "Service", "Restart") == "always"
    assert one(u, "Service", "RestartSec") == "5"
    assert one(u, "Unit", "StartLimitIntervalSec") == "300"
    assert one(u, "Unit", "StartLimitBurst") == "5"
    assert "RestartForceExitStatus" not in u["Service"]


def test_restart_prevention_does_not_cover_exit_1_exit_70_or_unrelated_failures():
    from collector.collector.disk_pressure import EXIT_DISK_PRESSURE
    from collector.collector.failure_topology import EXIT_FATAL_STORAGE
    u = parse()
    prevented = _exit_status_tokens(u, "RestartPreventExitStatus")
    assert prevented == {"69"}                           # nothing else: no 1, no 70, no signals, no 0
    assert str(EXIT_DISK_PRESSURE) not in prevented and str(EXIT_FATAL_STORAGE) not in prevented
    assert one(u, "Service", "Restart") == "always"      # so every other exit still restarts
    # 69 must remain a FAILURE for systemd (visible as `failed`), never a "success" status.
    assert "69" not in _exit_status_tokens(u, "SuccessExitStatus")


def test_application_exit_precedence_is_70_over_1_over_69():
    import types
    import collector.run_collector as rc
    from collector.collector.disk_pressure import EXIT_DISK_MONITOR_UNAVAILABLE, EXIT_DISK_PRESSURE
    from collector.collector.failure_topology import EXIT_FATAL_STORAGE

    def code(terminal, emergency, unavailable):
        stub = types.SimpleNamespace(_ensure_failure_state=lambda: None,
                                     terminal_failure=object() if terminal else None,
                                     _disk_emergency={"x": 1} if emergency else None,
                                     _disk_monitor_unavailable={"x": 1} if unavailable else None)
        return rc.CollectorApp.exit_code.fget(stub)

    assert (EXIT_FATAL_STORAGE, EXIT_DISK_PRESSURE, EXIT_DISK_MONITOR_UNAVAILABLE) == (70, 1, 69)
    assert code(True, True, True) == code(True, False, True) == code(True, True, False) == 70
    assert code(False, True, True) == code(False, True, False) == 1
    assert code(False, False, True) == 69
    assert code(False, False, False) == 0


def test_start_limit_cannot_contain_exit_69_which_is_why_it_is_prevented_explicitly():
    """Arithmetic, not a host measurement. systemd allows StartLimitBurst starts per StartLimitIntervalSec,
    so containment needs the 6th start inside the window, i.e. a restart cycle <= interval / burst."""
    from collector.collector.disk_pressure import DiskPressureConfig
    u = parse()
    interval = float(one(u, "Unit", "StartLimitIntervalSec"))
    burst = int(one(u, "Unit", "StartLimitBurst"))
    restart_sec = float(one(u, "Service", "RestartSec"))
    grace = DiskPressureConfig().unknown_grace_s
    max_containing_cycle = interval / burst                       # 60 s
    cycle_69 = grace + restart_sec                                # lower bound: startup/shutdown only add
    cycle_1 = restart_sec + 2                                     # confirmed emergency exits within seconds
    assert cycle_69 > max_containing_cycle, "start limit would contain exit 69 now; revisit the directive"
    assert cycle_1 <= max_containing_cycle                        # exit 1 is still contained by the start limit
    # simulate systemd's fixed start window for both cycles
    def blocked_at(cycle):
        begin, num = None, 0
        for i in range(60):
            t = i * cycle
            if begin is None or t - begin > interval:
                begin, num = t, 1
            elif num < burst:
                num += 1
            else:
                return t
        return None
    assert blocked_at(cycle_1) is not None and blocked_at(cycle_69) is None


def test_documentation_describes_the_failed_state_and_the_recovery_action():
    for doc in ("DEPLOYMENT.md", "F5_QA_04_STORAGE_PRESSURE.md"):
        text = (REPO / "docs" / doc).read_text()
        assert "RestartPreventExitStatus=69" in text, doc
        assert "failed" in text and "systemctl restart btc-collector" in text, doc
        assert "not verified" in text.lower() or "unverified" in text.lower(), doc
    storage = (REPO / "docs" / "F5_QA_04_STORAGE_PRESSURE.md").read_text()
    assert "Bybit" in storage and "UNKNOWN" in storage and "no hard wal cap" in storage.lower()
