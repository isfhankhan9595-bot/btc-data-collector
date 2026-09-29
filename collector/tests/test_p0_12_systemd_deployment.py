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


def test_only_one_service_file_exists():
    found = [p for p in REPO.rglob("*.service") if ".git" not in p.parts]
    assert found == [UNIT]


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
    drain = re.search(r"wait_for\(self\._ingest_queue\.join\(\), timeout=(\d+(?:\.\d+)?)\)",
                      (PKG / "collector" / "websocket_client.py").read_text())
    assert drain, "P0-1 drain timeout not found; shutdown contract changed"
    u = parse()
    assert int(one(u, "Service", "TimeoutStopSec")) > float(drain.group(1))
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
