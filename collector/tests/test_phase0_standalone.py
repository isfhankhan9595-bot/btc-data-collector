"""Phase 0: the collector must stand alone.

``collector.collector.utils`` previously carried a hard, top-level
``from telegram_bot import ...``. That made the whole package unimportable
without a trading-bot file at the repository root, and put an operational
convenience on the ingestion correctness path.
"""
from __future__ import annotations

import ast
import pathlib

import pytest

from collector.collector import notifications


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
COLLECTOR_ROOT = REPO_ROOT / "collector"


def _production_sources():
    for path in COLLECTOR_ROOT.rglob("*.py"):
        if "tests" in path.parts:
            continue
        yield path


def test_no_production_module_hard_imports_the_trading_bot():
    offenders = []
    for path in _production_sources():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                # level > 0 is a relative import and can never be telegram_bot
                names = [] if node.level else [node.module or ""]
            else:
                continue
            for name in names:
                if name.split(".")[0] == "telegram_bot":
                    # Permitted only inside a function body (lazy, optional).
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}")
    # notifications.py imports telegram_bot lazily *inside functions*; those are
    # still found by ast.walk, so filter to module-level imports only.
    module_level = []
    for path in _production_sources():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in tree.body:
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [] if node.level else [node.module or ""]
            else:
                continue
            if any(name.split(".")[0] == "telegram_bot" for name in names):
                module_level.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}")

    assert module_level == [], f"module-level telegram_bot imports: {module_level}"


# ------------------------------------------------------------- notifications

class _Recorder:
    def __init__(self):
        self.messages = []

    def notify(self, message: str) -> bool:
        self.messages.append(message)
        return True


@pytest.fixture(autouse=True)
def _reset_notifier():
    yield
    notifications.set_notifier(None)


def test_injected_notifier_receives_messages():
    recorder = _Recorder()
    notifications.set_notifier(recorder)
    assert notifications.send_alert("hello") is True
    assert recorder.messages == ["hello"]


def test_null_notifier_is_the_default_when_backend_absent(monkeypatch):
    monkeypatch.setattr(notifications, "_resolve_default", lambda: notifications.NullNotifier())
    notifications.set_notifier(None)
    notifications._resolved = False
    assert notifications.send_alert("nobody listening") is False


def test_alert_failure_never_propagates():
    class Exploding:
        def notify(self, message: str) -> bool:
            raise RuntimeError("backend on fire")

    notifications.set_notifier(Exploding())
    # Must fail open: ingestion correctness cannot depend on alert delivery.
    assert notifications.send_alert("boom") is False


def test_gap_detector_alert_failure_does_not_break_gap_tracking(monkeypatch):
    """A failing notifier must not stop the hot path updating its state."""
    from collector.collector.gap_detector import GapDetector

    def exploding_alert(message):
        raise RuntimeError("notifier down")

    monkeypatch.setattr("collector.collector.gap_detector.send_telegram_alert", exploding_alert)

    detector = GapDetector()
    detector.check_gap("trades", 1_000_000)
    # A gap large enough to trigger an alert.
    detector.check_gap("trades", 1_000_000 + 10_000)

    assert detector.last_seen["trades"] == 1_000_000 + 10_000, \
        "hot-path state was lost because an alert raised"
