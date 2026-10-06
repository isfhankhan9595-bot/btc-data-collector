"""Regression tests: Telegram credentials come from the environment only.

No test here contacts the Telegram API, and none needs a real credential.
Synthetic values are built at runtime so no token-shaped literal exists in
this file (the repository-wide scan below would otherwise flag it).
"""
from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TELEGRAM_SRC = REPO_ROOT / "telegram_bot.py"

# Real-looking Telegram bot token shape: <8-10 digits>:<34+ url-safe chars>.
TOKEN_LITERAL_RE = re.compile(r"[0-9]{8,10}:[A-Za-z0-9_-]{34,}")
FORBIDDEN_NAMES = {"TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "BOT_TOKEN", "CHAT_ID"}


def _fake_token() -> str:
    return "1" * 9 + ":" + "A" * 35


def _fake_chat_id() -> str:
    return "-" + "1" * 10


@pytest.fixture(autouse=True)
def isolated_telegram(monkeypatch, tmp_path):
    """No ambient env, no .env file, no cached config, no real network."""
    import telegram_bot

    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(telegram_bot.TelegramConfigManager, "_dotenv_loaded", True)
    telegram_bot.TelegramConfigManager.reset_for_tests()
    telegram_bot.TelegramConfigManager._dotenv_loaded = True

    def _no_network(*args, **kwargs):  # pragma: no cover - must never run
        raise AssertionError("test attempted a real Telegram HTTP call")

    if telegram_bot.requests is not None:
        monkeypatch.setattr(telegram_bot.requests, "post", _no_network)
    yield telegram_bot
    telegram_bot.TelegramConfigManager.reset_for_tests()


# --- 1. credentials are read from the environment --------------------------

def test_credentials_are_read_from_environment(monkeypatch, isolated_telegram):
    tb = isolated_telegram
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", _fake_token())
    monkeypatch.setenv("TELEGRAM_CHAT_ID", _fake_chat_id())
    cfg = tb.load_telegram_config(validate=True)
    assert cfg.enabled is True
    assert cfg.token == _fake_token()
    assert cfg.chat_id == _fake_chat_id()


def test_env_names_are_the_documented_contract(isolated_telegram):
    assert isolated_telegram.TELEGRAM_TOKEN_ENV == "TELEGRAM_BOT_TOKEN"
    assert isolated_telegram.TELEGRAM_CHAT_ID_ENV == "TELEGRAM_CHAT_ID"


# --- 2. no hard-coded source fallback ---------------------------------------

def test_module_exposes_no_credential_constants(isolated_telegram):
    for name in FORBIDDEN_NAMES:
        assert not hasattr(isolated_telegram, name), f"{name} must not exist in source"


def test_source_has_no_credential_assignment_or_getenv_default():
    tree = ast.parse(TELEGRAM_SRC.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in FORBIDDEN_NAMES:
                    pytest.fail(f"module-level credential assignment: {target.id}")
        if isinstance(node, ast.Call):
            func = node.func
            is_getenv = (isinstance(func, ast.Attribute) and func.attr in {"getenv", "get"}) or (
                isinstance(func, ast.Name) and func.id == "getenv"
            )
            if is_getenv and node.args and isinstance(node.args[0], ast.Constant):
                if node.args[0].value in {"TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"}:
                    pytest.fail("credential env lookup with a literal name")
    src = TELEGRAM_SRC.read_text(encoding="utf-8")
    assert not TOKEN_LITERAL_RE.search(src)


def test_getenv_default_for_credentials_is_empty_not_a_source_value():
    tree = ast.parse(TELEGRAM_SRC.read_text(encoding="utf-8"))
    seen = 0
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "getenv"
        ):
            seen += 1
            assert len(node.args) == 2
            default = node.args[1]
            assert isinstance(default, ast.Constant) and default.value == ""
    assert seen == 2


def _tracked_text_files():
    out = subprocess.run(
        ["git", "ls-files"], cwd=REPO_ROOT, capture_output=True, text=True, check=False
    )
    if out.returncode != 0:
        pytest.skip("not a git checkout")
    for rel in out.stdout.splitlines():
        path = REPO_ROOT / rel
        if path.suffix in {".py", ".md", ".service", ".yml", ".yaml", ".txt", ".cfg", ".toml", ".ini", ".env", ".json", ""}:
            try:
                yield rel, path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue


def test_no_token_shaped_literal_in_any_tracked_text_file():
    offenders = [rel for rel, text in _tracked_text_files() if TOKEN_LITERAL_RE.search(text)]
    assert offenders == [], f"token-shaped literal in: {offenders}"


# --- 3. missing/invalid credentials never fall back --------------------------

def test_missing_credentials_disable_without_fallback(isolated_telegram):
    cfg = isolated_telegram.load_telegram_config(validate=False)
    assert cfg.enabled is False
    assert cfg.token == "" and cfg.chat_id == ""
    assert "missing token" in cfg.disabled_reason
    assert "missing chat_id" in cfg.disabled_reason


def test_explicit_empty_arguments_do_not_fall_back_to_anything(isolated_telegram):
    cfg = isolated_telegram.load_telegram_config(token="", chat_id="", validate=False)
    assert cfg.enabled is False and cfg.token == ""


def test_explicit_request_fails_closed_when_unconfigured(isolated_telegram):
    with pytest.raises(isolated_telegram.TelegramConfigError) as exc:
        isolated_telegram.load_telegram_config(validate=True)
    assert "TELEGRAM_BOT_TOKEN" in str(exc.value)
    assert "TELEGRAM_CHAT_ID" in str(exc.value)


def test_test_alert_diagnostic_fails_closed_when_unconfigured(isolated_telegram):
    with pytest.raises(isolated_telegram.TelegramConfigError):
        isolated_telegram.send_test_telegram_alert()


@pytest.mark.parametrize("bad_token", ["YOUR_BOT_TOKEN", "abc", "123456:short", "not a token"])
def test_placeholder_or_malformed_token_is_rejected(monkeypatch, isolated_telegram, bad_token):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", bad_token)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", _fake_chat_id())
    cfg = isolated_telegram.load_telegram_config(validate=False)
    assert cfg.enabled is False
    assert "invalid token format" in cfg.disabled_reason
    with pytest.raises(isolated_telegram.TelegramConfigError):
        isolated_telegram.TelegramConfigManager.reset_for_tests()
        isolated_telegram.TelegramConfigManager._dotenv_loaded = True
        isolated_telegram.load_telegram_config(validate=True)


def test_malformed_chat_id_is_rejected(monkeypatch, isolated_telegram):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", _fake_token())
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID")
    cfg = isolated_telegram.load_telegram_config(validate=False)
    assert cfg.enabled is False
    assert "invalid chat_id format" in cfg.disabled_reason


def test_secrets_not_in_config_repr_or_disabled_state(monkeypatch, isolated_telegram):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", _fake_token())
    monkeypatch.setenv("TELEGRAM_CHAT_ID", _fake_chat_id())
    cfg = isolated_telegram.load_telegram_config(validate=False)
    assert _fake_token() not in repr(cfg)
    assert _fake_chat_id() not in repr(cfg)


# --- 4. valid env credentials reach the client correctly ---------------------

class _FakeResponse:
    status_code = 200

    @staticmethod
    def json():
        return {"ok": True}


class _FakeRequests:
    def __init__(self):
        self.calls = []

    def post(self, url, json=None, timeout=None):
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        return _FakeResponse()


def test_env_credentials_are_passed_to_telegram_client(monkeypatch, isolated_telegram):
    tb = isolated_telegram
    fake = _FakeRequests()
    monkeypatch.setattr(tb, "requests", fake)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", _fake_token())
    monkeypatch.setenv("TELEGRAM_CHAT_ID", _fake_chat_id())
    tb.TelegramConfigManager.reset_for_tests()
    tb.TelegramConfigManager._dotenv_loaded = True

    assert tb._post_telegram_once("hello", parse_mode=None) is True
    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["url"] == f"https://api.telegram.org/bot{_fake_token()}/sendMessage"
    assert call["json"] == {"chat_id": _fake_chat_id(), "text": "hello"}


def test_unconfigured_never_reaches_the_client(monkeypatch, isolated_telegram):
    tb = isolated_telegram
    fake = _FakeRequests()
    monkeypatch.setattr(tb, "requests", fake)
    assert tb._post_telegram_once("hello") is False
    assert tb.send_telegram_message("hello") is False
    assert fake.calls == []


# --- 5. isolation needs no real credential -----------------------------------

def test_isolation_fixture_leaves_no_ambient_credentials(isolated_telegram):
    import os

    assert "TELEGRAM_BOT_TOKEN" not in os.environ
    assert "TELEGRAM_CHAT_ID" not in os.environ
    assert isolated_telegram.load_telegram_config(validate=False).enabled is False


# --- 6. collector keeps working with Telegram optional and disabled ----------

def test_collector_alert_path_fails_open_when_disabled(isolated_telegram):
    from collector.collector import notifications
    from collector.collector.utils import send_telegram_alert, validate_telegram_startup

    notifications.set_notifier(None)
    try:
        assert send_telegram_alert("collector message") is False  # no raise, no send
        validate_telegram_startup()  # must not raise
        assert isolated_telegram.get_telegram_health()["enabled"] is False
    finally:
        notifications.set_notifier(None)


def test_startup_validation_does_not_print_chat_id(monkeypatch, capsys, isolated_telegram):
    tb = isolated_telegram
    monkeypatch.setattr(tb, "requests", _FakeRequests())
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", _fake_token())
    monkeypatch.setenv("TELEGRAM_CHAT_ID", _fake_chat_id())
    monkeypatch.setattr(tb, "send_telegram_message", lambda *a, **k: False)
    tb.TelegramConfigManager.reset_for_tests()
    tb.TelegramConfigManager._dotenv_loaded = True
    tb.validate_telegram_startup()
    out = capsys.readouterr().out
    assert _fake_chat_id() not in out
    assert _fake_token() not in out
