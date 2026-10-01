from __future__ import annotations

import re
from pathlib import Path

import pytest

import telegram_bot


def _reset() -> None:
    telegram_bot.TelegramConfigManager.reset_for_tests()


def test_telegram_config_reads_environment_only(monkeypatch: pytest.MonkeyPatch) -> None:
    _reset()
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcde")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123456789")

    config = telegram_bot.load_telegram_config(validate=False)

    assert config.token == "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcde"
    assert config.chat_id == "123456789"
    assert config.enabled is (telegram_bot.requests is not None)


def test_missing_telegram_credentials_disable_without_crash(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _reset()
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    with caplog.at_level("WARNING"):
        config = telegram_bot.validate_telegram_startup()

    assert config.enabled is False
    assert "Telegram disabled" in caplog.text


def test_valid_telegram_credentials_preserve_notification_behavior(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Response:
        status_code = 200

        @staticmethod
        def json() -> dict[str, bool]:
            return {"ok": True}

    calls: list[tuple[str, dict[str, str], tuple[int, int]]] = []

    class _Requests:
        @staticmethod
        def post(url: str, json: dict[str, str], timeout: tuple[int, int]) -> _Response:
            calls.append((url, json, timeout))
            return _Response()

    _reset()
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcde")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123456789")
    monkeypatch.setattr(telegram_bot, "requests", _Requests())

    assert telegram_bot.send_test_telegram_alert() is True
    assert calls, "Telegram request was not issued"
    assert calls[0][0].startswith("https://api.telegram.org/bot")
    assert calls[0][1]["chat_id"] == "123456789"


def test_telegram_bot_module_has_no_hardcoded_credentials() -> None:
    source = Path(telegram_bot.__file__).read_text(encoding="utf-8")
    pattern = re.compile(
        r"^\s*(?:TELEGRAM_BOT_TOKEN|TELEGRAM_CHAT_ID|BOT_TOKEN|CHAT_ID)\s*=\s*[\"'][^\"']+[\"']",
        re.MULTILINE,
    )
    assert pattern.search(source) is None


def test_no_production_python_source_contains_telegram_bot_token_pattern() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    token_like = re.compile(r"\b\d{8,10}:[A-Za-z0-9_-]{30,}\b")

    offenders: list[str] = []
    for path in repo_root.rglob("*.py"):
        rel = path.relative_to(repo_root)
        if "tests" in rel.parts:
            continue
        if token_like.search(path.read_text(encoding="utf-8")):
            offenders.append(str(rel))

    assert offenders == []
