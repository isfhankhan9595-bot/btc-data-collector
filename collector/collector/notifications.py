"""Optional, best-effort operator notifications.

The collector is a standalone data/perception system. Notifications are an
operational convenience, never part of the ingestion correctness path, so:

* the backend is resolved lazily and optionally -- a missing backend degrades
  to a no-op rather than breaking ``import collector.collector.utils``;
* delivery never raises into a caller, because an alert failure must not
  interrupt market-data processing;
* delivery must not block. A backend that performs synchronous network I/O is
  the caller's problem to avoid; the shipped Telegram backend enqueues.

Nothing here is permitted to import a trading-bot module at collector import
time. The repository-root ``telegram_bot`` module is probed at first use and
treated as absent if it cannot be imported.
"""
from __future__ import annotations

import logging
from typing import Callable, Optional, Protocol

_log = logging.getLogger(__name__)


class Notifier(Protocol):
    """Anything that can deliver an operator message."""

    def notify(self, message: str) -> bool:  # pragma: no cover - protocol
        ...


class NullNotifier:
    """Drops messages. The default when no backend is configured."""

    def notify(self, message: str) -> bool:
        _log.debug("notification dropped (no backend configured): %s", message)
        return False


class CallableNotifier:
    """Adapts a plain callable into the Notifier protocol."""

    def __init__(self, function: Callable[[str], bool]) -> None:
        self._function = function

    def notify(self, message: str) -> bool:
        return bool(self._function(message))


_notifier: Optional[Notifier] = None
_resolved = False


def _resolve_default() -> Notifier:
    """Probe for an optional Telegram backend exactly once.

    Import failure is expected and normal in a standalone deployment, so it is
    logged at debug level and produces a NullNotifier.
    """
    try:
        from telegram_bot import send_telegram_message  # type: ignore[import-not-found]
    except Exception as exc:  # noqa: BLE001 - any import problem means "absent"
        _log.debug("optional telegram backend unavailable: %s", exc)
        return NullNotifier()

    def _send(message: str) -> bool:
        return bool(send_telegram_message(message, parse_mode=None))

    return CallableNotifier(_send)


def set_notifier(notifier: Optional[Notifier]) -> None:
    """Inject a backend (or ``None`` to fall back to auto-resolution)."""
    global _notifier, _resolved
    _notifier = notifier
    _resolved = notifier is not None


def get_notifier() -> Notifier:
    global _notifier, _resolved
    if not _resolved:
        _notifier = _resolve_default()
        _resolved = True
    return _notifier or NullNotifier()


def send_alert(message: str) -> bool:
    """Deliver an operator alert. Never raises, never blocks ingestion."""
    try:
        return bool(get_notifier().notify(message))
    except Exception as exc:  # noqa: BLE001 - alerting must fail open
        _log.warning("notification failed open: %s (message=%s)", exc, message)
        return False


def validate_startup() -> bool:
    """Best-effort startup probe of the optional backend."""
    try:
        from telegram_bot import validate_telegram_startup  # type: ignore[import-not-found]
    except Exception as exc:  # noqa: BLE001
        _log.info("no optional notification backend at startup: %s", exc)
        return False
    try:
        validate_telegram_startup()
        return True
    except Exception as exc:  # noqa: BLE001
        _log.warning("notification startup validation failed open: %s", exc)
        return False
