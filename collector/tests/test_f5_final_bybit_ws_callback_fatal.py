"""F5 final hardening: ``BybitCollectorApp._on_client_quality_event`` and typed fatals.

Defect
------
``WebSocketClient`` calls ``on_quality_event`` UNGUARDED (connect / disconnect /
error paths), so the callback must not raise. It caught every ``Exception`` around
``_emit_quality_event`` and only logged. ``_emit_quality_event`` deliberately lets a
typed ``FatalStorageError`` of ANOTHER stream propagate (it is not quality telemetry),
so that fatal reached the callback's catch-all and was logged and swallowed: Bybit's
``StandaloneFailurePolicy.on_fatal`` was never called. A raw-evidence or unknown
stream fatal therefore never produced the terminal non-zero exit, and a derived
stream fatal never isolated its route -- the default-deny rule was bypassed.

Contract under test (the REAL callback, real policy, real quality WAL / writer)
-------------------------------------------------------------------------------
* a fatal of Bybit's own quality channel degrades quality telemetry only;
* a derived-stream fatal is classified ISOLATE for its own route;
* a raw-evidence or unknown-stream fatal is TERMINATE: terminal latch, client discard
  mode, supervisor wake-up and ``EXIT_FATAL_STORAGE``;
* no foreign fatal is ever latched as a quality-channel failure;
* an ordinary exception keeps the existing ordinary-exception behaviour;
* the callback contains the exception after classifying it (the client calls it
  unguarded), never re-reports it per event and never re-enters the quality writer.
"""
from __future__ import annotations

import asyncio
import logging

import pytest

from collector.collector import notifications
from collector.collector.failure_topology import (
    VERDICT_DEGRADE_QUALITY, VERDICT_ISOLATE, VERDICT_TERMINATE)
from collector.collector.standalone_failure_policy import EXIT_FATAL_STORAGE, supervise_standalone_runner
from collector.collector.storage_errors import FatalStorageError
from collector.run_bybit_collector import BybitCollectorApp
import collector.run_bybit_collector as rbc
from collector.tests.test_bybit_quality_wal import _CapturingLogger, _pending, _wal_records
from collector.tests.test_f5_final_quality_writer_latch import (
    _Disk, _error_events, _spy_on_fatal, _spy_quality_writer)
from collector.tests.test_f5_final_bybit_quality_containment import _cleanup
from collector.tests.test_f5_standalone_runners import SUPERVISED_BOUND_S, _assert_all_closed

QUALITY_STREAM = "bybit_quality_events"
N_EVENTS = 40


@pytest.fixture(autouse=True)
def alerts():
    sent: list[str] = []
    notifications.set_notifier(notifications.CallableNotifier(lambda message: sent.append(message) or True))
    yield sent
    notifications.set_notifier(None)


def _app(tmp_path) -> BybitCollectorApp:
    return BybitCollectorApp(data_dir=str(tmp_path))


def _fatal(stream) -> FatalStorageError:
    return FatalStorageError(f"injected fatal of {stream!r}", stream=stream,
                             component="parquet_writer", stage="rename", durability="unpublished")


def _fail_quality_write_once(monkeypatch, app, exc: BaseException) -> None:
    """The quality writer raises ``exc`` for the FIRST row only, then behaves normally:
    the failure belongs to the event under test, while the policy's own durable report
    of the resulting failure still reaches a working writer."""
    real = app.quality_writer.write
    state = {"armed": True}

    def write(row, **kw):
        if state["armed"]:
            state["armed"] = False
            raise exc
        return real(row, **kw)
    monkeypatch.setattr(app.quality_writer, "write", write)


def _callback(app, reason: str) -> None:
    """One quality event through the runner's REAL callback, exactly as the client calls it."""
    app._on_client_quality_event("DISCONNECT", reason, "bybit-1", "bybit")


def _alerts_for(alerts, stream) -> list[str]:
    return [m for m in alerts if f"stream={stream}" in m]


# ================================================ 1. a quality-channel fatal: degrade telemetry only
def test_quality_channel_fatal_degrades_quality_only(tmp_path, monkeypatch, alerts):
    app = _app(tmp_path)
    try:
        policy, client = app.failure_policy, app.client
        _Disk(monkeypatch).arm(app.quality_writer)             # a REAL ENOSPC on the quality writer
        seen = _spy_on_fatal(policy)

        _callback(app, "t_quality_fatal")                      # must not raise

        record = policy.quality_degraded
        assert record is not None
        assert (record.verdict, record.stream) == (VERDICT_DEGRADE_QUALITY, QUALITY_STREAM)
        assert [type(e) for e in seen] == [FatalStorageError]
        assert policy.terminal_failure is None and not policy.isolated_routes
        assert app.exit_code == 0 and not policy.terminal_event.is_set()
        assert not client.discard_mode
        assert set(policy.failed_components) == {record.key}
        assert len(_alerts_for(alerts, QUALITY_STREAM)) == 1
    finally:
        _cleanup(app)


# ======================================== 2. a derived-stream fatal: isolate THAT route, nothing else
def test_trades_fatal_isolates_the_trades_route_and_is_not_relabelled(tmp_path, monkeypatch, alerts):
    app = _app(tmp_path)
    try:
        policy, client = app.failure_policy, app.client
        fatal = _fatal("bybit_trades")
        _fail_quality_write_once(monkeypatch, app, fatal)
        seen = _spy_on_fatal(policy)

        _callback(app, "t_trades_fatal")                       # must not raise

        assert seen == [fatal], "the callback never handed the typed fatal to the failure policy"
        assert set(policy.isolated_routes) == {"trades"}
        record = policy.isolated_routes["trades"]
        assert (record.verdict, record.stream, record.route) == (VERDICT_ISOLATE, "bybit_trades", "trades")
        assert policy.quality_degraded is None, "a foreign fatal was relabelled as a quality-channel failure"
        assert policy.terminal_failure is None and app.exit_code == 0 and not policy.terminal_event.is_set()
        assert not client.discard_mode, "an isolated derived route must leave the client collecting"
        assert {r.verdict for r in policy.failed_components.values()} == {VERDICT_ISOLATE}
        assert len(_alerts_for(alerts, "bybit_trades")) == 1
        assert app.quality_channel_status()["quality_degraded"] is False
    finally:
        _cleanup(app)


# ==================== 3 + 4. a raw-evidence or unknown-stream fatal: terminal, non-zero exit (default-deny)
@pytest.mark.parametrize("stream", ["bybit_raw_wire", "not_a_bybit_stream", None])
def test_raw_or_unknown_fatal_takes_the_terminal_path(tmp_path, monkeypatch, alerts, stream):
    app = _app(tmp_path)
    try:
        policy, client = app.failure_policy, app.client
        fatal = _fatal(stream)
        _fail_quality_write_once(monkeypatch, app, fatal)
        seen = _spy_on_fatal(policy)

        _callback(app, "t_terminal_fatal")                     # must not raise

        assert seen == [fatal], "the callback never handed the typed fatal to the failure policy"
        record = policy.terminal_failure
        assert record is not None
        assert (record.verdict, record.stream, record.route) == (VERDICT_TERMINATE, stream, None)
        assert record.origin != "raw_frame", "the origin must say where it was really classified"
        assert app.exit_code == EXIT_FATAL_STORAGE != 0
        assert policy.terminal_event.is_set(), "the supervisor was never woken"
        assert client.discard_mode and client.running is False, "terminal mode must stop the client's ingestion"
        assert policy.quality_degraded is None, "a foreign fatal was relabelled as a quality-channel failure"
        assert not policy.isolated_routes
        assert len(_alerts_for(alerts, stream)) == 1
    finally:
        _cleanup(app)


@pytest.mark.parametrize("stream", ["bybit_raw_wire", "not_a_bybit_stream"])
def test_the_supervisor_exits_non_zero_after_the_callback_classified_a_terminal_fatal(tmp_path, monkeypatch, stream):
    app = _app(tmp_path)
    try:
        _fail_quality_write_once(monkeypatch, app, _fatal(stream))

        async def run() -> None:                               # a connection that reports a drop, then idles
            _callback(app, "t_supervised_fatal")
            await asyncio.sleep(3600)
        app.run = run

        async def main() -> int:
            stop = asyncio.Event()
            return await asyncio.wait_for(
                supervise_standalone_runner(app, stop, task_timeout_s=2), min(SUPERVISED_BOUND_S, 15))
        assert asyncio.run(main()) == EXIT_FATAL_STORAGE == app.exit_code
        assert app.failure_policy.terminal_failure.stream == stream
        _assert_all_closed(app)
    finally:
        _cleanup(app)


# ============================================ 5. an ordinary exception keeps the existing behaviour
def test_ordinary_exception_keeps_the_ordinary_exception_behaviour(tmp_path, monkeypatch, alerts):
    app = _app(tmp_path)
    try:
        policy, client = app.failure_policy, app.client
        log = _CapturingLogger()
        monkeypatch.setattr(rbc, "logger", log)
        seen = _spy_on_fatal(policy)

        def boom(row, **kw):
            raise RuntimeError("ordinary bug, not a storage fatal")
        monkeypatch.setattr(app.quality_writer, "write", boom)

        _callback(app, "t_ordinary")                           # must not raise

        assert any(e == "bybit_websocket_quality_event_persist_failed" for e, _ in log.errors)
        assert app._quality_checkpoint_blocked is True
        assert [r["reason"] for r in _pending(tmp_path)] == ["t_ordinary"], "the WAL copy must stay recoverable"
        assert seen == [], "an ordinary exception must never be offered to the failure policy"
        assert not policy.failed_components and policy.quality_degraded is None
        assert policy.terminal_failure is None and not policy.isolated_routes
        assert app.exit_code == 0 and not client.discard_mode and not alerts
    finally:
        _cleanup(app)


# ============================== 6. no duplicate alerts, per-event log flood or recursive writer calls
@pytest.mark.parametrize("stream, verdict", [("bybit_trades", VERDICT_ISOLATE),
                                             ("bybit_raw_wire", VERDICT_TERMINATE)])
def test_repeated_foreign_fatals_are_latched_once_and_never_flood(tmp_path, monkeypatch, alerts, caplog,
                                                                  stream, verdict):
    caplog.set_level(logging.INFO)
    app = _app(tmp_path)
    try:
        policy = app.failure_policy
        seen = _spy_on_fatal(policy)
        real = app.quality_writer.write
        calls: list[int] = []

        def write(row, **kw):
            calls.append(1)
            if row.get("reason", "").startswith("t_flood"):    # only the events under test fail, not the report
                raise _fatal(stream)
            return real(row, **kw)
        monkeypatch.setattr(app.quality_writer, "write", write)

        for i in range(N_EVENTS):
            _callback(app, f"t_flood_{i}")                     # must never raise

        assert len(seen) == N_EVENTS, "every fatal must be classified (idempotently)"
        (record,) = policy.failed_components.values()
        assert record.verdict == verdict and record.stream == stream
        assert sum(policy.failure_repeats.values()) == N_EVENTS - 1, "repeats are counted, not re-reported"
        assert len(_alerts_for(alerts, stream)) == 1, "duplicate operator alerts"
        errors = _error_events(caplog)
        assert errors.count("storage_failure_latched") == 1
        assert "bybit_websocket_quality_event_persist_failed" not in errors, "a classified fatal is not re-logged per event"
        assert policy.quality_degraded is None
        # no recursion into the quality writer: one write per event, plus the policy's ONE durable report
        assert len(calls) == N_EVENTS + 1
        assert sum(1 for r in _wal_records(tmp_path) if "fatal_storage:" in r.get("reason", "")) == 1
    finally:
        _cleanup(app)


def test_quality_channel_fatal_is_not_reclassified_by_the_callback(tmp_path, monkeypatch, alerts):
    """The quality-channel fatal is contained by ``_emit_quality_event`` (latched once, writer never
    called again); the callback must add nothing: no second ``on_fatal``, no extra log, no extra alert."""
    app = _app(tmp_path)
    try:
        policy = app.failure_policy
        _Disk(monkeypatch).arm(app.quality_writer)
        _callback(app, "t_trip")
        assert policy.quality_degraded is not None
        seen = _spy_on_fatal(policy)
        writes = _spy_quality_writer(app)

        for i in range(N_EVENTS):
            _callback(app, f"t_after_{i}")

        assert seen == [] and len(writes) == 0, "the failed quality writer was re-entered"
        assert len(_alerts_for(alerts, QUALITY_STREAM)) == 1
    finally:
        _cleanup(app)
