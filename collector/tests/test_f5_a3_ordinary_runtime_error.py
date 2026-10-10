"""F5 A3: an ordinary ``RuntimeError`` in the quality-event path is NOT a ``FatalStorageError``.

Why this needs its own tests
----------------------------
``FatalStorageError`` subclasses ``RuntimeError`` (so pre-F5 callers that handled a failed
writer's refusal as ``RuntimeError`` keep working). Classification must therefore use the
TYPED exception (``isinstance(exc, FatalStorageError)`` plus the failed writer's own
``stream``), never the base class, never a message substring, never a class-name string and
never "has a ``.stream`` attribute". A regression to any of those would make an ordinary
handler/writer bug look like a storage failure: a false quality-channel latch, false
``quality_events_lost`` / ``quality_events_unconfirmed`` counts, a spurious alert, and, in the
worst case, a false terminal shutdown or route isolation.

Established behaviour asserted here (Binance Spot, OKX collector, OKX capture-only)
-----------------------------------------------------------------------------------
``_persist_quality_event`` keeps its pre-F5 ordinary-exception path: log
``<prefix>_quality_write_failed`` and continue. Specifically, for a plain ``RuntimeError``
raised by ``quality_writer.write``:

* it does not propagate out of the quality callback (quality failures never break ingest);
* ``failure_policy`` latches nothing: ``quality_degraded``, ``terminal_failure``,
  ``failed_components``, ``isolated_routes`` all empty, ``exit_code`` 0, ``terminal_event``
  unset, the client not in discard mode, ``on_fatal`` never called, no alert;
* ``quality_events_lost`` and ``quality_events_unconfirmed`` stay 0 and the operator status
  reports a healthy channel;
* the writer is NOT treated as failed: it is called again for the next event (an ordinary
  error is not a latch), and once it works again the next event is durably published;
* market-data / raw capture keeps flowing and a supervised run is not shut down by it.

Real seams, no mocked classification
------------------------------------
The primary seam is a REAL plain ``RuntimeError``: ``ParquetWriter.write`` on a CLOSED quality
writer raises one ("closing is not a storage failure"). Further cases inject look-alikes at
``quality_writer.write``: a plain error, one whose message imitates a fatal, one carrying the
typed fatal's attributes (``stream``, ``stage``, ...) and one whose class is literally named
``FatalStorageError`` but is not the real type. A positive control on the SAME seam, differing
only in the exception type, proves the harness does discriminate. Everything runs through the
real runners, the real ``WebSocketClient`` quality callback / worker, the real
``StandaloneFailurePolicy`` and the real ``ParquetWriter``.
"""
from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from collector.collector import notifications
from collector.collector import parquet_writer as pw_module
from collector.collector.failure_topology import VERDICT_DEGRADE_QUALITY
from collector.collector.standalone_failure_policy import supervise_standalone_runner
from collector.collector.storage_errors import FatalStorageError
from collector.tests.test_f5_a1_startup_quality_failure import _is_quality_stream, _seed_orphans
from collector.tests.test_f5_final_quality_writer_latch import (
    KINDS, _cleanup_any, _client, _emit_quality, _error_events, _frame_kind, _make, _run_frames,
    _spy_on_fatal, _spy_quality_writer)
from collector.tests.test_f5_standalone_runners import _raw_frames

#: ``logger.error`` event name an ordinary quality-write failure produces, per runner.
LOG_EVENT = {"spot": "spot_quality_write_failed", "okx": "okx_quality_write_failed",
             "okx_capture": "okx_quality_write_failed"}
HEALTHY_STATUS = {"quality_degraded": False, "quality_events_lost": 0,
                  "quality_events_unconfirmed": 0, "quality_wal": False, "quality_failure": None}


@pytest.fixture(autouse=True)
def alerts():
    sent: list[str] = []
    notifications.set_notifier(notifications.CallableNotifier(lambda message: sent.append(message) or True))
    yield sent
    notifications.set_notifier(None)


# --------------------------------------------------------------------------- impostors
def _fatal_lookalike_attrs(exc: BaseException, stream: str) -> BaseException:
    exc.stream, exc.component, exc.stage, exc.durability = stream, "parquet_writer", "rename", "unpublished"
    exc.record_accepted = True
    return exc


def _plain(stream: str) -> BaseException:
    return RuntimeError("quality writer exploded")


def _message_mimic(stream: str) -> BaseException:
    return RuntimeError(f"ParquetWriter for {stream!r} is FAILED (FatalStorageError): No space left on device")


def _attribute_mimic(stream: str) -> BaseException:
    return _fatal_lookalike_attrs(RuntimeError("looks typed, is not"), stream)


def _name_mimic(stream: str) -> BaseException:
    impostor = type("FatalStorageError", (RuntimeError,), {})   # same name, NOT the real class
    return _fatal_lookalike_attrs(impostor("same class name, different class"), stream)


IMPOSTORS = {"plain": _plain, "message_mimic": _message_mimic,
             "attribute_mimic": _attribute_mimic, "name_mimic": _name_mimic}


@pytest.mark.parametrize("name", sorted(IMPOSTORS))
def test_the_impostors_really_are_untyped_runtime_errors(name):
    exc = IMPOSTORS[name]("spot_quality_events")
    assert isinstance(exc, RuntimeError) and not isinstance(exc, FatalStorageError)
    assert type(exc) is not FatalStorageError


# --------------------------------------------------------------------------- helpers
def _arm_raising(app, make_exc) -> list[str]:
    """Make ``quality_writer.write`` raise ``make_exc(stream)`` on every call; return the
    ``reason`` of every call made."""
    calls: list[str] = []
    stream = app.quality_writer.stream_name

    def write(record, *a, **k):
        calls.append(record["reason"])
        raise make_exc(stream)
    app.quality_writer.write = write
    return calls


def _arm_one_shot(app, make_exc) -> list[str]:
    """Raise ``make_exc`` for the FIRST call only, then delegate to the real writer."""
    calls: list[str] = []
    real, stream = app.quality_writer.write, app.quality_writer.stream_name

    def write(record, *a, **k):
        calls.append(record["reason"])
        if len(calls) == 1:
            raise make_exc(stream)
        return real(record, *a, **k)
    app.quality_writer.write = write
    return calls


def _spy_raised(app) -> list[BaseException]:
    """Record every exception the REAL quality writer raises, unchanged."""
    seen: list[BaseException] = []
    real = app.quality_writer.write

    def write(*a, **k):
        try:
            return real(*a, **k)
        except BaseException as exc:  # noqa: BLE001 - observe only
            seen.append(exc)
            raise
    app.quality_writer.write = write
    return seen


def _published_reasons(writer) -> list[str]:
    reasons: list[str] = []
    for seg in sorted(Path(writer.stream_dir).rglob("*.seg")):
        reasons += [row["reason"] for row in pq.read_table(seg).to_pylist()]
    return reasons


def _assert_nothing_misclassified(app, alerts, latches=None) -> None:
    """The ordinary exception left the failure policy, the counters and the alert channel
    exactly as a healthy runner has them."""
    policy, client = app.failure_policy, _client(app)
    assert app.quality_degraded is None and policy.quality_degraded is None, "false quality latch"
    assert policy.terminal_failure is None and app.exit_code == 0, "false terminal verdict"
    assert not policy.terminal_event.is_set(), "false terminal wake-up of the supervisor"
    assert policy.failed_components == {} and policy.failure_repeats == {}
    assert policy.isolated_routes == {} and policy.route_short_circuits == {}, "false route isolation"
    assert not client.discard_mode, "client put in terminal discard mode"
    assert app.quality_events_lost == 0, "false quality_events_lost"
    assert app.quality_events_unconfirmed == 0, "false quality_events_unconfirmed"
    assert app.quality_channel_status() == HEALTHY_STATUS
    assert alerts == [], f"a storage alert was raised for an ordinary error: {alerts}"
    if latches is not None:
        assert latches == [], "on_fatal was reached by an ordinary exception"


def _assert_logged_and_continued(app, caplog, events: int) -> None:
    names = _error_events(caplog)
    assert names.count(LOG_EVENT[_kind_of(app)]) == events, "ordinary path is log-and-continue, once per event"
    assert "storage_failure_latched" not in names


def _kind_of(app) -> str:
    return "okx_capture" if hasattr(app, "capture") else ("spot" if type(app).__name__.startswith("Binance") else "okx")


def _rows(writer) -> int:
    return writer.record_count + len(writer.buffer)


# =============================================== 1. a REAL plain RuntimeError: the closed quality writer
@pytest.mark.parametrize("kind", KINDS)
def test_closed_quality_writer_runtime_error_is_ordinary_log_and_continue(kind, tmp_path, monkeypatch, alerts, caplog):
    """``ParquetWriter.write`` on a CLOSED writer raises a plain ``RuntimeError`` (it is not a
    storage failure). Driven through the runner's real client quality callback."""
    caplog.set_level(logging.INFO)
    app = _make(kind, tmp_path, monkeypatch)
    try:
        app.quality_writer.close()
        raised = _spy_raised(app)
        calls = _spy_quality_writer(app)
        latches = _spy_on_fatal(app.failure_policy)

        for i in range(3):
            _emit_quality(app, i)                    # must not raise out of the callback

        assert len(raised) == 3 and all(type(e) is RuntimeError for e in raised), \
            "the seam must be a genuine plain RuntimeError from the real writer"
        assert not any(isinstance(e, FatalStorageError) for e in raised)
        assert len(calls) == 3, "not a failed writer: it is called again for every event (no latch, no short-circuit)"
        _assert_nothing_misclassified(app, alerts, latches)
        _assert_logged_and_continued(app, caplog, 3)
    finally:
        _cleanup_any(app)


# =============================================== 2. look-alikes injected at quality_writer.write
@pytest.mark.parametrize("impostor", sorted(IMPOSTORS))
@pytest.mark.parametrize("kind", KINDS)
def test_untyped_runtime_error_is_never_classified_as_a_storage_fatal(kind, impostor, tmp_path, monkeypatch, alerts, caplog):
    caplog.set_level(logging.INFO)
    app = _make(kind, tmp_path, monkeypatch)
    try:
        calls = _arm_raising(app, IMPOSTORS[impostor])
        latches = _spy_on_fatal(app.failure_policy)

        for i in range(5):
            _emit_quality(app, i)

        assert calls == [f"drop-{i}" for i in range(5)], "every event reaches the writer: nothing is short-circuited"
        _assert_nothing_misclassified(app, alerts, latches)
        _assert_logged_and_continued(app, caplog, 5)
    finally:
        _cleanup_any(app)


# =============================================== 3. the channel is not poisoned: it recovers
@pytest.mark.parametrize("impostor", ["plain", "attribute_mimic"])
@pytest.mark.parametrize("kind", KINDS)
def test_quality_channel_keeps_persisting_after_an_ordinary_runtime_error(kind, impostor, tmp_path, monkeypatch, alerts):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        calls = _arm_one_shot(app, IMPOSTORS[impostor])
        _emit_quality(app, 0)                         # raises RuntimeError inside the writer
        _emit_quality(app, 1)
        _emit_quality(app, 2)

        assert calls == ["drop-0", "drop-1", "drop-2"], "the writer was retried: it is not latched"
        published = _published_reasons(app.quality_writer)
        assert published == ["drop-1", "drop-2"], "later events are durably persisted; only the failed call is absent"
        _assert_nothing_misclassified(app, alerts)
    finally:
        _cleanup_any(app)


# =============================================== 4. market data / raw capture is unaffected
@pytest.mark.parametrize("kind", KINDS)
def test_ordinary_quality_runtime_error_does_not_stop_or_isolate_ingest(kind, tmp_path, monkeypatch, alerts):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        client = _client(app)
        _arm_raising(app, _plain)
        _emit_quality(app, 0)
        assert _rows(app.raw_wire_writer) == 0

        frames = _raw_frames(_frame_kind(kind))
        asyncio.run(_run_frames(client, frames))

        assert _rows(app.raw_wire_writer) == len(frames), "raw capture kept every frame"
        assert client.processing_errors == 0 and client.fatal_storage_errors == 0
        assert not client.discard_mode
        _assert_nothing_misclassified(app, alerts)
    finally:
        _cleanup_any(app)


# =============================================== 5. the startup window (no policy yet)
@pytest.mark.parametrize("impostor", sorted(IMPOSTORS))
@pytest.mark.parametrize("kind", KINDS)
def test_ordinary_runtime_error_before_the_policy_exists_is_not_held_as_a_quality_failure(
        kind, impostor, tmp_path, monkeypatch, alerts, caplog):
    """A raw writer's orphan recovery emits quality events while the runner is still being
    constructed (``failure_policy`` does not exist yet). An ordinary error from the quality
    writer in that window must not be HELD for a later latch."""
    caplog.set_level(logging.INFO)
    emitted = _seed_orphans(tmp_path, kind)
    real_write = pw_module.ParquetWriter.write
    calls: list[str] = []

    def write(self, record, **kwargs):
        if _is_quality_stream(self):
            calls.append(self.stream_name)
            raise IMPOSTORS[impostor](self.stream_name)
        return real_write(self, record, **kwargs)
    monkeypatch.setattr(pw_module.ParquetWriter, "write", write)

    app = _make(kind, tmp_path, monkeypatch)          # must NOT raise
    try:
        assert app._pre_policy_quality_failure is None, "an ordinary error was held as a pre-policy fatal"
        assert len(calls) == emitted, "ordinary path: the writer is called for every startup event"
        _assert_nothing_misclassified(app, alerts)
        _assert_logged_and_continued(app, caplog, emitted)

        monkeypatch.setattr(pw_module.ParquetWriter, "write", real_write)
        _emit_quality(app, 0)                         # the channel works afterwards
        assert _published_reasons(app.quality_writer) == ["drop-0"]
        _assert_nothing_misclassified(app, alerts)
    finally:
        _cleanup_any(app)


# =============================================== 6. no terminal shutdown of the supervised run
@pytest.mark.parametrize("kind", ["spot", "okx"])
def test_supervised_runner_is_not_shut_down_by_an_ordinary_quality_runtime_error(kind, tmp_path, monkeypatch, alerts):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        app.quality_writer.close()                    # real plain RuntimeError on every quality write
        shutdown_calls: list[bool] = []
        real_shutdown = app.shutdown

        async def main() -> int:
            stop = asyncio.Event()

            async def run() -> None:
                _client(app).running = True
                for i in range(3):
                    _emit_quality(app, i)
                await asyncio.sleep(3600)

            async def shutdown() -> None:
                shutdown_calls.append(stop.is_set())
                await real_shutdown()

            app.run, app.shutdown = run, shutdown
            task = asyncio.ensure_future(supervise_standalone_runner(app, stop, task_timeout_s=2.0))
            await asyncio.sleep(0.5)
            assert not task.done(), "the supervisor ended the run on its own"
            assert shutdown_calls == [], "a terminal shutdown was triggered by the ordinary exception"
            stop.set()
            return await asyncio.wait_for(task, 45)

        code = asyncio.run(main())
        assert code == 0, "exit status must be a clean signalled stop, not EXIT_FATAL_STORAGE or a crash"
        assert shutdown_calls == [True], "shutdown ran exactly once, and only after the stop request"
        _assert_nothing_misclassified(app, alerts)
    finally:
        _cleanup_any(app)


def test_okx_capture_run_is_not_terminated_by_an_ordinary_quality_runtime_error(tmp_path, monkeypatch, alerts):
    """``OKXCaptureApp.run`` wakes on a terminal latch. With only an ordinary quality error it
    must run for the whole requested duration."""
    app = _make("okx_capture", tmp_path, monkeypatch)
    try:
        app.quality_writer.close()
        duration = 0.6

        async def main():
            stopped = asyncio.Event()

            async def capture_run() -> None:
                for i in range(3):
                    _emit_quality(app, i)
                await stopped.wait()

            app.capture.run = capture_run
            app.capture.stop = stopped.set
            started = time.monotonic()
            status = await asyncio.wait_for(app.run(duration), 45)
            return status, time.monotonic() - started

        status, elapsed = asyncio.run(main())
        assert elapsed >= duration - 0.1, f"capture ended after {elapsed:.2f}s: terminated early by the ordinary error"
        assert app.failure_policy.terminal_failure is None and app.exit_code == 0
        assert status["quality_degraded"] is False
        assert (status["quality_events_lost"], status["quality_events_unconfirmed"]) == (0, 0)
        _assert_nothing_misclassified(app, alerts)
    finally:
        _cleanup_any(app)


# =============================================== 7. positive control: the SAME seam, the typed exception
@pytest.mark.parametrize("kind", KINDS)
def test_control_the_typed_fatal_on_the_same_seam_is_still_latched(kind, tmp_path, monkeypatch, alerts):
    """Identical seam and stream as the look-alikes above; only the TYPE differs. Proves the
    tests above would see a misclassification, and that the typed path is unchanged."""
    app = _make(kind, tmp_path, monkeypatch)
    try:
        def typed(stream: str) -> BaseException:
            return FatalStorageError("injected", stream=stream, component="parquet_writer",
                                     stage="rename", durability="unpublished")

        calls = _arm_raising(app, typed)
        latches = _spy_on_fatal(app.failure_policy)
        _emit_quality(app, 0)

        record = app.failure_policy.quality_degraded
        assert record is not None and record.verdict == VERDICT_DEGRADE_QUALITY
        assert record.origin == "quality_writer" and len(latches) == 1
        assert app.failure_policy.terminal_failure is None
        assert app.quality_events_unconfirmed == 1 and app.quality_events_lost == 0   # evidence-less disposition
        _emit_quality(app, 1)
        assert calls == ["drop-0"], "a typed fatal latches: the failed writer is never called again"
        assert app.quality_events_lost == 1
    finally:
        _cleanup_any(app)
