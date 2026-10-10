"""F5 final fix, part 1 (Finding A): an unobservable quality-writer failure in the
standalone runners.

Defect
------
``_persist_quality_event`` in Binance Spot, OKX and OKX capture-only caught every
exception from ``quality_writer.write`` and logged it -- for EVERY event. A
quality-writer ENOSPC was therefore never latched into the runner's
``StandaloneFailurePolicy``: the quality state looked healthy, no operator alert
was raised, and the failed writer was called (and logged) again for each later
event (53 identical log lines in Agent J's run).

Contract under test
-------------------
* a typed ``FatalStorageError`` of THIS runner's quality writer is latched ONCE
  through the existing policy (degrade the quality channel, origin
  ``quality_writer``), with exactly one bounded operator alert;
* the failed writer is never called again; later events only bump a counter;
* market-data / raw capture and derived routes keep running, the process does not
  exit because telemetry failed;
* ``quality_degraded`` is observable from the runner;
* these runners keep NO durable quality WAL, and nothing claims one;
* a typed fatal of any other stream is NOT swallowed by the quality handler, and a
  later raw-evidence fatal still ends in the controlled non-zero exit;
* an ordinary exception keeps the pre-existing log-and-continue behaviour.

Everything drives the real runners, the real ``WebSocketClient`` boundary and real
``ParquetWriter`` publication; ENOSPC is injected at the real ``os.replace`` seam,
scoped to the armed writers' directories. The real ``StandaloneFailurePolicy`` is
always in the loop.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from collector import run_okx_capture as rcap
from collector.collector import notifications
from collector.collector import parquet_writer as pw_module
from collector.collector.failure_topology import VERDICT_DEGRADE_QUALITY, VERDICT_TERMINATE
from collector.collector.standalone_failure_policy import (
    EXIT_FATAL_STORAGE, StandaloneFailurePolicy, build_stream_table, supervise_standalone_runner)
from collector.collector.storage_errors import FatalStorageError
from collector.tests.test_f5_fatal_storage_topology import _FakeWs
from collector.tests.test_f5_standalone_runners import (
    SUPERVISED_BOUND_S, _assert_all_closed, _cleanup, _item, _raw_frames, _terminal_run, _trade_frame)
from collector.tests.test_p0_4_runner_lifecycle import _build, _publish_all

REAL_REPLACE = os.replace
REPO_ROOT = Path(__file__).resolve().parents[2]
KINDS = ["spot", "okx", "okx_capture"]
COLLECTORS = ["spot", "okx"]
N_EVENTS = 60                    # far more than any sane per-event reporting budget
QUALITY_WRITE_FAILED_LOG = {"spot": "spot_quality_write_failed", "okx": "okx_quality_write_failed",
                            "okx_capture": "okx_quality_write_failed"}


@pytest.fixture(autouse=True)
def alerts():
    sent: list[str] = []
    notifications.set_notifier(notifications.CallableNotifier(lambda message: sent.append(message) or True))
    yield sent
    notifications.set_notifier(None)


# --------------------------------------------------------------------------- helpers
class _Disk:
    """Real ``os.replace`` seam: segment publication fails with ENOSPC for exactly
    the writers armed on it. Arming more writers later does not un-arm earlier ones."""

    def __init__(self, monkeypatch) -> None:
        self.prefixes: set[str] = set()
        monkeypatch.setattr(pw_module.os, "replace", self._replace)

    def arm(self, writer) -> None:
        writer.segment_rows = 1
        self.prefixes.add(str(writer.stream_dir))

    def _replace(self, src, dst, *a, **k):
        target = str(dst)
        if target.endswith(".seg") and any(target.startswith(prefix) for prefix in self.prefixes):
            raise OSError(28, "No space left on device (injected)")
        return REAL_REPLACE(src, dst, *a, **k)


def _make(kind, tmp_path, monkeypatch):
    if kind == "okx_capture":
        (tmp_path / "data").mkdir(exist_ok=True)
        monkeypatch.chdir(tmp_path)
        return rcap.OKXCaptureApp(["trades"], "BTC-USDT-SWAP", str(tmp_path / "data"), "ws://unused")
    return _build(kind, tmp_path, monkeypatch)


def _client(app):
    return app.capture.client if hasattr(app, "capture") else app.client


def _frame_kind(kind: str) -> str:
    return "okx" if kind.startswith("okx") else kind


def _emit_quality(app, i: int) -> None:
    """One quality event through the runner's REAL client wiring (the callback the
    ``WebSocketClient`` calls on connect / disconnect)."""
    _client(app).on_quality_event("DISCONNECT", f"drop-{i}", "c", "stream")


def _error_events(caplog) -> list[str]:
    names = []
    for record in caplog.records:
        if record.levelno < logging.ERROR:
            continue
        message = record.getMessage()
        try:
            names.append(json.loads(message)["event"])
        except (ValueError, KeyError, TypeError):
            names.append(message[:60])
    return names


def _spy_quality_writer(app) -> list[int]:
    """Count real calls to the quality writer without altering its behaviour."""
    calls: list[int] = []
    real_write = app.quality_writer.write

    def write(*a, **k):
        calls.append(1)
        return real_write(*a, **k)
    app.quality_writer.write = write
    return calls


def _spy_on_fatal(policy) -> list[BaseException]:
    seen: list[BaseException] = []
    real = policy.on_fatal

    def on_fatal(exc, *a, **k):
        seen.append(exc)
        return real(exc, *a, **k)
    policy.on_fatal = on_fatal
    return seen


def _quality_alerts(alerts, app) -> list[str]:
    return [m for m in alerts if "degrade_quality" in m and app.quality_writer.stream_name in m]


async def _run_frames(client, frames, *, worker: bool = True) -> None:
    """Feed raw frames through the REAL ``_consume`` boundary, with the REAL worker."""
    client.running = True
    task = asyncio.create_task(client._process_queue()) if worker else None
    await client._consume(_FakeWs(frames))
    if task is not None:
        await asyncio.wait_for(client._ingest_queue.join(), 10)
        client.running = False
        await asyncio.wait_for(task, 10)


def _cleanup_any(app) -> None:
    _publish_all(app)
    dedup = getattr(app, "segment_dedup", None)
    if dedup is not None:
        dedup.close()


# =================================================== 1. latches once, one alert, bounded logs
@pytest.mark.parametrize("kind", KINDS)
def test_quality_writer_fatal_latches_once_with_one_alert_and_bounded_logs(kind, tmp_path, monkeypatch, alerts, caplog):
    caplog.set_level(logging.INFO)
    app = _make(kind, tmp_path, monkeypatch)
    try:
        policy = app.failure_policy
        assert app.quality_degraded is None and app.quality_channel_status() == {
            "quality_degraded": False, "quality_events_lost": 0, "quality_failure": None}
        _Disk(monkeypatch).arm(app.quality_writer)
        writes = _spy_quality_writer(app)
        latches = _spy_on_fatal(policy)

        _emit_quality(app, 0)
        errors_after_first = _error_events(caplog)
        for i in range(1, N_EVENTS):
            _emit_quality(app, i)

        # latched, exactly once, through the existing policy, with a quality-specific origin
        record = policy.quality_degraded
        assert record is not None and app.quality_degraded is record
        assert (record.verdict, record.origin) == (VERDICT_DEGRADE_QUALITY, "quality_writer")
        assert record.stream == app.quality_writer.stream_name
        assert len(latches) == 1 and len(policy.failed_components) == 1
        assert policy.terminal_failure is None and not policy.isolated_routes

        # the failed writer is never called again
        assert len(writes) == 1, f"the FAILED quality writer was called {len(writes)} times"

        # exactly one bounded operator alert, and the log volume does NOT grow with the event count
        assert len(alerts) == 1 and len(_quality_alerts(alerts, app)) == 1, alerts
        assert _error_events(caplog) == errors_after_first, "an error log was emitted per later event"
        errors = _error_events(caplog)
        assert errors.count("storage_failure_latched") == 1
        assert QUALITY_WRITE_FAILED_LOG[kind] not in errors
        assert len(errors) <= 3

        # observable, and honest about loss: no WAL is claimed for a runner that has none
        status = app.quality_channel_status()
        assert status["quality_degraded"] is True and status["quality_events_lost"] == N_EVENTS
        assert status["quality_failure"]["origin"] == "quality_writer"
        assert not hasattr(app, "_quality_wal")
        assert "wal" not in (alerts[0] + json.dumps(status)).lower()
    finally:
        _cleanup_any(app)


# ================================================== 2. healthy capture continues while degraded
@pytest.mark.parametrize("kind", KINDS)
def test_market_data_and_raw_capture_continue_while_the_quality_channel_is_degraded(kind, tmp_path, monkeypatch):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        client, policy = _client(app), app.failure_policy
        _Disk(monkeypatch).arm(app.quality_writer)
        writes = _spy_quality_writer(app)
        _emit_quality(app, 0)
        assert app.quality_degraded is not None

        async def traffic() -> None:         # one event loop: the client's queue binds to it
            await _run_frames(client, _raw_frames(_frame_kind(kind), 5), worker=False)
            for i in range(1, 11):           # quality traffic interleaved with the market data
                _emit_quality(app, i)
            await _run_frames(client, _raw_frames(_frame_kind(kind), 3), worker=False)
            if kind != "okx_capture":        # drain what the raw boundary enqueued, with the REAL worker
                client.running = True
                worker = asyncio.create_task(client._process_queue())
                await asyncio.wait_for(client._ingest_queue.join(), 10)
                client.running = False
                await asyncio.wait_for(worker, 10)
        asyncio.run(traffic())

        assert app.raw_capture.wire_captured == 8, "raw evidence must keep being captured"
        assert app.raw_wire_writer.failure_snapshot() is None
        assert client.fatal_storage_errors == 0 and client.processing_errors == 0
        assert not client.discard_mode
        assert policy.terminal_failure is None and app.exit_code == 0 and not policy.terminal_event.is_set()
        if kind != "okx_capture":
            assert client.frames_processed == 8
            assert app.trades_writer.failure_snapshot() is None and app.trades_writer.has_unpublished_rows(), \
                "the derived trade route must keep receiving rows"
            assert not policy.isolated_routes
        # every event after the latch -- the 10 injected AND any the runner itself emitted
        # during real traffic -- is counted, and the FAILED writer was never called again
        assert app.quality_events_lost >= 11 and len(writes) == 1
    finally:
        _cleanup_any(app)


# ======================================= 3. telemetry failure alone never ends the process
@pytest.mark.parametrize("kind", COLLECTORS)
def test_supervised_collector_keeps_running_after_a_quality_failure_and_stops_cleanly_on_a_signal(kind, tmp_path, monkeypatch):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        client = app.client
        _Disk(monkeypatch).arm(app.quality_writer)

        async def run() -> None:
            for i in range(N_EVENTS):
                _emit_quality(app, i)
            await _run_frames(client, _raw_frames(kind, 4))
            client.running = True
            await asyncio.sleep(3600)
        app.run = run

        async def main() -> int:
            stop = asyncio.Event()
            supervisor = asyncio.create_task(supervise_standalone_runner(app, stop, task_timeout_s=2))
            await asyncio.sleep(1.0)
            assert not supervisor.done(), "a quality-channel failure must not end the process"
            assert app.quality_degraded is not None and app.failure_policy.terminal_failure is None
            assert app.raw_capture.wire_captured == 4
            stop.set()
            return await asyncio.wait_for(supervisor, SUPERVISED_BOUND_S)
        assert asyncio.run(main()) == 0 == app.exit_code
        _assert_all_closed(app)
    finally:
        _cleanup_any(app)


def test_okx_capture_only_runs_to_its_duration_after_a_quality_failure_and_reports_the_degradation(tmp_path, monkeypatch):
    app = _make("okx_capture", tmp_path, monkeypatch)
    try:
        client = app.capture.client
        _Disk(monkeypatch).arm(app.quality_writer)

        async def capture_run() -> None:
            for i in range(N_EVENTS):
                _emit_quality(app, i)
            await _run_frames(client, _raw_frames("okx", 4), worker=False)
            while client.running:               # like the real client.start(): returns once stop() is called
                await asyncio.sleep(0.05)
        app.capture.run = capture_run
        monkeypatch.setattr(rcap, "TERMINAL_STOP_GRACE_S", 1.0)

        started = time.monotonic()
        status = asyncio.run(asyncio.wait_for(app.run(duration_s=1.5), SUPERVISED_BOUND_S))

        assert time.monotonic() - started < 10, "must end at its 1.5 s duration, not run on"
        assert app.failure_policy.terminal_failure is None and app.exit_code == 0, \
            "telemetry failure alone must not make the capture exit non-zero"
        assert status["quality_degraded"] is True and status["quality_events_lost"] >= N_EVENTS
        assert status["quality_failure"]["origin"] == "quality_writer"
        assert status["raw_captured"] == 4 and status["capture_failures"] == 0
        _assert_all_closed(app)
    finally:
        _publish_all(app)


# ======================== 4. a later raw-evidence fatal still ends in the controlled non-zero exit
@pytest.mark.parametrize("kind", COLLECTORS)
def test_raw_fatal_after_a_quality_failure_still_terminates_with_exit_70(kind, tmp_path, monkeypatch, alerts):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        disk = _Disk(monkeypatch)
        disk.arm(app.quality_writer)
        for i in range(N_EVENTS):
            _emit_quality(app, i)
        assert app.quality_degraded is not None and app.exit_code == 0
        writes = _spy_quality_writer(app)

        disk.arm(app.raw_wire_writer)
        code = _terminal_run(app, _raw_frames(kind))

        assert code == EXIT_FATAL_STORAGE != 0
        record = app.failure_policy.terminal_failure
        assert record is not None and (record.verdict, record.stream) == (VERDICT_TERMINATE, app.raw_wire_writer.stream_name)
        assert app.quality_degraded is not None
        assert writes == [], "the FAILED quality writer must not be called while reporting the raw failure"
        assert len(_quality_alerts(alerts, app)) == 1, "the quality degradation is alerted once, never again"
        assert sum(1 for m in alerts if "[terminate]" in m and app.raw_wire_writer.stream_name in m) == 1
        _assert_all_closed(app)
    finally:
        _cleanup_any(app)


def test_okx_capture_only_raw_fatal_after_a_quality_failure_still_exits_non_zero(tmp_path, monkeypatch, alerts):
    app = _make("okx_capture", tmp_path, monkeypatch)
    try:
        disk = _Disk(monkeypatch)
        disk.arm(app.quality_writer)
        for i in range(N_EVENTS):
            _emit_quality(app, i)
        assert app.quality_degraded is not None
        writes = _spy_quality_writer(app)
        disk.arm(app.raw_wire_writer)
        client = app.capture.client

        async def capture_run() -> None:
            client.running = True
            await client._consume(_FakeWs(_raw_frames("okx")))
            await asyncio.sleep(3600)
        app.capture.run = capture_run
        monkeypatch.setattr(rcap, "TERMINAL_STOP_GRACE_S", 1.0)

        started = time.monotonic()
        status = asyncio.run(asyncio.wait_for(app.run(duration_s=3600.0), SUPERVISED_BOUND_S))

        assert time.monotonic() - started < SUPERVISED_BOUND_S, "must not wait out the 3600 s duration"
        assert app.exit_code == EXIT_FATAL_STORAGE and app.failure_policy.terminal_failure is not None
        assert status["quality_degraded"] is True and writes == []
        assert len(_quality_alerts(alerts, app)) == 1
        _assert_all_closed(app)
    finally:
        _publish_all(app)


# ============== 5. full disk: the failure's OWN report is the first thing to hit the quality writer
@pytest.mark.parametrize("kind", COLLECTORS)
def test_derived_failure_whose_report_trips_the_quality_writer_alerts_both_once(kind, tmp_path, monkeypatch, alerts, caplog):
    """Spot/OKX derived writers have no quality sink, so on a full disk the FIRST write
    to the quality writer is the one made by the derived failure's own reporter, i.e.
    inside the policy's ``_report``. The quality latch made there must be alerted too."""
    caplog.set_level(logging.INFO)
    app = _make(kind, tmp_path, monkeypatch)
    try:
        client, policy = app.client, app.failure_policy
        disk = _Disk(monkeypatch)
        disk.arm(app.quality_writer)
        disk.arm(app.trades_writer)

        asyncio.run(_run_frames(client, [json.dumps(_trade_frame(kind, i)) for i in range(1, 6)]))

        assert set(policy.isolated_routes) == {"trades"} and policy.terminal_failure is None
        assert policy.quality_degraded is not None, "the quality failure was swallowed inside the report path"
        assert app.exit_code == 0 and not client.discard_mode
        assert sum("[isolate_route]" in m for m in alerts) == 1
        assert len(_quality_alerts(alerts, app)) == 1, alerts
        assert _error_events(caplog).count("storage_failure_latched") == 2
        assert policy._reporting is False
    finally:
        _cleanup_any(app)


# ================================ 6. an unrelated typed fatal is NOT swallowed by the quality handler
def _other_streams(app) -> dict[str, str]:
    streams = {"raw_wire": app.raw_wire_writer.stream_name, "unknown": "mystery_stream"}
    if hasattr(app, "trades_writer"):
        streams["derived"] = app.trades_writer.stream_name
    if hasattr(app, "raw_rest_writer"):
        streams["raw_rest"] = app.raw_rest_writer.stream_name
    return streams


@pytest.mark.parametrize("kind", KINDS)
def test_a_typed_fatal_of_another_stream_is_not_swallowed_by_the_quality_handler(kind, tmp_path, monkeypatch, alerts, caplog):
    caplog.set_level(logging.INFO)
    app = _make(kind, tmp_path, monkeypatch)
    try:
        policy = app.failure_policy
        for label, stream in _other_streams(app).items():
            def write(*a, _stream=stream, **k):
                raise FatalStorageError("not the quality writer", stream=_stream,
                                        component="parquet_writer", stage="rename", durability="unpublished")
            monkeypatch.setattr(app.quality_writer, "write", write)
            with pytest.raises(FatalStorageError) as raised:
                app._persist_quality_event({"event_type": "ERROR", "reason": label})
            assert raised.value.stream == stream
            assert policy.quality_degraded is None, f"{label}: misclassified as a quality failure"
            assert not policy.failed_components and app.quality_events_lost == 0 and not alerts
        assert QUALITY_WRITE_FAILED_LOG[kind] not in _error_events(caplog), "must not be logged-and-dropped"
    finally:
        _cleanup_any(app)


@pytest.mark.parametrize("kind", KINDS)
def test_an_unrelated_fatal_is_classified_by_its_own_stream_once_it_reaches_the_policy(kind, tmp_path, monkeypatch):
    """What the re-raise buys: the propagated fatal is still classified by ITS stream
    (raw -> terminate; unknown -> default-deny terminate), never as a quality failure."""
    app = _make(kind, tmp_path, monkeypatch)
    try:
        for label, stream in _other_streams(app).items():
            if label in ("derived", "raw_rest"):
                continue
            exc = FatalStorageError("x", stream=stream, component="parquet_writer", stage="rename", durability="unpublished")
            assert app.failure_policy.is_quality_channel_failure(exc) is False
        assert app.failure_policy.is_quality_channel_failure(
            FatalStorageError("x", stream=app.quality_writer.stream_name)) is True
        assert app.failure_policy.is_quality_channel_failure(OSError(28, "ENOSPC")) is False
    finally:
        _cleanup_any(app)


# ================================== 7. ordinary exceptions keep their existing contract
@pytest.mark.parametrize("kind", KINDS)
def test_an_ordinary_quality_write_error_keeps_the_log_and_continue_behaviour(kind, tmp_path, monkeypatch, alerts, caplog):
    caplog.set_level(logging.INFO)
    app = _make(kind, tmp_path, monkeypatch)
    try:
        def write(*a, **k):
            raise OSError(5, "transient I/O error")
        monkeypatch.setattr(app.quality_writer, "write", write)
        for i in range(3):
            app._persist_quality_event({"event_type": "ERROR", "reason": f"r{i}"})   # must not raise

        # Only attributes that exist on the pre-fix runners: this test must pass BEFORE and AFTER.
        assert _error_events(caplog).count(QUALITY_WRITE_FAILED_LOG[kind]) == 3, "existing per-event log kept"
        assert app.failure_policy.quality_degraded is None and not app.failure_policy.failed_components
        assert getattr(app, "quality_events_lost", 0) == 0 and not alerts
    finally:
        _cleanup_any(app)


# ================================ 8. the policy's own reporting path (what the runners rely on)
def _quality_fatal() -> FatalStorageError:
    return FatalStorageError("quality dead", stream="x_quality", component="parquet_writer",
                             stage="rename", durability="unpublished")


def _raw_fatal() -> FatalStorageError:
    return FatalStorageError("raw dead", stream="x_raw", component="parquet_writer",
                             stage="rename", durability="unpublished")


def test_a_quality_latch_made_inside_another_failures_report_is_alerted_and_never_recurses(alerts):
    table = build_stream_table(raw=["x_raw"], derived={}, quality=["x_quality"])
    holder: dict = {}
    reporter_calls: list[str] = []

    def report(record):                       # the runner's reporter writes to the (failed) quality stream
        reporter_calls.append(record.verdict)
        holder["policy"].on_fatal(_quality_fatal(), origin="quality_writer")

    policy = holder["policy"] = StandaloneFailurePolicy(venue="X", streams=table, report=report)
    assert policy.on_fatal(_raw_fatal(), origin="raw_frame") == VERDICT_TERMINATE

    assert policy.quality_degraded is not None and policy.terminal_failure is not None
    assert [m.split("]")[0].split("[")[1] for m in alerts] == ["terminate", "degrade_quality"]
    assert reporter_calls == [VERDICT_TERMINATE], "the runner's reporter is never called for a quality record"
    assert policy._reporting is False
    # repeated quality observations count, and stay silent
    policy.on_fatal(_quality_fatal(), origin="quality_writer")
    assert len(alerts) == 2 and sum(policy.failure_repeats.values()) == 1


def test_a_non_quality_report_nested_inside_a_report_is_still_suppressed(alerts):
    table = build_stream_table(raw=["x_raw", "x_raw2"], derived={}, quality=["x_quality"])
    holder: dict = {}

    def report(record):
        if record.stream == "x_raw":
            holder["policy"].on_fatal(
                FatalStorageError("raw2 dead", stream="x_raw2", component="parquet_writer", stage="rename"),
                origin="raw_frame")

    policy = holder["policy"] = StandaloneFailurePolicy(venue="X", streams=table, report=report)
    policy.on_fatal(_raw_fatal(), origin="raw_frame")
    assert len(alerts) == 1 and "x_raw " in alerts[0] + " ", "a nested non-quality report must not recurse"
    assert "x_raw2" in {r.stream for r in policy.failed_components.values()}, "but it is still latched"
    assert policy._reporting is False


# ============================== 9. a real process: bounded output, and exit 70 on the later raw fatal
_SUBPROCESS_SCRIPT = textwrap.dedent('''
    import asyncio, json, os, sys
    from collector.collector import notifications
    from collector.collector import parquet_writer as pw
    from collector.collector import websocket_client as ws
    notifications.set_notifier(notifications.CallableNotifier(
        lambda m: print("ALERT|" + m, file=sys.stderr, flush=True) or True))
    venue, data_dir = sys.argv[1], sys.argv[2]

    RAW_ARMED = [False]
    REAL = os.replace
    def replace(src, dst, *a, **k):
        d = str(dst)
        if d.endswith(".seg") and ("quality_events" in d or ("raw_wire" in d and RAW_ARMED[0])):
            raise OSError(28, "No space left on device (injected)")
        return REAL(src, dst, *a, **k)
    os.replace = replace

    _init = pw.ParquetWriter.__init__
    def init(self, *a, **k):
        _init(self, *a, **k)
        if "raw_wire" in str(self.stream_name):
            self.segment_rows = 1
    pw.ParquetWriter.__init__ = init

    class Ws:
        def __init__(self, frames): self.frames = frames
        def __aiter__(self):
            async def gen():
                for f in self.frames: yield f
            return gen()

    async def fake_start(self):
        self.running = True
        for i in range(50):                       # quality traffic: the quality writer is dead (ENOSPC)
            self.on_quality_event("DISCONNECT", "drop-%d" % i, "c", "stream")
        print("QUALITY_EVENTS_DONE", file=sys.stderr, flush=True)
        RAW_ARMED[0] = True                        # now the raw-evidence writer dies as well
        await self._consume(Ws([json.dumps({"stream": "btcusdt@trade", "data": {"n": i}}) for i in range(4)]))
        await asyncio.sleep(3600)
    ws.WebSocketClient.start = fake_start

    if venue == "spot":
        from collector import run_binance_spot_collector as m
        sys.exit(asyncio.run(m._main(data_dir, "ws://unused")))
    if venue == "okx":
        from collector import run_okx_collector as m
        sys.exit(asyncio.run(m._main(data_dir, "ws://unused")))
    from collector import run_okx_capture as m
    m.TERMINAL_STOP_GRACE_S = 1.0
    sys.exit(m.main(["--forever", "--data-dir", data_dir]))
''')


@pytest.mark.parametrize("venue", KINDS)
def test_real_process_quality_failure_is_alerted_once_with_bounded_output_then_raw_fatal_exits_70(venue, tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    env = {**os.environ, "COLLECTOR_ALLOW_UNVERIFIED_FS": "1",
           "PYTHONPATH": f"{REPO_ROOT}{os.pathsep}{os.environ.get('PYTHONPATH', '')}"}
    started = time.monotonic()
    proc = subprocess.run([sys.executable, "-c", _SUBPROCESS_SCRIPT, venue, str(data_dir)],
                          cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=SUPERVISED_BOUND_S + 30)
    assert time.monotonic() - started < SUPERVISED_BOUND_S + 30
    assert proc.returncode == EXIT_FATAL_STORAGE, proc.stderr[-2000:]

    lines = [line for line in proc.stderr.splitlines() if line.strip()]
    assert "QUALITY_EVENTS_DONE" in lines
    after_events = lines[lines.index("QUALITY_EVENTS_DONE") + 1:]
    before_events = lines[:lines.index("QUALITY_EVENTS_DONE")]
    # 50 quality events with a dead quality writer: ONE latch log, no per-event flood
    assert not [line for line in lines if "_quality_write_failed" in line]
    latched = [json.loads(line) for line in lines if line.startswith("{") and '"storage_failure_latched"' in line]
    assert [r["verdict"] for r in latched].count(VERDICT_DEGRADE_QUALITY) == 1
    assert [r["verdict"] for r in latched].count(VERDICT_TERMINATE) == 1
    alert_lines = [line for line in lines if line.startswith("ALERT|")]
    assert sum("[degrade_quality]" in line for line in alert_lines) == 1
    assert sum("[terminate]" in line for line in alert_lines) == 1 and len(alert_lines) == 2
    assert len(before_events) < 30, f"quality failure produced {len(before_events)} lines for 50 events"
