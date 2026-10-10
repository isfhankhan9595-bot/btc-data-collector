"""F5 remediation, part 1: the standalone venue runners and terminal shutdown.

F-1  Binance Spot, Bybit and OKX (collector + capture-only) share the
     ``WebSocketClient`` with the USD-M collector. Before this change none of them
     had a supervised path for a typed storage fatal: a derived-writer ENOSPC put
     the shared client in terminal discard mode, its task ended with
     ``FatalStorageError`` and nobody observed it -- the process stayed alive
     collecting nothing.

        raw-wire / raw-REST writer fatal -> controlled shutdown, non-zero exit
        derived writer fatal             -> isolate that route; process keeps collecting
        ordinary exception               -> P0-1 worker survival, unchanged
        unknown typed fatal              -> terminate (default-deny)
        application task                 -> always supervised, never abandoned

F-2  A failing quality writer must never stop terminal shutdown from closing the
     writers and reaching the intended non-zero exit.

Everything drives the real runners, the real ``WebSocketClient`` worker /
``_consume`` boundary and real ``ParquetWriter`` publication; storage faults are
injected at the real ``os.replace`` seam, scoped to one writer's directory.
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from collector import run_binance_spot_collector as rsc
from collector import run_collector as rc
from collector import run_okx_capture as rcap
from collector.collector import notifications
from collector.collector import parquet_writer as pw_module
from collector.collector.failure_topology import (
    EXIT_FATAL_STORAGE, VERDICT_DEGRADE_QUALITY, VERDICT_ISOLATE, VERDICT_TERMINATE, classify_stream)
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.standalone_failure_policy import (
    EXIT_RUN_TASK_CRASHED, StandaloneFailurePolicy, build_stream_table, supervise_standalone_runner)
from collector.collector.storage_errors import FatalStorageError
from collector.collector.websocket_client import IngestItem
from collector.tests.test_f5_fatal_storage_topology import _FakeWs
from collector.tests.test_p0_4_runner_lifecycle import _build, _now, _publish_all

REAL_REPLACE = os.replace
REPO_ROOT = Path(__file__).resolve().parents[2]
VENUES = ["spot", "bybit", "okx"]
SUPERVISED_BOUND_S = 45


@pytest.fixture(autouse=True)
def alerts():
    sent: list[str] = []
    notifications.set_notifier(notifications.CallableNotifier(lambda message: sent.append(message) or True))
    yield sent
    notifications.set_notifier(None)


# --------------------------------------------------------------------------- helpers
def _fail_publish_into(monkeypatch, writer: ParquetWriter) -> None:
    """Real ``os.replace`` failure (ENOSPC) for ONE writer's segment rename."""
    prefix = str(writer.stream_dir)

    def replace(src, dst, *a, **k):
        target = str(dst)
        if target.endswith(".seg") and target.startswith(prefix):
            raise OSError(28, "No space left on device (injected)")
        return REAL_REPLACE(src, dst, *a, **k)
    monkeypatch.setattr(pw_module.os, "replace", replace)


def _break(monkeypatch, writer: ParquetWriter) -> None:
    writer.segment_rows = 1
    _fail_publish_into(monkeypatch, writer)


def _trade_frame(kind: str, i: int, ts: int | None = None) -> dict:
    ts = ts or _now()
    if kind == "spot":
        return {"stream": "btcusdt@trade", "data": {"e": "trade", "E": ts, "T": ts, "t": i,
                                                    "p": "1", "q": "1", "m": False}}
    if kind == "bybit":
        return {"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": ts, "data": [
            {"T": ts, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "100", "i": str(i), "BT": False}]}
    return {"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [
        {"instId": "BTC-USDT-SWAP", "tradeId": str(i), "px": "100", "sz": "1", "side": "buy", "ts": str(ts)}]}


def _item(data, i: int = 0) -> IngestItem:
    return IngestItem(msg=data, local_receive_ts=_now() + i, connection_id="c", connection_generation=1,
                      data=data, decode_error=None, is_control=False)


async def _drive_worker(client, items) -> None:
    """Run the REAL worker loop over ``items`` and wait for the queue to drain."""
    client.running = True
    worker = asyncio.create_task(client._process_queue())
    for item in items:
        client._ingest_queue.put_nowait(item)
    await asyncio.wait_for(client._ingest_queue.join(), 10)
    client.running = False
    await asyncio.wait_for(worker, 10)


def _all_writers(app) -> dict[str, ParquetWriter]:
    return {k: v for k, v in vars(app).items() if isinstance(v, ParquetWriter)}


def _assert_all_closed(app) -> None:
    open_writers = [name for name, w in _all_writers(app).items() if w._lock_handle is not None]
    assert not open_writers, f"writers left unclosed: {open_writers}"


def _build_runner(kind, tmp_path, monkeypatch):
    return _build(kind, tmp_path, monkeypatch)


def _cleanup(app) -> None:
    _publish_all(app)
    segment_dedup = getattr(app, "segment_dedup", None)
    if segment_dedup is not None:
        segment_dedup.close()


def _raw_frames(kind: str, n: int = 4) -> list[str]:
    return [json.dumps(_trade_frame(kind, i + 1)) for i in range(n)]


def _terminal_run(app, frames, *, task_timeout_s: float = 2.0) -> int:
    """Supervise the real runner whose ``run()`` is a client that consumes
    ``frames`` and then NEVER returns -- the stalled-process shape of F-1."""
    client = app.client

    async def run() -> None:
        client.running = True
        await client._consume(_FakeWs(frames))
        await asyncio.sleep(3600)

    app.run = run

    async def main() -> int:
        stop = asyncio.Event()
        code = await asyncio.wait_for(
            supervise_standalone_runner(app, stop, task_timeout_s=task_timeout_s), SUPERVISED_BOUND_S)
        leftover = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        assert not leftover, f"unobserved background tasks survived the supervisor: {leftover}"
        return code
    return asyncio.run(main())


# ================================================ 1-3: derived-writer fatal never stalls the process
@pytest.mark.parametrize("kind", VENUES)
def test_derived_writer_fatal_isolates_the_route_and_leaves_a_live_collecting_process(kind, tmp_path, monkeypatch):
    app = _build_runner(kind, tmp_path, monkeypatch)
    try:
        client, policy = app.client, app.failure_policy
        _break(monkeypatch, app.trades_writer)

        asyncio.run(_drive_worker(client, [_item(_trade_frame(kind, i), i) for i in range(1, 6)]))

        assert set(policy.isolated_routes) == {"trades"}
        record = policy.isolated_routes["trades"]
        assert (record.verdict, record.route) == (VERDICT_ISOLATE, "trades")
        assert record.origin == "worker", "classified at the client's worker boundary, not swallowed by the handler"
        assert policy.terminal_failure is None and app.exit_code == 0
        assert not client.discard_mode, "the client must NOT be left in terminal discard mode"
        assert client.processing_errors == 0, "a typed fatal is never an ordinary processing error"
        assert client.fatal_storage_errors == 1 and policy.route_short_circuits.get("trades", 0) >= 1
        assert client._ingest_queue._unfinished_tasks == 0
    finally:
        _cleanup(app)


@pytest.mark.parametrize("kind", VENUES)
def test_process_keeps_running_after_a_derived_fatal_and_still_stops_on_a_signal(kind, tmp_path, monkeypatch):
    app = _build_runner(kind, tmp_path, monkeypatch)
    try:
        _break(monkeypatch, app.trades_writer)
        client = app.client

        async def run() -> None:
            await _drive_worker(client, [_item(_trade_frame(kind, i), i) for i in range(1, 4)])
            client.running = True
            await asyncio.sleep(3600)
        app.run = run

        async def main():
            stop = asyncio.Event()
            supervisor = asyncio.create_task(supervise_standalone_runner(app, stop, task_timeout_s=2))
            await asyncio.sleep(1.0)
            assert not supervisor.done(), "an isolated derived route must not end the process"
            assert "trades" in app.failure_policy.isolated_routes
            stop.set()
            return await asyncio.wait_for(supervisor, SUPERVISED_BOUND_S)
        assert asyncio.run(main()) == 0
        _assert_all_closed(app)
    finally:
        _cleanup(app)


# ======================================== 4: raw-evidence writer fatal reaches the terminal boundary
@pytest.mark.parametrize("kind", VENUES)
def test_raw_wire_fatal_is_latched_terminal_at_the_raw_frame_boundary(kind, tmp_path, monkeypatch):
    app = _build_runner(kind, tmp_path, monkeypatch)
    try:
        _break(monkeypatch, app.raw_wire_writer)
        client, policy = app.client, app.failure_policy
        client.running = True
        asyncio.run(client._consume(_FakeWs(_raw_frames(kind))))

        record = policy.terminal_failure
        assert record is not None, "the raw-wire writer's typed fatal must not be swallowed (fail-open)"
        assert (record.verdict, record.origin) == (VERDICT_TERMINATE, "raw_frame")
        assert record.stream == app.raw_wire_writer.stream_name
        assert policy.terminal_event.is_set() and app.exit_code == EXIT_FATAL_STORAGE
        assert client.discard_mode and client.frames_enqueued == 0
    finally:
        _cleanup(app)


@pytest.mark.parametrize("kind", VENUES)
def test_raw_fatal_terminates_through_the_supervisor_with_exit_70_and_every_writer_closed(kind, tmp_path, monkeypatch, alerts):
    app = _build_runner(kind, tmp_path, monkeypatch)
    try:
        _break(monkeypatch, app.raw_wire_writer)
        started = time.monotonic()
        code = _terminal_run(app, _raw_frames(kind))
        assert code == EXIT_FATAL_STORAGE != 0
        assert time.monotonic() - started < SUPERVISED_BOUND_S
        _assert_all_closed(app)
        assert any(app.raw_wire_writer.stream_name in message for message in alerts)
    finally:
        _cleanup(app)


def test_spot_raw_rest_fatal_terminates_and_the_response_is_not_used(tmp_path, monkeypatch):
    app = _build_runner("spot", tmp_path, monkeypatch)
    try:
        _break(monkeypatch, app.raw_rest_writer)
        record = rsc.RawRestRecord(
            request_ts=_now(), response_receive_ts=_now(), endpoint="https://x", purpose="orderbook_snapshot",
            venue=rsc.VENUE, request_params={}, http_status=200, ok=True, payload="{}", symbol="BTCUSDT",
            market_type="spot", local_process_ts=_now())
        results = [app._capture_rest(record) for _ in range(3)]
        assert False in results, "a lost raw REST capture must be reported to the caller"
        policy = app.failure_policy
        assert policy.terminal_failure is not None and policy.terminal_failure.origin == "raw_rest"
        assert app.exit_code == EXIT_FATAL_STORAGE
    finally:
        _cleanup(app)


def test_okx_capture_only_raw_fatal_terminates_instead_of_running_on_to_the_duration(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    app = rcap.OKXCaptureApp(["trades"], "BTC-USDT-SWAP", str(tmp_path / "data"), "ws://unused")
    try:
        _break(monkeypatch, app.raw_wire_writer)
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
        assert app.failure_policy.terminal_failure is not None and app.exit_code == EXIT_FATAL_STORAGE
        assert status is not None
        _assert_all_closed(app)
    finally:
        _publish_all(app)


# ============================================ 7: terminal subprocess exits non-zero, bounded
_SUBPROCESS_SCRIPT = textwrap.dedent('''
    import asyncio, json, os, sys
    from collector.collector import notifications
    from collector.collector import parquet_writer as pw
    from collector.collector import websocket_client as ws
    notifications.set_notifier(notifications.NullNotifier())
    venue, data_dir = sys.argv[1], sys.argv[2]

    REAL = os.replace
    def replace(src, dst, *a, **k):
        if str(dst).endswith(".seg") and "raw_wire" in str(dst):
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
        # a start() that never returns: the stalled-process shape of F-1
        self.running = True
        await self._consume(Ws([json.dumps({"stream": "btcusdt@trade", "data": {"n": i}}) for i in range(4)]))
        await asyncio.sleep(3600)
    ws.WebSocketClient.start = fake_start

    if venue == "spot":
        from collector import run_binance_spot_collector as m
        sys.exit(asyncio.run(m._main(data_dir, "ws://unused")))
    if venue == "bybit":
        from collector import run_bybit_collector as m
        sys.exit(asyncio.run(m._main(data_dir, "ws://unused")))
    if venue == "okx":
        from collector import run_okx_collector as m
        sys.exit(asyncio.run(m._main(data_dir, "ws://unused")))
    from collector import run_okx_capture as m
    m.TERMINAL_STOP_GRACE_S = 1.0
    sys.exit(m.main(["--forever", "--data-dir", data_dir]))
''')


@pytest.mark.parametrize("venue", ["spot", "bybit", "okx", "okx_capture"])
def test_terminal_subprocess_exits_non_zero_within_a_bounded_timeout(venue, tmp_path):
    (tmp_path / "data").mkdir()
    env = {**os.environ, "PYTHONPATH": f"{REPO_ROOT}{os.pathsep}{REPO_ROOT / 'collector'}"}
    started = time.monotonic()
    done = subprocess.run([sys.executable, "-c", _SUBPROCESS_SCRIPT, venue, str(tmp_path / "data")],
                          cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120)
    elapsed = time.monotonic() - started
    assert done.returncode == EXIT_FATAL_STORAGE, (done.returncode, done.stderr[-2000:])
    assert elapsed < SUPERVISED_BOUND_S, f"terminal exit took {elapsed:.1f}s"
    assert "never retrieved" not in done.stderr, "the application task was abandoned unobserved"


# ===================================================== supervisor policy: observed task, default-deny
def _policy_app(kind, tmp_path, monkeypatch):
    return _build_runner(kind, tmp_path, monkeypatch)


@pytest.mark.parametrize("kind", VENUES)
def test_a_typed_fatal_escaping_the_application_task_is_terminal_default_deny(kind, tmp_path, monkeypatch):
    app = _policy_app(kind, tmp_path, monkeypatch)
    try:
        async def run() -> None:
            # no classifier saw it: it escapes the task, and even a DERIVED stream is terminal here
            raise FatalStorageError("escaped", stream=app.trades_writer.stream_name,
                                    component="parquet_writer", stage="rename", durability="unpublished")
        app.run = run
        code = asyncio.run(asyncio.wait_for(supervise_standalone_runner(app, asyncio.Event(), task_timeout_s=2),
                                            SUPERVISED_BOUND_S))
        assert code == EXIT_FATAL_STORAGE
        assert app.failure_policy.terminal_failure.origin == "run_task"
        _assert_all_closed(app)
    finally:
        _cleanup(app)


@pytest.mark.parametrize("kind", VENUES)
def test_unknown_typed_fatal_stream_terminates(kind, tmp_path, monkeypatch):
    app = _policy_app(kind, tmp_path, monkeypatch)
    try:
        verdict = app.failure_policy.on_fatal(FatalStorageError("mystery", stream="no_such_stream"), "worker")
        assert verdict == VERDICT_TERMINATE and app.exit_code == EXIT_FATAL_STORAGE
    finally:
        _cleanup(app)


def test_stream_tables_use_each_venues_own_stream_names_and_never_the_usdm_ones(tmp_path, monkeypatch):
    for kind, prefix in (("spot", "spot_"), ("bybit", "bybit_"), ("okx", "okx_")):
        (tmp_path / kind).mkdir()
        app = _policy_app(kind, tmp_path / kind, monkeypatch)
        try:
            table = app.failure_policy._streams
            assert table[app.raw_wire_writer.stream_name][0] == VERDICT_TERMINATE
            assert table[app.trades_writer.stream_name] == (VERDICT_ISOLATE, "trades")
            assert table[app.quality_writer.stream_name][0] == VERDICT_DEGRADE_QUALITY
            assert all(name.startswith(prefix) or name == "trades" for name in table), sorted(table)
            # a Binance USD-M name is NOT silently mapped on a venue that does not write it
            assert classify_stream("markprice", table=table)[0] == VERDICT_TERMINATE
        finally:
            _cleanup(app)


def test_build_stream_table_default_denies_anything_unlisted():
    table = build_stream_table(raw=("raw_a",), derived={"der_b": "b"}, quality=("q_c",), dedup_route="b")
    assert classify_stream("raw_a", table=table)[0] == VERDICT_TERMINATE
    assert classify_stream("der_b", table=table) == (VERDICT_ISOLATE, "b")
    assert classify_stream("trades", table=table) == (VERDICT_ISOLATE, "b")        # DedupStateError's stream
    assert classify_stream("q_c", table=table)[0] == VERDICT_DEGRADE_QUALITY
    assert classify_stream("unlisted", table=table)[0] == VERDICT_TERMINATE
    assert classify_stream(None, table=table)[0] == VERDICT_TERMINATE


def test_on_fatal_refuses_a_non_typed_exception():
    policy = StandaloneFailurePolicy(venue="X", streams={})
    with pytest.raises(TypeError):
        policy.on_fatal(RuntimeError("ParquetWriter is FAILED"), "worker")


# ======================================================= 8: ordinary errors keep P0-1 behaviour
@pytest.mark.parametrize("kind", VENUES)
def test_ordinary_worker_error_keeps_the_worker_alive_and_never_touches_the_failure_policy(kind, tmp_path, monkeypatch):
    app = _build_runner(kind, tmp_path, monkeypatch)
    try:
        client, policy = app.client, app.failure_policy
        real, calls = app.adapter.normalize, {"n": 0}

        def flaky(data, **kw):
            calls["n"] += 1
            if calls["n"] == 2:
                raise ValueError("ordinary adapter bug")
            return real(data, **kw)
        app.adapter.normalize = flaky

        asyncio.run(_drive_worker(client, [_item(_trade_frame(kind, i), i) for i in range(1, 5)]))

        assert client.processing_errors == 1 and client.frames_processed == 3, "FIFO survivors keep flowing"
        assert not client.discard_mode and client.fatal_storage_errors == 0
        assert not policy.failed_components and policy.terminal_failure is None and app.exit_code == 0
        assert client._ingest_queue._unfinished_tasks == 0
    finally:
        _cleanup(app)


@pytest.mark.parametrize("kind", VENUES)
def test_ordinary_application_crash_exits_non_zero_without_claiming_storage_loss(kind, tmp_path, monkeypatch):
    app = _build_runner(kind, tmp_path, monkeypatch)
    try:
        async def run() -> None:
            raise ValueError("not a storage failure")
        app.run = run
        code = asyncio.run(asyncio.wait_for(supervise_standalone_runner(app, asyncio.Event(), task_timeout_s=2),
                                            SUPERVISED_BOUND_S))
        assert code == EXIT_RUN_TASK_CRASHED and code != EXIT_FATAL_STORAGE
        assert app.failure_policy.terminal_failure is None
        _assert_all_closed(app)
    finally:
        _cleanup(app)


# ================================================ 9: queue accounting and reconnect stay safe
@pytest.mark.parametrize("kind", VENUES)
def test_terminal_discard_mode_balances_task_done_and_join_completes(kind, tmp_path, monkeypatch):
    app = _build_runner(kind, tmp_path, monkeypatch)
    try:
        client = app.client
        app.failure_policy.on_fatal(FatalStorageError("raw gone", stream=app.raw_wire_writer.stream_name), "raw_frame")
        assert client.discard_mode

        asyncio.run(_drive_worker(client, [_item(_trade_frame(kind, i), i) for i in range(1, 6)]))

        assert client.frames_discarded == 5 and client.frames_processed == 0
        assert client._ingest_queue._unfinished_tasks == 0
    finally:
        _cleanup(app)


class _ConnWs(_FakeWs):
    """A fake connection that also accepts the runner's on_open / keepalive traffic."""
    async def send(self, *a, **k) -> None:
        return None

    async def ping(self, *a, **k):
        return asyncio.get_running_loop().create_future()

    async def close(self, *a, **k) -> None:
        return None


@pytest.mark.parametrize("kind", VENUES)
def test_raw_fatal_does_not_reconnect_around_the_failure_and_isolation_survives(kind, tmp_path, monkeypatch):
    """Reconnect safety through each runner's REAL client: after a raw-evidence
    fatal the receive loop does not reconnect, and a route isolated earlier stays
    isolated (a reconnect must never resurrect a failed component)."""
    app = _build_runner(kind, tmp_path, monkeypatch)
    try:
        policy, client = app.failure_policy, app.client
        _break(monkeypatch, app.raw_wire_writer)
        connects: list[int] = []

        class Conn:
            async def __aenter__(self):
                connects.append(1)
                return _ConnWs(_raw_frames(kind))

            async def __aexit__(self, *a):
                return False
        monkeypatch.setattr("collector.collector.websocket_client.websockets.connect", lambda *a, **k: Conn())
        policy.on_fatal(FatalStorageError("dedup", stream="trades"), "worker")
        before = dict(policy.failed_components)

        asyncio.run(asyncio.wait_for(client.start(), 30))

        assert connects == [1], "no reconnect attempt after a raw-evidence failure"
        assert policy.terminal_failure is not None and policy.terminal_failure.origin == "raw_frame"
        assert client.discard_mode and client.running is False
        assert "trades" in policy.isolated_routes
        assert all(policy.failed_components[k] is v for k, v in before.items()), "earlier latches untouched"
    finally:
        _cleanup(app)


# ======================================================================== F-2: terminal shutdown
def _usdm(tmp_path, monkeypatch):
    return _build("usdm", tmp_path, monkeypatch)


def _break_quality_channel(app) -> None:
    """The first quality-writer operation during shutdown fails."""
    def boom(*a, **k):
        raise OSError(28, "No space left on device (injected, quality)")
    app.quality_writer.write = boom
    app._persist_quality_event = boom
    event = {"exchange": "BINANCE", "stream": "orderbook", "event_type": "ERROR", "reason": "x",
             "local_ts": _now()}
    app.validator.drain_quality_events = lambda: [event, event]


def test_quality_failure_during_async_shutdown_still_completes_shutdown(tmp_path, monkeypatch):
    app = _usdm(tmp_path, monkeypatch)
    try:
        _break_quality_channel(app)
        app.running = True

        async def run():
            app.tasks.append(asyncio.create_task(asyncio.sleep(3600)))
            app.tasks.append(asyncio.create_task(app.health_monitor.start()))
            await asyncio.sleep(0)
            assert app.health_monitor.running is True
            await asyncio.wait_for(app._async_shutdown(0, terminal=True), 30)
            await asyncio.gather(*app.tasks, return_exceptions=True)

        asyncio.run(run())
        assert app._closed and getattr(app, "_shutdown_done", False)
        _assert_all_closed(app)
        assert app.health_monitor.running is False, "the health monitor was stopped"
        assert all(t.done() for t in app.tasks), "application tasks were cancelled"
    finally:
        _cleanup(app)


def test_quality_failure_inside_shutdown_still_attempts_every_healthy_writer_close(tmp_path, monkeypatch):
    app = _usdm(tmp_path, monkeypatch)
    try:
        app._capture_raw_frame('{"keep": "me"}', local_receive_ts=_now())
        _break_quality_channel(app)

        def drain_boom():
            raise RuntimeError("quality queue drain exploded")
        app._drain_quality_queue_sync = drain_boom
        closed: list[str] = []
        real_close = app._close_writer_reporting_failure
        app._close_writer_reporting_failure = lambda name: closed.append(name) or real_close(name)

        app.shutdown()

        assert {"ob_writer", "raw_book_writer", "trades_writer", "raw_trades_writer", "mark_writer", "oi_writer",
                "liq_writer", "raw_wire_writer", "raw_rest_writer", "quality_writer"} <= set(closed)
        _assert_all_closed(app)
        assert list(app.raw_wire_writer.stream_dir.glob("*.seg")), "the healthy raw tail was published"
        assert app._closed and app._shutdown_done
    finally:
        _cleanup(app)


def test_one_failing_writer_close_does_not_stop_the_other_closes(tmp_path, monkeypatch):
    app = _usdm(tmp_path, monkeypatch)
    try:
        def close_boom(*a, **k):
            raise OSError(5, "I/O error (injected close)")
        app.ob_writer.close = close_boom
        app.shutdown()
        for name, writer in _all_writers(app).items():
            if name != "ob_writer":
                assert writer._lock_handle is None, f"{name} was skipped behind the failing close"
    finally:
        _publish_all(app)
        app.segment_dedup.close()


def test_an_interrupted_shutdown_is_retryable_and_never_leaves_writers_unclosed(tmp_path, monkeypatch):
    app = _usdm(tmp_path, monkeypatch)
    try:
        attempts: list[str] = []
        real_close = app._close_writer_reporting_failure
        state = {"interrupt": True}

        def close(name):
            attempts.append(name)
            if name == "raw_trades_writer" and state["interrupt"]:
                state["interrupt"] = False
                raise KeyboardInterrupt("interrupted partway through shutdown")
            return real_close(name)
        app._close_writer_reporting_failure = close

        with pytest.raises(KeyboardInterrupt):
            app.shutdown()
        assert app._closed and not getattr(app, "_shutdown_done", False), \
            "_closed means 'shutdown began', not 'cleanup finished'"
        assert any(w._lock_handle is not None for w in _all_writers(app).values())

        app.shutdown()                                           # the retry finishes the job

        _assert_all_closed(app)
        assert app._shutdown_done
        assert attempts.count("ob_writer") == 1, "a writer that was already closed is not closed twice"
        assert attempts.count("raw_trades_writer") == 2, "the interrupted close is attempted again"
        app.shutdown()                                           # and a third call is a no-op
        assert attempts.count("raw_trades_writer") == 2
    finally:
        _publish_all(app)
        app.segment_dedup.close()


_F2_SUBPROCESS = textwrap.dedent('''
    import asyncio, sys
    from collector import run_collector as rc
    from collector.collector import notifications
    from collector.collector.storage_errors import FatalStorageError
    notifications.set_notifier(notifications.NullNotifier())
    rc.validate_telegram_startup = lambda: None

    class App(rc.CollectorApp):
        async def start(self):
            def boom(*a, **k):
                raise OSError(28, "No space left on device (injected, quality)")
            self.quality_writer.write = boom
            self._persist_quality_event = boom
            ev = {"exchange": "BINANCE", "stream": "orderbook", "event_type": "ERROR", "reason": "x", "local_ts": 1}
            self.validator.drain_quality_events = lambda: [ev]
            self.running = True
            self.tasks.append(asyncio.create_task(self._failure_supervisor_loop()))
            self._on_fatal_storage(FatalStorageError(
                "raw boom", stream="raw_wire", component="parquet_writer",
                stage="rename", durability="unpublished"), origin="raw_frame")
            for _ in range(300):
                if getattr(self, "_shutdown_done", False):
                    break
                await asyncio.sleep(0.1)
            unclosed = [k for k, v in vars(self).items()
                        if v.__class__.__name__ == "ParquetWriter" and v._lock_handle is not None]
            print("UNCLOSED", unclosed)
    sys.exit(rc.main(App))
''')


def test_terminal_exit_70_survives_a_failing_quality_writer_in_a_real_process(tmp_path):
    (tmp_path / "data").mkdir()
    env = {**os.environ, "PYTHONPATH": f"{REPO_ROOT}{os.pathsep}{REPO_ROOT / 'collector'}"}
    started = time.monotonic()
    done = subprocess.run([sys.executable, "-c", _F2_SUBPROCESS], cwd=tmp_path, env=env,
                          capture_output=True, text=True, timeout=120)
    assert done.returncode == rc.EXIT_FATAL_STORAGE == 70, (done.returncode, done.stderr[-2500:])
    assert time.monotonic() - started < SUPERVISED_BOUND_S
    assert "UNCLOSED []" in done.stdout, done.stdout[-500:]
