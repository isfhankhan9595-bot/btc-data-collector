"""F5: fatal storage topology / fail-closed routing.

    RAW FAILURE (raw_wire / raw_rest)  -> controlled, non-zero process termination
    DERIVED FAILURE                    -> isolate ONLY that route
    QUALITY FAILURE                    -> degrade the quality channel, not the process
    ORDINARY ERROR                     -> continue (P0-1 worker isolation)
    UNKNOWN typed fatal                -> terminate (default-deny)

Everything below drives the REAL ``CollectorApp`` / ``WebSocketClient`` /
``RawCapture`` / ``ParquetWriter``. Storage faults are injected at the real
``os.replace`` seam, scoped to one stream directory, so the typed exception
comes from the writer's own failure latch -- nothing here fabricates a
``FatalStorageError`` except the tests whose point is classification itself.
"""
from __future__ import annotations

import asyncio
import inspect
import os
import re
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from collector import run_collector as rc
from collector.collector import notifications
from collector.collector import parquet_writer as pw_module
from collector.collector import raw_capture as raw_capture_module
from collector.collector import websocket_client as ws_module
from collector.collector.failure_topology import (
    STORAGE_STREAM_VERDICTS, VERDICT_DEGRADE_QUALITY, VERDICT_ISOLATE, VERDICT_TERMINATE,
    FailureRecord, classify_fatal, classify_stream)
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_wal import QualityEventWAL
from collector.collector.raw_capture import RawCapture, RawRestRecord, RawWireRecord
from collector.collector.segment_dedup import DedupStateError
from collector.collector.storage_errors import FatalStorageError
from collector.collector.websocket_client import IngestItem, WebSocketClient
from collector.tests.test_p0_4_runner_lifecycle import _build, _feed, _now, _publish_all
from collector.tests.test_parquet_writer_publication_failure import (
    SCHEMA, _fail_replace_to, _row, expect_fatal)

REAL_REPLACE = os.replace
REPO_ROOT = Path(__file__).resolve().parents[2]


# ----------------------------------------------------------------- fixtures / helpers
@pytest.fixture(autouse=True)
def alerts():
    """Hermetic operator alerts: captured, never sent anywhere."""
    sent: list[str] = []
    notifications.set_notifier(notifications.CallableNotifier(lambda message: sent.append(message) or True))
    yield sent
    notifications.set_notifier(None)


def _fail_segment_publish_for(monkeypatch, stream_dir: str):
    """Real ``os.replace`` failure for ONE stream directory's segment rename."""
    def replace(src, dst, *a, **k):
        target = str(dst)
        if target.endswith(".seg") and f"/raw/{stream_dir}/" in target:
            raise OSError(28, f"No space left on device (injected, {stream_dir})")
        return REAL_REPLACE(src, dst, *a, **k)
    monkeypatch.setattr(pw_module.os, "replace", replace)


@pytest.fixture
def app(tmp_path, monkeypatch):
    built = _build("usdm", tmp_path, monkeypatch)
    for name in ("validate_markprice", "validate_liquidation", "validate_orderbook"):
        monkeypatch.setattr(built.validator, name, lambda features: (True, ""))
    yield built
    _publish_all(built)
    built.segment_dedup.close()


def _frame(route: str, i: int = 1) -> dict:
    now = _now()
    if route == "markprice":
        return {"stream": "btcusdt@markPrice@1s",
                "data": {"E": now, "p": "100.5", "r": "0.0001", "T": now + 3_600_000}}
    if route == "liquidation":
        return {"stream": "btcusdt@forceOrder",
                "data": {"o": {"S": "BUY", "p": "100.5", "q": "1.0", "T": now, "X": "FILLED", "f": "IOC"}}}
    if route == "orderbook":
        return {"stream": "btcusdt@depth@100ms",
                "data": {"E": now, "U": 10 + i, "u": 10 + i, "pu": 9 + i,
                         "b": [[str(100.0 - k * 0.1), "1.0"] for k in range(10)],
                         "a": [[str(101.0 + k * 0.1), "1.0"] for k in range(10)]}}
    if route == "trades":
        return {"stream": "btcusdt@aggTrade", "data": {"E": now, "a": i, "p": "100", "q": "1", "m": False}}
    raise AssertionError(route)


#: route -> (app writer attribute that must fail, its stream directory)
ROUTE_WRITER = {
    "trades": ("raw_trades_writer", "binance_trades_raw"),
    "orderbook": ("ob_writer", "orderbook"),
    "markprice": ("mark_writer", "markprice"),
    "liquidation": ("liq_writer", "liquidation"),
}


def _break_route(app, monkeypatch, route: str) -> None:
    """Make the route's writer publish (and so fail) on its very next row."""
    attr, directory = ROUTE_WRITER[route]
    getattr(app, attr).segment_rows = 1
    _fail_segment_publish_for(monkeypatch, directory)


class _FakeWs:
    def __init__(self, frames):
        self.frames = list(frames)

    def __aiter__(self):
        async def gen():
            for frame in self.frames:
                yield frame
        return gen()


def _item(data, i=0) -> IngestItem:
    return IngestItem(msg=data, local_receive_ts=_now() + i, connection_id="c", connection_generation=1,
                      data=data, decode_error=None, is_control=False)


def _fatal(stream="markprice", **kw) -> FatalStorageError:
    return FatalStorageError("injected fatal", stream=stream, component=kw.pop("component", "parquet_writer"),
                             stage=kw.pop("stage", "rename"), durability=kw.pop("durability", "unpublished"))


def _wal_events(app) -> list[dict]:
    return QualityEventWAL.recover(app.quality_writer.stream_dir / "wal")


# =============================================================== 1-4, 13: taxonomy & classification
def test_fatal_storage_error_is_typed_and_is_not_an_oserror():
    """Mutation guard: FatalStorageError must never inherit OSError (an
    ``except OSError`` for a recoverable condition must not swallow it)."""
    assert issubclass(FatalStorageError, RuntimeError) and not issubclass(FatalStorageError, OSError)
    assert issubclass(DedupStateError, FatalStorageError) and not issubclass(DedupStateError, OSError)
    error = DedupStateError("dedup lookup failed")           # message-only constructor is preserved
    assert (error.stream, error.component, error.stage) == ("trades", "dedup", "dedup_state")


def test_ordinary_handler_error_remains_non_fatal_and_keeps_the_worker_alive():
    seen, fatals = [], []

    async def on_message(data, ts, connection_id=None):
        if data["boom"]:
            raise ValueError("ordinary handler bug")
        seen.append(data["n"])

    async def run():
        client = WebSocketClient("ws://x", on_message, on_fatal=lambda exc, origin: fatals.append((exc, origin)))
        client.running = True
        worker = asyncio.create_task(client._process_queue())
        for n, boom in enumerate([False, True, False]):
            client._ingest_queue.put_nowait(_item({"n": n, "boom": boom}))
        await asyncio.wait_for(client._ingest_queue.join(), 5)
        client.running = False
        await worker
        return client

    client = asyncio.run(run())
    assert seen == [0, 2], "the worker survived the ordinary error and kept FIFO order"
    assert client.processing_errors == 1 and fatals == [] and not client.discard_mode


def test_typed_fatal_at_the_worker_boundary_is_not_an_ordinary_processing_error():
    fatals = []

    async def on_message(data, ts, connection_id=None):
        raise _fatal("markprice")

    async def run():
        client = WebSocketClient("ws://x", on_message, on_fatal=lambda exc, origin: fatals.append((exc.stream, origin)))
        client.running = True
        worker = asyncio.create_task(client._process_queue())
        client._ingest_queue.put_nowait(_item({"n": 1}))
        await asyncio.wait_for(client._ingest_queue.join(), 5)
        client.running = False
        await worker
        return client

    client = asyncio.run(run())
    assert fatals == [("markprice", "worker")]
    assert client.processing_errors == 0, "a typed fatal must never be demoted to an ordinary error"
    assert client.fatal_storage_errors == 1
    assert not client.discard_mode, "the app (not the client) decides a derived route is not terminal"


def test_plain_runtime_error_stays_ordinary_even_when_its_text_says_FAILED():
    """Mutation guards: no RuntimeError classification, no 'FAILED' substring matching."""
    fatals = []
    lookalike = RuntimeError("ParquetWriter for 'raw_wire' is FAILED: segment publication did not complete")

    async def on_message(data, ts, connection_id=None):
        raise lookalike

    async def run():
        client = WebSocketClient("ws://x", on_message, on_fatal=lambda exc, origin: fatals.append(exc))
        client.running = True
        worker = asyncio.create_task(client._process_queue())
        client._ingest_queue.put_nowait(_item({"n": 1}))
        await asyncio.wait_for(client._ingest_queue.join(), 5)
        client.running = False
        await worker
        return client

    client = asyncio.run(run())
    assert client.processing_errors == 1 and fatals == [] and client.fatal_storage_errors == 0
    with pytest.raises(TypeError):
        classify_fatal(lookalike)                              # the classifier refuses non-typed input outright


def test_app_refuses_to_classify_a_non_typed_exception(app):
    with pytest.raises(TypeError):
        app._on_fatal_storage(RuntimeError("writer is FAILED"), origin="handler")
    assert app.failed_components == {} and app.terminal_failure is None


def test_raw_capture_treats_a_failed_looking_runtime_error_as_ordinary_fail_open():
    class Writer:
        def write(self, row):
            raise RuntimeError("ParquetWriter is FAILED")
    sunk = []
    capture = RawCapture(Writer(), Writer(), quality_event_sink=sunk.append, fail_closed_on_fatal_storage=True)
    assert capture.capture_wire(RawWireRecord(local_receive_ts=_now(), payload="{}", venue="BINANCE")) is False
    assert capture.capture_failures == 1 and capture.fatal_capture_failures == 0 and len(sunk) == 1


def test_unknown_typed_fatal_defaults_to_terminate(app):
    assert classify_stream("a_stream_nobody_mapped") == (VERDICT_TERMINATE, None)
    assert classify_stream(None) == (VERDICT_TERMINATE, None)
    verdict = app._on_fatal_storage(_fatal("a_stream_nobody_mapped"), origin="handler")
    assert verdict == VERDICT_TERMINATE and app.terminal_failure is not None
    assert app.exit_code != 0 and app.isolated_routes == {}


def test_stream_verdict_table_pins_the_frozen_topology():
    assert STORAGE_STREAM_VERDICTS["raw_wire"][0] == STORAGE_STREAM_VERDICTS["raw_rest"][0] == VERDICT_TERMINATE
    for stream, route in (("trades", "trades"), ("binance_trades_raw", "trades"), ("orderbook", "orderbook"),
                          ("binance_orderbook_raw", "orderbook"), ("markprice", "markprice"),
                          ("openinterest", "openinterest"), ("liquidation", "liquidation")):
        assert STORAGE_STREAM_VERDICTS[stream] == (VERDICT_ISOLATE, route), stream
    assert STORAGE_STREAM_VERDICTS["quality_events"] == (VERDICT_DEGRADE_QUALITY, None)
    # A raw-evidence ORIGIN is terminal whatever stream name the exception carries.
    assert classify_fatal(_fatal("markprice"), origin="raw_frame") == (VERDICT_TERMINATE, None)
    assert classify_fatal(_fatal("markprice"), origin="raw_rest") == (VERDICT_TERMINATE, None)


# ============================================================ 26, 27: writer evidence / closed refusal
def test_first_failure_is_typed_with_cause_stage_and_durability(tmp_path, monkeypatch):
    w = ParquetWriter("s", SCHEMA, base_dir=str(tmp_path))
    w.write(_row(0))
    _fail_replace_to(monkeypatch, ".seg")
    with expect_fatal(OSError, stage="rename", durability="unpublished", stream="s") as info:
        w.publish_open_segment()
    assert info.value.__cause__.errno == 28
    described = info.value.describe()
    assert described["stage"] == "rename" and "No space left" in described["cause"]
    snapshot = w.failure_snapshot()                      # read-only latch view for the supervisor
    assert (snapshot.stage, snapshot.durability, snapshot.kind) == ("rename", "unpublished", "storage")
    with expect_fatal(OSError, stage="rename"):          # every later refusal is the same typed fatal
        w.write(_row(1))


def test_closed_writer_refusal_is_not_a_fatal_storage_error(tmp_path):
    w = ParquetWriter("s", SCHEMA, base_dir=str(tmp_path))
    w.write(_row(0))
    w.close()
    with pytest.raises(RuntimeError) as info:
        w.write(_row(1))
    assert not isinstance(info.value, FatalStorageError), "closing is not a storage failure"
    assert w.failure_snapshot() is None


# ================================================ 7-9, 11: derived writer failure isolates ONLY that route
@pytest.mark.parametrize("route", ["trades", "orderbook", "markprice", "liquidation"])
def test_derived_writer_failure_isolates_only_its_route_and_not_the_process(app, monkeypatch, route):
    monkeypatch.chdir(app.quality_writer.stream_dir.parents[2])
    _break_route(app, monkeypatch, route)
    other = next(r for r in ROUTE_WRITER if r != route)

    asyncio.run(app.handle_message(_frame(route)))        # real writer failure inside the real handler

    assert set(app.isolated_routes) == {route}
    record = app.isolated_routes[route]
    assert (record.verdict, record.route, record.origin) == (VERDICT_ISOLATE, route, "handler") or \
        record.verdict == VERDICT_ISOLATE
    assert record.stage == "rename" and record.durability == "unpublished" and record.first_observed_ts > 0
    assert app.terminal_failure is None and app.exit_code == 0, "a derived failure must not kill the process"
    assert all(not c.discard_mode for c in app.ws_clients), "ingestion and raw capture stay alive"

    # the unrelated route keeps working end to end
    monkeypatch.undo()
    _fail_segment_publish_for(monkeypatch, ROUTE_WRITER[route][1])
    asyncio.run(app.handle_message(_frame(other, 2)))
    assert other not in app.isolated_routes
    assert app.stream_counters[other]["received"] >= 1


def test_dedup_state_failure_isolates_trades(app):
    coord = app.segment_dedup.coordinators["trades"]
    coord.index._conn.close()                             # the durable dedup state is unreachable
    _feed("usdm", app, ["61"])
    record = app.isolated_routes["trades"]
    assert (record.component, record.stream, record.verdict) == ("dedup", "trades", VERDICT_ISOLATE)
    assert app.terminal_failure is None
    assert app.raw_trades_writer.buffer == [], "an unreadable index must not be mapped to 'new'"


# ============================================ 15, 16: no flood after isolation; unrelated FIFO untouched
def test_isolated_route_short_circuits_without_a_per_frame_error_flood(app, monkeypatch, alerts):
    _break_route(app, monkeypatch, "markprice")
    asyncio.run(app.handle_message(_frame("markprice")))
    assert "markprice" in app.isolated_routes
    calls = {"quality": 0, "report": 0, "mark_write": 0}
    real_persist, real_report = app._persist_quality_event, app._report_storage_failure
    app._persist_quality_event = lambda event: calls.__setitem__("quality", calls["quality"] + 1) or real_persist(event)
    app._report_storage_failure = lambda record: calls.__setitem__("report", calls["report"] + 1) or real_report(record)
    app.mark_writer.write = lambda *a, **k: calls.__setitem__("mark_write", calls["mark_write"] + 1)
    alerts_before = len(alerts)

    async def burst():
        for _ in range(200):
            await app.handle_message(_frame("markprice"))
    asyncio.run(burst())

    assert app.route_short_circuits["markprice"] == 200
    assert calls == {"quality": 0, "report": 0, "mark_write": 0}, "no event, report or writer call per frame"
    assert len(alerts) == alerts_before


def test_unrelated_routes_keep_fifo_order_while_a_route_is_isolated_and_the_queue_balances(app, monkeypatch):
    _break_route(app, monkeypatch, "markprice")
    client = app.ws_clients[1]
    order = [("markprice", 0), ("trades", 1), ("markprice", 2), ("trades", 3), ("markprice", 4), ("trades", 5)]

    async def run():
        client.running = True
        worker = asyncio.create_task(client._process_queue())
        for i, (route, n) in enumerate(order):
            client._ingest_queue.put_nowait(_item(_frame(route, n), i))
        await asyncio.wait_for(client._ingest_queue.join(), 10)
        client.running = False
        await worker

    asyncio.run(run())
    written = [r["native_trade_id"] for r in app.raw_trades_writer.buffer]
    assert written == ["1", "3", "5"], "unrelated trades kept strict arrival order"
    assert set(app.isolated_routes) == {"markprice"} and app.route_short_circuits["markprice"] == 2
    assert client._ingest_queue._unfinished_tasks == 0 and client.processing_errors == 0   # task_done exactly once each


# ============================================================ 5, 6, 24: raw failure terminates
def _break_raw(app, monkeypatch, name):
    getattr(app, f"{name}_writer").segment_rows = 1
    _fail_segment_publish_for(monkeypatch, name)


def test_raw_wire_failure_terminates_through_the_controlled_path_with_nonzero_exit(app, monkeypatch, alerts):
    _break_raw(app, monkeypatch, "raw_wire")
    client = app.ws_clients[0]

    async def run():
        app.running = True
        client.running = True
        supervisor = asyncio.create_task(app._failure_supervisor_loop())
        await client._consume(_FakeWs(['{"stream": "btcusdt@aggTrade", "data": {}}', "second frame"]))
        assert app.terminal_failure is not None, "the raw-frame callback's typed fatal reached the app"
        await asyncio.wait_for(supervisor, 10)             # the SUPERVISOR starts the shutdown, not the worker
        await asyncio.wait_for(app._terminal_shutdown_task, 10)

    asyncio.run(run())
    record = app.terminal_failure
    assert (record.stream, record.verdict, record.origin) == ("raw_wire", VERDICT_TERMINATE, "raw_frame")
    assert app._closed is True and app.running is False
    assert app.exit_code != 0
    assert all(c.discard_mode for c in app.ws_clients), "normal ingestion stopped on every client"
    assert client.frames_enqueued == 0, "no frame that missed durable capture was enqueued"
    assert any("raw_wire" in message for message in alerts)


def test_raw_rest_failure_terminates_and_never_feeds_the_canonical_oi_writer(app, monkeypatch):
    _break_raw(app, monkeypatch, "raw_rest")
    captured = []
    real_capture = app._capture_rest
    app._capture_rest = lambda record: captured.append(record) or real_capture(record)
    app.oi_writer.write = lambda *a, **k: pytest.fail("a response that was not durably captured fed canonical OI")
    _install_fake_oi_http(monkeypatch, app, polls=3)

    asyncio.run(app._poll_openinterest())

    record = app.terminal_failure
    assert (record.stream, record.verdict, record.origin) == ("raw_rest", VERDICT_TERMINATE, "raw_rest")
    assert app.exit_code != 0
    assert [r.ok for r in captured] == [True], "polling stopped at the first lost capture; no contradictory ok=False record"


def test_terminal_process_exit_status_is_nonzero_in_a_real_process(tmp_path):
    (tmp_path / "data").mkdir()
    script = textwrap.dedent('''
        import asyncio, sys
        from collector import run_collector as rc
        from collector.collector import notifications
        from collector.collector.storage_errors import FatalStorageError
        notifications.set_notifier(notifications.NullNotifier())
        rc.validate_telegram_startup = lambda: None
        FAIL = sys.argv[1] == "fail"

        class App(rc.CollectorApp):
            async def start(self):
                self.running = True
                self.tasks.append(asyncio.create_task(self._failure_supervisor_loop()))
                if FAIL:
                    self._on_fatal_storage(FatalStorageError(
                        "raw boom", stream="raw_wire", component="parquet_writer",
                        stage="rename", durability="unpublished"), origin="raw_frame")
                    for _ in range(100):
                        if self._closed:
                            break
                        await asyncio.sleep(0.1)
                else:
                    self.shutdown()
        sys.exit(rc.main(App))
    ''')
    env = {**os.environ, "PYTHONPATH": str(REPO_ROOT)}
    failed = subprocess.run([sys.executable, "-c", script, "fail"], cwd=tmp_path, env=env,
                            capture_output=True, text=True, timeout=120)
    healthy = subprocess.run([sys.executable, "-c", script, "ok"], cwd=tmp_path, env=env,
                             capture_output=True, text=True, timeout=120)
    assert failed.returncode == rc.EXIT_FATAL_STORAGE != 0, failed.stderr[-1500:]
    assert healthy.returncode == 0, healthy.stderr[-1500:]


def test_raw_capture_fail_closed_reraises_typed_fatal_without_touching_the_quality_sink():
    class Writer:
        def write(self, row):
            raise _fatal("raw_wire")
    sunk = []
    closed = RawCapture(Writer(), Writer(), quality_event_sink=sunk.append, fail_closed_on_fatal_storage=True)
    with pytest.raises(FatalStorageError):
        closed.capture_wire(RawWireRecord(local_receive_ts=_now(), payload="{}", venue="BINANCE"))
    with pytest.raises(FatalStorageError):
        closed.capture_rest(RawRestRecord(request_ts=_now(), response_receive_ts=_now(), endpoint="e", purpose="p"))
    assert sunk == [], "the quality sink (possibly the failing component) is never consulted"
    # Other venues' runners (no application termination path) are unchanged: fail open.
    opened = RawCapture(Writer(), Writer(), quality_event_sink=sunk.append)
    assert opened.capture_wire(RawWireRecord(local_receive_ts=_now(), payload="{}", venue="BINANCE")) is False
    assert opened.fatal_capture_failures == 1 and len(sunk) == 1


# ================================================================== 10: OI canonical isolation
def _install_fake_oi_http(monkeypatch, app, polls):
    body = '{"openInterest": "2.5", "time": %d, "symbol": "BTCUSDT"}'

    class FakeResponse:
        status = 200
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def text(self): return body % _now()
        def raise_for_status(self): return None

    class FakeSession:
        def __init__(self, *a, **k): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        def get(self, url): return FakeResponse()

    class FakeAiohttp:
        ClientSession = FakeSession
        class ClientTimeout:
            def __init__(self, total): self.total = total

    state = {"n": 0}

    async def fast_sleep(seconds):
        state["n"] += 1
        if state["n"] >= polls:
            app.running = False
    monkeypatch.setitem(sys.modules, "aiohttp", FakeAiohttp)
    monkeypatch.setattr(rc.asyncio, "sleep", fast_sleep)
    app.running = True


def test_oi_writer_failure_isolates_canonical_oi_while_raw_rest_stays_authoritative(app, monkeypatch):
    captured = []
    real_capture = app._capture_rest
    app._capture_rest = lambda record: captured.append(record) or real_capture(record)
    app.oi_writer.segment_rows = 1
    _fail_segment_publish_for(monkeypatch, "openinterest")
    oi_calls = {"n": 0}
    real_write = app.oi_writer.write
    app.oi_writer.write = lambda *a, **k: oi_calls.__setitem__("n", oi_calls["n"] + 1) or real_write(*a, **k)
    _install_fake_oi_http(monkeypatch, app, polls=3)

    asyncio.run(app._poll_openinterest())

    assert set(app.isolated_routes) == {"openinterest"} and app.terminal_failure is None
    assert oi_calls["n"] == 1, "the failed canonical writer is never called again"
    assert app.route_short_circuits["openinterest"] == 2
    assert [(r.purpose, r.ok) for r in captured] == [("open_interest", True)] * 3, \
        "raw REST kept capturing every poll, each exactly once, never contradicted by an ok=False twin"
    assert app.stream_counters["openinterest"]["received"] == 3


# ========================================================== 11b, 14: liquidation boundary distinctions
def test_liquidation_writer_fatal_isolates_liquidation_and_is_not_swallowed(app, monkeypatch):
    _break_route(app, monkeypatch, "liquidation")
    asyncio.run(app.handle_message(_frame("liquidation")))
    assert set(app.isolated_routes) == {"liquidation"}
    assert app.stream_counters["liquidation"]["rejected"] == 0, "no longer swallowed as a 'rejected' frame"


def test_a_quality_fatal_inside_the_liquidation_handler_is_not_misclassified_as_liquidation(app, monkeypatch):
    def quality_dies(features):
        raise _fatal("quality_events")
    monkeypatch.setattr(app.liq_writer, "write", quality_dies)
    asyncio.run(app.handle_message(_frame("liquidation")))
    assert "liquidation" not in app.isolated_routes, "must follow the failing component's own boundary"
    assert app.quality_degraded is not None and app.quality_degraded.stream == "quality_events"
    assert app.terminal_failure is None

    # An unmapped typed fatal in the same handler terminates (default-deny); ordinary errors stay ordinary.
    monkeypatch.setattr(app.liq_writer, "write", lambda features: (_ for _ in ()).throw(_fatal("mystery")))
    asyncio.run(app.handle_message(_frame("liquidation")))
    assert app.terminal_failure is not None and app.terminal_failure.stream == "mystery"


def test_ordinary_liquidation_error_keeps_the_existing_rejected_behaviour(app, monkeypatch):
    monkeypatch.setattr(app.liq_writer, "write", lambda features: (_ for _ in ()).throw(ValueError("ordinary")))
    asyncio.run(app.handle_message(_frame("liquidation")))
    assert app.stream_counters["liquidation"]["rejected"] == 1
    assert app.isolated_routes == {} and app.failed_components == {}


# ==================================== 19: reconnect cannot clear a latch (client AND application level)
def test_reconnect_does_not_clear_an_isolated_route_or_a_client_fatal_latch(app, monkeypatch):
    _break_route(app, monkeypatch, "markprice")
    asyncio.run(app.handle_message(_frame("markprice")))
    before = dict(app.failed_components)
    for url in (rc.BINANCE_PUBLIC_WS_URL, rc.BINANCE_MARKET_WS_URL):
        app._make_reconnect_handler(url)()                 # exactly what the client calls on every (re)connect
    assert app.failed_components == before and set(app.isolated_routes) == {"markprice"}
    asyncio.run(app.handle_message(_frame("markprice", 2)))
    assert app.route_short_circuits["markprice"] == 1, "still isolated after the reconnect handler ran"
    assert app.mark_writer.failure_snapshot() is not None, "the writer is not resurrected"


def test_raw_frame_fatal_stops_the_client_and_it_does_not_reconnect_around_the_failure(monkeypatch):
    connects, fatals = [], []

    class Conn:
        async def __aenter__(self):
            connects.append(1)
            return _FakeWs(["frame-1", "frame-2"])
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(ws_module.websockets, "connect", lambda url: Conn())

    def raw_frame(frame, **kw):
        raise _fatal("raw_wire")

    async def run():
        client = WebSocketClient("ws://x", lambda data, ts: asyncio.sleep(0),
                                 on_raw_frame=raw_frame, on_fatal=lambda exc, origin: fatals.append(origin))
        await asyncio.wait_for(client.start(), 10)
        return client

    client = asyncio.run(run())
    assert connects == [1], "no reconnect attempt after a raw-evidence failure"
    assert fatals == ["raw_frame"] and client.discard_mode and client.running is False
    assert isinstance(client.fatal_error, FatalStorageError), "the first fatal stays latched on the client"


def test_client_without_a_classifier_defaults_to_deny_and_surfaces_the_fatal(monkeypatch):
    class Conn:
        async def __aenter__(self): return _FakeWs(["f"])
        async def __aexit__(self, *a): return False
    monkeypatch.setattr(ws_module.websockets, "connect", lambda url: Conn())

    def raw_frame(frame, **kw):
        raise _fatal("raw_wire")

    async def run():
        client = WebSocketClient("ws://x", lambda data, ts: asyncio.sleep(0), on_raw_frame=raw_frame)
        await asyncio.wait_for(client.start(), 10)
    with pytest.raises(FatalStorageError):
        asyncio.run(run())


# =================================================== 17, 18: discard mode keeps the queue accounting exact
def test_terminal_discard_mode_balances_task_done_and_queue_join_completes():
    """Discard mode must drain exactly what is queued, call task_done() once per
    item, never process a frame, and leave NO worker task behind."""
    processed = []

    async def on_message(data, ts, connection_id=None):
        processed.append(data["n"])

    async def run():
        client = WebSocketClient("ws://x", on_message, ingest_queue_maxsize=8)
        client.running = True
        for n in range(8):                                  # the bounded queue is completely full
            client._ingest_queue.put_nowait(_item({"n": n}, n))
        client.enter_discard_mode("test")
        worker = asyncio.create_task(client._process_queue())
        await asyncio.wait_for(client._ingest_queue.join(), 10)     # must not hang
        await asyncio.wait_for(worker, 10)                  # the worker exits by itself: no leaked task
        # a late producer (the receive loop) is stopped at its first frame: nothing new is enqueued
        await client._consume(_FakeWs(["late-1", "late-2"]))
        return client

    client = asyncio.run(run())
    assert processed == [], "no frame is ever processed in discard mode"
    assert client.frames_discarded == 8
    assert client._ingest_queue._unfinished_tasks == 0, "task_done() exactly once per queue item"
    assert client.frames_enqueued == 0 and client.discard_reason == "test"


def test_a_producer_blocked_on_a_full_queue_is_released_by_the_latch_and_join_still_completes():
    async def on_message(data, ts, connection_id=None):
        raise AssertionError("discarded frames must never reach on_message")

    async def run():
        client = WebSocketClient("ws://x", on_message, ingest_queue_maxsize=2)
        client.running = True
        producer = asyncio.create_task(client._consume(_FakeWs([f"{{\"n\": {n}}}" for n in range(20)])))
        for _ in range(200):                                # let it fill the queue and block on the full-queue wait
            await asyncio.sleep(0.01)
            if client.queue_backpressure_events:
                break
        assert client.queue_backpressure_events == 1 and not producer.done()
        client.enter_discard_mode("test")
        await asyncio.wait_for(producer, 10)                # released, never blocked forever
        worker = asyncio.create_task(client._process_queue())
        await asyncio.wait_for(client._ingest_queue.join(), 10)
        await asyncio.wait_for(worker, 10)
        return client

    client = asyncio.run(run())
    assert client._ingest_queue._unfinished_tasks == 0
    assert client.frames_discarded == 2 and client.frames_abandoned_at_shutdown == 1


# ================================================================== 20: latent failure supervision
def test_supervisor_detects_a_latched_writer_failure_without_any_further_write(app):
    writer = app.raw_trades_writer
    writer.segment_rows = 2

    def hostile(token, path):
        raise DedupStateError("simulated index commit failure")
    writer.on_segment_published = hostile
    _feed("usdm", app, ["31", "32"])                        # 2nd row publishes; hook fails SILENTLY (no raise)
    assert writer._publication_failure is not None
    assert app.isolated_routes == {} and app.failed_components == {}, "nothing has noticed yet"

    app.supervise_once()                                    # no further row ever arrives

    record = app.isolated_routes["trades"]
    assert (record.stream, record.stage, record.durability, record.origin) == \
        ("binance_trades_raw", "publication_hook", "published", "supervisor")
    assert writer.failure_snapshot() is not None, "the supervisor reports; it never repairs"
    assert writer._publication_failure is not None


def test_supervisor_loop_polls_and_stops_after_a_terminal_failure(app, monkeypatch):
    monkeypatch.setattr(rc, "FAILURE_SUPERVISOR_INTERVAL_S", 0.01)
    app.raw_wire_writer._storage_failure = OSError("latched while quiet")
    app.raw_wire_writer._storage_failure_stage = "tmp_fsync"
    app.raw_wire_writer._storage_failure_durability = "unpublished"

    async def run():
        app.running = True
        await asyncio.wait_for(app._failure_supervisor_loop(), 10)
        await asyncio.wait_for(app._terminal_shutdown_task, 10)
    asyncio.run(run())
    assert app.terminal_failure.stream == "raw_wire" and app.terminal_failure.origin == "supervisor"
    assert app._closed and app.exit_code != 0


def test_supervisor_ignores_non_snapshot_writers(app):
    app.mark_writer = object()                              # e.g. a test double with no latch API
    app.supervise_once()
    assert app.failed_components == {}


# ===================================================== 25: F1 marker / publication failures are typed
def test_f1_marker_failure_on_a_derived_anchor_writer_isolates_trades(app, monkeypatch):
    writer = app.raw_trades_writer
    writer.segment_rows = 2
    _fail_replace_to(monkeypatch, ".meta.json")             # the F1 publication marker cannot be made durable
    _feed("usdm", app, ["41", "42"])
    assert writer._publication_failure is not None
    _feed("usdm", app, ["43"], _now() + 5)                  # next write hits the typed refusal
    record = app.isolated_routes["trades"]
    assert (record.stage, record.durability) == ("marker", "published")
    assert app.terminal_failure is None


def test_f1_marker_failure_on_raw_wire_is_terminal(app, monkeypatch):
    writer = app.raw_wire_writer
    writer.segment_rows = 1
    _fail_replace_to(monkeypatch, ".meta.json")
    app._capture_raw_frame("{}", local_receive_ts=_now())    # publishes; marker fails silently, segment durable
    assert writer._publication_failure is not None
    with pytest.raises(FatalStorageError) as info:
        app._capture_raw_frame("{}", local_receive_ts=_now())
    assert info.value.stage == "marker" and info.value.stream == "raw_wire"
    assert app._on_fatal_storage(info.value, origin="raw_frame") == VERDICT_TERMINATE


# ======================================== 21, 22, 23: quality channel, recursion, shutdown
def test_terminal_reporting_never_recurses_through_the_quality_writer(app, alerts):
    def forbidden(*a, **k):
        raise AssertionError("failure reporting called the quality writer / quality persist path")
    app.quality_writer.write = forbidden
    app.quality_writer.publish_open_segment = forbidden
    app._persist_quality_event = forbidden

    verdict = app._on_fatal_storage(_fatal("raw_wire"), origin="raw_frame")

    assert verdict == VERDICT_TERMINATE and app.terminal_failure is not None
    assert any("raw_wire" in m and "terminate" in m for m in alerts), "bounded operator alert sent"
    reports = [e for e in _wal_events(app) if e.get("stream") == "storage_failure"]
    assert len(reports) == 1 and "terminate" in reports[0]["reason"] and "raw_wire" in reports[0]["reason"]
    app._on_fatal_storage(_fatal("raw_wire"), origin="raw_frame")           # latch once: no second report
    assert len([e for e in _wal_events(app) if e.get("stream") == "storage_failure"]) == 1
    assert app.failure_repeats["parquet_writer:raw_wire"] == 1


def test_quality_writer_failure_degrades_the_channel_without_killing_healthy_raw_capture(app, monkeypatch):
    app.quality_writer.segment_rows = 1
    _fail_segment_publish_for(monkeypatch, "quality_events")
    with pytest.raises(FatalStorageError):                  # the first failing event still raises (existing contract)
        app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "first"})
    assert app.quality_degraded is not None and app.quality_degraded.verdict == VERDICT_DEGRADE_QUALITY
    assert app.terminal_failure is None and app.exit_code == 0

    # later events: no writer call, no per-event raise/log flood; the WAL copy is the durable record
    app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "later"})
    # both events have an established WAL record: "first" (which tripped the writer) and "later"
    assert app._quality_events_wal_only == 2
    assert {"first", "later"} <= {e["reason"] for e in _wal_events(app)}

    # raw capture is untouched
    monkeypatch.undo()
    app._capture_raw_frame('{"x": 1}', local_receive_ts=_now())
    assert app.raw_capture.wire_captured == 1 and not any(c.discard_mode for c in app.ws_clients)
    assert app.raw_wire_writer.failure_snapshot() is None


def test_shutdown_after_a_writer_failure_still_closes_every_unrelated_healthy_writer(app, monkeypatch):
    app._capture_raw_frame('{"keep": "me"}', local_receive_ts=_now())       # healthy raw_wire holds a buffered row
    _break_route(app, monkeypatch, "markprice")
    asyncio.run(app.handle_message(_frame("markprice")))
    assert "markprice" in app.isolated_routes

    app.shutdown()                                           # the failed writer's close() raises internally

    assert list((app.raw_wire_writer.stream_dir).glob("*.seg")), "raw_wire's tail was published at shutdown"
    for name in ("raw_wire_writer", "raw_rest_writer", "raw_trades_writer", "trades_writer", "ob_writer",
                 "oi_writer", "liq_writer", "quality_writer"):
        assert getattr(app, name)._lock_handle is None, f"{name} was not closed"
    assert app.mark_writer._lock_handle is None, "even the failed writer releases its stream lock"
    assert app._closed


# ============================================== mutation-resistance (structure) guards
def _source(obj) -> str:
    return inspect.getsource(obj)


def _code_only(text: str) -> str:
    """Source with comments and docstrings removed: the guards below must look at
    what the code DOES, not at prose that merely mentions a forbidden name."""
    text = re.sub(r"\"\"\"[\s\S]*?\"\"\"|'''[\s\S]*?'''", "", text)
    return re.sub(r"(?m)^\s*#.*$|\s+#[^\n\"']*$", "", text)


def test_typed_fatal_branches_precede_the_generic_exception_handlers():
    """A typed-fatal branch placed AFTER ``except Exception`` would be dead code."""
    def order(source: str, typed: str, generic: str) -> bool:
        return source.index(typed) < source.index(generic)
    assert order(_source(WebSocketClient._process_queue), "except FatalStorageError", "except Exception")
    assert order(_source(WebSocketClient._consume), "except FatalStorageError", "except Exception as exc")
    assert order(_source(WebSocketClient.start), "except FatalStorageError", "except Exception as e:\n")
    assert order(_source(RawCapture.capture_wire), "except FatalStorageError", "except Exception")
    assert order(_source(RawCapture.capture_rest), "except FatalStorageError", "except Exception")
    assert order(_source(rc.CollectorApp._handle_liquidation), "except FatalStorageError", "except Exception")
    assert order(_source(rc.CollectorApp._poll_openinterest), "except FatalStorageError", "except Exception as e")
    assert order(_source(rc.CollectorApp._recover_binance_book), "except FatalStorageError", "except asyncio.TimeoutError")


def test_no_forbidden_exit_or_classification_shortcuts_in_the_f5_code():
    forbidden_exit = re.compile(r"os\._exit|\bexit\(|sys\.exit\(")
    for module in (ws_module, raw_capture_module, pw_module):
        assert not forbidden_exit.search(_code_only(_source(module))), f"{module.__name__} must never exit the process"
    runner = _code_only(_source(rc))
    assert [m.group(0) for m in forbidden_exit.finditer(runner)] == ["sys.exit("], \
        "the only process exit is the single sys.exit(main()) in the main thread"
    assert "os._exit" not in runner
    for module in (rc, ws_module, raw_capture_module):
        text = _code_only(_source(module))
        assert not re.search(r"isinstance\([^)]*RuntimeError", text), "never classify by RuntimeError"
        assert "FAILED" not in text, "never classify by the 'FAILED' substring"
    worker = _source(WebSocketClient._process_queue) + _source(WebSocketClient._handle_fatal)
    assert ".shutdown(" not in worker and "stop(" not in worker, "a worker must not shut down its parent"
    assert "task_done()" in _source(WebSocketClient._process_queue)
