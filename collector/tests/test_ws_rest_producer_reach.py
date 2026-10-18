"""A live REST producer keeps exactly the storage it can still write open.

The recovery path is not just raw REST capture: it persists reconstructed book rows
(raw_book_writer) and emits quality events (quality_writer + WAL). The open-interest poller
reaches oi_writer and quality events. A producer that ignores its cancellation therefore must
hold ALL of that open -- closing "only raw_rest/oi" left the producer writing into closed
writers. These tests use the REAL ``_recover_binance_book`` write path (fake HTTP edge only),
a producer whose HTTP wait swallows cancellation, and real writers instrumented to record
whether they were closed at the moment of each write.
"""
import ast
import asyncio
import json
import os
import signal
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

import run_collector
from collector.collector.canonical import CanonicalOrderBookEvent


def _event(u, U, pu=None):
    return CanonicalOrderBookEvent("BINANCE", "orderbook", 1, None, 2,
                                   bids=((Decimal("100.10000001"), Decimal("1.00000001")),),
                                   asks=((Decimal("101"), Decimal("1")),), update_id=u,
                                   first_update_id=U, previous_update_id=pu)


class _FakeWs:
    shutdown_deadline_s, worker_retire_timeout_s = 0.05, 0.05
    lifecycle_idle = ingress_stopped = True

    def stop(self):
        pass

    async def wait_stopped(self, timeout_s):
        return True


class _Resp:
    status, headers = 200, {}

    def __init__(self, body):
        self._b = body

    async def text(self):
        return self._b

    def raise_for_status(self):
        return None


class _Get:
    def __init__(self, box):
        self.box = box

    async def __aenter__(self):
        self.box.started.set()
        while not self.box.gate.is_set():            # CANCELLATION-RESISTANT: swallows every cancel
            try:
                await asyncio.sleep(0.005)
            except asyncio.CancelledError:
                self.box.cancels_swallowed += 1
        return _Resp(json.dumps({"lastUpdateId": 10, "bids": [["100.10000001", "1.00000001"]],
                                 "asks": [["101", "1"]]}))

    async def __aexit__(self, *e):
        return False


class _Session:
    def __init__(self, box, **kw):
        self.box = box

    async def __aenter__(self):
        return self

    async def __aexit__(self, *e):
        return False

    def get(self, url, **kw):
        return _Get(self.box)


class _Box:
    def __init__(self):
        self.gate, self.started, self.cancels_swallowed = asyncio.Event(), asyncio.Event(), 0


WRITERS = ("ob_writer", "raw_book_writer", "trades_writer", "raw_trades_writer", "mark_writer",
           "oi_writer", "liq_writer", "raw_wire_writer", "raw_rest_writer", "quality_writer")


def _instrument(app, producer_alive):
    """Record (writer, closed-at-write) per write and (writer, producer_alive) per close."""
    writes, closes = [], []
    for name in WRITERS:
        w = getattr(app, name)
        real_write, real_close = w.write, w.close

        def write(*a, _n=name, _w=w, _r=real_write, **k):
            writes.append((_n, _w._closed))
            return _r(*a, **k)

        def close(_n=name, _r=real_close):
            closes.append((_n, producer_alive()))
            return _r()
        w.write, w.close = write, close
    return writes, closes


def _app(tmp_path, monkeypatch):
    (tmp_path / "data").mkdir()
    monkeypatch.chdir(tmp_path)
    app = run_collector.CollectorApp()
    app.ws_clients = [_FakeWs()]
    return app


def _rows(writer):
    return sum(pq.read_table(p).num_rows for p in sorted(writer.stream_dir.glob("*.seg")))


@pytest.mark.asyncio
async def test_a_recovery_that_ignores_cancel_keeps_every_writer_it_can_reach_open(tmp_path, monkeypatch):
    import aiohttp
    app, box = _app(tmp_path, monkeypatch), _Box()
    monkeypatch.setattr(aiohttp, "ClientSession", lambda **kw: _Session(box, **kw))
    app.binance_book.buffer = [_event(11, 10), _event(12, 12, 11)]      # the snapshot WILL bridge
    shutdown_task = None
    producer = asyncio.ensure_future(app._recover_binance_book("regression"))
    app._recovery_task = producer
    writes, closes = _instrument(app, lambda: not producer.done())
    try:
        await asyncio.wait_for(box.started.wait(), 10)
        shutdown_task = asyncio.ensure_future(app._async_shutdown(signal.SIGTERM))
        done, _ = await asyncio.wait({shutdown_task}, timeout=15)       # not wait_for: it would wait on cancel
        assert done, "shutdown hung on a cancellation-resistant recovery (unbounded wait)"
        shutdown_task.result()

        # precondition: the producer really survived the cancel request
        assert not producer.done() and box.cancels_swallowed >= 1
        # everything the recovery can reach is still open ...
        for name in ("raw_rest_writer", "raw_book_writer", "quality_writer"):
            assert getattr(app, name)._closed is False, f"{name} closed beneath a live recovery"
        assert not getattr(app, "_quality_wal_closed", False), "quality WAL closed beneath a live recovery"
        # ... what it cannot reach is not held hostage (precise, not "close nothing")
        for name in ("ob_writer", "trades_writer", "oi_writer", "raw_wire_writer"):
            assert getattr(app, name)._closed is True, name
        # failure is explicit and shutdown is not reported done
        assert app.shutdown_failure == "rest_producer_not_stopped" and app.exit_code != 0
        assert not getattr(app, "_shutdown_done", False)

        # the producer now runs its REAL write path to completion
        box.gate.set()
        assert await asyncio.wait_for(producer, 10) is True
        assert ("raw_rest_writer", False) in writes, "recovery response was not captured"
        assert [w for w in writes if w[0] == "raw_book_writer"] == [("raw_book_writer", False)] * 2
        assert all(closed is False for _, closed in writes), f"a write hit a CLOSED writer: {writes}"
        assert not any(alive for n, alive in closes if n in ("raw_rest_writer", "raw_book_writer", "quality_writer")), closes

        # eventual cleanup: a later shutdown() finishes the job once the producer has stopped
        app.shutdown()
        assert all(getattr(app, n)._closed for n in WRITERS)
        assert getattr(app, "_quality_wal_closed", False) and app._shutdown_done
        assert app.shutdown_failure == "rest_producer_not_stopped"        # the failure is not erased
        assert _rows(app.raw_rest_writer) >= 1 and _rows(app.raw_book_writer) == 2
    finally:
        box.gate.set()                       # let a stubborn producer end, so nothing below can hang
        await asyncio.wait({t for t in (producer, shutdown_task) if t is not None}, timeout=10)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        app.ws_clients = []
        app.shutdown()


@pytest.mark.asyncio
async def test_an_oi_poller_that_ignores_cancel_holds_oi_raw_rest_and_quality_but_not_raw_book(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    release, wrote = asyncio.Event(), []

    async def stubborn_poll():
        while not release.is_set():
            try:
                await asyncio.sleep(0.005)
            except asyncio.CancelledError:
                pass
        wrote.append(("oi_writer", app.oi_writer._closed))
    app._oi_task = asyncio.ensure_future(stubborn_poll())
    await asyncio.sleep(0)
    try:
        await asyncio.wait_for(app._async_shutdown(signal.SIGTERM), 15)
        assert not app._oi_task.done()
        for name in ("oi_writer", "raw_rest_writer", "quality_writer"):
            assert getattr(app, name)._closed is False, name
        assert not getattr(app, "_quality_wal_closed", False)
        assert app.raw_book_writer._closed is True and app.trades_writer._closed is True   # not reachable by OI
        assert app.shutdown_failure == "rest_producer_not_stopped" and not getattr(app, "_shutdown_done", False)
        release.set()
        await asyncio.wait_for(asyncio.gather(app._oi_task), 10)
        assert wrote == [("oi_writer", False)]
        app.shutdown()
        assert all(getattr(app, n)._closed for n in WRITERS) and app._shutdown_done
    finally:
        release.set()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        app.ws_clients = []
        app.shutdown()


def test_the_declared_reach_covers_what_the_producers_can_actually_write():
    """Recompute each producer's reachable storage from the source (AST closure over self.<method>
    calls and getattr(self, "<name>") strings) and require REST_PRODUCER_REACH to contain it."""
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "run_collector.py")
    cls = [n for n in ast.parse(open(path).read()).body
           if isinstance(n, ast.ClassDef) and n.name == "CollectorApp"][0]
    meths = {n.name: n for n in cls.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    storage = {"ob_writer", "raw_book_writer", "trades_writer", "raw_trades_writer", "mark_writer", "oi_writer",
               "liq_writer", "raw_wire_writer", "quality_writer", "_quality_wal", "raw_capture"}

    def names(m):
        out = set()
        for n in ast.walk(meths[m]):
            if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id == "self":
                out.add(n.attr)
            if (isinstance(n, ast.Call) and getattr(n.func, "id", None) in ("getattr", "hasattr")
                    and len(n.args) >= 2 and isinstance(n.args[1], ast.Constant)):
                out.add(n.args[1].value)
        return out

    def reach(root):
        seen, stack, found = {root}, [root], set()
        while stack:
            for x in names(stack.pop()):
                if x in storage:
                    found.add({"_quality_wal": "quality_wal", "raw_capture": "raw_rest_writer"}.get(x, x))
                if x in meths and x not in seen:
                    seen.add(x)
                    stack.append(x)
        return found

    for root, key in (("_recover_binance_book", "recovery"), ("_poll_openinterest", "oi_poll")):
        found = reach(root)
        assert found, f"{root}: traced nothing (the tracer is broken)"
        assert found <= run_collector.REST_PRODUCER_REACH[key], (
            f"{root} can reach {sorted(found - run_collector.REST_PRODUCER_REACH[key])} "
            f"which REST_PRODUCER_REACH[{key!r}] does not hold open")
