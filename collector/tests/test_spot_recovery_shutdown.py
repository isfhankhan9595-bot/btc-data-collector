"""Spot: a cancellation-resistant book recovery must not hang shutdown, and must keep every
writer it can still reach open (raw REST capture, the snapshot-derived orderbook rows, quality
events) while the failure is made visible.

Uses the REAL ``_recover_book`` write path (only the HTTP edge is faked) and real writers
instrumented to record whether they were closed at each write.
"""
import ast
import asyncio
import json
import os
import time
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

import run_binance_spot_collector as spot
import run_collector
from collector.collector.canonical import CanonicalOrderBookEvent
from collector.collector.standalone_failure_policy import (
    REST_PRODUCER_CANCEL_BOUND_S, cancel_and_wait)


def _event(u, U, pu=None):
    return CanonicalOrderBookEvent("BINANCE", "orderbook", 1, None, 2,
                                   bids=((Decimal("100.10000001"), Decimal("1.00000001")),),
                                   asks=((Decimal("101"), Decimal("1")),), update_id=u,
                                   first_update_id=U, previous_update_id=pu)


class _IdleClient:
    shutdown_deadline_s = worker_retire_timeout_s = 0.05
    lifecycle_idle = ingress_stopped = True

    def stop(self):
        return None

    async def wait_stopped(self, timeout_s):
        return True


class _Resp:
    status, headers = 200, {}

    def __init__(self, b):
        self._b = b

    async def text(self):
        return self._b

    def raise_for_status(self):
        return None


class _Box:
    def __init__(self):
        self.gate, self.started, self.cancels_swallowed = asyncio.Event(), asyncio.Event(), 0


class _Get:
    def __init__(self, box):
        self.box = box

    async def __aenter__(self):
        self.box.started.set()
        while not self.box.gate.is_set():             # CANCELLATION-RESISTANT
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


WRITERS = ("raw_wire_writer", "raw_rest_writer", "trades_writer", "ob_writer", "quality_writer")


def _rows(writer):
    return sum(pq.read_table(p).num_rows for p in sorted(writer.stream_dir.glob("*.seg")))


@pytest.mark.asyncio
async def test_a_cancellation_resistant_recovery_is_bounded_holds_its_storage_open_and_fails_explicitly(
        tmp_path, monkeypatch):
    import aiohttp
    app, box = spot.BinanceSpotCollectorApp(data_dir=str(tmp_path)), _Box()
    monkeypatch.setattr(aiohttp, "ClientSession", lambda **kw: _Session(box, **kw))
    app.client = _IdleClient()
    app.book.buffer = [_event(11, 10), _event(12, 12, 11)]              # the snapshot WILL bridge
    producer = asyncio.ensure_future(app._recover_book("regression"))
    app._recovery_task = producer
    writes = []
    for name in WRITERS:
        w = getattr(app, name)
        real = w.write

        def write(*a, _n=name, _w=w, _r=real, **k):
            writes.append((_n, _w._closed))
            return _r(*a, **k)
        w.write = write
    shutdown_task = None
    try:
        await asyncio.wait_for(box.started.wait(), 10)
        t0 = time.monotonic()
        shutdown_task = asyncio.ensure_future(app.shutdown())
        done, _ = await asyncio.wait({shutdown_task}, timeout=15)   # not wait_for: it would wait on cancel
        assert done, "Spot shutdown hung on a cancellation-resistant recovery (unbounded wait)"
        shutdown_task.result()
        assert time.monotonic() - t0 < 2 * REST_PRODUCER_CANCEL_BOUND_S + 1.5, "shutdown was not bounded"

        assert not producer.done() and box.cancels_swallowed >= 1          # precondition
        for name in ("raw_rest_writer", "ob_writer", "quality_writer"):
            assert getattr(app, name)._closed is False, f"{name} closed beneath a live recovery"
        assert app.raw_wire_writer._closed and app.trades_writer._closed   # not reachable by the recovery
        assert app.failure_policy.shutdown_failure == "rest_producer_not_stopped"
        assert app.exit_code == 1                                          # never reported as success

        box.gate.set()                                                     # the producer runs its REAL writes
        assert await asyncio.wait_for(producer, 10) is True
        assert ("raw_rest_writer", False) in writes
        assert [w for w in writes if w[0] == "ob_writer"] == [("ob_writer", False)] * 2
        assert all(closed is False for n, closed in writes if n in ("raw_rest_writer", "ob_writer", "quality_writer"))

        await app.shutdown()                                               # eventual cleanup
        assert all(getattr(app, n)._closed for n in WRITERS)
        assert _rows(app.raw_rest_writer) >= 1 and _rows(app.ob_writer) == 2
        assert app.failure_policy.shutdown_failure == "rest_producer_not_stopped"   # not erased
    finally:
        box.gate.set()
        await asyncio.wait({t for t in (producer, shutdown_task) if t is not None}, timeout=10)
        app.client = _IdleClient()
        try:
            await app.shutdown()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_a_recovery_that_ends_on_cancel_leaves_a_clean_shutdown(tmp_path):
    app = spot.BinanceSpotCollectorApp(data_dir=str(tmp_path))
    app.client = _IdleClient()

    async def well_behaved(reason):
        await asyncio.Event().wait()
    app._recovery_task = asyncio.ensure_future(well_behaved("x"))
    await asyncio.sleep(0)
    await app.shutdown()
    assert app.failure_policy.shutdown_failure is None and app.exit_code == 0
    assert all(getattr(app, n)._closed for n in WRITERS)


@pytest.mark.asyncio
async def test_cancel_and_wait_reports_truthfully_and_never_swallows_the_callers_cancel():
    release = asyncio.Event()

    async def stubborn():
        while not release.is_set():
            try:
                await asyncio.sleep(0.005)
            except asyncio.CancelledError:
                pass
    t = asyncio.ensure_future(stubborn())
    await asyncio.sleep(0)
    assert await cancel_and_wait(t, 0.05, "x") is False                  # alive: not reported as ended
    waiter = asyncio.ensure_future(cancel_and_wait(t, 5.0, "x"))
    await asyncio.sleep(0.02)
    waiter.cancel()                                                      # the CALLER is cancelled
    await asyncio.gather(waiter, return_exceptions=True)
    assert waiter.cancelled(), "the caller's cancellation was swallowed"
    release.set()
    assert await cancel_and_wait(t, 1.0, "x") is True
    assert await cancel_and_wait(None, 1.0, "x") is True

    async def quick():
        await asyncio.Event().wait()
    q = asyncio.ensure_future(quick())
    await asyncio.sleep(0)
    assert await cancel_and_wait(q, 1.0, "x") is True and q.cancelled()


def test_the_cancel_bound_matches_the_usdm_runners():
    assert REST_PRODUCER_CANCEL_BOUND_S == run_collector.WS_FALLBACK_SLACK_S


def test_declared_spot_reach_covers_what_the_recovery_can_actually_write():
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "run_binance_spot_collector.py")
    cls = [n for n in ast.parse(open(path).read()).body
           if isinstance(n, ast.ClassDef) and n.name == "BinanceSpotCollectorApp"][0]
    meths = {n.name: n for n in cls.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    storage = {"ob_writer", "trades_writer", "raw_wire_writer", "raw_rest_writer", "quality_writer",
               "raw_capture", "segment_dedup"}

    def names(m):
        out = set()
        for n in ast.walk(meths[m]):
            if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id == "self":
                out.add(n.attr)
            if (isinstance(n, ast.Call) and getattr(n.func, "id", None) in ("getattr", "hasattr")
                    and len(n.args) >= 2 and isinstance(n.args[1], ast.Constant)):
                out.add(n.args[1].value)
        return out
    seen, stack, found = {"_recover_book"}, ["_recover_book"], set()
    while stack:
        for x in names(stack.pop()):
            if x in storage:
                found.add("raw_rest_writer" if x == "raw_capture" else x)
            if x in meths and x not in seen:
                seen.add(x)
                stack.append(x)
    assert found, "traced nothing (the tracer is broken)"
    assert found <= spot.SPOT_RECOVERY_REACH, sorted(found - spot.SPOT_RECOVERY_REACH)
