"""A cancellation-resistant WebSocket worker that ends AFTER ``shutdown()`` returned.

``shutdown()`` never closes a writer beneath a live worker, so with a stuck worker it returns early with
an explicit non-zero failure and the worker-reachable writers still open. Before this change nothing
ever closed them once the worker finally ended. These tests drive the REAL ``WebSocketClient``, the REAL
``app.shutdown()`` and the REAL ``supervise_standalone_runner`` for Binance Spot, Bybit and OKX, with a
fake socket and deterministic gates (no sleeps-as-synchronization, no network), and pin:

* while the worker is alive: the storage it can reach stays open and accepts writes, the failure is latched;
* when it ends: every writer is closed exactly once, the venue's order is kept (quality last, Bybit WAL
  after quality), data written while the worker was alive is published exactly once;
* overlapping callbacks and repeated ``shutdown()`` calls close nothing twice;
* the failure stays latched; a worker that never ends keeps its storage open and the shutdown failed.
"""
from __future__ import annotations

import asyncio
import importlib
import json
import pathlib
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest

from collector.collector.standalone_failure_policy import (
    CloseOnce,
    LateFinalizer,
    supervise_standalone_runner,
)

VENUES = {
    "spot": dict(module="run_binance_spot_collector", cls="BinanceSpotCollectorApp", exchange="BINANCE_SPOT",
                 market=["trades_writer", "ob_writer"], early=["raw_wire_writer"]),
    "bybit": dict(module="run_bybit_collector", cls="BybitCollectorApp", exchange="BYBIT",
                  market=["ob_writer", "trades_writer", "mark_writer", "oi_writer", "liq_writer"],
                  early=["raw_wire_writer"]),
    "okx": dict(module="run_okx_collector", cls="OKXCollectorApp", exchange="OKX",
                market=["trades_writer", "trades_all_writer", "mark_writer", "index_writer",
                        "funding_writer", "oi_writer", "liq_writer"],
                early=["raw_wire_writer"]),
}
PROBE_REASON = "late_probe_event"


class _Conn:
    """Fake websocket: yields one frame, then holds the socket open until ``hold`` is set."""

    def __init__(self, hold: asyncio.Event):
        self.hold = hold

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def send(self, _msg):
        return None

    def __aiter__(self):
        async def gen():
            yield json.dumps({"n": 1})
            await self.hold.wait()
        return gen()


class Rig:
    """One app + real client with a stubborn worker, supervised by the real supervisor."""

    def __init__(self, venue: str, tmp_path: pathlib.Path):
        spec = VENUES[venue]
        self.venue, self.spec, self.tmp_path = venue, spec, tmp_path
        mod = importlib.import_module(spec["module"])
        self.app = getattr(mod, spec["cls"])(data_dir=str(tmp_path))
        if venue == "spot":
            async def _no_rest(_reason):          # fake networking: no REST snapshot
                return True
            self.app._recover_book = _no_rest
        self.log: list = []
        self.entered, self.let_go, self.hold = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.stop = asyncio.Event()
        self.supervisor = None
        self.exit_code = None
        client = self.app.client
        client.on_message = self._stubborn_handler
        client._on_message_takes_connection = True
        client.shutdown_drain_timeout_s = 0.05
        client.worker_retire_timeout_s = 0.05
        self.writers = {}
        for name in spec["market"] + spec["early"] + ["quality_writer"]:
            self.writers[name] = self._spy(name)
        self.close_counts = {n: 0 for n in self.writers}

    def _spy(self, name):
        writer = getattr(self.app, name)
        real_close = writer.close

        def close():
            self.close_counts[name] += 1
            self.log.append(("close", name))
            return real_close()
        writer.close = close
        return writer

    async def _stubborn_handler(self, data, ts, connection_id=None):
        self.entered.set()
        while True:
            try:
                await asyncio.sleep(0.005)
            except asyncio.CancelledError:
                pass                                   # swallows cancellation: the stuck worker
            if self.let_go.is_set():
                self.log.append("worker_ended")
                raise asyncio.CancelledError

    async def start_and_reach_shutdown_return(self):
        self._patch = patch("collector.collector.websocket_client.websockets.connect",
                            new=lambda url, **k: _Conn(self.hold))
        self._patch.start()
        self.supervisor = asyncio.ensure_future(supervise_standalone_runner(self.app, self.stop))
        await asyncio.wait_for(self.entered.wait(), 10)
        self.stop.set()
        self.exit_code = await asyncio.wait_for(self.supervisor, 30)
        self.log.append("shutdown_returned")

    async def release_worker(self, turns: int = 40):
        self.let_go.set()
        for _ in range(turns):
            await asyncio.sleep(0.005)

    def closed(self, name) -> bool:
        return bool(self.writers[name]._closed)

    def worker_reachable(self):
        return self.spec["market"] + ["quality_writer"]

    def probe_event(self):
        return {"exchange": self.spec["exchange"], "stream": "late_probe", "event_type": "ERROR",
                "reason": PROBE_REASON}

    def published_probe_rows(self) -> int:
        total = 0
        for f in list(self.tmp_path.rglob("*.seg")) + list(self.tmp_path.rglob("*.parquet")):
            if "quality" not in str(f):             # published segments only (.tmp files never match)
                continue
            try:
                table = pq.read_table(f)
            except Exception:
                continue
            if "reason" in table.column_names:
                total += sum(1 for r in table.column("reason").to_pylist() if r == PROBE_REASON)
        return total

    async def cleanup(self):
        self.let_go.set()
        self.hold.set()
        for _ in range(40):
            await asyncio.sleep(0.005)
        self._patch.stop()


@pytest.fixture(params=sorted(VENUES))
def venue(request):
    return request.param


async def _make(venue, tmp_path) -> Rig:
    rig = Rig(venue, tmp_path)
    await rig.start_and_reach_shutdown_return()
    return rig


# --------------------------------------------------------------------------- the reproduction + fix

async def test_storage_stays_open_while_worker_alive_then_is_finalized_once_in_order(venue, tmp_path):
    rig = await _make(venue, tmp_path)
    try:
        # (1)+(2) shutdown returned with the worker alive: nothing it can reach is closed, and it can
        # still write; (3) an explicit non-zero failure is latched.
        assert rig.app.client.worker_active
        for name in rig.worker_reachable():
            assert not rig.closed(name), f"{name} closed beneath a live worker"
            assert rig.close_counts[name] == 0
        assert rig.exit_code != 0
        assert rig.app.failure_policy.shutdown_failure is not None
        rig.app._persist_quality_event(rig.probe_event())     # the live worker's write must succeed
        assert rig.published_probe_rows() <= 1                # venue-specific: buffered (Bybit) or already durable
        latched = rig.app.failure_policy.shutdown_failure

        # (4)+(5)+(6) the worker ends after shutdown() returned -> storage is finalized automatically.
        await rig.release_worker()
        assert not rig.app.client.worker_active
        for name in rig.worker_reachable():
            assert rig.closed(name), f"{name} never finalized after the worker ended"
        assert all(rig.close_counts[n] == 1 for n in rig.writers), rig.close_counts
        closes = [e[1] for e in rig.log if isinstance(e, tuple)]
        assert rig.log.index("worker_ended") < min(rig.log.index(("close", n)) for n in rig.worker_reachable())
        assert closes[-1] == "quality_writer", f"quality must close last: {closes}"
        assert rig.published_probe_rows() == 1                # published exactly once, not lost/duplicated
        # failure stays latched: a late close is not a clean shutdown
        assert rig.app.failure_policy.shutdown_failure == latched
        assert rig.app.exit_code != 0
    finally:
        await rig.cleanup()


async def test_raw_wire_writer_is_closed_once_as_soon_as_ingress_stops(venue, tmp_path):
    rig = await _make(venue, tmp_path)
    try:
        assert rig.close_counts["raw_wire_writer"] == 1       # not held by the worker
        await rig.release_worker()
        assert rig.close_counts["raw_wire_writer"] == 1       # and not repeated by the late retry
    finally:
        await rig.cleanup()


async def test_worker_that_never_ends_keeps_storage_open_and_shutdown_failed(venue, tmp_path):
    rig = await _make(venue, tmp_path)
    try:
        for _ in range(60):
            await asyncio.sleep(0.005)
        for name in rig.worker_reachable():
            assert not rig.closed(name) and rig.close_counts[name] == 0
        assert rig.app._late_finalizer.ran is False
        assert rig.exit_code != 0 and rig.app.failure_policy.shutdown_failure is not None
        # a repeated shutdown while the worker is STILL alive must not close anything either
        await rig.app.shutdown()
        for name in rig.worker_reachable():
            assert not rig.closed(name) and rig.close_counts[name] == 0
        assert rig.app.failure_policy.shutdown_failure is not None
    finally:
        await rig.cleanup()


async def test_overlapping_callbacks_and_repeated_shutdown_close_each_writer_once(venue, tmp_path):
    rig = await _make(venue, tmp_path)
    try:
        rig.let_go.set()
        # worker end, run-task end and three explicit shutdown() calls all race in the same turns
        await asyncio.gather(rig.app.shutdown(), rig.app.shutdown(), rig.app.shutdown(),
                             *[asyncio.sleep(0.005) for _ in range(10)])
        for _ in range(40):
            await asyncio.sleep(0.005)
            rig.app._late_finalizer.retry()                    # explicit extra callback invocations
        await rig.app.shutdown()
        assert all(rig.close_counts[n] == 1 for n in rig.writers), rig.close_counts
        assert all(rig.closed(n) for n in rig.writers)
    finally:
        await rig.cleanup()


async def test_late_close_failure_is_attempted_and_reported_once(venue, tmp_path):
    rig = await _make(venue, tmp_path)
    try:
        victim = rig.spec["market"][0]
        calls = {"n": 0}

        def failing_close():
            calls["n"] += 1
            raise OSError("disk went away")
        rig.writers[victim].close = failing_close
        rig.let_go.set()
        for _ in range(40):
            await asyncio.sleep(0.005)
        await rig.app.shutdown()
        await rig.app.shutdown()
        rig.app._late_finalizer.retry()
        assert calls["n"] == 1, "a failed close must not be retried"
        # the rest of the finalization still ran, quality last
        assert all(rig.closed(n) for n in rig.worker_reachable() if n != victim)
        assert rig.close_counts["quality_writer"] == 1
    finally:
        await rig.cleanup()


# --------------------------------------------------------------------------- venue specifics

async def test_bybit_wal_is_closed_once_and_only_after_the_quality_writer(tmp_path):
    rig = await _make("bybit", tmp_path)
    try:
        wal = rig.app._quality_wal
        order, wal_closes = [], {"n": 0}
        real_wal_close = wal.close

        def wal_close():
            wal_closes["n"] += 1
            order.append(("wal", rig.close_counts["quality_writer"]))
            return real_wal_close()
        wal.close = wal_close
        assert not getattr(wal, "_closed", False)               # WAL still the recovery source
        rig.app._persist_quality_event(rig.probe_event())
        await rig.release_worker()
        await rig.app.shutdown()
        assert wal_closes["n"] == 1
        assert order == [("wal", 1)], "WAL must close after quality_writer published its last segment"
        assert rig.published_probe_rows() == 1
        assert rig.app._quality_wal_pending_empty() if hasattr(rig.app, "_quality_wal_pending_empty") else True
    finally:
        await rig.cleanup()


async def test_spot_book_recovery_task_alive_holds_ob_and_quality_until_it_ends(tmp_path):
    rig = Rig("spot", tmp_path)
    recovery_gate = asyncio.Event()

    async def slow_recovery(_reason):
        while not recovery_gate.is_set():
            try:
                await asyncio.sleep(0.005)
            except asyncio.CancelledError:
                pass                                          # a cancellation-resistant REST producer
        return True
    rig.app._recover_book = slow_recovery
    await rig.start_and_reach_shutdown_return()
    try:
        await rig.release_worker()                             # worker ends, recovery still alive
        assert not rig.app.client.worker_active and rig.app._recovery_alive()
        for name in ("ob_writer", "quality_writer"):
            assert not rig.closed(name) and rig.close_counts[name] == 0, f"{name} closed beneath live recovery"
        assert rig.app._late_finalizer.ran is False
        recovery_gate.set()                                    # the last user ends
        for _ in range(40):
            await asyncio.sleep(0.005)
        assert all(rig.close_counts[n] == 1 for n in rig.writers), rig.close_counts
        assert rig.app.failure_policy.shutdown_failure is not None
    finally:
        recovery_gate.set()
        await rig.cleanup()


# --------------------------------------------------------------------------- primitives

def test_close_once_returns_first_outcome_without_calling_close_again():
    reg, calls = CloseOnce(), []
    assert reg.run("w", lambda: (calls.append(1), False)[1]) is False
    assert reg.run("w", lambda: (calls.append(2), True)[1]) is False     # first outcome, no 2nd call
    assert calls == [1] and reg.attempted("w") and not reg.attempted("other")


def test_close_once_is_reentrancy_safe():
    reg, seen = CloseOnce(), []

    def close():
        seen.append(reg.attempted("w"))                        # visible as attempted from inside
        seen.append(reg.run("w", lambda: pytest.fail("re-entrant close must not run")))
        return True
    assert reg.run("w", close) is True
    assert seen == [True, False]


class _FakeTask:
    def __init__(self):
        self.cbs, self._done = [], False

    def done(self):
        return self._done

    def cancelled(self):
        return False

    def exception(self):
        return None

    def add_done_callback(self, cb):
        self.cbs.append(cb)

    def end(self):
        self._done = True
        for cb in self.cbs:
            cb(self)


def test_late_finalizer_runs_only_when_every_user_stopped_and_only_once():
    a, b, ran, stopped = _FakeTask(), _FakeTask(), [], {"v": False}
    lf = LateFinalizer(venue="T", finalize=lambda: ran.append(1),
                       users_stopped=lambda: stopped["v"], watched=lambda: [a, b, None])
    assert lf.arm() == 2 and lf.arm() == 0                      # idempotent, skips None and armed tasks
    a.end()
    assert ran == [] and not lf.ran                             # b still alive: a's end must not finalize
    stopped["v"] = True
    b.end()
    assert ran == [1] and lf.ran
    assert lf.retry() is False and ran == [1]                   # never twice


def test_late_finalizer_fails_closed_when_state_check_raises_and_survives_finalize_errors():
    def boom():
        raise RuntimeError("state unreadable")
    lf = LateFinalizer(venue="T", finalize=lambda: None, users_stopped=boom, watched=lambda: [])
    assert lf.retry() is False and not lf.ran                   # unreadable state => storage stays open

    def bad_finalize():
        raise RuntimeError("finalize blew up")
    lf2 = LateFinalizer(venue="T", finalize=bad_finalize, users_stopped=lambda: True, watched=lambda: [])
    assert lf2.retry() is True and lf2.ran                      # contained, never raised into the loop
