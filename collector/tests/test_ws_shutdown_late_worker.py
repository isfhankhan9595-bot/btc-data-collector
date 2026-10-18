"""A websocket ingest worker that ends AFTER ``shutdown()`` has returned must get storage finalized.

``CollectorApp.shutdown()`` keeps the storage a live worker can reach open while the client is not
idle (``_ws_clients_idle()``) and latches ``ws_client_worker_not_retired``. REST producers and the
quality consumer already have a done-callback that retries ``shutdown()`` when they end late; the
websocket lifecycle (the client's ``start()`` task and its ingest worker) did not, so once a worker
that ignored its cancel finally ended nothing finalized the held writers, the quality writer or the WAL.

Everything here runs the real ``CollectorApp.start()`` / ``_async_shutdown()`` / ``shutdown()`` with a REAL
``WebSocketClient`` (fake socket only). "Closed" means ``close()`` was CALLED and RETURNED (ordered event
log) and the app's own verdict (``_writer_close_results``) says it succeeded; a writer never attempted has
neither. Nothing here claims a cancellation-resistant coroutine can be stopped: a worker that never ends is
only observed to keep its storage open and the failure latched.
"""
import asyncio
import signal
from contextlib import ExitStack

import pytest

import run_collector
from collector.collector.websocket_client import WebSocketClient, WorkerRetirementError
from collector.tests.test_ws_shutdown_late_producer import (
    _ALL_WRITERS, _OI_HELD, _StubbornPoller, _assert_fully_finalized_after_producer_end,
    _assert_only_these_are_open, _closed, _closes, _idle_turns, _recovered, _spy_on_closes)
from collector.tests.test_ws_shutdown_lifecycle import _connect_patch, _frames
from collector.tests.test_ws_shutdown_startup_recovery import _app_with_stubbed_neighbours, _drain

DEADLINE_S = 10
#: what a live ingest worker can still reach: everything except the two raw writers whose producers
#: (receive loop, REST tasks) are proven stopped. The WAL is held through quality_writer.
_WS_HELD = set(_ALL_WRITERS) - {"raw_wire_writer", "raw_rest_writer"}
_PROBES = ("probe_a", "probe_b", "probe_c")


class _Scenario:
    """A real client whose ingest worker swallows its cancel until ``release()``."""

    def __init__(self, app, log, marker=("producer_ended",)):
        self.app, self.log, self.marker = app, log, marker
        self.entered, self.let_go, self.hold = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.swallowed = 0
        self.client = WebSocketClient("ws://x", self._handler, ingest_queue_maxsize=10,
                                      on_quality_event=app._websocket_quality_event)
        self.client.shutdown_drain_timeout_s = 0.05
        self.client.worker_retire_timeout_s = 0.05
        app.ws_clients = [self.client]
        self.worker = None

    async def _handler(self, data, ts, connection_id=None):
        self.entered.set()
        while True:
            try:
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                self.swallowed += 1                      # pathological: ignores cancellation
            if self.let_go.is_set():
                self.log.append(self.marker)
                raise asyncio.CancelledError             # ... until it finally honours it

    def release(self):
        self.let_go.set()


async def _run_until_worker_survived_shutdown(app, monkeypatch, scenario, *, start_cb=None):
    monkeypatch.setattr(app, "_recover_binance_book", _recovered)
    monkeypatch.setattr(run_collector, "WS_FALLBACK_SLACK_S", 0.05)
    start_task = asyncio.create_task(app.start())
    await asyncio.wait_for(scenario.entered.wait(), DEADLINE_S)
    deadline = asyncio.get_running_loop().time() + DEADLINE_S
    while app._oi_task is None and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)                        # steady state: start() created every task
    assert app._oi_task is not None, "start() never reached its steady state"
    scenario.worker = scenario.client._worker_task
    for reason in _PROBES:                               # events that must neither be dropped nor duplicated
        app._websocket_quality_event("ERROR", reason)
    await _idle_turns()
    shutdown_task = asyncio.create_task(app._async_shutdown(signal.SIGTERM))
    await asyncio.wait_for(shutdown_task, DEADLINE_S)
    done, _ = await asyncio.wait({start_task}, timeout=DEADLINE_S)
    assert done, "start() never returned"
    if not start_task.cancelled() and start_task.exception() is not None:
        assert isinstance(start_task.exception(), WorkerRetirementError)   # the client reported it, loudly
    assert scenario.client.worker_retirement_failed and scenario.client.worker_active
    return start_task, shutdown_task


def _published_reasons(app):
    import pyarrow.parquet as pq
    rows = []
    for f in sorted(app.quality_writer.stream_dir.glob("*.seg")):
        rows.extend(pq.read_table(f).to_pylist())
    return [r["reason"] for r in rows]


def _wal_pending_reasons(app):
    from collector.collector.quality_wal import QualityEventWAL
    return [e["reason"] for e in QualityEventWAL.recover(app._quality_wal.wal_dir)]


def _assert_events_neither_dropped_nor_duplicated(app, *, published_expected):
    """Every probe is in the published segments XOR still pending in the WAL -- exactly once overall."""
    published, pending = _published_reasons(app), _wal_pending_reasons(app)
    for reason in (*_PROBES, "ingest_worker_retirement_failed"):
        assert published.count(reason) + pending.count(reason) == 1, (
            f"{reason}: published={published.count(reason)} pending={pending.count(reason)}")
        if published_expected:
            assert published.count(reason) == 1 and pending.count(reason) == 0
    if published_expected:
        assert pending == [], f"events still pending in the WAL after finalization: {pending}"


@pytest.mark.asyncio
async def test_a_worker_that_ends_after_shutdown_returned_gets_every_remaining_writer_finalized(
        tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log = []
    _spy_on_closes(app, monkeypatch, log)
    scenario = _Scenario(app, log)
    start_task = shutdown_task = None
    try:
        with ExitStack() as stack:
            stack.enter_context(_connect_patch(_frames(1), scenario.hold))
            start_task, shutdown_task = await _run_until_worker_survived_shutdown(app, monkeypatch, scenario)
            worker = scenario.worker
            # --- while the worker is alive: failure latched, reachable storage preserved ----------------
            assert not worker.done() and scenario.swallowed >= 1
            assert app.shutdown_failure == "ws_client_worker_not_retired" and app.exit_code != 0
            assert not getattr(app, "_shutdown_done", False)
            _assert_only_these_are_open(app, log, _WS_HELD)
            _assert_events_neither_dropped_nor_duplicated(app, published_expected=False)
            # --- the worker ends after shutdown() returned: finalization must follow automatically -------
            scenario.release()
            assert (await asyncio.wait({worker}, timeout=DEADLINE_S))[0]
            await _idle_turns()
            _assert_fully_finalized_after_producer_end(app, log, _WS_HELD | {"quality_writer"})
            assert app.shutdown_failure == "ws_client_worker_not_retired" and app.exit_code != 0
            _assert_events_neither_dropped_nor_duplicated(app, published_expected=True)
    finally:
        scenario.release()
        await _drain(app, start_task, shutdown_task)


def _fully_closed_exactly_once(app, log):
    assert getattr(app, "_shutdown_done", False)
    for name in (*_ALL_WRITERS, "quality_wal"):
        assert len(_closes(log, name)) == 1, f"{name} closed {len(_closes(log, name))} times"
    assert all(app._writer_close_results.get(n) is True for n in _ALL_WRITERS)


@pytest.mark.asyncio
async def test_a_worker_that_never_ends_keeps_its_storage_open_and_never_reports_a_clean_shutdown(
        tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log = []
    _spy_on_closes(app, monkeypatch, log)
    scenario = _Scenario(app, log)
    start_task = shutdown_task = None
    try:
        with ExitStack() as stack:
            stack.enter_context(_connect_patch(_frames(1), scenario.hold))
            start_task, shutdown_task = await _run_until_worker_survived_shutdown(app, monkeypatch, scenario)
            worker = scenario.worker
            assert (await asyncio.wait({worker}, timeout=0.3))[1] == {worker}     # observation window >> bound
            await _idle_turns()
            for _ in range(3):                                                    # explicit retries are no-ops
                app.shutdown()
                _assert_only_these_are_open(app, log, _WS_HELD)
            assert not getattr(app, "_shutdown_done", False) and not getattr(app, "_quality_wal_closed", False)
            assert app.shutdown_failure == "ws_client_worker_not_retired"
            assert app.exit_code == run_collector.EXIT_RUN_TASK_CRASHED != 0
            assert not worker.done() and not app._ws_clients_idle()               # not falsely considered stopped
            _assert_events_neither_dropped_nor_duplicated(app, published_expected=False)
    finally:
        scenario.release()                                                        # no leaked task past the test
        await _drain(app, start_task, shutdown_task)


@pytest.mark.asyncio
async def test_repeated_and_concurrent_late_completion_callbacks_close_every_writer_at_most_once(
        tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log = []
    _spy_on_closes(app, monkeypatch, log)
    scenario = _Scenario(app, log)
    start_task = shutdown_task = None
    try:
        with ExitStack() as stack:
            stack.enter_context(_connect_patch(_frames(1), scenario.hold))
            start_task, shutdown_task = await _run_until_worker_survived_shutdown(app, monkeypatch, scenario)
            worker = scenario.worker
            loop = asyncio.get_running_loop()
            for _ in range(2):                                    # arming again / retrying early: no-ops
                app._arm_late_shutdown_retry()
                app.shutdown()
                _assert_only_these_are_open(app, log, _WS_HELD)
            scenario.release()
            assert (await asyncio.wait({worker}, timeout=DEADLINE_S))[0]
            await _idle_turns()
            # phase A -- nothing but the registered callback has run: it alone must have finalized everything
            _fully_closed_exactly_once(app, log)
            # phase B -- duplicate / overlapping late completions and explicit shutdowns change nothing
            for _ in range(3):
                loop.call_soon(app._on_rest_producer_ended_late, worker)
                loop.call_soon(app.shutdown)
                loop.call_soon(app._arm_late_shutdown_retry)
            await _idle_turns()
            for _ in range(3):
                app._on_rest_producer_ended_late(worker)
                app.shutdown()
            _fully_closed_exactly_once(app, log)
            _assert_fully_finalized_after_producer_end(app, log, _WS_HELD | {"quality_writer"})
            assert app.shutdown_failure == "ws_client_worker_not_retired" and app.exit_code != 0
    finally:
        scenario.release()
        await _drain(app, start_task, shutdown_task)


@pytest.mark.parametrize("worker_ends_first", [True, False])
@pytest.mark.asyncio
async def test_the_websocket_callback_cannot_close_shared_storage_beneath_a_stubborn_rest_producer(
        tmp_path, monkeypatch, worker_ends_first):
    """A stuck worker AND a stuck OI poller: whichever ends first, nothing the other can reach is closed,
    and only the end of the LAST one finalizes the rest (quality_writer, then the WAL)."""
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log = []
    _spy_on_closes(app, monkeypatch, log)
    scenario = _Scenario(app, log, marker=("worker_ended",))
    poller = _StubbornPoller(log)
    monkeypatch.setattr(app, "_poll_openinterest", poller)
    start_task = shutdown_task = None
    try:
        with ExitStack() as stack:
            stack.enter_context(_connect_patch(_frames(1), scenario.hold))
            start_task, shutdown_task = await _run_until_worker_survived_shutdown(app, monkeypatch, scenario)
            worker, producer = scenario.worker, app._oi_task
            assert not producer.done() and not worker.done()
            assert app.shutdown_failure == "ws_client_worker_not_retired" and app.exit_code != 0
            _assert_only_these_are_open(app, log, _WS_HELD | _OI_HELD)

            first, second = ((scenario.release, worker), (poller.release.set, producer)) if worker_ends_first \
                else ((poller.release.set, producer), (scenario.release, worker))
            first[0]()
            assert (await asyncio.wait({first[1]}, timeout=DEADLINE_S))[0]
            await _idle_turns()
            # ONE of the two is still alive: no writer, not quality_writer, not the WAL, may have been closed
            # beneath it. (Whatever neither can reach may be closed; the producer's reach is not.)
            still_alive_reach = (_WS_HELD if not worker_ends_first else _OI_HELD) | {"quality_writer"}
            for name in still_alive_reach:
                assert not _closed(app, name) and _closes(log, name) == [], f"{name} closed beneath a live user"
            assert not getattr(app, "_shutdown_done", False) and ("close_called", "quality_wal") not in log
            assert app.shutdown_failure is not None and app.exit_code != 0
            second[0]()
            assert (await asyncio.wait({second[1]}, timeout=DEADLINE_S))[0]
            await _idle_turns()
            _fully_closed_exactly_once(app, log)
            ended = max(log.index(("worker_ended",)), log.index(("producer_ended",)))
            for name in ("quality_writer", "quality_wal"):
                assert _closes(log, name)[0] > ended, f"{name} closed before the LAST user ended"
            assert _closes(log, "quality_writer")[0] < _closes(log, "quality_wal")[0]
            assert app.shutdown_failure is not None and app.exit_code != 0
    finally:
        scenario.release()
        poller.release.set()
        await _drain(app, start_task, shutdown_task)


@pytest.mark.asyncio
async def test_the_quality_writer_publishes_its_final_segment_before_the_wal_closes_after_a_late_worker(
        tmp_path, monkeypatch):
    app = _app_with_stubbed_neighbours(tmp_path, monkeypatch)
    log = []
    _spy_on_closes(app, monkeypatch, log)
    scenario = _Scenario(app, log)
    start_task = shutdown_task = None
    try:
        with ExitStack() as stack:
            stack.enter_context(_connect_patch(_frames(1), scenario.hold))
            start_task, shutdown_task = await _run_until_worker_survived_shutdown(app, monkeypatch, scenario)
            assert _published_reasons(app) == [], "nothing may be published while the worker can still write"
            pending_before = sorted(_wal_pending_reasons(app))
            assert {*_PROBES} <= set(pending_before)                  # WAL-retained: replayable if the process dies
            scenario.release()
            assert (await asyncio.wait({scenario.worker}, timeout=DEADLINE_S))[0]
            await _idle_turns()
            q_returned = log.index(("close_returned", "quality_writer"))
            assert q_returned < log.index(("close_called", "quality_wal")), "WAL closed before the final publish"
            _assert_events_neither_dropped_nor_duplicated(app, published_expected=True)
            assert app._quality_wal.checkpointed_seq >= 0 and not app._quality_checkpoint_is_blocked()
            assert getattr(app, "_quality_wal_closed", False)
            assert app.shutdown_failure == "ws_client_worker_not_retired" and app.exit_code != 0
    finally:
        scenario.release()
        await _drain(app, start_task, shutdown_task)
