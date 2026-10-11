"""F5 final fix, part 2 (Finding B): Bybit quality-channel containment.

Defect
------
``BybitCollectorApp._apply_orderbook`` and ``_record_adapter_unhandled`` called
``_persist_quality_event`` directly from the market-data path. ``_persist_quality_event``
re-raises when the quality writer is FAILED, so a quality-channel ENOSPC escaped
``normalize()`` / ``_apply_orderbook`` into the websocket worker boundary: the
order-book write that followed the book-state event never ran (the canonical row
was lost) and the client counted a worker fatal, although the quality channel is
supposed to degrade independently of market data.

Contract under test
-------------------
* a typed ``FatalStorageError`` is contained ONLY when Bybit's own failure policy
  classifies it as a failure of Bybit's quality channel;
* it is latched ONCE (one structured log, one operator alert) and the failed quality
  writer is never called again -- later events are accounted, not written;
* a degraded quality channel never interrupts otherwise-valid order-book processing;
* a raw-evidence fatal still ends in the controlled non-zero exit, also while quality
  is degraded;
* a genuine order-book writer fatal still isolates the order-book route;
* an unrelated typed fatal is never swallowed, an unknown typed fatal stays terminal
  (default-deny) and an ordinary exception is never turned into a fatal or hidden;
* the degraded-channel counters distinguish events durably retained in the quality WAL
  from events that are lost, and agree with what is really on disk.

Everything drives the real ``BybitCollectorApp`` (real quality WAL, real
``_recover_quality_wal``), the real ``WebSocketClient._consume`` / worker boundary, the
real ``StandaloneFailurePolicy`` and real ``ParquetWriter`` publication; ENOSPC is
injected at the real ``os.replace`` seam, scoped to the armed writers' directories.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time

import pytest

from collector.collector import notifications
from collector.collector.failure_topology import (
    VERDICT_DEGRADE_QUALITY, VERDICT_ISOLATE, VERDICT_TERMINATE)
from collector.collector.quality_events import BookQuality
from collector.collector.standalone_failure_policy import EXIT_FATAL_STORAGE, supervise_standalone_runner
from collector.collector.storage_errors import FatalStorageError
from collector.run_bybit_collector import BybitCollectorApp
from collector.tests.test_bybit_quality_wal import (
    _Unhandled, _crash, _disk_ckpt, _fail_wal_fsync, _pending, _rows, _wal_records)
from collector.tests.test_f5_final_quality_writer_latch import (
    _Disk, _error_events, _run_frames, _spy_on_fatal, _spy_quality_writer)
from collector.tests.test_f5_standalone_runners import (
    SUPERVISED_BOUND_S, _assert_all_closed, _raw_frames, _terminal_run)

N_EVENTS = 60                 # far more than any sane per-event reporting budget
QUALITY_STREAM = "bybit_quality_events"


@pytest.fixture(autouse=True)
def alerts():
    sent: list[str] = []
    notifications.set_notifier(notifications.CallableNotifier(lambda message: sent.append(message) or True))
    yield sent
    notifications.set_notifier(None)


# --------------------------------------------------------------------------- helpers
def _app(tmp_path) -> BybitCollectorApp:
    return BybitCollectorApp(data_dir=str(tmp_path))


def _ob_frame(typ: str, update_id: int, seq: int, *, bid: str = "1.0", ts: int | None = None) -> str:
    ts = ts or int(time.time() * 1000)
    return json.dumps({"topic": "orderbook.50.BTCUSDT", "type": typ, "ts": ts, "data": {
        "s": "BTCUSDT", "b": [["50000", bid]], "a": [["50001", "1.0"]], "u": update_id, "seq": seq}})


def _snapshot_and_decreasing_delta() -> list[str]:
    """The hostile-review input: a snapshot, then a delta whose update id DECREASES
    (a reset signal: the book leaves VALID and a SEQUENCE_GAP event is emitted)."""
    now = int(time.time() * 1000)
    return [_ob_frame("snapshot", 10, 100, bid="1.0", ts=now),
            _ob_frame("delta", 3, 104, bid="9.0", ts=now + 10)]


def _trade_frame(trade_ids: list[str], ts: int | None = None) -> str:
    ts = ts or int(time.time() * 1000)
    return json.dumps({"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": ts, "data": [
        {"T": ts, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "100", "i": i, "BT": False} for i in trade_ids]})


def _quality_alerts(alerts) -> list[str]:
    return [m for m in alerts if "degrade_quality" in m and QUALITY_STREAM in m]


def _cleanup(app) -> None:
    from collector.tests.test_p0_4_runner_lifecycle import _publish_all
    _publish_all(app)
    dedup = getattr(app, "segment_dedup", None)
    if dedup is not None:
        dedup.close()
    wal = getattr(app, "_quality_wal", None)
    if wal is not None:
        try:
            wal.close()
        except Exception:  # noqa: BLE001
            pass


# ===================================================================== 1. hostile-review reproduction
def test_hostile_review_quality_writer_failure_does_not_interrupt_orderbook_processing(tmp_path, monkeypatch):
    app = _app(tmp_path)
    try:
        client, policy = app.client, app.failure_policy
        _Disk(monkeypatch).arm(app.quality_writer)        # inject the quality-writer failure FIRST

        asyncio.run(_run_frames(client, _snapshot_and_decreasing_delta()))

        # the expected canonical order-book row IS written (the snapshot, before the book left VALID)
        assert len(app.ob_writer.buffer) == 1, "the canonical order-book row was lost to a quality-channel failure"
        row = app.ob_writer.buffer[0]
        assert row["is_snapshot"] is True and row["update_id"] == 10
        assert row["bids_price"][0] == 50000.0 and row["bids_qty"][0] == 1.0
        # the book still went through both transitions (the decreasing delta was detected)
        assert app.book.state.state is not BookQuality.VALID
        # worker_fatals == 0: nothing reached the websocket worker boundary
        assert client.fatal_storage_errors == 0 and client.processing_errors == 0
        assert not client.discard_mode
        # ... while the quality channel IS degraded, exactly once, and nothing else is affected
        record = policy.quality_degraded
        assert record is not None and (record.verdict, record.stream) == (VERDICT_DEGRADE_QUALITY, QUALITY_STREAM)
        assert policy.terminal_failure is None and not policy.isolated_routes and app.exit_code == 0
    finally:
        _cleanup(app)


def _emit_ws(app, reason: str) -> None:
    """One quality event through the runner's REAL websocket-client wiring."""
    app.client.on_quality_event("DISCONNECT", reason, "c", "bybit")


def _degrade(app, disk) -> None:
    """Arm the quality writer and trip the latch with one real event."""
    disk.arm(app.quality_writer)
    _emit_ws(app, "trip")
    assert app.quality_degraded is not None


def _status_counts(app) -> tuple:
    s = app.quality_channel_status()
    return (s["quality_events_wal_only"], s["quality_events_wal_unconfirmed"], s["quality_events_lost"])


# ============================================================== 2. adapter-unhandled quality-event path
def test_adapter_unhandled_event_is_contained_latched_once_and_retained_in_the_wal(tmp_path, monkeypatch, alerts):
    app = _app(tmp_path)
    try:
        policy = app.failure_policy
        _Disk(monkeypatch).arm(app.quality_writer)
        writes = _spy_quality_writer(app)

        app._record_adapter_unhandled(_Unhandled("t_unhandled_1"))     # must NOT raise out of normalize()
        app._record_adapter_unhandled(_Unhandled("t_unhandled_2"))

        record = policy.quality_degraded
        assert record is not None and (record.verdict, record.origin) == (VERDICT_DEGRADE_QUALITY, "quality_writer")
        assert len(writes) == 1, "the FAILED quality writer was called again"
        assert len(_quality_alerts(alerts)) == 1
        assert _status_counts(app) == (2, 0, 0)
        assert {r["reason"] for r in _wal_records(tmp_path)} == {"t_unhandled_1", "t_unhandled_2"}
    finally:
        _cleanup(app)


def test_duplicate_trade_through_the_real_adapter_latches_the_quality_failure_once(tmp_path, monkeypatch, alerts):
    """The adapter calls its unhandled sink INSIDE normalize(), under ``except Exception: pass``.
    Before the fix a quality-channel failure on this path was therefore INVISIBLE: swallowed by
    the adapter, never latched, never alerted, and the dead writer was called again for every
    duplicate / unrouted frame. The valid trades of the same frame were never at risk here."""
    app = _app(tmp_path)
    try:
        client, policy = app.client, app.failure_policy
        _Disk(monkeypatch).arm(app.quality_writer)
        writes = _spy_quality_writer(app)
        now = int(time.time() * 1000)
        frames = [_trade_frame(["1", "1", "2"], ts=now),                                    # duplicate "1" inside the frame
                  json.dumps({"topic": "weird.BTCUSDT", "ts": now, "data": []}),            # unrouted -> DATA_DROP
                  _trade_frame(["3"], ts=now + 1)]

        asyncio.run(_run_frames(client, frames))

        assert [r["trade_id"] for r in app.trades_writer.buffer] == ["1", "2", "3"], \
            "valid trades were lost to a quality-channel failure"
        assert client.fatal_storage_errors == 0 and client.processing_errors == 0 and not client.discard_mode
        assert policy.quality_degraded is not None, "the quality failure was swallowed by the adapter, never latched"
        assert policy.terminal_failure is None and not policy.isolated_routes
        assert len(writes) == 1, f"the FAILED quality writer was called {len(writes)} times"
        assert len(_quality_alerts(alerts)) == 1
        reasons = [r["reason"] for r in _wal_records(tmp_path)]
        assert any("duplicate_trade" in r for r in reasons) and any("no_route" in r for r in reasons)
        # exactly two quality events existed (the in-frame duplicate and the unrouted frame); the
        # counter, the WAL and reality agree
        assert app.quality_channel_status()["quality_events_wal_only"] == len(_wal_records(tmp_path)) == 2
    finally:
        _cleanup(app)


@pytest.mark.parametrize("stream, expected", [("bybit_raw_wire", VERDICT_TERMINATE),
                                              ("not_a_bybit_stream", VERDICT_TERMINATE)])
def test_a_non_quality_typed_fatal_on_the_adapter_path_is_not_lost_to_the_adapters_catch_all(tmp_path, monkeypatch,
                                                                                            stream, expected):
    """``ExchangeAdapter.unhandled`` drops every exception its sink raises. A typed fatal that is NOT
    this runner's quality channel must still reach the failure policy (and so the controlled exit)."""
    app = _app(tmp_path)
    try:
        client, policy = app.client, app.failure_policy
        fatal = FatalStorageError("injected: not the quality channel", stream=stream,
                                  component="parquet_writer", stage="rename", durability="unpublished")

        def boom(row, **kw):
            raise fatal
        monkeypatch.setattr(app.quality_writer, "write", boom)

        asyncio.run(_run_frames(client, [_trade_frame(["1", "1"])]))      # the duplicate drives the unhandled sink

        assert policy.quality_degraded is None, "it must NOT be relabelled a quality-channel failure"
        record = policy.terminal_failure
        assert record is not None and record.verdict == expected and record.origin == "adapter_unhandled"
        assert app.exit_code == EXIT_FATAL_STORAGE and client.discard_mode
        assert _status_counts(app) == (0, 0, 0)
    finally:
        _cleanup(app)


# ======================================= 3. raw-evidence fatal still terminates while quality is degraded
def test_raw_evidence_fatal_still_exits_non_zero_while_quality_is_degraded(tmp_path, monkeypatch, alerts):
    app = _app(tmp_path)
    try:
        policy = app.failure_policy
        disk = _Disk(monkeypatch)
        _degrade(app, disk)
        degraded = policy.quality_degraded
        disk.arm(app.raw_wire_writer)
        writes = _spy_quality_writer(app)

        code = _terminal_run(app, _raw_frames("bybit"))

        assert code == EXIT_FATAL_STORAGE != 0
        record = policy.terminal_failure
        assert record is not None and (record.verdict, record.origin) == (VERDICT_TERMINATE, "raw_frame")
        assert record.stream == app.raw_wire_writer.stream_name
        assert policy.quality_degraded is degraded, "the quality latch must be neither re-latched nor lost"
        assert len(writes) == 0, "the failed quality writer was called while terminating"
        _assert_all_closed(app)
        assert len(_quality_alerts(alerts)) == 1 and any(app.raw_wire_writer.stream_name in m for m in alerts)
        # the terminal-failure report is not lost: it is retained in the quality WAL
        assert any("fatal_storage:terminate" in r.get("reason", "") for r in _wal_records(tmp_path))
        assert app.quality_channel_status()["quality_events_lost"] == 0
    finally:
        _cleanup(app)


# ============================================ 4. a real order-book writer fatal still isolates the route
def test_orderbook_writer_fatal_still_isolates_the_orderbook_route(tmp_path, monkeypatch):
    app = _app(tmp_path)
    try:
        client, policy = app.client, app.failure_policy
        _Disk(monkeypatch).arm(app.ob_writer)
        now = int(time.time() * 1000)
        frames = [_ob_frame("snapshot", 10, 100, ts=now), _ob_frame("delta", 11, 101, bid="2.0", ts=now + 1),
                  _ob_frame("delta", 12, 102, bid="3.0", ts=now + 2), _trade_frame(["7"], ts=now + 3)]

        asyncio.run(_run_frames(client, frames))

        assert set(policy.isolated_routes) == {"orderbook"}
        record = policy.isolated_routes["orderbook"]
        assert (record.verdict, record.route, record.origin) == (VERDICT_ISOLATE, "orderbook", "worker")
        assert client.fatal_storage_errors == 1 and client.processing_errors == 0
        assert policy.route_short_circuits.get("orderbook", 0) >= 1
        assert policy.quality_degraded is None and policy.terminal_failure is None and app.exit_code == 0
        assert not client.discard_mode
        assert [r["trade_id"] for r in app.trades_writer.buffer] == ["7"], "healthy routes must keep running"
    finally:
        _cleanup(app)


def test_orderbook_writer_fatal_is_not_swallowed_by_the_quality_containment_while_quality_is_degraded(tmp_path, monkeypatch):
    app = _app(tmp_path)
    try:
        client, policy = app.client, app.failure_policy
        disk = _Disk(monkeypatch)
        _degrade(app, disk)
        degraded = policy.quality_degraded
        disk.arm(app.ob_writer)

        asyncio.run(_run_frames(client, _snapshot_and_decreasing_delta()))

        assert set(policy.isolated_routes) == {"orderbook"}, "a real order-book writer fatal must isolate the route"
        assert policy.quality_degraded is degraded and policy.terminal_failure is None and app.exit_code == 0
        assert {r.verdict for r in policy.failed_components.values()} == {VERDICT_ISOLATE, VERDICT_DEGRADE_QUALITY}
        assert client.fatal_storage_errors == 1
    finally:
        _cleanup(app)


# ===================== 5. unrelated typed fatal / unknown typed fatal / ordinary exception: never hidden or relabelled
@pytest.mark.parametrize("stream, expected", [("bybit_trades", VERDICT_ISOLATE),
                                              ("bybit_raw_wire", VERDICT_TERMINATE),
                                              ("not_a_bybit_stream", VERDICT_TERMINATE)])
def test_a_typed_fatal_of_another_stream_raised_on_the_quality_path_is_never_swallowed(tmp_path, monkeypatch, stream, expected):
    app = _app(tmp_path)
    try:
        client, policy = app.client, app.failure_policy
        fatal = FatalStorageError("injected: not the quality channel", stream=stream,
                                  component="parquet_writer", stage="rename", durability="unpublished")

        def boom(row, **kw):
            raise fatal
        monkeypatch.setattr(app.quality_writer, "write", boom)

        asyncio.run(_run_frames(client, _snapshot_and_decreasing_delta()))

        assert client.fatal_storage_errors >= 1, "an unrelated typed fatal must reach the worker boundary"
        assert policy.quality_degraded is None, "it must NOT be latched as a quality-channel failure"
        verdicts = {r.verdict for r in policy.failed_components.values()}
        assert verdicts == {expected}, f"classified {verdicts}, expected only {expected}"
        if expected == VERDICT_TERMINATE:
            assert policy.terminal_failure is not None and app.exit_code == EXIT_FATAL_STORAGE
            assert client.discard_mode
        else:
            assert policy.terminal_failure is None
        assert _status_counts(app) == (0, 0, 0), "an unrelated fatal must not touch the degraded-channel counters"
    finally:
        _cleanup(app)


def test_an_ordinary_exception_on_the_quality_path_stays_ordinary(tmp_path, monkeypatch):
    app = _app(tmp_path)
    try:
        client, policy = app.client, app.failure_policy

        def boom(row, **kw):
            raise RuntimeError("ordinary bug, not a storage fatal")
        monkeypatch.setattr(app.quality_writer, "write", boom)

        asyncio.run(_run_frames(client, _snapshot_and_decreasing_delta()))

        assert client.processing_errors >= 1, "P0-1: an ordinary exception is still an ordinary processing error"
        assert client.fatal_storage_errors == 0, "an ordinary exception must never be promoted to a fatal"
        assert policy.quality_degraded is None and policy.terminal_failure is None and not policy.failed_components
        assert _status_counts(app) == (0, 0, 0)
    finally:
        _cleanup(app)


def test_writer_quality_sink_contains_only_this_runners_quality_channel_fatal(tmp_path, monkeypatch):
    app = _app(tmp_path)
    try:
        policy = app.failure_policy
        _Disk(monkeypatch).arm(app.quality_writer)
        app.raw_wire_writer._emit_quality("ERROR", "t_sink_quality")      # a raw writer's own self-report
        assert policy.quality_degraded is not None and policy.terminal_failure is None

        other = FatalStorageError("injected", stream="bybit_raw_wire", component="parquet_writer",
                                  stage="rename", durability="unpublished")
        monkeypatch.setattr(app, "_persist_quality_event", lambda event: (_ for _ in ()).throw(other))
        with pytest.raises(FatalStorageError) as info:
            app._writer_quality_sink({"stream": "x", "event_type": "ERROR", "reason": "t_sink_other"})
        assert info.value is other, "the sink swallowed or replaced a typed fatal of another stream"
    finally:
        _cleanup(app)


# ================= 6. no unbounded logging, repeated alerts or recursive writer calls (all entry points)
def test_degraded_channel_never_calls_the_failed_writer_again_and_does_not_flood(tmp_path, monkeypatch, alerts, caplog):
    caplog.set_level(logging.INFO)
    app = _app(tmp_path)
    try:
        policy = app.failure_policy
        _Disk(monkeypatch).arm(app.quality_writer)
        writes = _spy_quality_writer(app)
        latches = _spy_on_fatal(policy)

        _emit_ws(app, "drop-0")
        errors_after_first = _error_events(caplog)
        emitted = 1
        for i in range(1, N_EVENTS):
            _emit_ws(app, f"drop-{i}")                                          # websocket client
            app._record_adapter_unhandled(_Unhandled(f"u-{i}"))                 # adapter unhandled
            app.raw_wire_writer._emit_quality("ERROR", f"w-{i}")                # a writer's own sink
            app._emit_quality_event({"stream": "bybit_orderbook", "event_type": "ERROR", "reason": f"b-{i}"})
            emitted += 4

        assert len(writes) == 1, f"the FAILED quality writer was called {len(writes)} times"
        assert len(latches) == 1 and policy.failure_repeats == {}, "the latch must not be re-entered per event"
        assert len(policy.failed_components) == 1 and policy.terminal_failure is None
        assert len(alerts) == 1 and len(_quality_alerts(alerts)) == 1, alerts
        assert _error_events(caplog) == errors_after_first, "an error log was emitted per later event"
        assert _error_events(caplog).count("storage_failure_latched") == 1
        # every event is accounted, none is lost, and the count agrees with the WAL on disk
        assert _status_counts(app) == (emitted, 0, 0)
        assert len(_wal_records(tmp_path)) == emitted
    finally:
        _cleanup(app)


# ========================================= 7. counters: durably retained vs unconfirmed vs lost
def test_counters_are_zero_and_the_channel_healthy_before_and_after_normal_traffic(tmp_path):
    app = _app(tmp_path)
    try:
        healthy = {"quality_degraded": False, "quality_events_wal_only": 0, "quality_events_wal_unconfirmed": 0,
                   "quality_events_lost": 0, "quality_checkpoint_blocked": False, "quality_failure": None}
        assert app.quality_channel_status() == healthy
        for i in range(5):
            _emit_ws(app, f"healthy-{i}")
        assert app.quality_channel_status() == healthy, "healthy events must not be counted as degraded-channel events"
        assert len(_rows(tmp_path)) == 5
    finally:
        _cleanup(app)


def test_wal_retained_events_are_really_replayed_by_the_next_start(tmp_path, monkeypatch):
    """'wal_only' must mean what it says: proven by the WAL on disk and by an actual restart."""
    app = _app(tmp_path)
    reasons = ["trip", "r1", "r2", "r3", "r4"]
    try:
        with monkeypatch.context() as mp:
            disk = _Disk(mp)
            _degrade(app, disk)                                                # emits "trip"
            for reason in reasons[1:]:
                _emit_ws(app, reason)
            assert app.quality_channel_status()["quality_events_wal_only"] == len(reasons)
            assert _status_counts(app) == (len(reasons), 0, 0)
            pending = _pending(tmp_path)
            assert [e["reason"] for e in pending] == reasons, "the WAL must hold every counted event, un-checkpointed"
            assert _disk_ckpt(tmp_path) < min(e["seq"] for e in pending)
            assert not [r for r in _rows(tmp_path) if r["reason"] in reasons], "none reached Parquet: the writer is dead"
            _crash(app)
        # disk healthy again, new process on the same data dir
        app2 = _app(tmp_path)
        try:
            for reason in reasons:
                assert len([r for r in _rows(tmp_path) if r["reason"] == reason]) == 1, f"{reason} was not replayed once"
            assert _pending(tmp_path) == []
            assert app2.quality_degraded is None and not app2._quality_checkpoint_blocked
        finally:
            _cleanup(app2)
    finally:
        _cleanup(app)


def test_events_whose_wal_append_is_in_doubt_are_unconfirmed_not_retained_and_not_lost(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    app = _app(tmp_path)
    try:
        disk = _Disk(monkeypatch)
        disk.arm(app.quality_writer)
        _fail_wal_fsync(monkeypatch, app)       # bytes written + flushed, fsync fails: a record MAY exist
        for i in range(N_EVENTS):
            _emit_ws(app, f"u{i}")

        assert app.quality_degraded is not None
        assert _status_counts(app) == (0, N_EVENTS, 0), "in-doubt events must be neither 'retained' nor 'lost'"
        errors = _error_events(caplog)
        assert errors.count("bybit_quality_event_wal_append_failed_in_persist") == 1, "per-event log while degraded"
        assert errors.count("bybit_quality_event_wal_unconfirmed") == N_EVENTS.bit_length(), errors  # 1,2,4,...,32
        assert len(errors) <= 12
        assert len(_wal_records(tmp_path)) == N_EVENTS, "the bytes did reach the file; durability is just unproven"
    finally:
        _cleanup(app)


def test_events_with_no_wal_record_and_no_writer_are_counted_lost_with_one_alert(tmp_path, monkeypatch, alerts, caplog):
    caplog.set_level(logging.INFO)
    app = _app(tmp_path)
    try:
        policy = app.failure_policy
        _Disk(monkeypatch).arm(app.quality_writer)

        def wal_unavailable(event):
            raise OSError("quality WAL unavailable (injected)")        # no quality_event_id: nothing was recorded
        monkeypatch.setattr(app._quality_wal, "append", wal_unavailable)
        writes = _spy_quality_writer(app)

        for i in range(N_EVENTS):
            _emit_ws(app, f"lost-{i}")

        assert policy.quality_degraded is not None and len(writes) == 1
        assert _status_counts(app) == (0, 0, N_EVENTS), "unrecorded events must be reported as LOST, never as retained"
        assert app.quality_channel_status()["quality_checkpoint_blocked"] is True
        assert not _wal_records(tmp_path) and not [r for r in _rows(tmp_path) if r["reason"].startswith("lost-")], \
            "'lost' must mean there is no copy anywhere"
        lost_alerts = [m for m in alerts if "QUALITY EVENTS UNRECORDED" in m]
        assert len(lost_alerts) == 1 and len(alerts) == 2, alerts          # one degrade alert + one loss alert
        errors = _error_events(caplog)
        assert errors.count("bybit_quality_event_unrecorded") == N_EVENTS.bit_length(), errors
        assert errors.count("bybit_quality_event_wal_append_failed_in_persist") == 1
        assert len(errors) <= 14
    finally:
        _cleanup(app)


# ============== 8. the process keeps collecting order-book data and still stops cleanly (supervised)
def test_supervised_bybit_keeps_collecting_orderbook_data_after_a_quality_failure(tmp_path, monkeypatch):
    app = _app(tmp_path)
    try:
        client = app.client
        _Disk(monkeypatch).arm(app.quality_writer)

        async def run() -> None:
            await _run_frames(client, _snapshot_and_decreasing_delta())
            client.running = True
            await asyncio.sleep(3600)
        app.run = run

        async def main() -> int:
            stop = asyncio.Event()
            supervisor = asyncio.create_task(supervise_standalone_runner(app, stop, task_timeout_s=2))
            await asyncio.sleep(1.0)
            assert not supervisor.done(), "a quality-channel failure must not end the process"
            assert app.quality_degraded is not None and app.failure_policy.terminal_failure is None
            assert len(app.ob_writer.buffer) == 1 and client.fatal_storage_errors == 0
            stop.set()
            return await asyncio.wait_for(supervisor, SUPERVISED_BOUND_S)
        assert asyncio.run(main()) == 0 == app.exit_code
        _assert_all_closed(app)
    finally:
        _cleanup(app)
