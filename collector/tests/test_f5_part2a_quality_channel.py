"""F5 remediation Part 2A: quality-channel failure, WAL accounting, RawCapture counter.

F-3  A failing QUALITY writer must degrade telemetry only. It must never (a) escape
     into a market-data task, (b) be mistaken for a raw-evidence failure, or
     (c) hide a genuine raw / derived failure.
F-4  ``_quality_events_wal_only`` counts an event ONLY when its WAL record is
     established. An event with no durable record is reported as unrecorded, never as
     "retained", and never through the failed quality writer.
RC   ``RawCapture.capture_failures`` counts every failed capture attempt exactly once.

Everything drives the real ``CollectorApp`` / ``RawCapture`` / ``ParquetWriter``. Storage
faults are injected at the real ``os.replace`` seam (scoped to one stream directory), so
every typed ``FatalStorageError`` comes from the writer's own failure latch.
"""
from __future__ import annotations

import asyncio
import sys

import pytest

from collector import run_collector as rc
from collector.collector import notifications
from collector.collector.failure_topology import VERDICT_DEGRADE_QUALITY
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import QualityEventType
from collector.collector.raw_capture import RawCapture, RawRestRecord, RawWireRecord
from collector.collector.storage_errors import FatalStorageError
from collector.tests.test_f5_fatal_storage_topology import (
    _break_raw, _break_route, _fail_segment_publish_for, _fatal, _frame, _wal_events)
from collector.tests.test_p0_4_runner_lifecycle import _build, _now, _publish_all
from collector.tests import test_quality_event_durability as qed
from collector.tests.test_parquet_writer_publication_failure import SCHEMA


# ----------------------------------------------------------------------------- fixtures
@pytest.fixture(autouse=True)
def alerts():
    sent: list[str] = []
    notifications.set_notifier(notifications.CallableNotifier(lambda message: sent.append(message) or True))
    yield sent
    notifications.set_notifier(None)


@pytest.fixture
def app(tmp_path, monkeypatch):
    built = _build("usdm", tmp_path, monkeypatch)
    for name in ("validate_markprice", "validate_liquidation", "validate_orderbook"):
        monkeypatch.setattr(built.validator, name, lambda features: (True, ""))
    yield built
    _publish_all(built)
    built.segment_dedup.close()


def _quality_down(app, monkeypatch):
    """Next quality write publishes (segment_rows=1) and fails at the real rename seam."""
    app.quality_writer.segment_rows = 1
    _fail_segment_publish_for(monkeypatch, "quality_events")


def _count_quality_writes(app) -> dict:
    calls = {"n": 0}
    real = app.quality_writer.write

    def counting(*a, **k):
        calls["n"] += 1
        return real(*a, **k)
    app.quality_writer.write = counting
    return calls


def _install_fake_aiohttp(monkeypatch, app, *, polls=3, status=503, session_error=None):
    """Fake aiohttp whose responses fail ``raise_for_status`` (OI/REST error path)."""
    class FakeResponse:
        def __init__(self): self.status = status; self.headers = {}
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def text(self): return "{}"
        def raise_for_status(self): raise RuntimeError(f"HTTP {status}")

    class FakeSession:
        def __init__(self, *a, **k): pass
        async def __aenter__(self):
            if session_error is not None:
                raise session_error
            return self
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


# ================================================================================== F-3
def test_F3_oi_error_handler_survives_a_quality_writer_failure(app, monkeypatch):
    """The hostile reproduction: a quality-writer failure inside the OI error handler
    escaped the task and crashed the collector (exit 1) while raw capture was healthy."""
    _quality_down(app, monkeypatch)
    writes = _count_quality_writes(app)
    captured = []
    real_capture = app._capture_rest
    app._capture_rest = lambda record: captured.append(record) or real_capture(record)
    _install_fake_aiohttp(monkeypatch, app, polls=3)

    asyncio.run(app._poll_openinterest())          # must not raise out of the task

    assert app.quality_degraded is not None and app.quality_degraded.verdict == VERDICT_DEGRADE_QUALITY
    assert app.terminal_failure is None and app.exit_code == 0
    assert app.failed_components.keys() == {"parquet_writer:quality_events"}, "only the quality channel failed"
    assert [(r.purpose, r.ok) for r in captured] == [("open_interest", False)] * 3, \
        "raw REST kept capturing every poll, once each"
    assert app.raw_rest_writer.failure_snapshot() is None
    assert writes["n"] == 1, "no per-poll flood: the failed quality writer is never called again"
    reasons = [e["reason"] for e in _wal_events(app)]
    assert reasons.count("oi_poll_failed:RuntimeError") == 3, "every OI error is still durable in the WAL"


def test_F3_orderbook_recovery_completes_its_lifecycle_when_quality_fails(app, monkeypatch):
    """RESYNC / ERROR quality events raised out of recovery used to abort it mid-lifecycle
    (controller never failed/succeeded => stuck in flight => no further recoveries)."""
    _quality_down(app, monkeypatch)
    _install_fake_aiohttp(monkeypatch, app, session_error=RuntimeError("snapshot unreachable"))
    controller = app.recovery_controller

    result = asyncio.run(app._recover_binance_book("gap"))

    assert result is False
    assert app.quality_degraded is not None and app.terminal_failure is None
    assert controller.in_flight is False, "recovery lifecycle finished (fail() ran); not stuck in flight"


def test_F3_record_book_quality_does_not_raise_on_quality_channel_failure(app, monkeypatch):
    _quality_down(app, monkeypatch)
    writes = _count_quality_writes(app)
    app._record_book_quality(QualityEventType.RESYNC, "first")        # latches the channel
    app._record_book_quality(QualityEventType.ERROR, "second")        # fast path, no writer call
    assert writes["n"] == 1 and app.quality_degraded is not None


def test_F3_drains_keep_going_after_a_quality_channel_failure(app, monkeypatch):
    _quality_down(app, monkeypatch)
    events = [{"stream": "orderbook", "event_type": "ERROR", "reason": f"integrity{i}"} for i in range(5)]
    app.binance_book.drain_quality_events = lambda: list(events)
    app._drain_integrity_quality_events()                              # strict hot-path drain
    assert {e["reason"] for e in _wal_events(app)} >= {f"integrity{i}" for i in range(5)}
    assert app._quality_events_wal_only == 4


def test_F3_unrouted_frame_survives_a_quality_channel_failure(app, monkeypatch):
    _quality_down(app, monkeypatch)
    asyncio.run(app.handle_message({"stream": "ethusdt@mystery", "data": {}}))
    asyncio.run(app.handle_message({"nope": 1}))
    assert app.quality_degraded is not None and app.terminal_failure is None


def test_F3_a_quality_fatal_through_a_raw_writers_sink_is_not_a_raw_terminal(app, monkeypatch):
    """The unguarded sink paths inside ParquetWriter (migration notice, crashed-segment
    DATA_DROP) run inside write(). A quality fatal there must not surface as the RAW
    writer's failure: _capture_raw_frame forces TERMINATE for anything raw_wire raises."""
    _quality_down(app, monkeypatch)
    writer = app.raw_wire_writer
    writer.write = lambda row, **k: writer._emit_quality("STORAGE_MIGRATION", "legacy_hourly_file_present")

    app._capture_raw_frame('{"x": 1}', local_receive_ts=_now())

    assert app.terminal_failure is None and app.exit_code == 0
    assert app.raw_capture.fatal_capture_failures == 0
    assert app.quality_degraded is not None
    assert not any(c.discard_mode for c in app.ws_clients)


def _writer(tmp_path, sink):
    return ParquetWriter("orderbook", SCHEMA, base_dir=str(tmp_path), quality_event_sink=sink)


@pytest.mark.parametrize("emit", ["quality", "drop"])
def test_F3_writer_contains_only_a_quality_channel_fatal_from_its_unguarded_sink(tmp_path, emit):
    def call(writer):
        if emit == "quality":
            writer._emit_quality("STORAGE_MIGRATION", "legacy_hourly_file_present")
        else:
            writer._emit_drop(3)

    # quality-channel typed fatal: contained (the owner already latched it)
    call(_writer(tmp_path / "a", lambda e: (_ for _ in ()).throw(_fatal("quality_events"))))
    # a typed fatal of ANY other stream is not the quality channel: never swallowed
    with pytest.raises(FatalStorageError):
        call(_writer(tmp_path / "b", lambda e: (_ for _ in ()).throw(_fatal("raw_wire"))))
    # ordinary exceptions keep their pre-existing propagation (P0-1 untouched)
    with pytest.raises(ValueError):
        call(_writer(tmp_path / "c", lambda e: (_ for _ in ()).throw(ValueError("ordinary"))))


def test_F3_genuine_raw_wire_fatal_still_terminates_while_quality_is_degraded(app, monkeypatch):
    _quality_down(app, monkeypatch)
    with pytest.raises(FatalStorageError):
        app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "kill quality"})
    assert app.quality_degraded is not None
    monkeypatch.undo()
    _break_raw(app, monkeypatch, "raw_wire")

    with pytest.raises(FatalStorageError) as info:                     # fail-closed raw capture still raises
        app._capture_raw_frame('{"y": 1}', local_receive_ts=_now())
    assert info.value.stream == "raw_wire"
    assert app._on_fatal_storage(info.value, origin="raw_frame") == "terminate"
    assert app.terminal_failure is not None and app.exit_code != 0


def test_F3_genuine_raw_rest_fatal_still_terminates_while_quality_is_degraded(app, monkeypatch):
    _quality_down(app, monkeypatch)
    with pytest.raises(FatalStorageError):
        app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "kill quality"})
    monkeypatch.undo()
    _break_raw(app, monkeypatch, "raw_rest")
    record = RawRestRecord(request_ts=_now(), response_receive_ts=_now(), endpoint="e", purpose="p", ok=True)
    assert app._capture_rest(record) is False                          # segment_rows=1: first write publishes and fails
    assert app.terminal_failure is not None and app.terminal_failure.stream == "raw_rest"


def test_F3_derived_writer_failure_still_isolates_its_route_while_quality_is_degraded(app, monkeypatch):
    _quality_down(app, monkeypatch)
    with pytest.raises(FatalStorageError):
        app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "kill quality"})
    monkeypatch.undo()
    _break_route(app, monkeypatch, "markprice")
    asyncio.run(app.handle_message(_frame("markprice")))
    assert set(app.isolated_routes) == {"markprice"}
    assert app.terminal_failure is None


def test_F3_emit_quality_event_distinguishes_the_boundaries(app, monkeypatch):
    # healthy -> True
    assert app._emit_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "ok"}) is True
    # quality-channel typed fatal -> contained (False); already latched degraded by the persist path
    app._persist_quality_event = lambda e: (_ for _ in ()).throw(_fatal("quality_events"))
    assert app._emit_quality_event({"reason": "q"}) is False
    # a typed fatal of any other stream is NOT quality telemetry: propagates
    app._persist_quality_event = lambda e: (_ for _ in ()).throw(_fatal("raw_wire"))
    with pytest.raises(FatalStorageError):
        app._emit_quality_event({"reason": "r"})
    # ordinary exceptions keep P0-1 behaviour: propagate
    app._persist_quality_event = lambda e: (_ for _ in ()).throw(ValueError("ordinary"))
    with pytest.raises(ValueError):
        app._emit_quality_event({"reason": "o"})


def test_F3_no_error_flood_after_the_channel_is_degraded(app, monkeypatch):
    _quality_down(app, monkeypatch)
    writes = _count_quality_writes(app)
    for i in range(200):
        app._emit_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": f"e{i}"})
    assert writes["n"] == 1, "the failed writer is never resurrected or re-called"
    assert app.failure_repeats == {}, "no repeated latch/report churn per event"
    assert app._quality_events_wal_only == 199


def test_F3_idle_publish_failure_latches_degraded_and_reports_once(app, monkeypatch):
    app.quality_writer.segment_rows = 10_000
    app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "buffered"})
    _fail_segment_publish_for(monkeypatch, "quality_events")
    app.quality_writer.publish_if_due = lambda: app.quality_writer.publish_open_segment()
    app._quality_publish_if_due()
    assert app.quality_degraded is not None and app.terminal_failure is None
    assert app._quality_checkpoint_is_blocked()


def test_F3_startup_recovery_reports_wal_corruption_without_aborting_construction(tmp_path, monkeypatch):
    app = qed._real_app(tmp_path, monkeypatch)
    qed._emit(app, 3)
    qed._drain(app)
    qed._crash(app)
    wal_dir = qed._qdir(tmp_path) / "wal"
    victim = sorted(wal_dir.glob("*.wal"))[0]
    victim.write_text(victim.read_text() + "{this is not json\n")

    real_write = ParquetWriter.write

    def write(self, row, *a, **k):
        if self.stream_name == "quality_events":
            raise _fatal("quality_events")
        return real_write(self, row, *a, **k)
    monkeypatch.setattr(ParquetWriter, "write", write)

    app2 = qed._real_app(tmp_path, monkeypatch)                        # must not raise
    try:
        assert app2.quality_degraded is not None and app2.terminal_failure is None
        assert app2._quality_checkpoint_is_blocked() and victim.exists()
    finally:
        monkeypatch.setattr(ParquetWriter, "write", real_write)
        app2.shutdown()


# ================================================================================== F-4
def _degrade(app, monkeypatch):
    """Kill the quality writer with one real event; return the writes counter."""
    _quality_down(app, monkeypatch)
    writes = _count_quality_writes(app)
    with pytest.raises(FatalStorageError):
        app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "A-kills-writer"})
    assert app.quality_degraded is not None and writes["n"] == 1
    return writes


def _wal_append_fails(app, monkeypatch, *, quality_event_id=None):
    def append(event):
        error = OSError(5, "injected WAL append failure")
        if quality_event_id is not None:
            error.quality_event_id = quality_event_id
        raise error
    monkeypatch.setattr(app._quality_wal, "append", append)


def test_F4_failed_wal_append_is_not_counted_as_wal_retained(app, monkeypatch, alerts, tmp_path):
    """The hostile test: events C and D were dropped (no WAL record, no writer call) yet
    ``_quality_events_wal_only`` counted all three."""
    writes = _degrade(app, monkeypatch)
    app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "B-wal-ok"})
    assert app._quality_events_wal_only == 1

    ckpt_before = qed._disk_ckpt(tmp_path)
    _wal_append_fails(app, monkeypatch)
    app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "C-no-wal"})   # no raise
    app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "D-no-wal"})

    assert app._quality_events_wal_only == 1, "C and D are NOT WAL-retained"
    assert app._quality_events_unrecorded == 2, "...they are reported as unrecorded"
    assert writes["n"] == 1, "fallback reporting never touched the failed quality writer"
    reasons = {e["reason"] for e in _wal_events(app)}
    assert {"A-kills-writer", "B-wal-ok"} <= reasons and not ({"C-no-wal", "D-no-wal"} & reasons)
    assert app._quality_checkpoint_is_blocked(), "no checkpoint may advance past an unproven event"
    assert qed._disk_ckpt(tmp_path) == ckpt_before, "the on-disk checkpoint did not move"
    assert len([m for m in alerts if "quality" in m.lower() and "unrecorded" in m.lower()]) == 1, \
        "one bounded operator alert, not one per lost event"


def test_F4_event_without_wal_protection_is_not_counted_as_retained(app, monkeypatch):
    writes = _degrade(app, monkeypatch)
    app._quality_wal = None                                  # no WAL at all: nothing can protect the event
    app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "no-wal-at-all"})
    assert app._quality_events_wal_only == 0
    assert app._quality_events_unrecorded == 1
    assert writes["n"] == 1


def test_F4_wal_append_with_a_possible_record_is_unconfirmed_and_pins_the_checkpoint(app, monkeypatch):
    """write+flush succeeded but fsync failed: a record MAY exist. Not proven => never
    described as retained, but its seq must still hold the checkpoint back."""
    _degrade(app, monkeypatch)
    _wal_append_fails(app, monkeypatch, quality_event_id="evt-0000000000000099")
    app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "maybe-in-wal"})
    assert app._quality_events_wal_only == 0
    assert app._quality_events_wal_unconfirmed == 1
    assert 99 in app._quality_wal_inflight, "an unproven seq pins the checkpoint below it"
    assert app._quality_events_unrecorded == 0, "a record may exist: not claimed lost either"


def test_F4_event_with_wal_provenance_still_counts_as_retained(app, monkeypatch):
    _degrade(app, monkeypatch)
    event_id = app._quality_wal.append({"stream": "unrouted", "event_type": "ERROR", "reason": "provenance"})
    seq = int(event_id.rsplit("-", 1)[-1])
    app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "provenance",
                                "quality_event_id": event_id, "_wal_seq": seq})
    assert app._quality_events_wal_only == 1 and app._quality_events_unrecorded == 0


def test_F4_loss_reporting_is_bounded_and_never_floods(app, monkeypatch, alerts):
    writes = _degrade(app, monkeypatch)
    _wal_append_fails(app, monkeypatch)
    for i in range(500):
        app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": f"lost{i}"})
    assert app._quality_events_unrecorded == 500 and app._quality_events_wal_only == 0
    assert writes["n"] == 1
    assert len([m for m in alerts if "unrecorded" in m.lower()]) == 1


def test_F4_healthy_channel_accounting_is_unchanged(app):
    app._persist_quality_event({"stream": "unrouted", "event_type": "ERROR", "reason": "fine"})
    assert app.quality_degraded is None
    assert getattr(app, "_quality_events_wal_only", 0) == 0
    assert getattr(app, "_quality_events_unrecorded", 0) == 0


# ======================================================================== RawCapture counter
class _RaisingWriter:
    def __init__(self, exc_factory):
        self.exc_factory = exc_factory

    def write(self, row):
        raise self.exc_factory()


def _wire():
    return RawWireRecord(local_receive_ts=_now(), payload="{}", venue="BINANCE")


def _rest():
    return RawRestRecord(request_ts=_now(), response_receive_ts=_now(), endpoint="e", purpose="p")


@pytest.mark.parametrize("kind", ["wire", "rest"])
def test_RC_typed_fatal_fail_open_counts_each_failed_attempt_once(kind):
    """PR counted 116 failures for 58 frames (base: 58): the typed branch incremented,
    then _fail_open() incremented again."""
    writer = _RaisingWriter(lambda: _fatal("raw_wire"))
    sunk = []
    capture = RawCapture(writer, writer, quality_event_sink=sunk.append)       # fail-open (not fail-closed)
    call = capture.capture_wire if kind == "wire" else capture.capture_rest
    record = _wire if kind == "wire" else _rest
    for _ in range(58):
        assert call(record()) is False
    assert capture.capture_failures == 58
    assert capture.fatal_capture_failures == 58
    assert capture.stats()["capture_failures"] == 58
    assert len(sunk) == 58, "one DATA_DROP per failed attempt, as on base"


@pytest.mark.parametrize("kind", ["wire", "rest"])
def test_RC_fail_closed_counts_once_per_attempt_and_still_propagates(kind):
    writer = _RaisingWriter(lambda: _fatal("raw_wire"))
    sunk = []
    capture = RawCapture(writer, writer, quality_event_sink=sunk.append, fail_closed_on_fatal_storage=True)
    call = capture.capture_wire if kind == "wire" else capture.capture_rest
    record = _wire if kind == "wire" else _rest
    for _ in range(58):
        with pytest.raises(FatalStorageError):
            call(record())
    assert capture.capture_failures == 58 and capture.fatal_capture_failures == 58
    assert sunk == [], "fail-closed never consults the quality sink"


@pytest.mark.parametrize("kind", ["wire", "rest"])
def test_RC_ordinary_failure_counts_once_and_is_not_fatal(kind):
    writer = _RaisingWriter(lambda: ValueError("ordinary"))
    capture = RawCapture(writer, writer, quality_event_sink=lambda e: None)
    call = capture.capture_wire if kind == "wire" else capture.capture_rest
    record = _wire if kind == "wire" else _rest
    for _ in range(58):
        assert call(record()) is False
    assert capture.capture_failures == 58 and capture.fatal_capture_failures == 0


def test_RC_mixed_failures_without_a_sink_count_once_each():
    writes = iter([_fatal("raw_wire"), ValueError("x"), _fatal("raw_wire"), RuntimeError("y")])
    capture = RawCapture(_RaisingWriter(lambda: next(writes)), None, quality_event_sink=None)
    for _ in range(4):
        assert capture.capture_wire(_wire()) is False
    assert capture.capture_failures == 4 and capture.fatal_capture_failures == 2
