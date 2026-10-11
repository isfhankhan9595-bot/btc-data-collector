"""F5 Finding A1: a quality-only failure during startup must not stop raw capture starting.

Defect
------
A raw writer's crashed-segment recovery (``_recover_orphans``) emits a quality event
while the raw writer is still being CONSTRUCTED. That runs ``_persist_quality_event``
BEFORE the runner's ``failure_policy`` exists. If the runner's own quality writer then
failed (ENOSPC on its segment publication), ``_persist_quality_event`` re-raised the
typed ``FatalStorageError`` because ``policy is None``; the writer's sink classifier
(default table, which does not know the venue-prefixed ``spot_`` / ``okx_`` quality
streams) re-raised it again, and it escaped the runner's constructor. A telemetry
failure therefore prevented otherwise-healthy raw capture from starting.

Contract under test (all three runners: Binance Spot, OKX collector, OKX capture-only)
--------------------------------------------------------------------------------------
* a typed fatal naming THIS runner's own quality stream, seen before the policy exists,
  is held (not classified, not contained as a raw failure) and startup completes;
* it is latched ONCE through the runner's own policy as soon as the policy is built
  (degrade the quality channel, origin ``quality_writer``), with ONE alert;
* the failed quality writer is never called again, in the startup window or after it;
* nothing is fabricated: no quality row is published, the writer stays failed, and the
  lost events are counted, never claimed as retained (these runners keep no quality WAL);
* a typed fatal of ANY other stream (raw evidence, derived, another venue's quality
  stream, unknown, no stream) is NOT contained merely because it happened during
  startup: it still escapes the constructor;
* a raw-evidence fatal after such a startup is still terminal (exit 70).

Everything drives the real runners and the real ``ParquetWriter``. The quality failure
is a real ENOSPC injected at the ``os.replace`` seam, and the orphan is a real
``*.seg.tmp`` plus ``*.count.json`` sidecar in the raw stream directory.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path

import pytest

from collector import run_okx_capture as rcap
from collector.collector import notifications
from collector.collector import parquet_writer as pw_module
from collector.collector.failure_topology import VERDICT_DEGRADE_QUALITY, VERDICT_TERMINATE
from collector.collector.standalone_failure_policy import EXIT_FATAL_STORAGE
from collector.collector.storage_errors import FatalStorageError
from collector.collector.storage_layout import segment_path
from collector.tests.test_f5_final_quality_writer_latch import (
    SUPERVISED_BOUND_S, _Disk, _assert_all_closed, _cleanup_any, _emit_quality, _error_events,
    _make, _quality_alerts)
from collector.tests.test_f5_fatal_storage_topology import _FakeWs
from collector.tests.test_f5_standalone_runners import _raw_frames, _terminal_run
from collector.tests.test_p0_4_runner_lifecycle import _publish_all

REAL_REPLACE = os.replace
KINDS = ["spot", "okx", "okx_capture"]
PREFIX = {"spot": "spot_", "okx": "okx_", "okx_capture": "okx_"}
#: Raw writers that carry a quality sink, i.e. whose orphan recovery emits during construction.
SINK_RAW_STREAMS = {"spot": ("raw_wire", "raw_rest"), "okx": ("raw_wire",), "okx_capture": ("raw_wire",)}
ORPHANS_PER_STREAM = 2
LATER_EVENTS = 25


@pytest.fixture(autouse=True)
def alerts():
    sent: list[str] = []
    notifications.set_notifier(notifications.CallableNotifier(lambda message: sent.append(message) or True))
    yield sent
    notifications.set_notifier(None)


# --------------------------------------------------------------------------- helpers
def _is_quality_stream(writer) -> bool:
    return writer.stream_name.endswith("quality_events")


def _seed_orphans(tmp_path: Path, kind: str) -> int:
    """Leave crashed segments behind in every sink-carrying raw stream; return how many
    quality events the runner's construction will therefore emit (one per orphan)."""
    data = tmp_path / "data"
    emitted = 0
    for stream in SINK_RAW_STREAMS[kind]:
        for seq in range(1, ORPHANS_PER_STREAM + 1):
            final = segment_path(data, PREFIX[kind] + stream, "2026-10-10-23", seq)
            final.parent.mkdir(parents=True, exist_ok=True)
            Path(str(final) + ".tmp").write_bytes(b"crashed-before-publication")
            Path(str(final) + ".count.json").write_text(json.dumps({"rows": 7}))
            emitted += 1
    return emitted


class _QualityDiskFull:
    """Real ``os.replace`` seam: publishing a segment of any ``*quality_events`` stream
    fails with ENOSPC. Armed BEFORE the runner exists, so it is in force during construction."""

    def __init__(self, monkeypatch) -> None:
        monkeypatch.setattr(pw_module.os, "replace", self._replace)

    @staticmethod
    def _replace(src, dst, *a, **k):
        target = Path(str(dst))
        if target.name.endswith(".seg") and target.parent.name.endswith("quality_events"):
            raise OSError(28, "No space left on device (injected)")
        return REAL_REPLACE(src, dst, *a, **k)


def _spy_quality_writes(monkeypatch) -> list[str]:
    """Count real ``write`` calls on any quality writer, class-wide so it is in place
    before the runner is constructed. Behaviour is unchanged."""
    calls: list[str] = []
    real_write = pw_module.ParquetWriter.write

    def write(self, record, **kwargs):
        if _is_quality_stream(self):
            calls.append(self.stream_name)
        return real_write(self, record, **kwargs)
    monkeypatch.setattr(pw_module.ParquetWriter, "write", write)
    return calls


def _build_orphaned_with_failed_quality(kind, tmp_path, monkeypatch):
    emitted = _seed_orphans(tmp_path, kind)
    _QualityDiskFull(monkeypatch)
    calls = _spy_quality_writes(monkeypatch)
    app = _make(kind, tmp_path, monkeypatch)          # must NOT raise
    return app, calls, emitted


def _stream_dir(app, writer) -> Path:
    return writer.stream_dir


# =========================================== 1. startup completes; latched once; one alert
@pytest.mark.parametrize("kind", KINDS)
def test_startup_completes_and_the_quality_failure_is_latched_once_with_one_alert(
        kind, tmp_path, monkeypatch, alerts, caplog):
    caplog.set_level(logging.INFO)
    app, calls, emitted = _build_orphaned_with_failed_quality(kind, tmp_path, monkeypatch)
    try:
        policy = app.failure_policy
        quality_stream = app.quality_writer.stream_name
        assert quality_stream == PREFIX[kind] + "quality_events"

        # observable once the policy exists: the quality channel is degraded, by the quality writer
        record = app.quality_degraded
        assert record is not None and record is policy.quality_degraded
        assert (record.verdict, record.origin, record.stream) == (
            VERDICT_DEGRADE_QUALITY, "quality_writer", quality_stream)

        # ... and ONLY the quality channel: raw capture is healthy and the process is not terminal
        assert policy.terminal_failure is None and app.exit_code == 0 and not policy.terminal_event.is_set()
        assert not policy.isolated_routes
        assert app.raw_wire_writer.failure_snapshot() is None
        if kind == "spot":
            assert app.raw_rest_writer.failure_snapshot() is None
            assert app.trades_writer.failure_snapshot() is None and app.ob_writer.failure_snapshot() is None

        # latched exactly once: one component, never re-observed, one alert, one latch log
        assert len(policy.failed_components) == 1 and policy.failure_repeats == {}
        assert len(alerts) == 1 and len(_quality_alerts(alerts, app)) == 1, alerts
        assert _error_events(caplog).count("storage_failure_latched") == 1

        # the pending hold is spent: nothing is left to be latched or reported a second time
        assert app._pre_policy_quality_failure is None

        # every startup event is accounted as LOST (none claimed retained): the one that tripped
        # the writer plus every later one, which was short-circuited
        assert emitted == ORPHANS_PER_STREAM * len(SINK_RAW_STREAMS[kind]) and emitted >= 2
        status = app.quality_channel_status()
        assert status["quality_degraded"] is True and status["quality_events_lost"] == emitted
        assert status["quality_failure"]["origin"] == "quality_writer"
        assert not hasattr(app, "_quality_wal")
        assert status["quality_wal"] is False, "the status must say these runners have no quality WAL"
        assert "wal" not in alerts[0].lower(), "the alert must not claim a WAL"
    finally:
        _cleanup_any(app)


# =========================================== 2. the dead writer is never called again
@pytest.mark.parametrize("kind", KINDS)
def test_the_failed_quality_writer_is_not_retried_during_or_after_startup(kind, tmp_path, monkeypatch, alerts, caplog):
    caplog.set_level(logging.INFO)
    app, calls, emitted = _build_orphaned_with_failed_quality(kind, tmp_path, monkeypatch)
    try:
        assert emitted >= 2, "the repro needs several startup events to prove the writer is not retried"
        assert len(calls) == 1, f"the FAILED quality writer was called {len(calls)} times during startup"
        errors_after_startup = _error_events(caplog)

        for i in range(LATER_EVENTS):                  # through the runner's REAL client callback
            _emit_quality(app, i)

        assert len(calls) == 1, f"the FAILED quality writer was called again ({len(calls)} calls)"
        assert app.quality_channel_status()["quality_events_lost"] == emitted + LATER_EVENTS
        assert len(alerts) == 1, "later events must not raise further alerts"
        # A2: a loss is logged at a bounded cadence (first, then powers of two); anything else is silent
        later = _error_events(caplog)[len(errors_after_startup):]
        assert all(e.endswith("_quality_event_lost") for e in later), later
        assert len(later) <= LATER_EVENTS and len(later) <= (emitted + LATER_EVENTS).bit_length()
        assert len(app.failure_policy.failed_components) == 1 and app.failure_policy.failure_repeats == {}
    finally:
        _cleanup_any(app)


# =========================================== 3. nothing fabricated; the writer is not made to look healthy
@pytest.mark.parametrize("kind", KINDS)
def test_no_quality_record_is_fabricated_and_the_failed_writer_does_not_look_healthy(kind, tmp_path, monkeypatch):
    app, calls, emitted = _build_orphaned_with_failed_quality(kind, tmp_path, monkeypatch)
    try:
        # no quality row was published, and no segment/marker pretends one was
        quality_dir = _stream_dir(app, app.quality_writer)
        assert list(quality_dir.glob("*.seg")) == [], "a quality segment was published despite ENOSPC"
        assert not any(quality_dir.glob("*.marker*")), "a publication marker exists for an unpublished segment"

        # the writer is still FAILED: its own latch says so and it still refuses writes
        assert app.quality_writer.failure_snapshot() is not None
        with pytest.raises(FatalStorageError):
            app.quality_writer.write({})
        assert app.quality_degraded is not None and app.quality_channel_status()["quality_degraded"] is True
    finally:
        _cleanup_any(app)


# =========================================== 4. any OTHER typed fatal still escapes the constructor
def _other_stream_cases(kind: str) -> dict[str, str | None]:
    p = PREFIX[kind]
    return {"own_raw_evidence": p + "raw_wire",
            "own_derived": p + "trades",
            "foreign_venue_quality": "bybit_quality_events",
            "unknown_stream": "mystery_stream",
            "no_stream": None}


@pytest.mark.parametrize("label", list(_other_stream_cases("okx")))
@pytest.mark.parametrize("kind", KINDS)
def test_any_other_typed_fatal_during_startup_still_escapes_the_constructor(
        kind, label, tmp_path, monkeypatch, alerts):
    stream = _other_stream_cases(kind)[label]
    _seed_orphans(tmp_path, kind)
    calls: list[str] = []

    def write(self, record, **kwargs):
        if _is_quality_stream(self):
            calls.append(self.stream_name)
            raise FatalStorageError("not this runner's quality writer", stream=stream,
                                    component="parquet_writer", stage="rename", durability="unpublished")
        return real_write(self, record, **kwargs)
    real_write = pw_module.ParquetWriter.write
    monkeypatch.setattr(pw_module.ParquetWriter, "write", write)

    with pytest.raises(FatalStorageError) as raised:
        _make(kind, tmp_path, monkeypatch)

    assert raised.value.stream == stream, f"{label}: the typed fatal was replaced or contained"
    assert len(calls) == 1, f"{label}: startup carried on after a non-quality typed fatal"
    assert alerts == [], f"{label}: nothing may be reported as a quality degradation"


# =========================================== 5. an ordinary quality error is exactly as before
@pytest.mark.parametrize("kind", KINDS)
def test_an_ordinary_error_in_the_quality_writer_during_startup_is_still_log_and_continue(
        kind, tmp_path, monkeypatch, alerts, caplog):
    caplog.set_level(logging.INFO)
    emitted = _seed_orphans(tmp_path, kind)
    real_write = pw_module.ParquetWriter.write
    calls: list[str] = []

    def write(self, record, **kwargs):
        if _is_quality_stream(self):
            calls.append(self.stream_name)
            raise OSError(5, "transient I/O error")
        return real_write(self, record, **kwargs)
    monkeypatch.setattr(pw_module.ParquetWriter, "write", write)

    app = _make(kind, tmp_path, monkeypatch)
    try:
        # Only attributes that exist on the pre-fix runners: this test must pass BEFORE and AFTER.
        assert app.quality_degraded is None
        assert len(calls) == emitted, "an ordinary error is not a failed writer: the pre-existing path is unchanged"
        assert alerts == [] and app.failure_policy.failed_components == {}
        assert app.quality_events_lost == 0
    finally:
        _cleanup_any(app)


# =========================================== 6. raw evidence is still terminal after such a startup
@pytest.mark.parametrize("kind", ["spot", "okx"])
def test_a_raw_evidence_fatal_after_a_startup_quality_failure_still_terminates_with_exit_70(
        kind, tmp_path, monkeypatch, alerts):
    app, calls, emitted = _build_orphaned_with_failed_quality(kind, tmp_path, monkeypatch)
    try:
        assert app.quality_degraded is not None and app.exit_code == 0 and len(calls) == 1
        disk = _Disk(monkeypatch)
        disk.arm(app.raw_wire_writer)

        code = _terminal_run(app, _raw_frames(kind))

        assert code == EXIT_FATAL_STORAGE and code != 0
        record = app.failure_policy.terminal_failure
        assert record is not None and (record.verdict, record.stream) == (
            VERDICT_TERMINATE, app.raw_wire_writer.stream_name)
        assert app.quality_degraded is not None, "the earlier quality latch must survive, not be re-reported"
        assert len(calls) == 1, "the FAILED quality writer must not be called while reporting the raw failure"
        assert len(_quality_alerts(alerts, app)) == 1
        assert sum(1 for m in alerts if "[terminate]" in m and app.raw_wire_writer.stream_name in m) == 1
        _assert_all_closed(app)
    finally:
        _cleanup_any(app)


def test_okx_capture_only_raw_evidence_fatal_after_a_startup_quality_failure_still_exits_non_zero(
        tmp_path, monkeypatch, alerts):
    app, calls, emitted = _build_orphaned_with_failed_quality("okx_capture", tmp_path, monkeypatch)
    try:
        assert app.quality_degraded is not None and app.exit_code == 0 and len(calls) == 1
        _Disk(monkeypatch).arm(app.raw_wire_writer)
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
        assert status["quality_degraded"] is True and len(calls) == 1
        assert len(_quality_alerts(alerts, app)) == 1
        _assert_all_closed(app)
    finally:
        _publish_all(app)
