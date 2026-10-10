"""F5 A2: operator visibility of quality-channel degradation and event loss.

Binance Spot, OKX and OKX capture-only keep NO quality WAL. Once their quality writer
has failed (latched once, one alert, never retried: unchanged) every further quality
event is gone. Before A2 that loss was only a counter that nothing reported; a long
outage was silent until somebody read ``quality_channel_status()``.

Contract under test
-------------------
* the first loss is logged, then only at power-of-two counts: log volume grows with
  log2(losses), never with the losses (``<prefix>_quality_event_lost``);
* ``quality_events_lost`` and ``quality_events_unconfirmed`` are reported distinctly and
  each on its own cadence; an unconfirmed event is never labelled lost or retained;
* the final counters are logged once at shutdown (``<prefix>_quality_channel_final``),
  after every writer is closed, and never mask an existing failure, change the exit
  status, or stop writer cleanup, even when logging itself raises;
* ``quality_channel_status()`` gains ``quality_wal: False`` and every loss report says
  the runner has no quality WAL and that lost events cannot be recovered;
* a quality-only failure never ends otherwise healthy market-data / raw ingestion;
* the three runners behave identically.

Everything drives the real runners, the real ``WebSocketClient`` quality callback, the
real ``StandaloneFailurePolicy`` and real ``ParquetWriter`` publication; faults are
injected at the real ``os.replace`` / directory ``os.fsync`` / parquet-open seams.
"""
from __future__ import annotations

import asyncio
import json
import logging

import pytest

from collector import run_okx_capture as rcap
from collector.collector import notifications
from collector.collector import standalone_failure_policy as sfp
from collector.collector.standalone_failure_policy import (
    EXIT_FATAL_STORAGE, DegradedQualityLossReporting, is_bounded_report_point)
from collector.tests.test_f5_a1_startup_quality_failure import (
    ORPHANS_PER_STREAM, _build_orphaned_with_failed_quality)
from collector.tests.test_f5_a4_counter_accuracy import EXPECTED, FAULTS, _emit, _Faults
from collector.tests.test_f5_fatal_storage_topology import _FakeWs
from collector.tests.test_f5_final_quality_writer_latch import (
    KINDS, COLLECTORS, _cleanup_any, _client, _Disk, _error_events, _frame_kind, _make,
    _quality_alerts, _run_frames, _spy_quality_writer)
from collector.tests.test_f5_standalone_runners import (
    SUPERVISED_BOUND_S, _assert_all_closed, _raw_frames, _terminal_run)

N_EVENTS = 200
LATER = 5
LOSS_SUFFIX = "_quality_event_lost"
UNCONFIRMED_SUFFIX = "_quality_event_unconfirmed"
FINAL_SUFFIX = "_quality_channel_final"
PREFIX = {"spot": "spot", "okx": "okx", "okx_capture": "okx_capture"}
STATUS_KEYS = {"quality_degraded", "quality_events_lost", "quality_events_unconfirmed",
               "quality_wal", "quality_failure"}


@pytest.fixture(autouse=True)
def alerts():
    sent: list[str] = []
    notifications.set_notifier(notifications.CallableNotifier(lambda message: sent.append(message) or True))
    yield sent
    notifications.set_notifier(None)


# --------------------------------------------------------------------------- helpers
def _records(caplog, suffix: str) -> list[dict]:
    """Structured log records whose event name ends with ``suffix``."""
    found = []
    for record in caplog.records:
        try:
            payload = json.loads(record.getMessage())
        except (ValueError, TypeError):
            continue
        if isinstance(payload, dict) and str(payload.get("event", "")).endswith(suffix):
            found.append(payload)
    return found


def _counts(records: list[dict]) -> list[int]:
    return [r["count"] for r in records]


def _powers_of_two_up_to(n: int) -> list[int]:
    out, p = [], 1
    while p <= n:
        out.append(p)
        p *= 2
    return out


def _shutdown(app) -> None:
    """The runner's own shutdown path: ``shutdown()`` for the collectors, ``close()``
    (what ``run()`` calls in its ``finally``) for the capture-only runner."""
    if hasattr(app, "capture"):
        app.close()
    else:
        asyncio.run(app.shutdown())


def _degrade(app, monkeypatch) -> _Disk:
    disk = _Disk(monkeypatch)
    disk.arm(app.quality_writer)
    return disk


def _log_sink_that_raises(monkeypatch, *suffixes: str) -> None:
    """Make the shared logger raise for the named A2 events only."""
    real = sfp.logger

    class _Boom:
        def error(self, event, *a, **k):
            if any(str(event).endswith(s) for s in suffixes):
                raise RuntimeError("log sink is down")
            return real.error(event, *a, **k)

        def __getattr__(self, name):
            return getattr(real, name)

    monkeypatch.setattr(sfp, "logger", _Boom())


# ======================================================== 0. the cadence itself
def test_report_points_are_exactly_the_powers_of_two():
    assert [n for n in range(0, 70) if is_bounded_report_point(n)] == [1, 2, 4, 8, 16, 32, 64]
    assert not is_bounded_report_point(0) and not is_bounded_report_point(-4)
    assert is_bounded_report_point(2 ** 40) and not is_bounded_report_point(2 ** 40 + 1)
    # a million losses are reported 20 times, not a million
    assert sum(1 for n in range(1, 1_000_001) if is_bounded_report_point(n)) == 20


class _Stub(DegradedQualityLossReporting):
    quality_log_prefix = "stub"

    def __init__(self) -> None:
        self.quality_events_lost = 0
        self.quality_events_unconfirmed = 0


def test_lost_and_unconfirmed_are_counted_and_reported_on_independent_cadences(caplog):
    caplog.set_level(logging.INFO)
    stub = _Stub()
    for i in range(9):
        stub._count_quality_event_unconfirmed({"reason": f"u{i}"})
    for i in range(3):
        stub._count_quality_event_lost({"reason": f"l{i}"})

    assert (stub.quality_events_lost, stub.quality_events_unconfirmed) == (3, 9)
    unconfirmed, lost = _records(caplog, UNCONFIRMED_SUFFIX), _records(caplog, LOSS_SUFFIX)
    assert _counts(unconfirmed) == [1, 2, 4, 8] and _counts(lost) == [1, 2]
    assert all(r["event"] == "stub_quality_event_unconfirmed" for r in unconfirmed)
    assert all(r["event"] == "stub_quality_event_lost" for r in lost)
    # each line carries both totals, but only its own counter is the cadence
    assert unconfirmed[-1]["quality_events_unconfirmed"] == 8 and lost[-1]["quality_events_lost"] == 2


# ================================================ 1. bounded logs under repeated loss
@pytest.mark.parametrize("kind", KINDS)
def test_repeated_losses_log_at_powers_of_two_not_per_event(kind, tmp_path, monkeypatch, alerts, caplog):
    caplog.set_level(logging.INFO)
    app = _make(kind, tmp_path, monkeypatch)
    try:
        _degrade(app, monkeypatch)
        writes = _spy_quality_writer(app)
        for i in range(N_EVENTS):
            _emit(app, f"drop-{i}")

        loss = _records(caplog, LOSS_SUFFIX)
        assert _counts(loss) == _powers_of_two_up_to(N_EVENTS) == [1, 2, 4, 8, 16, 32, 64, 128]
        assert all(r["event"] == f"{PREFIX[kind]}{LOSS_SUFFIX}" for r in loss)
        # the report for count 2^k is the event that took the counter there: 0-based reasons 0,1,3,7,...
        assert [r["reason"] for r in loss] == ["drop-0"] + [f"drop-{c - 1}" for c in _counts(loss)[1:]]
        # the log volume is O(log N): N losses, 8 loss lines, and nothing else grew
        assert len(loss) == 8 < N_EVENTS // 10
        assert len(_error_events(caplog)) <= len(loss) + 2          # + the one latch log (+ slack for a writer log)
        assert _records(caplog, UNCONFIRMED_SUFFIX) == []
        # the unchanged contract: one alert, the failed writer never called again
        assert len(alerts) == 1 and len(_quality_alerts(alerts, app)) == 1, alerts
        assert len(writes) == 1
        assert app.quality_channel_status()["quality_events_lost"] == N_EVENTS
    finally:
        _cleanup_any(app)


@pytest.mark.parametrize("kind", KINDS)
def test_a_loss_report_is_honest_about_the_missing_wal_and_stays_bounded_in_size(kind, tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    app = _make(kind, tmp_path, monkeypatch)
    try:
        _degrade(app, monkeypatch)
        app._persist_quality_event({"event_type": "ERROR", "stream": "s", "reason": "x" * 5000})
        (record,) = _records(caplog, LOSS_SUFFIX)

        assert record["quality_wal"] is False and record["count"] == 1
        assert "no quality WAL" in record["note"] and "cannot be recovered" in record["note"]
        assert "retained" not in record["note"] and "replay" not in record["note"].lower()
        assert len(record["reason"]) == 200 and record["event_type"] == "ERROR"
    finally:
        _cleanup_any(app)


# ============================== 2. lost vs unconfirmed: distinct counters, never mislabelled
@pytest.mark.parametrize("fault", FAULTS)
@pytest.mark.parametrize("kind", KINDS)
def test_lost_and_unconfirmed_are_reported_distinctly_per_trigger_stage(kind, fault, tmp_path, monkeypatch, caplog):
    stage, durability, (trigger_lost, trigger_unconfirmed) = EXPECTED[fault]
    caplog.set_level(logging.INFO)
    app = _make(kind, tmp_path, monkeypatch)
    try:
        _Faults(monkeypatch).arm(fault, app.quality_writer)
        _emit(app, "trigger")
        for i in range(LATER):
            _emit(app, f"later-{i}")

        lost_total = trigger_lost + LATER
        status = app.quality_channel_status()
        assert (status["quality_events_lost"], status["quality_events_unconfirmed"]) == (lost_total, trigger_unconfirmed)
        assert status["quality_failure"]["stage"] == stage and status["quality_failure"]["durability"] == durability

        loss, unconfirmed = _records(caplog, LOSS_SUFFIX), _records(caplog, UNCONFIRMED_SUFFIX)
        assert _counts(loss) == _powers_of_two_up_to(lost_total)
        assert _counts(unconfirmed) == _powers_of_two_up_to(trigger_unconfirmed)
        # the trigger event is named in a report ONLY under the outcome the writer's evidence gave it
        assert ("trigger" in [r["reason"] for r in loss]) is bool(trigger_lost)
        assert ("trigger" in [r["reason"] for r in unconfirmed]) is bool(trigger_unconfirmed)
        for record in unconfirmed:
            assert record["event"] == f"{PREFIX[kind]}{UNCONFIRMED_SUFFIX}" and "lost" not in record["event"]
            assert "neither confirmed lost nor confirmed retained" in record["note"]
            assert "cannot be recovered" not in record["note"], "an unconfirmed event must not be called unrecoverable"
        for record in loss:
            assert "unconfirmed" not in record["event"]
    finally:
        _cleanup_any(app)


# ==================================================== 3. status and counters stay correct
@pytest.mark.parametrize("kind", KINDS)
def test_status_exposes_counters_and_says_there_is_no_quality_wal(kind, tmp_path, monkeypatch):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        healthy = app.quality_channel_status()
        assert healthy == {"quality_degraded": False, "quality_events_lost": 0, "quality_events_unconfirmed": 0,
                           "quality_wal": False, "quality_failure": None}

        _Faults(monkeypatch).arm("dir_fsync", app.quality_writer)
        _emit(app, "trigger")
        for i in range(LATER):
            _emit(app, f"later-{i}")
        degraded = app.quality_channel_status()

        assert set(degraded) == STATUS_KEYS
        assert degraded["quality_degraded"] is True and degraded["quality_wal"] is False
        assert (degraded["quality_events_lost"], degraded["quality_events_unconfirmed"]) == (LATER, 1)
        assert degraded["quality_failure"]["origin"] == "quality_writer"
        assert json.loads(json.dumps(degraded, default=str))["quality_wal"] is False
        assert not hasattr(app, "_quality_wal")
    finally:
        _cleanup_any(app)


@pytest.mark.parametrize("kind", KINDS)
def test_loss_logging_failure_never_breaks_counting_or_market_data(kind, tmp_path, monkeypatch):
    """A broken log sink must not raise into the quality callback, change a counter, or
    alter the single latch."""
    app = _make(kind, tmp_path, monkeypatch)
    try:
        _degrade(app, monkeypatch)
        _log_sink_that_raises(monkeypatch, LOSS_SUFFIX, UNCONFIRMED_SUFFIX)
        for i in range(40):
            _emit(app, f"drop-{i}")                       # must not raise

        assert app.quality_channel_status()["quality_events_lost"] == 40
        assert app.quality_degraded is not None and len(app.failure_policy.failed_components) == 1
        assert app.failure_policy.terminal_failure is None and app.exit_code == 0
    finally:
        _cleanup_any(app)


# ==================================================================== 4. shutdown report
@pytest.mark.parametrize("kind", KINDS)
def test_shutdown_reports_the_final_counts_once_and_still_closes_every_writer(kind, tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    app = _make(kind, tmp_path, monkeypatch)
    try:
        _Faults(monkeypatch).arm("dir_fsync", app.quality_writer)
        _emit(app, "trigger")
        for i in range(N_EVENTS - 1):
            _emit(app, f"later-{i}")
        assert _records(caplog, FINAL_SUFFIX) == [], "no final report before shutdown"

        _shutdown(app)

        (final,) = _records(caplog, FINAL_SUFFIX)
        assert final["event"] == f"{PREFIX[kind]}{FINAL_SUFFIX}"
        assert final["quality_degraded"] is True and final["quality_wal"] is False
        assert (final["quality_events_lost"], final["quality_events_unconfirmed"]) == (N_EVENTS - 1, 1)
        assert "no quality WAL" in final["note"] and "neither confirmed lost nor confirmed retained" in final["note"]
        _assert_all_closed(app)

        _shutdown(app)                                      # shutdown is repeatable; the report is not
        assert len(_records(caplog, FINAL_SUFFIX)) == 1
    finally:
        _cleanup_any(app)


@pytest.mark.parametrize("kind", KINDS)
def test_a_healthy_runner_logs_no_loss_report_at_shutdown(kind, tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    app = _make(kind, tmp_path, monkeypatch)
    try:
        _emit(app, "healthy")                               # a normal, durable event
        _shutdown(app)

        assert app.quality_channel_status()["quality_degraded"] is False
        assert _records(caplog, FINAL_SUFFIX) == [] and _records(caplog, LOSS_SUFFIX) == []
        assert _records(caplog, UNCONFIRMED_SUFFIX) == []
        _assert_all_closed(app)
    finally:
        _cleanup_any(app)


@pytest.mark.parametrize("kind", KINDS)
def test_a_failing_final_report_does_not_prevent_cleanup_or_raise(kind, tmp_path, monkeypatch):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        _degrade(app, monkeypatch)
        for i in range(10):
            _emit(app, f"drop-{i}")
        _log_sink_that_raises(monkeypatch, FINAL_SUFFIX)

        _shutdown(app)                                      # must not raise

        _assert_all_closed(app)
        assert app.exit_code == 0 and app.failure_policy.terminal_failure is None
        assert app.quality_channel_status()["quality_events_lost"] == 10
    finally:
        _cleanup_any(app)


@pytest.mark.parametrize("kind", COLLECTORS)
def test_shutdown_report_does_not_mask_a_terminal_failure_or_change_the_exit_status(kind, tmp_path, monkeypatch, caplog, alerts):
    caplog.set_level(logging.INFO)
    app = _make(kind, tmp_path, monkeypatch)
    try:
        disk = _degrade(app, monkeypatch)
        for i in range(30):
            _emit(app, f"drop-{i}")
        disk.arm(app.raw_wire_writer)

        code = _terminal_run(app, _raw_frames(kind))          # raw-evidence fatal -> supervised shutdown

        assert code == EXIT_FATAL_STORAGE == app.exit_code and app.failure_policy.terminal_failure is not None
        (final,) = _records(caplog, FINAL_SUFFIX)
        assert final["quality_degraded"] is True
        assert final["quality_events_lost"] == app.quality_events_lost >= 30
        assert final["quality_events_unconfirmed"] == app.quality_events_unconfirmed == 0
        assert len(_quality_alerts(alerts, app)) == 1
        _assert_all_closed(app)
    finally:
        _cleanup_any(app)


@pytest.mark.parametrize("kind", COLLECTORS)
def test_a_raising_final_report_still_leaves_the_terminal_exit_status_and_writers_closed(kind, tmp_path, monkeypatch):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        disk = _degrade(app, monkeypatch)
        for i in range(10):
            _emit(app, f"drop-{i}")
        disk.arm(app.raw_wire_writer)
        _log_sink_that_raises(monkeypatch, FINAL_SUFFIX, LOSS_SUFFIX)

        code = _terminal_run(app, _raw_frames(kind))

        assert code == EXIT_FATAL_STORAGE
        _assert_all_closed(app)
    finally:
        _cleanup_any(app)


def test_okx_capture_only_reports_final_counts_in_its_returned_status_and_log_without_masking_a_terminal_exit(
        tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    app = _make("okx_capture", tmp_path, monkeypatch)
    try:
        disk = _degrade(app, monkeypatch)
        for i in range(30):
            _emit(app, f"drop-{i}")
        disk.arm(app.raw_wire_writer)
        client = app.capture.client

        async def capture_run() -> None:
            client.running = True
            await client._consume(_FakeWs(_raw_frames("okx")))
            await asyncio.sleep(3600)
        app.capture.run = capture_run
        monkeypatch.setattr(rcap, "TERMINAL_STOP_GRACE_S", 1.0)

        status = asyncio.run(asyncio.wait_for(app.run(duration_s=3600.0), SUPERVISED_BOUND_S))

        assert app.exit_code == EXIT_FATAL_STORAGE and app.failure_policy.terminal_failure is not None
        assert status["quality_degraded"] is True and status["quality_wal"] is False
        (final,) = _records(caplog, FINAL_SUFFIX)
        assert final["quality_events_lost"] == status["quality_events_lost"] >= 30
        assert final["quality_events_unconfirmed"] == status["quality_events_unconfirmed"] == 0
        _assert_all_closed(app)
    finally:
        _cleanup_any(app)


# ======================================= 5. startup losses (before the policy exists) are reported too
@pytest.mark.parametrize("kind", KINDS)
def test_startup_losses_before_the_policy_exists_are_counted_and_reported_at_the_same_cadence(
        kind, tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    app, calls, emitted = _build_orphaned_with_failed_quality(kind, tmp_path, monkeypatch)
    try:
        assert emitted >= 2 and len(calls) == 1
        loss = _records(caplog, LOSS_SUFFIX)
        assert _counts(loss) == _powers_of_two_up_to(emitted)
        assert app.quality_channel_status()["quality_events_lost"] == emitted
        assert app.quality_channel_status()["quality_wal"] is False
        _shutdown(app)
        (final,) = _records(caplog, FINAL_SUFFIX)
        assert final["quality_events_lost"] == emitted == ORPHANS_PER_STREAM * (emitted // ORPHANS_PER_STREAM)
    finally:
        _cleanup_any(app)


# ======================== 6. a quality-only failure never ends otherwise healthy ingestion
@pytest.mark.parametrize("kind", KINDS)
def test_market_data_keeps_flowing_while_losses_are_reported(kind, tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    app = _make(kind, tmp_path, monkeypatch)
    try:
        client, policy = _client(app), app.failure_policy
        _degrade(app, monkeypatch)
        writes = _spy_quality_writer(app)
        _emit(app, "trigger")
        assert app.quality_degraded is not None

        async def traffic() -> None:
            await _run_frames(client, _raw_frames(_frame_kind(kind), 5), worker=False)
            for i in range(N_EVENTS):
                _emit(app, f"drop-{i}")                      # heavy quality traffic between market frames
            await _run_frames(client, _raw_frames(_frame_kind(kind), 3), worker=False)
            if kind != "okx_capture":
                client.running = True
                worker = asyncio.create_task(client._process_queue())
                await asyncio.wait_for(client._ingest_queue.join(), 10)
                client.running = False
                await asyncio.wait_for(worker, 10)
        asyncio.run(traffic())

        assert app.raw_capture.wire_captured == 8 and app.raw_wire_writer.failure_snapshot() is None
        assert client.fatal_storage_errors == 0 and client.processing_errors == 0 and not client.discard_mode
        assert policy.terminal_failure is None and app.exit_code == 0 and not policy.terminal_event.is_set()
        if kind != "okx_capture":
            assert client.frames_processed == 8 and not policy.isolated_routes
            assert app.trades_writer.has_unpublished_rows(), "the trade route must keep receiving rows"
        assert len(writes) == 1
        lost = app.quality_events_lost
        assert lost >= N_EVENTS + 1
        assert _counts(_records(caplog, LOSS_SUFFIX)) == _powers_of_two_up_to(lost)
        assert len(_records(caplog, LOSS_SUFFIX)) <= lost.bit_length()
    finally:
        _cleanup_any(app)


@pytest.mark.parametrize("kind", COLLECTORS)
def test_supervised_collector_survives_heavy_loss_and_stops_cleanly_on_a_signal(kind, tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    app = _make(kind, tmp_path, monkeypatch)
    try:
        client = app.client
        _degrade(app, monkeypatch)

        async def run() -> None:
            for i in range(N_EVENTS):
                _emit(app, f"drop-{i}")
            await _run_frames(client, _raw_frames(kind, 4))
            client.running = True
            await asyncio.sleep(3600)
        app.run = run

        async def main() -> int:
            stop = asyncio.Event()
            supervisor = asyncio.create_task(sfp.supervise_standalone_runner(app, stop, task_timeout_s=2))
            await asyncio.sleep(1.0)
            assert not supervisor.done(), "a quality-channel failure must not end the process"
            assert app.failure_policy.terminal_failure is None and app.raw_capture.wire_captured == 4
            stop.set()
            return await asyncio.wait_for(supervisor, SUPERVISED_BOUND_S)

        assert asyncio.run(main()) == 0 == app.exit_code
        (final,) = _records(caplog, FINAL_SUFFIX)
        assert final["quality_events_lost"] == app.quality_events_lost >= N_EVENTS
        _assert_all_closed(app)
    finally:
        _cleanup_any(app)


# ===================================================== 7. the three runners behave identically
def test_the_three_runners_report_identically(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    observed: dict[str, dict] = {}
    for kind in KINDS:
        caplog.clear()
        monkeypatch.undo()                                   # fresh seams for every runner
        workdir = tmp_path / kind
        workdir.mkdir()
        app = _make(kind, workdir, monkeypatch)
        try:
            _Faults(monkeypatch).arm("dir_fsync", app.quality_writer)
            _emit(app, "trigger")
            for i in range(49):
                _emit(app, f"later-{i}")
            status = app.quality_channel_status()
            _shutdown(app)
            observed[kind] = {
                "status_keys": set(status),
                "status_counts": (status["quality_degraded"], status["quality_events_lost"],
                                  status["quality_events_unconfirmed"], status["quality_wal"]),
                "loss_counts": _counts(_records(caplog, LOSS_SUFFIX)),
                "unconfirmed_counts": _counts(_records(caplog, UNCONFIRMED_SUFFIX)),
                "loss_fields": sorted(_records(caplog, LOSS_SUFFIX)[0]),
                "final_fields": sorted(_records(caplog, FINAL_SUFFIX)[0]),
                "final_counts": tuple(_records(caplog, FINAL_SUFFIX)[0][k] for k in (
                    "quality_degraded", "quality_events_lost", "quality_events_unconfirmed", "quality_wal")),
                "events": sorted({r["event"].removeprefix(PREFIX[kind]) for r in
                                  _records(caplog, LOSS_SUFFIX) + _records(caplog, UNCONFIRMED_SUFFIX)
                                  + _records(caplog, FINAL_SUFFIX)}),
            }
        finally:
            _cleanup_any(app)

    reference = observed["spot"]
    assert reference["status_keys"] == STATUS_KEYS and reference["status_counts"] == (True, 49, 1, False)
    assert reference["loss_counts"] == [1, 2, 4, 8, 16, 32] and reference["unconfirmed_counts"] == [1]
    assert reference["final_counts"] == (True, 49, 1, False)
    for kind in ("okx", "okx_capture"):
        assert observed[kind] == reference, f"{kind} diverges from spot"


# ========================================================= 8. nothing else about the topology moved
@pytest.mark.parametrize("kind", KINDS)
def test_the_one_time_latch_alert_and_no_retry_are_unchanged(kind, tmp_path, monkeypatch, alerts):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        policy = app.failure_policy
        _degrade(app, monkeypatch)
        writes = _spy_quality_writer(app)
        for i in range(N_EVENTS):
            _emit(app, f"drop-{i}")
        _shutdown(app)

        assert len(writes) == 1 and len(policy.failed_components) == 1 and policy.failure_repeats == {}
        assert len(alerts) == 1 and len(_quality_alerts(alerts, app)) == 1, alerts
        assert all(m.startswith("STORAGE FAILURE [degrade_quality]") for m in alerts), "shutdown sent an extra alert"
    finally:
        _cleanup_any(app)
