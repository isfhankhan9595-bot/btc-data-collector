"""F5 A4: quality-event counter accuracy when the quality writer fails.

Defect
------
``_persist_quality_event`` (Binance Spot, OKX, OKX capture-only) counted the event
whose ``quality_writer.write`` raised as ``quality_events_lost`` unconditionally.
That is wrong whenever the event's row had already been published: the quality
writers use one-row segments, so ``write`` appends the event, publishes the
segment (fsync + rename + directory fsync + marker), and only THEN opens the next
segment. A failure of that last step raised out of ``write`` for an event that
was durable on disk, and the runner reported it lost.

Ground truth, established from the writer and asserted here against the real
Parquet files, markers and ``.tmp`` leftovers (never from the counters alone):

  stage              the triggering event is      counter
  -----------------  ---------------------------  ---------------------------
  rename             in an unpublished .tmp       quality_events_lost
  dir_fsync          in a renamed .seg whose      quality_events_unconfirmed
                     durability is unconfirmed    (neither lost nor retained)
  open_next_segment  in a published, marked .seg  neither (it is retained)
  open_next_segment  never admitted (hour         quality_events_lost
  on rollover        rollover failed first)

Everything else is unchanged and re-asserted: the quality-failure latch (once,
one alert, origin ``quality_writer``), no retry (the failed writer is never called
again), later events only bump ``quality_events_lost``.

Faults are injected at the real seams (``os.replace``, directory ``os.fsync``,
``pyarrow.parquet.ParquetWriter``), scoped to the quality writer's directory, with
the real runners, the real ``WebSocketClient`` quality callback, the real
``StandaloneFailurePolicy`` and the real ``ParquetWriter``.
"""
from __future__ import annotations

import os
import stat
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from collector import run_okx_capture as rcap
from collector.collector import notifications
from collector.collector import parquet_writer as pw_module
from collector.collector.config import QUALITY_EVENTS_SCHEMA
from collector.collector.failure_topology import VERDICT_DEGRADE_QUALITY
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.storage_errors import (
    EVENT_LOST, EVENT_PUBLISHED, EVENT_UNCONFIRMED, FatalStorageError)
from collector.tests.test_p0_4_runner_lifecycle import _build, _publish_all

REAL_REPLACE = os.replace
REAL_FSYNC = os.fsync
REAL_PQ_WRITER = pw_module.pq.ParquetWriter
KINDS = ["spot", "okx", "okx_capture"]
LATER_EVENTS = 2
FAULTS = ["rename", "dir_fsync", "open_next_segment"]
#: (stage, durability, (quality_events_lost from the trigger, quality_events_unconfirmed))
EXPECTED = {
    "rename": ("rename", "unpublished", (1, 0)),
    "dir_fsync": ("dir_fsync", "unconfirmed", (0, 1)),
    "open_next_segment": ("open_next_segment", "published", (0, 0)),
}


@pytest.fixture(autouse=True)
def alerts():
    sent: list[str] = []
    notifications.set_notifier(notifications.CallableNotifier(lambda message: sent.append(message) or True))
    yield sent
    notifications.set_notifier(None)


# --------------------------------------------------------------------------- fault seams
class _Faults:
    """Real-seam fault injection scoped to the armed writers' directories."""

    def __init__(self, monkeypatch) -> None:
        self.rename: set[str] = set()
        self.dir_fsync: set[str] = set()
        self.open_next: set[str] = set()
        monkeypatch.setattr(pw_module.os, "replace", self._replace)
        monkeypatch.setattr(pw_module.os, "fsync", self._fsync)
        monkeypatch.setattr(pw_module.pq, "ParquetWriter", self._parquet_writer)

    def arm(self, fault: str, writer) -> None:
        # Both spellings: a runner may hold a relative base dir, while /proc/self/fd
        # (directory fsync) reports the absolute path.
        target = {"rename": self.rename, "dir_fsync": self.dir_fsync, "open_next_segment": self.open_next}[fault]
        target.add(str(writer.stream_dir))
        target.add(str(Path(writer.stream_dir).resolve()))

    @staticmethod
    def _under(path: str, prefixes: set[str]) -> bool:
        return any(path.startswith(prefix) for prefix in prefixes)

    def _replace(self, src, dst, *a, **k):
        target = str(dst)
        if target.endswith(".seg") and self._under(target, self.rename):
            raise OSError(28, "No space left on device (injected at rename)")
        return REAL_REPLACE(src, dst, *a, **k)

    def _fsync(self, fd, *a, **k):
        # Only the DIRECTORY fsync that follows the rename is failed; file fsyncs pass.
        try:
            if stat.S_ISDIR(os.fstat(fd).st_mode) and self._under(os.readlink(f"/proc/self/fd/{fd}"), self.dir_fsync):
                raise OSError(5, "Input/output error (injected at directory fsync)")
        except FileNotFoundError:  # pragma: no cover - fd vanished; not ours
            pass
        return REAL_FSYNC(fd, *a, **k)

    def _parquet_writer(self, where, *a, **k):
        if self._under(str(where), self.open_next):
            raise OSError(28, "No space left on device (injected at open of next segment)")
        return REAL_PQ_WRITER(where, *a, **k)


def _make(kind, tmp_path, monkeypatch):
    if kind == "okx_capture":
        (tmp_path / "data").mkdir(exist_ok=True)
        monkeypatch.chdir(tmp_path)
        return rcap.OKXCaptureApp(["trades"], "BTC-USDT-SWAP", str(tmp_path / "data"), "ws://unused")
    return _build(kind, tmp_path, monkeypatch)


def _client(app):
    return app.capture.client if hasattr(app, "capture") else app.client


def _emit(app, reason: str) -> None:
    """One quality event through the runner's REAL client wiring."""
    _client(app).on_quality_event("DISCONNECT", reason, "c", "stream")


# --------------------------------------------------------------------------- disk evidence
def _published_reasons(writer) -> list[str]:
    """``reason`` of every row in every PUBLISHED ``.seg`` of the quality stream."""
    reasons: list[str] = []
    for seg in sorted(Path(writer.stream_dir).rglob("*.seg")):
        reasons += [row["reason"] for row in pq.read_table(seg).to_pylist()]
    return reasons


def _tmp_reasons(writer) -> list[str]:
    """``reason`` of every row left in an UNPUBLISHED ``.seg.tmp`` (complete parquet)."""
    reasons: list[str] = []
    for tmp in sorted(Path(writer.stream_dir).rglob("*.seg.tmp")):
        try:
            reasons += [row["reason"] for row in pq.read_table(tmp).to_pylist()]
        except Exception:  # noqa: BLE001 - an unfinished tmp has no footer: no readable row
            pass
    return reasons


def _markers(writer) -> list[Path]:
    return sorted(Path(writer.stream_dir).rglob("*.meta.json"))


def _spy_writes(app) -> list[str]:
    calls: list[str] = []
    real = app.quality_writer.write

    def write(record, *a, **k):
        calls.append(record["reason"])
        return real(record, *a, **k)
    app.quality_writer.write = write
    return calls


def _spy_on_fatal(policy) -> list[BaseException]:
    seen: list[BaseException] = []
    real = policy.on_fatal

    def on_fatal(exc, *a, **k):
        seen.append(exc)
        return real(exc, *a, **k)
    policy.on_fatal = on_fatal
    return seen


def _quality_alerts(alerts, app) -> list[str]:
    return [m for m in alerts if "degrade_quality" in m and app.quality_writer.stream_name in m]


# =============================================================== 1. the three stages, all runners
@pytest.mark.parametrize("fault", FAULTS)
@pytest.mark.parametrize("kind", KINDS)
def test_trigger_event_counted_from_durability_evidence(kind, fault, tmp_path, monkeypatch, alerts):
    stage, durability, (trigger_lost, trigger_unconfirmed) = EXPECTED[fault]
    app = _make(kind, tmp_path, monkeypatch)
    try:
        writer = app.quality_writer
        faults = _Faults(monkeypatch)

        _emit(app, "healthy-before")                       # a normal, durable event first
        assert _published_reasons(writer) == ["healthy-before"] and len(_markers(writer)) == 1
        assert (app.quality_events_lost, app.quality_events_unconfirmed) == (0, 0)

        faults.arm(fault, writer)
        calls = _spy_writes(app)
        latches = _spy_on_fatal(app.failure_policy)
        _emit(app, "TRIGGER")
        for i in range(LATER_EVENTS):
            _emit(app, f"later-{i}")

        # ---- the latch: once, one alert, quality origin, writer never called again
        record = app.failure_policy.quality_degraded
        assert record is not None and app.quality_degraded is record
        assert (record.verdict, record.origin) == (VERDICT_DEGRADE_QUALITY, "quality_writer")
        assert (record.stage, record.durability) == (stage, durability)
        assert len(latches) == 1 and isinstance(latches[0], FatalStorageError)
        assert calls == ["TRIGGER"], "no retry: the failed writer must not be called for later events"
        assert len(_quality_alerts(alerts, app)) == 1
        assert app.failure_policy.terminal_failure is None

        # ---- the evidence on disk, independent of the counters
        published, in_tmp, markers = _published_reasons(writer), _tmp_reasons(writer), _markers(writer)
        assert "healthy-before" in published and not any(r.startswith("later-") for r in published + in_tmp)
        if fault == "rename":
            assert "TRIGGER" not in published and "TRIGGER" in in_tmp, "never published; left in the .tmp"
            assert len(markers) == 1, "no publication marker for the failed segment"
        elif fault == "dir_fsync":
            assert "TRIGGER" in published, "renamed into place (durability merely unconfirmed)"
            assert len(markers) == 1, "marker NOT written: nothing downstream treated it as durable"
        else:
            assert published.count("TRIGGER") == 1, "the event's row IS on disk"
            assert len(markers) == 2, "its segment was published AND marked before the next open failed"
            assert "TRIGGER" not in in_tmp

        # ---- the counters
        assert app.quality_events_lost == trigger_lost + LATER_EVENTS
        assert app.quality_events_unconfirmed == trigger_unconfirmed
        status = app.quality_channel_status()
        assert status["quality_events_lost"] == trigger_lost + LATER_EVENTS
        assert status["quality_events_unconfirmed"] == trigger_unconfirmed
        assert status["quality_failure"]["stage"] == stage

        # conservation: every event after the healthy one is on exactly one side of the ledger
        # conservation: every event after the healthy one is accounted for on exactly one side
        persisted = [r for r in published if r != "healthy-before"]
        events = 1 + LATER_EVENTS
        if fault == "rename":
            assert persisted == [] and app.quality_events_lost == events
        elif fault == "dir_fsync":
            assert persisted == ["TRIGGER"] and app.quality_events_lost + app.quality_events_unconfirmed == events
        else:
            assert persisted == ["TRIGGER"] and app.quality_events_lost == events - 1
    finally:
        _publish_all(app)
        dedup = getattr(app, "segment_dedup", None)
        if dedup is not None:
            dedup.close()


# =============================================================== 2. rollover: trigger never admitted
@pytest.mark.parametrize("kind", KINDS)
def test_open_next_segment_on_hour_rollover_is_lost_not_retained(kind, tmp_path, monkeypatch, alerts):
    """Same stage/durability as the regression case, opposite truth: the open failed on an HOUR
    ROLLOVER, before the event was admitted, so the event is in no segment and IS lost."""
    app = _make(kind, tmp_path, monkeypatch)
    try:
        writer = app.quality_writer
        faults = _Faults(monkeypatch)
        _emit(app, "healthy-before")
        faults.arm("open_next_segment", writer)
        monkeypatch.setattr(writer, "_get_current_hour_str", lambda: "2099-01-01-00")

        _emit(app, "TRIGGER")

        record = app.failure_policy.quality_degraded
        assert record is not None and (record.stage, record.durability) == ("open_next_segment", "published")
        assert _published_reasons(writer) == ["healthy-before"], "the event was never written"
        assert "TRIGGER" not in _tmp_reasons(writer)
        assert (app.quality_events_lost, app.quality_events_unconfirmed) == (1, 0)
    finally:
        _publish_all(app)
        dedup = getattr(app, "segment_dedup", None)
        if dedup is not None:
            dedup.close()


# =============================================================== 3. latched before the first event
@pytest.mark.parametrize("fault", FAULTS)
@pytest.mark.parametrize("kind", KINDS)
def test_pre_policy_path_uses_the_same_evidence(kind, fault, tmp_path, monkeypatch, alerts):
    """A quality-writer failure held while ``failure_policy`` does not exist yet is counted by the
    same rule, then latched once, with the counters unchanged by the latch itself."""
    stage, durability, (trigger_lost, trigger_unconfirmed) = EXPECTED[fault]
    app = _make(kind, tmp_path, monkeypatch)
    try:
        writer = app.quality_writer
        policy = app.failure_policy
        faults = _Faults(monkeypatch)
        faults.arm(fault, writer)
        monkeypatch.setattr(app, "failure_policy", None)

        app._persist_quality_event({"stream": "stream", "event_type": "DISCONNECT",
                                    "reason": "TRIGGER", "connection_id": "c"})

        assert app._pre_policy_quality_failure is not None
        assert (app.quality_events_lost, app.quality_events_unconfirmed) == (trigger_lost, trigger_unconfirmed)
        published = _published_reasons(writer)
        assert ("TRIGGER" in published) is (fault != "rename")

        monkeypatch.setattr(app, "failure_policy", policy)
        app._latch_pre_policy_quality_failure()
        record = policy.quality_degraded
        assert record is not None and (record.stage, record.durability) == (stage, durability)
        assert (app.quality_events_lost, app.quality_events_unconfirmed) == (trigger_lost, trigger_unconfirmed)

        calls = _spy_writes(app)
        _emit(app, "later")
        assert calls == [] and app.quality_events_lost == trigger_lost + 1
        assert len(_quality_alerts(alerts, app)) == 1
    finally:
        _publish_all(app)
        dedup = getattr(app, "segment_dedup", None)
        if dedup is not None:
            dedup.close()


# =============================================================== 4. writer-level evidence matrix
def _quality_row(reason: str) -> dict:
    return {"timestamp": 1, "exchange": "X", "stream": "s", "event_type": "DISCONNECT", "reason": reason,
            "local_ts": 1}


def _writer(tmp_path, monkeypatch) -> tuple[ParquetWriter, _Faults]:
    faults = _Faults(monkeypatch)
    writer = ParquetWriter("a4_quality", QUALITY_EVENTS_SCHEMA, base_dir=str(tmp_path), exchange="X",
                           segment_rows=1, segment_seconds=60)
    return writer, faults


def _raise_from_write(writer, reason="E") -> FatalStorageError:
    with pytest.raises(FatalStorageError) as info:
        writer.write(_quality_row(reason))
    return info.value


@pytest.mark.parametrize("fault, accepted, disposition", [
    ("rename", True, EVENT_LOST),
    ("dir_fsync", True, EVENT_UNCONFIRMED),
    ("open_next_segment", True, EVENT_PUBLISHED),
])
def test_writer_reports_whether_the_record_was_admitted(fault, accepted, disposition, tmp_path, monkeypatch):
    writer, faults = _writer(tmp_path, monkeypatch)
    try:
        faults.arm(fault, writer)
        exc = _raise_from_write(writer)
        assert exc.stage == EXPECTED[fault][0] and exc.durability == EXPECTED[fault][1]
        assert exc.record_accepted is accepted
        assert exc.trigger_event_disposition() == disposition
        # a later refusal by the already-failed writer admitted nothing, whatever the stage says
        refusal = _raise_from_write(writer, "after")
        assert refusal.record_accepted is False and refusal.trigger_event_disposition() == EVENT_LOST
        assert (refusal.stage, refusal.durability) == (exc.stage, exc.durability)
    finally:
        writer._release_lock()


def test_flush_failure_is_lost(tmp_path, monkeypatch):
    writer, _ = _writer(tmp_path, monkeypatch)
    try:
        def boom(*a, **k):
            raise OSError(28, "No space left on device (injected at flush)")
        monkeypatch.setattr(writer.writer, "write_table", boom, raising=False)
        exc = _raise_from_write(writer)
        assert exc.stage == "flush" and exc.record_accepted is True
        assert exc.trigger_event_disposition() == EVENT_LOST
    finally:
        writer._release_lock()


def test_rollover_open_failure_never_admits_the_record(tmp_path, monkeypatch):
    writer, faults = _writer(tmp_path, monkeypatch)
    try:
        faults.arm("open_next_segment", writer)
        monkeypatch.setattr(writer, "_get_current_hour_str", lambda: "2099-01-01-00")
        exc = _raise_from_write(writer)
        assert (exc.stage, exc.durability) == ("open_next_segment", "published")
        assert exc.record_accepted is False and exc.trigger_event_disposition() == EVENT_LOST
    finally:
        writer._release_lock()


def test_no_evidence_is_unconfirmed_never_a_definite_outcome():
    bare = FatalStorageError("x", stream="s", stage="open_next_segment", durability="published")
    assert bare.record_accepted is None and bare.trigger_event_disposition() == EVENT_UNCONFIRMED
    odd = FatalStorageError("x", stream="s", stage="?", durability=None)
    odd.record_accepted = True
    assert odd.trigger_event_disposition() == EVENT_UNCONFIRMED
