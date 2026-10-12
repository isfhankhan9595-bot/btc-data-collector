"""F5-QA-01: accounting of the quality event whose write/publish TRIGGERS the quality-writer fatal.

Defect
------
Futures (``CollectorApp``) and Bybit (``BybitCollectorApp``) counted the triggering event only by
its WAL evidence (``_account_degraded_quality_event``) and ignored what the writer had proven:

  * Bybit one-row segments / Futures with a full segment: the segment holding the event was
    PUBLISHED (and the WAL checkpoint already advanced past it) before opening the NEXT segment
    failed -> the event was counted ``quality_events_wal_only`` ("replayed at next start": false,
    the checkpoint covers it).
  * WAL append failed with no id, the event was published directly, opening the next segment
    failed -> counted ``quality_events_lost`` and a definite-loss "QUALITY EVENTS UNRECORDED"
    alert fired for an event that is on disk.
  * On Futures the failure surfaces from ``publish_open_segment()`` AFTER ``write()`` returned, so
    ``FatalStorageError.record_accepted`` is ``None`` there: ``trigger_event_disposition()`` alone
    would call it ``unconfirmed``. The runner passes ``record_admitted=True`` for that call.

Disposition matrix (what each case must produce). ``cov`` = durable checkpoint covers the seq.

 id  case                                   accepted durability   WAL evidence        cov  (wal_only, unconf, lost)  loss alert
 M1  published, next open fails, WAL ok     True     published    record + id         yes  (0,0,0)                   no
 M2  published, next open fails, WAL no id  True/None published    none                 -   (0,0,0)                   no
 M3a dir fsync fails, WAL ok                True     unconfirmed  record + id          no  (1,0,0)                   no
 M3b dir fsync fails, WAL no id             True/None unconfirmed  none                 -   (0,1,0)                   no
 M4a rollover rejected, WAL ok              False    published*   record + id          no  (1,0,0)                   no
 M4b rollover rejected, WAL no id           False    published*   none                 -   (0,0,1)                   ONE
 M5  later events while latched             (writer not called)   each own evidence        +1 each, exactly once     ONE (only no-WAL)
 M6a append raised after id, rename fails   True     unpublished  id, record MAY exist no  (0,1,0)                   no
 M6b append raised after id, published      True     published    id (fsync failed)   yes  (0,0,0)                   no
 M7  published, checkpoint write failed     True     published    record + id          no  (1,0,0) (awaits replay)   no
 (* durability describes the writer's PREVIOUS segment; the record itself was never admitted)

Faults are injected at the real seams (``os.replace``, directory ``os.fsync``,
``pyarrow.parquet.ParquetWriter``, the WAL's own append/fsync, ``QualityEventWAL.checkpoint``);
assertions read the actual Parquet rows, markers, WAL records and checkpoint. Nothing here is a
power-loss claim: these are injected exceptions.
"""
from __future__ import annotations

import errno
import json
import os
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from collector.collector import notifications
from collector.collector.quality_events import QualityEventType
from collector.collector.quality_wal import CHECKPOINT_FILENAME, QualityEventWAL
from collector.collector.storage_errors import (
    EVENT_LOST, EVENT_PUBLISHED, EVENT_UNCONFIRMED, FatalStorageError)
from collector.tests.test_f5_a4_counter_accuracy import (
    _Faults, _markers, _published_reasons, _spy_writes, _tmp_reasons)
import collector.run_bybit_collector as rbc
import collector.run_collector as rc
from collector.tests.test_p0_4_runner_lifecycle import _build, _publish_all

KINDS = ["usdm", "bybit"]
APP_CLASS = {"usdm": rc.CollectorApp, "bybit": rbc.BybitCollectorApp}
LOSS_ALERT = "QUALITY EVENTS UNRECORDED"


@pytest.fixture(autouse=True)
def alerts():
    sent: list[str] = []
    notifications.set_notifier(notifications.CallableNotifier(lambda message: sent.append(message) or True))
    yield sent
    notifications.set_notifier(None)


# ----------------------------------------------------------------------------- helpers
def _make(kind, tmp_path, monkeypatch):
    tmp_path.mkdir(parents=True, exist_ok=True)
    app = _build(kind, tmp_path, monkeypatch)
    # Both venues batch quality rows (500/segment, P0-3). These tests' subject is accounting at the
    # publication boundary, which fires at write() time with one-row segments, so both venues are
    # pinned to that shape. Batched behaviour: _make_batched here and test_bybit_quality_batching.py.
    app.quality_writer.segment_rows = 1
    return app


def _make_batched(kind, tmp_path, monkeypatch):
    """Venue as shipped (batched): an unprotected event reaches disk via publish_open_segment()."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    return _build(kind, tmp_path, monkeypatch)


def _event(kind, reason):
    if kind == "usdm":
        return {"exchange": "BINANCE", "stream": "orderbook", "event_type": QualityEventType.ERROR,
                "reason": reason}
    return {"stream": "bybit_orderbook", "event_type": QualityEventType.ERROR, "reason": reason}


def _trip(kind, app, reason):
    """Persist one event through the real choke point; return the raised exception (or None)."""
    try:
        app._persist_quality_event(_event(kind, reason))
    except BaseException as exc:  # noqa: BLE001 - the test inspects exactly what escaped
        return exc
    return None


def _counts(app):
    return (app._quality_events_wal_only, app._quality_events_wal_unconfirmed, app._quality_events_unrecorded)


def _loss_alerts(alerts):
    return [m for m in alerts if LOSS_ALERT in m]


def _wal_dir(app) -> Path:
    return Path(app.quality_writer.stream_dir) / "wal"


def _wal_records(app, reason):
    out = []
    for f in sorted(_wal_dir(app).glob("*.wal")):
        out.extend(json.loads(line) for line in f.read_text().splitlines() if line.strip())
    return [r for r in out if r.get("reason") == reason]


def _disk_checkpoint(app) -> int:
    path = _wal_dir(app) / CHECKPOINT_FILENAME
    return json.loads(path.read_text())["checkpointed_seq"] if path.exists() else -1


def _pending(app, reason):
    """Seqs of un-checkpointed WAL records for ``reason`` (what the next start would replay). The
    runner's own storage-failure report is a different record and is deliberately not counted here."""
    return [r["seq"] for r in QualityEventWAL.recover(_wal_dir(app)) if r.get("reason") == reason]


def _rows(app, reason):
    rows = []
    for seg in sorted(Path(app.quality_writer.stream_dir).rglob("*.seg")):
        rows += [r for r in pq.read_table(seg).to_pylist() if r["reason"] == reason]
    return rows


def _no_wal_id(app, monkeypatch):
    """WAL append raises WITHOUT assigning an id (e.g. the journal handle is unusable):
    nothing in the WAL backs the event."""
    def append(event):
        raise ValueError("I/O operation on closed file (injected: append fails before an id exists)")
    monkeypatch.setattr(app._quality_wal, "append", append)


def _wal_fsync_fails_after_id(app, monkeypatch):
    """WAL append writes + flushes, then its fsync fails: an id WAS assigned and a record MAY exist."""
    wal = app._quality_wal
    real_append = wal.append

    def append(event):
        real_fsync = os.fsync

        def fsync(fd, *a, **k):
            if fd == wal._handle.fileno():
                raise OSError(errno.EIO, "injected WAL fsync failure")
            return real_fsync(fd, *a, **k)
        with monkeypatch.context() as m:
            m.setattr(os, "fsync", fsync)
            return real_append(event)
    monkeypatch.setattr(wal, "append", append)


def _cleanup(app):
    _publish_all(app)
    dedup = getattr(app, "segment_dedup", None)
    if dedup is not None:
        dedup.close()


def _degraded(app):
    """The latched quality-channel record: Futures keeps it on the app, Bybit on its policy."""
    policy = getattr(app, "failure_policy", None)
    return policy.quality_degraded if policy is not None else app.quality_degraded


def _latched(app):
    record = _degraded(app)
    assert record is not None
    return record


def _typed_fatal(exc, stage, durability, accepted):
    assert isinstance(exc, FatalStorageError), exc
    assert (exc.stage, exc.durability, exc.record_accepted) == (stage, durability, accepted)
    assert isinstance(exc.__cause__, OSError), "the original storage cause stays chained"


# ----------------------------------------------------------------------------- the disposition API
def test_disposition_uses_caller_evidence_only_when_the_writer_gave_none():
    def fatal(stage, durability, accepted):
        exc = FatalStorageError("x", stream="s", stage=stage, durability=durability)
        exc.record_accepted = accepted
        return exc
    # write() evidence always wins over caller evidence
    assert fatal("open_next_segment", "published", False).trigger_event_disposition(record_admitted=True) == EVENT_LOST
    assert fatal("open_next_segment", "published", True).trigger_event_disposition(record_admitted=False) == EVENT_PUBLISHED
    # publish_open_segment() failure: no write() evidence; unchanged default is "unconfirmed"
    assert fatal("open_next_segment", "published", None).trigger_event_disposition() == EVENT_UNCONFIRMED
    assert fatal("open_next_segment", "published", None).trigger_event_disposition(record_admitted=True) == EVENT_PUBLISHED
    assert fatal("rename", "unpublished", None).trigger_event_disposition(record_admitted=True) == EVENT_LOST
    assert fatal("dir_fsync", "unconfirmed", None).trigger_event_disposition(record_admitted=True) == EVENT_UNCONFIRMED
    assert fatal("flush", "unpublished", None).trigger_event_disposition(record_admitted=False) == EVENT_LOST


# ----------------------------------------------------------------------------- scenario bodies
def _scenario_m1(kind, tmp_path, monkeypatch, alerts):
    """Published, next open fails, WAL record established and checkpoint-covered."""
    app = _make(kind, tmp_path, monkeypatch)
    try:
        faults = _Faults(monkeypatch)
        faults.arm("open_next_segment", app.quality_writer)
        exc = _trip(kind, app, "M1")

        _typed_fatal(exc, "open_next_segment", "published", True)
        assert exc.trigger_event_disposition() == EVENT_PUBLISHED
        (row,) = _rows(app, "M1")                                   # on disk exactly once ...
        assert len(_markers(app.quality_writer)) == 1 and "M1" not in _tmp_reasons(app.quality_writer)
        (wal_record,) = _wal_records(app, "M1")                     # ... with its WAL record ...
        assert row["quality_event_id"] == wal_record["quality_event_id"]
        assert app._quality_wal.checkpointed_seq >= wal_record["seq"]            # ... covered,
        assert _disk_checkpoint(app) >= wal_record["seq"], "covered on DISK, not just in memory"
        assert _pending(app, "M1") == [], "nothing awaits replay"
        assert _counts(app) == (0, 0, 0), "published + covered: neither WAL-only, unconfirmed nor lost"
        assert _loss_alerts(alerts) == []
        assert _latched(app).stage == "open_next_segment"
        return app
    finally:
        _cleanup(app)


def _scenario_m2(kind, tmp_path, monkeypatch, alerts, *, batched):
    """Published, next open fails, NO WAL id (append failed before assigning one)."""
    app = _make_batched(kind, tmp_path, monkeypatch) if batched else _make(kind, tmp_path, monkeypatch)
    try:
        _no_wal_id(app, monkeypatch)
        faults = _Faults(monkeypatch)
        faults.arm("open_next_segment", app.quality_writer)
        exc = _trip(kind, app, "M2")

        _typed_fatal(exc, "open_next_segment", "published", None if batched else True)
        if batched:
            # the real Futures shape: raised by publish_open_segment() AFTER write() returned, so the
            # exception carries no admission evidence and trigger_event_disposition() alone is "unconfirmed"
            assert exc.trigger_event_disposition() == EVENT_UNCONFIRMED
        (row,) = _rows(app, "M2")                                   # the row IS on disk
        assert row["quality_event_id"] is None and len(_markers(app.quality_writer)) == 1
        assert _wal_records(app, "M2") == [], "no WAL record backs this event"
        assert _counts(app) == (0, 0, 0), "an event on disk is not lost"
        assert _loss_alerts(alerts) == [], "no false definite-loss alert for a published event"
        return app
    finally:
        _cleanup(app)


# ----------------------------------------------------------------------------- M1 / M2
@pytest.mark.parametrize("kind", KINDS)
def test_M1_published_next_open_fails_wal_ok_is_not_wal_only(kind, tmp_path, monkeypatch, alerts):
    _scenario_m1(kind, tmp_path, monkeypatch, alerts)


@pytest.mark.parametrize("kind", KINDS)
def test_M2_published_next_open_fails_no_wal_id_is_not_lost(kind, tmp_path, monkeypatch, alerts):
    _scenario_m2(kind, tmp_path, monkeypatch, alerts, batched=False)


def test_M2_futures_batched_publish_open_segment_path_is_not_lost(tmp_path, monkeypatch, alerts):
    """Futures as shipped: write() buffers, publish_open_segment() publishes, then the next open fails."""
    _scenario_m2("usdm", tmp_path, monkeypatch, alerts, batched=True)


# ----------------------------------------------------------------------------- M3 dir fsync
@pytest.mark.parametrize("kind", KINDS)
def test_M3a_dir_fsync_failure_with_wal_keeps_the_wal_recovery_evidence(kind, tmp_path, monkeypatch, alerts):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        _Faults(monkeypatch).arm("dir_fsync", app.quality_writer)
        exc = _trip(kind, app, "M3a")

        _typed_fatal(exc, "dir_fsync", "unconfirmed", True)
        assert exc.trigger_event_disposition() == EVENT_UNCONFIRMED
        assert len(_rows(app, "M3a")) == 1, "renamed into place; durability merely unconfirmed"
        (wal_record,) = _wal_records(app, "M3a")
        assert app._quality_wal.checkpointed_seq < wal_record["seq"], "the durable hook never ran"
        assert _pending(app, "M3a") == [wal_record["seq"]], "the WAL record is preserved for recovery"
        assert _counts(app) == (1, 0, 0) and _loss_alerts(alerts) == []
    finally:
        _cleanup(app)


@pytest.mark.parametrize("kind", KINDS)
def test_M3b_dir_fsync_failure_without_wal_is_unconfirmed_not_lost(kind, tmp_path, monkeypatch, alerts):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        _no_wal_id(app, monkeypatch)
        _Faults(monkeypatch).arm("dir_fsync", app.quality_writer)
        exc = _trip(kind, app, "M3b")

        _typed_fatal(exc, "dir_fsync", "unconfirmed", True)
        assert len(_rows(app, "M3b")) == 1 and _wal_records(app, "M3b") == []
        assert _counts(app) == (0, 1, 0), "publication unproven: unconfirmed, never claimed lost"
        assert _loss_alerts(alerts) == [], "no definite-loss alert for an event that may well be durable"
    finally:
        _cleanup(app)


# ----------------------------------------------------------------------------- M4 rollover rejection
def _rollover_reject(kind, tmp_path, monkeypatch, *, wal_ok):
    app = _make(kind, tmp_path, monkeypatch)
    writer = app.quality_writer
    assert _trip(kind, app, "healthy-before") is None
    faults = _Faults(monkeypatch)
    faults.arm("open_next_segment", writer)
    monkeypatch.setattr(writer, "_get_current_hour_str", lambda: "2099-01-01-00")
    if not wal_ok:
        _no_wal_id(app, monkeypatch)
    return app, _trip(kind, app, "TRIGGER")


@pytest.mark.parametrize("kind", KINDS)
def test_M4a_rejected_before_admission_with_wal_is_wal_only(kind, tmp_path, monkeypatch, alerts):
    app, exc = _rollover_reject(kind, tmp_path, monkeypatch, wal_ok=True)
    try:
        _typed_fatal(exc, "open_next_segment", "published", False)
        assert exc.trigger_event_disposition() == EVENT_LOST, "never admitted: NOT published"
        assert "healthy-before" in _published_reasons(app.quality_writer)
        assert _rows(app, "TRIGGER") == [] and "TRIGGER" not in _tmp_reasons(app.quality_writer)
        (wal_record,) = _wal_records(app, "TRIGGER")
        assert _pending(app, "TRIGGER") == [wal_record["seq"]]
        assert _counts(app) == (1, 0, 0), "recoverable from the WAL, so retained there"
        assert _loss_alerts(alerts) == []
    finally:
        _cleanup(app)


@pytest.mark.parametrize("kind", KINDS)
def test_M4b_rejected_before_admission_without_wal_is_really_lost_with_one_alert(kind, tmp_path, monkeypatch, alerts):
    app, exc = _rollover_reject(kind, tmp_path, monkeypatch, wal_ok=False)
    try:
        _typed_fatal(exc, "open_next_segment", "published", False)
        assert _rows(app, "TRIGGER") == [] and _wal_records(app, "TRIGGER") == []
        assert _counts(app) == (0, 0, 1), "no WAL record and never written: genuinely lost"
        assert len(_loss_alerts(alerts)) == 1
    finally:
        _cleanup(app)


# ----------------------------------------------------------------------------- M5 later events
@pytest.mark.parametrize("kind", KINDS)
def test_M5_later_events_are_counted_once_each_and_never_reenter_the_failed_writer(kind, tmp_path, monkeypatch, alerts):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        _Faults(monkeypatch).arm("open_next_segment", app.quality_writer)
        assert isinstance(_trip(kind, app, "TRIGGER"), FatalStorageError)
        assert _counts(app) == (0, 0, 0)
        calls = _spy_writes(app)

        assert _trip(kind, app, "later-1") is None and _trip(kind, app, "later-2") is None
        assert _counts(app) == (2, 0, 0), "each later event counted exactly once, by its WAL record"
        assert calls == [], "the failed writer is never called again"
        assert len(_wal_records(app, "later-1")) == len(_wal_records(app, "later-2")) == 1

        _no_wal_id(app, monkeypatch)
        assert _trip(kind, app, "later-3") is None and _trip(kind, app, "later-4") is None
        assert _counts(app) == (2, 0, 2)
        assert len(_loss_alerts(alerts)) == 1, "ONE operator alert however many events are lost"
        assert calls == []
        assert [r for r in _published_reasons(app.quality_writer) if r.startswith("later")] == []
    finally:
        _cleanup(app)


# ----------------------------------------------------------------------------- M6 append raised after id
@pytest.mark.parametrize("kind", KINDS)
def test_M6a_wal_append_raised_after_id_and_not_published_stays_uncertain(kind, tmp_path, monkeypatch, alerts):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        _wal_fsync_fails_after_id(app, monkeypatch)
        _Faults(monkeypatch).arm("rename", app.quality_writer)
        exc = _trip(kind, app, "M6a")

        _typed_fatal(exc, "rename", "unpublished", True)
        assert exc.trigger_event_disposition() == EVENT_LOST
        assert _rows(app, "M6a") == []
        assert _counts(app) == (0, 1, 0), "a WAL record MAY exist: neither retained nor lost"
        assert _loss_alerts(alerts) == []
    finally:
        _cleanup(app)


@pytest.mark.parametrize("kind", KINDS)
def test_M6b_wal_append_raised_after_id_but_published_and_covered_is_not_uncertain(kind, tmp_path, monkeypatch, alerts):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        _wal_fsync_fails_after_id(app, monkeypatch)
        _Faults(monkeypatch).arm("open_next_segment", app.quality_writer)
        exc = _trip(kind, app, "M6b")

        _typed_fatal(exc, "open_next_segment", "published", True)
        (row,) = _rows(app, "M6b")
        assert row["quality_event_id"] is not None, "the id the failed append assigned is stamped on the row"
        assert app._quality_wal.checkpointed_seq >= int(row["quality_event_id"].rsplit("-", 1)[-1])
        assert _pending(app, "M6b") == [], "a WAL-resident copy is covered, never replayed"
        assert _counts(app) == (0, 0, 0) and _loss_alerts(alerts) == []
    finally:
        _cleanup(app)


# ----------------------------------------------------------------------------- M7 not covered
@pytest.mark.parametrize("kind", KINDS)
def test_M7_published_but_checkpoint_not_covering_it_still_awaits_replay(kind, tmp_path, monkeypatch, alerts):
    """The honesty rule in reverse: WAL-only is still correct when the checkpoint does NOT cover a
    published event, because the next start WILL replay it."""
    app = _make(kind, tmp_path, monkeypatch)
    try:
        def failing_checkpoint(self, up_to_seq):
            raise OSError(errno.EIO, "injected checkpoint write failure")
        monkeypatch.setattr(QualityEventWAL, "checkpoint", failing_checkpoint)
        _Faults(monkeypatch).arm("open_next_segment", app.quality_writer)
        exc = _trip(kind, app, "M7")

        _typed_fatal(exc, "open_next_segment", "published", True)
        assert len(_rows(app, "M7")) == 1
        (wal_record,) = _wal_records(app, "M7")
        assert app._quality_wal.checkpointed_seq < wal_record["seq"]
        assert _pending(app, "M7") == [wal_record["seq"]]
        assert _counts(app) == (1, 0, 0) and _loss_alerts(alerts) == []
    finally:
        _cleanup(app)


# ----------------------------------------------------------------------------- exception behavior
@pytest.mark.parametrize("kind", KINDS)
def test_ordinary_exception_propagates_unchanged_and_leaves_counters_alone(kind, tmp_path, monkeypatch, alerts):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        boom = OSError(errno.EIO, "ordinary, not a typed storage fatal")

        def write(*a, **k):
            raise boom
        monkeypatch.setattr(app.quality_writer, "write", write)
        exc = _trip(kind, app, "ordinary")

        assert exc is boom, "the original exception object escapes unchanged"
        assert _counts(app) == (0, 0, 0) and _loss_alerts(alerts) == []
        assert _degraded(app) is None, "an ordinary exception never degrades the channel"
    finally:
        _cleanup(app)


@pytest.mark.parametrize("kind", KINDS)
def test_accounting_failure_never_masks_the_original_storage_failure(kind, tmp_path, monkeypatch, alerts):
    app = _make(kind, tmp_path, monkeypatch)
    try:
        def broken(*a, **k):
            raise RuntimeError("accounting blew up")
        monkeypatch.setattr(app, "_account_degraded_quality_event", broken)
        monkeypatch.setattr(app, "_quality_seq_checkpoint_covered", broken)
        _Faults(monkeypatch).arm("dir_fsync", app.quality_writer)
        exc = _trip(kind, app, "masked")
        _typed_fatal(exc, "dir_fsync", "unconfirmed", True)      # still the typed storage fatal
    finally:
        _cleanup(app)


# ----------------------------------------------------------------------------- mutation probes
def _mutant_wal_only_for_published(self, exc, event, *, wal_protected, wal_seq, record_admitted=None):
    """The OLD behaviour: count the trigger by WAL evidence alone, ignoring that it was published."""
    self._account_degraded_quality_event(event, wal_protected=wal_protected, wal_seq=wal_seq)


def _mutant_loss_alert_for_published(self, exc, event, *, wal_protected, wal_seq, record_admitted=None):
    """Keeps the checkpoint-covered rule but treats a published, WAL-less event as unrecorded."""
    if exc.trigger_event_disposition(record_admitted=record_admitted) == EVENT_PUBLISHED and wal_seq is not None \
            and self._quality_seq_checkpoint_covered(wal_seq):
        return
    self._account_degraded_quality_event(event, wal_protected=wal_protected, wal_seq=wal_seq)


@pytest.mark.parametrize("kind", KINDS)
def test_mutation_wal_only_for_a_checkpoint_covered_published_event_fails_M1(kind, tmp_path, monkeypatch, alerts):
    cls = APP_CLASS[kind]
    monkeypatch.setattr(cls, "_account_tripping_quality_event", _mutant_wal_only_for_published)
    with pytest.raises(AssertionError):
        _scenario_m1(kind, tmp_path / "m1", monkeypatch, alerts)


@pytest.mark.parametrize("kind", KINDS)
def test_mutation_loss_alert_for_an_already_published_event_fails_M2(kind, tmp_path, monkeypatch, alerts):
    cls = APP_CLASS[kind]
    monkeypatch.setattr(cls, "_account_tripping_quality_event", _mutant_loss_alert_for_published)
    with pytest.raises(AssertionError):
        _scenario_m2(kind, tmp_path / "m2", monkeypatch, alerts, batched=False)


def test_mutation_loss_alert_also_fails_the_futures_batched_publish_open_segment_path(tmp_path, monkeypatch, alerts):
    cls = APP_CLASS["usdm"]
    monkeypatch.setattr(cls, "_account_tripping_quality_event", _mutant_loss_alert_for_published)
    with pytest.raises(AssertionError):
        _scenario_m2("usdm", tmp_path / "m2b", monkeypatch, alerts, batched=True)
