"""F5-QA-03: quality-WAL checkpoint-latch observability, alerting and durability-state accuracy.

Defect (reproduced before the fix, both runners)
------------------------------------------------
When the checkpoint file cannot be written after a quality segment has been PUBLISHED,
``_block_quality_checkpoint`` latched the checkpoint closed for the rest of the process and
did nothing else but write one log line. No operator alert was sent, Futures had no status
method at all, Bybit's status carried a bare boolean without the reason, and a throwing
logger escaped ``_block_quality_checkpoint``. Meanwhile later quality events kept being
appended to the WAL and published to Parquet, with every WAL record staying uncheckpointed.

Contract under test (the existing at-least-once contract is NOT changed)
------------------------------------------------------------------------
* The latch is set first and never undone by reporting; its first reason is preserved.
* The blocked state and reason are visible immediately through ``quality_channel_status()``,
  distinct from "the quality writer failed", "append unconfirmed" and "unrecorded/lost".
* ONE operator alert on the first transition; no alert per later event.
* A failing logger / alert backend cannot clear the latch, mask the reason, or raise.
* Published rows are never counted lost because a checkpoint failed; nothing is deduplicated;
  uncheckpointed WAL records are never deleted; restart replays them (duplicates allowed,
  identical ``quality_event_id``). Market-data routes are never terminated by this latch.

Everything runs through the real runner paths and the real WAL / ``ParquetWriter``; the only
injected fault is ``os.replace`` of ``checkpoint.json`` after the segment is already durable.
This is syscall-level fault injection: it is NOT evidence of real power-loss, real disk-full
or production-systemd behaviour.
"""
from __future__ import annotations

import asyncio
import json
import os
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq
import pytest

import collector.run_bybit_collector as rbc
import collector.run_collector as rc
from collector.collector import notifications
from collector.collector.book_engine import LocalBook
from collector.collector.config import QUALITY_EVENTS_SCHEMA
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import QualityEventType
from collector.collector.quality_wal import CHECKPOINT_FILENAME, QualityEventWAL
from collector.run_bybit_collector import BybitCollectorApp
from collector.tests._bybit_quality_fixtures import one_row_bybit_quality_segments
from collector.tests.test_bybit_quality_wal import _crash as _bybit_crash
from collector.tests.test_quality_wal_collector_integration import _minimal_app

CollectorApp = rc.CollectorApp
ALERT_MARK = "QUALITY WAL CHECKPOINT BLOCKED"
CKPT_REASON = "checkpoint_write_failed:OSError"


# ------------------------------------------------------------------ alert seam (the repo's own)
@pytest.fixture(autouse=True)
def alerts():
    sent: list[str] = []
    notifications.set_notifier(notifications.CallableNotifier(lambda message: sent.append(message) or True))
    yield sent
    notifications.set_notifier(None)


# ------------------------------------------------------------------ one rig per runner
class Rig:
    """Uniform handle over a REAL runner instance. ``emit`` drives the runner's own quality path."""

    def __init__(self, kind: str, root: Path, monkeypatch):
        self.kind, self.root, self.mp = kind, root, monkeypatch
        self._apps: list = []
        self.armed = False
        self.mod = rbc if kind == "bybit" else rc
        self.seg_dir = root / "raw" / ("bybit_quality_events" if kind == "bybit" else "quality_events")
        self.wal_dir = self.seg_dir / "wal"
        self.app = self._new_app()
        real = os.replace

        def replace(src, dst, *a, **kw):                # fail ONLY the checkpoint-file replace
            if self.armed and str(src).endswith("checkpoint.json.tmp"):
                raise OSError(28, "No space left on device (injected checkpoint replace)")
            return real(src, dst, *a, **kw)
        monkeypatch.setattr(os, "replace", replace)

    # -- construction / restart
    def _new_app(self):
        if self.kind == "bybit":
            # real constructor + real recovery; P0-3: one-row segments set BEFORE construction so they
            # govern replay too (Bybit batches in production; see test_bybit_quality_batching.py)
            with one_row_bybit_quality_segments():
                app = BybitCollectorApp(data_dir=str(self.root))
        else:
            app = _minimal_app(self.root)
        self._apps.append(app)
        return app

    def restart(self) -> None:
        """Process death (nothing published/checkpointed on the way out), then a real startup recovery."""
        self._crash(self.app)
        if self.kind == "bybit":
            self.app = self._new_app()
            return
        fresh = CollectorApp.__new__(CollectorApp)
        fresh.binance_book = LocalBook("BINANCE")
        fresh._init_quality_durability_state()
        fresh.quality_writer = ParquetWriter("quality_events", QUALITY_EVENTS_SCHEMA, base_dir=str(self.root),
                                             segment_rows=1, segment_seconds=1,
                                             on_segment_durable=fresh._on_quality_segment_durable)
        fresh._quality_queue = asyncio.Queue(maxsize=1024)
        fresh._quality_overflow = 0
        fresh._recover_quality_wal(self.wal_dir)                    # the real startup path
        self._apps.append(fresh)
        self.app = fresh

    def _crash(self, app) -> None:
        if self.kind == "bybit":
            _bybit_crash(app)
            return
        try:
            app.quality_writer.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            app._quality_wal._handle.close()
        except Exception:  # noqa: BLE001
            pass

    def release(self) -> None:
        for app in self._apps:
            try:
                self._crash(app)
            except Exception:  # noqa: BLE001
                pass

    # -- the runner's own quality path
    def emit(self, reason: str) -> None:
        if self.kind == "bybit":
            self.app._on_client_quality_event("DISCONNECT", reason, "bybit-1", "bybit")
        else:
            self.app._websocket_quality_event(QualityEventType.DISCONNECT, reason)
            # At the hard WAL cap the runner skips the RAM queue and directly
            # force-publishes the event, so an empty queue is the expected
            # durable-degraded path rather than an event loss.
            try:
                event = self.app._quality_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            self.app._persist_quality_event(event)
            self.app._quality_queue.task_done()

    def emit_many(self, prefix: str, n: int) -> None:
        for i in range(n):
            self.emit(f"{prefix}{i}")

    # -- evidence
    def status(self) -> dict:
        return self.app.quality_channel_status()

    def rows(self) -> list[dict]:
        out: list[dict] = []
        for f in sorted(self.seg_dir.glob("*.seg")):
            out.extend(pq.read_table(f).to_pylist())
        return out

    def wal_files(self) -> list[Path]:
        return sorted(self.wal_dir.glob("*.wal"))

    def wal_records(self) -> list[dict]:
        out: list[dict] = []
        for f in self.wal_files():
            out.extend(json.loads(line) for line in f.read_text().splitlines() if line.strip())
        return out

    def wal_bytes(self) -> int:
        return sum(f.stat().st_size for f in self.wal_files())

    def disk_ckpt(self) -> int:
        path = self.wal_dir / CHECKPOINT_FILENAME
        return json.loads(path.read_text())["checkpointed_seq"] if path.exists() else -1

    def pending(self) -> list[dict]:
        return QualityEventWAL.recover(self.wal_dir)

    def policy(self):
        return getattr(self.app, "failure_policy", self.app)

    def assert_market_data_untouched(self) -> None:
        assert self.app.exit_code == 0, "a checkpoint latch must never request process termination"
        policy = self.policy()
        assert policy.terminal_failure is None and not policy.isolated_routes, \
            "a checkpoint latch must never terminate or isolate a market-data route"


@pytest.fixture(params=["futures", "bybit"])
def rig(request, tmp_path, monkeypatch):
    r = Rig(request.param, tmp_path, monkeypatch)
    yield r
    r.release()


def _checkpoint_alerts(alerts) -> list[str]:
    return [m for m in alerts if ALERT_MARK in m]


class _BoomLogger:
    """A logger whose error() raises: logging must never be a precondition for the latch."""

    def error(self, *a, **k):
        raise RuntimeError("logger exploded")

    def warning(self, *a, **k):
        pass

    def info(self, *a, **k):
        pass

    def debug(self, *a, **k):
        pass


# ======================= 1-3. checkpoint failure after a SUCCESSFUL publication =================
def test_checkpoint_failure_after_publication_latches_with_reason_and_is_visible_immediately(rig):
    rig.emit("healthy_1")
    healthy_ckpt = rig.disk_ckpt()
    assert healthy_ckpt >= 0 and len(rig.rows()) == 1
    assert rig.status()["quality_checkpoint_blocked"] is False
    assert rig.status()["quality_checkpoint_block_reason"] is None

    rig.armed = True
    rig.emit("trigger_ckpt_fail")                       # segment publishes, THEN the checkpoint write fails

    # the row was published: a failed checkpoint is not a failed publication
    assert [r["reason"] for r in rig.rows()] == ["healthy_1", "trigger_ckpt_fail"]
    # latch + reason, recorded by the runner itself
    assert rig.app._quality_checkpoint_blocked is True
    assert rig.app._quality_checkpoint_block_reason == CKPT_REASON
    assert rig.disk_ckpt() == healthy_ckpt, "the failed checkpoint must not have advanced on disk"
    # visible IMMEDIATELY (no further event needed), and distinct from the other conditions
    status = rig.status()
    assert status["quality_checkpoint_blocked"] is True
    assert status["quality_checkpoint_block_reason"] == CKPT_REASON
    assert status["quality_degraded"] is False and status["quality_failure"] is None, \
        "the quality WRITER has not failed; only the WAL checkpoint is blocked"
    assert (status["quality_events_wal_only"], status["quality_events_wal_unconfirmed"],
            status["quality_events_lost"]) == (0, 0, 0), "a blocked checkpoint must not be reported as loss"
    rig.assert_market_data_untouched()


def test_status_shape_is_identical_across_runners(tmp_path, monkeypatch):
    futures = Rig("futures", tmp_path / "f", monkeypatch)
    bybit = Rig("bybit", tmp_path / "b", monkeypatch)
    try:
        assert set(futures.status()) == set(bybit.status())
        assert "quality_checkpoint_block_reason" in futures.status()
    finally:
        futures.release()
        bybit.release()


# ======================= 4-5. exactly one alert, no storm ======================================
def test_exactly_one_actionable_alert_on_first_transition_and_none_per_later_event(rig, alerts):
    rig.emit("healthy_1")
    assert alerts == [], "healthy operation must not alert"
    rig.armed = True
    rig.emit("trigger_ckpt_fail")

    assert len(alerts) == 1 and len(_checkpoint_alerts(alerts)) == 1, alerts      # raised immediately
    text = alerts[0]
    assert f"[{rig.kind.upper()}]" in text and CKPT_REASON in text
    assert "blocked for the rest of this process" in text
    assert "keep publishing" in text and "uncheckpointed" in text           # events may continue to publish
    assert "keep growing" in text                                           # the WAL can continue growing
    assert "restart" in text and "inspect" in text.lower()                  # restart / inspection may be needed
    assert "duplicate" in text and "quality_event_id" in text               # at-least-once, said honestly

    rig.emit_many("later_", 40)                                              # a flood of later events
    assert len(alerts) == 1, "an alert per event is an alert storm"
    rig.app._block_quality_checkpoint("some_other_reason")                   # re-entering the latch is idempotent
    assert len(alerts) == 1
    assert rig.app._quality_checkpoint_block_reason == CKPT_REASON, "the FIRST reason is the one preserved"


# ======================= 6. a failing logger / alert backend cannot disturb the latch =============
@pytest.mark.parametrize("failing", ["alert", "logger", "both"])
def test_failing_alert_or_logger_cannot_clear_the_latch_or_mask_the_original_failure(rig, failing, monkeypatch):
    attempts: list[str] = []
    if failing in ("alert", "both"):
        def exploding_alert(message):
            attempts.append(message)
            raise RuntimeError("alert backend exploded")
        monkeypatch.setattr(rig.mod, "send_telegram_alert", exploding_alert)
    if failing in ("logger", "both"):
        monkeypatch.setattr(rig.mod, "logger", _BoomLogger())

    rig.emit("healthy_1")
    rig.armed = True
    rig.emit("trigger_ckpt_fail")                                  # must not raise out of the quality path

    assert rig.app._quality_checkpoint_blocked is True
    assert rig.app._quality_checkpoint_block_reason == CKPT_REASON, "the reporting failure masked the cause"
    assert rig.status()["quality_checkpoint_block_reason"] == CKPT_REASON
    assert [r["reason"] for r in rig.rows()] == ["healthy_1", "trigger_ckpt_fail"]
    if failing in ("alert", "both"):
        assert len(attempts) == 1, "the alert is attempted once, never retried per event"
    rig.emit_many("later_", 10)                                   # the channel keeps working afterwards
    assert rig.app._quality_checkpoint_blocked is True and len(rig.rows()) == 12
    if failing in ("alert", "both"):
        assert len(attempts) == 1

    # and the method itself is non-raising, whatever the logger does (several callers are non-raising)
    rig.app._block_quality_checkpoint("direct_call")
    assert rig.app._quality_checkpoint_block_reason == CKPT_REASON
    rig.assert_market_data_untouched()


def test_an_alert_backend_that_raises_through_the_notifier_seam_is_also_contained(rig):
    def bad(message):
        raise RuntimeError("notifier exploded")
    notifications.set_notifier(notifications.CallableNotifier(bad))
    rig.armed = True
    rig.emit("trigger_ckpt_fail")
    assert rig.app._quality_checkpoint_blocked is True
    assert rig.status()["quality_checkpoint_block_reason"] == CKPT_REASON


# ======================= 7-8. accounting while checkpointing stays blocked ======================
def test_published_events_wal_records_seqs_and_ids_agree_while_blocked(rig):
    rig.emit("healthy_1")
    rig.armed = True
    rig.emit("trigger_ckpt_fail")
    rig.emit_many("later_", 7)
    total = 9

    rows, wal = rig.rows(), rig.wal_records()
    assert len(rows) == total and len(wal) == total, "every event is published AND still in the WAL"
    seqs = [r["seq"] for r in wal]
    assert seqs == list(range(seqs[0], seqs[0] + total)), "WAL seq numbers are contiguous and unchanged"
    row_ids, wal_ids = sorted(r["quality_event_id"] for r in rows), sorted(r["quality_event_id"] for r in wal)
    assert None not in row_ids and row_ids == wal_ids, "published ids and WAL ids must agree one-to-one"
    assert [r["reason"] for r in rows] == [r["reason"] for r in wal]
    # counters stay truthful: nothing published is "lost", nothing here is unconfirmed
    status = rig.status()
    assert (status["quality_events_wal_only"], status["quality_events_wal_unconfirmed"],
            status["quality_events_lost"]) == (0, 0, 0)
    assert status["quality_degraded"] is False
    # the durable checkpoint did not move past the first (healthy) event
    assert rig.disk_ckpt() == seqs[0]
    assert len(rig.pending()) == total - 1
    rig.assert_market_data_untouched()


def test_the_latch_is_never_cleared_by_a_later_transient_success(rig):
    rig.armed = True
    rig.emit("trigger_ckpt_fail")
    ckpt = rig.disk_ckpt()
    rig.armed = False                                     # the disk "recovers"
    rig.emit_many("after_recovery_", 5)
    assert rig.app._quality_checkpoint_blocked is True, "a transient success must not silently clear the latch"
    assert rig.status()["quality_checkpoint_block_reason"] == CKPT_REASON
    assert rig.disk_ckpt() == ckpt, "no checkpoint may advance past records whose coverage the process lost track of"


# ======================= WAL growth while blocked (deterministic probe) =========================
def test_wal_keeps_every_uncheckpointed_record_and_only_grows_while_blocked(rig):
    """Rotation of FILES is not a total-size cap, and this does not assert whether rotation happens.
    It pins what must never happen: deletion or loss of uncheckpointed evidence."""
    rig.app._quality_wal.max_bytes = 800                  # tiny, so size-based rotation WOULD be eligible
    rig.armed = True
    rig.emit("trigger_ckpt_fail")
    files_before, sizes = set(rig.wal_files()), []
    for _ in range(6):
        rig.emit_many("grow_", 10)
        sizes.append(rig.wal_bytes())
    assert sizes == sorted(set(sizes)), f"WAL size must grow monotonically while blocked: {sizes}"
    assert files_before <= set(rig.wal_files()), "an uncheckpointed WAL file was deleted"
    assert len(rig.wal_records()) == 61 and len(rig.pending()) == 61, "no uncheckpointed record may be dropped"
    assert len(rig.rows()) == 61 and rig.status()["quality_events_lost"] == 0
    assert rig.disk_ckpt() == -1, "no checkpoint was ever durable in this scenario"


# ======================= 9. restart replays uncheckpointed records (at-least-once) ==============
def test_restart_replays_uncheckpointed_records_with_matching_ids_and_no_deduplication(rig, alerts):
    rig.emit("healthy_1")
    rig.armed = True
    rig.emit_many("blocked_", 4)
    published_before = Counter(r["quality_event_id"] for r in rig.rows())
    wal_ids = [r["quality_event_id"] for r in rig.wal_records()]
    pending_ids = [r["quality_event_id"] for r in rig.pending()]
    assert len(pending_ids) == 4 and len(alerts) == 1
    top_seq = max(r["seq"] for r in rig.wal_records())

    rig.armed = False                                      # a new process starts on a healthy disk
    rig.restart()

    after = Counter(r["quality_event_id"] for r in rig.rows())
    assert set(wal_ids) <= set(after), "no event may disappear across a restart"
    for event_id in wal_ids:
        assert after[event_id] >= 1
    for event_id in pending_ids:                            # replayed: a duplicate row is PERMITTED and carries the same id
        assert after[event_id] >= published_before[event_id]
    healthy_id = next(i for i in wal_ids if i not in pending_ids)
    assert after[healthy_id] == 1, "an already-checkpointed event must not be replayed"
    assert sum(after.values()) >= len(wal_ids), "the collector never deduplicates (and never needs exactly-once)"
    # recovery fully checkpoints what it replayed (a real startup may also emit its own quality events,
    # e.g. crashed-segment reports, which take later seqs -- hence >=); the new process is healthy and silent
    assert rig.pending() == [] and rig.disk_ckpt() >= top_seq
    status = rig.status()
    assert status["quality_checkpoint_blocked"] is False and status["quality_checkpoint_block_reason"] is None
    assert len(alerts) == 1, "a healthy restart must not alert"
    # seq numbers are never reused after the restart
    rig.emit("post_restart")
    assert max(r["seq"] for r in rig.wal_records()) > top_seq


# ======================= 10. healthy operation is not reported as degraded =======================
def test_healthy_checkpointing_never_reports_degradation(rig, alerts):
    rig.emit_many("healthy_", 30)
    status = rig.status()
    assert status["quality_checkpoint_blocked"] is False and status["quality_checkpoint_block_reason"] is None
    assert status["quality_degraded"] is False and status["quality_failure"] is None
    assert (status["quality_events_wal_only"], status["quality_events_wal_unconfirmed"],
            status["quality_events_lost"]) == (0, 0, 0)
    assert alerts == []
    assert rig.disk_ckpt() == max(r["seq"] for r in rig.wal_records()) and rig.pending() == []
    assert len(rig.rows()) == 30
    rig.assert_market_data_untouched()


# ======================= startup corruption: the other way into the blocked state =================
def test_startup_wal_corruption_is_reported_with_one_alert_and_its_reason(tmp_path, monkeypatch, alerts):
    r = Rig("bybit", tmp_path / "b", monkeypatch)
    try:
        r.app._persist_quality_event({"stream": "bybit_orderbook", "event_type": QualityEventType.ERROR, "reason": "c1"})
        r.app._persist_quality_event({"stream": "bybit_orderbook", "event_type": QualityEventType.ERROR, "reason": "c2"})
        r._crash(r.app)
        wal_file = r.wal_files()[0]
        lines = wal_file.read_text().splitlines()
        lines[0] = "{not json"                                   # mid-file corruption, not a torn tail
        wal_file.write_text("\n".join(lines) + "\n")
        assert alerts == []

        r.app = r._new_app()                                       # real startup: must still start
        assert r.status()["quality_checkpoint_blocked"] is True
        assert r.status()["quality_checkpoint_block_reason"] == "wal_corruption_on_startup"
        assert len(_checkpoint_alerts(alerts)) == 1 and "wal_corruption_on_startup" in alerts[0]
        assert "{not json" in wal_file.read_text(), "the corrupt evidence must be preserved"
        r.app._persist_quality_event({"stream": "bybit_orderbook", "event_type": QualityEventType.ERROR, "reason": "c3"})
        assert len(_checkpoint_alerts(alerts)) == 1
    finally:
        r.release()


def test_futures_startup_wal_corruption_is_reported_with_one_alert_and_its_reason(tmp_path, monkeypatch, alerts):
    r = Rig("futures", tmp_path, monkeypatch)
    try:
        for i in range(3):
            r.app._websocket_quality_event(QualityEventType.CONNECT, f"c{i}")
        r.app._quality_wal.close()
        corrupt = r.wal_files()[0]
        lines = corrupt.read_text().splitlines()
        lines.insert(1, "{garbage")
        corrupt.write_text("\n".join(lines) + "\n")
        assert alerts == []

        fresh = CollectorApp.__new__(CollectorApp)
        fresh.binance_book = LocalBook("BINANCE")
        fresh.quality_writer = r.app.quality_writer
        fresh._quality_queue = asyncio.Queue(maxsize=8)
        fresh._quality_overflow = 0
        fresh._recover_quality_wal(r.wal_dir)                     # the real startup path
        status = fresh.quality_channel_status()
        assert status["quality_checkpoint_blocked"] is True
        assert status["quality_checkpoint_block_reason"] == "wal_corruption_on_startup"
        assert len(_checkpoint_alerts(alerts)) == 1 and "wal_corruption_on_startup" in alerts[0]
        assert "{garbage" in corrupt.read_text(), "the corrupt evidence must be preserved"
    finally:
        r.release()
