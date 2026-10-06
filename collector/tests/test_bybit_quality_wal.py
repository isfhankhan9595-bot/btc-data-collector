"""Bybit quality-event WAL durability (parity with PR #94 / run_collector.CollectorApp).

Contract under test -- the existing quality-WAL contract, not a new one:

    If WAL checkpoint N is on disk, every quality event with WAL seq <= N is
    durably present in a PUBLISHED ``bybit_quality_events`` segment.

Everything runs against the real ``BybitCollectorApp`` constructor (real WAL,
real ``_recover_quality_wal``, real writers, real ``WebSocketClient._consume``
for the frame-driven cases), with failures injected at the exact boundary
(``os.fsync`` on the WAL's own file descriptor, ``os.replace`` of a segment,
``ParquetWriter.write``). A "crash" abandons every writer and the WAL handle
without publishing or checkpointing anything.
"""
from __future__ import annotations

import asyncio
import json
import os
import time

import pyarrow.parquet as pq
import pytest

from collector.collector.parquet_writer import ParquetWriter
from collector.collector.quality_events import QualityEventType
from collector.collector.quality_wal import CHECKPOINT_FILENAME, QualityEventWAL
from collector.run_bybit_collector import BybitCollectorApp
import collector.run_bybit_collector as rbc


# ----------------------------------------------------------------- helpers
def _qdir(root):
    return root / "raw" / "bybit_quality_events"


def _wal_dir(root):
    return _qdir(root) / "wal"


def _rows(root):
    rows = []
    for f in sorted(_qdir(root).glob("*.seg")):
        rows.extend(pq.read_table(f).to_pylist())
    return rows


def _rows_for(root, reason):
    return [r for r in _rows(root) if r["reason"] == reason]


def _wal_records(root):
    """Every record ever appended (checkpointed or not), read raw off disk."""
    out = []
    for f in sorted(_wal_dir(root).glob("*.wal")):
        out.extend(json.loads(line) for line in f.read_text().splitlines() if line.strip())
    return out


def _wal_for(root, reason):
    return [r for r in _wal_records(root) if r.get("reason") == reason]


def _disk_ckpt(root) -> int:
    path = _wal_dir(root) / CHECKPOINT_FILENAME
    return json.loads(path.read_text())["checkpointed_seq"] if path.exists() else -1


def _pending(root):
    return QualityEventWAL.recover(_wal_dir(root))


def _ms(dt) -> int:
    return round(dt.timestamp() * 1000)


def _app(root):
    return BybitCollectorApp(data_dir=str(root))


def _crash(app):
    """Process death: nothing is published, nothing is checkpointed, locks and
    handles vanish. Unpublished .seg.tmp files stay on disk, exactly as after
    a kill -9."""
    for obj in list(vars(app).values()):
        if isinstance(obj, ParquetWriter):
            try:
                if obj.writer is not None:
                    obj.writer.close()
            except Exception:  # noqa: BLE001
                pass
            obj.writer = None
            obj._closed = True
            obj._release_lock()
    dedup = getattr(app, "segment_dedup", None)
    if dedup is not None:
        try:
            dedup.close()
        except Exception:  # noqa: BLE001
            pass
    wal = getattr(app, "_quality_wal", None)
    if wal is not None:
        try:
            wal._handle.close()
        except Exception:  # noqa: BLE001
            pass


def _ev(reason, **extra):
    return {"stream": "bybit_orderbook", "event_type": QualityEventType.ERROR,
            "reason": reason, **extra}


class _FakeSocket:
    def __init__(self, frames):
        self.frames = frames

    def __aiter__(self):
        async def gen():
            for frame in self.frames:
                yield frame
        return gen()


def _drive(app, frames):
    """Run frames through the real WebSocketClient._consume() + _process_queue()."""
    app.client.running = True

    async def _run():
        await app.client._consume(_FakeSocket(frames))
        app.client.running = False
        await app.client._process_queue()
    asyncio.run(_run())


class _CapturingLogger:
    def __init__(self):
        self.errors = []

    def error(self, event, **kw):
        self.errors.append((event, kw))

    def warning(self, *a, **k):
        pass

    def info(self, *a, **k):
        pass

    def debug(self, *a, **k):
        pass


class _Unhandled:
    """Stand-in for an adapter's unhandled/duplicate message (the only thing
    _record_adapter_unhandled touches is ``to_quality_event``)."""

    def __init__(self, reason):
        self._reason = reason

    def to_quality_event(self):
        return {"stream": "bybit", "event_type": "DUPLICATE", "reason": self._reason,
                "local_ts": int(time.time() * 1000)}


def _fail_wal_fsync(monkeypatch, app):
    """fsync failure on the WAL's own fd only (bytes already written+flushed),
    the case the WAL itself documents. Parquet/checkpoint fsyncs are untouched."""
    wal_fd = app._quality_wal._handle.fileno()
    real = os.fsync

    def fsync(fd):
        if fd == wal_fd:
            raise OSError(5, "injected WAL fsync failure")
        return real(fd)
    monkeypatch.setattr(os, "fsync", fsync)


def _fail_segment_publish(monkeypatch):
    real = os.replace

    def replace(src, dst, *a, **kw):
        if str(src).endswith(".seg.tmp"):
            raise OSError("disk full (injected)")
        return real(src, dst, *a, **kw)
    monkeypatch.setattr(os, "replace", replace)


def _fail_quality_write(monkeypatch, app):
    def boom(row, **kw):
        raise OSError("disk full (injected)")
    monkeypatch.setattr(app.quality_writer, "write", boom)


# ======================= A. normal quality event ===========================
def test_A_websocket_event_is_appended_published_and_checkpointed_under_one_id(tmp_path):
    app = _app(tmp_path)
    app._on_client_quality_event("DISCONNECT", "t_a_disconnect", "bybit-1", "bybit")

    (rec,) = _wal_for(tmp_path, "t_a_disconnect")          # reached the WAL
    (row,) = _rows_for(tmp_path, "t_a_disconnect")         # reached Parquet
    assert row["quality_event_id"] == rec["quality_event_id"]
    assert (row["exchange"], row["stream"], row["event_type"], row["connection_id"]) == \
        ("BYBIT", "bybit", "DISCONNECT", "bybit-1")
    assert _disk_ckpt(tmp_path) == rec["seq"]              # checkpoint only after publication
    assert _pending(tmp_path) == []
    assert app._quality_wal_inflight == set() and not app._quality_checkpoint_blocked
    _crash(app)


def test_A_book_transition_generated_by_real_frames_reaches_wal_and_parquet(tmp_path):
    app = _app(tmp_path)
    now = int(time.time() * 1000)
    snapshot = json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": now,
                           "data": {"s": "BTCUSDT", "b": [["50000", "1.0"]], "a": [["50001", "1.0"]],
                                    "u": 10, "seq": 100}})
    decreased = json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": now + 10,
                            "data": {"s": "BTCUSDT", "b": [["50000", "9.0"]], "a": [],
                                     "u": 3, "seq": 104}})
    _drive(app, [snapshot, decreased])

    gaps = [r for r in _rows(tmp_path) if r["stream"] == "bybit_orderbook"
            and r["event_type"] == QualityEventType.SEQUENCE_GAP.value]
    assert gaps, "the decreasing update_id must be generated as a quality event"
    for row in gaps:
        assert row["quality_event_id"], "every Parquet quality row must carry its WAL id"
        wal_match = [r for r in _wal_records(tmp_path) if r["quality_event_id"] == row["quality_event_id"]]
        assert len(wal_match) == 1 and wal_match[0]["stream"] == "bybit_orderbook"
        assert wal_match[0]["event_type"] == "SEQUENCE_GAP"
    assert _disk_ckpt(tmp_path) == max(r["seq"] for r in _wal_records(tmp_path))
    _crash(app)


def test_A_event_timestamps_and_provenance_are_preserved_not_rewritten(tmp_path):
    app = _app(tmp_path)
    ts = 1_700_000_000_123
    app._persist_quality_event({
        "stream": "bybit_orderbook", "event_type": QualityEventType.SEQUENCE_GAP, "reason": "t_prov",
        "local_ts": ts, "local_receive_ts": ts - 23, "update_id": 7, "previous_update_id": 5,
        "new_state": "SEQUENCE_GAP", "previous_state": "VALID", "connection_id": "bybit-9"})

    (rec,) = _wal_for(tmp_path, "t_prov")
    (row,) = _rows_for(tmp_path, "t_prov")
    assert _ms(row["timestamp"]) == ts == rec["local_ts"]
    assert _ms(row["local_receive_ts"]) == ts - 23
    assert (row["update_id"], row["previous_update_id"], row["connection_id"]) == (7, 5, "bybit-9")
    assert (row["previous_state"], row["new_state"]) == ("VALID", "SEQUENCE_GAP")
    assert row["event_type"] == rec["event_type"] == "SEQUENCE_GAP"       # enum -> value, once
    assert _ms(row["local_process_ts"]) == rec["local_process_ts"]         # one instant, stamped once
    _crash(app)


# ======================= B. multiple quality events ========================
def test_B_every_entry_point_reaches_the_wal_exactly_once_with_no_loss_or_duplicates(tmp_path):
    app = _app(tmp_path)
    app._on_client_quality_event("CONNECT", "t_b_ws1", "bybit-1", "bybit")
    app._persist_quality_event(_ev("t_b_direct1"))
    app._record_adapter_unhandled(_Unhandled("t_b_unhandled"))
    app.raw_wire_writer._emit_quality("ERROR", "t_b_writer_sink")          # a writer's own self-report
    app._on_client_quality_event("DISCONNECT", "t_b_ws2", "bybit-1", "bybit")
    app._persist_quality_event(_ev("t_b_direct2"))

    expected = ["t_b_ws1", "t_b_direct1", "t_b_unhandled", "t_b_writer_sink", "t_b_ws2", "t_b_direct2"]
    wal = [r for r in _wal_records(tmp_path) if r.get("reason") in expected]
    rows = [r for r in _rows(tmp_path) if r["reason"] in expected]
    assert [r["reason"] for r in wal] == expected                           # all, once, in order
    assert sorted(r["reason"] for r in rows) == sorted(expected)
    assert {r["quality_event_id"] for r in rows} == {r["quality_event_id"] for r in wal}
    assert len({r["quality_event_id"] for r in rows}) == len(expected)
    seqs = [r["seq"] for r in wal]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)
    assert _disk_ckpt(tmp_path) == max(r["seq"] for r in _wal_records(tmp_path))
    assert _pending(tmp_path) == [] and app._quality_wal_inflight == set()
    _crash(app)


# ======================= C. WAL / persistence failure ======================
def test_C_wal_append_failure_still_persists_the_event_with_the_same_id_lineage(tmp_path, monkeypatch):
    app = _app(tmp_path)
    _fail_wal_fsync(monkeypatch, app)
    app._persist_quality_event(_ev("t_c_walfail"))        # must not lose the event, must not raise

    assert app._quality_wal.write_failed is True          # visible, not absorbed
    (row,) = _rows_for(tmp_path, "t_c_walfail")           # force-published, never a RAM-only row
    (rec,) = _wal_for(tmp_path, "t_c_walfail")            # bytes had reached the file before fsync failed
    assert row["quality_event_id"] == rec["quality_event_id"]   # WAL-resident copy reconcilable by id
    monkeypatch.undo()
    _crash(app)
    app2 = _app(tmp_path)                                  # restart: no duplicate of the published row
    assert len(_rows_for(tmp_path, "t_c_walfail")) == 1
    _crash(app2)


def test_C_quality_write_failure_is_not_swallowed_and_the_event_stays_in_the_wal(tmp_path, monkeypatch):
    app = _app(tmp_path)
    _fail_quality_write(monkeypatch, app)
    with pytest.raises(OSError, match="injected|disk full"):
        app._persist_quality_event(_ev("t_c_writefail"))

    assert app._quality_checkpoint_blocked is True
    assert app._quality_checkpoint_block_reason == "quality_writer_write_failed"
    assert [r["reason"] for r in _pending(tmp_path)] == ["t_c_writefail"]   # retained for recovery
    monkeypatch.undo()
    app._persist_quality_event(_ev("t_c_later"))           # later success must NOT checkpoint past it
    pending_seq = _pending(tmp_path)[0]["seq"]
    assert _disk_ckpt(tmp_path) < pending_seq
    _crash(app)


def test_C_segment_publication_failure_raises_blocks_checkpoint_and_keeps_wal_record(tmp_path, monkeypatch):
    app = _app(tmp_path)
    _fail_segment_publish(monkeypatch)
    with pytest.raises(OSError, match="disk full"):
        app._persist_quality_event(_ev("t_c_publishfail"))

    assert [r["reason"] for r in _pending(tmp_path)] == ["t_c_publishfail"]
    assert _disk_ckpt(tmp_path) == -1
    assert app._quality_checkpoint_blocked is True
    monkeypatch.undo()
    _crash(app)


def test_C_websocket_boundary_does_not_raise_but_failure_is_loud_latched_and_recoverable(tmp_path, monkeypatch):
    """WebSocketClient calls on_quality_event UNGUARDED; a raise would escape
    its connection loop. This boundary logs at ERROR, latches, keeps the WAL copy."""
    app = _app(tmp_path)
    log = _CapturingLogger()
    monkeypatch.setattr(rbc, "logger", log)
    _fail_quality_write(monkeypatch, app)

    app._on_client_quality_event("DISCONNECT", "t_c_ws", "bybit-1", "bybit")   # must not raise

    assert any(e == "bybit_websocket_quality_event_persist_failed" for e, _ in log.errors)
    assert app._quality_checkpoint_blocked is True
    assert [r["reason"] for r in _pending(tmp_path)] == ["t_c_ws"]
    monkeypatch.undo()
    _crash(app)


def test_C_wal_and_writer_both_failing_is_loud_and_leaves_the_wal_copy(tmp_path, monkeypatch):
    app = _app(tmp_path)
    _fail_wal_fsync(monkeypatch, app)
    _fail_quality_write(monkeypatch, app)
    with pytest.raises(OSError):
        app._persist_quality_event(_ev("t_c_double"))

    assert app._quality_checkpoint_blocked is True
    assert [r["reason"] for r in _wal_for(tmp_path, "t_c_double")] != []     # WAL-resident copy survives
    monkeypatch.undo()
    _crash(app)


def test_C_checkpoint_write_failure_latches_and_never_advances_the_disk_checkpoint(tmp_path, monkeypatch):
    app = _app(tmp_path)
    real = QualityEventWAL.checkpoint

    def failing(self, up_to_seq):
        raise OSError("checkpoint write failed (injected)")
    monkeypatch.setattr(QualityEventWAL, "checkpoint", failing)
    app._persist_quality_event(_ev("t_c_ckpt"))            # hook must not raise out of the writer

    assert app._quality_checkpoint_blocked is True
    assert app._quality_checkpoint_block_reason.startswith("checkpoint_write_failed")
    assert _disk_ckpt(tmp_path) == -1
    assert len(_rows_for(tmp_path, "t_c_ckpt")) == 1       # the row itself is published
    monkeypatch.setattr(QualityEventWAL, "checkpoint", real)
    _crash(app)


# ======================= D. restart / recovery =============================
def test_D_unpublished_event_is_replayed_once_with_original_provenance(tmp_path, monkeypatch):
    app = _app(tmp_path)
    ts = 1_700_000_000_123
    with monkeypatch.context() as m:
        _fail_segment_publish(m)
        with pytest.raises(OSError):
            app._persist_quality_event(_ev("t_d_replay", event_type=QualityEventType.SEQUENCE_GAP,
                                           local_ts=ts, local_receive_ts=ts - 5, update_id=42))
    (orig,) = _pending(tmp_path)
    _crash(app)
    time.sleep(0.02)                                       # replay time must differ from original

    app2 = _app(tmp_path)
    (row,) = _rows_for(tmp_path, "t_d_replay")
    assert row["quality_event_id"] == orig["quality_event_id"]
    assert _ms(row["timestamp"]) == ts and _ms(row["local_receive_ts"]) == ts - 5
    assert row["update_id"] == 42 and row["event_type"] == "SEQUENCE_GAP"
    assert _ms(row["local_process_ts"]) == orig["local_process_ts"]          # not the replay instant
    assert _pending(tmp_path) == [] and _disk_ckpt(tmp_path) >= orig["seq"]
    _crash(app2)

    app3 = _app(tmp_path)                                  # second restart: nothing left to replay
    assert len(_rows_for(tmp_path, "t_d_replay")) == 1
    _crash(app3)


def test_D_restart_resumes_the_sequence_and_never_reuses_an_event_id(tmp_path):
    app = _app(tmp_path)
    app._persist_quality_event(_ev("t_d_first"))
    (first,) = _wal_for(tmp_path, "t_d_first")
    _crash(app)

    app2 = _app(tmp_path)
    app2._persist_quality_event(_ev("t_d_second"))
    (second,) = _wal_for(tmp_path, "t_d_second")
    assert second["seq"] > first["seq"]
    assert second["quality_event_id"] != first["quality_event_id"]
    assert len(_rows_for(tmp_path, "t_d_first")) == 1      # checkpointed events are not replayed
    _crash(app2)


def test_D_corrupt_wal_is_reported_preserved_and_never_checkpointed_over(tmp_path):
    app = _app(tmp_path)
    app._persist_quality_event(_ev("t_d_c1"))
    app._persist_quality_event(_ev("t_d_c2"))
    ckpt_before = _disk_ckpt(tmp_path)
    _crash(app)
    wal_file = sorted(_wal_dir(tmp_path).glob("*.wal"))[0]
    lines = wal_file.read_text().splitlines()
    lines[0] = "{not json"                                 # mid-file corruption, not a torn tail
    wal_file.write_text("\n".join(lines) + "\n")

    app2 = _app(tmp_path)                                  # must still start: capture is the mission
    assert app2._quality_checkpoint_blocked is True
    assert app2._quality_checkpoint_block_reason == "wal_corruption_on_startup"
    assert any(str(r["reason"]).startswith("quality_wal_corruption_on_startup:") for r in _rows(tmp_path))
    app2._persist_quality_event(_ev("t_d_c3"))
    assert _disk_ckpt(tmp_path) == ckpt_before             # never advanced over the corrupt file
    assert "{not json" in wal_file.read_text()             # evidence preserved
    _crash(app2)


# ======================= E. regression =====================================
def test_E_market_data_processing_is_unchanged_and_every_quality_row_is_wal_backed(tmp_path):
    app = _app(tmp_path)
    now = int(time.time() * 1000)
    snapshot = json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": now,
                           "data": {"s": "BTCUSDT", "b": [["50000", "1.0"], ["49999", "2.0"]],
                                    "a": [["50001", "1.5"], ["50002", "0.5"]], "u": 1, "seq": 100}})
    delta = json.dumps({"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": now + 10,
                        "data": {"s": "BTCUSDT", "b": [["50000", "3.0"]], "a": [], "u": 2, "seq": 101}})
    trade = json.dumps({"topic": "publicTrade.BTCUSDT", "ts": now,
                        "data": [{"T": now, "s": "BTCUSDT", "S": "Buy", "v": "0.01",
                                  "p": "50000.5", "i": "abc123", "BT": False}]})
    _drive(app, [snapshot, delta, trade])

    assert len(app.ob_writer.buffer) == 2
    assert app.ob_writer.buffer[-1]["bids_qty"][0] == 3.0 and app.ob_writer.buffer[-1]["is_snapshot"] is False
    assert len(app.trades_writer.buffer) == 1 and app.trades_writer.buffer[0]["price"] == 50000.5
    assert len(app.raw_wire_writer.buffer) == 3
    rows = _rows(tmp_path)
    assert {r["quality_event_id"] for r in rows} == {r["quality_event_id"] for r in _wal_records(tmp_path)}
    assert all(r["quality_event_id"] for r in rows)
    _crash(app)


# ======================= shutdown ordering =================================
def _shutdown(app, monkeypatch):
    # WebSocketClient.stop() is synchronous but BybitCollectorApp.shutdown()
    # awaits it (pre-existing, out of scope here). Stub it so the WAL shutdown
    # sequencing can be exercised at all.
    async def _stop():
        return None
    monkeypatch.setattr(app.client, "stop", _stop)
    asyncio.run(app.shutdown())


def test_shutdown_closes_quality_writer_then_the_wal_and_leaves_nothing_pending(tmp_path, monkeypatch):
    app = _app(tmp_path)
    app._persist_quality_event(_ev("t_s_one"))
    app._persist_quality_event(_ev("t_s_two"))
    _shutdown(app, monkeypatch)

    assert app._quality_wal._closed is True
    assert _pending(tmp_path) == []
    assert len(_rows_for(tmp_path, "t_s_one")) == len(_rows_for(tmp_path, "t_s_two")) == 1
    app._on_client_quality_event("DISCONNECT", "t_s_late", "bybit-1", "bybit")   # must not raise


def test_shutdown_quality_writer_close_failure_latches_and_still_closes_the_wal(tmp_path, monkeypatch):
    app = _app(tmp_path)

    def boom():
        raise OSError("close failed (injected)")
    monkeypatch.setattr(app.quality_writer, "close", boom)
    _shutdown(app, monkeypatch)

    assert app._quality_checkpoint_blocked is True
    assert app._quality_checkpoint_block_reason == "quality_writer_close_failed"
    assert app._quality_wal._closed is True
    monkeypatch.undo()
    _crash(app)
