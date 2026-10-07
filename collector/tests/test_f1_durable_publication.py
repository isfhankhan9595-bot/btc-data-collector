"""F1: durable publication boundary.

Invariant under test:  seen(K) => a confirmed segment S contains K AND the index
evidence for S equals S's publication marker (sha256 + size). A visible ``.seg``
is never authority by itself.

Method. Everything drives the REAL ``ParquetWriter`` / ``SegmentDedupCoordinator``
/ ``SegmentDedupIndex``. Faults are injected at the real syscall seam
(``os.fsync`` / ``os.replace`` / ``os.open``) and recorded BY PATH, so ordering
assertions are about actual syscalls, not about our own bookkeeping.

* "crash"  = a ``BaseException`` raised at a chosen syscall (nothing after it
  runs, like SIGKILL), after which the writer/index objects are abandoned and
  fresh ones are built on the same directory.
* "power-loss / storage-loss" = SIMULATED by renaming/deleting files. It is NOT
  proof of real ext4/XFS power-loss behaviour, which is UNKNOWN here.
"""
from __future__ import annotations

import errno
import hashlib
import json
import os
import sqlite3
import stat
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.collector import publication as pub
from collector.collector import segment_dedup as sd
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.publication import (
    DEEP_VERIFY_ENV, MarkerStatus, PublicationError, UNVERIFIED_FS_ENV, encode_marker, fs_guard,
    marker_path, quarantine_segment, read_marker, sha256_file,
)
from collector.collector.segment_dedup import (
    UNIDENTIFIED, DedupStateError, SegmentDedupCoordinator, SegmentDedupIndex, dedup_identity_key,
)
from collector.collector.storage_layout import iter_segments, parse_segment_name

SCHEMA = pa.schema([("timestamp", pa.int64()), ("instrument_key", pa.string()), ("trade_id", pa.string())])
INSTR = "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT"
STREAM = "f1_trades"
REAL_FSYNC, REAL_REPLACE, REAL_OPEN = os.fsync, os.replace, os.open


@pytest.fixture(autouse=True)
def _allow_unverified_fs_for_hermetic_tests(monkeypatch):
    # These tests must pass on tmpfs/overlay dev machines. The guard itself is
    # tested with an explicit ``environ`` / ``mountinfo`` and never reads this.
    monkeypatch.setenv(UNVERIFIED_FS_ENV, "1")
    monkeypatch.delenv(DEEP_VERIFY_ENV, raising=False)


class Crash(BaseException):
    """Simulated process death at a syscall (never caught by ``except Exception``)."""


def key_for(trade_id, instr=INSTR):
    return dedup_identity_key("BINANCE", "linear_perpetual", instr, "trades", trade_id)


def row_identity(row):
    if row["trade_id"] is None:
        return None
    return key_for(row["trade_id"], row["instrument_key"] or UNIDENTIFIED)


class Spy:
    """Records fsync/replace/index-commit by path and applies fault rules."""

    def __init__(self, mp):
        self.ops, self.rules, self._dirs = [], [], 0
        mp.setattr(os, "fsync", self._fsync)
        mp.setattr(os, "replace", self._replace)
        real_commit = SegmentDedupIndex.commit_segment

        def commit(index, *a, **k):
            self.ops.append(("index", a[0]))
            return real_commit(index, *a, **k)
        mp.setattr(SegmentDedupIndex, "commit_segment", commit)

    def on(self, predicate, exc):
        self.rules.append((predicate, exc))
        return self

    def _apply(self, op):
        self.ops.append(op)
        for predicate, exc in self.rules:
            if predicate(op):
                raise exc

    def _fsync(self, fd):
        path = os.readlink(f"/proc/self/fd/{fd}")
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            self._dirs += 1
            self._apply(("dirfsync", path, self._dirs))
        else:
            self._apply(("fsync", path, None))
        return REAL_FSYNC(fd)

    def _replace(self, src, dst, *a, **k):
        self._apply(("replace", str(dst), None))
        return REAL_REPLACE(src, dst, *a, **k)

    def kinds(self):
        return [(o[0], Path(o[1]).name) for o in self.ops]


def EIO(msg="injected"):
    return OSError(errno.EIO, f"Input/output error ({msg})")


def is_seg_tmp_fsync(op): return op[0] == "fsync" and op[1].endswith(".seg.tmp")
def is_marker_tmp_fsync(op): return op[0] == "fsync" and op[1].endswith(".meta.json.tmp")
def is_marker_replace(op): return op[0] == "replace" and op[1].endswith(".meta.json")
def is_seg_replace(op): return op[0] == "replace" and op[1].endswith(".seg")
def nth_dirfsync(n): return lambda op: op[0] == "dirfsync" and op[2] == n


class Rig:
    """Runner-shaped wiring of coordinator + real writer on one directory."""

    def __init__(self, base, *, segment_rows=5_000, hook=None):
        self.base, self.segment_rows, self.custom_hook = str(base), segment_rows, hook
        self.events = []
        self.writer = self.index = self.co = None
        self.start()

    def start(self):
        self.index = SegmentDedupIndex(self.base + "/dedup.sqlite3")
        self.co = SegmentDedupCoordinator(self.index, row_identity)
        self.writer = ParquetWriter(STREAM, SCHEMA, base_dir=self.base, segment_rows=self.segment_rows,
                                    segment_seconds=3600, quality_event_sink=self.events.append,
                                    on_segment_published=self._hook)
        self.co.startup_reconcile(self.writer.stream_dir)

    def _hook(self, token, path):
        if self.custom_hook is not None:
            return self.custom_hook(self, token, path)
        return self.co.on_segment_published(token, path)

    def offer(self, trade_id):
        k = key_for(trade_id)
        if not self.co.check_and_admit(k):
            return False
        self.writer.write({"timestamp": 1, "instrument_key": INSTR, "trade_id": trade_id},
                          bind=lambda t: self.co.note_written(k, t))
        self.co.end_message()
        return True

    def publish(self, *ids):
        for t in ids:
            assert self.offer(t)
        self.writer.publish_open_segment()

    def crash(self):
        self.writer._release_lock()          # process death releases the OS lock, nothing else
        self.index.close()

    def restart(self):
        self.crash()
        self.events.clear()
        self.start()

    @property
    def stream_dir(self) -> Path:
        return self.writer.stream_dir

    def segs(self):
        return sorted(self.stream_dir.glob("*.seg"))


def drop_marker(seg: Path):
    marker_path(seg).unlink()


def of_type(events, t):
    return [e for e in events if e["event_type"] == t]


# ===================================================================== 1-5: marker + ordering

def test_marker_written_after_dir_fsync_and_before_dedup_hook(tmp_path, monkeypatch):
    spy = Spy(monkeypatch)
    r = Rig(tmp_path, hook=lambda rig, token, path: (spy.ops.append(("hook", str(path), None)),
                                                      rig.co.on_segment_published(token, path)))
    r.offer("A")
    spy.ops.clear()
    r.writer.publish_open_segment()
    ops = spy.ops
    dirs = [i for i, o in enumerate(ops) if o[0] == "dirfsync"]
    assert len(dirs) == 2, "exactly the two directory fsyncs that existed before F1"
    first = lambda pred: next(i for i, o in enumerate(ops) if pred(o))
    order = [first(is_seg_tmp_fsync), first(is_seg_replace), dirs[0], first(is_marker_tmp_fsync),
             first(is_marker_replace), dirs[1], first(lambda o: o[0] == "hook"), first(lambda o: o[0] == "index")]
    assert order == sorted(order) and len(set(order)) == len(order), f"wrong order: {ops}"


def test_marker_schema_v2_exact_fields_and_sha256_binds_bytes(tmp_path):
    r = Rig(tmp_path)
    r.publish("A", "B")
    seg = r.segs()[0]
    raw = marker_path(seg).read_bytes()
    obj = json.loads(raw)
    assert set(obj) == {"first_record_ts", "last_record_ts", "record_count", "publication"}
    p = obj["publication"]
    assert set(p) == {"boot_id", "confirmed_at_utc", "confirmed_by", "legacy", "schema", "segment", "sha256",
                      "size_bytes", "state", "writer_schema"}
    assert (p["schema"], p["state"], p["confirmed_by"], p["legacy"], p["segment"], p["writer_schema"]) == (
        2, "PUBLICATION_CONFIRMED", "writer", False, seg.name, "parquet_writer/2")
    assert p["sha256"] == hashlib.sha256(seg.read_bytes()).hexdigest()
    assert p["size_bytes"] == seg.stat().st_size and obj["record_count"] == 2
    assert obj["first_record_ts"] == obj["last_record_ts"] == 1
    assert raw == encode_marker(obj) and raw.endswith(b"}\n") and not raw.endswith(b"\n\n")
    assert p["boot_id"] is None or isinstance(p["boot_id"], str)
    assert read_marker(seg).valid


@pytest.mark.parametrize("fault", ["write", "fsync", "replace", "dir_fsync"])
def test_marker_failure_skips_dedup_hook_and_latches_writer(tmp_path, monkeypatch, fault):
    spy = Spy(monkeypatch)
    hooks = []
    r = Rig(tmp_path, hook=lambda rig, token, path: hooks.append(path))
    r.offer("A")
    if fault == "write":
        real = os.open
        monkeypatch.setattr(os, "open", lambda p, *a, **k: (_ for _ in ()).throw(EIO("marker open"))
                            if str(p).endswith(".meta.json.tmp") else real(p, *a, **k))
    elif fault == "fsync":
        spy.on(is_marker_tmp_fsync, EIO("marker fsync"))
    elif fault == "replace":
        spy.on(is_marker_replace, EIO("marker replace"))
    else:
        spy.on(nth_dirfsync(2), EIO("marker dir fsync"))
    r.writer.publish_open_segment()                       # must not raise: the segment is durable
    seg = r.segs()
    assert len(seg) == 1 and pq.read_table(seg[0]).num_rows == 1, "durable segment must survive"
    assert hooks == [], "dedup hook must not run without a durable marker"
    assert r.writer._publication_failure is not None and r.writer._storage_failure is None
    with pytest.raises(RuntimeError):
        r.writer.write({"timestamp": 2, "instrument_key": INSTR, "trade_id": "Z"})
    assert of_type(r.events, "STORAGE_METADATA_FAILED") and of_type(r.events, "DEDUP_STATE_FAILED")
    assert not of_type(r.events, "DATA_DROP"), "a durable segment is not a data loss"
    assert not list(r.stream_dir.glob("*.meta.json.tmp")) or fault == "dir_fsync"


def test_marker_dir_fsync_failure_marker_still_implies_durable_segment(tmp_path, monkeypatch):
    spy = Spy(monkeypatch).on(nth_dirfsync(2), EIO("marker dir fsync"))
    r = Rig(tmp_path)
    r.offer("A")
    r.writer.publish_open_segment()
    seg = r.segs()[0]
    assert read_marker(seg).valid, "marker visible (replace done) even though its own dir fsync failed"
    dirs = [o for o in spy.ops if o[0] == "dirfsync"]
    assert dirs[0][2] == 1 and dirs[1][2] == 2, "marker visibility is preceded by a SUCCESSFUL segment dir fsync"
    assert r.index.identity_count() == 0, "hook withheld"
    monkeypatch.undo()
    r.restart()
    rep = r.co.last_report
    assert (rep.confirmed_by_refsync, rep.indexed) == (0, 1), "valid marker => indexed with no refsync needed"
    assert not r.offer("A")


def test_dir_fsync_failure_leaves_seg_unmarked_unindexed(tmp_path, monkeypatch):
    Spy(monkeypatch).on(nth_dirfsync(1), EIO("segment dir fsync"))
    hooks = []
    r = Rig(tmp_path, hook=lambda rig, token, path: hooks.append(path))
    r.offer("A")
    with pytest.raises(OSError):
        r.writer.publish_open_segment()
    seg = r.segs()
    assert len(seg) == 1, "left in place, never deleted"
    assert not marker_path(seg[0]).exists() and hooks == [] and r.index.identity_count() == 0
    assert r.writer._storage_failure_durability == "unconfirmed"
    assert of_type(r.events, "STORAGE_PUBLICATION_FAILED") and not of_type(r.events, "DATA_DROP")


# ===================================================================== 6-13: crash / restart matrix

def test_crash_before_tmp_fsync_restart_discards_tmp_redelivery_accepted(tmp_path, monkeypatch):
    Spy(monkeypatch).on(is_seg_tmp_fsync, Crash())
    r = Rig(tmp_path)
    r.offer("A")
    with pytest.raises(Crash):
        r.writer.publish_open_segment()
    monkeypatch.undo()
    assert not r.segs() and list(r.stream_dir.glob("*.seg.tmp"))
    r.restart()
    assert of_type(r.events, "DATA_DROP"), "the discarded orphan is reported, never silent"
    assert r.index.identity_count() == 0 and r.offer("A")


def test_crash_after_rename_before_dir_fsync_restart_confirms_via_refsync_then_indexes(tmp_path, monkeypatch):
    Spy(monkeypatch).on(nth_dirfsync(1), Crash())
    r = Rig(tmp_path)
    r.offer("A")
    with pytest.raises(Crash):
        r.writer.publish_open_segment()
    monkeypatch.undo()
    seg = r.segs()[0]
    assert not marker_path(seg).exists() and r.index.identity_count() == 0
    spy = Spy(monkeypatch)
    r.restart()
    rep = r.co.last_report
    assert (rep.confirmed_by_refsync, rep.indexed, rep.rebuilt) == (1, 1, False)
    m = read_marker(seg)
    assert m.valid and m.publication["confirmed_by"] == "startup_refsync"
    kinds = [o[0] for o in spy.ops]
    assert kinds.index("dirfsync") < kinds.index("index"), "refsync of the directory precedes index authority"
    assert not r.offer("A")


def test_crash_after_dir_fsync_before_marker_confirms_and_indexes(tmp_path, monkeypatch):
    Spy(monkeypatch).on(is_marker_tmp_fsync, Crash())
    r = Rig(tmp_path)
    r.offer("A")
    with pytest.raises(Crash):
        r.writer.publish_open_segment()
    monkeypatch.undo()
    assert not marker_path(r.segs()[0]).exists()
    stray = Path(str(marker_path(r.segs()[0])) + ".tmp")
    stray.write_text("half-written marker")               # what a real kill leaves behind
    r.restart()
    assert not stray.exists(), "stray marker tmp is discarded by orphan recovery"
    assert r.co.last_report.confirmed_by_refsync == 1 and not r.offer("A")


def test_crash_after_marker_before_sqlite_commit_indexes_on_restart(tmp_path):
    def die(rig, token, path):
        raise Crash()
    r = Rig(tmp_path, hook=die)
    r.offer("A")
    with pytest.raises(Crash):
        r.writer.publish_open_segment()
    assert read_marker(r.segs()[0]).valid and r.index.identity_count() == 0
    r.custom_hook = None
    r.restart()
    assert (r.co.last_report.confirmed_by_refsync, r.co.last_report.indexed) == (0, 1)
    assert read_marker(r.segs()[0]).publication["confirmed_by"] == "writer"
    assert not r.offer("A")


def test_crash_during_sqlite_txn_rolls_back_then_reindexes(tmp_path):
    r = Rig(tmp_path)
    r.offer("A"); r.offer("B")
    real = r.index._conn

    class Boom:
        def execute(self, *a): return real.execute(*a)
        def executemany(self, *a): raise sqlite3.OperationalError("simulated crash mid-transaction")
    r.index._conn = Boom()
    r.writer.publish_open_segment()                       # hook fails -> writer latched, segment durable
    r.index._conn = real
    assert r.writer._publication_failure is not None
    assert r.index.identity_count() == 0 and not r.index.is_segment_reconciled(f"{STREAM}/{r.segs()[0].name}")
    r.restart()
    assert r.index.identity_count() == 2 and not r.offer("A") and not r.offer("B")


def test_crash_after_sqlite_commit_restart_is_noop_and_suppresses_redelivery(tmp_path):
    r = Rig(tmp_path)
    r.publish("A")
    assert r.index.identity_count() == 1
    r.restart()
    assert (r.co.last_report.indexed, r.co.last_report.confirmed_by_refsync, r.co.last_report.rebuilt) == (0, 0, False)
    assert not r.offer("A") and r.index.identity_count() == 1


def test_power_loss_after_unconfirmed_rename_never_leaves_index_ahead_of_disk(tmp_path, monkeypatch):
    """D1. rename visible, no marker -> a restart that cannot establish durability
    must index NOTHING; if the rename is then lost (SIMULATED by renaming the
    segment back to ``.tmp``, what an un-journalled rename does) the index must
    not hold the identity, so the redelivery is accepted."""
    Spy(monkeypatch).on(nth_dirfsync(1), Crash())
    r = Rig(tmp_path)
    r.offer("A")
    with pytest.raises(Crash):
        r.writer.publish_open_segment()
    monkeypatch.undo()
    seg = r.segs()[0]
    unverified = fs_guard(tmp_path, mountinfo="1 0 0:1 / / rw - tmpfs tmpfs rw", environ={})
    monkeypatch.setattr(sd, "fs_guard", lambda path: unverified)
    with pytest.raises(DedupStateError):
        r.restart()                                       # cannot confirm: fail closed, index nothing
    assert r.index.identity_count() == 0 and not marker_path(seg).exists()
    REAL_REPLACE(seg, str(seg) + ".tmp")                  # SIMULATED loss of the un-durable rename
    monkeypatch.undo()
    r.restart()
    assert r.index.identity_count() == 0, "index must never be ahead of what is durable on disk"
    assert of_type(r.events, "DATA_DROP") and r.offer("A"), "redelivery accepted: it was never durable"


def test_sequence_reuse_after_revert_conflicts_evidence_and_rebuilds(tmp_path):
    """D2. X.seg vanishes after being indexed; the next segment reuses X's name.
    Stale evidence must never silently attach to the new bytes."""
    r = Rig(tmp_path)
    r.publish("A")
    old = r.segs()[0]
    old_name = old.name
    old.unlink()                                          # SIMULATED loss of the published segment
    r.restart()                                           # dangling marker + indexed row -> rebuild
    assert r.co.last_report.rebuilt and r.index.identity_count() == 0 and r.offer("A") is True
    r.writer.publish_open_segment()
    assert r.segs()[0].name == old_name, "sequence number is reused"
    # live defence in depth: bypass startup entirely, stale row + new bytes => conflict, not a silent no-op
    (tmp_path / "second").mkdir()
    r2 = Rig(tmp_path / "second")
    r2.publish("A")
    seg2 = r2.segs()[0]
    r2.crash()
    seg2.unlink(); marker_path(seg2).unlink()
    idx = SegmentDedupIndex(str(tmp_path / "second" / "dedup.sqlite3"))
    co = SegmentDedupCoordinator(idx, row_identity)
    w = ParquetWriter(STREAM, SCHEMA, base_dir=str(tmp_path / "second"), segment_rows=5000, segment_seconds=3600,
                      on_segment_published=co.on_segment_published)     # NO startup_reconcile
    k = key_for("B")
    assert co.check_and_admit(k)
    w.write({"timestamp": 1, "instrument_key": INSTR, "trade_id": "B"}, bind=lambda t: co.note_written(k, t))
    w.publish_open_segment()
    assert w._publication_failure is not None and "EVIDENCE_CONFLICT" in str(w._publication_failure)
    assert not idx.contains(k), "new identity must not be marked seen on a conflicting row"


# ===================================================================== 14-21: corruption

@pytest.mark.parametrize("bad", [b'{"record_count":1,"publi', b"\xff\xfe not json",
                                 None])   # None => structurally valid JSON with a malformed sha256
def test_truncated_marker_treated_as_absent_and_preserved_as_invalid_file(tmp_path, bad):
    r = Rig(tmp_path)
    r.publish("A")
    seg = r.segs()[0]
    if bad is None:
        obj = json.loads(marker_path(seg).read_bytes())
        obj["publication"]["sha256"] = "NOT-HEX"
        bad = encode_marker(obj)
    marker_path(seg).write_bytes(bad)
    assert read_marker(seg).status is MarkerStatus.INVALID
    r.restart()
    kept = list(r.stream_dir.glob("*.meta.json.invalid.*"))
    assert len(kept) == 1 and kept[0].read_bytes() == bad, "untrusted evidence preserved byte-for-byte"
    assert read_marker(seg).valid and r.co.last_report.confirmed_by_refsync == 1
    assert r.co.last_report.invalid_markers == 1 and not r.offer("A")


def test_marker_with_missing_segment_is_dangling_renamed_orphan_and_triggers_rebuild_if_reconciled(tmp_path):
    r = Rig(tmp_path)
    r.publish("A")
    seg = r.segs()[0]
    seg.unlink()
    r.restart()
    assert not marker_path(seg).exists(), "a dangling marker must not remain at its authoritative name"
    assert len(list(r.stream_dir.glob("*.meta.json.orphan.*"))) == 1, "preserved, not deleted"
    assert r.co.last_report.dangling == 1 and r.co.last_report.rebuilt
    assert r.index.identity_count() == 0 and r.offer("A")


def test_marker_size_mismatch_raises_dedup_state_error(tmp_path):
    r = Rig(tmp_path)
    r.publish("A")
    seg = r.segs()[0]
    bad = seg.read_bytes() + b"\0"
    seg.write_bytes(bad)
    before = marker_path(seg).read_bytes()
    with pytest.raises(DedupStateError, match="marker"):
        r.restart()
    assert seg.read_bytes() == bad and marker_path(seg).read_bytes() == before, "evidence untouched"


def test_unmarked_corrupt_parquet_raises_and_file_untouched(tmp_path):
    r = Rig(tmp_path)
    junk = r.stream_dir / "2026-01-01-00-000000.seg"
    junk.write_bytes(b"this is not parquet" * 10)
    with pytest.raises(DedupStateError, match="not readable"):
        r.restart()
    assert junk.read_bytes() == b"this is not parquet" * 10
    assert not list(r.stream_dir.glob("*.meta.json")) and r.index.identity_count() == 0


def test_refsync_file_error_raises_nothing_indexed_nothing_marked(tmp_path, monkeypatch):
    r = Rig(tmp_path)
    r.publish("A")
    drop_marker(r.segs()[0])
    r.index.rebuild_reset()                               # crash window: neither marker nor index row
    Spy(monkeypatch).on(lambda op: op[0] == "fsync" and op[1].endswith(".seg"), EIO("segment fsync"))
    with pytest.raises(DedupStateError, match="fsync"):
        r.restart()
    assert not list(r.stream_dir.glob("*.meta.json*")) and r.index.identity_count() == 0


def test_refsync_dir_error_raises_before_any_marker_written(tmp_path, monkeypatch):
    r = Rig(tmp_path)
    r.publish("A")
    drop_marker(r.segs()[0])
    r.index.rebuild_reset()
    Spy(monkeypatch).on(lambda op: op[0] == "dirfsync", EIO("directory fsync"))
    with pytest.raises(DedupStateError, match="directory fsync"):
        r.restart()
    assert not list(r.stream_dir.glob("*.meta.json*")), "no marker may precede a successful directory fsync"
    assert r.index.identity_count() == 0


MOUNTS = {
    "ext4": "30 1 8:1 / / rw,relatime - ext4 /dev/sda1 rw,errors=remount-ro",
    "xfs": "30 1 8:1 / / rw - xfs /dev/nvme0n1p1 rw,attr2",
    "tmpfs": "30 1 0:5 / / rw - tmpfs tmpfs rw",
    "nfs4": "30 1 0:40 / / rw - nfs4 srv:/x rw",
    "overlay": "30 1 0:40 / / rw - overlay overlay rw,lowerdir=/a",
    "nobarrier": "30 1 8:1 / / rw - ext4 /dev/sda1 rw,nobarrier",
}


def test_fs_guard_unverified_filesystem_blocks_promotion(tmp_path, monkeypatch):
    assert fs_guard(tmp_path, mountinfo=MOUNTS["ext4"], environ={}).verified
    assert fs_guard(tmp_path, mountinfo=MOUNTS["xfs"], environ={}).verified
    for name in ("tmpfs", "nfs4", "overlay", "nobarrier"):
        v = fs_guard(tmp_path, mountinfo=MOUNTS[name], environ={})
        assert not v.verified and not v.allows_promotion, name
    assert not fs_guard(tmp_path, mountinfo="garbage", environ={}).allows_promotion, "unknown => closed"
    over = fs_guard(tmp_path, mountinfo=MOUNTS["tmpfs"], environ={UNVERIFIED_FS_ENV: "1"})
    assert over.overridden and over.allows_promotion and not over.verified
    for junk in ("true", "yes", "0", ""):
        assert not fs_guard(tmp_path, mountinfo=MOUNTS["tmpfs"], environ={UNVERIFIED_FS_ENV: junk}).allows_promotion
    # a nested, more specific mount wins
    nested = MOUNTS["ext4"] + "\n" + f"31 30 0:5 / {tmp_path} rw - tmpfs tmpfs rw"
    assert fs_guard(tmp_path, mountinfo=nested, environ={}).fstype == "tmpfs"
    # integration: unmarked is refused, already-marked is NOT disturbed
    (tmp_path / "w").mkdir()
    r = Rig(tmp_path / "w")
    r.publish("A")
    r.publish("B")
    blocked = fs_guard(tmp_path, mountinfo=MOUNTS["tmpfs"], environ={})
    monkeypatch.setattr(sd, "fs_guard", lambda path: blocked)
    r.restart()                                           # everything marked: unaffected
    assert r.co.last_report.indexed == 0
    drop_marker(r.segs()[0])
    with pytest.raises(DedupStateError, match="Refusing to treat a visible .seg as durable"):
        r.restart()
    assert not marker_path(r.segs()[0]).exists(), "nothing fabricated"


def test_deep_verify_detects_bitflip_with_same_size(tmp_path, monkeypatch):
    r = Rig(tmp_path)
    r.publish("A")
    seg = r.segs()[0]
    data = bytearray(seg.read_bytes())
    data[len(data) // 2] ^= 0x01
    seg.write_bytes(bytes(data))
    r.restart()                                           # default: size-only check, O(1) per segment
    monkeypatch.setenv(DEEP_VERIFY_ENV, "1")
    with pytest.raises(DedupStateError, match="sha256"):
        r.restart()


# ===================================================================== 22-25: legacy / migration

def test_legacy_seg_without_marker_verified_and_marked_legacy_true(tmp_path):
    r = Rig(tmp_path)
    r.publish("A")
    seg = r.segs()[0]
    drop_marker(seg)
    r.restart()
    m = read_marker(seg)
    assert m.valid and m.publication["legacy"] is True and m.publication["confirmed_by"] == "startup_refsync"
    assert m.publication["sha256"] == sha256_file(seg)[0] and m.data["record_count"] == 1
    assert not r.offer("A")


def test_legacy_v1_meta_not_trusted_without_confirmation(tmp_path):
    r = Rig(tmp_path)
    r.publish("A")
    seg = r.segs()[0]
    marker_path(seg).write_text(json.dumps({"record_count": 1, "first_record_ts": 1, "last_record_ts": 1}))
    m = read_marker(seg)
    assert m.status is MarkerStatus.ABSENT and m.v1, "a v1 hint is never a marker"
    r.index.rebuild_reset()
    r.restart()
    assert r.co.last_report.confirmed_by_refsync == 1
    m = read_marker(seg)
    assert m.valid and m.data["first_record_ts"] == 1, "a consistent v1 hint is carried as metadata only"
    # an INCONSISTENT hint is dropped, never propagated
    marker_path(seg).write_text(json.dumps({"record_count": 99, "first_record_ts": 7, "last_record_ts": 8}))
    r.restart()
    assert read_marker(seg).data["first_record_ts"] is None and read_marker(seg).data["record_count"] == 1


def _write_v1_index(path, rows):
    c = sqlite3.connect(path)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("CREATE TABLE seen (identity_key TEXT PRIMARY KEY) WITHOUT ROWID")
    c.execute("CREATE TABLE reconciled_segments (segment_key TEXT PRIMARY KEY, identity_count INTEGER NOT NULL) WITHOUT ROWID")
    for seg_key, ids in rows.items():
        c.executemany("INSERT INTO seen VALUES (?)", [(i,) for i in ids])
        c.execute("INSERT INTO reconciled_segments VALUES (?,?)", (seg_key, len(ids)))
    c.commit()
    c.close()


def _drop_index_files(base):
    for suffix in ("", "-wal", "-shm"):
        Path(str(base) + "/dedup.sqlite3" + suffix).unlink(missing_ok=True)


def test_user_version_1_index_rebuilt_once_and_idempotent_on_crash(tmp_path):
    r = Rig(tmp_path)
    r.publish("A")
    seg_key = f"{STREAM}/{r.segs()[0].name}"
    r.crash()
    _drop_index_files(tmp_path)
    _write_v1_index(tmp_path / "dedup.sqlite3", {seg_key: [key_for("A"), key_for("POISON")]})
    # (a) the rebuild transaction is atomic: a failure inside it changes NOTHING, version included
    idx = SegmentDedupIndex(str(tmp_path / "dedup.sqlite3"))
    assert idx.user_version() < 2 and idx.identity_count() == 2
    real = idx._conn

    class Boom:
        def execute(self, sql, *a):
            if sql.startswith("DELETE FROM reconciled_segments"):
                raise sqlite3.OperationalError("simulated crash inside rebuild")
            return real.execute(sql, *a)
    idx._conn = Boom()
    with pytest.raises(DedupStateError):
        idx.rebuild_reset()
    idx._conn = real
    assert idx.user_version() < 2 and idx.identity_count() == 2, "rolled back as a whole"
    idx.close()
    # (b) the real restart rebuilds exactly once, keyed by the schema version
    r.restart()
    rep = r.co.last_report
    assert rep.rebuilt and any("user_version" in x for x in rep.rebuild_reasons)
    assert r.index.user_version() == 2 and not r.index.contains(key_for("POISON")), "poisoned authority removed"
    assert r.index.contains(key_for("A")) and r.offer("POISON")
    # (c) idempotent
    r.restart()
    assert not r.co.last_report.rebuilt
    # (d) crash AFTER the reset txn but BEFORE re-indexing: restart completes the job
    r.index.rebuild_reset()
    r.restart()
    assert r.index.contains(key_for("A")) and r.co.last_report.indexed == 1


def test_rebuild_preserves_all_segments_and_loses_no_valid_identity(tmp_path):
    r = Rig(tmp_path, segment_rows=2)
    ids = [f"T{i}" for i in range(5)]
    for t in ids:
        assert r.offer(t)
    r.writer.close()
    assert len(r.segs()) == 3
    before = r.index.identity_count()
    r.crash()
    _drop_index_files(tmp_path)                           # derived state lost entirely
    r.start()
    assert r.index.identity_count() == before == 5 and r.co.last_report.indexed == 3
    assert all(not r.offer(t) for t in ids)


# ===================================================================== 26-29: dedup contract / readers / quarantine

def test_commit_segment_equal_evidence_noop_different_evidence_conflict(tmp_path):
    idx = SegmentDedupIndex(str(tmp_path / "i.sqlite3"))
    h1, h2 = "a" * 64, "b" * 64
    assert idx.commit_segment("s/x.seg", ["k1"], h1, 10, "writer") is True
    assert idx.commit_segment("s/x.seg", ["k1"], h1, 10, "writer") is False
    for sha, size in ((h2, 10), (h1, 11)):
        with pytest.raises(DedupStateError, match="EVIDENCE_CONFLICT"):
            idx.commit_segment("s/x.seg", ["k2"], sha, size, "writer")
    assert idx.contains("k1") and not idx.contains("k2"), "a conflicting commit writes nothing"
    assert idx.evidence_of("s/x.seg") == (h1, 10, "writer")


def test_hook_rejects_missing_or_mismatching_marker(tmp_path):
    idx = SegmentDedupIndex(str(tmp_path / "i.sqlite3"))
    co = SegmentDedupCoordinator(idx, row_identity)
    w = ParquetWriter(STREAM, SCHEMA, base_dir=str(tmp_path), segment_rows=5000, segment_seconds=3600)
    w.write({"timestamp": 1, "instrument_key": INSTR, "trade_id": "A"})
    w.publish_open_segment()
    seg = sorted(w.stream_dir.glob("*.seg"))[0]
    good = marker_path(seg).read_bytes()
    obj = json.loads(good)

    def attempt(raw):
        if raw is None:
            marker_path(seg).unlink(missing_ok=True)
        else:
            marker_path(seg).write_bytes(raw)
        with pytest.raises(DedupStateError):
            co.on_segment_published((STREAM, 0), seg)
        assert idx.identity_count() == 0, "nothing committed on a rejected hook"
    attempt(None)                                                         # missing
    attempt(b"{garbage")                                                  # invalid
    for field, value in (("sha256", "c" * 64), ("size_bytes", obj["publication"]["size_bytes"] + 1)):
        mutated = json.loads(good)
        mutated["publication"][field] = value
        attempt(encode_marker(mutated))                                   # mismatching evidence
    mutated = json.loads(good)
    mutated["record_count"] = 5
    attempt(encode_marker(mutated))                                       # footer rows disagree
    marker_path(seg).write_bytes(good)
    co.on_segment_published((STREAM, 0), seg)                             # the genuine marker is accepted
    assert idx.contains(key_for("A"))


def test_readers_unchanged_iter_segments_ignores_sidecars_and_quarantine_dir(tmp_path):
    r = Rig(tmp_path)
    r.publish("A")
    seg = r.segs()[0]
    d = r.stream_dir
    for extra in (str(marker_path(seg)) + ".invalid.0", str(marker_path(seg)) + ".orphan.20260101T000000Z",
                  str(seg) + ".quarantine.json", str(marker_path(seg)) + ".tmp"):
        Path(extra).write_text("x")
        assert parse_segment_name(extra) is None
    (d / "quarantine").mkdir()
    (d / "quarantine" / seg.name).write_bytes(b"x")
    assert list(iter_segments(str(tmp_path), STREAM)) == [seg]
    assert parse_segment_name(marker_path(seg)) is None
    r.writer._sequence_cache.clear()
    assert r.writer._next_sequence(seg.name[:13]) == 1, "sidecars/quarantine do not perturb sequence allocation"


def test_quarantine_segment_moves_never_deletes_and_is_not_authority(tmp_path):
    r = Rig(tmp_path)
    r.publish("A")
    seg = r.segs()[0]
    original = seg.read_bytes()
    dest = quarantine_segment(seg, reason="operator test", operator="pytest")
    assert not seg.exists() and not marker_path(seg).exists()
    assert dest.read_bytes() == original, "moved, never deleted or altered"
    note = json.loads((dest.parent / (dest.name + ".quarantine.json")).read_text())
    assert note["reason"] == "operator test" and note["operator"] == "pytest"
    assert note["sha256"] == hashlib.sha256(original).hexdigest() and note["original_path"] == str(seg)
    assert list(iter_segments(str(tmp_path), STREAM)) == []
    with pytest.raises(PublicationError):
        quarantine_segment(seg, reason="again", operator="pytest")        # nothing left at the old path
    seg.write_bytes(b"new bytes under the same name")
    with pytest.raises(PublicationError):
        quarantine_segment(seg, reason="again", operator="pytest")        # refuses to overwrite evidence
    assert dest.read_bytes() == original
    seg.unlink()
    r.restart()                                                           # its index row is now divergence
    assert r.co.last_report.rebuilt and r.index.identity_count() == 0 and r.offer("A")


# ===================================================================== extras

def test_hookless_writer_also_fails_closed_on_marker_failure(tmp_path, monkeypatch):
    Spy(monkeypatch).on(is_marker_replace, EIO("marker replace"))
    w = ParquetWriter(STREAM, SCHEMA, base_dir=str(tmp_path), segment_rows=5000, segment_seconds=3600)
    w.write({"timestamp": 1, "instrument_key": INSTR, "trade_id": "A"})
    w.publish_open_segment()
    with pytest.raises(RuntimeError):
        w.write({"timestamp": 2, "instrument_key": INSTR, "trade_id": "B"})
    assert len(list(w.stream_dir.glob("*.seg"))) == 1


def test_normal_path_adds_no_fsync_beyond_the_pre_f1_structure(tmp_path, monkeypatch):
    spy = Spy(monkeypatch)
    r = Rig(tmp_path)
    r.offer("A")
    spy.ops.clear()
    r.writer.publish_open_segment()
    assert sum(1 for o in spy.ops if is_seg_tmp_fsync(o)) == 1
    assert sum(1 for o in spy.ops if is_marker_tmp_fsync(o)) == 1
    assert sum(1 for o in spy.ops if o[0] == "dirfsync") == 2


def test_startup_is_idempotent_and_deterministic(tmp_path):
    r = Rig(tmp_path, segment_rows=2)
    for t in "ABCDE":
        r.offer(t)
    r.writer.close()
    for seg in r.segs()[:2]:
        drop_marker(seg)
    r.restart()
    first = (r.co.last_report.confirmed_by_refsync, r.index.identity_count())
    r.restart()
    assert first == (2, 5), "re-confirmed segments keep identical evidence: nothing re-indexed or lost"
    assert (r.co.last_report.confirmed_by_refsync, r.co.last_report.indexed, r.co.last_report.rebuilt) == (0, 0, False)


# ===================================================================== real process death (SIGKILL)

_CHILD = r'''
import os, signal, stat, sys
from collector.collector.parquet_writer import ParquetWriter
import pyarrow as pa
base, point = sys.argv[1], sys.argv[2]
real, dirs = os.fsync, [0]
def fsync(fd):
    path = os.readlink(f"/proc/self/fd/{fd}")
    isdir = stat.S_ISDIR(os.fstat(fd).st_mode)
    dirs[0] += isdir
    hit = {"seg_tmp": not isdir and path.endswith(".seg.tmp"), "dir1": isdir and dirs[0] == 1,
           "marker_tmp": not isdir and path.endswith(".meta.json.tmp"), "dir2": isdir and dirs[0] == 2}[point]
    if hit:
        os.kill(os.getpid(), signal.SIGKILL)       # real process death: no finally, no except, no atexit
    return real(fd)
os.fsync = fsync
schema = pa.schema([("timestamp", pa.int64()), ("instrument_key", pa.string()), ("trade_id", pa.string())])
w = ParquetWriter("f1_trades", schema, base_dir=base, segment_rows=5000, segment_seconds=3600)
w.write({"timestamp": 1, "instrument_key": "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT", "trade_id": "A"})
w.publish_open_segment()
'''


@pytest.mark.parametrize("point,expect", [
    ("seg_tmp", dict(seg=False, marker=False, drop=True, refsync=0, indexed=0)),
    ("dir1", dict(seg=True, marker=False, drop=False, refsync=1, indexed=1)),
    ("marker_tmp", dict(seg=True, marker=False, drop=False, refsync=1, indexed=1)),
    ("dir2", dict(seg=True, marker=True, drop=False, refsync=0, indexed=1)),
])
def test_real_sigkill_at_each_publication_step_recovers_safely(tmp_path, point, expect):
    import signal
    import subprocess
    import sys
    root = Path(__file__).resolve().parents[2]
    env = dict(os.environ, PYTHONPATH=f"{root}{os.pathsep}{root / 'collector'}")
    proc = subprocess.run([sys.executable, "-c", _CHILD, str(tmp_path), point], env=env,
                          capture_output=True, timeout=60, cwd=str(root))
    assert proc.returncode == -signal.SIGKILL, proc.stderr.decode()[-500:]
    stream_dir = tmp_path / "raw" / STREAM
    segs = sorted(stream_dir.glob("*.seg"))
    assert bool(segs) is expect["seg"]
    assert bool(segs and marker_path(segs[0]).exists()) is expect["marker"]
    r = Rig(tmp_path)                                       # the restart
    rep = r.co.last_report
    assert bool(of_type(r.events, "DATA_DROP")) is expect["drop"]
    assert (rep.confirmed_by_refsync, rep.indexed) == (expect["refsync"], expect["indexed"])
    assert not list(stream_dir.glob("*.meta.json.tmp")), "a real kill's stray marker tmp is discarded"
    if expect["indexed"]:
        assert not r.offer("A") and read_marker(r.segs()[0]).valid
    else:
        assert r.offer("A"), "nothing durable existed: redelivery accepted"
