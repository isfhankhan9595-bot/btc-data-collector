"""Fail closed on publication-marker I/O uncertainty.

An unreadable marker is unknown evidence -- not proof of corruption and not proof of absence.

``read_marker`` used to map every ``OSError`` other than ``FileNotFoundError`` to ``INVALID``, so a
transient EIO/EACCES on a perfectly healthy marker made ``startup_reconcile`` rename it
(``preserve_invalid``), re-confirm the segment from the segment bytes and write a *replacement* marker
(``confirmed_by`` silently changing from ``writer`` to ``startup_refsync``). These tests inject read
faults into temporary directories (never live data) and assert the before/after state of every marker
path, every directory entry, the SQLite index and the coordinator's pending identities -- not just the
exception raised.
"""
from __future__ import annotations

import errno
import hashlib
import json
import os
import sqlite3
from pathlib import Path

import pyarrow as pa
import pytest

from collector.collector import publication as pub
from collector.collector import segment_dedup as sd
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.publication import (
    UNVERIFIED_FS_ENV, MarkerResult, MarkerStatus, PublicationError, PublicationState, UnmarkedSegment,
    classify_segment, confirm_unmarked, fs_guard, marker_path, read_marker,
)
from collector.collector.segment_dedup import (
    DedupStateError, SegmentDedupCoordinator, SegmentDedupIndex, dedup_identity_key,
)

SCHEMA = pa.schema([("timestamp", pa.int64()), ("instrument_key", pa.string()), ("trade_id", pa.string())])
INSTR = "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT"
STREAM = "mio_trades"
MALFORMED = b'{"record_count":1,"publi'


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv(UNVERIFIED_FS_ENV, "1")
    monkeypatch.delenv(sd.DEEP_VERIFY_ENV, raising=False)


def key_for(trade_id):
    return dedup_identity_key("BINANCE", "linear_perpetual", INSTR, "trades", trade_id)


def row_identity(row):
    return None if row["trade_id"] is None else key_for(row["trade_id"])


def row(trade_id):
    return {"timestamp": 1, "instrument_key": INSTR, "trade_id": trade_id}


class Node:
    """One collector process: index + coordinator + writer. ``reconcile()`` is explicit so a test can
    snapshot the state AFTER construction (the writer opens its own ``.seg.tmp``) and BEFORE recovery."""

    def __init__(self, base, *, segment_rows=2):
        self.base = Path(base)
        self.index = SegmentDedupIndex(str(self.base / "dedup.sqlite3"))
        self.co = SegmentDedupCoordinator(self.index, row_identity)
        self.writer = ParquetWriter(STREAM, SCHEMA, base_dir=str(self.base), segment_rows=segment_rows,
                                    segment_seconds=3600, on_segment_published=self.co.on_segment_published)

    @property
    def stream_dir(self) -> Path:
        return self.writer.stream_dir

    def reconcile(self):
        return self.co.startup_reconcile(self.stream_dir)

    def offer(self, trade_id):
        k = key_for(trade_id)
        assert self.co.check_and_admit(k)
        self.writer.write(row(trade_id), bind=lambda t: self.co.note_written(k, t))
        self.co.end_message()

    def stop(self):
        self.writer._release_lock()
        self.index.close()

    def segs(self):
        return sorted(self.stream_dir.glob("*.seg"))


def three_published_segments(base) -> Path:
    """A,B | C,D | E -> three published, marked and indexed segments; the process is then 'down'."""
    node = Node(base)
    node.reconcile()
    for t in "ABCDE":
        node.offer(t)
    node.writer.close()
    assert len(node.segs()) == 3
    node.stop()
    return Path(base)


# --------------------------------------------------------------------------------- state capture

def fs_state(base: Path) -> dict:
    """Every entry under ``base`` (sqlite files excluded; see :func:`index_state`): path -> content
    hash + size + inode, so a renamed, rewritten or replaced file (even with identical bytes) differs."""
    out = {}
    for p in sorted(Path(base).rglob("*")):
        if ".sqlite3" in p.name:
            continue
        rel = str(p.relative_to(base))
        if p.is_dir():
            out[rel] = "<dir>"
        else:
            st = p.stat()
            with open(p, "rb") as fh:                         # not Path.read_bytes: that is the injected fault
                out[rel] = (hashlib.sha256(fh.read()).hexdigest(), st.st_size, st.st_ino)
    return out


def index_state(base: Path) -> dict:
    """Logical content of the dedup index (read-only connection): every table + user_version."""
    con = sqlite3.connect(f"file:{Path(base) / 'dedup.sqlite3'}?mode=ro", uri=True)
    try:
        names = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY 1")]
        state = {n: con.execute(f"SELECT * FROM {n} ORDER BY 1, 2").fetchall() for n in names}
        state["user_version"] = con.execute("PRAGMA user_version").fetchone()[0]
        return state
    finally:
        con.close()


def raw_bytes(path) -> bytes:
    """Read a file while ``Path.read_bytes`` is the injected fault."""
    with open(path, "rb") as fh:
        return fh.read()


def tamper_index(base: Path, sql: str) -> None:
    con = sqlite3.connect(str(Path(base) / "dedup.sqlite3"))
    try:
        con.execute(sql)
        con.commit()
    finally:
        con.close()


# --------------------------------------------------------------------------------- fault injection

def inject_marker_read_error(monkeypatch, match, exc, *, nth=None):
    """Make ``Path.read_bytes`` raise ``exc`` for marker files whose path satisfies ``match``.
    ``nth`` (a set of 1-based hit numbers) fails only those reads; None fails every read."""
    real = Path.read_bytes
    hits = []

    def faulty(self):
        if str(self).endswith(".seg.meta.json") and match(str(self)):
            hits.append(str(self))
            if nth is None or len(hits) in nth:
                raise exc
        return real(self)
    monkeypatch.setattr(Path, "read_bytes", faulty)
    return hits


def only(*paths):
    names = {str(p) for p in paths}
    return lambda s: s in names


def errno_exc(code):
    return OSError(code, os.strerror(code))


READ_FAULTS = [errno.EIO, errno.EACCES, errno.EPERM, errno.ESTALE, errno.EMFILE]


class Mutations:
    """Records every call to a function that changes marker / segment / index / directory state."""

    def __init__(self, monkeypatch):
        self.calls = []
        self._mp = monkeypatch

    def wrap(self, owner, name, *, counts=lambda *a, **k: True):
        real = getattr(owner, name)

        def spy(*a, **k):
            if counts(*a, **k):
                self.calls.append(name)
            return real(*a, **k)
        self._mp.setattr(owner, name, spy)

    def install(self):
        # startup_reconcile always calls confirm_unmarked([]) (a documented no-op): only a call that
        # actually has segments to confirm is a mutation.
        self.wrap(sd, "confirm_unmarked", counts=lambda unmarked, *a, **k: bool(unmarked))
        for owner, name in ((sd, "preserve_invalid"), (sd, "orphan_marker"),
                            (sd, "fsync_dir"), (pub, "preserve_invalid"), (pub, "orphan_marker"),
                            (pub, "write_marker_atomic"), (pub, "fsync_dir"), (pub, "fsync_file"),
                            (os, "replace"), (os, "rename"),
                            (SegmentDedupIndex, "rebuild_reset"), (SegmentDedupIndex, "commit_segment"),
                            (SegmentDedupCoordinator, "_ensure_evidence")):
            self.wrap(owner, name)
        return self


# ===================================================================== 1-3, 11: read_marker status

@pytest.mark.parametrize("bad", [MALFORMED, b"\xff\xfe not json", b"", b"[]", b'{"publication": 3}'])
def test_malformed_bytes_are_invalid_content_not_unknown_io(tmp_path, bad):
    seg = tmp_path / "x.seg"
    seg.write_bytes(b"s")
    marker_path(seg).write_bytes(bad)
    m = read_marker(seg)
    assert m.status is MarkerStatus.INVALID and not m.valid and not m.v1


@pytest.mark.parametrize("code", READ_FAULTS)
def test_read_errors_are_unknown_io_never_invalid_or_absent(tmp_path, monkeypatch, code):
    seg = tmp_path / "x.seg"
    seg.write_bytes(b"s")
    marker_path(seg).write_bytes(b"{}")                      # the file EXISTS; only reading it fails
    inject_marker_read_error(monkeypatch, only(marker_path(seg)), errno_exc(code))
    m = read_marker(seg)
    assert m.status is MarkerStatus.UNKNOWN_IO
    assert m.status is not MarkerStatus.INVALID and m.status is not MarkerStatus.ABSENT
    assert not m.valid and not m.v1 and m.data is None
    assert "unreadable" in m.reason and os.strerror(code) in m.reason


def test_a_directory_in_place_of_the_marker_is_a_real_unreadable_marker(tmp_path):
    seg = tmp_path / "x.seg"
    seg.write_bytes(b"s")
    marker_path(seg).mkdir()                                 # genuine FS fault: IsADirectoryError, no mocking
    assert read_marker(seg).status is MarkerStatus.UNKNOWN_IO


def test_genuine_missing_marker_is_still_absent(tmp_path, monkeypatch):
    seg = tmp_path / "x.seg"
    seg.write_bytes(b"s")
    m = read_marker(seg)
    assert m.status is MarkerStatus.ABSENT and not m.v1 and m.reason == "no marker file"
    marker_path(seg).write_bytes(b"{}")
    inject_marker_read_error(monkeypatch, only(marker_path(seg)), OSError(errno.ENOENT, "gone"))
    assert isinstance(OSError(errno.ENOENT, "gone"), FileNotFoundError)
    assert read_marker(seg).status is MarkerStatus.ABSENT   # ENOENT keeps its absent-marker semantics


def test_v1_hint_still_absent_with_v1_flag(tmp_path):
    seg = tmp_path / "x.seg"
    seg.write_bytes(b"s")
    marker_path(seg).write_bytes(json.dumps({"record_count": 1}).encode())
    m = read_marker(seg)
    assert m.status is MarkerStatus.ABSENT and m.v1


# ===================================================================== 10: classify_segment

@pytest.mark.parametrize("exists", [True, False])
def test_classify_segment_raises_for_unknown_io_and_never_returns_a_state(tmp_path, exists):
    seg = tmp_path / "x.seg"
    if exists:
        seg.write_bytes(b"s")
    with pytest.raises(PublicationError, match="could not be read"):
        classify_segment(seg, MarkerResult(MarkerStatus.UNKNOWN_IO, reason="unreadable: EIO"))


def test_classify_segment_other_statuses_are_unchanged(tmp_path):
    seg = tmp_path / "x.seg"
    seg.write_bytes(b"12345")
    assert classify_segment(seg, MarkerResult(MarkerStatus.ABSENT)) is PublicationState.RENAMED_UNCONFIRMED
    assert classify_segment(seg, MarkerResult(MarkerStatus.ABSENT, v1=True)) is PublicationState.LEGACY_UNKNOWN
    assert classify_segment(seg, MarkerResult(MarkerStatus.INVALID, reason="x")) is PublicationState.RENAMED_UNCONFIRMED
    good = MarkerResult(MarkerStatus.VALID, data={"publication": {"size_bytes": 5}})
    bad = MarkerResult(MarkerStatus.VALID, data={"publication": {"size_bytes": 6}})
    assert classify_segment(seg, good) is PublicationState.PUBLICATION_CONFIRMED
    assert classify_segment(seg, bad) is PublicationState.CORRUPT


# ===================================================================== 4, 6, 7, 8: startup_reconcile

@pytest.mark.parametrize("code", READ_FAULTS)
def test_one_unreadable_marker_aborts_reconcile_with_zero_mutation(tmp_path, monkeypatch, code):
    base = three_published_segments(tmp_path)
    node = Node(base)
    victim = node.segs()[1]
    fs_before, idx_before = fs_state(base), index_state(base)
    muts = Mutations(monkeypatch).install()
    hits = inject_marker_read_error(monkeypatch, only(marker_path(victim)), errno_exc(code))

    with pytest.raises(DedupStateError, match="could not be read"):
        node.reconcile()

    assert hits, "the fault was really exercised"
    assert muts.calls == [], f"recovery mutated state after a read error: {muts.calls}"
    assert fs_state(base) == fs_before, "marker bytes, filenames, inodes and directory entries unchanged"
    assert index_state(base) == idx_before, "index rows, reconciled rows and user_version unchanged"
    assert not list(node.stream_dir.glob("*.invalid.*")) and not list(node.stream_dir.glob("*.orphan.*"))
    rep = node.co.last_report
    assert (rep.unreadable_markers, rep.invalid_markers, rep.confirmed_by_refsync, rep.indexed, rep.rebuilt) == (
        1, 0, 0, 0, False)
    node.stop()


@pytest.mark.parametrize("invalid_first", [True, False])
def test_malformed_marker_sorting_around_an_unreadable_one_is_not_renamed(tmp_path, monkeypatch, invalid_first):
    base = three_published_segments(tmp_path)
    node = Node(base)
    a, b, _ = node.segs()
    invalid, unreadable = (a, b) if invalid_first else (b, a)
    marker_path(invalid).write_bytes(MALFORMED)
    fs_before, idx_before = fs_state(base), index_state(base)
    muts = Mutations(monkeypatch).install()
    inject_marker_read_error(monkeypatch, only(marker_path(unreadable)), errno_exc(errno.EIO))

    with pytest.raises(DedupStateError):
        node.reconcile()

    assert muts.calls == []
    assert marker_path(invalid).read_bytes() == MALFORMED, "the genuinely malformed marker is still in place"
    assert not list(node.stream_dir.glob("*.invalid.*")), "neither marker was renamed by this attempt"
    assert fs_state(base) == fs_before and index_state(base) == idx_before
    assert node.co.last_report.invalid_markers == 0
    node.stop()


def _kitchen_sink(tmp_path):
    """seg0: malformed marker | seg1: (made unreadable by the test) | seg2: marker deleted (unmarked) |
    a dangling marker | an index that has diverged from the evidence (which would trigger a rebuild)."""
    base = three_published_segments(tmp_path)
    node = Node(base)
    s0, s1, s2 = node.segs()
    node.stop()
    marker_path(s0).write_bytes(MALFORMED)
    dangling_bytes = marker_path(s2).read_bytes()
    marker_path(s2).unlink()
    dangling = s2.parent / "2026-01-01-00-999999.seg.meta.json"
    dangling.write_bytes(dangling_bytes)
    tamper_index(base, "DELETE FROM seen WHERE identity_key = (SELECT identity_key FROM seen ORDER BY 1 LIMIT 1)")
    return base, (s0, s1, s2), dangling


def test_mixed_multi_segment_set_changes_nothing_until_every_marker_is_known(tmp_path, monkeypatch):
    base, (s0, s1, s2), dangling = _kitchen_sink(tmp_path)
    node = Node(base)
    fs_before, idx_before = fs_state(base), index_state(base)
    muts = Mutations(monkeypatch).install()
    hits = inject_marker_read_error(monkeypatch, only(marker_path(s1)), errno_exc(errno.EIO))

    with pytest.raises(DedupStateError):
        node.reconcile()

    assert hits and muts.calls == [], f"no rename / confirmation / rebuild / commit / fsync: {muts.calls}"
    assert fs_state(base) == fs_before, "malformed marker, unmarked segment and dangling marker all untouched"
    assert index_state(base) == idx_before, "the diverged index was NOT rebuilt because of the uncertainty"
    assert marker_path(s0).read_bytes() == MALFORMED and not marker_path(s2).exists() and dangling.exists()
    node.stop()
    monkeypatch.undo()

    # The uncertainty was transient: with the fault gone the SAME directory recovers, and the genuine
    # evidence handling (preserve malformed, orphan dangling, confirm unmarked, rebuild) is intact.
    node = Node(base)
    node.reconcile()
    rep = node.co.last_report
    assert (rep.invalid_markers, rep.dangling, rep.confirmed_by_refsync, rep.rebuilt) == (1, 1, 2, True)
    kept = sorted(node.stream_dir.glob("*.meta.json.invalid.*"))
    assert [p.read_bytes() for p in kept] == [MALFORMED], "preserved byte-for-byte"
    assert len(list(node.stream_dir.glob("*.meta.json.orphan.*"))) == 1
    assert all(read_marker(p).valid for p in node.segs())
    assert node.index.identity_count() == 5
    node.stop()


def test_transient_error_no_longer_rewrites_a_healthy_marker(tmp_path, monkeypatch):
    """The original defect: ONE transient EIO used to rename a healthy marker and write a replacement
    (``confirmed_by`` writer -> startup_refsync) while reconcile returned normally."""
    base = three_published_segments(tmp_path)
    node = Node(base)
    victim = node.segs()[1]
    original = marker_path(victim).read_bytes()
    assert json.loads(original)["publication"]["confirmed_by"] == "writer"
    inject_marker_read_error(monkeypatch, only(marker_path(victim)), errno_exc(errno.EIO), nth={1})

    with pytest.raises(DedupStateError):
        node.reconcile()
    assert marker_path(victim).read_bytes() == original
    assert not list(node.stream_dir.glob("*.invalid.*"))
    node.stop()

    node = Node(base)
    node.reconcile()                                     # the fault has passed: nothing to repair
    assert marker_path(victim).read_bytes() == original
    assert (node.co.last_report.confirmed_by_refsync, node.co.last_report.invalid_markers) == (0, 0)
    node.stop()


def test_existing_malformed_marker_preservation_is_unchanged(tmp_path):
    base = three_published_segments(tmp_path)
    node = Node(base)
    victim = node.segs()[0]
    marker_path(victim).write_bytes(MALFORMED)
    node.reconcile()
    kept = list(node.stream_dir.glob("*.meta.json.invalid.*"))
    assert len(kept) == 1 and kept[0].read_bytes() == MALFORMED
    assert read_marker(victim).valid and node.co.last_report.invalid_markers == 1
    node.stop()


def test_genuinely_missing_marker_is_still_confirmed_by_refsync(tmp_path):
    base = three_published_segments(tmp_path)
    node = Node(base)
    victim = node.segs()[2]
    marker_path(victim).unlink()
    node.reconcile()
    assert read_marker(victim).valid and node.co.last_report.confirmed_by_refsync == 1
    assert not list(node.stream_dir.glob("*.invalid.*"))
    node.stop()


# ----- a marker that is readable at the gate but not when re-read later in the same reconciliation

def test_audit_reread_of_a_marker_fails_closed_before_any_rebuild(tmp_path, monkeypatch):
    base = three_published_segments(tmp_path)
    tamper_index(base, "DELETE FROM seen WHERE identity_key = (SELECT identity_key FROM seen ORDER BY 1 LIMIT 1)")
    node = Node(base)
    victim = node.segs()[1]
    fs_before, idx_before = fs_state(base), index_state(base)
    muts = Mutations(monkeypatch).install()
    hits = inject_marker_read_error(monkeypatch, only(marker_path(victim)), errno_exc(errno.EIO), nth={2})

    with pytest.raises(DedupStateError, match="no longer valid"):
        node.reconcile()

    assert len(hits) == 2, "read #1 (the gate) succeeded; read #2 (the audit) failed"
    assert muts.calls == [], "the divergence that would have triggered a rebuild_reset was not acted on"
    assert fs_state(base) == fs_before and index_state(base) == idx_before
    node.stop()


def test_late_reread_fault_never_touches_a_marker_and_the_next_start_completes(tmp_path, monkeypatch):
    base = three_published_segments(tmp_path)
    tamper_index(base, "DELETE FROM reconciled_segments")
    tamper_index(base, "DELETE FROM seen")
    node = Node(base)
    victim = node.segs()[1]
    markers_before = {p.name: marker_path(p).read_bytes() for p in node.segs()}
    muts = Mutations(monkeypatch).install()
    hits = inject_marker_read_error(monkeypatch, only(marker_path(victim)), errno_exc(errno.EIO), nth={3})

    with pytest.raises(DedupStateError, match="unknown_io"):
        node.reconcile()

    assert len(hits) == 3, "gate and audit reads succeeded; the indexing read failed"
    assert not {"preserve_invalid", "orphan_marker", "confirm_unmarked", "write_marker_atomic",
                "rebuild_reset"} & set(muts.calls), muts.calls
    assert {p.name: marker_path(p).read_bytes() for p in node.segs()} == markers_before
    assert not list(node.stream_dir.glob("*.invalid.*"))
    state = index_state(base)
    assert key_for("C") not in {r[0] for r in state["seen"]} and key_for("E") not in {r[0] for r in state["seen"]}, (
        "nothing at or after the failing segment was committed")
    node.stop()

    node = Node(base)
    node.reconcile()
    assert node.index.identity_count() == 5
    assert {p.name: marker_path(p).read_bytes() for p in node.segs()} == markers_before
    node.stop()


# ===================================================================== 8: retroactive confirmation

def test_confirm_unmarked_refuses_unreadable_marker_before_any_write(tmp_path, monkeypatch):
    base = three_published_segments(tmp_path)
    node = Node(base)
    s0, s1, _ = node.segs()
    marker_path(s0).unlink()                                         # genuinely absent -> promotable
    fs_before = fs_state(base)
    muts = Mutations(monkeypatch).install()
    unmarked = [UnmarkedSegment(s0, read_marker(s0)),
                UnmarkedSegment(s1, MarkerResult(MarkerStatus.UNKNOWN_IO, reason="unreadable: EIO"))]

    with pytest.raises(PublicationError, match="unreadable publication marker"):
        confirm_unmarked(unmarked, node.stream_dir, verdict=fs_guard(node.stream_dir))

    assert muts.calls == [], "no fsync, no replacement marker -- not even for the genuinely absent one"
    assert fs_state(base) == fs_before and not marker_path(s0).exists()
    node.stop()


# ===================================================================== 9: publication hook

def test_publication_hook_fails_closed_when_the_marker_cannot_be_inspected(tmp_path, monkeypatch):
    node = Node(tmp_path, segment_rows=5000)
    node.reconcile()
    node.offer("A")
    pending_before = node.co.ram_identity_count
    assert pending_before > 0
    idx_before = index_state(tmp_path)
    inject_marker_read_error(monkeypatch, lambda s: True, errno_exc(errno.EIO))

    node.writer.publish_open_segment()

    seg = node.segs()[0]
    failure = node.writer._publication_failure
    assert isinstance(failure, DedupStateError) and "unknown_io" in str(failure)
    assert node.writer._publication_failure_stage == "publication_hook"
    assert node.co.ram_identity_count == pending_before, "pending identities were NOT released"
    assert not node.index.contains(key_for("A")) and index_state(tmp_path) == idx_before
    assert marker_path(seg).exists() and not list(node.stream_dir.glob("*.invalid.*"))
    assert json.loads(raw_bytes(marker_path(seg)))["publication"]["confirmed_by"] == "writer"
    with pytest.raises(Exception):
        node.writer.write(row("B"))                                  # the writer refuses further rows
    original = raw_bytes(marker_path(seg))
    node.stop()
    monkeypatch.undo()

    node = Node(tmp_path, segment_rows=5000)
    node.reconcile()                                                 # restart indexes the intact segment
    assert node.index.contains(key_for("A"))
    assert marker_path(seg).read_bytes() == original
    assert node.co.last_report.confirmed_by_refsync == 0
    node.stop()


@pytest.mark.parametrize("code", [errno.EIO, errno.EACCES])
def test_hook_called_directly_raises_and_commits_nothing(tmp_path, monkeypatch, code):
    node = Node(tmp_path, segment_rows=5000)
    node.reconcile()
    node.writer.write(row("A"))
    node.writer.publish_open_segment()
    seg = node.segs()[0]
    idx_before, fs_before = index_state(tmp_path), fs_state(tmp_path)
    inject_marker_read_error(monkeypatch, only(marker_path(seg)), errno_exc(code))
    with pytest.raises(DedupStateError, match="unknown_io"):
        node.co.on_segment_published((STREAM, 0), seg)
    assert index_state(tmp_path) == idx_before and fs_state(tmp_path) == fs_before
    node.stop()
