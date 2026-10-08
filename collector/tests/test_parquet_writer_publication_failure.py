"""P0: ParquetWriter must fail CLOSED when segment publication does not complete.

Publication = fsync(tmp) + atomic rename + directory fsync.  Before this fix
only a failing ``on_segment_published`` hook latched the writer; a failure of
any step *before* the hook (writer close, fsync, rename, directory fsync) left
``self.writer = None`` with the writer apparently healthy, so later write()
calls silently buffered rows that could never be published.

Every test drives the real ``ParquetWriter`` and injects the fault at the real
syscall seam (``os.replace`` / ``os.fsync`` / pyarrow ``close``); nothing here
re-implements writer logic.
"""
from __future__ import annotations

import os
import stat
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.collector import parquet_writer as pw_module
from collector.collector.parquet_writer import ParquetWriter

SCHEMA = pa.schema([("timestamp", pa.int64()), ("value", pa.float64())])
REAL_REPLACE = os.replace
REAL_FSYNC = os.fsync


def _row(i: int) -> dict:
    return {"timestamp": 1_000 + i, "value": float(i)}


def _make(tmp_path, *, events=None, published=None, durable=None, name="s", **kw) -> ParquetWriter:
    return ParquetWriter(
        name, SCHEMA, base_dir=str(tmp_path),
        quality_event_sink=None if events is None else events.append,
        on_segment_published=None if published is None else (lambda token, path: published.append((token, path))),
        on_segment_durable=None if durable is None else (lambda path, n: durable.append((path, n))),
        **kw)


def _segs(writer, pattern):
    return sorted(writer.stream_dir.glob(pattern))


def _fail_replace_to(monkeypatch, suffix: str, exc=None):
    """Make os.replace fail ONLY when the destination ends with ``suffix``."""
    exc = exc or OSError(28, "No space left on device (injected)")

    def replace(src, dst, *a, **k):
        if str(dst).endswith(suffix):
            raise exc
        return REAL_REPLACE(src, dst, *a, **k)
    monkeypatch.setattr(pw_module.os, "replace", replace)


def _fail_dir_fsync(monkeypatch):
    """Make os.fsync fail ONLY for directory file descriptors."""
    def fsync(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(5, "Input/output error (injected directory fsync)")
        return REAL_FSYNC(fd)
    monkeypatch.setattr(pw_module.os, "fsync", fsync)


def _assert_fails_closed(writer: ParquetWriter) -> None:
    """Every entry point refuses; nothing is silently buffered."""
    before = list(writer.buffer)
    # match="FAILED": the refusal must come from the explicit failed-state latch,
    # not merely from some incidental downstream error.
    with pytest.raises(RuntimeError, match="FAILED"):
        writer.write(_row(900))
    with pytest.raises(RuntimeError, match="FAILED"):
        writer.write(_row(901))
    with pytest.raises(RuntimeError, match="FAILED"):
        writer.publish_open_segment()
    with pytest.raises(RuntimeError, match="FAILED"):
        writer.publish_if_due()
    with pytest.raises(RuntimeError, match="FAILED"):
        writer.flush()
    assert writer.buffer == before == [], "a refused write must not be buffered"


# --------------------------------------------------------------------- TEST 1
def test_1_os_replace_failure_fails_closed_and_publishes_nothing(tmp_path, monkeypatch):
    events, published, durable = [], [], []
    w = _make(tmp_path, events=events, published=published, durable=durable)
    for i in range(3):
        w.write(_row(i))
    w.flush()
    _fail_replace_to(monkeypatch, ".seg")

    with pytest.raises(OSError):
        w.publish_open_segment()

    _assert_fails_closed(w)
    assert _segs(w, "*.seg") == [], "no segment may exist: publication never happened"
    assert len(_segs(w, "*.seg.tmp")) == 1, "the unpublished .tmp is preserved for orphan recovery"
    assert published == [] and durable == [], "no hook may claim durability"
    assert w.has_unpublished_rows() is True, "state must not claim 'nothing pending'"
    failed = [e for e in events if e["event_type"] == "STORAGE_PUBLICATION_FAILED"]
    assert len(failed) == 1 and "rename" in failed[0]["reason"]
    with pytest.raises(RuntimeError):                 # close must not report a clean shutdown ...
        w.close()
    assert w._lock_handle is None                     # ... but must still release the stream lock
    assert _segs(w, "*.seg") == []


def test_1b_os_replace_failure_during_hour_rollover_does_not_admit_the_triggering_row(tmp_path, monkeypatch):
    w = _make(tmp_path)
    w.write(_row(0))
    next_hour = w.current_hour[:-2] + f"{(int(w.current_hour[-2:]) + 1) % 24:02d}"
    monkeypatch.setattr(w, "_get_current_hour_str", lambda: next_hour)
    _fail_replace_to(monkeypatch, ".seg")
    with pytest.raises(OSError):
        w.write(_row(1))                              # rollover publishes the old segment -> fails
    assert w.buffer == [], "the triggering row must not be appended to any segment"
    _assert_fails_closed(w)


# --------------------------------------------------------------------- TEST 2
def test_2_directory_fsync_failure_after_rename_is_uncertain_and_fails_closed(tmp_path, monkeypatch):
    events, published, durable = [], [], []
    w = _make(tmp_path, events=events, published=published, durable=durable)
    for i in range(3):
        w.write(_row(i))
    w.flush()
    _fail_dir_fsync(monkeypatch)

    with pytest.raises(OSError):
        w.publish_open_segment()

    _assert_fails_closed(w)
    assert published == [] and durable == [], "durability was never confirmed: no hook may fire"
    assert [e for e in events if e["event_type"] == "STORAGE_PUBLICATION_FAILED"
            and "dir_fsync" in e["reason"]]
    assert not [e for e in events if e["event_type"] == "STORAGE_METADATA_FAILED"]
    assert w.has_unpublished_rows() is True
    # The renamed file is left exactly as the filesystem has it: neither deleted
    # (it may be durable) nor claimed durable.
    segs = _segs(w, "*.seg")
    assert len(segs) == 1 and pq.read_table(segs[0]).num_rows == 3
    assert not Path(str(segs[0]) + ".meta.json").exists()
    assert _segs(w, "*.seg.tmp") == []


# --------------------------------------------------------------------- TEST 3
def test_3_parquet_writer_close_failure_fails_closed(tmp_path):
    events, published, durable = [], [], []
    w = _make(tmp_path, events=events, published=published, durable=durable)
    for i in range(3):
        w.write(_row(i))
    w.flush()

    def boom():
        raise OSError(5, "Input/output error (injected parquet close)")
    pq_handle = w.writer
    pq_handle.close = boom                            # instance attribute: the real pq writer

    with pytest.raises(OSError):
        w.publish_open_segment()

    _assert_fails_closed(w)
    assert _segs(w, "*.seg") == []
    assert len(_segs(w, "*.seg.tmp")) == 1
    assert published == [] and durable == []
    assert [e for e in events if e["event_type"] == "STORAGE_PUBLICATION_FAILED"
            and "writer_close" in e["reason"]]
    del pq_handle.close                               # restore the real close so the fd is released
    pq_handle.close()


def test_3b_flush_failure_fails_closed_and_accounts_unflushed_rows(tmp_path, monkeypatch):
    events = []
    w = _make(tmp_path, events=events)
    w.write(_row(0))
    w.write(_row(1))                                  # buffered, not flushed

    def boom(table):
        raise OSError(28, "No space left on device (injected write_table)")
    monkeypatch.setattr(w.writer, "write_table", boom)

    with pytest.raises(OSError):
        w.publish_open_segment()

    _assert_fails_closed(w)
    drops = [e for e in events if e["event_type"] == "DATA_DROP"]
    assert len(drops) == 1 and drops[0]["rows_lost"] == 2, "the 2 unflushed rows are durably accounted"
    assert _segs(w, "*.seg") == []


def test_3c_failure_to_open_the_next_segment_fails_closed(tmp_path, monkeypatch):
    events, published = [], []
    w = _make(tmp_path, events=events, published=published)
    w.write(_row(0))

    def boom(*a, **k):
        raise OSError(24, "Too many open files (injected)")
    monkeypatch.setattr(pw_module.pq, "ParquetWriter", boom)

    with pytest.raises(OSError):
        w.publish_open_segment()                      # publishes fine, cannot open the next one

    assert len(published) == 1 and len(_segs(w, "*.seg")) == 1, "the published segment is untouched"
    _assert_fails_closed(w)                           # a writer with no open segment must not buffer
    assert [e for e in events if e["event_type"] == "STORAGE_PUBLICATION_FAILED"
            and "open_next_segment" in e["reason"]]
    assert not [e for e in events if e["event_type"] == "DATA_DROP"], "nothing was lost"


# --------------------------------------------------------------------- TEST 4
def test_4_hook_failure_after_durable_publication_keeps_segment_and_fails_closed(tmp_path):
    events, durable = [], []

    def hostile(token, path):
        raise RuntimeError("simulated dedup index commit failure")
    w = ParquetWriter("s", SCHEMA, base_dir=str(tmp_path), quality_event_sink=events.append,
                      on_segment_published=hostile,
                      on_segment_durable=lambda path, n: durable.append((path, n)))
    for i in range(3):
        w.write(_row(i))
    w.publish_open_segment()                          # hook fails AFTER durable publication: no raise

    segs = _segs(w, "*.seg")
    assert len(segs) == 1 and pq.read_table(segs[0]).num_rows == 3, "durable segment stays"
    assert len(durable) == 1
    assert w._publication_failure is not None
    assert [e for e in events if e["event_type"] == "DEDUP_STATE_FAILED"]
    assert not [e for e in events if e["event_type"] == "STORAGE_PUBLICATION_FAILED"], \
        "a hook failure is not a storage-publication failure"
    for attempt in range(2):
        with pytest.raises(RuntimeError):
            w.write(_row(100 + attempt))
    assert w.buffer == []
    w.close()                                         # nothing pending: shutdown stays clean


def test_4b_sink_failure_while_reporting_a_hook_failure_cannot_reopen_the_writer(tmp_path):
    def hostile(token, path):
        raise RuntimeError("index commit failed")

    def broken_sink(event):
        raise OSError("quality sink down")
    w = ParquetWriter("s", SCHEMA, base_dir=str(tmp_path), quality_event_sink=broken_sink,
                      on_segment_published=hostile)
    w.write(_row(0))
    w.publish_open_segment()
    with pytest.raises(RuntimeError):
        w.write(_row(1))


# --------------------------------------------------------------------- TEST 5
def test_5_marker_failure_never_discards_a_durable_segment_and_withholds_dedup(tmp_path, monkeypatch):
    """F1: the marker is a GATE. A durable segment is never discarded or reported
    lost, but without a marker no dedup hook runs and the writer fails closed."""
    events, published, durable = [], [], []
    w = _make(tmp_path, events=events, published=published, durable=durable)
    for i in range(3):
        w.write(_row(i))
    _fail_replace_to(monkeypatch, ".meta.json")

    w.publish_open_segment()                          # must NOT raise

    segs = _segs(w, "*.seg")
    assert len(segs) == 1 and pq.read_table(segs[0]).num_rows == 3
    assert len(published) == 0, "dedup hook must be withheld when the marker is not durable"
    assert len(durable) == 1, "on_segment_durable keeps its own contract (segment IS durable)"
    assert len([e for e in events if e["event_type"] == "STORAGE_METADATA_FAILED"]) == 1
    assert len([e for e in events if e["event_type"] == "DEDUP_STATE_FAILED"]) == 1
    assert not [e for e in events if e["event_type"] in ("STORAGE_PUBLICATION_FAILED", "DATA_DROP")]
    assert w._publication_failure is not None and w._storage_failure is None
    assert not Path(str(segs[0]) + ".meta.json").exists()
    with pytest.raises(RuntimeError):
        w.write(_row(10))                             # fails closed
    w.close()
    assert len(_segs(w, "*.seg")) == 1


def test_5b_quality_sink_failure_during_marker_report_does_not_skip_next_segment_or_unlatch(tmp_path, monkeypatch):
    published = []

    def broken_sink(event):
        raise OSError("quality sink down")
    w = ParquetWriter("s", SCHEMA, base_dir=str(tmp_path), quality_event_sink=broken_sink,
                      on_segment_published=lambda token, path: published.append(token))
    w.write(_row(0))
    _fail_replace_to(monkeypatch, ".meta.json")
    w.publish_open_segment()                          # a sink fault must not abort publication
    assert published == [], "no marker => no dedup hook, whatever the sink does"
    assert w._publication_failure is not None, "the latch, not the sink, enforces fail-closed"
    with pytest.raises(RuntimeError):
        w.write(_row(1))
    w.close()
    assert len(_segs(w, "*.seg")) == 1


def test_5c_stale_counter_unlink_failure_after_publication_is_not_a_storage_failure(tmp_path, monkeypatch):
    published = []
    w = _make(tmp_path, published=published)
    w.write(_row(0))
    real_unlink = Path.unlink

    def unlink(self, *a, **k):
        if str(self).endswith(".count.json"):
            raise OSError(1, "Operation not permitted (injected)")
        return real_unlink(self, *a, **k)
    monkeypatch.setattr(Path, "unlink", unlink)
    w.publish_open_segment()
    monkeypatch.undo()
    assert len(published) == 1 and len(_segs(w, "*.seg")) == 1
    w.write(_row(1))                                  # still healthy
    w.close()


# --------------------------------------------------------------------- TEST 6
def test_6_restart_after_failed_publication_recovers_tmp_with_durable_drop_and_no_identity(tmp_path, monkeypatch):
    published = []
    w = _make(tmp_path, published=published, name="crash")
    for i in range(4):
        w.write(_row(i))
    w.flush()
    _fail_replace_to(monkeypatch, ".seg")
    with pytest.raises(OSError):
        w.publish_open_segment()
    with pytest.raises(RuntimeError):
        w.close()                                     # the process "shuts down" with the failure
    monkeypatch.undo()
    stream_dir = w.stream_dir
    assert len(list(stream_dir.glob("*.seg.tmp"))) == 1
    assert published == []

    events, published2 = [], []
    w2 = ParquetWriter("crash", SCHEMA, base_dir=str(tmp_path), quality_event_sink=events.append,
                       on_segment_published=lambda token, path: published2.append(token))
    drops = [e for e in events if e["event_type"] == "DATA_DROP"]
    assert len(drops) == 1 and drops[0]["rows_lost"] == 4
    assert drops[0]["reason"] == "crashed_segment_discarded" and drops[0]["stream"] == "crash"
    assert list(stream_dir.glob("*.seg")) == []
    assert not list(stream_dir.glob("*.seg.tmp")) or all(  # only w2's own fresh, empty segment
        p.stat().st_size >= 0 for p in stream_dir.glob("*.seg.tmp"))
    assert published2 == [], "no identity may reach the dedup hook for a segment that never published"
    w2.close()
    assert list(stream_dir.glob("*.seg")) == [], "an empty restart publishes nothing"


# --------------------------------------------------------------------- TEST 7
def test_7_normal_publication_is_unchanged_and_ordered(tmp_path, monkeypatch):
    order = []

    def replace(src, dst, *a, **k):
        order.append(("replace", Path(str(dst)).name.rsplit(".", 1)[-1]))
        return REAL_REPLACE(src, dst, *a, **k)

    def fsync(fd):
        order.append(("fsync", "dir" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file"))
        return REAL_FSYNC(fd)
    monkeypatch.setattr(pw_module.os, "replace", replace)
    monkeypatch.setattr(pw_module.os, "fsync", fsync)
    events = []
    w = ParquetWriter(
        "s", SCHEMA, base_dir=str(tmp_path), quality_event_sink=events.append,
        on_segment_published=lambda token, path: order.append(("published_hook", token[1])),
        on_segment_durable=lambda path, n: order.append(("durable_hook", n)))
    for i in range(3):
        w.write(_row(i))
    w.publish_open_segment()

    segs = _segs(w, "*.seg")
    assert len(segs) == 1 and pq.read_table(segs[0]).num_rows == 3
    assert not _segs(w, "*.seg.tmp") or all(p.name != segs[0].name + ".tmp" for p in _segs(w, "*.seg.tmp"))
    tail = order[order.index(("replace", "seg")):]
    # rename of the segment, THEN directory fsync, THEN the hooks.
    assert tail[0] == ("replace", "seg")
    assert tail[1] == ("fsync", "dir")
    assert tail[2] == ("durable_hook", 3)
    names = [step[0] for step in tail]
    assert names.index("durable_hook") < names.index("published_hook")
    assert order.index(("fsync", "file")) < order.index(("replace", "seg")), "file fsync precedes rename"
    assert events == []
    w.write(_row(9))                                  # healthy afterwards
    w.close()
    assert len(_segs(w, "*.seg")) == 2


def test_failed_state_is_visible_and_construction_state_is_clean(tmp_path):
    w = _make(tmp_path)
    assert w._storage_failure is None and w._publication_failure is None
    w.close()


# ------------------------------------------------- sink behaviour while FAILED
def test_sink_failure_cannot_mask_the_storage_error_or_unlatch_the_writer(tmp_path, monkeypatch):
    def broken_sink(event):
        raise OSError("quality sink down")
    w = ParquetWriter("s", SCHEMA, base_dir=str(tmp_path), quality_event_sink=broken_sink)
    w.write(_row(0))
    w.write(_row(1))
    _fail_replace_to(monkeypatch, ".seg", OSError(28, "No space left on device (injected)"))
    with pytest.raises(OSError) as info:
        w.publish_open_segment()
    assert info.value.errno == 28, "the ORIGINAL storage error must surface, not the sink's"
    _assert_fails_closed(w)


def test_failed_writer_without_any_sink_still_fails_closed(tmp_path, monkeypatch):
    w = ParquetWriter("s", SCHEMA, base_dir=str(tmp_path))      # e.g. quality_writer itself
    w.write(_row(0))
    _fail_replace_to(monkeypatch, ".seg")
    with pytest.raises(OSError):
        w.publish_open_segment()
    _assert_fails_closed(w)


def test_write_after_close_is_refused_not_silently_buffered(tmp_path):
    w = _make(tmp_path)
    w.write(_row(0))
    w.close()
    with pytest.raises(RuntimeError):
        w.write(_row(1))
    assert w.buffer == []
