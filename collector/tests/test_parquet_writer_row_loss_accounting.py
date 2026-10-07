"""PR #100 follow-up: F2 row-loss accounting and F4 open-next-segment semantics.

F2 -- a failed segment must never UNDER-report the rows a restart will discard.
The ``.count.json`` sidecar can be missing, stale, or fail to persist; the old
code reported only what the sidecar said (often 0 / None / a stale figure).

THE RULE under test (see ``ParquetWriter._fail_closed``): every row of an
unpublished segment is reported lost exactly once --
  * rows covered by the sidecar that is on disk at failure -> the RESTART's
    DATA_DROP (``crashed_segment_discarded``);
  * every other row (written after the last sidecar that landed, or with no
    sidecar, plus RAM-only rows)                              -> reported NOW.
No usable sidecar -> the restart event says ``rows_lost=None`` (UNKNOWN), never 0.

F4 -- a segment that published fully (fsync + rename + dir fsync + hooks) and
whose NEXT segment then failed to open has lost nothing: ``rows_in_segment`` is
0, ``has_unpublished_rows()`` is False, no DATA_DROP, a distinct reason -- and
the writer still fails closed.

Every fault is injected at a real seam (``os.replace``, ``pq.ParquetWriter``,
``write_table``, a real ``RLIMIT_FSIZE`` subprocess); nothing re-implements the
writer.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from collector.collector import parquet_writer as pw_module
from collector.collector.parquet_writer import ParquetWriter
from collector.tests.test_parquet_writer_publication_failure import (
    SCHEMA, _assert_fails_closed, _fail_dir_fsync, _fail_replace_to, _make, _row, _segs)

TMP_ROWS_REASON = "tmp_rows_not_covered_by_counter_discarded_on_publication_failure"
RESTART_REASON = "crashed_segment_discarded"
BIG = 10_000  # segment_rows large enough that nothing auto-rotates


def _of(events, event_type):
    return [e for e in events if e["event_type"] == event_type]


def _numeric(events):
    """Sum of the NUMERIC DATA_DROP rows_lost; an UNKNOWN (None) adds nothing."""
    return sum(e["rows_lost"] for e in _of(events, "DATA_DROP") if isinstance(e["rows_lost"], int))


def _restart(tmp_path, name="s"):
    """What a restarted process reports. A fresh, clean writer is created and closed."""
    events = []
    w = ParquetWriter(name, SCHEMA, base_dir=str(tmp_path), quality_event_sink=events.append)
    w.close()
    return events


def _shutdown(w):
    """The process 'exits': close() on a FAILED writer raises but releases the lock."""
    try:
        w.close()
    except RuntimeError:
        pass
    assert w._lock_handle is None


def _sidecar(w):
    paths = sorted(w.stream_dir.glob("*.count.json"))
    return paths[0] if paths else None


def _fill(w, n, start=0):
    for i in range(start, start + n):
        w.write(_row(i))


# ============================================================ F2: accounting
def test_f2_1_counter_persist_fails_after_write_table_with_no_previous_sidecar(tmp_path, monkeypatch):
    events = []
    w = _make(tmp_path, events=events, segment_rows=BIG)
    _fill(w, 3)
    _fail_replace_to(monkeypatch, ".count.json")        # write_table succeeds, persisting the counter fails

    with pytest.raises(OSError):
        w.flush()

    assert w.record_count == 3, "the rows ARE in the .tmp"
    assert _sidecar(w) is None, "no counter ever landed"
    _assert_fails_closed(w)
    assert _segs(w, "*.seg") == [] and len(_segs(w, "*.seg.tmp")) == 1
    failed = _of(events, "STORAGE_PUBLICATION_FAILED")
    assert len(failed) == 1
    assert "rows_in_segment=3" in failed[0]["reason"]
    assert "counter_sidecar=none_persisted" in failed[0]["reason"]
    assert "rows_in_tmp_not_in_counter=3" in failed[0]["reason"]
    drops = _of(events, "DATA_DROP")
    assert [(d["rows_lost"], d["reason"]) for d in drops] == [(3, TMP_ROWS_REASON)], \
        "all 3 rows reported NOW: nothing under-reported, nothing relies on a sidecar that does not exist"
    assert w.has_unpublished_rows() is True

    _shutdown(w)
    monkeypatch.undo()
    restart = _restart(tmp_path)
    r_drops = _of(restart, "DATA_DROP")
    assert len(r_drops) == 1 and r_drops[0]["reason"] == RESTART_REASON
    assert r_drops[0]["rows_lost"] is None, "UNKNOWN is reported as UNKNOWN: never fabricated as 0"
    assert _numeric(events) + _numeric(restart) == 3, "3 rows written, 3 rows reported: no under- or over-count"


def test_f2_2_counter_persist_fails_after_a_previous_persisted_count(tmp_path, monkeypatch):
    events = []
    w = _make(tmp_path, events=events, segment_rows=BIG)
    _fill(w, 300)
    w.flush()                                            # counter lands: 300
    assert json.loads(_sidecar(w).read_text()) == {"rows": 300}
    _fill(w, 200, start=300)
    _fail_replace_to(monkeypatch, ".count.json")        # 200 more rows reach the .tmp; counter stays 300

    with pytest.raises(OSError):
        w.flush()

    assert w.record_count == 500
    assert json.loads(_sidecar(w).read_text()) == {"rows": 300}, "the sidecar is now stale"
    failed = _of(events, "STORAGE_PUBLICATION_FAILED")[0]["reason"]
    assert "rows_in_segment=500" in failed and "counter_sidecar=current(rows=300)" in failed
    assert "rows_in_tmp_not_in_counter=200" in failed
    assert [(d["rows_lost"], d["reason"]) for d in _of(events, "DATA_DROP")] == [(200, TMP_ROWS_REASON)]

    _shutdown(w)
    monkeypatch.undo()
    restart = _restart(tmp_path)
    assert [d["rows_lost"] for d in _of(restart, "DATA_DROP")] == [300], "the restart reports exactly the sidecar"
    total = _numeric(events) + _numeric(restart)
    assert total == 500, "all 500 rows accounted for"
    assert total != 300, "the old under-report (the stale sidecar's figure alone)"


@pytest.mark.parametrize("flushed_rows", [1, 300])
def test_f2_3_missing_counter_sidecar_is_explicit_unknown_never_a_fabricated_zero(tmp_path, monkeypatch, flushed_rows):
    events = []
    w = _make(tmp_path, events=events, segment_rows=BIG)
    _fill(w, flushed_rows)
    w.flush()
    _sidecar(w).unlink()                                 # the sidecar disappears under the writer
    _fail_replace_to(monkeypatch, ".seg")

    with pytest.raises(OSError):
        w.publish_open_segment()

    reason = _of(events, "STORAGE_PUBLICATION_FAILED")[0]["reason"]
    assert f"counter_sidecar=MISSING_OR_UNREADABLE(expected_rows={flushed_rows})" in reason
    assert [(d["rows_lost"], d["reason"]) for d in _of(events, "DATA_DROP")] == [(flushed_rows, TMP_ROWS_REASON)]

    _shutdown(w)
    monkeypatch.undo()
    restart = _restart(tmp_path)
    r_drops = _of(restart, "DATA_DROP")
    assert len(r_drops) == 1 and r_drops[0]["rows_lost"] is None and r_drops[0]["rows_lost"] != 0
    assert _numeric(events) + _numeric(restart) == flushed_rows


def test_f2_4_stale_counter_sidecar_uses_the_known_boundary(tmp_path, monkeypatch):
    events = []
    w = _make(tmp_path, events=events, segment_rows=BIG)
    _fill(w, 300)
    w.flush()
    _sidecar(w).write_text(json.dumps({"rows": 100}))    # stale: claims 100, the .tmp holds 300
    _fail_replace_to(monkeypatch, ".seg")

    with pytest.raises(OSError):
        w.publish_open_segment()

    reason = _of(events, "STORAGE_PUBLICATION_FAILED")[0]["reason"]
    assert "counter_sidecar=DIVERGED(disk_rows=100,tracked_rows=300)" in reason
    assert [(d["rows_lost"], d["reason"]) for d in _of(events, "DATA_DROP")] == [(200, TMP_ROWS_REASON)]
    _shutdown(w)
    monkeypatch.undo()
    restart = _restart(tmp_path)
    assert [d["rows_lost"] for d in _of(restart, "DATA_DROP")] == [100]
    assert _numeric(events) + _numeric(restart) == 300


@pytest.mark.parametrize("content", ['{"rows": -5}', "not json at all", '{"rows": true}', '{"rows": 3.5}', "[]", ""])
def test_f2_4b_unusable_sidecar_content_is_unknown_not_zero_and_not_negative(tmp_path, monkeypatch, content):
    events = []
    w = _make(tmp_path, events=events, segment_rows=BIG)
    _fill(w, 300)
    w.flush()
    _sidecar(w).write_text(content)
    _fail_replace_to(monkeypatch, ".seg")
    with pytest.raises(OSError):
        w.publish_open_segment()
    assert [d["rows_lost"] for d in _of(events, "DATA_DROP")] == [300], "an unusable sidecar covers NOTHING"
    _shutdown(w)
    monkeypatch.undo()
    restart = _restart(tmp_path)
    r_drops = _of(restart, "DATA_DROP")
    assert len(r_drops) == 1 and r_drops[0]["rows_lost"] is None, \
        "a negative / malformed counter must not silently suppress or distort the restart event"


def test_f2_4c_sidecar_overstating_the_tmp_is_flagged_and_never_produces_a_negative_drop(tmp_path, monkeypatch):
    events = []
    w = _make(tmp_path, events=events, segment_rows=BIG)
    _fill(w, 10)
    w.flush()
    _sidecar(w).write_text(json.dumps({"rows": 999}))
    _fail_replace_to(monkeypatch, ".seg")
    with pytest.raises(OSError):
        w.publish_open_segment()
    reason = _of(events, "STORAGE_PUBLICATION_FAILED")[0]["reason"]
    assert "EXCEEDS_ROWS_WRITTEN(10)" in reason and "rows_in_tmp_not_in_counter=0" in reason
    assert _of(events, "DATA_DROP") == [], "nothing uncovered: no negative or invented drop"


def test_f2_5_buffered_and_persisted_and_uncovered_rows_are_each_reported_exactly_once(tmp_path, monkeypatch):
    """300 covered by the sidecar + 200 written but uncovered + 40 only in RAM."""
    events = []
    w = _make(tmp_path, events=events, segment_rows=BIG)
    _fill(w, 300)
    w.flush()                                            # sidecar = 300
    _fill(w, 200, start=300)
    with monkeypatch.context() as m:
        _fail_replace_to(m, ".count.json")
        with pytest.raises(OSError):
            w.flush()                                    # 200 more in the .tmp, sidecar stays 300 -> FAILED
    assert w.record_count == 500 and w._storage_failure is not None
    assert [(d["rows_lost"], d["reason"]) for d in _of(events, "DATA_DROP")] == [(200, TMP_ROWS_REASON)]
    _shutdown(w)
    restart = _restart(tmp_path)
    assert _numeric(events) + _numeric(restart) == 500

    # RAM-only rows on a writer whose write_table itself fails.
    events2 = []
    w2 = _make(tmp_path, events=events2, name="ram", segment_rows=BIG)
    _fill(w2, 300)
    w2.flush()
    _fill(w2, 40, start=300)

    def boom(table):
        raise OSError(28, "No space left on device (injected write_table)")
    monkeypatch.setattr(w2.writer, "write_table", boom)
    with pytest.raises(OSError):
        w2.flush()
    drops = {d["reason"]: d["rows_lost"] for d in _of(events2, "DATA_DROP")}
    assert drops == {"unflushed_rows_discarded_on_publication_failure": 40}, \
        "300 are covered by the sidecar (restart reports them); only the 40 RAM rows are reported now"
    _shutdown(w2)
    restart2 = _restart(tmp_path, name="ram")
    assert _numeric(events2) + _numeric(restart2) == 340


def test_f2_5c_counter_state_does_not_leak_from_a_published_segment_into_the_next(tmp_path, monkeypatch):
    events = []
    w = _make(tmp_path, events=events, segment_rows=BIG)
    _fill(w, 3)
    w.publish_open_segment()                             # segment 0 durable; its counter had landed (3)
    _fill(w, 2, start=10)                                # segment 1: no counter has EVER landed for it
    _fail_replace_to(monkeypatch, ".count.json")
    with pytest.raises(OSError):
        w.flush()
    reason = _of(events, "STORAGE_PUBLICATION_FAILED")[0]["reason"]
    assert "counter_sidecar=none_persisted" in reason, "segment 0's counter must not be mistaken for segment 1's"
    assert "rows_in_segment=2" in reason and "rows_in_tmp_not_in_counter=2" in reason
    assert [(d["rows_lost"], d["reason"]) for d in _of(events, "DATA_DROP")] == [(2, TMP_ROWS_REASON)]


def test_f2_5b_failed_counter_sidecar_leaves_nothing_that_is_published(tmp_path, monkeypatch):
    events, published, durable = [], [], []
    w = _make(tmp_path, events=events, published=published, durable=durable, segment_rows=BIG)
    _fill(w, 5)
    _fail_replace_to(monkeypatch, ".count.json")
    with pytest.raises(OSError):
        w.publish_open_segment()
    assert _segs(w, "*.seg") == [] and published == [] and durable == []


# ----------------------------------------------- F2 partial-write (real fault)
_FSIZE_SCRIPT = r"""
import json, random, resource, signal, sys
import pyarrow as pa
from collector.collector.parquet_writer import ParquetWriter
base, result = sys.argv[1], sys.argv[2]
S = pa.schema([("timestamp", pa.int64()), ("value", pa.float64())])
events = []
w = ParquetWriter("fs", S, base_dir=base, quality_event_sink=events.append, segment_rows=100_000)
signal.signal(signal.SIGXFSZ, signal.SIG_IGN)               # EFBIG instead of being killed
soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
resource.setrlimit(resource.RLIMIT_FSIZE, (4096, hard))     # the .tmp can never exceed 4 KiB
rng, out = random.Random(7), {}
def attempt(name, fn):
    try:
        fn(); out[name] = "ok"
    except BaseException as exc:
        out[name] = type(exc).__name__
def feed():
    for i in range(3000):
        w.write({"timestamp": i, "value": rng.random()})
attempt("write", feed)
attempt("flush", w.flush)
attempt("publish", w.publish_open_segment)
attempt("close", w.close)
resource.setrlimit(resource.RLIMIT_FSIZE, (soft, hard))
out["events"] = [[e["event_type"], e["rows_lost"]] for e in events]
out["failed"] = w._storage_failure is not None
open(result, "w").write(json.dumps(out))
"""


def test_f2_6_partial_write_fault_is_never_published(tmp_path):
    result = tmp_path / "result.json"
    proc = subprocess.run(
        [sys.executable, "-c", _FSIZE_SCRIPT, str(tmp_path), str(result)],
        cwd=str(Path(__file__).resolve().parents[2]), capture_output=True, text=True, timeout=120)
    assert result.exists(), proc.stderr[-2000:]
    out = json.loads(result.read_text())
    stream_dir = tmp_path / "raw" / "fs"

    assert out["write"] == "OSError", "the real EFBIG surfaced from write_table"
    assert out["failed"] is True
    assert out["flush"] == out["publish"] == out["close"] == "RuntimeError", "everything after it fails closed"
    tmps = sorted(stream_dir.glob("*.seg.tmp"))
    assert len(tmps) == 1 and tmps[0].stat().st_size <= 4096, "a truncated .tmp really exists"
    assert sorted(stream_dir.glob("*.seg")) == [], "a partially written segment must NEVER be published"
    with pytest.raises(Exception):
        pq.read_table(tmps[0])                           # it is not a readable parquet file
    kinds = [e[0] for e in out["events"]]
    assert "STORAGE_PUBLICATION_FAILED" in kinds
    assert ("DATA_DROP", 1000) in [tuple(e) for e in out["events"]], "the 1000 RAM-only rows are reported now"

    restart = _restart(tmp_path, name="fs")
    r_drops = _of(restart, "DATA_DROP")
    assert len(r_drops) == 1 and r_drops[0]["reason"] == RESTART_REASON and r_drops[0]["rows_lost"] is None
    assert sorted(stream_dir.glob("*.seg")) == [], "restart publishes nothing from the truncated file"


# =========================================================== F4: open-next
def _fail_open_next(monkeypatch, *, leave_header: bool = False):
    """Make opening the NEXT segment fail at pq.ParquetWriter (optionally after
    it already created an empty .tmp, as a real mid-open failure can)."""
    def failing(path, *a, **k):
        if leave_header:
            Path(path).write_bytes(b"PAR1")
        raise OSError(24, "Too many open files (injected)")
    monkeypatch.setattr(pw_module.pq, "ParquetWriter", failing)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_f4_7_published_then_open_next_fails_loses_nothing_and_fails_closed(tmp_path, monkeypatch):
    events, published, durable = [], [], []
    w = _make(tmp_path, events=events, published=published, durable=durable)
    _fill(w, 3)
    _fail_open_next(monkeypatch, leave_header=True)

    with pytest.raises(OSError):
        w.publish_open_segment()                         # publishes fine, cannot open the next one

    segs = _segs(w, "*.seg")
    assert len(segs) == 1 and pq.read_table(segs[0]).num_rows == 3, "first segment holds the 3 rows"
    assert _of(events, "DATA_DROP") == [], "no DATA_DROP for rows that are durably published"
    failed = _of(events, "STORAGE_PUBLICATION_FAILED")
    assert len(failed) == 1
    reason = failed[0]["reason"]
    assert reason.startswith("failed to open next segment"), reason
    assert "segment publication failed" not in reason, "must not be described as a failed publication"
    assert "rows_in_segment=0" in reason and "NOT lost" in reason and "already published" in reason
    assert "rows_in_segment=3" not in reason
    assert w._storage_failure_rows == 0
    assert w.has_unpublished_rows() is False
    assert w._storage_failure is not None, "still FAILED: no healthy open segment exists"
    assert _segs(w, "*.seg.tmp") == [], "the empty .tmp the failed open left behind is removed"

    _assert_fails_closed(w)                              # write / publish / publish_if_due / flush all refuse
    with pytest.raises(RuntimeError, match="FAILED"):
        w._finalize_segment()
    with pytest.raises(RuntimeError, match="failed to open next segment") as info:
        w.close()
    assert "publication did not complete" not in str(info.value), "the finalize message must not blame the published segment"
    assert w._lock_handle is None
    assert len(_segs(w, "*.seg")) == 1 and pq.read_table(_segs(w, "*.seg")[0]).num_rows == 3


def test_f4_7b_open_next_failure_through_an_hour_rollover_has_the_same_semantics(tmp_path, monkeypatch):
    events, published = [], []
    w = _make(tmp_path, events=events, published=published)
    _fill(w, 3)
    next_hour = w.current_hour[:-2] + f"{(int(w.current_hour[-2:]) + 1) % 24:02d}"
    monkeypatch.setattr(w, "_get_current_hour_str", lambda: next_hour)
    _fail_open_next(monkeypatch)

    with pytest.raises(OSError):
        w.write(_row(99))                                # publishes the old hour, cannot open the new one

    assert w.buffer == [], "the triggering row is not admitted anywhere"
    assert len(_segs(w, "*.seg")) == 1 and len(published) == 1
    assert _of(events, "DATA_DROP") == []
    reason = _of(events, "STORAGE_PUBLICATION_FAILED")[0]["reason"]
    assert reason.startswith("failed to open next segment") and "rows_in_segment=0" in reason
    assert w.has_unpublished_rows() is False
    _assert_fails_closed(w)


def test_f4_7c_a_broken_sink_cannot_unlatch_or_change_the_open_next_semantics(tmp_path, monkeypatch):
    def broken_sink(event):
        raise OSError("quality sink down")
    w = ParquetWriter("s", SCHEMA, base_dir=str(tmp_path), quality_event_sink=broken_sink)
    _fill(w, 3)
    _fail_open_next(monkeypatch)
    with pytest.raises(OSError) as info:
        w.publish_open_segment()
    assert info.value.errno == 24, "the ORIGINAL error surfaces, not the sink's"
    assert w.has_unpublished_rows() is False
    _assert_fails_closed(w)


def test_f4_8_restart_after_open_next_failure_sees_a_normal_published_segment(tmp_path, monkeypatch):
    events = []
    w = _make(tmp_path, events=events)
    _fill(w, 3)
    _fail_open_next(monkeypatch, leave_header=True)
    with pytest.raises(OSError):
        w.publish_open_segment()
    seg = _segs(w, "*.seg")[0]
    before = _sha(seg)
    _shutdown(w)
    monkeypatch.undo()

    restart, published2 = [], []
    w2 = ParquetWriter("s", SCHEMA, base_dir=str(tmp_path), quality_event_sink=restart.append,
                       on_segment_published=lambda token, path: published2.append(token))
    assert restart == [], "restart fabricates no loss and no quality event of any kind"
    assert _sha(seg) == before and pq.read_table(seg).num_rows == 3, "the published segment is byte-identical"
    assert w2._storage_failure is None
    w2.write(_row(10))                                   # a normal, healthy writer
    w2.close()
    segs = _segs(w2, "*.seg")
    assert [pq.read_table(p).num_rows for p in segs] == [3, 1], "the sequence continues; nothing is overwritten"
    assert len(published2) == 1
    assert restart == [], "still nothing"


def test_f4_9_hooks_for_the_published_segment_ran_before_the_failure_and_never_again(tmp_path, monkeypatch):
    events, published, durable = [], [], []
    w = _make(tmp_path, events=events, published=published, durable=durable)
    _fill(w, 3)
    _fail_open_next(monkeypatch)
    with pytest.raises(OSError):
        w.publish_open_segment()

    seg = _segs(w, "*.seg")[0]
    assert published == [((w.current_hour, 0), seg)], "dedup hook ran, with the published segment's identity"
    assert durable == [(seg, 3)], "durable hook ran with the published row count"
    for attempt in (lambda: w.write(_row(1)), w.publish_open_segment, w.publish_if_due, w.flush):
        with pytest.raises(RuntimeError):
            attempt()
    _shutdown(w)
    assert len(published) == 1 and len(durable) == 1, "a failed writer never re-runs a hook"


def test_f4_10_a_genuine_next_segment_with_rows_is_still_accounted_as_unpublished(tmp_path, monkeypatch):
    """The F4 fix must not blind the writer: if the NEXT segment really holds
    rows and then fails to publish, they are unpublished and reported."""
    events = []
    w = _make(tmp_path, events=events, segment_rows=BIG)
    _fill(w, 3)
    w.publish_open_segment()                             # segment 0 published; segment 1 opens fine
    _fill(w, 2, start=10)
    w.flush()
    _fail_replace_to(monkeypatch, ".seg")
    with pytest.raises(OSError):
        w.publish_open_segment()
    assert w.has_unpublished_rows() is True and w._storage_failure_rows == 2
    reason = _of(events, "STORAGE_PUBLICATION_FAILED")[0]["reason"]
    assert reason.startswith("segment publication failed at rename") and "rows_in_segment=2" in reason
    assert len(_segs(w, "*.seg")) == 1 and pq.read_table(_segs(w, "*.seg")[0]).num_rows == 3


# ================================================ state-machine matrix (A-G)
def _scn_a_rename(tmp_path, mp, events, published, durable):
    w = _make(tmp_path, events=events, published=published, durable=durable, segment_rows=BIG)
    _fill(w, 3); w.flush()
    _fail_replace_to(mp, ".seg")
    with pytest.raises(OSError):
        w.publish_open_segment()
    return w


def _scn_b_dir_fsync(tmp_path, mp, events, published, durable):
    w = _make(tmp_path, events=events, published=published, durable=durable, segment_rows=BIG)
    _fill(w, 3); w.flush()
    _fail_dir_fsync(mp)
    with pytest.raises(OSError):
        w.publish_open_segment()
    return w


def _scn_c_open_next(tmp_path, mp, events, published, durable):
    w = _make(tmp_path, events=events, published=published, durable=durable, segment_rows=BIG)
    _fill(w, 3)
    _fail_open_next(mp)
    with pytest.raises(OSError):
        w.publish_open_segment()
    return w


def _scn_d_hook_failure(tmp_path, mp, events, published, durable):
    def hostile(token, path):
        raise RuntimeError("simulated dedup index commit failure")
    w = ParquetWriter("s", SCHEMA, base_dir=str(tmp_path), quality_event_sink=events.append,
                      on_segment_published=hostile,
                      on_segment_durable=lambda path, n: durable.append((path, n)), segment_rows=BIG)
    _fill(w, 3)
    w.publish_open_segment()                             # durable; hook failure does not raise
    return w


def _scn_e_metadata(tmp_path, mp, events, published, durable):
    w = _make(tmp_path, events=events, published=published, durable=durable, segment_rows=BIG)
    _fill(w, 3)
    _fail_replace_to(mp, ".meta.json")
    w.publish_open_segment()
    return w


def _scn_f_flush(tmp_path, mp, events, published, durable):
    w = _make(tmp_path, events=events, published=published, durable=durable, segment_rows=BIG)
    _fill(w, 2)                                          # buffered only

    def boom(table):
        raise OSError(28, "No space left on device (injected write_table)")
    mp.setattr(w.writer, "write_table", boom)
    with pytest.raises(OSError):
        w.publish_open_segment()
    return w


def _scn_g_close(tmp_path, mp, events, published, durable):
    w = _make(tmp_path, events=events, published=published, durable=durable, segment_rows=BIG)
    _fill(w, 3)
    w.close()
    return w


#   name: (scenario, segs, tmps, (published_hooks, durable_hooks), writes_refused, unpublished,
#          failure_rows, durability, immediate_drops, restart_drops)
MATRIX = {
    "A_fail_before_rename":    (_scn_a_rename,       0, 1, (0, 0), True,  True,  3,    "unpublished", [],      [3]),
    "B_fail_after_rename":     (_scn_b_dir_fsync,    1, 0, (0, 0), True,  True,  3,    "unconfirmed", [],      []),
    "C_open_next_failed":      (_scn_c_open_next,    1, 0, (1, 1), True,  False, 0,    "published",   [],      []),
    "D_dedup_hook_failed":     (_scn_d_hook_failure, 1, 1, (0, 1), True,  False, None, None,          [],      []),
    "E_metadata_failed":       (_scn_e_metadata,     1, 1, (1, 1), False, False, None, None,          [],      []),
    "F_flush_failed":          (_scn_f_flush,        0, 1, (0, 0), True,  True,  2,    "unpublished", [2],     [None]),
    "G_closed":                (_scn_g_close,        1, 0, (1, 1), True,  False, None, None,          [],      []),
}


@pytest.mark.parametrize("name", list(MATRIX))
def test_state_machine_matrix(name, tmp_path, monkeypatch):
    (scenario, segs, tmps, hooks, refused, unpublished, rows, durability,
     immediate_drops, restart_drops) = MATRIX[name]
    events, published, durable = [], [], []
    with monkeypatch.context() as mp:
        w = scenario(tmp_path, mp, events, published, durable)
        assert len(_segs(w, "*.seg")) == segs, ".seg count"
        assert len(_segs(w, "*.seg.tmp")) == tmps, ".tmp count"
        assert (len(published), len(durable)) == hooks, "hooks that ran"
        # State is asserted BEFORE probing with a write: a probe row on a healthy
        # writer is (correctly) pending and would change has_unpublished_rows().
        assert w.has_unpublished_rows() is unpublished
        assert w._storage_failure_rows == (rows or 0)
        assert w._storage_failure_durability == durability
        assert [d["rows_lost"] for d in _of(events, "DATA_DROP")] == immediate_drops
        assert (w._storage_failure is not None) is (durability is not None)
        if refused:
            with pytest.raises(RuntimeError):
                w.write(_row(500))
            assert w.buffer == [], "a refused write is never buffered"
        else:
            w.write(_row(500))                          # E: a healthy writer keeps accepting rows
        _shutdown(w)
    restart = _restart(tmp_path)
    assert [d["rows_lost"] for d in _of(restart, "DATA_DROP")] == restart_drops
