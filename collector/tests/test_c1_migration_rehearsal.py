"""C1 migration rehearsal (collector/scripts/c1_migration_rehearsal.py): a READ-ONLY census of a dataset.

What is proved here:

* the reference numbers come from the segment bytes (an independent brute-force oracle checks them),
  while marker / evidence / index are only measured and compared;
* nothing is mutated: no input file changes (content hash of the whole tree), no marker/evidence/index
  writer is ever called, and the current cross-segment-duplicate startup policy is untouched;
* every problem class the operator asked about is surfaced (duplicates, malformed, markers, evidence,
  old index schema) without the tool raising;
* the report is deterministic and independent of where the copy lives.
"""
from __future__ import annotations

import builtins
import hashlib
import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from collections import namedtuple
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.collector import publication as pub
from collector.collector import segment_dedup as sd
from collector.scripts import c1_migration_rehearsal as c1

STREAM = "c1_trades"
INSTR = "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT"
SPEC = sd.StreamSpec("trades", None, "BINANCE", "linear_perpetual")
HOUR = "2026-01-01-00"


# ----------------------------------------------------------------------------------------- helpers
def stream_dir(base: Path) -> Path:
    return base / "raw" / STREAM


def seg_path(base: Path, seq: int, hour: str = HOUR) -> Path:
    return stream_dir(base) / f"{hour}-{seq:06d}.seg"


def write_marker(path: Path, record_count: int, *, sha: str | None = None, size: int | None = None) -> None:
    real_sha, real_size = pub.sha256_file(path)
    pub.write_marker_atomic(path, record_count=record_count, first_ts=0, last_ts=max(record_count - 1, 0),
                            sha256=sha or real_sha, size_bytes=real_size if size is None else size,
                            confirmed_by=pub.CONFIRMED_BY_WRITER, legacy=False, fsync_dir_after=False)


def write_seg(base: Path, seq: int, ids, *, hour: str = HOUR, marker: str = "valid", column: str = "trade_id") -> Path:
    stream_dir(base).mkdir(parents=True, exist_ok=True)
    path = seg_path(base, seq, hour)
    n = len(ids)
    schema = pa.schema([("timestamp", pa.int64()), ("instrument_key", pa.string()), (column, pa.string())])
    pq.write_table(pa.table({"timestamp": list(range(n)), "instrument_key": [INSTR] * n,
                             column: pa.array(ids, type=pa.string())}, schema=schema), path)
    if marker == "valid":
        write_marker(path, n)
    elif marker == "garbage":
        pub.marker_path(path).write_bytes(b"{not json")
    elif marker == "v1":
        pub.marker_path(path).write_text(json.dumps({"record_count": n, "first_record_ts": 0, "last_record_ts": 0}))
    elif marker != "none":
        raise ValueError(marker)
    return path


def build_index(base: Path, *, keep_open: bool = False):
    (base / "dedup_state").mkdir(parents=True, exist_ok=True)
    index = sd.SegmentDedupIndex(str(base / "dedup_state" / f"{STREAM}.sqlite3"))
    sd.SegmentDedupCoordinator(index, SPEC.row_identity).startup_reconcile(stream_dir(base))
    if keep_open:
        return index
    index.close()
    return None


def evidence_file(base: Path, seq: int) -> Path:
    return base / "dedup_state" / f"{STREAM}.identity_evidence" / f"{STREAM}%2F{HOUR}-{seq:06d}.seg.ids.json"


def rehearse(base: Path, tmp_path: Path, *, trade_id_field: str = "trade_id", with_index: bool = True,
             detail_out: Path | None = None, **kw):
    scratch = tmp_path / "_scratch"
    scratch.mkdir(exist_ok=True)
    result = c1.Rehearsal(
        stream_dir(base), exchange="BINANCE", market_type="linear_perpetual", event_stream="trades",
        trade_id_field=trade_id_field,
        index_path=(base / "dedup_state" / f"{STREAM}.sqlite3") if with_index else None, evidence_dir=None,
        sample_limit=kw.get("sample_limit", 10), top_segments=kw.get("top_segments", 25),
        example_limit=kw.get("example_limit", 5), detail_out=detail_out, scratch_dir=scratch,
        scratch_preflight=kw.get("scratch_preflight", "error"), data_root=kw.get("data_root")).run()
    assert list(scratch.iterdir()) == [], "scratch directory must be removed"
    return result


def tree_state(root: Path) -> dict:
    state = {}
    for p in sorted(root.rglob("*")):
        rel = str(p.relative_to(root))
        state[rel] = hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else "<dir>"
    return state


def ids(prefix: str, n: int):
    return [f"{prefix}-{i}" for i in range(n)]


# ----------------------------------------------------------------------------------------- clean
def test_clean_dataset(tmp_path):
    base = tmp_path / "data"
    for s in range(3):
        write_seg(base, s, ids(f"s{s}", 4))
    out = rehearse(base, tmp_path)
    r = out["report"]
    assert r["segments"]["total_seg_files"] == 3 and r["segments"]["readable"] == 3
    assert r["segments"]["malformed_or_unreadable"]["count"] == 0 and r["segments"]["empty"] == 0
    assert r["rows"] == {"physical_total": 12, "with_identity": 12, "without_identity": 0}
    assert r["identities"]["distinct_total"] == 12
    d = r["duplicates"]
    assert d["intra_segment"]["identities"] == 0 and d["intra_segment"]["segments_affected"] == 0
    assert d["cross_segment"]["identities"] == 0 and d["cross_segment"]["segments_affected"] == 0
    assert r["affected_segments"]["total"] == 0 and r["affected_segments"]["segments"] == []
    assert r["markers"]["valid"] == 3 and r["markers"]["absent"]["count"] == 0
    assert r["dedup_index"]["present"] is False                     # no index yet: reported, not an error
    assert r["identity_evidence"]["per_segment"]["missing"]["count"] == 3
    assert all(v == 0 for k, v in r["fail_closed_conditions"].items() if k != "note")
    assert r["read_only_check"]["unchanged"] is True
    assert out["run"]["elapsed_seconds"] >= 0


def test_clean_dataset_with_real_index_and_evidence_is_fully_consistent(tmp_path):
    """Cross-validates the tool's read-only re-implementation against what the real coordinator wrote."""
    base = tmp_path / "data"
    for s in range(4):
        write_seg(base, s, ids(f"s{s}", 5))
    write_seg(base, 4, [])                                            # an empty segment too
    build_index(base)
    r = rehearse(base, tmp_path)["report"]
    ev = r["identity_evidence"]["per_segment"]
    assert ev["ok"]["count"] == 5 and sum(ev[k]["count"] for k in ("missing", "invalid", "stale", "mismatch")) == 0
    idx = r["dedup_index"]
    assert idx["user_version"] == sd.INDEX_SCHEMA_VERSION and idx["schema_problem"] is None
    assert idx["reconciled_segments"] == 5 and idx["identity_rows"] == 20
    for name, value in idx["comparison"].items():
        assert (value if isinstance(value, int) else value["count"]) == 0, name
    assert r["estimated_work"]["index_rebuild_expected"] is False
    assert r["estimated_work"]["segments_to_index_incrementally"] == 0
    assert r["estimated_work"]["evidence_files_to_write"] == 0


# ----------------------------------------------------------------------------------------- duplicates
def test_one_intra_segment_duplicate(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["a", "b", "a", "c"])
    write_seg(base, 1, ["d", "e"])
    r = rehearse(base, tmp_path)["report"]
    intra = r["duplicates"]["intra_segment"]
    assert (intra["identities"], intra["extra_rows"], intra["segments_affected"]) == (1, 1, 1)
    assert r["duplicates"]["cross_segment"]["identities"] == 0
    assert r["rows"]["physical_total"] == 6 and r["identities"]["distinct_total"] == 5
    assert r["identities"]["sum_of_per_segment_distinct"] == 5
    seg = r["affected_segments"]["segments"][0]
    assert seg["segment"] == f"{STREAM}/{HOUR}-000000.seg"
    assert (seg["rows"], seg["distinct_identities"], seg["intra_duplicate_identities"],
            seg["intra_duplicate_extra_rows"], seg["cross_duplicate_identities"]) == (4, 3, 1, 1, 0)
    sample = intra["samples"][0]
    assert sample["trade_id"] == "a" and sample["occurrences"] == 2
    assert sample["segments"] == [{"segment": f"{STREAM}/{HOUR}-000000.seg", "count": 2}]


def test_intra_segment_semantics_are_unchanged_in_production_code(tmp_path):
    """The census must describe the code as it is: an intra-segment duplicate is collapsed by startup."""
    base = tmp_path / "data"
    write_seg(base, 0, ["a", "b", "a"])
    build_index(base)                                                   # real startup: accepted, 2 identities
    index = sd.SegmentDedupIndex(str(base / "dedup_state" / f"{STREAM}.sqlite3"))
    try:
        assert index.identity_count() == 2
    finally:
        index.close()


def test_one_cross_segment_duplicate_is_reported_and_policy_is_unchanged(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["a", "b"])
    write_seg(base, 1, ["b", "c"])
    before = tree_state(base)
    out = rehearse(base, tmp_path, with_index=False)
    assert tree_state(base) == before
    r = out["report"]
    cross = r["duplicates"]["cross_segment"]
    assert (cross["identities"], cross["rows_total"], cross["extra_rows_beyond_one_per_identity"],
            cross["segments_affected"]) == (1, 2, 1, 2)
    assert cross["segments_per_identity_histogram"] == {"2": 1}
    assert cross["ordinal_distance_histogram"] == {"adjacent": 1}
    assert cross["calendar_span_histogram"] == {"same_hour": 1}
    assert r["identities"]["distinct_total"] == 3 and r["rows"]["physical_total"] == 4
    sample = cross["samples"][0]
    assert sample["trade_id"] == "b" and sample["occurrences"] == 2
    assert sample["segments"] == [{"segment": f"{STREAM}/{HOUR}-000000.seg", "count": 1},
                                  {"segment": f"{STREAM}/{HOUR}-000001.seg", "count": 1}]
    assert "winner" not in json.dumps(r).replace("winner/loser", "").replace("winner loser", "")  # no ranking
    assert r["fail_closed_conditions"]["cross_segment_duplicate_identities"] == 1
    for s in r["affected_segments"]["segments"]:
        assert s["cross_duplicate_identities"] == 1 and s["cross_duplicate_rows"] == 1
    # POLICY UNCHANGED: the real startup still fails closed on exactly this dataset (run on a copy).
    copy = tmp_path / "copy"
    shutil.copytree(base, copy)
    index = sd.SegmentDedupIndex(str(copy / "dedup.sqlite3"))
    try:
        with pytest.raises(sd.DedupStateError, match="CROSS_SEGMENT_DUPLICATE"):
            sd.SegmentDedupCoordinator(index, SPEC.row_identity).startup_reconcile(stream_dir(copy))
    finally:
        index.close()


def test_same_identity_in_many_segments_keeps_full_provenance(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["x", "x", "p"])        # twice inside segment 0 (intra) ...
    write_seg(base, 1, ["x", "q"])
    write_seg(base, 5, ["x", "r"], hour="2026-01-01-03")
    r = rehearse(base, tmp_path, with_index=False)["report"]
    cross, intra = r["duplicates"]["cross_segment"], r["duplicates"]["intra_segment"]
    assert (cross["identities"], cross["rows_total"], cross["extra_rows_beyond_one_per_identity"]) == (1, 4, 3)
    assert cross["segments_per_identity_histogram"] == {"3": 1} and cross["segments_affected"] == 3
    assert cross["calendar_span_histogram"] == {"same_date_different_hour": 1}
    assert cross["ordinal_distance_histogram"] == {"2-10": 1}
    sample = cross["samples"][0]
    assert sample["occurrences"] == 4
    assert [(s["segment"].split("/")[1], s["count"]) for s in sample["segments"]] == [
        (f"{HOUR}-000000.seg", 2), (f"{HOUR}-000001.seg", 1), ("2026-01-01-03-000005.seg", 1)]
    assert (intra["identities"], intra["extra_rows"], intra["segments_affected"]) == (1, 1, 1)
    assert r["duplicates"]["identities_both_intra_and_cross"] == 1
    assert r["identities"]["distinct_total"] == 4 and r["rows"]["physical_total"] == 7


def test_histogram_buckets_wide_spread(tmp_path):
    base = tmp_path / "data"
    for s in range(12):
        write_seg(base, s, ["z", f"u{s}"])
    r = rehearse(base, tmp_path, with_index=False)["report"]["duplicates"]["cross_segment"]
    assert r["segments_per_identity_histogram"] == {"11+": 1} and r["segments_affected"] == 12
    assert r["ordinal_distance_histogram"] == {"11-100": 1}


def test_null_trade_id_rows_have_no_identity(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["a", None, None, "b"])
    r = rehearse(base, tmp_path, with_index=False)["report"]
    assert r["rows"] == {"physical_total": 4, "with_identity": 2, "without_identity": 2}
    assert r["identities"]["distinct_total"] == 2 and r["duplicates"]["intra_segment"]["identities"] == 0


# ----------------------------------------------------------------------------------------- problems
def test_malformed_segment_is_reported_not_raised(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["a", "b"])
    bad = seg_path(base, 1)
    bad.write_bytes(b"PAR1 this is not parquet")
    write_marker(bad, 2)                                                # a marker over garbage bytes
    trunc = write_seg(base, 2, ids("t", 50))
    trunc.write_bytes(trunc.read_bytes()[:-40])                         # truncated footer
    write_marker(trunc, 50)
    write_seg(base, 3, ["c"])
    r = rehearse(base, tmp_path)["report"]
    seg = r["segments"]
    assert seg["total_seg_files"] == 4 and seg["readable"] == 2 and seg["malformed_or_unreadable"]["count"] == 2
    assert all(f"{STREAM}/" in e for e in seg["malformed_or_unreadable"]["examples"])
    assert r["rows"]["physical_total"] == 3 and r["identities"]["distinct_total"] == 3
    assert r["fail_closed_conditions"]["unreadable_parquet"] == 2
    assert r["identity_evidence"]["per_segment"]["not_evaluated"]["count"] == 2


def test_missing_and_v1_markers(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["a"], marker="none")
    write_seg(base, 1, ["b"], marker="v1")
    write_seg(base, 2, ["c"])
    r = rehearse(base, tmp_path, with_index=False)["report"]
    m = r["markers"]
    assert (m["valid"], m["absent"]["count"], m["absent_with_v1_hint"]["count"], m["invalid"]["count"]) == (1, 1, 1, 0)
    assert r["identities"]["distinct_total"] == 3                      # bytes are still the reference
    assert r["estimated_work"]["markers_to_write_by_startup_refsync"] == 2


def test_invalid_marker_is_reported_and_left_in_place(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["a"], marker="garbage")
    write_seg(base, 1, ["b"])
    before = sorted(p.name for p in stream_dir(base).iterdir())
    r = rehearse(base, tmp_path, with_index=False)["report"]
    assert r["markers"]["invalid"]["count"] == 1 and "not valid JSON" in r["markers"]["invalid"]["examples"][0]
    assert sorted(p.name for p in stream_dir(base).iterdir()) == before          # NOT renamed to .invalid.N


@pytest.mark.parametrize("mutation,field", [("sha", "sha256_mismatch"), ("size", "size_mismatch"),
                                            ("records", "record_count_mismatch")])
def test_valid_marker_that_disagrees_with_the_bytes(tmp_path, mutation, field):
    base = tmp_path / "data"
    path = write_seg(base, 0, ["a", "b", "c"], marker="none")
    real_sha, real_size = pub.sha256_file(path)
    if mutation == "sha":
        write_marker(path, 3, sha="0" * 64)
    elif mutation == "size":
        write_marker(path, 3, size=real_size + 1)
    else:
        write_marker(path, 7)
    m = rehearse(base, tmp_path, with_index=False)["report"]["markers"]
    assert m[field]["count"] == 1 and m["valid"] == 1
    assert sum(m[k]["count"] for k in ("sha256_mismatch", "size_mismatch", "record_count_mismatch")) == 1


def test_dangling_marker_and_stray_files_are_counted_and_untouched(tmp_path):
    base = tmp_path / "data"
    path = write_seg(base, 0, ["a"])
    shutil.copy(pub.marker_path(path), stream_dir(base) / f"{HOUR}-000099.seg.meta.json")
    (stream_dir(base) / f"{HOUR}-000098.seg.tmp").write_bytes(b"x")
    (stream_dir(base) / "2026-01-01-05.parquet").write_bytes(b"x")
    before = tree_state(base)
    other = rehearse(base, tmp_path, with_index=False)["report"]["segments"]["other_files"]
    assert other == {"legacy_hourly_parquet_files_not_audited": 1, "dangling_markers": 1,
                     "preserved_invalid_or_orphan_markers": 0, "tmp_files": 1}
    assert tree_state(base) == before


def test_identity_derivation_failure_is_reported(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["a"])
    p = seg_path(base, 1)
    pq.write_table(pa.table({"timestamp": [1], "instrument_key": [INSTR], "trade_id": [12345]}), p)   # int id
    write_marker(p, 1)
    r = rehearse(base, tmp_path, with_index=False)["report"]
    assert r["segments"]["identity_derivation_failed"]["count"] == 1
    assert r["identities"]["distinct_total"] == 1 and r["rows"]["physical_total"] == 2


def test_wrong_trade_id_field_is_loud(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["a", "b"], column="native_trade_id")
    wrong = rehearse(base, tmp_path, with_index=False)["report"]       # default field "trade_id" is absent
    assert wrong["segments"]["missing_trade_id_column"]["count"] == 1 and wrong["identities"]["distinct_total"] == 0
    assert any("--trade-id-field" in w for w in wrong["warnings"])
    right = rehearse(base, tmp_path, trade_id_field="native_trade_id", with_index=False)["report"]
    assert right["identities"]["distinct_total"] == 2 and right["warnings"] == []


def test_empty_segment(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, [])
    write_seg(base, 1, ["a"])
    r = rehearse(base, tmp_path, with_index=False)["report"]
    assert r["segments"]["empty"] == 1 and r["segments"]["readable"] == 2
    assert r["rows"]["physical_total"] == 1 and r["identities"]["distinct_total"] == 1


# ----------------------------------------------------------------------------------------- evidence
def test_missing_evidence_file(tmp_path):
    base = tmp_path / "data"
    for s in range(3):
        write_seg(base, s, ids(f"s{s}", 3))
    build_index(base)
    evidence_file(base, 1).unlink()
    r = rehearse(base, tmp_path)["report"]
    ev = r["identity_evidence"]["per_segment"]
    assert (ev["ok"]["count"], ev["missing"]["count"]) == (2, 1)
    assert ev["missing"]["examples"] == [f"{STREAM}/{HOUR}-000001.seg"]
    assert r["estimated_work"]["evidence_files_to_write"] == 1


def test_forged_and_stale_evidence_and_the_index_is_not_the_reference(tmp_path):
    base = tmp_path / "data"
    for s in range(6):
        write_seg(base, s, ids(f"s{s}", 3))
    build_index(base)

    def edit(seq, **changes):
        p = evidence_file(base, seq)
        obj = json.loads(p.read_text())
        obj.update(changes)
        p.write_text(json.dumps(obj))

    edit(1, identity_digest="0" * 64)                       # forged: right bytes binding, wrong identity set
    edit(2, segment_sha256="f" * 64)                        # stale: bound to other bytes
    evidence_file(base, 3).write_text("garbage")            # invalid
    edit(4, identity_encoding="v0:old-encoding")            # stale: older identity-key encoding
    reused = write_seg(base, 5, ["new-1", "new-2", "new-3"])   # sequence/name reuse: new bytes + new marker
    assert reused.exists()
    r = rehearse(base, tmp_path)["report"]
    ev = r["identity_evidence"]["per_segment"]
    assert (ev["ok"]["count"], ev["mismatch"]["count"], ev["stale"]["count"], ev["invalid"]["count"],
            ev["missing"]["count"]) == (1, 1, 3, 1, 0)
    # reference numbers are the bytes' (new ids), and the index is compared against them, not trusted:
    assert r["identities"]["distinct_total"] == 18
    cmp_ = r["dedup_index"]["comparison"]
    assert cmp_["indexed_evidence_differs_from_bytes"]["count"] == 1
    assert cmp_["indexed_identity_set_differs_from_bytes"]["count"] == 1
    assert cmp_["indexed_membership_differs_from_bytes"]["count"] == 1
    assert r["estimated_work"]["index_rebuild_expected"] is True
    assert r["estimated_work"]["evidence_files_to_write"] == 5


def test_evidence_dir_absent_is_flagged_as_wholesale(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["a"])
    build_index(base)
    shutil.rmtree(base / "dedup_state" / f"{STREAM}.identity_evidence")
    r = rehearse(base, tmp_path)["report"]
    assert r["identity_evidence"]["directory_present"] is False
    assert r["identity_evidence"]["per_segment"]["missing"]["count"] == 1
    assert any("evidence directory not found" in w for w in r["warnings"])


def test_orphan_evidence_files_are_counted(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["a"])
    build_index(base)
    shutil.copy(evidence_file(base, 0), evidence_file(base, 9))
    assert rehearse(base, tmp_path)["report"]["identity_evidence"]["orphan_evidence_files"]["count"] == 1


# ----------------------------------------------------------------------------------------- index
def test_old_index_schema_versions_are_reported(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["a", "b"])
    (base / "dedup_state").mkdir(parents=True)
    path = base / "dedup_state" / f"{STREAM}.sqlite3"
    con = sqlite3.connect(path)
    con.executescript("CREATE TABLE seen(identity_key TEXT PRIMARY KEY);"
                      "CREATE TABLE reconciled_segments(segment_key TEXT PRIMARY KEY, evidence_sha256 TEXT, "
                      "evidence_size INTEGER, confirmed_by TEXT);"
                      "INSERT INTO seen VALUES('x'),('y'),('z'); PRAGMA user_version=2;")
    con.commit()
    con.close()
    before = tree_state(base)
    r = rehearse(base, tmp_path)["report"]
    idx = r["dedup_index"]
    assert idx["user_version"] == 2 and idx["current_schema_version"] == sd.INDEX_SCHEMA_VERSION
    assert idx["needs_rebuild_for_schema"] is True and "user_version=2" in idx["schema_problem"]
    assert idx["identity_rows"] == 3 and idx["reconciled_segments"] is None
    assert r["estimated_work"]["index_rebuild_expected"] is True
    assert tree_state(base) == before


def test_missing_and_corrupt_index(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["a"])
    missing = rehearse(base, tmp_path)["report"]
    assert missing["dedup_index"]["present"] is False
    assert missing["estimated_work"]["index_rebuild_expected"] is True
    (base / "dedup_state").mkdir(parents=True)
    (base / "dedup_state" / f"{STREAM}.sqlite3").write_bytes(b"this is not a sqlite database" * 20)
    corrupt = rehearse(base, tmp_path)["report"]
    assert corrupt["dedup_index"]["present"] is True and corrupt["dedup_index"]["readable"] is False
    assert corrupt["identities"]["distinct_total"] == 1


def test_index_with_uncheckpointed_wal_is_read_from_a_copy_and_source_is_untouched(tmp_path):
    base = tmp_path / "data"
    for s in range(3):
        write_seg(base, s, ids(f"s{s}", 4))
    live = build_index(base, keep_open=True)                           # connection open => -wal not checkpointed
    try:
        wal = base / "dedup_state" / f"{STREAM}.sqlite3-wal"
        assert wal.exists() and wal.stat().st_size > 0
        before = tree_state(base)
        r = rehearse(base, tmp_path)["report"]
        assert r["dedup_index"]["reconciled_segments"] == 3 and r["dedup_index"]["identity_rows"] == 12
        assert r["read_only_check"]["unchanged"] is True
        assert tree_state(base) == before
    finally:
        live.close()


def test_tampered_index_membership_is_detected_without_changing_the_reference(tmp_path):
    base = tmp_path / "data"
    for s in range(2):
        write_seg(base, s, ids(f"s{s}", 4))
    build_index(base)
    clean = rehearse(base, tmp_path)["report"]
    con = sqlite3.connect(base / "dedup_state" / f"{STREAM}.sqlite3")
    con.execute("DELETE FROM seen WHERE identity_key = (SELECT identity_key FROM seen LIMIT 1)")
    con.commit()
    con.close()
    tampered = rehearse(base, tmp_path)["report"]
    assert tampered["dedup_index"]["comparison"]["indexed_membership_differs_from_bytes"]["count"] == 1
    assert tampered["dedup_index"]["identity_rows"] == 7
    for key in ("identities", "rows", "duplicates", "segments"):
        assert tampered[key] == clean[key]


# ----------------------------------------------------------------------------------------- read-only proof
def test_nothing_is_mutated_and_no_mutating_primitive_is_called(tmp_path, monkeypatch):
    base = tmp_path / "data"
    write_seg(base, 0, ["a", "b", "a"])
    write_seg(base, 1, ["b", "c"])                                      # cross-segment duplicate
    write_seg(base, 2, ["d"], marker="none")                            # unmarked  (startup would write a marker)
    write_seg(base, 3, ["e"], marker="garbage")                         # invalid   (startup would rename it)
    shutil.copy(pub.marker_path(seg_path(base, 0)), stream_dir(base) / f"{HOUR}-000099.seg.meta.json")   # dangling
    bad = seg_path(base, 4)
    bad.write_bytes(b"junk")
    build_index_base = tmp_path / "idx_src"
    write_seg(build_index_base, 0, ["a", "b"])                          # an index/evidence set to compare against
    build_index(build_index_base)
    shutil.copytree(build_index_base / "dedup_state", base / "dedup_state")
    before = tree_state(base)

    def forbidden(name):
        def _raise(*a, **k):
            raise AssertionError(f"mutating primitive called: {name}")
        return _raise

    for fn in ("write_marker_atomic", "confirm_unmarked", "preserve_invalid", "orphan_marker", "quarantine_segment",
               "fsync_file", "fsync_dir"):
        monkeypatch.setattr(pub, fn, forbidden(fn))
    for fn in ("startup_reconcile", "_ensure_evidence", "_index_segment"):
        monkeypatch.setattr(sd.SegmentDedupCoordinator, fn, forbidden(fn))
    for fn in ("commit_segment", "rebuild_reset"):
        monkeypatch.setattr(sd.SegmentDedupIndex, fn, forbidden(fn))
    real_open, real_replace, real_rename = builtins.open, os.replace, os.rename

    def under_base(value) -> bool:
        return isinstance(value, (str, os.PathLike)) and str(value).startswith(str(base))

    def guarded_open(file, mode="r", *a, **k):
        if any(c in mode for c in "wax+") and under_base(file):
            raise AssertionError(f"write-mode open of an input file: {file}")
        return real_open(file, mode, *a, **k)

    def guarded_move(real, name):
        def _move(src, dst, *a, **k):
            if under_base(src) or under_base(dst):
                raise AssertionError(f"{name} touched the dataset: {src} -> {dst}")
            return real(src, dst, *a, **k)
        return _move

    monkeypatch.setattr(builtins, "open", guarded_open)
    monkeypatch.setattr(io, "open", guarded_open)
    monkeypatch.setattr(os, "rename", guarded_move(real_rename, "os.rename"))
    monkeypatch.setattr(os, "replace", guarded_move(real_replace, "os.replace"))

    out = rehearse(base, tmp_path)
    monkeypatch.undo()
    assert tree_state(base) == before
    assert out["report"]["read_only_check"]["unchanged"] is True
    assert out["report"]["read_only_check"]["changed_examples"] == []
    assert out["report"]["duplicates"]["cross_segment"]["identities"] == 1


def test_outputs_inside_the_input_tree_are_refused(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["a"])
    with pytest.raises(SystemExit, match="inside an input directory"):
        rehearse(base, tmp_path, detail_out=stream_dir(base) / "dups.jsonl")
    with pytest.raises(SystemExit, match="inside an input directory"):
        c1.main(["--stream-dir", str(stream_dir(base)), "--exchange", "BINANCE", "--market-type", "linear_perpetual",
                 "--json-out", str(stream_dir(base) / "report.json")])
    assert not (stream_dir(base) / "dups.jsonl").exists() and not (stream_dir(base) / "report.json").exists()


# ----------------------------------------------------------------------------------------- large + deterministic
def _large_dataset(base: Path, segments: int = 120, rows: int = 2000):
    data = {s: ids(f"s{s}", rows) for s in range(segments)}
    for s in range(0, 100, 2):                       # 50 cross-segment duplicates, adjacent segments
        data[s + 1][0] = data[s][-1]
    for s in range(100, 115):                        # 15 intra duplicates with n=2
        data[s][1] = data[s][0]
    for s in range(115, 120):                        # 5 intra duplicates with n=3
        data[s][2] = data[s][3] = data[s][0]
    for s, lst in data.items():
        write_seg(base, s, lst)
    return data


def _oracle(data):
    where, per_seg = defaultdict(set), {}
    for s, lst in data.items():
        per_seg[s] = {k: lst.count(k) for k in set(lst)} if len(lst) < 50 else None
        for k in set(lst):
            where[k].add(s)
    cross = [k for k, ss in where.items() if len(ss) > 1]
    intra_ids, intra_extra, intra_segs = set(), 0, set()
    for s, lst in data.items():
        seen = defaultdict(int)
        for k in lst:
            seen[k] += 1
        for k, n in seen.items():
            if n > 1:
                intra_ids.add(k)
                intra_extra += n - 1
                intra_segs.add(s)
    return len(where), len(cross), len(intra_ids), intra_extra, len(intra_segs)


def test_large_synthetic_dataset_matches_an_independent_oracle(tmp_path):
    base = tmp_path / "data"
    data = _large_dataset(base)
    distinct, cross, intra, intra_extra, intra_segs = _oracle(data)
    assert (cross, intra, intra_extra, intra_segs) == (50, 20, 25, 20)              # planted, oracle sanity
    detail = tmp_path / "out" / "dups.jsonl"
    out = rehearse(base, tmp_path, with_index=False, detail_out=detail)
    r = out["report"]
    assert r["segments"]["total_seg_files"] == 120 and r["rows"]["physical_total"] == 240_000
    assert r["identities"]["distinct_total"] == distinct
    d = r["duplicates"]
    assert d["cross_segment"]["identities"] == cross and d["cross_segment"]["segments_affected"] == 100
    assert d["cross_segment"]["extra_rows_beyond_one_per_identity"] == 50
    assert d["cross_segment"]["ordinal_distance_histogram"] == {"adjacent": 50}
    assert (d["intra_segment"]["identities"], d["intra_segment"]["extra_rows"],
            d["intra_segment"]["segments_affected"]) == (intra, intra_extra, intra_segs)
    assert r["affected_segments"]["total"] == 120 and r["affected_segments"]["shown"] == 25
    # bounded default output: counters + a few samples, never the full identity list
    assert len(d["cross_segment"]["samples"]) == 10 and len(d["intra_segment"]["samples"]) == 10
    assert len(json.dumps(out)) < 80_000
    # the detail report carries every duplicate with provenance
    lines = [json.loads(line) for line in detail.read_text().splitlines()]
    classes = defaultdict(list)
    for rec in lines:
        classes[rec["class"]].append(rec)
    assert (len(classes["cross_segment"]), len(classes["intra_segment"]), len(classes["segment"])) == (50, 20, 120)
    assert all(len(rec["segments"]) == 2 and rec["occurrences"] == 2 for rec in classes["cross_segment"])
    assert out["run"]["detail_out"]["lines"] == 190
    assert out["run"]["peak_rss_bytes"] is None or out["run"]["peak_rss_bytes"] > 0
    assert out["run"]["read_throughput"]["rows_per_second"] > 0


def test_report_is_deterministic_and_independent_of_location(tmp_path):
    base = tmp_path / "data"
    for s in range(6):
        write_seg(base, s, ids(f"s{s}", 6))
    build_index(base)                                                   # real index/evidence over the clean part
    write_seg(base, 6, ["x", "x"], marker="garbage")                    # then: invalid marker + intra duplicate
    write_seg(base, 7, ["q", "s0-0"], marker="none")                    # unmarked + a cross-segment duplicate
    d1, d2, d3 = tmp_path / "d1.jsonl", tmp_path / "d2.jsonl", tmp_path / "d3.jsonl"
    a = rehearse(base, tmp_path, detail_out=d1)
    b = rehearse(base, tmp_path, detail_out=d2)
    moved = tmp_path / "elsewhere" / "copy"
    shutil.copytree(base, moved)
    c = rehearse(moved, tmp_path, detail_out=d3)
    assert a["report"] == b["report"] == c["report"]
    assert a["report_digest"] == b["report_digest"] == c["report_digest"]
    assert d1.read_bytes() == d2.read_bytes() == d3.read_bytes()
    assert json.dumps(a["report"], sort_keys=True) == json.dumps(b["report"], sort_keys=True)
    assert hashlib.sha256(c1._canonical(a["report"]).encode()).hexdigest() == a["report_digest"]
    assert a["report"]["duplicates"]["cross_segment"]["identities"] == 1       # sanity: the report is not trivially empty


# ----------------------------------------------------------------------------------------- CLI
def test_decode_identity_key_round_trips():
    for tid in ("123", "1:2", ":", "é-ü", "", "a" * 40):
        key = sd.dedup_identity_key("BINANCE", "linear_perpetual", INSTR, "trades", tid)
        assert c1.decode_identity_key(key) == ["BINANCE", "linear_perpetual", INSTR, "trades", tid]
    assert c1.decode_identity_key("99:short") is None


def test_cli_preset_json_out_and_detail_out(tmp_path, capsys):
    base = tmp_path / "data"
    sdir = base / "raw" / "binance_trades_raw"
    sdir.mkdir(parents=True)
    for s, lst in enumerate((["1", "2"], ["2", "3"])):
        p = sdir / f"{HOUR}-{s:06d}.seg"
        pq.write_table(pa.table({"timestamp": [1, 2], "instrument_key": [INSTR] * 2, "native_trade_id": lst}), p)
        write_marker(p, 2)
    rc = c1.main(["--preset", "binance-usdm", "--data-root", str(base), "--json-out", str(tmp_path / "o" / "r.json"),
                  "--detail-out", str(tmp_path / "o" / "d.jsonl")])
    assert rc == 0
    text = capsys.readouterr().out
    assert "CROSS-segment duplicates: 1 identities" in text and "source files unchanged=True" in text
    saved = json.loads((tmp_path / "o" / "r.json").read_text())
    assert saved["schema"] == c1.REPORT_SCHEMA and saved["report"]["identity_spec"]["trade_id_field"] == "native_trade_id"
    assert saved["report"]["dedup_index"]["present"] is False
    assert [json.loads(x)["class"] for x in (tmp_path / "o" / "d.jsonl").read_text().splitlines()].count("cross_segment") == 1


def test_cli_requires_identity_wiring(tmp_path):
    with pytest.raises(SystemExit):
        c1.main(["--stream-dir", str(tmp_path)])
    with pytest.raises(SystemExit):
        c1.main([])


def test_script_runs_from_a_clean_environment_without_pythonpath(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ["a", "b"])
    write_seg(base, 1, ["b", "c"])
    script = Path(c1.__file__)
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    proc = subprocess.run([sys.executable, str(script), "--stream-dir", str(stream_dir(base)), "--exchange", "BINANCE",
                           "--market-type", "linear_perpetual", "--no-index", "--json"],
                          capture_output=True, text=True, cwd=str(tmp_path), env=env, timeout=120)
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)
    assert out["report"]["duplicates"]["cross_segment"]["identities"] == 1
    assert out["report"]["read_only_check"]["unchanged"] is True


# ====================================================================================== final hardening pass
# ---------------------------------------------------------------------------- presets == production wiring
_RUNNERS = {   # preset -> (runner module, adapter event stream that runner wires)
    "binance-usdm": ("collector.run_collector", "trades"),
    "bybit": ("collector.run_bybit_collector", "trades"),
    "okx-trades": ("collector.run_okx_collector", "trades"),
    "okx-trades-all": ("collector.run_okx_collector", "trades-all"),
    "binance-spot": ("collector.run_binance_spot_collector", "spot_trades"),
}


@pytest.fixture(scope="module")
def production_specs(tmp_path_factory):
    """The StreamSpecs the REAL runners hand to ``attach_segment_dedup`` (constructed for real, wrapped to record)."""
    import importlib
    mp = pytest.MonkeyPatch()
    captured, apps = {}, []
    base = tmp_path_factory.mktemp("prod_wiring")
    try:
        for modname in sorted({m for m, _ in _RUNNERS.values()}):
            mod = importlib.import_module(modname)
            real = mod.attach_segment_dedup

            def wrapper(adapter, specs, _real=real, _mod=modname):
                specs = list(specs)
                for s in specs:
                    captured[(_mod, s.event_stream)] = s
                return _real(adapter, specs)

            mp.setattr(mod, "attach_segment_dedup", wrapper)
        mp.chdir(base)
        (base / "data").mkdir()
        for modname, cls, kwargs in (("collector.run_collector", "CollectorApp", {}),
                                     ("collector.run_bybit_collector", "BybitCollectorApp", {"data_dir": str(base / "b")}),
                                     ("collector.run_okx_collector", "OKXCollectorApp", {"data_dir": str(base / "o")}),
                                     ("collector.run_binance_spot_collector", "BinanceSpotCollectorApp", {"data_dir": str(base / "s")})):
            apps.append(getattr(importlib.import_module(modname), cls)(**kwargs))
        yield captured
    finally:
        for app in apps:
            try:
                app.segment_dedup.close()
            except Exception:  # noqa: BLE001
                pass
        for s in captured.values():
            try:
                s.writer._release_lock()
            except Exception:  # noqa: BLE001
                pass
        mp.undo()


def test_every_production_trade_stream_has_exactly_one_preset(production_specs):
    assert set(production_specs) == set(_RUNNERS.values()), "a production trade stream without a preset (or vice versa)"
    assert set(c1.PRESETS) == set(_RUNNERS)


@pytest.mark.parametrize("preset", sorted(_RUNNERS))
def test_preset_matches_the_production_runner_wiring(production_specs, preset, tmp_path, capsys):
    spec = production_specs[_RUNNERS[preset]]
    wiring = c1.PRESETS[preset]
    assert wiring == dict(stream=spec.writer.stream_name, exchange=spec.exchange, market_type=spec.market_type,
                          event_stream=spec.event_stream, trade_id_field=spec.trade_id_field)
    names = spec.writer.schema.names
    assert spec.trade_id_field in names and "instrument_key" in names, "identity columns exist in the real schema"

    # end to end through the CLI: the preset must reproduce the PRODUCTION identity (evidence written by the real
    # coordinator with the production spec must verify), under the production stream directory name.
    base, sdir = tmp_path / "data", tmp_path / "data" / "raw" / spec.writer.stream_name
    sdir.mkdir(parents=True)

    def seg(seq, ids):
        path = sdir / f"{HOUR}-{seq:06d}.seg"
        pq.write_table(pa.table({"timestamp": list(range(len(ids))), "instrument_key": [INSTR] * len(ids),
                                 spec.trade_id_field: pa.array(ids, type=pa.string())}), path)
        write_marker(path, len(ids))

    seg(0, ["1", "2"])
    seg(1, ["3", "4"])
    (base / "dedup_state").mkdir()
    index = sd.SegmentDedupIndex(str(base / "dedup_state" / f"{spec.writer.stream_name}.sqlite3"))
    sd.SegmentDedupCoordinator(index, spec.row_identity).startup_reconcile(sdir)
    index.close()
    seg(2, ["4", "5"])                                                   # a later, not-yet-indexed cross duplicate
    assert c1.main(["--preset", preset, "--data-root", str(base), "--json"]) == 0
    out = json.loads(capsys.readouterr().out)["report"]
    assert out["identity_spec"] == {k: wiring[k] for k in ("exchange", "market_type", "event_stream", "trade_id_field")}
    ev = out["identity_evidence"]["per_segment"]
    assert (ev["ok"]["count"], ev["missing"]["count"], ev["mismatch"]["count"], ev["stale"]["count"]) == (2, 1, 0, 0)
    cmp_ = out["dedup_index"]["comparison"]
    assert cmp_["segments_on_disk_not_indexed"]["count"] == 1
    assert all((v if isinstance(v, int) else v["count"]) == 0 for k, v in cmp_.items() if k != "segments_on_disk_not_indexed")
    assert out["duplicates"]["cross_segment"]["identities"] == 1
    sample = out["duplicates"]["cross_segment"]["samples"][0]
    assert c1.decode_identity_key(sample["identity"]) == [spec.exchange, spec.market_type, INSTR, spec.event_stream, "4"]
    assert sample["identity"] == spec.row_identity({"instrument_key": INSTR, spec.trade_id_field: "4"})


# ---------------------------------------------------------------------------- determinism across filesystem paths
def _damaged(base: Path, index_kind: str) -> None:
    """One dataset exercising EVERY exception-reporting path of the report."""
    write_seg(base, 0, ids("a", 3))
    write_seg(base, 1, ids("b", 3))
    write_seg(base, 2, ids("c", 3), marker="valid")
    build_index(base)
    garbage = write_seg(base, 3, ids("d", 2))                      # bytes that are not parquet (marker is valid JSON)
    garbage.write_bytes(b"this is not parquet")
    trunc = write_seg(base, 4, ids("e", 2))
    trunc.write_bytes(trunc.read_bytes()[:30])
    bad_type = write_seg(base, 5, ids("f", 2))                     # integer trade ids: identity derivation fails
    pq.write_table(pa.table({"instrument_key": [INSTR] * 2, "trade_id": pa.array([1, 2], type=pa.int64())}), bad_type)
    write_marker(bad_type, 2)
    mk = pub.marker_path(write_seg(base, 6, ids("g", 2)))          # an unreadable marker: OSError text embeds the path
    mk.unlink()
    mk.mkdir()
    pub.marker_path(write_seg(base, 7, ids("h", 2))).write_bytes(b"{not json")
    pq.write_table(pa.table({"x": [1]}), stream_dir(base) / "2026-01-01-00.parquet")        # legacy overlap
    state = base / "dedup_state"
    if index_kind == "corrupt":
        (state / f"{STREAM}.sqlite3").write_bytes(b"\x00garbage" * 64)
    elif index_kind == "future":
        con = sqlite3.connect(str(state / f"{STREAM}.sqlite3"))
        con.execute(f"PRAGMA user_version={sd.INDEX_SCHEMA_VERSION + 3}")
        con.commit()
        con.close()
    elif index_kind == "old":
        con = sqlite3.connect(str(state / f"{STREAM}.sqlite3"))
        con.execute("PRAGMA user_version=1")
        con.commit()
        con.close()


@pytest.mark.parametrize("index_kind", ["ok", "corrupt", "future", "old"])
def test_identical_damaged_datasets_at_different_paths_have_the_same_report_digest(tmp_path, index_kind):
    one, two = tmp_path / "a" / "data", tmp_path / "bbbb" / "deeper" / "elsewhere" / "data"
    for base in (one, two):
        base.mkdir(parents=True)
        _damaged(base, index_kind)
    (tmp_path / "s1").mkdir()
    (tmp_path / "s2").mkdir()
    r1 = rehearse(one, tmp_path / "s1")
    r2 = rehearse(two, tmp_path / "s2")
    assert r1["report"]["segments"]["malformed_or_unreadable"]["count"] >= 2, "the damage is really exercised"
    assert r1["report"]["segments"]["identity_derivation_failed"]["count"] == 1 if "identity_derivation_failed" in r1["report"]["segments"] else True
    assert r1["report_digest"] == r2["report_digest"]
    text = json.dumps(r1["report"]) + json.dumps(r2["report"])
    for leak in (str(tmp_path), "/tmp", "c1_rehearsal_", "_scratch", "Errno"):
        assert leak not in text, f"run-specific text {leak!r} leaked into the deterministic report"


def test_report_does_not_depend_on_directory_listing_order(tmp_path, monkeypatch):
    base = tmp_path / "data"
    for i in range(6):
        write_seg(base, i, ids(f"y{i}", 2))
    build_index(base)
    for i in range(6, 12):
        write_seg(base, i, ids(f"z{i}", 2), marker="none")
    for i in range(4):
        shutil.copy(evidence_file(base, 0), evidence_file(base, 0).with_name(f"orphan-{i}{sd.EVIDENCE_SUFFIX}"))
    (tmp_path / "s").mkdir()
    normal = rehearse(base, tmp_path / "s", example_limit=2)
    orphans = normal["report"]["identity_evidence"]["orphan_evidence_files"]
    assert orphans["count"] == 4 and orphans["examples"] == [f"orphan-{i}{sd.EVIDENCE_SUFFIX}" for i in range(2)]
    real_glob = Path.glob
    monkeypatch.setattr(Path, "glob", lambda self, pat: iter(list(reversed(sorted(real_glob(self, pat))))))
    flipped = rehearse(base, tmp_path / "s", example_limit=2)
    assert normal["report_digest"] == flipped["report_digest"]


# ---------------------------------------------------------------------------- scratch / output containment
def _two_seg(tmp_path):
    base = tmp_path / "data"
    write_seg(base, 0, ids("a", 3))
    write_seg(base, 1, ids("b", 3))
    build_index(base)
    return base


@pytest.mark.parametrize("where,cached", [("stream", False), ("state", False), ("root", False), ("stream", True)])
def test_tmpdir_inside_the_input_tree_is_refused(tmp_path, monkeypatch, where, cached):
    base = _two_seg(tmp_path)
    inside = {"stream": stream_dir(base) / "tmpdir", "state": base / "dedup_state" / "tmpdir", "root": base / "tmpdir"}[where]
    inside.mkdir()
    monkeypatch.setenv("TMPDIR", str(inside))
    monkeypatch.setattr(tempfile, "tempdir", str(inside) if cached else None)
    before, mtime = tree_state(base), os.stat(inside).st_mtime_ns
    with pytest.raises(SystemExit, match="inside an input directory"):
        c1.main(["--stream-dir", str(stream_dir(base)), "--data-root", str(base), "--exchange", "BINANCE",
                 "--market-type", "linear_perpetual", "--event-stream", "trades", "--trade-id-field", "trade_id"])
    assert tree_state(base) == before and list(inside.iterdir()) == [], "the tool must not contaminate its own input"
    assert os.stat(inside).st_mtime_ns == mtime, "not even probed"


def test_symlinks_into_the_input_tree_cannot_hide_scratch_or_outputs(tmp_path):
    base = _two_seg(tmp_path)
    link = tmp_path / "innocent_link"
    link.symlink_to(stream_dir(base), target_is_directory=True)
    before, stream_mtime = tree_state(base), os.stat(stream_dir(base)).st_mtime_ns
    common = ["--stream-dir", str(stream_dir(base)), "--data-root", str(base), "--exchange", "BINANCE",
              "--market-type", "linear_perpetual", "--event-stream", "trades", "--trade-id-field", "trade_id"]
    for extra in (["--scratch-dir", str(link)], ["--json-out", str(link / "r.json")], ["--detail-out", str(link / "d.jsonl")]):
        with pytest.raises(SystemExit, match="inside an input directory"):
            c1.main(common + extra)
    assert tree_state(base) == before
    assert os.stat(stream_dir(base)).st_mtime_ns == stream_mtime, "not even a transient scratch dir may appear in the input"


def test_input_reached_through_a_symlink_is_still_protected_and_watched(tmp_path, monkeypatch):
    base = _two_seg(tmp_path)
    link = tmp_path / "linked_stream"
    link.symlink_to(stream_dir(base), target_is_directory=True)
    with pytest.raises(SystemExit, match="inside an input directory"):
        c1.Rehearsal(link, exchange="BINANCE", market_type="linear_perpetual", event_stream="trades",
                     trade_id_field="trade_id", index_path=None, evidence_dir=None, sample_limit=1, top_segments=1,
                     example_limit=1, detail_out=stream_dir(base) / "d.jsonl", scratch_dir=tmp_path / "s").run()
    orig = c1.Rehearsal._aggregate

    def hooked(self, conn):
        with open(seg_path(base, 0), "ab") as fh:
            fh.write(b"x")
        return orig(self, conn)

    monkeypatch.setattr(c1.Rehearsal, "_aggregate", hooked)
    (tmp_path / "s").mkdir()
    out = c1.Rehearsal(link, exchange="BINANCE", market_type="linear_perpetual", event_stream="trades",
                       trade_id_field="trade_id", index_path=None, evidence_dir=None, sample_limit=1, top_segments=1,
                       example_limit=1, detail_out=None, scratch_dir=tmp_path / "s").run()
    assert out["report"]["read_only_check"]["unchanged"] is False


# ---------------------------------------------------------------------------- input snapshot
def _mut_append(base):
    with open(seg_path(base, 0), "ab") as fh:
        fh.write(b"x")


def _mut_add_file(base):
    (stream_dir(base) / "new.txt").write_bytes(b"n")


def _mut_remove_file(base):
    pub.marker_path(seg_path(base, 1)).unlink()


def _mut_new_empty_dir(base):
    (stream_dir(base) / "emptydir").mkdir()


def _mut_inplace_same_size_mtime_restored(base):
    path = seg_path(base, 0)
    st = os.stat(path)
    data = bytearray(path.read_bytes())
    data[len(data) // 2] ^= 0xFF
    with open(path, "r+b") as fh:
        fh.write(bytes(data))
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns))


def _mut_replace_new_inode_same_size_mtime_restored(base):
    path = seg_path(base, 0)
    st = os.stat(path)
    tmp = path.with_name("swap.tmp")
    tmp.write_bytes(path.read_bytes())
    os.replace(tmp, path)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns))


def _mut_index_sidecar(base):
    (base / "dedup_state" / f"{STREAM}.sqlite3-journal").write_bytes(b"j")


def _mut_evidence_edit(base):
    f = evidence_file(base, 0)
    st = os.stat(f)
    f.write_bytes(f.read_bytes().replace(b"1", b"2", 1))
    os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns))


@pytest.mark.parametrize("mutate,kind,prefix", [
    (_mut_append, "modified", "stream:"), (_mut_add_file, "added", "stream:"), (_mut_remove_file, "removed", "stream:"),
    (_mut_new_empty_dir, "added", "stream:"), (_mut_inplace_same_size_mtime_restored, "modified", "stream:"),
    (_mut_replace_new_inode_same_size_mtime_restored, "modified", "stream:"),
    (_mut_index_sidecar, "added", "index:"), (_mut_evidence_edit, "modified", "evidence:"),
], ids=lambda v: v.__name__.replace("_mut_", "") if callable(v) else str(v))
def test_input_changes_during_the_run_are_detected(tmp_path, monkeypatch, mutate, kind, prefix):
    base = _two_seg(tmp_path)
    orig = c1.Rehearsal._aggregate

    def hooked(self, conn):
        mutate(base)
        return orig(self, conn)

    monkeypatch.setattr(c1.Rehearsal, "_aggregate", hooked)
    rc = rehearse(base, tmp_path)["report"]["read_only_check"]
    assert rc["unchanged"] is False and rc[kind] >= 1
    assert any(e.startswith(f"{kind}:{prefix}") for e in rc["changed_examples"]), rc["changed_examples"]
    assert str(tmp_path) not in json.dumps(rc), "snapshot keys are label-relative"
    assert "tamper" in rc["method"].lower() and "NOT tamper-proofing" in rc["method"]


def test_untouched_input_reports_unchanged_with_counts(tmp_path):
    rc = rehearse(_two_seg(tmp_path), tmp_path)["report"]["read_only_check"]
    assert rc["unchanged"] is True and (rc["added"], rc["removed"], rc["modified"]) == (0, 0, 0)
    assert rc["files_snapshotted"] >= 5 and rc["directories_snapshotted"] >= 2


# ---------------------------------------------------------------------------- scratch disk preflight
_Usage = namedtuple("usage", "total used free")


def _random_ids_dataset(base, segments=6, rows=3000):
    import random
    rng = random.Random(7)
    for s in range(segments):
        write_seg(base, s, [str(rng.getrandbits(48)) for _ in range(rows)])


def test_scratch_preflight_estimate_is_measured_and_not_below_actual_use(tmp_path):
    base = tmp_path / "data"
    _random_ids_dataset(base)
    pf = rehearse(base, tmp_path, with_index=False)["run"]["scratch_preflight"]
    assert pf["status"] == "ok" and pf["occurrence_rows_upper_bound"] == 6 * 3000
    assert pf["bytes_per_row_dense"] > 0 and pf["bytes_per_row_scattered_scaled"] >= pf["bytes_per_row_dense"]
    assert pf["actual_scratch_bytes"] <= pf["conservative_bytes"], "the conservative estimate must cover real use"
    assert pf["expected_bytes"] <= pf["conservative_bytes"]
    assert "labeled estimate" in pf["basis"] and "large scratch volume" in pf["basis"]


def test_scratch_preflight_refuses_when_clearly_too_small_and_creates_nothing(tmp_path, monkeypatch):
    base = tmp_path / "data"
    _random_ids_dataset(base)
    monkeypatch.setattr(c1.shutil, "disk_usage", lambda p: _Usage(10**9, 10**9 - 1000, 1000))
    before = tree_state(base)
    scratch = tmp_path / "_scratch"
    scratch.mkdir()
    with pytest.raises(SystemExit, match="scratch preflight"):
        rehearse(base, tmp_path, with_index=False)
    assert list(scratch.iterdir()) == [] and tree_state(base) == before


def test_scratch_preflight_warn_and_conservative_zone_proceed_with_a_message(tmp_path, monkeypatch):
    base = tmp_path / "data"
    _random_ids_dataset(base)
    pf = rehearse(base, tmp_path, with_index=False)["run"]["scratch_preflight"]
    mid = (pf["expected_bytes"] + pf["conservative_bytes"]) // 2
    assert pf["expected_bytes"] < mid < pf["conservative_bytes"]
    monkeypatch.setattr(c1.shutil, "disk_usage", lambda p: _Usage(10**9, 0, mid))
    out = rehearse(base, tmp_path, with_index=False)
    assert out["run"]["scratch_preflight"]["status"] == "warning" and "larger volume" in out["run"]["scratch_preflight"]["message"]
    monkeypatch.setattr(c1.shutil, "disk_usage", lambda p: _Usage(10**9, 0, 1000))
    warned = rehearse(base, tmp_path, with_index=False, scratch_preflight="warn")
    assert warned["run"]["scratch_preflight"]["status"] == "warning"
    assert rehearse(base, tmp_path, with_index=False, scratch_preflight="off")["run"]["scratch_preflight"]["status"] == "off"
    assert warned["report_digest"] == out["report_digest"], "disk state never enters the deterministic report"


def test_scratch_preflight_cli_flags(tmp_path, monkeypatch, capsys):
    base = tmp_path / "data"
    _random_ids_dataset(base, segments=2)
    monkeypatch.setattr(c1.shutil, "disk_usage", lambda p: _Usage(10**9, 0, 1000))
    common = ["--stream-dir", str(stream_dir(base)), "--exchange", "BINANCE", "--market-type", "linear_perpetual",
              "--event-stream", "trades", "--trade-id-field", "trade_id", "--no-index", "--scratch-dir", str(tmp_path / "sc")]
    with pytest.raises(SystemExit, match="scratch preflight"):
        c1.main(common)
    assert c1.main(common + ["--allow-low-scratch"]) == 0 and "SCRATCH:" in capsys.readouterr().out
    assert c1.main(common + ["--no-scratch-preflight"]) == 0


# ---------------------------------------------------------------------------- cross_only_extra_rows
@pytest.mark.parametrize("segments,cross_only,cross_extra,intra_extra", [
    ([["A", "A", "B"]], 0, 0, 1),                                  # A,A,B : only an intra-segment repetition
    ([["A", "X"], ["A", "Y"]], 1, 1, 0),                           # A in two segments
    ([["A", "A"], ["A"]], 1, 2, 1),                                # A,A in one segment + A in another
    ([["A"], ["A"], ["A"]], 2, 2, 0),                              # A across 3 segments
    ([["A"], ["A"], ["A"], ["A"]], 3, 3, 0),                       # A across 4 segments
    ([["A", "A", "A"], ["A", "A"], ["A"]], 2, 5, 3),               # mixed: 6 occurrences, 3 segments, 3 intra reps
], ids=["A-A-B", "two-segments", "AA-plus-A", "three-segments", "four-segments", "mixed"])
def test_cross_only_extra_rows_metric(tmp_path, segments, cross_only, cross_extra, intra_extra):
    base = tmp_path / "data"
    for i, group in enumerate(segments):
        write_seg(base, i, group)
    out = rehearse(base, tmp_path, with_index=False, detail_out=tmp_path / "d.jsonl")["report"]
    dup = out["duplicates"]
    assert dup["cross_segment"]["cross_only_extra_rows"] == cross_only
    assert dup["cross_segment"]["extra_rows_beyond_one_per_identity"] == (cross_extra if cross_only else 0)
    assert dup["intra_segment"]["extra_rows"] == intra_extra
    assert dup["total_extra_rows_beyond_first_per_identity"] == out["rows"]["with_identity"] - out["identities"]["distinct_total"]
    assert dup["total_extra_rows_beyond_first_per_identity"] == intra_extra + cross_only
    recs = [json.loads(line) for line in (tmp_path / "d.jsonl").read_text().splitlines()]
    cross = [r for r in recs if r["class"] == "cross_segment"]
    assert sum(r["cross_only_extra_rows"] for r in cross) == cross_only


# ---------------------------------------------------------------------------- legacy hourly parquet vs .seg
def _legacy(base, name, rows=4):
    pq.write_table(pa.table({"instrument_key": [INSTR] * rows, "trade_id": [f"L{i}" for i in range(rows)]}),
                   stream_dir(base) / name)


def test_legacy_parquet_overlap_by_hour_is_reported_and_never_merged(tmp_path, monkeypatch):
    base = tmp_path / "data"
    write_seg(base, 0, ["a", "b"])
    write_seg(base, 1, ["c", "d", "e"])
    write_seg(base, 0, ["f"], hour="2026-01-01-03")
    plain = rehearse(base, tmp_path, with_index=False)["report"]
    _legacy(base, "2026-01-01-00.parquet", rows=4)                  # same hour as two .seg  -> overlap
    _legacy(base, "2026-01-01-05.parquet", rows=2)                  # legacy only
    _legacy(base, "2026-01-01-00-000009.parquet", rows=1)           # not a legacy hourly name
    (stream_dir(base) / "2026-01-01-07.parquet").write_bytes(b"junk")   # legacy hour 07, unreadable footer
    read = []
    real = pub.read_segment_table
    monkeypatch.setattr(pub, "read_segment_table", lambda data, label: read.append(label) or real(data, label))
    out = rehearse(base, tmp_path, with_index=False, detail_out=tmp_path / "d.jsonl")["report"]
    assert not [n for n in read if n.endswith(".parquet")], "legacy parquet bytes are never decoded as identities"
    lg = out["legacy_parquet"]
    assert lg["included_in_dedup_index"] is False and lg["legacy_hourly_files"] == 3
    assert lg["hours_with_both"] == 1 and lg["overlap"]["examples"] == ["2026-01-01-00"]
    assert (lg["legacy_only_hours"], lg["segment_only_hours"]) == (2, 1)
    assert lg["unparsed_parquet_names"]["count"] == 1 and lg["legacy_hourly_footer_unreadable"] == 1
    assert lg["legacy_hourly_rows_total"] is None
    assert lg["overlap"] == dict(lg["overlap"], hours=1, legacy_rows=4, segment_files=2, segment_rows=5,
                                 segment_files_unreadable=0, legacy_footer_unreadable=0)
    assert out["identities"] == plain["identities"] and out["duplicates"] == plain["duplicates"], "nothing merged"
    assert out["segments"]["other_files"]["legacy_hourly_parquet_files_not_audited"] == 4
    rec = [json.loads(line) for line in (tmp_path / "d.jsonl").read_text().splitlines() if '"legacy_overlap_hour"' in line]
    assert [(r["hour"], r["legacy_rows"], r["segment_rows"], len(r["segment_files"])) for r in rec] == [("2026-01-01-00", 4, 5, 2)]


# ---------------------------------------------------------------------------- index type safety / future schema (tool level)
def test_non_text_identity_key_is_reported_as_divergence_by_the_tool(tmp_path):
    base = _two_seg(tmp_path)
    db = base / "dedup_state" / f"{STREAM}.sqlite3"
    con = sqlite3.connect(str(db))
    con.execute("UPDATE seen SET identity_key = CAST(identity_key AS BLOB) WHERE identity_key = "
                "(SELECT identity_key FROM seen ORDER BY identity_key LIMIT 1)")
    con.commit()
    con.close()
    before = tree_state(base)
    idx = rehearse(base, tmp_path)["report"]["dedup_index"]
    assert tree_state(base) == before
    assert idx["comparison"]["identity_rows_with_non_text_storage_type"] == 1
    assert idx["comparison"]["indexed_membership_differs_from_bytes"]["count"] == 1


def test_future_index_schema_is_reported_as_unsupported_not_as_a_rebuild(tmp_path):
    base = _two_seg(tmp_path)
    db = base / "dedup_state" / f"{STREAM}.sqlite3"
    con = sqlite3.connect(str(db))
    con.execute(f"PRAGMA user_version={sd.INDEX_SCHEMA_VERSION + 4}")
    con.commit()
    con.close()
    before = tree_state(base)
    report = rehearse(base, tmp_path)["report"]
    assert tree_state(base) == before
    idx = report["dedup_index"]
    assert idx["unsupported_future_schema"] is True and idx["needs_rebuild_for_schema"] is False
    assert idx["schema_problem"].startswith("UNSUPPORTED_FUTURE_SCHEMA") and "comparison" not in idx
    est = report["estimated_work"]
    assert est["index_rebuild_expected"] is False and len(est["startup_refusal_reasons"]) == 1
    assert report["fail_closed_conditions"]["index_unsupported_future_schema"] == 1
    assert any("FUTURE" in w for w in report["warnings"])


def test_index_with_a_damaged_table_page_is_reported_not_raised(tmp_path):
    base = tmp_path / "data"
    for s in range(3):
        write_seg(base, s, [f"id-{s}-{i}" for i in range(4000)])
    build_index(base)
    db = base / "dedup_state" / f"{STREAM}.sqlite3"
    con = sqlite3.connect(str(db))
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.close()
    raw = bytearray(db.read_bytes())
    for page in range(2, len(raw) // 4096 - 1):                      # smash interior pages, keep header + page 1
        raw[page * 4096: page * 4096 + 64] = b"\xff" * 64
    db.write_bytes(bytes(raw))
    out = rehearse(base, tmp_path)["report"]["dedup_index"]
    assert out["readable"] is False and "error" in out and str(tmp_path) not in json.dumps(out)


def test_evidence_is_bound_to_segment_size_not_only_sha(tmp_path):
    """Evidence whose sha256, count and digest are all RIGHT but whose recorded size is wrong is bound to other bytes."""
    base = tmp_path / "data"
    write_seg(base, 0, ids("s", 3))
    write_seg(base, 1, ids("t", 3))
    build_index(base)
    p = evidence_file(base, 0)
    obj = json.loads(p.read_text())
    obj["segment_size"] += 1                                  # ONLY the size is changed
    p.write_text(json.dumps(obj))
    ev = rehearse(base, tmp_path)["report"]["identity_evidence"]["per_segment"]
    assert (ev["ok"]["count"], ev["stale"]["count"], ev["mismatch"]["count"], ev["invalid"]["count"]) == (1, 1, 0, 0)
    key = f"{STREAM}/{seg_path(base, 0).name}"
    s = seg_path(base, 0).read_bytes()
    derived = (3, json.loads(evidence_file(base, 1).read_text())["identity_digest"])
    assert c1.classify_evidence(evidence_file(base, 0).parent, key, hashlib.sha256(s).hexdigest(), len(s) + 1, derived) in ("stale", "mismatch")
    assert c1.classify_evidence(evidence_file(base, 0).parent, key, hashlib.sha256(s).hexdigest(), len(s), (3, obj["identity_digest"])) == "stale"
