"""Phase 1 Part 2: read-only source discovery (collector.collector.source_catalog)."""
import ast
import builtins
import hashlib
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest

from collector.collector import source_catalog as sc
from collector.collector.source_catalog import (
    IdentityStatus,
    IgnoredCategory,
    SourceRootError,
    TrustState,
    discover_sources,
)
from collector.collector.storage_layout import SegmentKind

SEG = "2026-10-11-12-000003.seg"
LEGACY = "2026-10-11-12.parquet"


def _touch(path: Path, data: bytes = b"x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _raw(tmp_path: Path, stream: str) -> Path:
    d = tmp_path / "raw" / stream
    d.mkdir(parents=True, exist_ok=True)
    return d


def _paths(result):
    return [r.relative_path for r in result.records]


def _ignored(result):
    return {i.relative_path: i.category for i in result.ignored}


def _snapshot(root: Path):
    snap = {}
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        for name in sorted(dirnames) + sorted(filenames):
            p = Path(dirpath) / name
            st = p.lstat()
            digest = hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() and not p.is_symlink() else None
            snap[str(p.relative_to(root))] = (st.st_mode, st.st_size, st.st_mtime_ns, digest)
        st = Path(dirpath).lstat()
        snap[str(Path(dirpath).relative_to(root)) + "/"] = (st.st_mode, st.st_mtime_ns)
    return snap


# ---------------------------------------------------------------- discovery

def test_approved_files_are_discovered_with_preserved_relative_paths(tmp_path):
    _touch(_raw(tmp_path, "trades") / SEG, b"abc")
    _touch(_raw(tmp_path, "trades") / "2026-10-11-13.parquet")
    _touch(_raw(tmp_path, "bybit_trades") / "2026-10-11-12-000000.seg")
    result = discover_sources(tmp_path)
    assert _paths(result) == [
        "raw/bybit_trades/2026-10-11-12-000000.seg",
        "raw/trades/2026-10-11-12-000003.seg",
        "raw/trades/2026-10-11-13.parquet",
    ]
    rec = {r.relative_path: r for r in result.records}["raw/trades/2026-10-11-12-000003.seg"]
    assert (rec.stream, rec.date, rec.hour, rec.sequence, rec.kind, rec.file_format, rec.size_bytes) == (
        "trades", "2026-10-11", 12, 3, SegmentKind.SEGMENT, "seg", 3)
    assert result.streams_scanned == ("bybit_trades", "trades")
    assert result.unreadable_streams == ()
    assert result.catalog_schema_version == 1
    assert all(r.catalog_schema_version == 1 for r in result.records)


def test_non_source_files_are_excluded_from_inventory_and_classified(tmp_path):
    d = _raw(tmp_path, "trades")
    _touch(d / SEG)
    _touch(d / (SEG + ".meta.json"))
    _touch(d / "2026-10-11-12-000004.seg.tmp")
    _touch(d / "2026-10-11-12-000005.seg.count.json")
    _touch(d / "2026-10-11-13.parquet.tmp")
    _touch(d / "2026-10-11-12-000003.seg.meta.json.tmp")
    _touch(d / "2026-10-11-12-000003.seg.meta.json.invalid.0")
    _touch(d / "2026-10-11-12-000003.seg.meta.json.orphan.20261011T000000Z")
    _touch(d / ".writer.lock")
    _touch(d / "2026-10-11-14-000001.seg.meta.json")            # marker whose segment is absent
    _touch(d / "2026-10-11-15.parquet.meta.json")               # not a marker name the writer produces
    _touch(d / "notes.txt")
    _touch(d / "quarantine" / "2026-10-11-12-000009.seg")        # must not be descended into
    _touch(d / "quarantine" / "2026-10-11-12-000009.quarantine.json")
    (d / "scratch").mkdir()
    result = discover_sources(tmp_path)
    assert _paths(result) == ["raw/trades/" + SEG]
    ign = _ignored(result)
    base = "raw/trades/"
    assert ign[base + ".writer.lock"] is IgnoredCategory.LOCK_FILE
    assert ign[base + "2026-10-11-12-000004.seg.tmp"] is IgnoredCategory.TEMP_FILE
    assert ign[base + "2026-10-11-13.parquet.tmp"] is IgnoredCategory.TEMP_FILE
    assert ign[base + "2026-10-11-12-000003.seg.meta.json.tmp"] is IgnoredCategory.TEMP_FILE
    assert ign[base + "2026-10-11-12-000005.seg.count.json"] is IgnoredCategory.COUNT_SIDECAR
    assert ign[base + "2026-10-11-12-000003.seg.meta.json.invalid.0"] is IgnoredCategory.MARKER_SIDECAR
    assert ign[base + "2026-10-11-12-000003.seg.meta.json.orphan.20261011T000000Z"] is IgnoredCategory.MARKER_SIDECAR
    assert ign[base + SEG + ".meta.json"] is IgnoredCategory.MARKER_FILE
    assert ign[base + "2026-10-11-14-000001.seg.meta.json"] is IgnoredCategory.MARKER_ORPHAN
    assert ign[base + "2026-10-11-15.parquet.meta.json"] is IgnoredCategory.MARKER_UNRECOGNIZED
    assert ign[base + "notes.txt"] is IgnoredCategory.UNRELATED
    assert ign[base + "quarantine"] is IgnoredCategory.QUARANTINE_DIR
    assert ign[base + "scratch"] is IgnoredCategory.UNRECOGNIZED_DIRECTORY
    assert not any("quarantine/" in p for p in ign), "quarantine contents must never be listed"
    codes = {w.code for w in result.warnings}
    assert {"TEMP_RESIDUE", "ORPHAN_MARKER"} <= codes


def test_marker_only_stream_yields_no_records(tmp_path):
    _touch(_raw(tmp_path, "trades") / "2026-10-11-12-000001.seg.meta.json")
    result = discover_sources(tmp_path)
    assert result.records == ()
    assert any(w.code == "ORPHAN_MARKER" for w in result.warnings)


def test_legacy_and_unmarked_files_carry_explicit_uncertainty_and_nothing_is_confirmed(tmp_path):
    d = _raw(tmp_path, "trades")
    _touch(d / LEGACY)
    _touch(d / "2026-10-11-13-000000.seg")                       # no marker file
    _touch(d / "2026-10-11-14-000000.seg")
    _touch(d / "2026-10-11-14-000000.seg.meta.json")              # marker present, content never read
    by_name = {Path(r.relative_path).name: r for r in discover_sources(tmp_path).records}
    legacy = by_name[LEGACY]
    assert legacy.trust_state is TrustState.LEGACY and legacy.legacy_kind == "hourly_parquet"
    assert legacy.kind is SegmentKind.LEGACY_HOURLY and legacy.sequence is None
    unmarked = by_name["2026-10-11-13-000000.seg"]
    assert unmarked.trust_state is TrustState.UNCONFIRMED and unmarked.reasons == ("NO_MARKER_FILE",)
    marked = by_name["2026-10-11-14-000000.seg"]
    assert marked.trust_state is TrustState.UNKNOWN and marked.marker_file_present is True
    assert marked.reasons == ("MARKER_PRESENT_CONTENT_NOT_READ",)
    assert not {r.trust_state for r in by_name.values()} & {
        TrustState.CONFIRMED, TrustState.CONFIRMED_RETROACTIVE, TrustState.CORRUPT}


# ---------------------------------------------------------------- collisions

def test_legacy_segment_collision_is_reported_and_never_resolved(tmp_path):
    d = _raw(tmp_path, "trades")
    _touch(d / LEGACY)
    _touch(d / "2026-10-11-12-000000.seg")
    _touch(d / "2026-10-11-12-000001.seg")
    _touch(d / "2026-10-11-13-000000.seg")                       # different hour: unaffected
    result = discover_sources(tmp_path)
    by_name = {Path(r.relative_path).name: r for r in result.records}
    assert len(result.records) == 4, "no member of a collision may be dropped or preferred"
    group = "raw/trades/2026-10-11-12"
    for name in (LEGACY, "2026-10-11-12-000000.seg", "2026-10-11-12-000001.seg"):
        assert by_name[name].trust_state is TrustState.AMBIGUOUS
        assert by_name[name].collision_group == group
        assert "COLLISION" in by_name[name].warnings
    assert by_name["2026-10-11-13-000000.seg"].trust_state is not TrustState.AMBIGUOUS
    assert by_name["2026-10-11-13-000000.seg"].collision_group is None
    collisions = [w for w in result.warnings if w.code == "COLLISION"]
    assert len(collisions) == 1 and group in collisions[0].detail


def test_same_hour_in_different_streams_is_not_a_collision(tmp_path):
    _touch(_raw(tmp_path, "trades") / LEGACY)
    _touch(_raw(tmp_path, "orderbook") / "2026-10-11-12-000000.seg")
    result = discover_sources(tmp_path)
    assert not any(w.code == "COLLISION" for w in result.warnings)
    assert all(r.trust_state is not TrustState.AMBIGUOUS for r in result.records)


# ---------------------------------------------------------------- identity

def test_identity_is_name_derived_and_unknown_when_unverified(tmp_path):
    for stream in ("raw_wire", "quality_events", "bybit_trades", "okx_raw_wire", "spot_trades", "orderbook"):
        _touch(_raw(tmp_path, stream) / SEG)
    by_stream = {r.stream: r for r in discover_sources(tmp_path).records}
    assert by_stream["raw_wire"].venue is None
    assert by_stream["raw_wire"].identity_status is IdentityStatus.SHARED_UNVERIFIED
    assert by_stream["quality_events"].venue is None
    assert (by_stream["bybit_trades"].venue, by_stream["bybit_trades"].identity_status) == (
        "BYBIT", IdentityStatus.NAME_PREFIX_REGISTERED)
    assert by_stream["okx_raw_wire"].venue == "OKX"
    assert by_stream["spot_trades"].venue == "BINANCE_SPOT"
    assert (by_stream["orderbook"].venue, by_stream["orderbook"].identity_status) == (
        "BINANCE", IdentityStatus.UNPREFIXED_CONVENTION)


def test_invalid_calendar_names_get_no_segment_identity(tmp_path):
    d = _raw(tmp_path, "trades")
    _touch(d / "2026-13-45-12-000001.seg")
    _touch(d / "2026-10-11-99-000001.seg")
    _touch(d / "2026-02-30-05.parquet")
    result = discover_sources(tmp_path)
    assert len(result.records) == 3
    for rec in result.records:
        assert rec.trust_state is TrustState.INVALID and rec.reasons == ("INVALID_DATE_OR_HOUR_IN_NAME",)
        assert (rec.date, rec.hour, rec.sequence, rec.kind) == (None, None, None, None)


def test_invalid_name_does_not_create_a_collision(tmp_path):
    d = _raw(tmp_path, "trades")
    _touch(d / "2026-10-11-99.parquet")
    _touch(d / "2026-10-11-99-000001.seg")
    result = discover_sources(tmp_path)
    assert not any(w.code == "COLLISION" for w in result.warnings)


# ---------------------------------------------------------------- inputs / traversal / symlinks

@pytest.mark.parametrize("bad", ["../x", "a/b", "", ".", "..", "x\x00y", "a b", "trades/../orderbook"])
def test_traversal_and_malformed_stream_names_are_rejected(tmp_path, bad):
    _raw(tmp_path, "trades")
    with pytest.raises(ValueError):
        discover_sources(tmp_path, streams=[bad])


def test_single_string_streams_argument_is_rejected(tmp_path):
    _raw(tmp_path, "trades")
    with pytest.raises(ValueError):
        discover_sources(tmp_path, streams="trades")


@pytest.mark.parametrize("kwargs", [{"date": "2026-13-01"}, {"date": "20261011"}, {"date": "2026-10-1"},
                                    {"hour": 24}, {"hour": -1}, {"hour": True}, {"hour": "3"}])
def test_bad_filters_are_rejected(tmp_path, kwargs):
    _raw(tmp_path, "trades")
    with pytest.raises(ValueError):
        discover_sources(tmp_path, **kwargs)


def test_invalid_data_roots_raise_source_root_error(tmp_path):
    a_file = _touch(tmp_path / "file.txt")
    for bad in (tmp_path / "missing", a_file, "", "   ", "bad\x00root", None, 123):
        with pytest.raises(SourceRootError):
            discover_sources(bad)


def test_missing_raw_dir_is_a_warning_not_an_error(tmp_path):
    result = discover_sources(tmp_path)
    assert result.records == () and result.streams_scanned == ()
    assert [w.code for w in result.warnings] == ["RAW_DIR_MISSING"]


def test_raw_dir_symlink_is_refused(tmp_path):
    outside = tmp_path / "outside"
    _touch(outside / "trades" / SEG)
    root = tmp_path / "root"
    root.mkdir()
    (root / "raw").symlink_to(outside, target_is_directory=True)
    with pytest.raises(SourceRootError):
        discover_sources(root)


def test_symlinks_are_never_followed(tmp_path):
    outside = tmp_path / "outside"
    _touch(outside / SEG, b"secret")
    root = tmp_path / "root"
    (root / "raw").mkdir(parents=True)
    (root / "raw" / "linked_stream").symlink_to(outside, target_is_directory=True)
    d = _raw(root, "trades")
    (d / "2026-10-11-12-000001.seg").symlink_to(outside / SEG)
    (d / "2026-10-11-13-000001.seg.meta.json").symlink_to(outside / SEG)
    (d / "elsewhere").symlink_to(outside, target_is_directory=True)
    result = discover_sources(root)
    assert _paths(result) == ["raw/trades/2026-10-11-12-000001.seg"]
    rec = result.records[0]
    assert rec.trust_state is TrustState.INVALID and rec.reasons == ("SYMLINK_NOT_FOLLOWED",)
    assert rec.size_bytes is None and rec.marker_file_present is False
    ign = _ignored(result)
    assert ign["raw/linked_stream"] is IgnoredCategory.SYMLINK
    assert ign["raw/trades/elsewhere"] is IgnoredCategory.SYMLINK
    assert "linked_stream" not in result.streams_scanned
    assert sum(w.code == "SYMLINK_NOT_FOLLOWED" for w in result.warnings) >= 3


def test_symlinked_marker_does_not_count_as_marker_evidence(tmp_path):
    outside = _touch(tmp_path / "outside.json")
    d = _raw(tmp_path / "root", "trades")
    _touch(d / SEG)
    (d / (SEG + ".meta.json")).symlink_to(outside)
    rec = discover_sources(tmp_path / "root").records[0]
    assert rec.marker_file_present is False and rec.trust_state is TrustState.UNCONFIRMED


def test_requested_missing_stream_is_a_warning(tmp_path):
    _raw(tmp_path, "trades")
    result = discover_sources(tmp_path, streams=["trades", "orderbook"])
    assert result.streams_scanned == ("trades",)
    assert [w.code for w in result.warnings] == ["STREAM_NOT_FOUND"]


def test_unreadable_stream_is_reported_as_unknown_not_empty(tmp_path, monkeypatch):
    _touch(_raw(tmp_path, "trades") / SEG)
    _touch(_raw(tmp_path, "orderbook") / SEG)
    real = os.scandir

    def flaky(path):
        if str(path).endswith(os.path.join("raw", "trades")):
            raise PermissionError(13, "denied")
        return real(path)

    monkeypatch.setattr(sc.os, "scandir", flaky)
    result = discover_sources(tmp_path)
    assert result.unreadable_streams == ("trades",)
    assert _paths(result) == ["raw/orderbook/" + SEG]
    assert any(w.code == "STREAM_UNREADABLE" for w in result.warnings)


# ---------------------------------------------------------------- per-file stat failure

class _StatFailsEntry:
    """Delegates to a real DirEntry but makes ``stat()`` raise for chosen names."""

    def __init__(self, entry, failing):
        self._entry, self._failing = entry, failing
        self.name = entry.name

    def is_symlink(self):
        return self._entry.is_symlink()

    def is_dir(self, *, follow_symlinks=True):
        return self._entry.is_dir(follow_symlinks=follow_symlinks)

    def is_file(self, *, follow_symlinks=True):
        return self._entry.is_file(follow_symlinks=follow_symlinks)

    def stat(self, *, follow_symlinks=True):
        if self.name in self._failing:
            raise OSError(5, "simulated per-file stat failure")
        return self._entry.stat(follow_symlinks=follow_symlinks)


def _fail_stat_for(monkeypatch, *names):
    real = os.scandir
    failing = frozenset(names)

    class _Scan:
        def __init__(self, it):
            self._it = it

        def __enter__(self):
            return iter([_StatFailsEntry(e, failing) for e in self._it])

        def __exit__(self, *exc):
            self._it.close()

    monkeypatch.setattr(sc.os, "scandir", lambda path: _Scan(real(path)))


def _by_name(result):
    return {Path(r.relative_path).name: r for r in result.records}


def test_stat_failure_on_unmarked_segment_is_unknown_not_unconfirmed(tmp_path, monkeypatch):
    d = _raw(tmp_path, "bybit_trades")
    _touch(d / SEG)
    _touch(d / "2026-10-11-13-000000.seg")                       # healthy neighbour, also unmarked
    _fail_stat_for(monkeypatch, SEG)
    rec = _by_name(discover_sources(tmp_path))[SEG]
    assert rec.trust_state is TrustState.UNKNOWN
    assert rec.reasons == ("STAT_FAILED",)
    assert rec.warnings == ("STAT_FAILED",)
    assert rec.size_bytes is None
    # diagnostic and identity evidence is preserved separately
    assert rec.marker_file_present is False
    assert (rec.venue, rec.identity_status) == ("BYBIT", IdentityStatus.NAME_PREFIX_REGISTERED)
    assert (rec.date, rec.hour, rec.sequence, rec.kind) == ("2026-10-11", 12, 3, SegmentKind.SEGMENT)
    assert rec.relative_path == "raw/bybit_trades/" + SEG
    healthy = _by_name(discover_sources(tmp_path))["2026-10-11-13-000000.seg"]
    assert healthy.trust_state is TrustState.UNCONFIRMED and healthy.size_bytes is not None


def test_stat_failure_on_legacy_parquet_is_unknown_but_stays_identified_as_legacy(tmp_path, monkeypatch):
    _touch(_raw(tmp_path, "trades") / LEGACY)
    _fail_stat_for(monkeypatch, LEGACY)
    rec = discover_sources(tmp_path).records[0]
    assert rec.trust_state is TrustState.UNKNOWN
    assert rec.reasons == ("STAT_FAILED",) and rec.warnings == ("STAT_FAILED",)
    assert rec.legacy_kind == "hourly_parquet"                   # filename identity survives
    assert rec.kind is SegmentKind.LEGACY_HOURLY and rec.sequence is None
    assert rec.size_bytes is None


def test_stat_failure_with_marker_present_keeps_marker_evidence_and_is_unknown(tmp_path, monkeypatch):
    d = _raw(tmp_path, "trades")
    _touch(d / SEG)
    _touch(d / (SEG + ".meta.json"))
    _fail_stat_for(monkeypatch, SEG)
    rec = discover_sources(tmp_path).records[0]
    assert rec.trust_state is TrustState.UNKNOWN and rec.reasons == ("STAT_FAILED",)
    assert rec.marker_file_present is True


def test_stat_failure_never_yields_confirmed_or_corrupt(tmp_path, monkeypatch):
    d = _raw(tmp_path, "trades")
    names = (LEGACY, "2026-10-11-13-000000.seg", "2026-10-11-14-000000.seg")
    for name in names:
        _touch(d / name)
    _touch(d / "2026-10-11-14-000000.seg.meta.json")
    _fail_stat_for(monkeypatch, *names)
    states = {r.trust_state for r in discover_sources(tmp_path).records}
    assert states == {TrustState.UNKNOWN}


def test_collision_outranks_stat_failure_and_keeps_both_diagnostics(tmp_path, monkeypatch):
    d = _raw(tmp_path, "trades")
    _touch(d / LEGACY)
    _touch(d / "2026-10-11-12-000000.seg")
    _fail_stat_for(monkeypatch, LEGACY)
    result = discover_sources(tmp_path)
    by_name = _by_name(result)
    failed, healthy = by_name[LEGACY], by_name["2026-10-11-12-000000.seg"]
    # the collision is established from names alone, independent of the failed stat
    assert failed.trust_state is TrustState.AMBIGUOUS
    assert failed.reasons == ("LEGACY_SEGMENT_COLLISION", "STAT_FAILED")
    assert failed.warnings == ("COLLISION", "STAT_FAILED")
    assert failed.collision_group == "raw/trades/2026-10-11-12" and failed.size_bytes is None
    assert failed.legacy_kind == "hourly_parquet"
    assert healthy.trust_state is TrustState.AMBIGUOUS and healthy.reasons == ("LEGACY_SEGMENT_COLLISION",)
    assert healthy.collision_group == failed.collision_group
    assert len(result.records) == 2, "a collision member with a failed stat must not be dropped"
    assert sum(w.code == "COLLISION" for w in result.warnings) == 1


def test_structural_invalidity_outranks_stat_failure(tmp_path, monkeypatch):
    bad = "2026-13-45-12-000001.seg"
    _touch(_raw(tmp_path, "trades") / bad)
    _fail_stat_for(monkeypatch, bad)
    rec = discover_sources(tmp_path).records[0]
    assert rec.trust_state is TrustState.INVALID
    assert rec.reasons == ("INVALID_DATE_OR_HOUR_IN_NAME", "STAT_FAILED")
    assert rec.warnings == ("STAT_FAILED",)


def test_stat_failure_does_not_remove_the_file_from_the_inventory_or_touch_it(tmp_path, monkeypatch):
    d = _raw(tmp_path, "trades")
    _touch(d / SEG, b"data")
    before = _snapshot(tmp_path)
    _fail_stat_for(monkeypatch, SEG)
    result = discover_sources(tmp_path)
    monkeypatch.undo()
    assert _paths(result) == ["raw/trades/" + SEG]
    assert _snapshot(tmp_path) == before


# ---------------------------------------------------------------- filters

def test_date_and_hour_filters(tmp_path):
    d = _raw(tmp_path, "trades")
    for name in ("2026-10-11-12-000000.seg", "2026-10-11-13-000000.seg", "2026-10-12-12-000000.seg", "notes.txt"):
        _touch(d / name)
    _touch(d / "2026-10-12-12-000000.seg.tmp")
    assert [Path(p).name for p in _paths(discover_sources(tmp_path, date="2026-10-11"))] == [
        "2026-10-11-12-000000.seg", "2026-10-11-13-000000.seg"]
    only = discover_sources(tmp_path, date="2026-10-12", hour=12)
    assert [Path(p).name for p in _paths(only)] == ["2026-10-12-12-000000.seg"]
    assert "raw/trades/2026-10-12-12-000000.seg.tmp" in _ignored(only)
    assert "raw/trades/2026-10-11-12-000000.seg.tmp" not in _ignored(only)
    assert not any(w.code == "ORPHAN_MARKER" for w in only.warnings)


def test_filtered_out_marker_is_not_reported_as_orphan(tmp_path):
    d = _raw(tmp_path, "trades")
    _touch(d / "2026-10-11-12-000000.seg")
    _touch(d / "2026-10-11-12-000000.seg.meta.json")
    result = discover_sources(tmp_path, date="2026-10-12")
    assert result.records == () and not any(w.code == "ORPHAN_MARKER" for w in result.warnings)


# ---------------------------------------------------------------- determinism

def test_ordering_is_stable_regardless_of_directory_listing_order(tmp_path, monkeypatch):
    for stream in ("trades", "orderbook", "bybit_trades"):
        d = _raw(tmp_path, stream)
        for name in ("2026-10-11-12-000002.seg", "2026-10-11-12-000010.seg", "2026-10-11-13.parquet", ".writer.lock"):
            _touch(d / name)
    baseline = discover_sources(tmp_path)
    real = os.scandir

    class Reversed:
        def __init__(self, it):
            self._items = list(it)[::-1]
            self._it = it

        def __enter__(self):
            return iter(self._items)

        def __exit__(self, *exc):
            self._it.close()

    monkeypatch.setattr(sc.os, "scandir", lambda p: Reversed(real(p)))
    shuffled = discover_sources(tmp_path)
    assert shuffled == baseline
    assert _paths(baseline) == sorted(_paths(baseline), key=os.fsencode)
    assert discover_sources(tmp_path) == shuffled


def test_relative_paths_do_not_depend_on_where_the_root_lives(tmp_path):
    for name in ("a", "b"):
        _touch(_raw(tmp_path / name, "trades") / SEG)
    assert discover_sources(tmp_path / "a").records == discover_sources(tmp_path / "b").records


def test_records_are_immutable(tmp_path):
    _touch(_raw(tmp_path, "trades") / SEG)
    result = discover_sources(tmp_path)
    with pytest.raises(Exception):
        result.records[0].trust_state = TrustState.CONFIRMED  # type: ignore[misc]
    assert isinstance(result.records, tuple)


# ---------------------------------------------------------------- safety invariants

def test_discovery_does_not_create_delete_rename_or_rewrite_anything(tmp_path, monkeypatch):
    d = _raw(tmp_path, "trades")
    _touch(d / SEG, b"segment-bytes")
    _touch(d / (SEG + ".meta.json"), b"{}")
    _touch(d / LEGACY, b"legacy")
    _touch(d / "2026-10-11-14-000000.seg.tmp", b"partial")
    _touch(d / ".writer.lock", b"pid 1")
    _touch(d / "quarantine" / "q.seg", b"q")
    _touch(d / "quarantine" / "q.seg.quarantine.json", b"{}")
    _touch(tmp_path / "dedup_state" / "trades.sqlite3", b"sqlite")
    _touch(tmp_path / "daily" / "trades" / "2026-10-11_trades.parquet", b"daily")
    before = _snapshot(tmp_path)

    def refuse(name):
        def _raise(*a, **k):
            raise AssertionError(f"discovery attempted a forbidden operation: {name}")
        return _raise

    for mod, attrs in (
        (os, ("rename", "replace", "unlink", "remove", "rmdir", "mkdir", "makedirs", "removedirs",
              "truncate", "chmod", "chown", "link", "symlink", "utime", "open", "fsync", "write")),
        (shutil, ("rmtree", "move", "copy", "copy2", "copyfile", "copytree")),
        (Path, ("write_bytes", "write_text", "touch", "unlink", "rename", "replace", "mkdir", "rmdir", "open")),
    ):
        for attr in attrs:
            if hasattr(mod, attr):
                monkeypatch.setattr(mod, attr, refuse(f"{mod.__name__}.{attr}"))
    monkeypatch.setattr(builtins, "open", refuse("builtins.open"))

    discover_sources(tmp_path)
    discover_sources(tmp_path, streams=["trades"], date="2026-10-11", hour=12)
    monkeypatch.undo()
    assert _snapshot(tmp_path) == before


def test_module_source_has_no_mutation_recovery_or_row_reading_dependencies():
    source = Path(sc.__file__).read_text()
    tree = ast.parse(source)
    forbidden_names = {
        "confirm_unmarked", "preserve_invalid", "orphan_marker", "quarantine_segment", "write_marker_atomic",
        "startup_reconcile", "iter_segments", "read_marker", "classify_segment", "sha256_file",
        "read_segment_table", "open", "fsync_file", "fsync_dir", "rmtree",
    }
    forbidden_modules = {"pandas", "pyarrow", "sqlite3", "shutil", "tempfile", "subprocess"}
    imported_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            imported_modules.add(module.split(".")[0] if node.level == 0 else module)
            assert module != "publication" and not module.endswith("segment_dedup") \
                and not module.endswith("parquet_writer"), f"forbidden import: {module}"
        elif isinstance(node, ast.Name):
            assert node.id not in forbidden_names, f"forbidden name used: {node.id}"
        elif isinstance(node, ast.Attribute):
            assert node.attr not in forbidden_names, f"forbidden attribute used: {node.attr}"
    assert not imported_modules & forbidden_modules, imported_modules & forbidden_modules


# ---------------------------------------------------------------- contract with the real writer

def test_constants_match_writer_and_publication_modules():
    from collector.collector import parquet_writer, publication
    assert sc.WRITER_LOCK_FILENAME == parquet_writer.WRITER_LOCK_FILENAME
    assert sc.QUARANTINE_DIRNAME == publication.QUARANTINE_DIRNAME


def test_real_writer_output_is_classified_correctly(tmp_path):
    from collector.collector.parquet_writer import ParquetWriter
    schema = pa.schema([("timestamp", pa.timestamp("ms", tz="UTC")), ("value", pa.int64())])
    writer = ParquetWriter("example", schema, base_dir=str(tmp_path), segment_rows=1)
    writer.write({"timestamp": datetime.now(timezone.utc), "value": 7})
    writer.close()
    stream_dir = tmp_path / "raw" / "example"
    assert (stream_dir / ".writer.lock").exists()
    before = _snapshot(tmp_path)
    result = discover_sources(tmp_path)
    assert _snapshot(tmp_path) == before
    assert len(result.records) == 1
    rec = result.records[0]
    assert rec.file_format == "seg" and rec.kind is SegmentKind.SEGMENT and rec.sequence == 0
    assert rec.marker_file_present is True
    assert rec.trust_state is TrustState.UNKNOWN
    ign = _ignored(result)
    assert ign["raw/example/.writer.lock"] is IgnoredCategory.LOCK_FILE
    assert ign[rec.relative_path + ".meta.json"] is IgnoredCategory.MARKER_FILE
    assert set(ign.values()) <= {IgnoredCategory.LOCK_FILE, IgnoredCategory.MARKER_FILE}


# ---------------------------------------------------------------- per-entry metadata failure (Part 2B)
#
# ``is_symlink()`` / ``is_dir()`` / ``is_file()`` can each raise OSError. When that happens the
# file type is NOT established, so a source-like name must stay in the inventory as UNKNOWN
# (never UNCONFIRMED / LEGACY / INVALID-as-symlink) and must never be dropped.

_META_OPS = ("is_symlink", "is_dir", "is_file")
_META_FAILED = "ENTRY_METADATA_FAILED"
_SEG_B = "2026-10-11-13-000000.seg"
_PROVEN_STATES = {
    TrustState.CONFIRMED, TrustState.CONFIRMED_RETROACTIVE, TrustState.UNCONFIRMED,
    TrustState.LEGACY, TrustState.CORRUPT,
}


class _MetaFailsEntry:
    """Delegates to a real DirEntry, but makes chosen type probes raise OSError for chosen
    names and records every probe so tests can prove what was (not) called."""

    def __init__(self, entry, failing, calls):
        self._entry, self._failing, self._calls = entry, failing, calls
        self.name = entry.name

    def _probe(self, op, follow_symlinks):
        self._calls.append((self.name, op, follow_symlinks))
        if op in self._failing.get(self.name, ()):
            raise OSError(5, f"simulated {op} failure")

    def is_symlink(self):
        self._probe("is_symlink", None)
        return self._entry.is_symlink()

    def is_dir(self, *, follow_symlinks=True):
        self._probe("is_dir", follow_symlinks)
        return self._entry.is_dir(follow_symlinks=follow_symlinks)

    def is_file(self, *, follow_symlinks=True):
        self._probe("is_file", follow_symlinks)
        return self._entry.is_file(follow_symlinks=follow_symlinks)

    def stat(self, *, follow_symlinks=True):
        self._probe("stat", follow_symlinks)
        return self._entry.stat(follow_symlinks=follow_symlinks)


def _fail_ops(monkeypatch, failing):
    """``failing``: {entry name: {probe names that raise}}. Returns the live probe-call log."""
    real = os.scandir
    frozen = {name: frozenset(ops) for name, ops in failing.items()}
    calls: list = []

    class _Scan:
        def __init__(self, it):
            self._it = it

        def __enter__(self):
            return iter([_MetaFailsEntry(e, frozen, calls) for e in self._it])

        def __exit__(self, *exc):
            self._it.close()

    monkeypatch.setattr(sc.os, "scandir", lambda path: _Scan(real(path)))
    return calls


def _meta_warnings(result):
    return [w for w in result.warnings if w.code == _META_FAILED]


def _probes(calls, name):
    return [op for (n, op, _follow) in calls if n == name]


@pytest.mark.parametrize("op", _META_OPS)
def test_type_probe_failure_on_segment_keeps_an_unknown_record(tmp_path, monkeypatch, op):
    d = _raw(tmp_path, "bybit_trades")
    _touch(d / SEG, b"payload")
    _touch(d / _SEG_B, b"neighbour")
    calls = _fail_ops(monkeypatch, {SEG: {op}})
    result = discover_sources(tmp_path)
    by_name = _by_name(result)

    assert len(result.records) == 2, "the uncertain candidate must not be dropped"
    rec = by_name[SEG]
    assert rec.relative_path == "raw/bybit_trades/" + SEG
    assert rec.trust_state is TrustState.UNKNOWN
    assert rec.reasons == (_META_FAILED,) and rec.warnings == (_META_FAILED,)
    assert rec.size_bytes is None
    # filename-derived identity and name-level evidence survive
    assert (rec.venue, rec.identity_status) == ("BYBIT", IdentityStatus.NAME_PREFIX_REGISTERED)
    assert (rec.date, rec.hour, rec.sequence, rec.kind) == ("2026-10-11", 12, 3, SegmentKind.SEGMENT)
    assert rec.file_format == "seg" and rec.marker_file_present is False
    assert rec.collision_group is None and rec.legacy_kind is None
    # no false confirmation and no false type claim
    assert rec.trust_state not in _PROVEN_STATES and rec.trust_state is not TrustState.INVALID
    assert "SYMLINK_NOT_FOLLOWED" not in rec.reasons and "NOT_A_REGULAR_FILE" not in rec.reasons
    assert not any(w.code == "SYMLINK_NOT_FOLLOWED" for w in result.warnings)
    assert "NO_MARKER_FILE" not in rec.reasons
    # not silently re-filed as an ignored entry; the failure is visible with its cause
    assert rec.relative_path not in _ignored(result)
    (warning,) = _meta_warnings(result)
    assert warning.relative_path == rec.relative_path and op in warning.detail
    # the probe sequence stops at the failing call; nothing else was attempted on the entry
    assert _probes(calls, SEG)[-1] == op and "stat" not in _probes(calls, SEG)
    # a healthy neighbour is classified exactly as before
    healthy = by_name[_SEG_B]
    assert healthy.trust_state is TrustState.UNCONFIRMED and healthy.size_bytes == len(b"neighbour")


@pytest.mark.parametrize("op", _META_OPS)
def test_type_probe_failure_on_legacy_parquet_is_unknown_but_stays_identified_as_legacy(tmp_path, monkeypatch, op):
    _touch(_raw(tmp_path, "okx_trades") / LEGACY)
    _fail_ops(monkeypatch, {LEGACY: {op}})
    result = discover_sources(tmp_path)
    (rec,) = result.records
    assert rec.trust_state is TrustState.UNKNOWN
    assert rec.reasons == (_META_FAILED,) and rec.warnings == (_META_FAILED,)
    assert "LEGACY_HOURLY_PARQUET" not in rec.reasons
    assert rec.legacy_kind == "hourly_parquet"                    # filename identity survives
    assert rec.kind is SegmentKind.LEGACY_HOURLY and rec.sequence is None and rec.file_format == "parquet"
    assert (rec.date, rec.hour) == ("2026-10-11", 12)
    assert (rec.venue, rec.identity_status) == ("OKX", IdentityStatus.NAME_PREFIX_REGISTERED)
    assert rec.size_bytes is None
    assert len(_meta_warnings(result)) == 1


@pytest.mark.parametrize("op", _META_OPS)
@pytest.mark.parametrize("bad", ["2026-13-45-12-000001.seg", "2026-10-11-24.parquet"])
def test_metadata_failure_on_impossible_date_hour_keeps_structural_invalidity(tmp_path, monkeypatch, op, bad):
    _touch(_raw(tmp_path, "trades") / bad)
    _fail_ops(monkeypatch, {bad: {op}})
    result = discover_sources(tmp_path)
    (rec,) = result.records
    assert rec.trust_state is TrustState.INVALID
    assert rec.reasons == ("INVALID_DATE_OR_HOUR_IN_NAME", _META_FAILED)
    assert rec.warnings == (_META_FAILED,)
    assert (rec.date, rec.hour, rec.sequence, rec.kind, rec.legacy_kind) == (None, None, None, None, None)
    assert rec.size_bytes is None
    assert "SYMLINK_NOT_FOLLOWED" not in rec.reasons and "NOT_A_REGULAR_FILE" not in rec.reasons
    (warning,) = _meta_warnings(result)
    assert warning.relative_path == "raw/trades/" + bad and op in warning.detail


@pytest.mark.parametrize("op", _META_OPS)
@pytest.mark.parametrize("failing_side", ["legacy", "segment"])
def test_metadata_failure_in_a_possible_collision_stays_ambiguous_and_is_not_resolved(
        tmp_path, monkeypatch, op, failing_side):
    d = _raw(tmp_path, "trades")
    seg = "2026-10-11-12-000000.seg"
    _touch(d / LEGACY, b"L")
    _touch(d / seg, b"S")
    failing_name, healthy_name = (LEGACY, seg) if failing_side == "legacy" else (seg, LEGACY)
    _fail_ops(monkeypatch, {failing_name: {op}})
    result = discover_sources(tmp_path)
    by_name = _by_name(result)
    failed, healthy = by_name[failing_name], by_name[healthy_name]

    assert len(result.records) == 2, "neither collision member may be dropped"
    # the collision is never resolved in favour of the verifiable member, and nothing is "valid"
    assert {r.trust_state for r in result.records} == {TrustState.AMBIGUOUS}
    assert not ({r.trust_state for r in result.records} & _PROVEN_STATES)
    assert failed.reasons == ("LEGACY_SEGMENT_COLLISION", _META_FAILED)
    assert failed.warnings == ("COLLISION", _META_FAILED)
    assert failed.size_bytes is None and healthy.size_bytes == 1
    assert healthy.reasons == ("LEGACY_SEGMENT_COLLISION",) and healthy.warnings == ("COLLISION",)
    assert failed.collision_group == healthy.collision_group == "raw/trades/2026-10-11-12"
    if failing_side == "legacy":
        assert failed.legacy_kind == "hourly_parquet"
    collisions = [w for w in result.warnings if w.code == "COLLISION"]
    assert len(collisions) == 1 and LEGACY in collisions[0].detail and seg in collisions[0].detail
    (warning,) = _meta_warnings(result)
    assert warning.relative_path == failed.relative_path and op in warning.detail


@pytest.mark.parametrize("op", _META_OPS)
@pytest.mark.parametrize("name", [
    "notes.txt",
    ".writer.lock",
    "2026-10-11-12-000004.seg.tmp",
    "2026-10-11-12-000005.seg.count.json",
    SEG + ".meta.json",
])
def test_unrelated_names_stay_excluded_but_their_inspection_failure_is_visible(tmp_path, monkeypatch, op, name):
    d = _raw(tmp_path, "trades")
    _touch(d / name)
    _touch(d / _SEG_B)
    _fail_ops(monkeypatch, {name: {op}})
    result = discover_sources(tmp_path)

    assert _paths(result) == ["raw/trades/" + _SEG_B], "a non-source name must not become a record"
    rel = "raw/trades/" + name
    # unverified type: only the generic bucket, never a category that implies a verified file kind
    assert _ignored(result)[rel] is IgnoredCategory.UNRELATED
    (warning,) = _meta_warnings(result)
    assert warning.relative_path == rel and op in warning.detail
    # and nothing else is claimed about it (no TEMP_RESIDUE / ORPHAN_MARKER / SYMLINK warnings)
    assert {w.code for w in result.warnings} == {_META_FAILED}


def test_uncertain_entries_are_never_opened_hashed_followed_or_modified(tmp_path, monkeypatch):
    import io
    d = _raw(tmp_path, "trades")
    outside = _touch(tmp_path / "outside.bin", b"outside-secret")
    (d / SEG).symlink_to(outside)                  # uncertain entry that really IS a symlink
    legacy = "2026-10-11-14.parquet"               # different hour from SEG: no collision here
    _touch(d / legacy, b"legacy-bytes")            # uncertain entry that really is a regular file
    before = _snapshot(tmp_path)
    guarded = (SEG, legacy, "outside.bin")
    calls = _fail_ops(monkeypatch, {SEG: {"is_symlink"}, legacy: {"is_file"}})

    def refuse(label):
        def _raise(*a, **k):
            raise AssertionError(f"forbidden operation on an uncertain entry: {label}")
        return _raise

    def guard(label, real):
        def _wrapped(path, *a, **k):
            if os.fspath(path).endswith(guarded):
                raise AssertionError(f"forbidden operation on an uncertain entry: {label}({path!r})")
            return real(path, *a, **k)
        return _wrapped

    monkeypatch.setattr(builtins, "open", refuse("builtins.open"))
    monkeypatch.setattr(io, "open", refuse("io.open"))
    monkeypatch.setattr(os, "open", refuse("os.open"))
    monkeypatch.setattr(os, "readlink", refuse("os.readlink"))
    monkeypatch.setattr(os, "stat", guard("os.stat", os.stat))
    monkeypatch.setattr(os, "lstat", guard("os.lstat", os.lstat))
    result = discover_sources(tmp_path)
    monkeypatch.undo()

    by_name = _by_name(result)
    assert set(by_name) == {SEG, legacy}
    for rec in by_name.values():
        assert rec.trust_state is TrustState.UNKNOWN and rec.reasons == (_META_FAILED,)
        assert rec.size_bytes is None
    # the real symlink is neither claimed as a symlink nor followed
    assert not any(w.code == "SYMLINK_NOT_FOLLOWED" for w in result.warnings)
    assert all(follow is not True for (_n, _op, follow) in calls), "a probe followed symlinks"
    assert not any(op == "stat" for (_n, op, _f) in calls), "an uncertain entry was stat()ed"
    assert _snapshot(tmp_path) == before
    assert outside.read_bytes() == b"outside-secret" and (d / legacy).read_bytes() == b"legacy-bytes"
