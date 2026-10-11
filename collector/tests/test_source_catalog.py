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
    MarkerPresence,
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
    assert marked.trust_state is TrustState.UNKNOWN and marked.marker_presence is MarkerPresence.REGULAR_FILE
    assert legacy.marker_presence is MarkerPresence.NOT_APPLICABLE
    assert unmarked.marker_presence is MarkerPresence.ABSENT
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
    assert rec.size_bytes is None and rec.marker_presence is MarkerPresence.ABSENT
    ign = _ignored(result)
    assert ign["raw/linked_stream"] is IgnoredCategory.SYMLINK
    assert ign["raw/trades/elsewhere"] is IgnoredCategory.SYMLINK
    assert "linked_stream" not in result.streams_scanned
    assert sum(w.code == "SYMLINK_NOT_FOLLOWED" for w in result.warnings) >= 3


def test_symlinked_marker_is_neither_marker_evidence_nor_a_confirmed_missing_marker(tmp_path):
    outside = _touch(tmp_path / "outside.json")
    d = _raw(tmp_path / "root", "trades")
    _touch(d / SEG)
    (d / (SEG + ".meta.json")).symlink_to(outside)
    rec = discover_sources(tmp_path / "root").records[0]
    assert rec.marker_presence is MarkerPresence.NON_REGULAR
    assert rec.trust_state is TrustState.UNKNOWN and rec.reasons == ("MARKER_NOT_REGULAR",)
    assert "NO_MARKER_FILE" not in rec.reasons


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
    assert rec.marker_presence is MarkerPresence.ABSENT
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
    assert rec.marker_presence is MarkerPresence.REGULAR_FILE


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
    assert rec.marker_presence is MarkerPresence.REGULAR_FILE
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
    assert rec.file_format == "seg" and rec.marker_presence is MarkerPresence.ABSENT
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


# ---------------------------------------------------------------- Part 2C: unknown is not absent
#
# 2C-A: a type probe that raises on an entry directly under ``raw/`` must not hide a possible
#       stream directory (it used to be filed as UNRELATED).
# 2C-B: a marker entry whose type probe raised is UNKNOWN presence, never confirmed absence (it used
#       to become "absent" -> UNCONFIRMED/NO_MARKER_FILE).
# 2D:   ``marker_presence`` is a five-state ``MarkerPresence``; only ABSENT is "confirmed missing".
#       A symlink / directory / other non-regular object at the marker path is NON_REGULAR, and a
#       legacy hourly Parquet record is NOT_APPLICABLE (see the 2D section at the end of the file).

_ROOT_OPS = ("is_symlink", "is_dir")        # the only probes the root-level loop makes
_NOT_FOUND = "STREAM_NOT_FOUND"
_UNINSPECTABLE = "STREAM_UNINSPECTABLE"
_MARKER_UNKNOWN = "MARKER_PRESENCE_UNKNOWN"


def _log_scandir(monkeypatch):
    """Wrap whichever ``os.scandir`` is installed and return the list of directories listed."""
    inner = sc.os.scandir
    listed: list = []

    def logged(path):
        listed.append(os.fspath(path))
        return inner(path)

    monkeypatch.setattr(sc.os, "scandir", logged)
    return listed


def _reverse_listing(monkeypatch):
    """Wrap the installed ``os.scandir`` so every directory is listed in reverse order."""
    inner = sc.os.scandir

    class _Rev:
        def __init__(self, cm):
            self._cm = cm

        def __enter__(self):
            return iter(list(self._cm.__enter__())[::-1])

        def __exit__(self, *exc):
            return self._cm.__exit__(*exc)

    monkeypatch.setattr(sc.os, "scandir", lambda path: _Rev(inner(path)))


def _forbid_io(monkeypatch, guarded_suffixes):
    """Make open/read/hash/readlink impossible, and stat/lstat of the guarded paths."""
    import io

    def refuse(label):
        def _raise(*a, **k):
            raise AssertionError(f"forbidden operation on an uncertain entry: {label}")
        return _raise

    def guard(label, real):
        def _wrapped(path, *a, **k):
            if os.fspath(path).endswith(tuple(guarded_suffixes)):
                raise AssertionError(f"forbidden operation on an uncertain entry: {label}({path!r})")
            return real(path, *a, **k)
        return _wrapped

    monkeypatch.setattr(builtins, "open", refuse("builtins.open"))
    monkeypatch.setattr(io, "open", refuse("io.open"))
    monkeypatch.setattr(os, "open", refuse("os.open"))
    monkeypatch.setattr(os, "readlink", refuse("os.readlink"))
    monkeypatch.setattr(os, "stat", guard("os.stat", os.stat))
    monkeypatch.setattr(os, "lstat", guard("os.lstat", os.lstat))


# ---- 2C-A: root-level probe failure -------------------------------------------------------

@pytest.mark.parametrize("op", _ROOT_OPS)
def test_root_probe_failure_keeps_a_possible_stream_visible_and_is_not_scanned(tmp_path, monkeypatch, op):
    _touch(_raw(tmp_path, "trades") / SEG, b"hidden-if-dropped")
    _touch(_raw(tmp_path, "orderbook") / SEG, b"ok")
    before = _snapshot(tmp_path)
    calls = _fail_ops(monkeypatch, {"trades": {op}})
    listed = _log_scandir(monkeypatch)
    result = discover_sources(tmp_path)
    monkeypatch.undo()          # os.walk in _snapshot must see the real scandir

    # not silently filed as anything: not "unrelated", not a directory/symlink/file category
    assert not any(i.relative_path == "raw/trades" or i.relative_path.startswith("raw/trades/")
                   for i in result.ignored)
    # the failure is visible, with the entry's relative path and the probe that failed
    (warning,) = _meta_warnings(result)
    assert warning.relative_path == "raw/trades"
    assert warning.detail.startswith(f"{op}: OSError") and "simulated" in warning.detail
    # claimed nothing about its type, and did not misreport it as absent or unlistable
    assert {w.code for w in result.warnings} == {_META_FAILED}
    assert result.unreadable_streams == ()
    # semantic accuracy of streams_scanned: only what was actually listed
    assert result.streams_scanned == ("orderbook",)
    assert _paths(result) == ["raw/orderbook/" + SEG]
    # never traversed, never followed, never stat()ed, and the probe sequence stopped at the failure
    assert not any(p.endswith(os.path.join("raw", "trades")) for p in listed)
    assert _probes(calls, "trades")[-1] == op and "is_file" not in _probes(calls, "trades")
    assert "stat" not in _probes(calls, "trades")
    assert all(follow is not True for (_n, _op, follow) in calls)
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("op", _ROOT_OPS)
def test_requested_stream_that_cannot_be_inspected_is_not_reported_as_not_found(tmp_path, monkeypatch, op):
    _touch(_raw(tmp_path, "trades") / SEG)
    _touch(_raw(tmp_path, "orderbook") / SEG)
    _fail_ops(monkeypatch, {"trades": {op}})
    result = discover_sources(tmp_path, streams=["trades", "orderbook", "absent"])

    by_code: dict = {}
    for w in result.warnings:
        by_code.setdefault(w.code, []).append(w)
    # the uninspectable request is "unknown", the genuinely absent one is still "not found"
    assert [w.relative_path for w in by_code[_UNINSPECTABLE]] == ["raw/trades"]
    assert [w.relative_path for w in by_code[_NOT_FOUND]] == ["raw/absent"]
    assert [w.relative_path for w in by_code[_META_FAILED]] == ["raw/trades"]
    assert set(by_code) == {_UNINSPECTABLE, _NOT_FOUND, _META_FAILED}
    assert "unknown" in by_code[_UNINSPECTABLE][0].detail
    assert result.streams_scanned == ("orderbook",) and result.unreadable_streams == ()
    assert _paths(result) == ["raw/orderbook/" + SEG]


def _mk_stream_dir(tmp_path):
    _touch(_raw(tmp_path, "trades") / SEG)
    return "trades"


def _mk_non_stream_dir(tmp_path):
    (tmp_path / "raw" / "some-dir").mkdir(parents=True)
    return "some-dir"


def _mk_regular_file(tmp_path):
    _touch(tmp_path / "raw" / "notes.txt")
    return "notes.txt"


def _mk_symlink(tmp_path):
    outside = tmp_path / "outside"
    _touch(outside / SEG, b"outside-secret")
    (tmp_path / "raw").mkdir(exist_ok=True)
    (tmp_path / "raw" / "linked").symlink_to(outside, target_is_directory=True)
    return "linked"


# A real symlink answers ``is_symlink() == True`` so ``is_dir`` is never reached for it: injecting
# an ``is_dir`` fault there cannot fire and the entry is (correctly) a confirmed symlink.
_ROOT_FAILURE_CASES = [
    (build, op)
    for build in (_mk_stream_dir, _mk_non_stream_dir, _mk_regular_file, _mk_symlink)
    for op in _ROOT_OPS
    if not (build is _mk_symlink and op == "is_dir")
]


@pytest.mark.parametrize("build,op", _ROOT_FAILURE_CASES)
def test_root_entry_of_unverified_type_is_never_classified_followed_or_modified(tmp_path, monkeypatch, build, op):
    _touch(_raw(tmp_path, "orderbook") / SEG)
    name = build(tmp_path)
    before = _snapshot(tmp_path)
    calls = _fail_ops(monkeypatch, {name: {op}})
    listed = _log_scandir(monkeypatch)
    _forbid_io(monkeypatch, [os.path.join("raw", name), "outside"])
    result = discover_sources(tmp_path)
    monkeypatch.undo()

    # whatever the entry really is, it is not claimed to be a symlink / directory / file / unrelated
    assert not any(i.relative_path == f"raw/{name}" or i.relative_path.startswith(f"raw/{name}/")
                   for i in result.ignored)
    assert {w.code for w in result.warnings} == {_META_FAILED}, "no SYMLINK_NOT_FOLLOWED etc."
    (warning,) = _meta_warnings(result)
    assert warning.relative_path == f"raw/{name}" and warning.detail.startswith(f"{op}: OSError")
    assert result.streams_scanned == ("orderbook",) and result.unreadable_streams == ()
    assert _paths(result) == ["raw/orderbook/" + SEG]
    assert not any(p.endswith(os.path.join("raw", name)) for p in listed), "traversed an unknown entry"
    assert all(follow is not True for (_n, _op, follow) in calls) and "stat" not in _probes(calls, name)
    assert _snapshot(tmp_path) == before


def test_confirmed_root_entries_keep_their_existing_classification(tmp_path):
    _touch(_raw(tmp_path, "orderbook") / SEG)
    _mk_symlink(tmp_path)                           # confirmed symlink
    _touch(tmp_path / "raw" / "notes.txt")          # confirmed non-directory
    (tmp_path / "raw" / "some-dir").mkdir()         # confirmed directory, not a stream name
    result = discover_sources(tmp_path, streams=["orderbook", "linked", "notes", "absent"])

    ign = _ignored(result)
    assert ign["raw/linked"] is IgnoredCategory.SYMLINK
    assert ign["raw/notes.txt"] is IgnoredCategory.UNRELATED
    assert ign["raw/some-dir"] is IgnoredCategory.UNRECOGNIZED_DIRECTORY
    assert result.streams_scanned == ("orderbook",)
    # a confirmed symlink / absent stream is still definitely "not found"
    not_found = sorted(w.relative_path for w in result.warnings if w.code == _NOT_FOUND)
    assert not_found == ["raw/absent", "raw/linked", "raw/notes"]
    assert [w.relative_path for w in result.warnings if w.code == "SYMLINK_NOT_FOLLOWED"] == ["raw/linked"]
    assert not {w.code for w in result.warnings} & {_META_FAILED, _UNINSPECTABLE}


@pytest.mark.parametrize("op", _ROOT_OPS)
def test_root_probe_failure_results_are_deterministic(tmp_path, monkeypatch, op):
    for stream in ("trades", "orderbook", "bybit_trades"):
        _touch(_raw(tmp_path, stream) / SEG)
    _touch(tmp_path / "raw" / "notes.txt")
    _fail_ops(monkeypatch, {"trades": {op}, "notes.txt": {op}})
    kwargs = {"streams": ["trades", "orderbook", "absent"]}
    first = discover_sources(tmp_path, **kwargs)
    assert discover_sources(tmp_path, **kwargs) == first
    _reverse_listing(monkeypatch)
    assert discover_sources(tmp_path, **kwargs) == first
    unfiltered = discover_sources(tmp_path)
    assert [w.relative_path for w in unfiltered.warnings if w.code == _META_FAILED] == [
        "raw/notes.txt", "raw/trades"]


# ---- 2C-B: marker presence is tri-state ---------------------------------------------------

@pytest.mark.parametrize("op", _META_OPS)
def test_marker_probe_failure_is_unknown_presence_not_confirmed_absence(tmp_path, monkeypatch, op):
    d = _raw(tmp_path, "trades")
    marker = SEG + ".meta.json"
    _touch(d / SEG, b"payload")
    _touch(d / marker, b"marker-bytes")
    before = _snapshot(tmp_path)
    calls = _fail_ops(monkeypatch, {marker: {op}})
    result = discover_sources(tmp_path)
    monkeypatch.undo()          # os.walk in _snapshot must see the real scandir

    rec = _by_name(result)[SEG]
    assert rec.marker_presence is MarkerPresence.UNKNOWN, "unknown evidence must not be labelled absent"
    assert rec.marker_presence not in (MarkerPresence.ABSENT, MarkerPresence.REGULAR_FILE)
    assert rec.trust_state is TrustState.UNKNOWN and rec.trust_state is not TrustState.UNCONFIRMED
    assert "NO_MARKER_FILE" not in rec.reasons
    assert rec.reasons == (_MARKER_UNKNOWN,) and rec.warnings == (_MARKER_UNKNOWN,)
    assert rec.size_bytes == len(b"payload"), "the segment itself was inspected fine"
    assert rec.trust_state not in _PROVEN_STATES
    # the marker entry is not classified as a marker (matched or orphan) and its failure is visible
    assert _ignored(result)["raw/trades/" + marker] is IgnoredCategory.UNRELATED
    assert not any(w.code == "ORPHAN_MARKER" for w in result.warnings)
    (warning,) = _meta_warnings(result)
    assert warning.relative_path == "raw/trades/" + marker and op in warning.detail
    # its content is never read, it is never stat()ed or followed, and nothing is modified
    assert _probes(calls, marker)[-1] == op and "stat" not in _probes(calls, marker)
    assert all(follow is not True for (_n, _op, follow) in calls)
    assert _snapshot(tmp_path) == before


def test_marker_presence_distinguishes_present_absent_and_unknown(tmp_path, monkeypatch):
    d = _raw(tmp_path, "trades")
    present, absent, unknown = ("2026-10-11-10-000000.seg", "2026-10-11-11-000000.seg",
                                "2026-10-11-12-000000.seg")
    for seg in (present, absent, unknown):
        _touch(d / seg)
    _touch(d / (present + ".meta.json"))
    _touch(d / (unknown + ".meta.json"))
    _fail_ops(monkeypatch, {unknown + ".meta.json": {"is_file"}})
    by_name = _by_name(discover_sources(tmp_path))

    assert by_name[present].marker_presence is MarkerPresence.REGULAR_FILE
    assert by_name[absent].marker_presence is MarkerPresence.ABSENT
    assert by_name[unknown].marker_presence is MarkerPresence.UNKNOWN
    assert (by_name[present].trust_state, by_name[present].reasons) == (
        TrustState.UNKNOWN, ("MARKER_PRESENT_CONTENT_NOT_READ",))
    assert (by_name[absent].trust_state, by_name[absent].reasons) == (
        TrustState.UNCONFIRMED, ("NO_MARKER_FILE",))
    assert (by_name[unknown].trust_state, by_name[unknown].reasons) == (
        TrustState.UNKNOWN, (_MARKER_UNKNOWN,))


def test_marker_failure_does_not_touch_other_segments_or_orphan_logic(tmp_path, monkeypatch):
    d = _raw(tmp_path, "trades")
    _touch(d / SEG)
    _touch(d / (SEG + ".meta.json"))
    _touch(d / "2026-10-11-14-000001.seg.meta.json")               # genuine orphan marker
    _fail_ops(monkeypatch, {SEG + ".meta.json": {"is_dir"}})
    result = discover_sources(tmp_path)
    orphans = [w.relative_path for w in result.warnings if w.code == "ORPHAN_MARKER"]
    assert orphans == ["raw/trades/2026-10-11-14-000001.seg.meta.json"]
    assert _ignored(result)["raw/trades/2026-10-11-14-000001.seg.meta.json"] is IgnoredCategory.MARKER_ORPHAN


def test_marker_unknown_and_segment_stat_failure_keep_both_diagnostics(tmp_path, monkeypatch):
    d = _raw(tmp_path, "trades")
    _touch(d / SEG)
    _touch(d / (SEG + ".meta.json"))
    _fail_ops(monkeypatch, {SEG: {"stat"}, SEG + ".meta.json": {"is_file"}})
    rec = _by_name(discover_sources(tmp_path))[SEG]
    assert rec.trust_state is TrustState.UNKNOWN and rec.size_bytes is None
    assert rec.marker_presence is MarkerPresence.UNKNOWN
    assert set(rec.reasons) == {"STAT_FAILED", _MARKER_UNKNOWN}
    assert set(rec.warnings) == {"STAT_FAILED", _MARKER_UNKNOWN}


def test_marker_unknown_does_not_override_collision_or_invalid_name(tmp_path, monkeypatch):
    d = _raw(tmp_path, "trades")
    _touch(d / LEGACY)
    _touch(d / SEG)                                                  # same logical hour as LEGACY
    bad = "2026-13-45-12-000001.seg"
    _touch(d / bad)
    _fail_ops(monkeypatch, {SEG + ".meta.json": {"is_symlink"}, bad + ".meta.json": {"is_dir"}})
    _touch(d / (SEG + ".meta.json"))
    _touch(d / (bad + ".meta.json"))
    by_name = _by_name(discover_sources(tmp_path))

    seg = by_name[SEG]
    assert seg.trust_state is TrustState.AMBIGUOUS and seg.marker_presence is MarkerPresence.UNKNOWN
    assert seg.reasons == ("LEGACY_SEGMENT_COLLISION", _MARKER_UNKNOWN)
    assert by_name[LEGACY].trust_state is TrustState.AMBIGUOUS
    assert by_name[LEGACY].marker_presence is MarkerPresence.NOT_APPLICABLE   # no marker convention
    invalid = by_name[bad]
    assert invalid.trust_state is TrustState.INVALID and invalid.marker_presence is MarkerPresence.UNKNOWN
    assert invalid.reasons == ("INVALID_DATE_OR_HOUR_IN_NAME", _MARKER_UNKNOWN)


def test_unverified_symlink_marker_is_not_followed_read_or_counted(tmp_path, monkeypatch):
    d = _raw(tmp_path, "trades")
    outside = _touch(tmp_path / "outside.bin", b"outside-secret")
    marker = SEG + ".meta.json"
    _touch(d / SEG, b"payload")
    (d / marker).symlink_to(outside)                                 # really a symlink, but unverified
    before = _snapshot(tmp_path)
    calls = _fail_ops(monkeypatch, {marker: {"is_symlink"}})
    _forbid_io(monkeypatch, [marker, "outside.bin"])
    result = discover_sources(tmp_path)
    monkeypatch.undo()

    rec = _by_name(result)[SEG]
    assert rec.marker_presence is MarkerPresence.UNKNOWN and rec.trust_state is TrustState.UNKNOWN
    assert not any(w.code == "SYMLINK_NOT_FOLLOWED" for w in result.warnings), "type must not be claimed"
    assert not any(w.code == "MARKER_NOT_REGULAR" for w in result.warnings), "type must not be claimed"
    assert all(follow is not True for (_n, _op, follow) in calls)
    assert not any(op == "stat" and n == marker for (n, op, _f) in calls)
    assert _snapshot(tmp_path) == before and outside.read_bytes() == b"outside-secret"


def test_marker_unknown_results_are_deterministic(tmp_path, monkeypatch):
    d = _raw(tmp_path, "trades")
    for hour in (10, 11):
        seg = f"2026-10-11-{hour}-000000.seg"
        _touch(d / seg)
        _touch(d / (seg + ".meta.json"))
    _fail_ops(monkeypatch, {"2026-10-11-11-000000.seg.meta.json": {"is_file"}})
    first = discover_sources(tmp_path)
    assert discover_sources(tmp_path) == first
    _reverse_listing(monkeypatch)
    assert discover_sources(tmp_path) == first
    assert [r.marker_presence for r in first.records] == [MarkerPresence.REGULAR_FILE, MarkerPresence.UNKNOWN]


# ---- 2D: marker-path semantics (MarkerPresence) -------------------------------------------------
#
# A marker path can be occupied in five distinguishable ways (see ``MarkerPresence``). Nothing but
# ABSENT may ever read as "the marker is missing", and no state may read as "published". These tests
# build every state side by side, then attack the boundaries: precedence, orphans, filtering, symlink
# following, mutation and determinism. Assertions are on structured fields, never on OS error wording.

_M = MarkerPresence
_HAS_MKFIFO = hasattr(os, "mkfifo")
_NON_REGULAR_EXPECTED = (_M.NON_REGULAR, TrustState.UNKNOWN, ("MARKER_NOT_REGULAR",))
_STATE_EXPECTED = {
    "absent": (_M.ABSENT, TrustState.UNCONFIRMED, ("NO_MARKER_FILE",)),
    "regular": (_M.REGULAR_FILE, TrustState.UNKNOWN, ("MARKER_PRESENT_CONTENT_NOT_READ",)),
    "symlink": _NON_REGULAR_EXPECTED,
    "dangling": _NON_REGULAR_EXPECTED,
    "directory": _NON_REGULAR_EXPECTED,
    "fifo": _NON_REGULAR_EXPECTED,
    "unknown": (_M.UNKNOWN, TrustState.UNKNOWN, (_MARKER_UNKNOWN,)),
    "legacy": (_M.NOT_APPLICABLE, TrustState.LEGACY, ("LEGACY_HOURLY_PARQUET",)),
}


def _seg(hour: int, seq: int = 0) -> str:
    return f"2026-10-11-{hour:02d}-{seq:06d}.seg"


def _marker_world(tmp_path):
    """One stream holding a source file for every way a marker path can be occupied.

    Returns ``{label: file name}`` (a segment for every label but ``legacy``). The ``unknown``
    segment has an ordinary regular marker; the test makes that marker's type probe fail."""
    d = _raw(tmp_path, "trades")
    outside = _touch(tmp_path / "outside.bin", b"outside-secret")
    names = {"absent": _seg(10), "regular": _seg(11), "symlink": _seg(12), "dangling": _seg(13),
             "directory": _seg(14), "unknown": _seg(15), "legacy": "2026-10-11-16.parquet"}
    if _HAS_MKFIFO:
        names["fifo"] = _seg(17)
    for name in names.values():
        _touch(d / name, b"payload")
    _touch(d / (names["regular"] + ".meta.json"), b"\xff not json \x00")
    (d / (names["symlink"] + ".meta.json")).symlink_to(outside)
    (d / (names["dangling"] + ".meta.json")).symlink_to(tmp_path / "does-not-exist")
    (d / (names["directory"] + ".meta.json")).mkdir()
    _touch(d / (names["directory"] + ".meta.json") / "inner.txt", b"inner")
    _touch(d / (names["unknown"] + ".meta.json"), b"{}")
    if _HAS_MKFIFO:
        os.mkfifo(d / (names["fifo"] + ".meta.json"))
    return names


def _forbid_mutation(monkeypatch):
    """Any attempt to create, rename, delete, relink or re-permission something fails the test."""
    def refuse(label):
        def _raise(*a, **k):
            raise AssertionError(f"forbidden filesystem mutation: {label}")
        return _raise

    for fn in ("rename", "replace", "remove", "unlink", "rmdir", "mkdir", "makedirs", "symlink",
               "link", "chmod", "utime", "truncate"):
        monkeypatch.setattr(os, fn, refuse(f"os.{fn}"))
    for fn in ("rmtree", "move", "copy", "copyfile"):
        monkeypatch.setattr(shutil, fn, refuse(f"shutil.{fn}"))


def test_marker_presence_enum_has_exactly_the_documented_states_and_the_ambiguous_bool_is_gone():
    assert {m.name for m in MarkerPresence} == {
        "ABSENT", "REGULAR_FILE", "NON_REGULAR", "UNKNOWN", "NOT_APPLICABLE"}
    assert "marker_presence" in sc.SourceRecord.__dataclass_fields__
    assert "marker_file_present" not in sc.SourceRecord.__dataclass_fields__


# -- 1, 6: every state is distinguishable; only ABSENT means "missing" ------------------------

def test_every_marker_path_state_is_distinct_and_only_absent_means_missing(tmp_path, monkeypatch):
    names = _marker_world(tmp_path)
    _fail_ops(monkeypatch, {names["unknown"] + ".meta.json": {"is_file"}})   # probe error -> UNKNOWN
    result = discover_sources(tmp_path)
    monkeypatch.undo()
    by_name = _by_name(result)

    for label, name in names.items():
        rec = by_name[name]
        assert (rec.marker_presence, rec.trust_state, rec.reasons) == _STATE_EXPECTED[label], label
    assert {r.marker_presence for r in result.records} == set(MarkerPresence), "all five exercised"
    assert len(result.records) == len(names), "no source file is dropped because of its marker path"
    # "confirmed missing" is a conclusion reachable from exactly one state
    missing = [Path(r.relative_path).name for r in result.records if "NO_MARKER_FILE" in r.reasons]
    assert missing == [names["absent"]]
    assert [r.trust_state for r in result.records if r.marker_presence is _M.ABSENT] == [TrustState.UNCONFIRMED]
    assert not {r.trust_state for r in result.records} & {
        TrustState.CONFIRMED, TrustState.CONFIRMED_RETROACTIVE, TrustState.CORRUPT}


def test_marker_entries_are_filed_under_their_true_category_never_as_markers(tmp_path, monkeypatch):
    names = _marker_world(tmp_path)
    _fail_ops(monkeypatch, {names["unknown"] + ".meta.json": {"is_file"}})
    result = discover_sources(tmp_path)
    monkeypatch.undo()
    ign, base = _ignored(result), "raw/trades/"

    assert ign[base + names["regular"] + ".meta.json"] is IgnoredCategory.MARKER_FILE
    assert ign[base + names["symlink"] + ".meta.json"] is IgnoredCategory.SYMLINK
    assert ign[base + names["dangling"] + ".meta.json"] is IgnoredCategory.SYMLINK
    assert ign[base + names["directory"] + ".meta.json"] is IgnoredCategory.UNRECOGNIZED_DIRECTORY
    assert ign[base + names["unknown"] + ".meta.json"] is IgnoredCategory.UNRELATED      # type unverified
    if _HAS_MKFIFO:
        assert ign[base + names["fifo"] + ".meta.json"] is IgnoredCategory.NON_REGULAR
    assert not any(c is IgnoredCategory.MARKER_ORPHAN for c in ign.values())
    assert not any(w.code == "ORPHAN_MARKER" for w in result.warnings)

    flagged = [w for w in result.warnings if w.code == "MARKER_NOT_REGULAR"]
    assert sorted(w.relative_path for w in flagged) == sorted(
        base + names[k] + ".meta.json" for k in ("symlink", "dangling", "directory", "fifo") if k in names)
    # diagnostics are fixed text: no OS exception message and no absolute path can leak through them
    assert all(str(tmp_path) not in w.detail and w.detail for w in flagged)
    # the unverifiable marker is reported as a failed probe, not as a (non-)regular file
    assert [w.relative_path for w in _meta_warnings(result)] == [base + names["unknown"] + ".meta.json"]


# -- 2: a regular marker is not publication evidence ------------------------------------------

@pytest.mark.parametrize("content", [b"", b"{not json", b'{"sha256": "00", "rows": 1}', b"\xff\x00" * 64])
def test_regular_marker_stays_unverified_whatever_it_contains(tmp_path, monkeypatch, content):
    d = _raw(tmp_path, "trades")
    marker = SEG + ".meta.json"
    _touch(d / SEG, b"payload")
    _touch(d / marker, content)
    before = _snapshot(tmp_path)
    _forbid_io(monkeypatch, [marker])                 # opening / stat()ing the marker would fail here
    result = discover_sources(tmp_path)
    monkeypatch.undo()

    rec = _by_name(result)[SEG]
    assert rec.marker_presence is _M.REGULAR_FILE
    assert (rec.trust_state, rec.reasons) == (TrustState.UNKNOWN, ("MARKER_PRESENT_CONTENT_NOT_READ",))
    assert rec.trust_state not in _PROVEN_STATES and rec.warnings == ()
    assert _ignored(result)["raw/trades/" + marker] is IgnoredCategory.MARKER_FILE
    assert _snapshot(tmp_path) == before


# -- 3, 11: a symlink at the marker path is never followed ------------------------------------

_LINK_TARGETS = {
    "file": lambda tmp, link: _touch(tmp / "outside" / "target.bin", b"outside-secret"),
    "directory": lambda tmp, link: _touch(tmp / "outside" / "targetdir" / SEG, b"inside-the-target").parent,
    "dangling": lambda tmp, link: tmp / "outside" / "missing",
    "loop": lambda tmp, link: link,                  # a symlink pointing at itself
}


@pytest.mark.parametrize("kind", sorted(_LINK_TARGETS))
def test_symlink_at_marker_path_is_never_followed_and_is_not_a_missing_marker(tmp_path, monkeypatch, kind):
    d = _raw(tmp_path, "trades")
    marker = SEG + ".meta.json"
    _touch(d / SEG, b"payload")
    target = _LINK_TARGETS[kind](tmp_path, d / marker)
    (d / marker).symlink_to(target)
    before = _snapshot(tmp_path)
    calls = _fail_ops(monkeypatch, {})                # no failures: only records every probe
    listed = _log_scandir(monkeypatch)
    _forbid_io(monkeypatch, [marker, "outside", "target.bin", "targetdir", "missing"])
    result = discover_sources(tmp_path)
    monkeypatch.undo()

    rec = _by_name(result)[SEG]
    assert rec.marker_presence is _M.NON_REGULAR
    assert (rec.trust_state, rec.reasons) == (TrustState.UNKNOWN, ("MARKER_NOT_REGULAR",))
    assert rec.warnings == ("MARKER_NOT_REGULAR",)
    assert rec.trust_state is not TrustState.UNCONFIRMED and "NO_MARKER_FILE" not in rec.reasons
    rel = "raw/trades/" + marker
    assert _ignored(result)[rel] is IgnoredCategory.SYMLINK
    codes = {w.code: w for w in result.warnings if w.relative_path == rel}
    assert set(codes) == {"SYMLINK_NOT_FOLLOWED", "MARKER_NOT_REGULAR"}
    assert "symlink" in codes["MARKER_NOT_REGULAR"].detail
    assert not any(w.code == "ORPHAN_MARKER" for w in result.warnings)
    assert _paths(result) == ["raw/trades/" + SEG], "the link target's contents are not inventory"
    # never followed: the target is not listed, no probe asked to follow, nothing stat()ed
    assert not any("outside" in p for p in listed)
    assert all(follow is not True for (_n, _op, follow) in calls)
    assert not any(op == "stat" for (n, op, _f) in calls if n == marker)
    assert _snapshot(tmp_path) == before
    if kind == "file":
        assert target.read_bytes() == b"outside-secret"


# -- 4: a directory at the marker path ---------------------------------------------------------

@pytest.mark.parametrize("populated", [False, True])
def test_directory_at_marker_path_is_not_a_marker_and_not_a_missing_marker(tmp_path, monkeypatch, populated):
    d = _raw(tmp_path, "trades")
    marker = SEG + ".meta.json"
    _touch(d / SEG, b"payload")
    (d / marker).mkdir()
    if populated:
        _touch(d / marker / "inner.txt", b"inner")
        _touch(d / marker / (SEG + ".meta.json"), b"{}")             # a nested "marker" must not count
        _touch(d / marker / "2026-10-11-13-000000.seg", b"nested")   # nor a nested source file
    before = _snapshot(tmp_path)
    listed = _log_scandir(monkeypatch)
    result = discover_sources(tmp_path)
    monkeypatch.undo()

    rec = _by_name(result)[SEG]
    assert rec.marker_presence is _M.NON_REGULAR
    assert (rec.trust_state, rec.reasons) == (TrustState.UNKNOWN, ("MARKER_NOT_REGULAR",))
    assert rec.trust_state is not TrustState.UNCONFIRMED
    rel = "raw/trades/" + marker
    assert _ignored(result)[rel] is IgnoredCategory.UNRECOGNIZED_DIRECTORY
    (warning,) = [w for w in result.warnings if w.code == "MARKER_NOT_REGULAR"]
    assert warning.relative_path == rel and "directory" in warning.detail
    assert not any(p.endswith(marker) for p in listed), "descended into the directory at the marker path"
    assert _paths(result) == ["raw/trades/" + SEG]
    assert not any("inner.txt" in i.relative_path or "nested" in i.relative_path for i in result.ignored)
    assert _snapshot(tmp_path) == before


# -- 5: other non-regular objects, where the platform supports them ---------------------------

@pytest.mark.skipif(not _HAS_MKFIFO, reason="os.mkfifo is not available on this platform")
def test_fifo_at_marker_path_is_classified_without_ever_being_opened(tmp_path, monkeypatch):
    d = _raw(tmp_path, "trades")
    marker = SEG + ".meta.json"
    _touch(d / SEG, b"payload")
    os.mkfifo(d / marker)
    before = _snapshot(tmp_path)
    _forbid_io(monkeypatch, [marker])      # opening a FIFO can block forever; here any open() raises
    result = discover_sources(tmp_path)
    monkeypatch.undo()

    rec = _by_name(result)[SEG]
    assert rec.marker_presence is _M.NON_REGULAR
    assert (rec.trust_state, rec.reasons) == (TrustState.UNKNOWN, ("MARKER_NOT_REGULAR",))
    rel = "raw/trades/" + marker
    assert _ignored(result)[rel] is IgnoredCategory.NON_REGULAR
    (warning,) = [w for w in result.warnings if w.code == "MARKER_NOT_REGULAR"]
    assert warning.relative_path == rel and "non-regular" in warning.detail
    assert _snapshot(tmp_path) == before


# -- 7: source-file stat() failures keep their uncertainty whatever the marker says ----------

def test_segment_stat_failure_keeps_unknown_uncertainty_for_every_marker_state(tmp_path, monkeypatch):
    names = _marker_world(tmp_path)
    _fail_stat_for(monkeypatch, *names.values())
    result = discover_sources(tmp_path)
    monkeypatch.undo()
    by_name = _by_name(result)
    expected = {"absent": _M.ABSENT, "regular": _M.REGULAR_FILE, "symlink": _M.NON_REGULAR,
                "dangling": _M.NON_REGULAR, "directory": _M.NON_REGULAR, "fifo": _M.NON_REGULAR,
                "unknown": _M.REGULAR_FILE,          # no probe fault injected in this test
                "legacy": _M.NOT_APPLICABLE}

    for label, name in names.items():
        rec = by_name[name]
        extra = ("MARKER_NOT_REGULAR",) if expected[label] is _M.NON_REGULAR else ()
        assert rec.marker_presence is expected[label], label
        assert rec.trust_state is TrustState.UNKNOWN and rec.size_bytes is None, label
        assert rec.reasons == ("STAT_FAILED",) + extra, label
        assert rec.warnings == tuple(sorted(("STAT_FAILED",) + extra)), label
    assert not any(r.trust_state is TrustState.UNCONFIRMED for r in result.records)
    assert by_name[names["legacy"]].legacy_kind == "hourly_parquet"     # identity survives


# -- 8: legacy hourly Parquet records marker non-applicability, in every trust state ----------

def _legacy_plain(d, mp):
    _touch(d / LEGACY)
    return LEGACY, TrustState.LEGACY


def _legacy_with_stray_meta_file(d, mp):
    _touch(d / LEGACY)
    _touch(d / (LEGACY + ".meta.json"))                  # not a marker name the writer produces
    return LEGACY, TrustState.LEGACY


def _legacy_invalid_date(d, mp):
    name = "2026-13-45-12.parquet"
    _touch(d / name)
    return name, TrustState.INVALID


def _legacy_symlink(d, mp):
    (d / LEGACY).symlink_to(d.parent.parent / "outside.bin")
    return LEGACY, TrustState.INVALID


def _legacy_collision(d, mp):
    _touch(d / LEGACY)
    _touch(d / SEG)                                      # same logical hour
    return LEGACY, TrustState.AMBIGUOUS


def _legacy_stat_failed(d, mp):
    _touch(d / LEGACY)
    _fail_stat_for(mp, LEGACY)
    return LEGACY, TrustState.UNKNOWN


def _legacy_probe_failed(d, mp):
    _touch(d / LEGACY)
    _fail_ops(mp, {LEGACY: {"is_file"}})
    return LEGACY, TrustState.UNKNOWN


_LEGACY_BUILDERS = {fn.__name__[len("_legacy_"):]: fn for fn in (
    _legacy_plain, _legacy_with_stray_meta_file, _legacy_invalid_date, _legacy_symlink,
    _legacy_collision, _legacy_stat_failed, _legacy_probe_failed)}


@pytest.mark.parametrize("name", sorted(_LEGACY_BUILDERS))
def test_legacy_hourly_parquet_never_claims_a_missing_marker(tmp_path, monkeypatch, name):
    _touch(tmp_path / "outside.bin")
    d = _raw(tmp_path, "trades")
    legacy_name, state = _LEGACY_BUILDERS[name](d, monkeypatch)
    rec = _by_name(discover_sources(tmp_path))[legacy_name]
    assert rec.marker_presence is _M.NOT_APPLICABLE
    assert rec.trust_state is state
    assert "NO_MARKER_FILE" not in rec.reasons
    assert not any(x.startswith("MARKER_") for x in rec.reasons + rec.warnings)


# -- 9: structural precedence survives a marker anomaly ---------------------------------------

def test_marker_anomalies_never_override_collision_invalid_name_or_symlink_precedence(tmp_path):
    d = _raw(tmp_path, "trades")
    outside = _touch(tmp_path / "outside.bin")
    bad, linked = "2026-13-45-12-000001.seg", "2026-10-11-13-000001.seg"
    _touch(d / LEGACY)
    _touch(d / SEG)                                                  # same logical hour as LEGACY
    (d / (SEG + ".meta.json")).mkdir()
    _touch(d / bad)
    (d / (bad + ".meta.json")).symlink_to(outside)
    (d / linked).symlink_to(outside)
    (d / (linked + ".meta.json")).mkdir()
    result = discover_sources(tmp_path)
    by_name = _by_name(result)

    seg = by_name[SEG]
    assert (seg.trust_state, seg.reasons) == (TrustState.AMBIGUOUS, ("LEGACY_SEGMENT_COLLISION", "MARKER_NOT_REGULAR"))
    assert seg.warnings == ("COLLISION", "MARKER_NOT_REGULAR")
    assert seg.collision_group is not None and seg.collision_group == by_name[LEGACY].collision_group
    assert (by_name[LEGACY].trust_state, by_name[LEGACY].marker_presence) == (
        TrustState.AMBIGUOUS, _M.NOT_APPLICABLE)
    invalid = by_name[bad]
    assert (invalid.trust_state, invalid.reasons) == (
        TrustState.INVALID, ("INVALID_DATE_OR_HOUR_IN_NAME", "MARKER_NOT_REGULAR"))
    symlinked = by_name[linked]
    assert (symlinked.trust_state, symlinked.reasons) == (
        TrustState.INVALID, ("SYMLINK_NOT_FOLLOWED", "MARKER_NOT_REGULAR"))
    assert any(w.code == "COLLISION" for w in result.warnings), "the collision warning is not erased"
    assert not {r.trust_state for r in result.records} & _PROVEN_STATES


# -- 10: orphans, matching, filtering and exact-path matching ---------------------------------

def test_non_regular_orphan_markers_are_classified_as_what_they_are_not_as_orphan_markers(tmp_path):
    d = _raw(tmp_path, "trades")
    outside = _touch(tmp_path / "outside.bin")
    base = "raw/trades/"
    _touch(d / _seg(10))
    _touch(d / (_seg(10) + ".meta.json"))                           # regular marker, segment listed
    _touch(d / (_seg(11) + ".meta.json"))                           # regular marker, no segment
    (d / (_seg(12) + ".meta.json")).symlink_to(outside)             # symlink, no segment
    (d / (_seg(13) + ".meta.json")).mkdir()                         # directory, no segment
    flagged = [_seg(12), _seg(13)]
    if _HAS_MKFIFO:
        os.mkfifo(d / (_seg(14) + ".meta.json"))                    # FIFO, no segment
        flagged.append(_seg(14))
    result = discover_sources(tmp_path)
    ign = _ignored(result)

    assert ign[base + _seg(10) + ".meta.json"] is IgnoredCategory.MARKER_FILE
    assert ign[base + _seg(11) + ".meta.json"] is IgnoredCategory.MARKER_ORPHAN
    assert ign[base + _seg(12) + ".meta.json"] is IgnoredCategory.SYMLINK
    assert ign[base + _seg(13) + ".meta.json"] is IgnoredCategory.UNRECOGNIZED_DIRECTORY
    if _HAS_MKFIFO:
        assert ign[base + _seg(14) + ".meta.json"] is IgnoredCategory.NON_REGULAR
    # only the genuine regular orphan is reported as an orphan marker file
    assert [w.relative_path for w in result.warnings if w.code == "ORPHAN_MARKER"] == [
        base + _seg(11) + ".meta.json"]
    # the others are explained by their own diagnostic even though no segment refers to them
    assert sorted(w.relative_path for w in result.warnings if w.code == "MARKER_NOT_REGULAR") == [
        base + n + ".meta.json" for n in flagged]
    assert _paths(result) == [base + _seg(10)]
    assert result.records[0].marker_presence is _M.REGULAR_FILE


def test_date_hour_filter_applies_to_non_regular_markers_like_any_other_entry(tmp_path):
    d = _raw(tmp_path, "trades")
    for hour in (12, 13):
        _touch(d / _seg(hour))
        (d / (_seg(hour) + ".meta.json")).mkdir()
    only_12 = discover_sources(tmp_path, hour=12)
    assert _paths(only_12) == ["raw/trades/" + _seg(12)]
    assert [w.relative_path for w in only_12.warnings if w.code == "MARKER_NOT_REGULAR"] == [
        "raw/trades/" + _seg(12) + ".meta.json"]
    assert not any(_seg(13) in i.relative_path for i in only_12.ignored)
    both = discover_sources(tmp_path)
    assert [r.marker_presence for r in both.records] == [_M.NON_REGULAR, _M.NON_REGULAR]
    other_day = discover_sources(tmp_path, date="2026-10-12")
    assert other_day.records == () and not any(w.code == "MARKER_NOT_REGULAR" for w in other_day.warnings)


@pytest.mark.parametrize("suffix", [".tmp", ".invalid.0", ".orphan.20261011T000000Z"])
def test_only_the_exact_marker_path_decides_marker_presence(tmp_path, suffix):
    d = _raw(tmp_path, "trades")
    _touch(d / SEG)
    (d / (SEG + ".meta.json" + suffix)).mkdir()                      # a sidecar name, not the marker path
    (d / (_seg(13) + ".meta.json")).mkdir()                          # another segment's marker path
    rec = _by_name(discover_sources(tmp_path))[SEG]
    assert rec.marker_presence is _M.ABSENT
    assert (rec.trust_state, rec.reasons, rec.warnings) == (TrustState.UNCONFIRMED, ("NO_MARKER_FILE",), ())


# -- 11: no following, no opening, no mutation, across every state at once --------------------

def test_discovery_never_follows_opens_or_modifies_anything_at_a_marker_path(tmp_path, monkeypatch):
    names = _marker_world(tmp_path)
    marker_names = [n + ".meta.json" for n in names.values()]
    before = _snapshot(tmp_path)
    calls = _fail_ops(monkeypatch, {names["unknown"] + ".meta.json": {"is_file"}})
    listed = _log_scandir(monkeypatch)
    _forbid_io(monkeypatch, marker_names + ["outside.bin", "does-not-exist", "inner.txt"])
    _forbid_mutation(monkeypatch)
    result = discover_sources(tmp_path)
    monkeypatch.undo()

    assert len(result.records) == len(names)
    assert all(follow is not True for (_n, _op, follow) in calls), "a probe was asked to follow a link"
    assert not any(op == "stat" and n.endswith(".meta.json") for (n, op, _f) in calls)
    root = tmp_path.resolve()
    assert [Path(p).relative_to(root).as_posix() for p in listed] == ["raw", "raw/trades"], \
        "only the raw/ and stream directories are listed: no marker directory, no link target"
    assert (tmp_path / "outside.bin").read_bytes() == b"outside-secret"
    assert _snapshot(tmp_path) == before


# -- 12: repeated scans are equivalent and deterministically ordered --------------------------

def test_repeated_scans_of_an_unchanged_tree_are_equivalent_and_deterministically_ordered(tmp_path, monkeypatch):
    names = _marker_world(tmp_path)
    _fail_ops(monkeypatch, {names["unknown"] + ".meta.json": {"is_file"}})
    first = discover_sources(tmp_path)
    assert discover_sources(tmp_path) == first
    _reverse_listing(monkeypatch)                      # same tree, opposite directory-listing order
    assert discover_sources(tmp_path) == first

    paths = _paths(first)
    assert paths == sorted(paths, key=os.fsencode)
    assert list(first.warnings) == sorted(
        first.warnings, key=lambda w: (w.code, os.fsencode(w.relative_path), w.detail))
    assert list(first.ignored) == sorted(
        first.ignored, key=lambda i: (os.fsencode(i.relative_path), i.category.value))
    assert [r.marker_presence for r in first.records] == [
        _M.ABSENT, _M.REGULAR_FILE, _M.NON_REGULAR, _M.NON_REGULAR, _M.NON_REGULAR, _M.UNKNOWN,
        _M.NOT_APPLICABLE] + ([_M.NON_REGULAR] if _HAS_MKFIFO else [])
