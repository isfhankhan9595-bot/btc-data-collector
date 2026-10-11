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
