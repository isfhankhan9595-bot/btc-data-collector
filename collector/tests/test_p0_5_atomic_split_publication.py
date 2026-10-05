"""P0-5: split artifact publication is transactional.

Invariant under test: whatever the state of ``<data>/splits`` after ANY
failure, ``split_manifest.json`` (if present) describes a complete generation
whose Parquet files all exist and match the sizes/hashes the manifest records.
A failed regeneration must leave the previous manifest and its generation
byte-for-byte intact.

Failure injection wraps the real publication boundary (Parquet serialisation,
directory publish, manifest commit); every assertion reads the actual
filesystem.
"""
from __future__ import annotations

import hashlib
import json
import os

import pandas as pd
import pytest

from collector.pipeline import split_generator
from collector.pipeline.split_generator import LeakageError, generate_splits


def _write_labeled(base_dir, count):
    labeled = base_dir / "aligned" / "labeled"
    labeled.mkdir(parents=True, exist_ok=True)
    for day in range(1, count + 1):
        d = f"2026-01-{day:02d}"
        pd.DataFrame({"date": [d], "return_60s": [0.0]}).to_parquet(labeled / f"{d}.parquet")


def _splits(base_dir):
    return base_dir / "splits"


def _manifest(base_dir):
    return json.loads((_splits(base_dir) / "split_manifest.json").read_text())


def assert_manifest_consistent(base_dir):
    """The core invariant: manifest <-> artifacts on disk agree."""
    manifest = _manifest(base_dir)
    assert manifest["artifacts"], "manifest must name the artifacts it describes"
    for name, info in manifest["artifacts"].items():
        path = _splits(base_dir) / info["path"]
        assert path.is_file(), f"manifest names {info['path']} but it is not on disk"
        raw = path.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == info["sha256"]
        assert len(raw) == info["bytes"]
        frame = pd.read_parquet(path)
        assert len(frame) == info["rows"]
        # row count in the Parquet matches the number of days the manifest lists
        assert len(frame) == len(manifest[name])
    return manifest


def _snapshot(base_dir):
    """Every file under splits/ -> bytes, for exact before/after comparison."""
    out = {}
    for root, _dirs, files in os.walk(_splits(base_dir)):
        for f in files:
            p = os.path.join(root, f)
            out[os.path.relpath(p, _splits(base_dir))] = open(p, "rb").read()
    return out


@pytest.fixture
def prior(tmp_path):
    """A successfully published generation built from 20 days, followed by new
    data (30 days) so a regeneration would produce a different generation."""
    _write_labeled(tmp_path, 20)
    generate_splits(str(tmp_path), embargo_days=0)
    before = _snapshot(tmp_path)
    manifest = assert_manifest_consistent(tmp_path)
    _write_labeled(tmp_path, 30)
    return tmp_path, before, manifest


def _fail_nth_to_parquet(monkeypatch, n):
    real = pd.DataFrame.to_parquet
    calls = {"n": 0}

    def flaky(self, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == n:
            raise OSError(f"injected failure serialising artifact #{n}")
        return real(self, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", flaky)


def _assert_prior_state_untouched(base_dir, before, prior_manifest):
    after = _snapshot(base_dir)
    # Nothing that existed changed...
    for rel, data in before.items():
        assert after.get(rel) == data, f"{rel} changed or vanished after a failed run"
    # ...and the manifest still describes the OLD, complete generation.
    manifest = assert_manifest_consistent(base_dir)
    assert manifest == prior_manifest
    # No half-published temp directories left behind.
    leftovers = [r for r in after if os.path.basename(os.path.dirname(r)).startswith(".tmp-") or
                 os.path.basename(r).startswith(".tmp-")]
    assert leftovers == []


# 1. failure before any new artifact is published
def test_failure_before_any_artifact_published(prior, monkeypatch):
    base, before, prior_manifest = prior
    _fail_nth_to_parquet(monkeypatch, 1)
    with pytest.raises(OSError):
        generate_splits(str(base), embargo_days=0)
    _assert_prior_state_untouched(base, before, prior_manifest)


# 2. failure after one artifact has been serialised (the reported scenario:
#    train done, val fails)
def test_failure_after_first_artifact(prior, monkeypatch):
    base, before, prior_manifest = prior
    _fail_nth_to_parquet(monkeypatch, 2)
    with pytest.raises(OSError):
        generate_splits(str(base), embargo_days=0)
    _assert_prior_state_untouched(base, before, prior_manifest)


# 3. failure during a later artifact publication (writing the 3rd file into
#    the staged generation)
def test_failure_during_later_artifact_write(prior, monkeypatch):
    base, before, prior_manifest = prior
    real_fsync = os.fsync
    calls = {"n": 0}

    def flaky_fsync(fd):
        calls["n"] += 1
        if calls["n"] == 3:
            raise OSError("injected disk failure writing third artifact")
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", flaky_fsync)
    with pytest.raises(OSError):
        generate_splits(str(base), embargo_days=0)
    monkeypatch.undo()
    _assert_prior_state_untouched(base, before, prior_manifest)


# 3b. failure in the directory publish itself
def test_failure_publishing_generation_directory(prior, monkeypatch):
    base, before, prior_manifest = prior

    def boom(src, dst):
        raise OSError("injected rename failure")

    monkeypatch.setattr(os, "rename", boom)
    with pytest.raises(OSError):
        generate_splits(str(base), embargo_days=0)
    monkeypatch.undo()
    _assert_prior_state_untouched(base, before, prior_manifest)


# 4/5. crash AFTER all artifacts are published but BEFORE the manifest commit:
#      the new generation exists on disk but must not be advertised.
def test_failure_at_manifest_commit_leaves_old_manifest_and_consistent_set(prior, monkeypatch):
    base, before, prior_manifest = prior
    real_replace = os.replace

    def flaky_replace(src, dst):
        if os.path.basename(dst) == "split_manifest.json":
            raise OSError("injected crash at manifest commit")
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", flaky_replace)
    with pytest.raises(OSError):
        generate_splits(str(base), embargo_days=0)
    monkeypatch.undo()

    after = _snapshot(base)
    # Old manifest byte-for-byte, old generation intact.
    assert after["split_manifest.json"] == before["split_manifest.json"]
    for rel, data in before.items():
        assert after[rel] == data
    manifest = assert_manifest_consistent(base)
    assert manifest == prior_manifest
    # The new generation is on disk but unreferenced: harmless, never advertised.
    extra = set(os.listdir(_splits(base) / "generations")) - {manifest["generation"]}
    assert len(extra) == 1  # the complete-but-uncommitted generation


# 5b. first-ever run failing leaves NO manifest (nothing can falsely advertise)
def test_first_run_failure_publishes_no_manifest(tmp_path, monkeypatch):
    _write_labeled(tmp_path, 20)
    _fail_nth_to_parquet(monkeypatch, 2)
    with pytest.raises(OSError):
        generate_splits(str(tmp_path), embargo_days=0)
    assert not (_splits(tmp_path) / "split_manifest.json").exists()


# 6. successful generation is fully readable; a regeneration atomically moves
#    the manifest to a new, complete generation and the old one stays valid.
def test_successful_generation_is_readable_and_supersedes_prior(prior):
    base, before, prior_manifest = prior
    generate_splits(str(base), embargo_days=0)
    manifest = assert_manifest_consistent(base)
    assert manifest["generation"] != prior_manifest["generation"]
    assert sum(len(manifest[s]) for s in ("train", "val", "test")) > \
        sum(len(prior_manifest[s]) for s in ("train", "val", "test"))
    # prior generation still intact on disk
    for rel, data in before.items():
        if rel.startswith("generations/"):
            assert _snapshot(base)[rel] == data


def test_regeneration_of_identical_data_is_deterministic_and_idempotent(tmp_path):
    _write_labeled(tmp_path, 20)
    generate_splits(str(tmp_path), embargo_days=0)
    first = _snapshot(tmp_path)
    generate_splits(str(tmp_path), embargo_days=0)
    assert _snapshot(tmp_path) == first


def test_altered_existing_generation_is_refused_not_trusted(tmp_path):
    _write_labeled(tmp_path, 20)
    generate_splits(str(tmp_path), embargo_days=0)
    manifest = _manifest(tmp_path)
    (_splits(tmp_path) / manifest["artifacts"]["train"]["path"]).write_bytes(b"corrupt")
    with pytest.raises(RuntimeError):
        generate_splits(str(tmp_path), embargo_days=0)


# 7. leakage verification unchanged: a failing verification publishes nothing
def test_leakage_failure_publishes_nothing(prior, monkeypatch):
    base, before, prior_manifest = prior
    monkeypatch.setattr(split_generator, "verify_manifest", lambda m: ["injected leakage"])
    with pytest.raises(LeakageError):
        generate_splits(str(base), embargo_days=0)
    _assert_prior_state_untouched(base, before, prior_manifest)


def test_write_parquet_false_manifest_advertises_no_artifacts(prior):
    base, _before, _prior_manifest = prior
    generate_splits(str(base), embargo_days=0, write_parquet=False)
    manifest = _manifest(base)
    assert manifest["artifacts"] == {} and manifest["generation"] is None
