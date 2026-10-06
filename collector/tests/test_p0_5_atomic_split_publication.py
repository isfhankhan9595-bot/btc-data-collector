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


# ---------------------------------------------------------------------------
# Directory durability (P1 review blocker): every rename/creation the manifest
# depends on must be fsynced in its parent directory BEFORE the manifest names
# it, the manifest replace itself must be fsynced in out_dir, and any failure
# to establish that durability must fail closed (raise, never report success).
#
# Instrumentation wraps the real os.fsync/os.rename/os.replace and resolves
# which directory each fsync hit, so the assertions are about actual syscalls
# on actual directories, not about which helper was called.
# ---------------------------------------------------------------------------
import errno
import stat

needs_proc = pytest.mark.skipif(
    not os.path.isdir("/proc/self/fd"), reason="needs /proc to resolve fsynced directory fds"
)


def _instrument(monkeypatch, fail_dir_fsync_at=None):
    """Record durability-relevant syscalls; optionally fail the Nth directory fsync."""
    events = []
    counter = {"n": 0}
    real_fsync, real_rename, real_replace = os.fsync, os.rename, os.replace

    def fsync(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            counter["n"] += 1
            events.append(("fsync_dir", os.path.realpath(os.readlink(f"/proc/self/fd/{fd}"))))
            if counter["n"] == fail_dir_fsync_at:
                raise OSError(errno.EIO, "injected directory fsync failure")
        return real_fsync(fd)

    def rename(src, dst):
        events.append(("rename", os.path.realpath(dst)))
        return real_rename(src, dst)

    def replace(src, dst):
        events.append(("replace", os.path.realpath(dst)))
        return real_replace(src, dst)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "rename", rename)
    monkeypatch.setattr(os, "replace", replace)
    return events


def _first(events, event, after=-1):
    for i in range(after + 1, len(events)):
        if events[i] == event:
            return i
    raise AssertionError(f"expected {event} after index {after}; events={events}")


def _dir_fsyncs(events):
    return [p for kind, p in events if kind == "fsync_dir"]


@needs_proc
def test_success_fsyncs_every_directory_in_the_publication_chain(prior, monkeypatch):
    base, _before, _prior_manifest = prior
    events = _instrument(monkeypatch)
    generate_splits(str(base), embargo_days=0)
    monkeypatch.undo()

    splits = os.path.realpath(_splits(base))
    gens = os.path.join(splits, "generations")
    final = os.path.join(gens, _manifest(base)["generation"])
    manifest_path = os.path.join(splits, "split_manifest.json")

    i_rename = _first(events, ("rename", final))
    # staged generation dir is fsynced BEFORE it is published by the rename
    assert any(
        os.path.dirname(p) == gens and os.path.basename(p).startswith(".tmp-")
        for p in _dir_fsyncs(events[:i_rename])
    )
    # rename durable in generations/, then generations/ durable in out_dir,
    # both BEFORE the manifest that names the generation is replaced
    i_gens = _first(events, ("fsync_dir", gens), after=i_rename)
    i_out = _first(events, ("fsync_dir", splits), after=i_gens)
    i_commit = _first(events, ("replace", manifest_path), after=i_out)
    # manifest replacement itself is made durable in its parent directory
    _first(events, ("fsync_dir", splits), after=i_commit)
    # exactly: staging, generations, out_dir (generation), out_dir (manifest)
    assert len(_dir_fsyncs(events)) == 4
    assert_manifest_consistent(base)


@needs_proc
def test_manifest_replacement_fsyncs_parent_directory(tmp_path, monkeypatch):
    _write_labeled(tmp_path, 20)
    events = _instrument(monkeypatch)
    generate_splits(str(tmp_path), embargo_days=0, write_parquet=False)
    monkeypatch.undo()

    splits = os.path.realpath(_splits(tmp_path))
    i_commit = _first(events, ("replace", os.path.join(splits, "split_manifest.json")))
    _first(events, ("fsync_dir", splits), after=i_commit)


@needs_proc
def test_atomic_write_bytes_fsyncs_parent_and_propagates_failure(tmp_path, monkeypatch):
    from collector.pipeline.split_generator import _atomic_write_bytes

    target = tmp_path / "x.json"
    events = _instrument(monkeypatch)
    _atomic_write_bytes(str(target), b"{}")
    root = os.path.realpath(tmp_path)
    i = _first(events, ("replace", os.path.join(root, "x.json")))
    _first(events, ("fsync_dir", root), after=i)
    monkeypatch.undo()

    _instrument(monkeypatch, fail_dir_fsync_at=1)
    with pytest.raises(OSError) as exc:
        _atomic_write_bytes(str(target), b"{\"new\": 1}")
    assert exc.value.errno == errno.EIO
    assert [p for p in tmp_path.iterdir() if p.name.startswith(".tmp-")] == []


def test_fsync_dir_does_not_swallow_failures(tmp_path, monkeypatch):
    from collector.pipeline.split_generator import _fsync_dir

    with pytest.raises(OSError):
        _fsync_dir(str(tmp_path / "does-not-exist"))

    def boom(fd):
        raise OSError(errno.EIO, "injected")

    monkeypatch.setattr(os, "fsync", boom)
    with pytest.raises(OSError) as exc:
        _fsync_dir(str(tmp_path))
    assert exc.value.errno == errno.EIO


# Fail EACH directory fsync in the chain in turn: publication must raise the
# real error, must not report success, and must never leave a manifest that
# names an incomplete generation.
_CHAIN = [
    (1, "staging dir", lambda p, s, g: os.path.dirname(p) == g and os.path.basename(p).startswith(".tmp-")),
    (2, "generations dir", lambda p, s, g: p == g),
    (3, "out_dir after generation", lambda p, s, g: p == s),
    (4, "out_dir after manifest", lambda p, s, g: p == s),
]


@needs_proc
@pytest.mark.parametrize("n,where,expect", _CHAIN, ids=[c[1] for c in _CHAIN])
def test_directory_fsync_failure_fails_closed(prior, monkeypatch, capsys, n, where, expect):
    base, before, prior_manifest = prior
    capsys.readouterr()
    events = _instrument(monkeypatch, fail_dir_fsync_at=n)
    with pytest.raises(OSError) as exc:
        generate_splits(str(base), embargo_days=0)
    monkeypatch.undo()

    # the REAL error surfaces (not swallowed, not converted) from the intended dir
    assert exc.value.errno == errno.EIO
    splits = os.path.realpath(_splits(base))
    assert expect(events[-1][1], splits, os.path.join(splits, "generations")), events[-1]
    # no success / committed state is reported
    assert "Saved split_manifest.json" not in capsys.readouterr().out

    if n < 4:
        # failed before the commit: old manifest + old generation untouched
        _assert_prior_state_untouched(base, before, prior_manifest)
    else:
        # manifest replace already happened but its durability is unproven:
        # the call still raised (caller must not treat it as committed), and
        # what is on disk is at least self-consistent.
        assert_manifest_consistent(base)
        assert [p for p in _splits(base).iterdir() if p.name.startswith(".tmp-")] == []


@needs_proc
def test_retry_after_post_rename_fsync_failure_reverifies_and_fsyncs_chain(prior, monkeypatch):
    base, before, prior_manifest = prior
    _instrument(monkeypatch, fail_dir_fsync_at=2)   # generations/ fsync fails after rename
    with pytest.raises(OSError):
        generate_splits(str(base), embargo_days=0)
    monkeypatch.undo()
    _assert_prior_state_untouched(base, before, prior_manifest)

    events = _instrument(monkeypatch)
    generate_splits(str(base), embargo_days=0)      # generation already present on disk
    monkeypatch.undo()

    splits = os.path.realpath(_splits(base))
    gens = os.path.join(splits, "generations")
    manifest = assert_manifest_consistent(base)
    assert manifest["generation"] != prior_manifest["generation"]
    # the reuse path did NOT skip durability: chain fsynced before the commit
    i_commit = _first(events, ("replace", os.path.join(splits, "split_manifest.json")))
    assert ("fsync_dir", gens) in events[:i_commit]
    assert ("fsync_dir", splits) in events[:i_commit]


# Restart/recovery: an incompletely published generation is never trusted.
def test_stale_staging_dir_from_a_crash_is_never_advertised(prior):
    base, _before, _prior_manifest = prior
    stale = _splits(base) / "generations" / ".tmp-crashed"
    stale.mkdir()
    (stale / "train.parquet").write_bytes(b"partial")
    generate_splits(str(base), embargo_days=0)
    manifest = assert_manifest_consistent(base)
    assert all(".tmp-" not in info["path"] for info in manifest["artifacts"].values())


def test_torn_uncommitted_generation_is_refused_on_restart(prior, monkeypatch):
    base, before, prior_manifest = prior
    real_replace = os.replace

    def crash_at_commit(src, dst):
        if os.path.basename(dst) == "split_manifest.json":
            raise OSError("injected crash at manifest commit")
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", crash_at_commit)
    with pytest.raises(OSError):
        generate_splits(str(base), embargo_days=0)
    monkeypatch.undo()

    # the uncommitted generation is torn (power loss before its entries were durable)
    gens = _splits(base) / "generations"
    (new_gen,) = set(os.listdir(gens)) - {prior_manifest["generation"]}
    os.remove(gens / new_gen / "val.parquet")

    with pytest.raises(RuntimeError):
        generate_splits(str(base), embargo_days=0)
    # old manifest still the only commit record, byte-for-byte
    assert _snapshot(base)["split_manifest.json"] == before["split_manifest.json"]
    assert assert_manifest_consistent(base) == prior_manifest
