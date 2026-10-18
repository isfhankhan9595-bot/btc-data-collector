"""The mutation harness must never touch the tree it copies.

Background: ``test_mutation_restamping_during_processing_is_caught`` used to
rewrite the tracked ``websocket_client.py`` in the shared checkout and restore
it in ``finally``. Two concurrent full-suite runs saw each other's mutant (and
one run committed it). These tests pin the replacement's properties on a tiny
fake package so they are fast and cannot themselves endanger the real source:

* original bytes identical after a mutant that FAILS, SURVIVES, TIMES OUT, or
  whose transform RAISES;
* the temporary copy is removed in every case;
* concurrent runs against ONE source root do not see each other's mutant, and a
  watcher polling the original never observes a deviation (no lock is used);
* a copy that aliases the original (hardlink) is refused before any write.
"""
from __future__ import annotations

import hashlib
import os
import pathlib
import shutil
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from . import _isolated_source_mutation as harness
from ._isolated_source_mutation import run_mutation_in_isolated_copy

_PRINT_VALUE = [sys.executable, "-c", "from pkg import mod; print(mod.VALUE)"]


@pytest.fixture
def fake_source(tmp_path, monkeypatch):
    """A one-module package + a private tempdir so leaks are observable."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(scratch))
    root = tmp_path / "src" / "pkg"
    root.mkdir(parents=True)
    (root / "__init__.py").write_text("")
    (root / "mod.py").write_text("VALUE = 1\n")
    (root / "test_mod.py").write_text("from pkg import mod\n\ndef test_value():\n    assert mod.VALUE == 1\n")
    return root


def _digest(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _pytest_argv(_root):
    return [sys.executable, "-m", "pytest", "-q", "--no-header", "-p", "no:cacheprovider", "pkg/test_mod.py"]


def _run(root, transform, *, argv=_pytest_argv, timeout=60.0):
    return run_mutation_in_isolated_copy(
        source_root=root, mutate_relpath="mod.py", transform=transform,
        argv_factory=argv, import_probe="pkg.mod", timeout=timeout,
    )


def _leftovers(scratch: pathlib.Path):
    return sorted(p.name for p in scratch.iterdir())


def test_failing_mutant_leaves_original_byte_identical_and_copy_removed(fake_source):
    mod, before = fake_source / "mod.py", _digest(fake_source / "mod.py")
    out = _run(fake_source, lambda s: s.replace("VALUE = 1", "VALUE = 2"))
    assert out.source_changed
    assert out.control.returncode == 0                       # clean copy passes
    assert out.mutated.returncode not in (0, None)           # mutant is caught
    assert "mutation_copy_" in out.import_origin and str(mod) not in out.import_origin
    assert _digest(mod) == before and mod.read_text() == "VALUE = 1\n"
    assert _leftovers(pathlib.Path(tempfile.tempdir)) == []


def test_surviving_mutant_is_reported_not_hidden_and_original_unchanged(fake_source):
    mod, before = fake_source / "mod.py", _digest(fake_source / "mod.py")
    out = _run(fake_source, lambda s: s + "# harmless\n")
    assert out.source_changed and out.mutated.returncode == 0   # the caller can see it survived
    assert _digest(mod) == before
    assert _leftovers(pathlib.Path(tempfile.tempdir)) == []


def test_noop_transform_is_flagged_as_unchanged(fake_source):
    out = _run(fake_source, lambda s: s)
    assert out.source_changed is False                          # callers assert on this


def test_timed_out_mutant_is_killed_and_original_unchanged(fake_source):
    mod, before = fake_source / "mod.py", _digest(fake_source / "mod.py")
    started = time.monotonic()
    out = _run(fake_source, lambda s: "import time\ntime.sleep(60)\n" + s, timeout=4.0)
    assert out.mutated.timed_out and out.mutated.returncode is None
    assert time.monotonic() - started < 40, "the hung mutant was not killed at the timeout"
    assert _digest(mod) == before
    assert _leftovers(pathlib.Path(tempfile.tempdir)) == []


def test_raising_transform_propagates_and_leaves_no_trace(fake_source):
    mod, before = fake_source / "mod.py", _digest(fake_source / "mod.py")

    def boom(_text):
        raise RuntimeError("transform exploded")

    with pytest.raises(RuntimeError, match="transform exploded"):
        _run(fake_source, boom)
    assert _digest(mod) == before
    assert _leftovers(pathlib.Path(tempfile.tempdir)) == []


def test_concurrent_mutations_are_isolated_without_any_lock(fake_source):
    """Eight overlapping runs against ONE source root, each with a different
    mutant (and one that hangs). Each must observe only its own mutant, and a
    watcher polling the original every ~1 ms must never see it change."""
    mod = fake_source / "mod.py"
    before = _digest(mod)
    deviations, stop = [], threading.Event()

    def watch():
        while not stop.is_set():
            if _digest(mod) != before:
                deviations.append(time.monotonic())
            time.sleep(0.001)

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    gate = threading.Barrier(8)

    def worker(i):
        gate.wait()
        if i == 7:   # one run hangs while the others finish
            return _run(fake_source, lambda s: "import time\ntime.sleep(60)\n" + s,
                        argv=lambda r: _PRINT_VALUE, timeout=4.0)
        return _run(fake_source, lambda s, i=i: s.replace("VALUE = 1", f"VALUE = {100 + i}"),
                    argv=lambda r: _PRINT_VALUE)

    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            outs = list(pool.map(worker, range(8)))
    finally:
        stop.set()
        watcher.join(timeout=5)

    assert deviations == [], "the original source file was observed mutated"
    for i, out in enumerate(outs[:7]):
        assert out.control.output.strip() == "1"                      # never someone else's mutant
        assert out.mutated.output.strip() == str(100 + i)             # exactly its own
    assert outs[7].mutated.timed_out
    assert len({o.copy_root for o in outs}) == 8                      # distinct private copies
    assert _digest(mod) == before
    assert _leftovers(pathlib.Path(tempfile.tempdir)) == []


def test_aliasing_copy_is_refused_before_any_write(fake_source, monkeypatch):
    """If a future change made the 'copy' share inodes with the original,
    writing the mutant would edit the original. The helper must refuse."""
    mod, before = fake_source / "mod.py", _digest(fake_source / "mod.py")
    real_copytree = shutil.copytree

    def hardlinking_copytree(src, dst, **kw):
        return real_copytree(src, dst, copy_function=os.link, **{k: v for k, v in kw.items() if k != "copy_function"})

    monkeypatch.setattr(harness.shutil, "copytree", hardlinking_copytree)
    with pytest.raises(AssertionError, match="shares an inode"):
        _run(fake_source, lambda s: s.replace("VALUE = 1", "VALUE = 2"))
    assert _digest(mod) == before and mod.read_text() == "VALUE = 1\n"
    assert _leftovers(pathlib.Path(tempfile.tempdir)) == []
