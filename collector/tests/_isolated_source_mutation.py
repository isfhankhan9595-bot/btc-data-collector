"""Run a source-mutation experiment against a PRIVATE copy of the source tree.

Mutation tests prove that a regression test is discriminating: break the
production code on purpose and check the test goes red. The unsafe way to do
that is to rewrite the tracked file in the shared checkout and restore it in
``finally`` -- a second concurrent run (or a killed process) then sees, or
leaves behind, the mutant. That has actually produced an accidental commit.

This helper never writes to the original tree. It copies ``source_root`` into
a fresh ``tempfile.mkdtemp()`` directory, mutates only the copy, runs the
target subprocess with ``PYTHONPATH`` pointing only at the copy, and removes
the copy afterwards -- whether the subprocess passed, failed or timed out.
No lock is involved: isolation is by construction, so any number of runs can
proceed concurrently against the same checkout.
"""
from __future__ import annotations

import dataclasses
import os
import pathlib
import shutil
import signal
import subprocess
import sys
import tempfile
from typing import Callable, Optional, Sequence

_IGNORE = shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache", ".git", ".mypy_cache")


@dataclasses.dataclass(frozen=True)
class ProcessOutcome:
    returncode: Optional[int]      # None when the process had to be killed
    timed_out: bool
    output: str                    # combined stdout+stderr


@dataclasses.dataclass(frozen=True)
class MutationOutcome:
    control: ProcessOutcome        # same command, UNMUTATED copy
    mutated: ProcessOutcome        # same command, MUTATED copy
    import_origin: str             # where the subprocess resolved the module from
    copy_root: pathlib.Path        # (already deleted) -- kept for assertions on path only
    source_changed: bool           # the transform really changed the text


def _run(argv: Sequence[str], *, cwd: pathlib.Path, env: dict, timeout: float) -> ProcessOutcome:
    """Run argv in its own process group so a timeout kills the whole tree."""
    proc = subprocess.Popen(
        list(argv), cwd=str(cwd), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, start_new_session=True,
    )
    try:
        out, _ = proc.communicate(timeout=timeout)
        return ProcessOutcome(proc.returncode, False, out or "")
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        out, _ = proc.communicate()
        return ProcessOutcome(None, True, out or "")


def _private_env(copy_root: pathlib.Path) -> dict:
    env = dict(os.environ)
    # Resolve imports from the private copy ONLY -- never from an inherited
    # PYTHONPATH that may name the original checkout. Mirrors CI's layout
    # (repo root + repo_root/collector): ``copy_root.parent`` makes the
    # ``collector`` package importable, ``copy_root`` mirrors the inner path.
    env["PYTHONPATH"] = os.pathsep.join([str(copy_root.parent), str(copy_root)])
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.pop("PYTEST_ADDOPTS", None)
    return env


def run_mutation_in_isolated_copy(
    *,
    source_root: pathlib.Path,
    mutate_relpath: str,
    transform: Callable[[str], str],
    argv_factory: Callable[[pathlib.Path], Sequence[str]],
    import_probe: Optional[str] = None,
    cwd_relpath: str = ".",
    timeout: float = 120.0,
) -> MutationOutcome:
    """Copy ``source_root``; run ``argv_factory(copy_root)`` on the clean copy
    (control) and again after ``transform`` rewrote ``mutate_relpath`` (mutant).

    ``argv_factory`` receives the copy root; test paths in it should be given
    relative to the copy's parent (the subprocess cwd), e.g.
    ``"collector/tests/test_x.py::test_y"``. ``source_root`` is only ever READ. ``import_probe`` is an optional dotted
    module name; the subprocess reports the file it resolves it from so the
    caller can assert the mutant, not the original, was what ran.
    """
    source_root = pathlib.Path(source_root).resolve()
    tmp_parent = pathlib.Path(tempfile.mkdtemp(prefix="mutation_copy_"))
    try:
        copy_root = tmp_parent / source_root.name
        shutil.copytree(source_root, copy_root, ignore=_IGNORE, symlinks=False)

        target = copy_root / mutate_relpath
        original_target = source_root / mutate_relpath
        # The copy must be a distinct inode: a symlink/hardlink copy would make
        # "mutate the copy" silently mean "mutate the original".
        assert not target.is_symlink(), "copy of the mutation target is a symlink"
        assert os.stat(target).st_ino != os.stat(original_target).st_ino, (
            "copy of the mutation target shares an inode with the original"
        )
        assert tmp_parent not in source_root.parents and source_root not in tmp_parent.parents, (
            "private copy overlaps the original tree"
        )

        env = _private_env(copy_root)
        # Run from the directory CONTAINING the copy (the analogue of the repo
        # root): there ``collector`` is the outer package. Running from inside
        # the copy would let its inner ``collector/`` shadow the outer one.
        cwd = tmp_parent / cwd_relpath

        origin = ""
        if import_probe:
            probe = _run(
                [sys.executable, "-c",
                 f"import importlib; print(importlib.import_module({import_probe!r}).__file__)"],
                cwd=cwd, env=env, timeout=timeout,
            )
            origin = probe.output.strip().splitlines()[-1] if probe.output.strip() else ""

        argv = list(argv_factory(copy_root))
        control = _run(argv, cwd=cwd, env=env, timeout=timeout)

        original_text = target.read_text()
        mutated_text = transform(original_text)
        changed = mutated_text != original_text
        target.write_text(mutated_text)
        mutated = _run(argv, cwd=cwd, env=env, timeout=timeout)

        return MutationOutcome(control, mutated, origin, copy_root, changed)
    finally:
        shutil.rmtree(tmp_parent, ignore_errors=True)
