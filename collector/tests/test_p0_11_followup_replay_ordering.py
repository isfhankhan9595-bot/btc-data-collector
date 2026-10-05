"""P0-11 follow-up: replay mixed-precision ordering correction.

Defect: ReplayFrame.order_key used (effective_ns(timestamp_ms, receive_ns),
kind_rank, source_index). effective_ns synthesizes a missing receive_ns as
the start of its millisecond (timestamp_ms * 1_000_000), which can sort
BELOW a WIRE frame's real, later-in-the-millisecond receive_ns -- pulling a
REST snapshot ahead of a websocket frame the collector actually received
first, purely because the snapshot has no finer evidence. That changes what
an order-book recovery sees as available first: a causality-relevant
reordering, not cosmetic.

Fix: (timestamp_ms, kind_rank, ns_tiebreak, source_index) -- kind_rank
compared before any ns evidence, so a REST snapshot can never be pulled
ahead of a same-millisecond WIRE frame by its own missing precision. The ns
tiebreak then only refines order within one (ms, kind) group.
"""
from __future__ import annotations

import importlib
import pathlib
import shutil
import sys
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor

from collector.collector.replay import FrameKind, ReplayFrame, ReplaySource

T = 1_780_000_000_000

#: The real package directory containing replay.py and every module it
#: resolves relative imports against (clock, canonical, sequence,
#: instrument, numeric, quality_events, storage_layout, utils, adapters/).
#: Mutation tests copy this whole tree rather than editing it in place.
_REAL_PACKAGE_ROOT = pathlib.Path(__file__).resolve().parents[1] / "collector"


def _load_mutated_replay(mutate_fn):
    """Load a mutated ``replay`` module from an isolated, uniquely-named copy
    of the real package -- never the shared checkout.

    Root cause this replaces: the original mutation tests wrote directly to
    ``collector/collector/replay.py`` on disk and reloaded the real
    ``collector.collector.replay`` module name. Under parallel execution two
    such tests (or a mutation test racing an unrelated test that imports
    ``collector.collector.replay``) can interleave: one test's write lands
    while another test's import or assertion is in flight, so a test can
    observe the wrong mutation, a half-written file, or a transient
    ``ImportError``. The shared on-disk file and the shared module name in
    ``sys.modules`` are both single points of contention.

    This copies the whole package tree (relative imports -- ``.clock``,
    ``..canonical``, etc. -- require the full tree, not just replay.py) into
    a fresh ``tempfile.mkdtemp()`` directory under a name containing a
    per-call ``uuid4``, mutates only that copy, and imports it as a
    top-level package whose name cannot collide with ``collector`` (the real
    package already in ``sys.modules``) or with any other concurrently
    running instance of this same test. Two workers -- threads or
    processes -- each get their own directory and their own package name,
    so neither can observe, overwrite, or import the other's mutation.

    Returns ``(mutated_replay_module, cleanup)``; the caller MUST call
    ``cleanup()`` (a ``finally`` block) so the mutant's modules are dropped
    from ``sys.modules``, its path entry is removed, and its temp directory
    is deleted -- nothing is left behind that a later import in the same
    process could pick up by accident.
    """
    unique_name = f"p011_mutant_{uuid.uuid4().hex}"
    tmp_root = pathlib.Path(tempfile.mkdtemp(prefix="p011_mutant_"))
    dest = tmp_root / unique_name
    shutil.copytree(_REAL_PACKAGE_ROOT, dest, ignore=shutil.ignore_patterns("__pycache__"))

    replay_path = dest / "replay.py"
    original_src = replay_path.read_text()
    mutated_src = mutate_fn(original_src)
    assert mutated_src != original_src, "mutation target not found -- test is stale"
    replay_path.write_text(mutated_src)

    sys.path.insert(0, str(tmp_root))
    try:
        module = importlib.import_module(f"{unique_name}.replay")
    except BaseException:
        sys.path.remove(str(tmp_root))
        shutil.rmtree(tmp_root, ignore_errors=True)
        raise

    def cleanup():
        # Never leak the mutated modules through the import cache: drop the
        # mutant package and every submodule it pulled in (replay imports
        # adapters/*, clock, canonical, ... all under the same unique
        # top-level name, so this prefix match catches all of them).
        for name in [n for n in list(sys.modules) if n.split(".")[0] == unique_name]:
            del sys.modules[name]
        try:
            sys.path.remove(str(tmp_root))
        except ValueError:
            pass
        shutil.rmtree(tmp_root, ignore_errors=True)

    return module, cleanup


def _real_replay_path():
    return _REAL_PACKAGE_ROOT / "replay.py"


def _wire(idx, ns=None, ms=T):
    return ReplayFrame(timestamp_ms=ms, kind=FrameKind.WIRE, source_index=idx,
                       payload=f'{{"stream":"x","data":{{"u":{idx}}}}}', receive_ns=ns)


def _rest(idx, ms=T, ns=None):
    return ReplayFrame(timestamp_ms=ms, kind=FrameKind.REST_SNAPSHOT, source_index=idx,
                       payload='{"lastUpdateId":1,"bids":[],"asks":[]}', receive_ns=ns)


# ---------------------------------------------------------------------------
# The defect, pinned directly
# ---------------------------------------------------------------------------


def test_rest_snapshot_never_sorts_before_a_same_millisecond_wire_frame():
    wire = _wire(0, ns=T * 1_000_000 + 700_123)
    rest = _rest(1)
    frames = ReplaySource([rest, wire]).frames  # fed in the "wrong" order too
    assert [f.kind for f in frames] == [FrameKind.WIRE, FrameKind.REST_SNAPSHOT]


def test_rest_snapshot_after_several_wire_frames_in_same_millisecond():
    wires = [_wire(i, ns=T * 1_000_000 + i * 1000) for i in range(5)]
    rest = _rest(99)
    frames = ReplaySource(wires + [rest]).frames
    assert [f.kind for f in frames] == [FrameKind.WIRE] * 5 + [FrameKind.REST_SNAPSHOT]
    assert [f.source_index for f in frames[:5]] == [0, 1, 2, 3, 4]


# ---------------------------------------------------------------------------
# WIRE-among-WIRE nanosecond ordering is retained
# ---------------------------------------------------------------------------


def test_multiple_wire_frames_with_distinct_ns_order_by_ns_within_the_millisecond():
    late = _wire(0, ns=T * 1_000_000 + 800_000)
    early = _wire(1, ns=T * 1_000_000 + 100_000)
    frames = ReplaySource([late, early]).frames
    assert [f.source_index for f in frames] == [1, 0]  # early (idx 1) first


def test_different_milliseconds_remain_correctly_ordered_regardless_of_ns():
    later_ms_but_early_ns = _wire(0, ms=T + 1, ns=(T + 1) * 1_000_000 + 1)
    earlier_ms_no_ns = _wire(1, ms=T, ns=None)
    frames = ReplaySource([later_ms_but_early_ns, earlier_ms_no_ns]).frames
    assert [f.source_index for f in frames] == [1, 0]


# ---------------------------------------------------------------------------
# Legacy / missing evidence: deterministic, never fabricated
# ---------------------------------------------------------------------------


def test_legacy_wire_rows_without_receive_ns_remain_deterministic():
    a = _wire(0, ns=None)
    b = _wire(1, ns=None)
    first = [f.source_index for f in ReplaySource([a, b]).frames]
    second = [f.source_index for f in ReplaySource([b, a]).frames]
    assert first == second == [0, 1]  # source_index settles it, every time


def test_legacy_wire_row_and_ns_bearing_wire_row_same_ms_do_not_crash_and_are_deterministic():
    """No TypeError from mixing a None-evidence sentinel with real ns values,
    and the result does not vary by input order."""
    legacy = _wire(0, ns=None)
    timed = _wire(1, ns=T * 1_000_000 + 5)
    a = [f.source_index for f in ReplaySource([legacy, timed]).frames]
    b = [f.source_index for f in ReplaySource([timed, legacy]).frames]
    assert a == b


# ---------------------------------------------------------------------------
# Ties and no fabricated precision
# ---------------------------------------------------------------------------


def test_equal_receive_ns_values_remain_tied_and_settled_by_source_index():
    a = _wire(0, ns=T * 1_000_000 + 500)
    b = _wire(1, ns=T * 1_000_000 + 500)
    frames = ReplaySource([b, a]).frames
    assert frames[0].order_key[:3] == frames[1].order_key[:3]  # same ms, kind, ns
    assert [f.source_index for f in frames] == [0, 1]  # recorded order breaks the exact tie


def test_no_synthetic_offset_is_added_to_distinguish_equal_timestamps():
    """A REST snapshot and a WIRE frame with IDENTICAL millisecond and no ns
    evidence on either side must not have a fabricated nanosecond inserted
    to force an ordering beyond kind_rank + source_index."""
    wire = _wire(0, ns=None)
    rest = _rest(1, ns=None)
    frames = ReplaySource([rest, wire]).frames
    assert frames[0].kind == FrameKind.WIRE  # kind_rank alone decides it
    assert frames[0].receive_ns is None and frames[1].receive_ns is None


def test_rest_oi_frame_follows_the_same_kind_rank_rule_as_rest_snapshot():
    """REST_OI shares REST_SNAPSHOT's kind_rank; the fix must not special-case one."""
    wire = _wire(0, ns=T * 1_000_000 + 999_000)
    oi = ReplayFrame(timestamp_ms=T, kind=FrameKind.REST_OI, source_index=1,
                     payload='{"openInterest":"1"}')
    frames = ReplaySource([oi, wire]).frames
    assert frames[0].kind == FrameKind.WIRE


# ---------------------------------------------------------------------------
# No monotonic-clock / no wall-clock use for ordering
# ---------------------------------------------------------------------------


def test_order_key_never_reads_a_clock():
    import inspect

    source = inspect.getsource(ReplayFrame.order_key.fget)
    for forbidden in ("time.time", "time.monotonic", "datetime.now", "utcnow"):
        assert forbidden not in source


def test_ordering_is_reproducible_across_repeated_construction():
    frames_in = [_wire(0, ns=T * 1_000_000 + 2), _rest(1), _wire(2, ns=T * 1_000_000 + 1)]
    first = [f.source_index for f in ReplaySource(list(frames_in)).frames]
    second = [f.source_index for f in ReplaySource(list(frames_in)).frames]
    assert first == second


# ---------------------------------------------------------------------------
# Mutation testing: real source, byte-verified restoration
# ---------------------------------------------------------------------------


def _mutate_kind_rank_after_ns(src: str) -> str:
    """Revert order_key to the pre-fix (ns-first) shape."""
    old = ("        ns_tiebreak = self.receive_ns if self.receive_ns is not None else -1\n"
           "        return (self.timestamp_ms, _KIND_RANK.get(self.kind, 9), ns_tiebreak, self.source_index)")
    new = ("        from .clock import effective_ns as _eff\n"
           "        return (_eff(self.timestamp_ms, self.receive_ns), "
           "_KIND_RANK.get(self.kind, 9), self.source_index)")
    assert old in src, "mutation target not found -- test is stale"
    return src.replace(old, new)


def _assert_kind_rank_after_ns_mutation_reproduces_the_defect(mutated_module) -> None:
    wire = mutated_module.ReplayFrame(timestamp_ms=T, kind=mutated_module.FrameKind.WIRE,
                                      source_index=0, payload="{}",
                                      receive_ns=T * 1_000_000 + 700_123)
    rest = mutated_module.ReplayFrame(timestamp_ms=T, kind=mutated_module.FrameKind.REST_SNAPSHOT,
                                      source_index=1, payload="{}")
    frames = mutated_module.ReplaySource([rest, wire]).frames
    assert frames[0].kind == mutated_module.FrameKind.REST_SNAPSHOT, (
        "mutation should have reproduced the original defect; if this "
        "assert fails, the mutation did not take effect"
    )


def test_mutation_reordering_kind_rank_after_ns_reintroduces_the_defect():
    """Mutate order_key back to the old (ns-first) shape, on an isolated
    copy; this exact regression test must fail under that mutation.

    Operates on a uniquely-named temp-directory copy of the package (see
    _load_mutated_replay), never on the shared checkout, so this cannot
    race any concurrently running test -- including another instance of
    itself -- for the same file or module name."""
    before = _real_replay_path().read_bytes()
    mutated, cleanup = _load_mutated_replay(_mutate_kind_rank_after_ns)
    try:
        _assert_kind_rank_after_ns_mutation_reproduces_the_defect(mutated)
    finally:
        cleanup()
    # The isolated-copy design means the real file was never touched; assert
    # that explicitly rather than merely relying on absence of a write.
    assert _real_replay_path().read_bytes() == before, (
        "the real checkout must never be modified by a mutation test"
    )


def _mutate_ignore_receive_ns(src: str) -> str:
    old = "        ns_tiebreak = self.receive_ns if self.receive_ns is not None else -1"
    new = "        ns_tiebreak = -1  # mutation: ignore receive_ns entirely"
    assert old in src, "mutation target not found -- test is stale"
    return src.replace(old, new)


def _assert_ignore_receive_ns_mutation_breaks_wire_ordering(mutated_module) -> None:
    late = mutated_module.ReplayFrame(timestamp_ms=T, kind=mutated_module.FrameKind.WIRE,
                                      source_index=0, payload="{}",
                                      receive_ns=T * 1_000_000 + 800_000)
    early = mutated_module.ReplayFrame(timestamp_ms=T, kind=mutated_module.FrameKind.WIRE,
                                       source_index=1, payload="{}",
                                       receive_ns=T * 1_000_000 + 100_000)
    frames = mutated_module.ReplaySource([late, early]).frames
    assert [f.source_index for f in frames] == [0, 1], (
        "mutation should make ns among WIRE frames irrelevant (falls back "
        "to source_index, 0 then 1); if this fails the mutation didn't take"
    )


def test_mutation_ignoring_receive_ns_among_wire_frames_is_caught():
    """Mutate the tiebreak to always use the sentinel (ignore real ns among
    WIRE frames), on an isolated copy. test_multiple_wire_frames_with_
    distinct_ns_order_by_ns... must fail under that mutation."""
    before = _real_replay_path().read_bytes()
    mutated, cleanup = _load_mutated_replay(_mutate_ignore_receive_ns)
    try:
        _assert_ignore_receive_ns_mutation_breaks_wire_ordering(mutated)
    finally:
        cleanup()
    assert _real_replay_path().read_bytes() == before


# ---------------------------------------------------------------------------
# Concurrency proof: two mutation runs in flight at once must not interfere.
#
# Threads are the more demanding case than separate pytest-xdist worker
# processes would be: threads share one process's sys.modules and sys.path,
# which is exactly the shared state the old (direct-file-edit,
# fixed-module-name) implementation collided on. Running many instances of
# both mutations concurrently in threads and asserting every one observed
# its own mutation and only its own is a stronger proof of isolation than
# xdist would give, and needs no new dependency.
# ---------------------------------------------------------------------------


def _run_one_mutation(which: str) -> None:
    if which == "kind_rank":
        mutated, cleanup = _load_mutated_replay(_mutate_kind_rank_after_ns)
        try:
            _assert_kind_rank_after_ns_mutation_reproduces_the_defect(mutated)
        finally:
            cleanup()
    else:
        mutated, cleanup = _load_mutated_replay(_mutate_ignore_receive_ns)
        try:
            _assert_ignore_receive_ns_mutation_breaks_wire_ordering(mutated)
        finally:
            cleanup()


def test_mutation_tests_are_concurrency_safe():
    """Run many interleaved instances of both mutations in parallel threads.

    Each call gets its own tempdir and uuid-named package (see
    _load_mutated_replay), so no thread can observe a half-applied mutation,
    the wrong mutation, or another thread's cleanup racing its own import --
    the exact failure mode the direct-file-edit design was exposed to. If
    any thread raises (including a stale/wrong assertion caused by
    cross-contamination), this test fails.
    """
    before = _real_replay_path().read_bytes()
    jobs = (["kind_rank", "ignore_ns"] * 8)  # 16 overlapping mutation loads
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(_run_one_mutation, jobs))
    assert len(results) == len(jobs)  # every job completed without raising
    assert _real_replay_path().read_bytes() == before
    # No mutant module or path entry may survive past cleanup.
    assert not [n for n in sys.modules if n.startswith("p011_mutant_")]
    assert not [p for p in sys.path if "p011_mutant_" in p]
