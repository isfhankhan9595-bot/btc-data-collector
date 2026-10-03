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

from collector.collector.replay import FrameKind, ReplayFrame, ReplaySource

T = 1_780_000_000_000


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


def test_mutation_reordering_kind_rank_after_ns_reintroduces_the_defect():
    """Mutate order_key back to the old (ns-first) shape; this exact
    regression test must fail under that mutation."""
    import hashlib
    import pathlib

    path = pathlib.Path(__file__).resolve().parents[1] / "collector" / "replay.py"
    original = path.read_bytes()
    before_hash = hashlib.sha256(original).hexdigest()

    src = original.decode()
    old = ("        ns_tiebreak = self.receive_ns if self.receive_ns is not None else -1\n"
           "        return (self.timestamp_ms, _KIND_RANK.get(self.kind, 9), ns_tiebreak, self.source_index)")
    new = ("        from .clock import effective_ns as _eff\n"
           "        return (_eff(self.timestamp_ms, self.receive_ns), "
           "_KIND_RANK.get(self.kind, 9), self.source_index)")
    assert old in src, "mutation target not found -- test is stale"
    path.write_text(src.replace(old, new))

    try:
        import importlib
        import sys
        sys.modules.pop("collector.collector.replay", None)
        mutated = importlib.import_module("collector.collector.replay")
        wire = mutated.ReplayFrame(timestamp_ms=T, kind=mutated.FrameKind.WIRE, source_index=0,
                                   payload="{}", receive_ns=T * 1_000_000 + 700_123)
        rest = mutated.ReplayFrame(timestamp_ms=T, kind=mutated.FrameKind.REST_SNAPSHOT,
                                   source_index=1, payload="{}")
        frames = mutated.ReplaySource([rest, wire]).frames
        assert frames[0].kind == mutated.FrameKind.REST_SNAPSHOT, (
            "mutation should have reproduced the original defect; if this "
            "assert fails, the mutation did not take effect"
        )
    finally:
        path.write_bytes(original)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == before_hash, "byte-exact restore failed"
        import sys
        sys.modules.pop("collector.collector.replay", None)
        import importlib
        importlib.import_module("collector.collector.replay")


def test_mutation_ignoring_receive_ns_among_wire_frames_is_caught():
    """Mutate the tiebreak to always use the sentinel (ignore real ns among
    WIRE frames). test_multiple_wire_frames_with_distinct_ns_order_by_ns...
    must fail under that mutation."""
    import hashlib
    import pathlib

    path = pathlib.Path(__file__).resolve().parents[1] / "collector" / "replay.py"
    original = path.read_bytes()
    before_hash = hashlib.sha256(original).hexdigest()

    src = original.decode()
    old = "        ns_tiebreak = self.receive_ns if self.receive_ns is not None else -1"
    new = "        ns_tiebreak = -1  # mutation: ignore receive_ns entirely"
    assert old in src
    path.write_text(src.replace(old, new))

    try:
        import importlib
        import sys
        sys.modules.pop("collector.collector.replay", None)
        mutated = importlib.import_module("collector.collector.replay")
        late = mutated.ReplayFrame(timestamp_ms=T, kind=mutated.FrameKind.WIRE, source_index=0,
                                   payload="{}", receive_ns=T * 1_000_000 + 800_000)
        early = mutated.ReplayFrame(timestamp_ms=T, kind=mutated.FrameKind.WIRE, source_index=1,
                                    payload="{}", receive_ns=T * 1_000_000 + 100_000)
        frames = mutated.ReplaySource([late, early]).frames
        assert [f.source_index for f in frames] == [0, 1], (
            "mutation should make ns among WIRE frames irrelevant (falls back "
            "to source_index, 0 then 1); if this fails the mutation didn't take"
        )
    finally:
        path.write_bytes(original)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == before_hash
        import sys
        sys.modules.pop("collector.collector.replay", None)
        import importlib
        importlib.import_module("collector.collector.replay")
