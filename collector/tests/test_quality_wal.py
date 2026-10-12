"""P0-2: quality-event WAL durability tests.

Every test operates on a real filesystem (tmp_path) with real file
handles -- no mocking of open/write/fsync. Crash scenarios are simulated
by directly truncating/corrupting files on disk after a WAL has written
them (the closest a test can get to an actual kill without literally
sending SIGKILL to a subprocess), then constructing a *fresh*
QualityEventWAL/recover() call exactly as a restarted process would.
"""
from __future__ import annotations

import json
import os

import pytest

from collector.collector.quality_wal import (
    CHECKPOINT_FILENAME,
    DEFAULT_MAX_TOTAL_BYTES,
    ENV_MAX_TOTAL_BYTES,
    QualityEventWAL,
    QualityWALCapacityExceeded,
    QualityWALCorruption,
)


def _event(reason="x", **overrides):
    base = {"exchange": "BINANCE", "stream": "orderbook", "event_type": "SEQUENCE_GAP", "reason": reason}
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Test 1 / 2: basic append + recovery, in order.
# ---------------------------------------------------------------------------


def test_appended_events_are_recovered_in_order(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    ids = [wal.append(_event(reason=f"r{i}")) for i in range(5)]
    wal.close()

    recovered = QualityEventWAL.recover(tmp_path)
    assert [r["reason"] for r in recovered] == [f"r{i}" for i in range(5)]
    assert [r["quality_event_id"] for r in recovered] == ids
    assert [r["seq"] for r in recovered] == [1, 2, 3, 4, 5]


def test_append_returns_the_same_id_stored_in_the_record(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    event_id = wal.append(_event())
    wal.close()
    recovered = QualityEventWAL.recover(tmp_path)
    assert recovered[0]["quality_event_id"] == event_id


# ---------------------------------------------------------------------------
# Test 3: stable event IDs survive restart.
# ---------------------------------------------------------------------------


def test_event_ids_are_stable_and_unique_across_a_simulated_restart(tmp_path):
    wal1 = QualityEventWAL(tmp_path, process_start_marker="proc-A")
    id1 = wal1.append(_event(reason="before restart"))
    wal1.close()

    # Simulated restart: a fresh WAL resumes its sequence past anything
    # already on disk, so a new process's IDs never collide with the old
    # process's, even though process_start_marker differs.
    resume_seq = QualityEventWAL.highest_recovered_seq(tmp_path)
    wal2 = QualityEventWAL(tmp_path, process_start_marker="proc-B", start_seq=resume_seq)
    id2 = wal2.append(_event(reason="after restart"))
    wal2.close()

    assert id1 != id2
    recovered = QualityEventWAL.recover(tmp_path)
    assert [r["quality_event_id"] for r in recovered] == [id1, id2]
    assert [r["reason"] for r in recovered] == ["before restart", "after restart"]


# ---------------------------------------------------------------------------
# Test 5: crash before Parquet -- events remain recoverable from WAL alone.
# ---------------------------------------------------------------------------


def test_crash_before_any_checkpoint_recovers_every_event(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    for i in range(3):
        wal.append(_event(reason=f"r{i}"))
    # No checkpoint() call at all -- simulates a crash before Parquet
    # persistence (or before acknowledgement) ever happened.
    wal.close()
    recovered = QualityEventWAL.recover(tmp_path)
    assert len(recovered) == 3


# ---------------------------------------------------------------------------
# Test 4 / 8: crash/idempotency around a checkpoint boundary.
# ---------------------------------------------------------------------------


def test_checkpointed_events_are_not_recovered_again(tmp_path):
    """Simulates: WAL append -> Parquet write succeeds -> checkpoint
    recorded -> (imagined) crash. Restart must not re-surface the
    already-safely-persisted events -- this is what makes
    replay-into-Parquet idempotent without needing Parquet-side dedup."""
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    for i in range(3):
        wal.append(_event(reason=f"r{i}"))
    wal.checkpoint(up_to_seq=2)   # first two events durably in Parquet
    wal.close()

    recovered = QualityEventWAL.recover(tmp_path)
    assert [r["reason"] for r in recovered] == ["r2"]   # only the uncheckpointed one


def test_double_checkpoint_of_the_same_bound_is_a_no_op(tmp_path):
    """Test 8's essence at the WAL layer: checkpointing twice (e.g. a
    compaction re-run) must not corrupt or regress the checkpoint state."""
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    wal.append(_event())
    wal.checkpoint(up_to_seq=1)
    first_checkpoint = json.loads((tmp_path / CHECKPOINT_FILENAME).read_text())
    wal.checkpoint(up_to_seq=1)   # repeat
    second_checkpoint = json.loads((tmp_path / CHECKPOINT_FILENAME).read_text())
    wal.close()
    assert first_checkpoint == second_checkpoint == {"checkpointed_seq": 1}


def test_checkpoint_cannot_move_backwards(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    for _ in range(5):
        wal.append(_event())
    wal.checkpoint(up_to_seq=5)
    wal.checkpoint(up_to_seq=2)   # a stale/out-of-order checkpoint call
    wal.close()
    assert json.loads((tmp_path / CHECKPOINT_FILENAME).read_text())["checkpointed_seq"] == 5


def test_start_seq_alone_does_not_imply_anything_was_checkpointed(tmp_path):
    """Independent audit finding: __init__'s start_seq resumes the ID
    counter only -- it must never be read as 'everything below this was
    already checkpointed'. A WAL constructed with start_seq=10 (e.g.
    because 10 events were previously written to disk) but with NO
    checkpoint.json on disk must still accept and honor a checkpoint call
    for any of those lower seqs -- the checkpoint() guard must not
    silently no-op just because seq < start_seq."""
    first = QualityEventWAL(tmp_path, process_start_marker="p1")
    for _ in range(10):
        first.append(_event())
    first.close()
    assert not (tmp_path / CHECKPOINT_FILENAME).exists()   # nothing checkpointed yet, genuinely

    resumed = QualityEventWAL(tmp_path, process_start_marker="p2", start_seq=10)
    resumed.checkpoint(up_to_seq=3)   # must NOT silently no-op
    resumed.close()
    checkpoint = json.loads((tmp_path / CHECKPOINT_FILENAME).read_text())
    assert checkpoint["checkpointed_seq"] == 3
    recovered = QualityEventWAL.recover(tmp_path)
    assert len(recovered) == 7          # seqs 4-10 remain pending
    assert min(r["seq"] for r in recovered) == 4


# ---------------------------------------------------------------------------
# Test 6: partial final record -- a normal, expected crash artifact.
# ---------------------------------------------------------------------------


def test_incomplete_final_line_is_quarantined_not_fabricated(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    wal.append(_event(reason="complete-1"))
    wal.append(_event(reason="complete-2"))
    wal.close()

    # Simulate a kill mid-write of a third record: append a truncated,
    # non-newline-terminated JSON fragment directly to the file.
    with open(wal._active_path, "a", encoding="utf-8") as handle:
        handle.write('{"quality_event_id": "p1-3", "seq": 3, "reason": "cut off mid-wri')

    recovered = QualityEventWAL.recover(tmp_path)
    assert [r["reason"] for r in recovered] == ["complete-1", "complete-2"]


def test_incomplete_final_line_does_not_raise():
    pass  # covered by the assertion above completing without raising; kept as a named marker of intent


# ---------------------------------------------------------------------------
# Test 7: corruption in the middle of the journal -- must not be silently
# treated as healthy.
# ---------------------------------------------------------------------------


def test_corrupt_middle_record_raises_and_does_not_silently_skip(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    wal.append(_event(reason="good-1"))
    wal.append(_event(reason="good-2"))
    wal.close()

    path = wal._active_path
    lines = path.read_text(encoding="utf-8").splitlines()
    lines.insert(1, "{not valid json at all")   # corruption in the middle, not the tail
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(QualityWALCorruption):
        QualityEventWAL.recover(tmp_path)


def test_corrupt_middle_record_is_distinguished_from_incomplete_tail(tmp_path):
    """The same malformed bytes are tolerated at the tail (Scenario C) and
    rejected in the middle (Scenario: real corruption) -- proving the
    distinction is about position, not content."""
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    wal.append(_event(reason="good"))
    wal.close()
    with open(wal._active_path, "a", encoding="utf-8") as handle:
        handle.write("{not valid json")   # no trailing newline -> tail position
    recovered = QualityEventWAL.recover(tmp_path)   # must NOT raise
    assert [r["reason"] for r in recovered] == ["good"]


# ---------------------------------------------------------------------------
# Test 2 (ordering) extended: multiple WAL files (post-rotation) still
# recover in true chronological order.
# ---------------------------------------------------------------------------


def test_rotation_then_recovery_preserves_cross_file_order(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p1", max_bytes=1)  # force rotation on every write
    reasons = [f"r{i}" for i in range(6)]
    for reason in reasons:
        wal.append(_event(reason=reason))
        wal.maybe_rotate_for_size()
    wal.close()
    assert len(list(tmp_path.glob("*.wal"))) > 1   # confirms rotation actually happened
    recovered = QualityEventWAL.recover(tmp_path)
    assert [r["reason"] for r in recovered] == reasons


def test_checkpoint_deletes_only_fully_covered_wal_files_not_the_active_one(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p1", max_bytes=1)
    wal.append(_event(reason="r0"))
    wal.maybe_rotate_for_size()          # rotates: file 0 now closed, file 1 active
    wal.append(_event(reason="r1"))
    wal.maybe_rotate_for_size()          # rotates: file 1 closed, file 2 active
    wal.append(_event(reason="r2"))      # stays in file 2, never checkpointed
    wal.checkpoint(up_to_seq=2)          # covers r0 and r1, not r2
    remaining = sorted(p.name for p in tmp_path.glob("*.wal"))
    assert len(remaining) == 1           # only the file holding the uncheckpointed r2 survives
    wal.close()
    recovered = QualityEventWAL.recover(tmp_path)
    assert [r["reason"] for r in recovered] == ["r2"]


def test_checkpoint_never_deletes_the_active_file_even_when_fully_covered(tmp_path):
    """Independent audit finding (Area 10): a checkpoint that happens to
    cover every seq the currently-active file holds must NOT delete that
    file -- it is still open for appending. Deleting an open file out
    from under an active writer is exactly the kind of catastrophic
    corruption this module's own single active-file exclusion exists to
    prevent; this test targets the case where checkpointed_seq reaches
    the active file's own max seq, not just an older, already-closed
    one."""
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    wal.append(_event(reason="only"))    # the one and only event, in the (only) active file
    wal.checkpoint(up_to_seq=1)          # covers everything the active file holds
    assert wal._active_path.exists()     # must survive -- it's still open
    wal.append(_event(reason="second"))  # must still be appendable afterward
    wal.close()
    recovered = QualityEventWAL.recover(tmp_path)
    assert [r["reason"] for r in recovered] == ["second"]   # "only" correctly checkpointed away


# ---------------------------------------------------------------------------
# Test 9: burst / bounded memory -- the WAL itself holds nothing in memory
# beyond one open file handle and a counter; this is a structural property,
# confirmed by inspecting what the object retains, plus a real large-burst
# round trip.
# ---------------------------------------------------------------------------


def test_wal_object_retains_no_per_event_memory():
    """The instance itself must not accumulate a growing list/dict per
    event -- only a plain int counter (_seq) and one open file handle,
    both O(1) regardless of how many events have been appended.
    recover()'s own local list (a return value built once per call, not
    instance state) is a different, unrelated thing and is not what this
    checks."""
    import inspect
    from collector.collector.quality_wal import QualityEventWAL
    init_src = inspect.getsource(QualityEventWAL.__init__)
    append_src = inspect.getsource(QualityEventWAL.append)
    for forbidden in ("self._events", "self._buffer", "self._records", "= []", "= {}"):
        assert forbidden not in init_src
        assert forbidden not in append_src


def test_large_burst_round_trips_without_loss(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    n = 2_000
    for i in range(n):
        wal.append(_event(reason=f"burst-{i}"))
    wal.close()
    recovered = QualityEventWAL.recover(tmp_path)
    assert len(recovered) == n
    assert recovered[0]["reason"] == "burst-0"
    assert recovered[-1]["reason"] == f"burst-{n - 1}"


# ---------------------------------------------------------------------------
# Test 10: concurrent producers cannot interleave bytes.
# ---------------------------------------------------------------------------


def test_concurrent_appends_from_multiple_threads_never_interleave_bytes(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    import threading as _threading
    errors = []

    def producer(n):
        try:
            for i in range(50):
                wal.append(_event(reason=f"t{n}-{i}"))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [_threading.Thread(target=producer, args=(t,)) for t in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()
    wal.close()

    assert not errors
    recovered = QualityEventWAL.recover(tmp_path)
    assert len(recovered) == 400                       # 8 threads * 50 -- nothing lost, nothing corrupted
    assert len({r["quality_event_id"] for r in recovered}) == 400   # every ID unique, no interleaving collision
    assert [r["seq"] for r in recovered] == list(range(1, 401))     # strictly sequential, no gaps or dupes


# ---------------------------------------------------------------------------
# Test 11: write failure is visible, never silently absorbed.
# ---------------------------------------------------------------------------


def test_append_failure_is_visible_not_silently_swallowed(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    wal.append(_event(reason="ok"))
    wal.close()   # closes the underlying file handle

    assert wal.write_failed is False
    # Append after close is an explicit OSError (previously an incidental
    # ValueError from the closed file object), so the collector's single
    # "WAL could not durably record this" failure path handles it.
    with pytest.raises(OSError, match="closed"):
        wal.append(_event(reason="after close"))


# ---------------------------------------------------------------------------
# Purity / determinism.
# ---------------------------------------------------------------------------


def test_recover_does_not_mutate_or_delete_anything(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    wal.append(_event())
    wal.close()
    before = sorted(p.name for p in tmp_path.iterdir())
    QualityEventWAL.recover(tmp_path)
    QualityEventWAL.recover(tmp_path)   # twice, for good measure
    after = sorted(p.name for p in tmp_path.iterdir())
    assert before == after


def test_recover_is_deterministic_across_repeated_calls(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    for i in range(4):
        wal.append(_event(reason=f"r{i}"))
    wal.close()
    first = QualityEventWAL.recover(tmp_path)
    second = QualityEventWAL.recover(tmp_path)
    assert first == second


def test_recover_of_an_empty_directory_returns_empty():
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        assert QualityEventWAL.recover(d) == []


def test_a_corrupted_checkpoint_file_is_treated_as_nothing_checkpointed_not_everything(tmp_path):
    """A torn/corrupted checkpoint file must never be read as 'safe to
    discard everything' -- that would be the single worst possible
    failure mode for a durability mechanism. Treated as the strictly
    safer 'nothing checkpointed yet' instead."""
    wal = QualityEventWAL(tmp_path, process_start_marker="p1")
    wal.append(_event(reason="r0"))
    wal.close()
    (tmp_path / CHECKPOINT_FILENAME).write_text("{not valid json", encoding="utf-8")
    recovered = QualityEventWAL.recover(tmp_path)
    assert [r["reason"] for r in recovered] == ["r0"]


# ---------------------------------------------------------------------------
# Aggregate admission capacity and reserved metadata invariants.
# ---------------------------------------------------------------------------


def test_aggregate_cap_counts_every_wal_file_and_rejects_before_writing(tmp_path):
    first = QualityEventWAL(
        tmp_path, process_start_marker="p1", max_total_bytes=1_000_000)
    first.append(_event(reason="first-file"))
    second = QualityEventWAL(
        tmp_path, process_start_marker="p2", max_total_bytes=1_000_000)
    second.append(_event(reason="second-file"))

    current = first._total_wal_bytes()
    candidate = {
        **_event(reason="candidate"),
        "quality_event_id": "p1-2",
        "seq": 2,
    }
    requested = len((json.dumps(candidate, default=str) + "\\n").encode("utf-8"))
    # Allow the candidate to fit beside the active file alone, but not when
    # every WAL file in the shared namespace is counted.
    first_active_size = first._active_path.stat().st_size
    first.max_total_bytes = current + requested - 1
    assert first_active_size + requested <= first.max_total_bytes
    before = {p.name: p.read_bytes() for p in tmp_path.glob("*.wal")}

    with pytest.raises(QualityWALCapacityExceeded) as caught:
        first.append(_event(reason="candidate"))

    assert caught.value.current_bytes == current
    assert caught.value.requested_bytes == requested
    assert caught.value.newly_exhausted is True
    assert not hasattr(caught.value, "quality_event_id")
    assert first.capacity_exhausted is True
    assert {p.name: p.read_bytes() for p in tmp_path.glob("*.wal")} == before
    with pytest.raises(QualityWALCapacityExceeded) as again:
        first.append(_event(reason="later"))
    assert again.value.newly_exhausted is False
    first.close()
    second.close()


def test_oversized_single_event_is_rejected_without_a_wal_id_or_partial_line(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="p", max_total_bytes=256)
    before = {p.name: p.read_bytes() for p in tmp_path.glob("*.wal")}

    with pytest.raises(QualityWALCapacityExceeded) as caught:
        wal.append(_event(reason="x" * 2048))

    assert caught.value.reason == "aggregate_budget_exhausted"
    assert not hasattr(caught.value, "quality_event_id")
    assert {p.name: p.read_bytes() for p in tmp_path.glob("*.wal")} == before
    assert wal.capacity_exhausted is True
    wal.close()


def test_wal_generated_identity_fields_cannot_be_overridden_by_event_data(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="owned")
    event_id = wal.append(_event(reason="reserved", seq=999, quality_event_id="caller-spoof"))
    wal.close()

    recovered = QualityEventWAL.recover(tmp_path)
    assert event_id == "owned-1"
    assert recovered[0]["seq"] == 1
    assert recovered[0]["quality_event_id"] == "owned-1"


@pytest.mark.parametrize("bad_cap", [0, -1, True, "100"])
def test_invalid_aggregate_cap_is_rejected(bad_cap, tmp_path):
    with pytest.raises(ValueError, match="max_total_bytes"):
        QualityEventWAL(tmp_path / str(type(bad_cap).__name__) , max_total_bytes=bad_cap)


def test_aggregate_cap_reads_environment_and_has_documented_default(tmp_path, monkeypatch):
    monkeypatch.delenv(ENV_MAX_TOTAL_BYTES, raising=False)
    default = QualityEventWAL(tmp_path / "default")
    assert default.max_total_bytes == DEFAULT_MAX_TOTAL_BYTES
    default.close()

    monkeypatch.setenv(ENV_MAX_TOTAL_BYTES, "4096")
    configured = QualityEventWAL(tmp_path / "configured")
    assert configured.max_total_bytes == 4096
    configured.close()

    monkeypatch.setenv(ENV_MAX_TOTAL_BYTES, "4K")
    with pytest.raises(ValueError, match=ENV_MAX_TOTAL_BYTES):
        QualityEventWAL(tmp_path / "invalid")


def test_startup_cleanup_removes_only_fully_checkpointed_valid_wal_files(tmp_path):
    previous = QualityEventWAL(tmp_path, process_start_marker="previous")
    previous.append(_event(reason="already-published"))
    previous.checkpoint(up_to_seq=1)
    previous.close()
    old_active = previous._active_path
    assert old_active.exists()
    assert QualityEventWAL.recover(tmp_path) == []   # full recovery validation succeeded
    QualityEventWAL.cleanup_fully_checkpointed_files(tmp_path)

    restarted = QualityEventWAL(
        tmp_path, process_start_marker="restarted", max_total_bytes=256)
    assert not old_active.exists()                 # only valid, checkpoint-covered evidence is cleaned
    assert restarted.capacity_exhausted is False
    assert restarted._total_wal_bytes() == 0
    restarted.close()


def test_startup_cleanup_preserves_any_corrupt_or_unterminated_wal_file(tmp_path):
    wal = QualityEventWAL(tmp_path, process_start_marker="previous")
    wal.append(_event(reason="covered"))
    wal.checkpoint(up_to_seq=1)
    wal.close()
    old_active = wal._active_path
    with old_active.open("ab") as handle:
        handle.write(b"{corrupt-tail}\n")

    with pytest.raises(QualityWALCorruption):
        QualityEventWAL.recover(tmp_path)       # failed recovery forbids cleanup
    restarted = QualityEventWAL(tmp_path, process_start_marker="restarted")
    assert old_active.exists()
    restarted.close()
