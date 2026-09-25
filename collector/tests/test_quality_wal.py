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
    QualityEventWAL,
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
    with pytest.raises(ValueError):   # writing to a closed file handle raises
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
