"""Quality-WAL restart sequence/checkpoint contract (data-integrity P0).

Invariant under test
--------------------
A freshly constructed ``QualityEventWAL`` must hand out sequence numbers that
are strictly greater than BOTH

* the highest ``seq`` still present in any surviving ``*.wal`` file, and
* the durable ``checkpointed_seq`` in ``checkpoint.json``.

Why the second bound matters: ``checkpoint()`` deletes fully-covered WAL
files, so after a normal restart the surviving files can hold a lower
highest-seq than the checkpoint -- or none at all
(``highest_recovered_seq() == -1``). A counter resumed only from the files
would then issue a seq at or below the durable checkpoint, and ``recover()``
(which drops ``seq <= checkpointed_seq``) and ``checkpoint()`` (which refuses
to move to ``seq <= checkpointed_seq``) would both silently treat that brand-new
event as already persisted.

Every "restart" below is a real fresh ``QualityEventWAL`` over the same
directory, built exactly the way ``CollectorApp._recover_quality_wal`` builds
it: ``start_seq=QualityEventWAL.highest_recovered_seq(wal_dir)``.
"""
from __future__ import annotations

import json
import pathlib

import pytest

from collector.collector.quality_wal import CHECKPOINT_FILENAME, QualityEventWAL


def _ev(reason: str = "x") -> dict:
    return {"exchange": "BINANCE", "stream": "orderbook", "event_type": "SEQUENCE_GAP", "reason": reason}


def _restart(wal_dir, marker: str, **kwargs) -> QualityEventWAL:
    """A restarted process, wired the way the production runner wires it."""
    resume_seq = QualityEventWAL.highest_recovered_seq(wal_dir)
    return QualityEventWAL(wal_dir, process_start_marker=marker, start_seq=resume_seq, **kwargs)


def _seq_of(event_id: str) -> int:
    return int(event_id.rsplit("-", 1)[-1])


def _durable_checkpoint(wal_dir) -> int:
    return json.loads((pathlib.Path(wal_dir) / CHECKPOINT_FILENAME).read_text())["checkpointed_seq"]


def _wal_files(wal_dir):
    return sorted(pathlib.Path(wal_dir).glob("*.wal"))


def _fully_checkpointed_then_deleted(wal_dir) -> int:
    """Drive the real, non-contrived path to 'every seq-bearing WAL file is
    gone but a durable checkpoint remains', and return that checkpoint.

    p1 writes seqs 1-5 and checkpoints only 1-2; p2 restarts, recovers the
    pending 3-5 and checkpoints them -- which deletes p1's (no longer active)
    file -- and shuts down having appended nothing, leaving only its own
    empty active file behind.
    """
    p1 = QualityEventWAL(wal_dir, process_start_marker="p1")
    for i in range(5):
        p1.append(_ev(f"old{i}"))
    p1.checkpoint(up_to_seq=2)
    p1.close()

    p2 = _restart(wal_dir, "p2")
    pending = QualityEventWAL.recover(wal_dir)
    assert [r["seq"] for r in pending] == [3, 4, 5]
    p2.checkpoint(up_to_seq=max(r["seq"] for r in pending))
    p2.close()
    return 5


# ---------------------------------------------------------------- A
def test_fully_checkpointed_wal_deleted_then_restart_new_event_is_above_checkpoint(tmp_path):
    checkpoint = _fully_checkpointed_then_deleted(tmp_path)

    # Precondition: this is exactly the dangerous state from the defect.
    assert _durable_checkpoint(tmp_path) == checkpoint
    assert QualityEventWAL.highest_recovered_seq(tmp_path) == -1   # no seq-bearing file survives
    assert QualityEventWAL.recover(tmp_path) == []

    p3 = _restart(tmp_path, "p3")
    new_id = p3.append(_ev("brand-new"))
    p3.close()

    assert _seq_of(new_id) > checkpoint                             # was 0 before the fix
    recovered = QualityEventWAL.recover(tmp_path)
    assert [r["reason"] for r in recovered] == ["brand-new"]        # NOT silently treated as persisted
    assert recovered[0]["seq"] > checkpoint
    assert recovered[0]["quality_event_id"] == new_id


# ---------------------------------------------------------------- B
def test_partially_surviving_wal_below_checkpoint_then_restart_new_event_is_above_checkpoint(tmp_path, monkeypatch):
    """checkpoint() tolerates a failed unlink (the checkpoint is already
    durable), so stale covered files can survive while newer covered files
    are deleted. The surviving files' highest seq is then BELOW the
    checkpoint but not absent."""
    p1 = QualityEventWAL(tmp_path, process_start_marker="p1", max_bytes=1)
    for i in range(10):                      # one record per file: seq i+1 lives in file i+1
        p1.append(_ev(f"old{i}"))
        p1.maybe_rotate_for_size()

    real_unlink = pathlib.Path.unlink

    def flaky_unlink(self, missing_ok=False):
        # Files holding seq 1..8 refuse deletion; files holding 9..10 delete fine.
        if self.suffix == ".wal" and any(self.name.endswith(f"-{n:012d}.wal") for n in range(0, 8)):
            raise PermissionError(13, "EACCES")
        return real_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(pathlib.Path, "unlink", flaky_unlink)
    p1.checkpoint(up_to_seq=10)
    monkeypatch.setattr(pathlib.Path, "unlink", real_unlink)
    p1.close()

    surviving_highest = QualityEventWAL.highest_recovered_seq(tmp_path)
    assert _durable_checkpoint(tmp_path) == 10
    assert 0 < surviving_highest < 10        # partially surviving, strictly below the checkpoint

    p2 = _restart(tmp_path, "p2")
    new_id = p2.append(_ev("after-partial"))
    p2.close()

    assert _seq_of(new_id) > 10
    assert [r["reason"] for r in QualityEventWAL.recover(tmp_path)] == ["after-partial"]


# ---------------------------------------------------------------- C
def test_checkpoint_advancement_after_restart_still_works(tmp_path):
    old_checkpoint = _fully_checkpointed_then_deleted(tmp_path)

    p3 = _restart(tmp_path, "p3")
    ids = [p3.append(_ev(f"new{i}")) for i in range(3)]
    new_seqs = [_seq_of(i) for i in ids]
    assert new_seqs == sorted(new_seqs) and new_seqs[0] > old_checkpoint

    # Checkpointing the first new seq must NOT be swallowed by the
    # 'up_to_seq <= checkpointed' guard, and must really advance on disk.
    p3.checkpoint(up_to_seq=new_seqs[0])
    assert _durable_checkpoint(tmp_path) == new_seqs[0]
    assert [r["reason"] for r in QualityEventWAL.recover(tmp_path)] == ["new1", "new2"]

    p3.checkpoint(up_to_seq=new_seqs[-1])
    assert _durable_checkpoint(tmp_path) == new_seqs[-1]
    assert QualityEventWAL.recover(tmp_path) == []
    p3.close()


# ---------------------------------------------------------------- D
def test_recovery_still_excludes_genuinely_checkpointed_old_events(tmp_path):
    p1 = QualityEventWAL(tmp_path, process_start_marker="p1")
    for i in range(5):
        p1.append(_ev(f"old{i}"))
    p1.checkpoint(up_to_seq=3)               # seqs 1-3 durably in Parquet; 4-5 still pending
    p1.close()

    p2 = _restart(tmp_path, "p2")
    p2.append(_ev("new"))
    p2.close()

    recovered = QualityEventWAL.recover(tmp_path)
    assert [r["reason"] for r in recovered] == ["old3", "old4", "new"]
    assert [r["seq"] for r in recovered] == [4, 5, 6]
    assert all(r["seq"] > 3 for r in recovered)    # checkpointed 1-3 never resurface


# ---------------------------------------------------------------- E
def test_sequence_ids_never_regress_across_repeated_restarts(tmp_path):
    """Several full process lifecycles. Each cycle is:

      writer   -- restarts, appends two events, checkpoints the first only,
                  and exits (so exactly one event is genuinely pending);
      recoverer-- restarts, recovers that one pending event, checkpoints it
                  (which deletes the writer's file) and exits having
                  appended nothing, leaving only an empty WAL file.

    The next writer therefore always starts from the dangerous state. Seqs
    must be strictly increasing over the whole history, and each writer's
    still-pending event must really be pending (not swallowed)."""
    all_seqs: list[int] = []
    for cycle in range(4):
        writer = _restart(tmp_path, f"w{cycle}")
        first = _seq_of(writer.append(_ev(f"c{cycle}-persisted")))
        second = _seq_of(writer.append(_ev(f"c{cycle}-pending")))
        writer.checkpoint(up_to_seq=first)
        writer.close()
        all_seqs.extend([first, second])

        pending = QualityEventWAL.recover(tmp_path)
        assert [r["reason"] for r in pending] == [f"c{cycle}-pending"]       # not swallowed by an old checkpoint
        assert [r["seq"] for r in pending] == [second]

        recoverer = _restart(tmp_path, f"r{cycle}")
        recoverer.checkpoint(up_to_seq=second)
        recoverer.close()
        assert QualityEventWAL.recover(tmp_path) == []
        assert QualityEventWAL.highest_recovered_seq(tmp_path) == -1         # only an empty file survives

    assert all_seqs == sorted(set(all_seqs))        # strictly increasing, no repeats, no regression
    assert len(all_seqs) == 8
    assert _durable_checkpoint(tmp_path) == all_seqs[-1]


def test_sequence_never_below_checkpoint_regardless_of_caller_supplied_start_seq(tmp_path):
    """start_seq is a lower bound on the counter, never an upper one: even a
    caller that passes a stale/low/default start_seq (or none) cannot make a
    fresh WAL issue a seq at or below the durable checkpoint."""
    checkpoint = _fully_checkpointed_then_deleted(tmp_path)

    for kwargs in ({"start_seq": -1}, {"start_seq": 0}, {}):
        wal = QualityEventWAL(tmp_path, process_start_marker="px", **kwargs)
        seq = _seq_of(wal.append(_ev()))
        wal.close()
        assert seq > checkpoint, f"{kwargs or 'default start_seq'} issued seq {seq} <= checkpoint {checkpoint}"


def test_a_higher_caller_supplied_start_seq_is_still_honoured(tmp_path):
    """The fix only RAISES the floor; an explicit start_seq above both the
    checkpoint and the on-disk highest keeps working as before."""
    checkpoint = _fully_checkpointed_then_deleted(tmp_path)
    wal = QualityEventWAL(tmp_path, process_start_marker="px", start_seq=checkpoint + 50)
    assert _seq_of(wal.append(_ev())) == checkpoint + 51
    wal.close()


def _wal_with_surviving_seqs_above_checkpoint(wal_dir) -> None:
    p1 = QualityEventWAL(wal_dir, process_start_marker="p1")
    for _ in range(7):
        p1.append(_ev())
    p1.checkpoint(up_to_seq=2)      # 1-2 durable; 3-7 pending and still on disk
    p1.close()


def test_surviving_wal_above_checkpoint_bounds_the_new_sequence_runner_wiring(tmp_path):
    """The on-disk half of the invariant, via the production wiring
    (start_seq=highest_recovered_seq). Held before the fix; pinned here."""
    _wal_with_surviving_seqs_above_checkpoint(tmp_path)
    p2 = _restart(tmp_path, "p2")
    assert _seq_of(p2.append(_ev())) == 8
    p2.close()


def test_surviving_wal_above_checkpoint_bounds_the_new_sequence_even_without_start_seq(tmp_path):
    """Same on-disk bound when the caller passes NO start_seq. Before the
    fix the counter began at 0 here and issued seq 1 -- a duplicate of a
    seq already on disk AND below the checkpoint (2), i.e. silently lost."""
    _wal_with_surviving_seqs_above_checkpoint(tmp_path)
    p2 = QualityEventWAL(tmp_path, process_start_marker="p2")
    assert _seq_of(p2.append(_ev())) == 8
    p2.close()
    assert [r["seq"] for r in QualityEventWAL.recover(tmp_path)] == [3, 4, 5, 6, 7, 8]


def test_fresh_empty_directory_still_starts_at_the_beginning(tmp_path):
    wal = _restart(tmp_path, "p0")
    assert _seq_of(wal.append(_ev())) == 0      # unchanged: nothing on disk, no checkpoint
    wal.close()


def test_corrupt_checkpoint_file_does_not_raise_and_falls_back_to_disk_state(tmp_path):
    p1 = QualityEventWAL(tmp_path, process_start_marker="p1")
    p1.append(_ev()); p1.append(_ev())
    p1.close()
    (tmp_path / CHECKPOINT_FILENAME).write_text("{not json")
    wal = _restart(tmp_path, "p2")
    assert _seq_of(wal.append(_ev())) == 3      # corrupt checkpoint == 'nothing checkpointed'; files still bound the seq
    wal.close()
