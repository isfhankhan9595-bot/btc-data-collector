"""Append-only durable journal for quality events (P0-2).

Replaces the previous ``quality_queue.pending.json`` marker, which only
recorded how many events were pending in the in-memory queue at the last
write -- never their actual content. After a hard kill, that meant the
collector could know "quality events were pending" without being able to
recover *which* events, or what they said. This module makes the actual
event content itself the thing that survives a crash.

============================================================================
DURABILITY CONTRACT -- read this before changing anything below.
============================================================================

An event is durable the instant ``append()`` returns. Each call
synchronously writes one complete JSON line, flushes Python's internal
buffer, and calls ``os.fsync()`` on the file descriptor before returning.
This is the same per-event fsync policy the marker file it replaces
already used (open + write + flush + fsync + atomic replace, on every
single event) -- quality events are low-volume relative to the trade
stream (this task's own framing), so this is not a new performance cost,
only a more complete guarantee than what already existed.

What survives:

* Normal shutdown: every event (the persistence loop drains the queue,
  and ``checkpoint()`` clears fully-durable WAL files, exactly as before).
* SIGTERM: every event that reached ``append()`` before the process
  actually exits -- the same window the collector's existing graceful
  shutdown already accepted.
* SIGKILL / power loss: every event whose ``append()`` call had already
  *returned* before the kill.

What can still be lost: an event whose ``append()`` call was itself
interrupted mid-write by the kill -- a single in-flight write, not the
entire queue. ``os.fsync()`` makes a *completed* write durable; it cannot
protect a write that never finished, and no software-only mechanism can.
This is a fundamentally narrower, explicitly documented loss window, not
an unstated gap -- do not describe this as zero-loss durability, because
it is not: it is "at most one in-flight event, never an unbounded queue".

============================================================================
RECORD IDENTITY
============================================================================

Every appended event gets a ``quality_event_id`` of the form
``"{process_start_marker}-{global_seq}"``: deterministic and testable (the
caller controls ``process_start_marker`` and the sequence is a plain
incrementing integer, not random), monotonically informative within one
``process_start_marker`` (a higher seq is always a later event, including
across a WAL rotation -- the sequence is never reset by rotation), and
resumed across a process restart so a new process's IDs never collide
with an old one's **for the common case where the two processes' default
``process_start_marker`` values differ**.

Sequence invariant: a freshly constructed WAL issues seqs strictly greater
than BOTH the highest seq still present in any surviving ``*.wal`` file AND
the durable ``checkpointed_seq`` (``start_seq`` can only raise that floor,
never lower it). Both bounds are needed because checkpointing deletes covered
files: after a restart the files alone may hold a lower highest-seq than the
checkpoint, or none at all, and a seq at or below the checkpoint would be
silently treated as already persisted by ``recover()`` and ``checkpoint()``.

That default is ``f"{int(time.time()*1000)}-{os.getpid()}"`` --
millisecond timestamp plus PID. This is NOT a mathematically proven
collision-free identifier: two process starts landing in the exact same
millisecond *and* receiving the same PID would produce the same marker.
On a bare-metal host or VM this is vanishingly unlikely (process restart
takes far longer than one millisecond, and PID reuse within that window
essentially never happens in practice). Under containerized deployment,
where the main process very commonly gets PID 1 in every fresh container,
the PID half of that pair is far more likely to repeat across restarts
than the reasoning above assumes -- the guarantee then rests entirely on
the millisecond-timestamp half not coinciding across two separate
container starts, which restart latency still makes practically
implausible, but is not proven, not tested, and not something this module
should be read as claiming to rule out. If that ever needs to be
eliminated rather than merely made implausible, the caller can pass its
own ``process_start_marker`` incorporating something guaranteed unique in
its deployment (a container instance ID, for example) -- the module
supports this today via the constructor parameter; it is not done by
default in order to keep the identifier deterministic and testable rather
than introducing a random or environment-sourced component.

============================================================================
CHECKPOINTING AND ROTATION
============================================================================

``checkpoint(up_to_seq)`` records, durably (tmp file + fsync + atomic
rename -- this project's own established sidecar-file pattern, see
``parquet_writer.py``'s ``_close_segment``), that every event with
``seq <= up_to_seq`` is now safely represented in Parquet. It then deletes
any WAL file whose own highest seq is fully covered by that checkpoint --
not necessarily the active file, since ``maybe_rotate_for_size()`` can
open a new file before the old one's contents are checkpointed. A WAL file
is never deleted, or rewritten, before its checkpoint is durable on disk;
the checkpoint file itself is the only thing this module ever overwrites
in place, and only atomically.
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Optional

CHECKPOINT_FILENAME = "checkpoint.json"


class QualityWALCorruption(Exception):
    """A WAL record failed to parse and is NOT the final line of its file.

    An incomplete final line is treated as a normal, expected crash
    artifact (Step 8, Scenario C) and silently quarantined during
    recovery -- it never raises. A malformed record anywhere else in the
    file is a materially more serious condition (Scenario: mid-file
    corruption) and must never be silently skipped past; it raises this
    instead, so a corrupted journal is never quietly treated as healthy.
    """


class QualityEventWAL:
    def __init__(self, wal_dir: Any, *, max_bytes: int = 10_000_000,
                process_start_marker: Optional[str] = None, start_seq: int = 0):
        self.wal_dir = Path(wal_dir)
        self.wal_dir.mkdir(parents=True, exist_ok=True)
        self.max_bytes = max_bytes
        self.process_start_marker = process_start_marker or f"{int(time.time() * 1000)}-{os.getpid()}"
        self._lock = threading.Lock()
        # Independent audit finding: this must read the ACTUAL on-disk
        # checkpoint state, never infer it from start_seq. start_seq only
        # tells the sequence counter where to resume from (so new IDs
        # never collide with old ones) -- it says nothing about what is
        # truly durable. Conflating the two meant a caller who legitimately
        # needed to checkpoint a LOWER seq than start_seq-1 (e.g. recovery
        # stopping at the first persist failure, checkpointing only the
        # contiguous prefix before it) would silently no-op: checkpoint()'s
        # own "up_to_seq <= self._checkpointed_seq -> return" guard would
        # incorrectly believe that lower seq was already checkpointed,
        # because start_seq alone said so, with no checkpoint.json ever
        # written to back that belief up.
        self._checkpointed_seq = _read_checkpointed_seq(self.wal_dir)
        # SEQUENCE FLOOR (restart-after-checkpoint data-integrity fix).
        # ``start_seq`` is only a caller-supplied LOWER bound on the counter.
        # The counter is the last seq already issued (append() pre-increments),
        # so it must be at least as large as BOTH durable facts this WAL can
        # observe for itself:
        #   * the durable checkpoint -- checkpoint() deletes fully-covered
        #     files, so every seq-bearing file may be gone while
        #     checkpoint.json still says N; and
        #   * the highest seq still present in any surviving WAL file.
        # Resuming from the files alone (what highest_recovered_seq() reports)
        # could issue a seq <= the checkpoint, which recover() and checkpoint()
        # would then both treat as "already persisted" -- silently losing a
        # brand-new event. Computed BEFORE the new active file is created.
        self._seq = max(start_seq, self._checkpointed_seq, self.highest_recovered_seq(self.wal_dir))
        self._active_path = self._new_wal_path()
        self._handle = open(self._active_path, "a", encoding="utf-8")
        self._write_failed = False
        self._closed = False

    def _new_wal_path(self) -> Path:
        stamp = time.strftime("%Y-%m-%d-%H%M%S", time.gmtime())
        return self.wal_dir / f"{stamp}-{self.process_start_marker}-{self._seq:012d}.wal"

    def append(self, event: dict) -> str:
        """Durably append one quality event. Returns its quality_event_id.

        On a write/flush/fsync failure, marks this WAL degraded (see
        ``write_failed``) and re-raises -- callers must not treat a failed
        append as if the event were durable (Step 11 / acceptance
        criterion: write failures are visible, never silently absorbed).
        """
        with self._lock:
            self._seq += 1
            event_id = f"{self.process_start_marker}-{self._seq}"
            if self._closed:
                # Explicit, loud, and an OSError so callers that already
                # treat "the WAL could not durably record this" as one
                # failure class (fall back / report) handle a late append
                # during teardown the same way, rather than seeing an
                # incidental ValueError from a closed file object.
                exc = OSError("quality WAL is closed")
                exc.quality_event_id = event_id
                raise exc
            record = {"quality_event_id": event_id, "seq": self._seq, **event}
            line = json.dumps(record, default=str)
            try:
                self._handle.write(line + "\n")
                self._handle.flush()
                os.fsync(self._handle.fileno())
            except OSError as exc:
                self._write_failed = True
                # The id was already assigned and bytes may already be in
                # the file (e.g. write() and flush() succeeded but fsync()
                # failed). Attach it so a caller that falls back to a
                # direct write can stamp the SAME id on that row -- a
                # WAL-resident copy recovered later is then reconcilable
                # by id instead of being an anonymous duplicate.
                exc.quality_event_id = event_id
                raise
            return event_id

    @property
    def write_failed(self) -> bool:
        return self._write_failed

    def checkpoint(self, up_to_seq: int) -> None:
        """Record that every event with seq <= up_to_seq is durably in
        Parquet, then delete any WAL file fully covered by that bound."""
        with self._lock:
            if up_to_seq <= self._checkpointed_seq:
                return
            checkpoint_path = self.wal_dir / CHECKPOINT_FILENAME
            tmp = checkpoint_path.with_suffix(".json.tmp")
            with tmp.open("w", encoding="utf-8") as handle:
                json.dump({"checkpointed_seq": up_to_seq}, handle)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, checkpoint_path)
            # In-memory state advances ONLY after the durable replace
            # succeeded. Advancing it first (the previous order) meant a
            # failed write left this object claiming a checkpoint that
            # never reached disk, and made an immediate retry of the
            # same seq a silent no-op via the guard above.
            self._checkpointed_seq = up_to_seq
            self._delete_fully_checkpointed_files()

    def _delete_fully_checkpointed_files(self) -> None:
        for wal_file in sorted(self.wal_dir.glob("*.wal")):
            if wal_file == self._active_path:
                continue  # never delete the file currently being appended to
            max_seq = _max_seq_in_file(wal_file)
            if max_seq is not None and max_seq <= self._checkpointed_seq:
                try:
                    wal_file.unlink(missing_ok=True)
                except OSError:
                    # The checkpoint is already durable, so a file we
                    # could not delete is harmless: recover() filters by
                    # seq, so it contributes nothing pending, and the
                    # next checkpoint() retries the deletion.
                    continue

    def maybe_rotate_for_size(self) -> None:
        """Open a new WAL file if the active one has grown past max_bytes.
        The old file is NOT deleted here -- only checkpoint() may delete a
        WAL file, and only once its contents are confirmed durable
        elsewhere."""
        with self._lock:
            if self._active_path.exists() and self._active_path.stat().st_size >= self.max_bytes:
                # Order matters: make the old file durable, open the new
                # one, and only then close the old. If any step raises,
                # self._handle is still the original, open, valid file --
                # a failed rotation must never leave the WAL holding a
                # closed handle that turns every later append into a
                # ValueError.
                self._handle.flush()
                os.fsync(self._handle.fileno())
                new_path = self._new_wal_path()
                new_handle = open(new_path, "a", encoding="utf-8")
                old_handle = self._handle
                self._handle = new_handle
                self._active_path = new_path
                old_handle.close()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._handle.flush()
            os.fsync(self._handle.fileno())
            self._handle.close()

    @staticmethod
    def recover(wal_dir: Any) -> list[dict]:
        """Read every WAL file in wal_dir, oldest first (filenames are
        zero-padded-seq-suffixed and therefore sort chronologically),
        skip a genuinely incomplete final line (Scenario C), raise
        QualityWALCorruption on any other malformed record (mid-file
        corruption), and return every event NOT already covered by the
        durable checkpoint, in original append order. Does not mutate or
        delete anything -- recovery is read-only; the caller checkpoints
        after successfully re-persisting what this returns.
        """
        wal_dir = Path(wal_dir)
        checkpointed_seq = _read_checkpointed_seq(wal_dir)

        recovered: list[dict] = []
        for wal_file in sorted(wal_dir.glob("*.wal")):
            raw = wal_file.read_bytes()
            if not raw:
                continue
            text = raw.decode("utf-8", errors="strict")
            ends_with_newline = text.endswith("\n")
            lines = text.splitlines()
            for i, stripped in enumerate(lines):
                if not stripped:
                    continue
                is_last = i == len(lines) - 1
                try:
                    record = json.loads(stripped)
                except json.JSONDecodeError:
                    if is_last and not ends_with_newline:
                        continue  # incomplete final line: expected crash artifact, quarantined silently
                    raise QualityWALCorruption(
                        f"corrupt quality-event WAL record in {wal_file.name} at line {i + 1} "
                        f"(not an incomplete final line): {stripped[:200]!r}")
                if record.get("seq", -1) <= checkpointed_seq:
                    continue
                recovered.append(record)
        return recovered

    @staticmethod
    def highest_recovered_seq(wal_dir: Any) -> int:
        """The highest seq present in any WAL file still on disk
        (checkpointed or not), or -1 if none.

        This looks at surviving files ONLY. checkpoint() deletes covered
        files, so it can legitimately be lower than the durable checkpoint
        (even -1) after a normal restart; it is therefore NOT sufficient on
        its own as the next sequence floor. QualityEventWAL.__init__ combines
        it with the durable checkpoint, so callers may keep passing it as
        ``start_seq`` safely."""
        wal_dir = Path(wal_dir)
        highest = -1
        for wal_file in sorted(wal_dir.glob("*.wal")):
            max_seq = _max_seq_in_file(wal_file)
            if max_seq is not None:
                highest = max(highest, max_seq)
        return highest


def _read_checkpointed_seq(wal_dir: Path) -> int:
    checkpoint_path = wal_dir / CHECKPOINT_FILENAME
    if not checkpoint_path.exists():
        return -1
    try:
        data = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        return int(data.get("checkpointed_seq", -1))
    except (json.JSONDecodeError, OSError, ValueError, TypeError):
        # A corrupted checkpoint file must never be read as "everything is
        # checkpointed" -- that would silently drop real recovery
        # candidates. Treated as "nothing checkpointed" instead: strictly
        # safer, at the cost of a possible harmless re-persist (idempotent
        # via quality_event_id at the Parquet-compaction layer).
        return -1


def _max_seq_in_file(wal_file: Path) -> Optional[int]:
    """The highest `seq` found in wal_file, tolerating a truncated final
    line (Scenario C) the same way recover() does, but never raising on
    mid-file corruption here -- this helper is used for cleanup/resume
    bookkeeping only, not for evidence; recover() is the sole place that
    enforces the corruption policy."""
    try:
        raw = wal_file.read_bytes()
    except OSError:
        return None
    if not raw:
        return None
    text = raw.decode("utf-8", errors="strict")
    ends_with_newline = text.endswith("\n")
    lines = text.splitlines()
    highest = None
    for i, stripped in enumerate(lines):
        if not stripped:
            continue
        try:
            record = json.loads(stripped)
        except json.JSONDecodeError:
            if i == len(lines) - 1 and not ends_with_newline:
                continue
            continue  # tolerate here; recover() is authoritative for raising
        seq = record.get("seq")
        if isinstance(seq, int):
            highest = seq if highest is None else max(highest, seq)
    return highest
