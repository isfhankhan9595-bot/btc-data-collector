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

These statements are scoped to the storage contract that ``os.fsync()``
returns only once the data is durable on the underlying device. The test
suite injects exceptions at syscall boundaries; it does not demonstrate
real power-loss behavior.
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

============================================================================
DELIVERY: AT-LEAST-ONCE, NO AUTOMATIC DEDUPLICATION
============================================================================

Recovery replays every record above the durable checkpoint. A crash (or a
checkpoint-write failure / latch) after a segment was published but before
the checkpoint covered it makes the next start write that event again, so
the same ``quality_event_id`` can appear in more than one published row.
``quality_event_id`` identifies an event and lets a consumer recognise
matching replay duplicates; NO component in this repository removes or
reconciles them. Treat delivery as at-least-once, never exactly-once.
A downstream reader that needs one row per event can key on
``(exchange, stream, quality_event_id)`` with null ids excluded (rows
written without a WAL id have none). That is a suggestion for consumers,
not existing collector behavior, and it is suitable only when the source
/ WAL namespace is unambiguous: the id is WAL-directory-unique and the row
carries no source or WAL-namespace column, so a reader combining several
WAL namespaces may also need a stable source identity from outside the row.
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Optional

CHECKPOINT_FILENAME = "checkpoint.json"
DEFAULT_MAX_TOTAL_BYTES = 100_000_000  # 100 MB; configurable before startup
ENV_MAX_TOTAL_BYTES = "COLLECTOR_QUALITY_WAL_MAX_TOTAL_BYTES"


class QualityWALCapacityExceeded(OSError):
    """An append was refused before writing because a safe aggregate size could not be proved.

    This exception deliberately has no quality_event_id: the rejected event
    has no WAL record and must not be counted as WAL-backed by a caller.
    """

    def __init__(self, *, reason: str, max_total_bytes: int,
                 current_bytes: Optional[int], requested_bytes: Optional[int],
                 newly_exhausted: bool = False):
        super().__init__(
            f"quality WAL append refused: {reason}; current={current_bytes}, "
            f"requested={requested_bytes}, limit={max_total_bytes}")
        self.reason = reason
        self.max_total_bytes = max_total_bytes
        self.current_bytes = current_bytes
        self.requested_bytes = requested_bytes
        self.newly_exhausted = newly_exhausted


def _fsync_directory(path: Path) -> None:
    """Make directory-entry changes durable after create/replace/unlink.

    The file's own fsync does not guarantee that its name survives power loss.
    If the platform/filesystem cannot sync this directory, fail closed rather
    than claiming the checkpoint or new WAL name is durable.
    """
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(str(path), flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


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
                max_total_bytes: Optional[int] = None,
                process_start_marker: Optional[str] = None, start_seq: int = 0):
        self.wal_dir = Path(wal_dir)
        self.wal_dir.mkdir(parents=True, exist_ok=True)
        self.max_bytes = max_bytes
        raw_cap = os.environ.get(ENV_MAX_TOTAL_BYTES) if max_total_bytes is None else None
        if max_total_bytes is None:
            if raw_cap is None:
                max_total_bytes = DEFAULT_MAX_TOTAL_BYTES
            else:
                try:
                    max_total_bytes = int(raw_cap, 10)
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"{ENV_MAX_TOTAL_BYTES} must be a positive integer") from exc
        if isinstance(max_total_bytes, bool) or not isinstance(max_total_bytes, int) or max_total_bytes <= 0:
            raise ValueError(f"max_total_bytes must be a positive integer, got {max_total_bytes!r}")
        self.max_total_bytes = max_total_bytes
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
        try:
            _fsync_directory(self.wal_dir)
        except Exception:
            self._handle.close()
            raise
        self._write_failed = False
        self._closed = False
        self._capacity_exhausted = False
        self._capacity_reason = None
        self._capacity_current_bytes = None
        # Preserve existing evidence and refuse more WAL appends if startup
        # finds the namespace over budget or cannot establish its size.
        try:
            current = self._total_wal_bytes()
        except OSError:
            self._latch_capacity("aggregate_size_unavailable", None)
        else:
            if current >= self.max_total_bytes:
                self._latch_capacity("existing_wal_at_or_over_capacity", current)

    @classmethod
    def cleanup_fully_checkpointed_files(cls, wal_dir: Any) -> None:
        """Clean stale covered files only after the caller's full recovery pass succeeded.

        Destructive startup cleanup is intentionally separate from __init__.
        Runners call this only after recover() has parsed the WAL namespace
        without corruption. Each file is still revalidated strictly here; a
        malformed row, incomplete tail, read error or invalid seq makes that
        file ineligible for deletion. The checkpoint file is already durable,
        so a directory-sync failure after unlink may only cause covered files
        to reappear after a crash, which is safe and retried on a later startup.
        """
        directory = Path(wal_dir)
        checkpointed_seq = _read_checkpointed_seq(directory)
        deleted = False
        for wal_file in sorted(directory.glob("*.wal")):
            max_seq = _safe_max_seq_in_file(wal_file)
            if max_seq is not None and max_seq <= checkpointed_seq:
                try:
                    wal_file.unlink(missing_ok=True)
                    deleted = True
                except OSError:
                    continue
        if deleted:
            try:
                _fsync_directory(directory)
            except OSError:
                pass

    def _total_wal_bytes(self) -> int:
        """Actual aggregate bytes across every .wal file in this namespace."""
        total = 0
        for wal_file in self.wal_dir.glob("*.wal"):
            try:
                total += wal_file.stat().st_size
            except FileNotFoundError:
                # A concurrent checkpoint cleanup may have removed this
                # directory entry between glob and stat. It contributes zero.
                continue
        return total

    def _latch_capacity(self, reason: str, current_bytes: Optional[int]) -> bool:
        first = not self._capacity_exhausted
        self._capacity_exhausted = True
        if first:
            self._capacity_reason = reason
            self._capacity_current_bytes = current_bytes
        return first

    def _capacity_error(self, *, requested_bytes: Optional[int] = None,
                        current_bytes: Optional[int] = None,
                        newly_exhausted: bool = False) -> QualityWALCapacityExceeded:
        return QualityWALCapacityExceeded(
            reason=self._capacity_reason or "capacity_latched",
            max_total_bytes=self.max_total_bytes,
            current_bytes=(self._capacity_current_bytes if current_bytes is None else current_bytes),
            requested_bytes=requested_bytes,
            newly_exhausted=newly_exhausted)

    @property
    def capacity_exhausted(self) -> bool:
        return self._capacity_exhausted

    @property
    def capacity_reason(self) -> Optional[str]:
        return self._capacity_reason

    def _new_wal_path(self) -> Path:
        stamp = time.strftime("%Y-%m-%d-%H%M%S", time.gmtime())
        return self.wal_dir / f"{stamp}-{self.process_start_marker}-{self._seq:012d}.wal"

    def append(self, event: dict) -> str:
        """Durably append one quality event, subject to an aggregate hard cap.

        Capacity is checked against the on-disk byte sizes of ALL .wal files
        and the exact UTF-8 line size (including its newline). A rejected
        record is not written and receives no WAL event ID. The capacity latch
        remains closed until restart; runners use their synchronous,
        force-published Parquet fallback for events rejected after the cap.

        On a write/flush/fsync failure, re-raises with the assigned event ID
        because bytes may already be present.
        """
        with self._lock:
            if self._closed:
                raise OSError("quality WAL is closed")
            if self._write_failed:
                raise OSError("quality WAL is in a write-failed state; restart/recovery is required")
            if self._capacity_exhausted:
                raise self._capacity_error()

            next_seq = self._seq + 1
            event_id = f"{self.process_start_marker}-{next_seq}"
            # WAL-owned fields must not be overridable by caller data.
            record = {**event, "quality_event_id": event_id, "seq": next_seq}
            line = json.dumps(record, default=str)
            encoded_size = len((line + "\n").encode("utf-8"))

            try:
                current_bytes = self._total_wal_bytes()
            except OSError as exc:
                first = self._latch_capacity("aggregate_size_unavailable", None)
                raise self._capacity_error(
                    requested_bytes=encoded_size, current_bytes=None,
                    newly_exhausted=first) from exc

            if encoded_size > self.max_total_bytes or current_bytes + encoded_size > self.max_total_bytes:
                first = self._latch_capacity("aggregate_budget_exhausted", current_bytes)
                raise self._capacity_error(
                    requested_bytes=encoded_size, current_bytes=current_bytes,
                    newly_exhausted=first)

            # Reserve the sequence only after admission, but before writing:
            # uncertain I/O may have left a partial record, so the same process
            # must not append a valid line behind that uncertain tail.
            self._seq = next_seq
            try:
                self._handle.write(line + "\n")
                self._handle.flush()
                os.fsync(self._handle.fileno())
            except OSError as exc:
                self._write_failed = True
                # Bytes may already be present after write/flush/fsync failure.
                exc.quality_event_id = event_id
                raise
            return event_id
    @property
    def write_failed(self) -> bool:
        return self._write_failed

    @property
    def checkpointed_seq(self) -> int:
        """Highest seq covered by the DURABLE checkpoint (-1 if none). In-memory state
        advances only after the checkpoint file's atomic replace succeeded, so this never
        claims more than is on disk. Read-only: used by runners to tell whether an event
        is already covered (and therefore will not be replayed)."""
        return self._checkpointed_seq

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
            # Sync the parent directory as well as the temporary file: its
            # fsync alone does not make the rename durable after power loss.
            # On failure, leave in-memory state unchanged and delete no WAL.
            _fsync_directory(self.wal_dir)
            self._checkpointed_seq = up_to_seq
            self._delete_fully_checkpointed_files()

    def _delete_fully_checkpointed_files(self) -> None:
        deleted = False
        for wal_file in sorted(self.wal_dir.glob("*.wal")):
            if wal_file == self._active_path:
                continue  # never delete the file currently being appended to
            max_seq = _safe_max_seq_in_file(wal_file)
            # Unknown/malformed/truncated contents are evidence, not a basis
            # for cleanup. Only a fully parseable file whose every seq is
            # at/below the durable checkpoint may be removed.
            if max_seq is not None and max_seq <= self._checkpointed_seq:
                try:
                    wal_file.unlink(missing_ok=True)
                    deleted = True
                except OSError:
                    # The checkpoint is already durable, so a file we
                    # could not delete is harmless: recover() filters by
                    # seq, so it contributes nothing pending, and the
                    # next checkpoint() retries the deletion.
                    continue
        if deleted:
            try:
                _fsync_directory(self.wal_dir)
            except OSError:
                # The checkpoint is already durable. Cleanup sync failure
                # can only cause covered files to return after a crash.
                pass

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
                try:
                    _fsync_directory(self.wal_dir)
                except Exception:
                    new_handle.close()
                    raise
                old_handle = self._handle
                self._handle = new_handle
                self._active_path = new_path
                old_handle.close()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._handle.flush()
                os.fsync(self._handle.fileno())
            except OSError:
                self._write_failed = True
                raise
            finally:
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
        # safer, at the cost of possible duplicate rows: a re-persisted
        # event carries the same quality_event_id, which lets a downstream
        # consumer identify the duplicate. Nothing in this repository
        # deduplicates quality events automatically (replay is
        # at-least-once; reconciliation is a downstream responsibility).
        return -1


def _safe_max_seq_in_file(wal_file: Path) -> Optional[int]:
    """Return the maximum seq only when every line is valid and complete.

    Used for destructive cleanup, so it is intentionally stricter than the
    resume-sequence helper below. Stream one line at a time: a large WAL must
    not be read into RAM just to decide whether it can be deleted. A corrupt
    or unterminated tail means the file cannot be proven fully covered and
    must be retained. Empty files contain no event evidence and return -1.
    """
    highest = -1
    try:
        with wal_file.open("rb") as handle:
            for raw_line in handle:
                if not raw_line.endswith(b"\\n"):
                    return None
                try:
                    line = raw_line.decode("utf-8", errors="strict").strip()
                except UnicodeDecodeError:
                    return None
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    return None
                if not isinstance(record, dict):
                    return None
                seq = record.get("seq")
                if isinstance(seq, bool) or not isinstance(seq, int):
                    return None
                highest = max(highest, seq)
    except OSError:
        return None
    return highest

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
