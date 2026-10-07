"""F1: durable-publication evidence for raw ``.seg`` segments.

A visible ``*.seg`` is NOT evidence that the segment's rename is durable: the
directory entry lives in the page cache until a *successful* ``fsync`` of the
parent directory. ``<segment>.meta.json`` is therefore a versioned
**publication marker** (schema 2) that is created strictly AFTER that
directory fsync succeeded, and binds the segment's exact bytes
(``sha256`` + ``size_bytes``). See ``docs/F1_DURABLE_PUBLICATION.md``.

Trust rules enforced here (never relaxed):

* a missing, unparseable or mismatching marker is "no marker" -- it never
  authorises anything and it is never deleted (it is preserved);
* nothing in this module deletes or moves a ``.seg`` except the explicit,
  operator-driven :func:`quarantine_segment` (move-only);
* every fsync helper raises on failure; none retries or swallows an errno.

``boot_id`` and ``confirmed_at_utc`` are provenance only. They are never a
trust condition and wall-clock time is never durability evidence.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from .utils import logger

MARKER_SCHEMA = 2
WRITER_SCHEMA = "parquet_writer/2"
STATE_CONFIRMED = "PUBLICATION_CONFIRMED"
CONFIRMED_BY_WRITER = "writer"
CONFIRMED_BY_STARTUP = "startup_refsync"
ALLOWED_CONFIRMED_BY = frozenset({CONFIRMED_BY_WRITER, CONFIRMED_BY_STARTUP})

#: Operator override. Only the exact value "1" enables it; anything else is off.
UNVERIFIED_FS_ENV = "COLLECTOR_ALLOW_UNVERIFIED_FS"
#: Re-hash every already-reconciled segment at startup (O(dataset); off by default).
DEEP_VERIFY_ENV = "DEDUP_DEEP_VERIFY"

#: Filesystems whose journalled directory-fsync semantics the design relies on
#: (a failed journal commit is sticky: ext4 jbd2 abort / XFS log shutdown).
VERIFIED_FILESYSTEMS = frozenset({"ext4", "xfs"})

QUARANTINE_DIRNAME = "quarantine"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CHUNK = 1024 * 1024


class PublicationError(RuntimeError):
    """Publication evidence cannot be established or is inconsistent.

    Always fail-closed: callers must not treat it as "published" nor as "not
    published". ``segment_dedup`` re-raises it as ``DedupStateError``."""


class PublicationState(str, Enum):
    """Derived from disk, never from process memory (see the design doc)."""

    UNPUBLISHED = "UNPUBLISHED"                      # only X.seg.tmp (maybe partial)
    TMP_DURABLE = "TMP_DURABLE"                      # X.seg.tmp fsynced (same on disk as UNPUBLISHED)
    RENAMED_UNCONFIRMED = "RENAMED_UNCONFIRMED"      # X.seg visible, no valid marker
    PUBLICATION_CONFIRMED = "PUBLICATION_CONFIRMED"  # X.seg + valid marker, size matches
    DEDUP_AUTHORITY = "DEDUP_AUTHORITY"              # confirmed AND index evidence equals marker
    CORRUPT = "CORRUPT"                              # valid marker but the file disagrees with it
    QUARANTINED = "QUARANTINED"                      # moved by quarantine_segment() only
    LEGACY_UNKNOWN = "LEGACY_UNKNOWN"                # pre-F1 segment with a v1 meta hint, no marker


class MarkerStatus(str, Enum):
    VALID = "valid"
    INVALID = "invalid"
    ABSENT = "absent"


@dataclass(frozen=True)
class MarkerResult:
    status: MarkerStatus
    #: Parsed marker (VALID), or the parsed v1 hint (ABSENT with ``v1`` True).
    data: Optional[Dict[str, Any]] = None
    reason: str = ""
    #: True when the file is a pre-F1 (v1) ``meta.json``: a legitimate hint, not corruption.
    v1: bool = False

    @property
    def valid(self) -> bool:
        return self.status is MarkerStatus.VALID

    @property
    def publication(self) -> Dict[str, Any]:
        assert self.data is not None and self.valid
        return self.data["publication"]


# --------------------------------------------------------------------------
# paths / low-level primitives
# --------------------------------------------------------------------------

def marker_path(seg_path: Path | str) -> Path:
    return Path(str(seg_path) + ".meta.json")


def fsync_file(path: Path | str) -> None:
    """fsync ``path`` through a fresh read-only fd. Raises on any failure."""
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fsync_dir(path: Path | str) -> None:
    """fsync a directory (makes completed renames/creates in it durable). Raises."""
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def sha256_file(path: Path | str) -> Tuple[str, int]:
    """``(hex digest, size)`` of exactly the bytes read from ``path``."""
    digest, size = hashlib.sha256(), 0
    with open(str(path), "rb") as handle:
        while True:
            chunk = handle.read(_CHUNK)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_boot_id() -> Optional[str]:
    try:
        text = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError):
        return None
    return text or None


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------
# marker encode / write / read
# --------------------------------------------------------------------------

def build_marker(seg_name: str, *, record_count: int, first_ts: Optional[int], last_ts: Optional[int],
                 sha256: str, size_bytes: int, confirmed_by: str, legacy: bool,
                 boot_id: Optional[str] = None, confirmed_at_utc: Optional[str] = None) -> Dict[str, Any]:
    return {
        "first_record_ts": first_ts,
        "last_record_ts": last_ts,
        "publication": {
            "boot_id": boot_id,
            "confirmed_at_utc": confirmed_at_utc or _utc_now(),
            "confirmed_by": confirmed_by,
            "legacy": bool(legacy),
            "schema": MARKER_SCHEMA,
            "segment": seg_name,
            "sha256": sha256,
            "size_bytes": size_bytes,
            "state": STATE_CONFIRMED,
            "writer_schema": WRITER_SCHEMA,
        },
        "record_count": record_count,
    }


def encode_marker(marker: Dict[str, Any]) -> bytes:
    """Deterministic encoding: sorted keys, compact separators, one trailing newline."""
    return (json.dumps(marker, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def write_marker_atomic(seg_path: Path | str, *, record_count: int, first_ts: Optional[int],
                        last_ts: Optional[int], sha256: str, size_bytes: int, confirmed_by: str,
                        legacy: bool, fsync_dir_after: bool = True) -> Path:
    """Write ``<seg>.meta.json`` as a publication marker. RAISES on any failure.

    Order (the only safe one): write tmp -> fsync(tmp) -> os.replace -> fsync(dir).
    ``fsync_dir_after=False`` is for the startup batch, which issues one
    directory fsync for the whole batch itself. The caller must have already
    made the segment's own rename durable (successful ``fsync(dir)``) BEFORE
    calling this: that ordering is what lets marker visibility imply it.
    """
    if confirmed_by not in ALLOWED_CONFIRMED_BY:
        raise ValueError(f"confirmed_by must be one of {sorted(ALLOWED_CONFIRMED_BY)}, got {confirmed_by!r}")
    seg = Path(seg_path)
    final = marker_path(seg)
    tmp = Path(str(final) + ".tmp")
    payload = encode_marker(build_marker(
        seg.name, record_count=record_count, first_ts=first_ts, last_ts=last_ts, sha256=sha256,
        size_bytes=size_bytes, confirmed_by=confirmed_by, legacy=legacy, boot_id=read_boot_id()))
    try:
        fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, final)
        if fsync_dir_after:
            fsync_dir(final.parent)
    except BaseException:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass  # _recover_orphans discards stray *.meta.json.tmp on restart
        raise
    return final


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def read_marker(seg_path: Path | str) -> MarkerResult:
    """Classify ``<seg>.meta.json``: VALID / INVALID / ABSENT. Never raises.

    * no file -> ABSENT
    * a JSON object with no ``publication`` key (pre-F1 hint) -> ABSENT, ``v1=True``
    * anything else that is not a fully valid schema-2 marker for THIS segment -> INVALID

    VALID does not check the segment's actual size/bytes: that is
    :func:`classify_segment`'s / the coordinator's job (they hold the file).
    """
    seg = Path(seg_path)
    path = marker_path(seg)
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return MarkerResult(MarkerStatus.ABSENT, reason="no marker file")
    except OSError as exc:
        return MarkerResult(MarkerStatus.INVALID, reason=f"unreadable: {type(exc).__name__}: {exc}")
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        return MarkerResult(MarkerStatus.INVALID, reason=f"not valid JSON: {type(exc).__name__}")
    if not isinstance(obj, dict):
        return MarkerResult(MarkerStatus.INVALID, reason="top level is not a JSON object")
    if "publication" not in obj:
        return MarkerResult(MarkerStatus.ABSENT, data=obj, reason="pre-F1 (v1) meta hint", v1=True)
    pub = obj["publication"]
    if not isinstance(pub, dict):
        return MarkerResult(MarkerStatus.INVALID, reason="publication is not an object")
    problems: List[str] = []
    if not _is_int(pub.get("schema")) or pub.get("schema") != MARKER_SCHEMA:
        problems.append("schema != 2")
    if pub.get("state") != STATE_CONFIRMED:
        problems.append("state != PUBLICATION_CONFIRMED")
    if pub.get("segment") != seg.name:
        problems.append("segment name does not match")
    if not _is_int(pub.get("size_bytes")) or pub["size_bytes"] < 0:
        problems.append("size_bytes invalid")
    sha = pub.get("sha256")
    if not isinstance(sha, str) or not _SHA256.match(sha):
        problems.append("sha256 invalid")
    if pub.get("confirmed_by") not in ALLOWED_CONFIRMED_BY:
        problems.append("confirmed_by not allowed")
    if not isinstance(pub.get("legacy"), bool):
        problems.append("legacy not a bool")
    if not _is_int(obj.get("record_count")) or obj["record_count"] < 0:
        problems.append("record_count invalid")
    for key in ("first_record_ts", "last_record_ts"):
        if obj.get(key) is not None and not _is_int(obj.get(key)):
            problems.append(f"{key} invalid")
    if problems:
        return MarkerResult(MarkerStatus.INVALID, reason="; ".join(problems))
    return MarkerResult(MarkerStatus.VALID, data=obj)


def classify_segment(seg_path: Path | str, marker: MarkerResult) -> PublicationState:
    """State of a visible ``.seg`` given its marker result (cheap: one stat)."""
    if marker.valid:
        try:
            size = Path(seg_path).stat().st_size
        except OSError:
            return PublicationState.CORRUPT
        return (PublicationState.PUBLICATION_CONFIRMED if size == marker.publication["size_bytes"]
                else PublicationState.CORRUPT)
    if marker.v1:
        return PublicationState.LEGACY_UNKNOWN
    return PublicationState.RENAMED_UNCONFIRMED


# --------------------------------------------------------------------------
# preserve-never-delete helpers
# --------------------------------------------------------------------------

def preserve_invalid(path: Path) -> Path:
    """Rename an untrusted marker to ``<name>.invalid.<n>``. Never deletes."""
    n = 0
    while True:
        target = Path(f"{path}.invalid.{n}")
        if not target.exists():
            os.replace(path, target)
            return target
        n += 1


def orphan_marker(path: Path) -> Path:
    """Rename a marker whose segment is missing to ``<name>.orphan.<utc>``. Never deletes."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target, n = Path(f"{path}.orphan.{stamp}"), 0
    while target.exists():
        n += 1
        target = Path(f"{path}.orphan.{stamp}.{n}")
    os.replace(path, target)
    return target


# --------------------------------------------------------------------------
# filesystem guard
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class FsVerdict:
    fstype: Optional[str]
    mount_point: Optional[str]
    options: str
    verified: bool        # a filesystem whose directory-fsync semantics the design relies on
    overridden: bool      # the operator explicitly accepted an unverified filesystem
    reason: str

    @property
    def allows_promotion(self) -> bool:
        return self.verified or self.overridden


def _unescape_mountinfo(field: str) -> str:
    return re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), field)


def parse_mountinfo(text: str, path: Path | str) -> Tuple[Optional[str], Optional[str], str]:
    """``(fstype, mount_point, options)`` for the mount containing ``path``, or Nones."""
    target = os.path.realpath(str(path))
    best: Optional[Tuple[str, str, str]] = None
    for line in text.splitlines():
        left, sep, right = line.partition(" - ")
        if not sep:
            continue
        lf, rf = left.split(), right.split()
        if len(lf) < 6 or len(rf) < 3:
            continue
        mount_point = _unescape_mountinfo(lf[4])
        prefix = mount_point.rstrip("/")
        if target == mount_point or target.startswith(prefix + "/") or mount_point == "/":
            # >= : a later line is an over-mount of an earlier one and wins.
            if best is None or len(mount_point) >= len(best[1]):
                best = (rf[0], mount_point, lf[5] + "," + rf[2])
    return best if best is not None else (None, None, "")


def fs_guard(path: Path | str, *, mountinfo: Optional[str] = None,
             environ: Optional[Dict[str, str]] = None) -> FsVerdict:
    """Is ``path`` on a filesystem whose fsync semantics the F1 design relies on?

    ext4/xfs -> verified. Anything else (NFS, tmpfs, overlayfs, fuse, unknown, or
    ``/proc/self/mountinfo`` unreadable) -> NOT verified: fail closed for any
    promotion of unmarked segments unless the operator exports
    ``COLLECTOR_ALLOW_UNVERIFIED_FS=1``. The default is never fail-open.
    ``nobarrier`` is treated as unverified (it disables the device flush).
    """
    env = os.environ if environ is None else environ
    override = env.get(UNVERIFIED_FS_ENV) == "1"
    if mountinfo is None:
        try:
            mountinfo = Path("/proc/self/mountinfo").read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            return FsVerdict(None, None, "", False, override,
                             f"cannot read /proc/self/mountinfo ({type(exc).__name__}): filesystem unknown")
    fstype, mount_point, options = parse_mountinfo(mountinfo, path)
    if fstype is None:
        return FsVerdict(None, None, "", False, override, "no mount entry found for the path: filesystem unknown")
    if fstype not in VERIFIED_FILESYSTEMS:
        return FsVerdict(fstype, mount_point, options, False, override,
                         f"filesystem {fstype!r} is not one of {sorted(VERIFIED_FILESYSTEMS)}")
    if "nobarrier" in options.split(","):
        return FsVerdict(fstype, mount_point, options, False, override,
                         "mounted with nobarrier: device flush is disabled")
    return FsVerdict(fstype, mount_point, options, True, override, "verified")


# --------------------------------------------------------------------------
# startup confirmation of unmarked segments
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class UnmarkedSegment:
    path: Path
    marker: MarkerResult


@dataclass(frozen=True)
class ConfirmedSegment:
    path: Path
    sha256: str
    size_bytes: int
    record_count: int


def read_segment_table(data: bytes, label: str) -> pa.Table:
    """Decode a parquet segment from ``data`` exactly once and return the table.

    Raises :class:`PublicationError` when it cannot be read or when its footer
    row count disagrees with what was actually decoded."""
    try:
        footer_rows = pq.ParquetFile(io.BytesIO(data)).metadata.num_rows
        table = pq.read_table(io.BytesIO(data))
    except Exception as exc:  # noqa: BLE001 - any parse failure means the segment is not trustworthy
        raise PublicationError(f"segment {label} is not readable parquet: {exc!r}") from exc
    if footer_rows != table.num_rows:
        raise PublicationError(f"segment {label}: footer says {footer_rows} rows but {table.num_rows} decoded")
    return table


def inspect_segment_bytes(data: bytes, label: str) -> int:
    """Footer row count of a fully-decoded, consistent segment (see read_segment_table)."""
    return read_segment_table(data, label).num_rows


def confirm_unmarked(unmarked: Sequence[UnmarkedSegment], stream_dir: Path, *,
                     verdict: FsVerdict) -> List[ConfirmedSegment]:
    """Startup refsync: turn RENAMED_UNCONFIRMED / LEGACY segments into
    PUBLICATION_CONFIRMED, or raise. Order (each step raises on failure; nothing
    is ever written before step 2 has succeeded):

      0. filesystem guard
      1. fsync(file) for each segment
      2. fsync(directory) ONCE  -> every rename of ``X.seg`` is now durable
      3. read + sha256 + size + full parquet parse + footer rows, ALL segments, no writes
      4. write marker v2 for each (tmp, fsync, replace) -- no per-marker dir fsync
      5. fsync(directory) ONCE  -> markers durable

    Why this is sound: ``X.seg`` is visible, so its rename belongs to some
    journal transaction; a *successful* directory fsync after that makes it and
    every earlier transaction durable (A1: a failed commit is sticky, so a
    previously failed commit cannot be laundered into success). Data durability
    is NOT proven here (a re-read hits the page cache); it rests on A3
    (``fsync(tmp)`` strictly preceded the rename in every writer version).
    """
    if not unmarked:
        return []
    if not verdict.allows_promotion:
        raise PublicationError(
            f"{len(unmarked)} unmarked segment(s) in {stream_dir} cannot be confirmed: {verdict.reason} "
            f"(fs={verdict.fstype!r} mount={verdict.mount_point!r}). Refusing to treat a visible .seg as "
            f"durable. Set {UNVERIFIED_FS_ENV}=1 only after verifying the storage out of band.")
    if verdict.overridden and not verdict.verified:
        logger.error("unverified_filesystem_override_in_effect", stream_dir=str(stream_dir),
                     fstype=verdict.fstype, reason=verdict.reason)
    for item in unmarked:                                           # step 1
        try:
            fsync_file(item.path)
        except OSError as exc:
            raise PublicationError(f"fsync of segment {item.path.name} failed: {exc!r}") from exc
    try:                                                            # step 2
        fsync_dir(stream_dir)
    except OSError as exc:
        raise PublicationError(f"directory fsync of {stream_dir} failed: {exc!r}") from exc
    inspected: List[ConfirmedSegment] = []
    for item in unmarked:                                           # step 3 (read-only)
        try:
            data = item.path.read_bytes()
        except OSError as exc:
            raise PublicationError(f"cannot read segment {item.path.name}: {exc!r}") from exc
        rows = inspect_segment_bytes(data, item.path.name)
        inspected.append(ConfirmedSegment(item.path, sha256_bytes(data), len(data), rows))
    for item, done in zip(unmarked, inspected):                     # step 4
        hint = item.marker.data if item.marker.v1 else None
        first_ts = last_ts = None
        if hint is not None and hint.get("record_count") == done.record_count:
            first_ts = hint.get("first_record_ts") if _is_int(hint.get("first_record_ts")) else None
            last_ts = hint.get("last_record_ts") if _is_int(hint.get("last_record_ts")) else None
        legacy = item.marker.status is not MarkerStatus.INVALID
        try:
            write_marker_atomic(done.path, record_count=done.record_count, first_ts=first_ts, last_ts=last_ts,
                                sha256=done.sha256, size_bytes=done.size_bytes,
                                confirmed_by=CONFIRMED_BY_STARTUP, legacy=legacy, fsync_dir_after=False)
        except OSError as exc:
            raise PublicationError(f"cannot write marker for {done.path.name}: {exc!r}") from exc
    try:                                                            # step 5
        fsync_dir(stream_dir)
    except OSError as exc:
        raise PublicationError(f"directory fsync of {stream_dir} after markers failed: {exc!r}") from exc
    return inspected


# --------------------------------------------------------------------------
# quarantine (operator-driven, move-only; never automatic)
# --------------------------------------------------------------------------

def quarantine_segment(seg_path: Path | str, reason: str, operator: str) -> Path:
    """Move a ``.seg`` (and its marker, if any) into ``<stream_dir>/quarantine/``.

    NEVER called automatically and never deletes. The moved file is invisible
    to every reader (``iter_segments`` skips sub-directories) and is not dedup
    authority: the next startup finds no segment, so any index row for it is
    detected as divergence and the index is rebuilt. Provenance is written to
    ``<name>.quarantine.json`` next to it. Refuses to overwrite.
    """
    seg = Path(seg_path)
    if not seg.is_file():
        raise PublicationError(f"cannot quarantine {seg}: not a file")
    qdir = seg.parent / QUARANTINE_DIRNAME
    qdir.mkdir(exist_ok=True)
    dest = qdir / seg.name
    note = qdir / (seg.name + ".quarantine.json")
    if dest.exists() or note.exists():
        raise PublicationError(f"quarantine target already exists for {seg.name}; refusing to overwrite")
    try:
        sha, size = sha256_file(seg)
    except OSError:
        sha, size = None, None
    record = {"original_path": str(seg), "reason": reason, "operator": operator,
              "quarantined_at_utc": _utc_now(), "sha256": sha, "size_bytes": size}
    marker = marker_path(seg)
    note.write_bytes((json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8"))
    fsync_file(note)
    os.replace(seg, dest)
    if marker.exists():
        os.replace(marker, qdir / marker.name)
    fsync_dir(qdir)
    fsync_dir(seg.parent)
    return dest
