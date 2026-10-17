
"""Read-only hashing of discovered raw source segments.

This is the next source-catalog evidence stage after bounded marker validation:
it hashes the opened segment and compares observed size / SHA-256 with schema-2
marker declarations only when marker validation produced VALID_V2 evidence.

A successful result means only that a stable file object was fully read and
hashed. It does NOT establish valid Parquet, row-count/timestamp agreement,
venue/instrument correctness, or source trust. This module never changes the
discovery record or filesystem.
"""
from __future__ import annotations

import errno
import hashlib
import os
import stat as _stat
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Sequence, Tuple, Union

from .marker_validation import (
    MarkerValidation,
    MarkerValidationStatus,
    _SAFE_FLAGS,
    _errno_name,
    _stable_identity,
)
from .source_catalog import (
    RAW_DIRNAME,
    DiscoveryResult,
    MarkerPresence,
    SourceRecord,
    TrustState,
    _resolve_root,
)
from .storage_layout import SegmentKind, parse_segment_name

SEGMENT_VERIFICATION_SCHEMA_VERSION = 1
READ_CHUNK_BYTES = 1024 * 1024


class SegmentVerificationStatus(str, Enum):
    """Status of observing and hashing bytes, not an overall trust state."""
    NOT_CHECKED = "NOT_CHECKED"
    HASHED_STABLE = "HASHED_STABLE"
    UNKNOWN_IO = "UNKNOWN_IO"


@dataclass(frozen=True)
class SegmentByteVerification:
    relative_path: str
    status: SegmentVerificationStatus
    reasons: Tuple[str, ...]
    observed_size: Optional[int] = None
    observed_sha256: Optional[str] = None
    bytes_hashed: int = 0
    discovery_size_matches: Optional[bool] = None
    declared_size_matches: Optional[bool] = None
    declared_sha256_matches: Optional[bool] = None
    verification_schema_version: int = SEGMENT_VERIFICATION_SCHEMA_VERSION


def _not_checked(rec: SourceRecord, reason: str) -> SegmentByteVerification:
    return SegmentByteVerification(
        relative_path=rec.relative_path,
        status=SegmentVerificationStatus.NOT_CHECKED,
        reasons=(reason,),
    )


def _unknown(
    rec: SourceRecord,
    *reasons: str,
    observed_size: Optional[int] = None,
    bytes_hashed: int = 0,
) -> SegmentByteVerification:
    return SegmentByteVerification(
        relative_path=rec.relative_path,
        status=SegmentVerificationStatus.UNKNOWN_IO,
        reasons=tuple(reasons),
        observed_size=observed_size,
        observed_sha256=None,
        bytes_hashed=bytes_hashed,
    )


def _split_record_path(rec: SourceRecord) -> Optional[Tuple[str, str]]:
    """Return (stream, filename) only for one canonical raw source path."""
    parts = rec.relative_path.split("/")
    if len(parts) != 3 or parts[0] != RAW_DIRNAME or parts[1] != rec.stream:
        return None
    filename = parts[2]
    if not filename or filename in (".", "..") or chr(0) in filename:
        return None
    parsed = parse_segment_name(filename)
    if parsed is None or parsed[3] is not rec.kind:
        return None
    return parts[1], filename


def _marker_evidence(rec: SourceRecord, marker: MarkerValidation):
    """Only validated schema-2 marker content is a usable declaration."""
    if marker.status is not MarkerValidationStatus.VALID_V2:
        return None
    if marker.marker_presence is not MarkerPresence.REGULAR_FILE:
        raise ValueError(
            f"VALID_V2 marker for {rec.relative_path!r} has inconsistent marker presence"
        )
    if "MARKER_SCHEMA_2_VALID" not in marker.reasons:
        raise ValueError(
            f"VALID_V2 marker for {rec.relative_path!r} lacks schema-validation evidence"
        )
    evidence = marker.evidence
    if evidence is None:
        raise ValueError(
            f"VALID_V2 marker for {rec.relative_path!r} is missing declared evidence"
        )
    if evidence.schema != 2:
        raise ValueError(
            f"VALID_V2 marker for {rec.relative_path!r} declares a non-v2 schema"
        )
    split = _split_record_path(rec)
    if split is None or rec.kind is not SegmentKind.SEGMENT or evidence.segment != split[1]:
        raise ValueError(
            f"VALID_V2 marker evidence does not identify {rec.relative_path!r}"
        )
    return evidence


def _open_dir(
    name: str, parent_fd: int, flags: int, open_fds: list[int]
) -> Union[int, Tuple[str, ...]]:
    """Open one path component without following symlinks."""
    try:
        fd = os.open(name, flags, dir_fd=parent_fd)
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.ENOTDIR):
            return ("PATH_COMPONENT_NOT_PLAIN_DIRECTORY",)
        if isinstance(exc, FileNotFoundError):
            return ("PATH_COMPONENT_MISSING_AT_OPEN",)
        return ("PATH_COMPONENT_OPEN_FAILED", f"ERRNO_{_errno_name(exc)}")
    open_fds.append(fd)
    return fd


def _hash_open_segment(
    stream_fd: int,
    filename: str,
    rec: SourceRecord,
    evidence,
    file_flags: int,
) -> SegmentByteVerification:
    """Hash one descriptor, checking that its object and pathname stayed stable."""
    try:
        fd = os.open(filename, file_flags, dir_fd=stream_fd)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            return _unknown(rec, "SEGMENT_SYMLINK_AT_OPEN")
        if isinstance(exc, FileNotFoundError):
            return _unknown(rec, "SEGMENT_MISSING_AT_OPEN")
        if exc.errno == errno.ENOTDIR:
            return _unknown(rec, "SEGMENT_NOT_REGULAR_AT_OPEN")
        return _unknown(rec, "SEGMENT_OPEN_FAILED", f"ERRNO_{_errno_name(exc)}")

    total = 0
    try:
        try:
            first = os.fstat(fd)
        except OSError as exc:
            return _unknown(rec, "SEGMENT_FSTAT_FAILED", f"ERRNO_{_errno_name(exc)}")
        if not _stat.S_ISREG(first.st_mode):
            return _unknown(rec, "SEGMENT_NOT_REGULAR_AT_OPEN", observed_size=first.st_size)
        if first.st_size < 0:
            return _unknown(rec, "SEGMENT_SIZE_INVALID_AT_OPEN")

        digest = hashlib.sha256()
        remaining = first.st_size
        while remaining:
            try:
                chunk = os.read(fd, min(READ_CHUNK_BYTES, remaining))
            except OSError as exc:
                return _unknown(
                    rec, "SEGMENT_READ_FAILED", f"ERRNO_{_errno_name(exc)}",
                    observed_size=first.st_size, bytes_hashed=total,
                )
            if not chunk:
                return _unknown(
                    rec, "SEGMENT_SHORT_READ", observed_size=first.st_size, bytes_hashed=total
                )
            digest.update(chunk)
            total += len(chunk)
            remaining -= len(chunk)

        # At most one extra byte is read to detect growth. The loop above is tied
        # to the initially observed size, so a concurrent appender cannot make it run forever.
        try:
            extra = os.read(fd, 1)
            second = os.fstat(fd)
            path_now = os.stat(filename, dir_fd=stream_fd, follow_symlinks=False)
        except FileNotFoundError:
            return _unknown(
                rec, "SEGMENT_REPLACED_DURING_READ",
                observed_size=first.st_size, bytes_hashed=total,
            )
        except OSError as exc:
            return _unknown(
                rec, "SEGMENT_STABILITY_CHECK_FAILED", f"ERRNO_{_errno_name(exc)}",
                observed_size=first.st_size, bytes_hashed=total,
            )

        if extra:
            return _unknown(
                rec, "SEGMENT_GREW_DURING_READ", observed_size=first.st_size, bytes_hashed=total
            )
        if (
            not _stat.S_ISREG(path_now.st_mode)
            or (path_now.st_dev, path_now.st_ino) != (first.st_dev, first.st_ino)
        ):
            return _unknown(
                rec, "SEGMENT_REPLACED_DURING_READ",
                observed_size=first.st_size, bytes_hashed=total,
            )
        if (
            _stable_identity(first) != _stable_identity(second)
            or total != first.st_size
            or total != second.st_size
        ):
            return _unknown(
                rec, "SEGMENT_CHANGED_DURING_READ",
                observed_size=first.st_size, bytes_hashed=total,
            )

        actual_digest = digest.hexdigest()
        catalog_match = rec.size_bytes == first.st_size if rec.size_bytes is not None else None
        declared_size_match = first.st_size == evidence.size_bytes if evidence is not None else None
        declared_hash_match = (
            actual_digest.lower() == evidence.sha256.lower() if evidence is not None else None
        )
        reasons = ["SEGMENT_BYTES_STABLE_AND_HASHED"]
        if catalog_match is False:
            reasons.append("DISCOVERY_SIZE_CHANGED")
        if declared_size_match is False:
            reasons.append("DECLARED_SIZE_MISMATCH")
        if declared_hash_match is False:
            reasons.append("DECLARED_SHA256_MISMATCH")
        return SegmentByteVerification(
            relative_path=rec.relative_path,
            status=SegmentVerificationStatus.HASHED_STABLE,
            reasons=tuple(reasons),
            observed_size=first.st_size,
            observed_sha256=actual_digest,
            bytes_hashed=total,
            discovery_size_matches=catalog_match,
            declared_size_matches=declared_size_match,
            declared_sha256_matches=declared_hash_match,
        )
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def _skip_record(rec: SourceRecord) -> Optional[SegmentByteVerification]:
    """Do not follow or judge sources whose path state was not safely established."""
    if rec.trust_state is TrustState.INVALID:
        return _not_checked(rec, "SOURCE_STATE_INVALID")
    if rec.trust_state is TrustState.AMBIGUOUS:
        return _not_checked(rec, "SOURCE_STATE_AMBIGUOUS")
    if "ENTRY_METADATA_FAILED" in rec.reasons:
        return _not_checked(rec, "SOURCE_ENTRY_METADATA_FAILED")
    if "STAT_FAILED" in rec.reasons or rec.size_bytes is None:
        return _not_checked(rec, "SOURCE_SIZE_UNAVAILABLE")
    if _split_record_path(rec) is None:
        return _not_checked(rec, "SOURCE_PATH_UNUSABLE")
    return None


def verify_segment_bytes(
    data_root: Union[str, os.PathLike[str]],
    discovery: DiscoveryResult,
    marker_validations: Sequence[MarkerValidation],
) -> Tuple[SegmentByteVerification, ...]:
    """Hash source bytes without writes, recovery, or trust-state promotion.

    marker_validations must be the ordered result of validate_markers for the
    same discovery result. Only VALID_V2 entries supply declaration comparisons.
    Unmarked sources and legacy hourly Parquet are still hashed, but declaration
    comparisons remain None rather than being inferred.

    Files are opened relative to no-follow directory descriptors. Hashing uses
    fixed-size reads, reads the initially observed size plus at most one extra
    byte, and re-checks descriptor and path identity. Memory is bounded by
    READ_CHUNK_BYTES; runtime is O(total bytes selected for verification).

    This is an observation, not a snapshot transaction. It does not lock the
    collector writer and cannot detect a replacement preserving all observed
    metadata across separate pipeline stages.
    """
    markers = tuple(marker_validations)
    records = discovery.records
    if len(markers) != len(records):
        raise ValueError("marker_validations must contain one result per discovery record")

    evidence_by_path = {}
    for rec, marker in zip(records, markers):
        if marker.relative_path != rec.relative_path:
            raise ValueError("marker_validations must match discovery order and paths")
        evidence_by_path[rec.relative_path] = _marker_evidence(rec, marker)

    root = _resolve_root(data_root)
    if _SAFE_FLAGS is None:
        output = []
        for rec in records:
            skipped = _skip_record(rec)
            output.append(skipped if skipped is not None else _unknown(rec, "SAFE_OPEN_UNAVAILABLE"))
        return tuple(output)

    dir_flags, file_flags = _SAFE_FLAGS
    output: list[SegmentByteVerification] = []
    open_fds: list[int] = []
    root_fd: Optional[Union[int, Tuple[str, ...]]] = None
    raw_fd: Optional[Union[int, Tuple[str, ...]]] = None
    stream_fds: dict[str, Union[int, Tuple[str, ...]]] = {}

    try:
        for rec in records:
            skipped = _skip_record(rec)
            if skipped is not None:
                output.append(skipped)
                continue
            split = _split_record_path(rec)
            if split is None:
                output.append(_unknown(rec, "SOURCE_PATH_UNUSABLE"))
                continue
            stream, filename = split
            if root_fd is None:
                try:
                    fd = os.open(str(root), dir_flags)
                    open_fds.append(fd)
                    root_fd = fd
                except OSError as exc:
                    root_fd = ("DATA_ROOT_OPEN_FAILED", f"ERRNO_{_errno_name(exc)}")
            if isinstance(root_fd, tuple):
                output.append(_unknown(rec, *root_fd))
                continue
            if raw_fd is None:
                raw_fd = _open_dir(RAW_DIRNAME, root_fd, dir_flags, open_fds)
            if isinstance(raw_fd, tuple):
                output.append(_unknown(rec, *raw_fd))
                continue
            if stream not in stream_fds:
                stream_fds[stream] = _open_dir(stream, raw_fd, dir_flags, open_fds)
            stream_fd = stream_fds[stream]
            if isinstance(stream_fd, tuple):
                output.append(_unknown(rec, *stream_fd))
                continue
            output.append(
                _hash_open_segment(
                    stream_fd, filename, rec, evidence_by_path.get(rec.relative_path), file_flags
                )
            )
    finally:
        for fd in open_fds:
            try:
                os.close(fd)
            except OSError:
                pass
    return tuple(output)
