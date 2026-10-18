"""Read-only Parquet *metadata* verification of discovered raw source files.

Evidence pipeline (this module is the fifth stage; each stage stays distinguishable):

    source discovery -> marker-path classification -> bounded marker validation
        -> segment byte verification -> **Parquet metadata verification** -> final trust classification

What a result here means
------------------------
``VALID_PARQUET`` says only that the footer of a *stable, fully hashed* file parsed as structurally
valid Parquet metadata. It does NOT say the file is complete, that the data pages decode, that rows
are semantically correct, ordered, causally available or suitable for research, or that the source is
trusted. Footer row counts are not timestamp evidence: ``first_record_ts`` / ``last_record_ts`` in a
marker are the writer's claims about arrival order and are never compared to Parquet statistics here.
This module never returns, changes or promotes a ``TrustState`` and never touches the filesystem
except to read: no repair, quarantine, marker write, or recovery.

How the parsed metadata is bound to the bytes it describes
---------------------------------------------------------
A previous ``HASHED_STABLE`` result does not prove the path still names the same bytes: a same-size
replacement after that stage is invisible to size or mtime comparisons made later. So this stage:

1. opens the file itself, descriptor-relative and no-follow, exactly like the earlier stages;
2. hashes every byte of that *one* open descriptor in a bounded sequential pass and, during that same
   pass, captures the leading magic and the trailing ``[footer | footer_len | magic]`` window;
3. compares the SHA-256 and size of what it just read with the supplied byte-verification result, and
   returns ``UNKNOWN_IO`` (never parse facts) if they differ;
4. parses the *captured* bytes with PyArrow from memory. The parser therefore never reopens the path
   and never sees a byte that was not part of the hashed, stable observation.

The cost is one extra full read of every inspected file (O(file size) I/O, memory bounded by
``READ_CHUNK_BYTES`` plus the footer limit). It is required: it is the only way to tie the parsed
footer to the earlier digest. Only metadata is parsed; no row group or data page is decoded.

Footer resource bound
---------------------
The trailing 4-byte length is read and checked *before* any footer bytes are buffered or handed to the
parser. A footer over the limit is never buffered or parsed, but the file still gets the full hashing
pass and must match the prior byte evidence before ``FOOTER_TOO_LARGE`` is reported; a mismatch is
``UNKNOWN_IO``, exactly as for any other file.

``DEFAULT_MAX_FOOTER_BYTES`` (32 MiB) is derived from the writer, not chosen arbitrarily:
``ParquetWriter`` flushes one row group per ``write_table`` call (>= 1 row each) and rotates segments
at ``segment_rows`` (5,000 by default, never configured higher in this repository); the widest
repository schema has 26 columns and PyArrow 24 measured about 110-120 bytes of footer per column
chunk for tiny values. 5,000 row groups x 26 columns x ~120 B ~= 15.6 MB is the theoretical per-segment
ceiling (realistic segments flush every 1,000 rows, i.e. ~20 KB). 32 MiB leaves ~2x margin over that
ceiling. Legacy hourly files have no such writer-side bound; a legitimate one larger than the limit
is reported as ``FOOTER_TOO_LARGE`` -- explicitly NOT as invalid Parquet and NOT as readable -- and the
caller may raise the limit up to ``MAX_ALLOWED_FOOTER_BYTES``. PyArrow's own thrift string/container
limits are set to the declared footer length, which no string or container can exceed.

Honest limits
-------------
* This is an observation, not a snapshot transaction: it does not lock the writer. Identity (device,
  inode, size, mtime, ctime) is re-checked after the pass and the path is re-``stat``-ed no-follow;
  a replacement that preserves every observed attribute *and* every byte is, by definition,
  indistinguishable and harmless to this stage's claims.
* Ancestors of the resolved ``data_root`` are trusted; only components below it are no-follow opened.
* ``marker_validations`` and ``byte_verifications`` are checked for internal consistency only; a
  caller-built result cannot be authenticated.
* Exceptions raised by PyArrow while parsing in-memory bytes (including ``OSError``, which PyArrow
  uses for thrift failures) are content evidence, not I/O failures. Any other exception type is
  deliberately not caught: an unexpected failure should be loud, not mislabelled.
* Where safe no-follow descriptor-relative access is unavailable every candidate is ``UNKNOWN_IO``
  (``SAFE_OPEN_UNAVAILABLE``); there is no weaker fallback.
Diagnostics are fixed codes plus errno *names*; raw exception text and absolute paths never appear.
"""
from __future__ import annotations

import errno
import hashlib
import os
import stat as _stat
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Sequence, Tuple, Union

import pyarrow as pa
import pyarrow.parquet as pq

from .marker_validation import (
    DeclaredMarkerEvidence,
    MarkerValidation,
    _SAFE_FLAGS,
    _errno_name,
    _stable_identity,
)
from .publication import _SHA256, _is_int
from .segment_validation import (
    SegmentByteVerification,
    SegmentVerificationStatus,
    _marker_evidence,
    _open_dir,
    _skip_record,
    _split_record_path,
)
from .source_catalog import RAW_DIRNAME, DiscoveryResult, SourceRecord, _resolve_root

PARQUET_METADATA_SCHEMA_VERSION = 1
READ_CHUNK_BYTES = 1024 * 1024

PARQUET_MAGIC = b"PAR1"
#: 4-byte little-endian footer length + 4-byte trailing magic.
_TRAILER_BYTES = 8
#: Leading magic + (non-empty) footer >= 1 byte + trailer. A footer cannot be empty, so the smallest
#: possible Parquet file is 4 + 1 + 8 = 13 bytes; 12 is the smallest size at which the footer-length
#: arithmetic below is even meaningful, and an empty footer is rejected separately.
_MIN_PARQUET_BYTES = len(PARQUET_MAGIC) + _TRAILER_BYTES

#: See the module docstring for the derivation. A resource boundary, not a plausibility statement.
DEFAULT_MAX_FOOTER_BYTES = 32 * 1024 * 1024
#: Hard ceiling for the configurable limit, so the bound itself cannot be configured away.
MAX_ALLOWED_FOOTER_BYTES = 256 * 1024 * 1024


class ParquetMetadataStatus(str, Enum):
    """What was established about a file's Parquet *metadata*. Not a trust state."""

    #: Metadata was not safely attempted (see ``reasons``); nothing was opened or the earlier stage did
    #: not produce stable byte evidence.
    NOT_CHECKED = "NOT_CHECKED"
    #: The footer of a stable, hashed file parsed as structurally valid Parquet metadata. Nothing
    #: about completeness, data pages, semantics, or trust.
    VALID_PARQUET = "VALID_PARQUET"
    #: Actual file content demonstrates a malformed Parquet structure (see ``reasons``).
    INVALID_PARQUET = "INVALID_PARQUET"
    #: The trailer declares a footer larger than the configured limit. The footer was not buffered or
    #: parsed, but the file's bytes were fully hashed and proved equal to the earlier byte evidence
    #: first. This is a resource decision, not evidence of corruption.
    FOOTER_TOO_LARGE = "FOOTER_TOO_LARGE"
    #: The parser reported a feature it does not support. Not proof of corruption, not readable.
    UNSUPPORTED_METADATA = "UNSUPPORTED_METADATA"
    #: Opening, reading, stable inspection, or agreement with the earlier byte evidence could not be
    #: established. Never converted into corruption or absence.
    UNKNOWN_IO = "UNKNOWN_IO"


@dataclass(frozen=True)
class ParquetMetadataVerification:
    relative_path: str
    status: ParquetMetadataStatus
    #: Stable machine-readable codes. Never raw exception text, never an absolute path.
    reasons: Tuple[str, ...]
    #: Facts observed from the single descriptor read by *this* stage.
    observed_size: Optional[int] = None
    observed_sha256: Optional[str] = None
    bytes_hashed: int = 0
    #: Footer length declared by the trailer (a claim until the footer parses). ``None`` if unread.
    footer_length: Optional[int] = None
    #: Parquet metadata facts: set only when ``status`` is ``VALID_PARQUET``, else ``None``.
    num_rows: Optional[int] = None
    num_row_groups: Optional[int] = None
    num_columns: Optional[int] = None
    #: Did what this stage read equal the earlier stage's size and SHA-256? ``None`` = not compared.
    byte_evidence_matches_prior: Optional[bool] = None
    #: Marker comparisons. ``None`` whenever there is no VALID_V2 marker declaration, or the
    #: comparison was not performed (see ``reasons``). Marker content is a claim, never evidence.
    declared_size_matches: Optional[bool] = None
    declared_sha256_matches: Optional[bool] = None
    declared_record_count_matches: Optional[bool] = None
    #: ``True`` only if BOTH declared size and SHA-256 equal what this stage hashed. The record-count
    #: comparison is performed only under this condition: a marker whose bytes do not match this
    #: file is not a claim about this file, so a count "match" would be a false confirmation.
    marker_claims_corroborated_by_bytes: Optional[bool] = None
    verification_schema_version: int = PARQUET_METADATA_SCHEMA_VERSION


def _validate_limit(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_ALLOWED_FOOTER_BYTES:
        raise ValueError(
            f"max_footer_bytes must be an int in 1..{MAX_ALLOWED_FOOTER_BYTES}, got {value!r}")
    return value


def _result(rec: SourceRecord, status: ParquetMetadataStatus, *reasons: str, **fields
            ) -> ParquetMetadataVerification:
    return ParquetMetadataVerification(rec.relative_path, status, tuple(reasons), **fields)


def _not_checked(rec: SourceRecord, *reasons: str) -> ParquetMetadataVerification:
    return _result(rec, ParquetMetadataStatus.NOT_CHECKED, *reasons)


def _unknown(rec: SourceRecord, *reasons: str, **fields) -> ParquetMetadataVerification:
    return _result(rec, ParquetMetadataStatus.UNKNOWN_IO, *reasons, **fields)


def _check_prior(rec: SourceRecord, prior: SegmentByteVerification,
                 evidence: Optional[DeclaredMarkerEvidence]) -> None:
    """Reject an inconsistent or fabricated byte-verification entry before any file is opened.

    Internal consistency only (it cannot authenticate a caller-built object). The ValueError names
    the offending field, never its value.
    """
    if not isinstance(prior, SegmentByteVerification):
        raise ValueError(f"byte verification for {rec.relative_path!r} has an unexpected type")
    if prior.relative_path != rec.relative_path:
        raise ValueError("byte_verifications must match discovery order and paths")
    problems = []
    if prior.status is SegmentVerificationStatus.HASHED_STABLE:
        if not _is_int(prior.observed_size) or prior.observed_size < 0:
            problems.append("observed_size")
        if not isinstance(prior.observed_sha256, str) or not _SHA256.match(prior.observed_sha256):
            problems.append("observed_sha256")
        if prior.bytes_hashed != prior.observed_size:
            problems.append("bytes_hashed")
        if "SEGMENT_BYTES_STABLE_AND_HASHED" not in prior.reasons:
            problems.append("reasons")
        if not problems:
            if evidence is None:
                expected = (None, None)
            else:
                expected = (
                    prior.observed_size == evidence.size_bytes,
                    prior.observed_sha256.lower() == evidence.sha256.lower(),
                )
            if (prior.declared_size_matches, prior.declared_sha256_matches) != expected:
                problems.append("declared_matches")
    else:
        if prior.observed_sha256 is not None:
            problems.append("observed_sha256")
        if prior.declared_size_matches is not None or prior.declared_sha256_matches is not None:
            problems.append("declared_matches")
    if problems:
        raise ValueError(
            f"byte verification for {rec.relative_path!r} is inconsistent: " + ", ".join(problems))


def _read_trailer(fd: int, size: int) -> Optional[bytes]:
    """The last 8 bytes via one positioned read, or ``None`` on a short read."""
    data = os.pread(fd, _TRAILER_BYTES, size - _TRAILER_BYTES)
    return data if len(data) == _TRAILER_BYTES else None


def _path_still_names(stream_fd: int, filename: str, first: os.stat_result) -> bool:
    now = os.stat(filename, dir_fd=stream_fd, follow_symlinks=False)
    return _stat.S_ISREG(now.st_mode) and (now.st_dev, now.st_ino) == (first.st_dev, first.st_ino)


def _structure_reasons(size: int, head: bytes, window: bytes) -> Tuple[str, ...]:
    """Structural violations provable from captured bytes. Empty tuple = structure is plausible."""
    if size < _MIN_PARQUET_BYTES:
        return ("PARQUET_FILE_TOO_SHORT",)
    reasons = []
    if head != PARQUET_MAGIC:
        reasons.append("PARQUET_HEAD_MAGIC_INVALID")
    trailer = window[-_TRAILER_BYTES:]
    if trailer[4:] != PARQUET_MAGIC:
        reasons.append("PARQUET_TAIL_MAGIC_INVALID")
    footer_len = int.from_bytes(trailer[:4], "little")
    if footer_len == 0:
        reasons.append("PARQUET_FOOTER_LENGTH_ZERO")
    elif footer_len > size - _MIN_PARQUET_BYTES:
        reasons.append("PARQUET_FOOTER_LENGTH_EXCEEDS_FILE")
    return tuple(reasons)


def _chunk_out_of_bounds(metadata, data_end: int) -> bool:
    """True only if some column chunk's byte range provably lies outside the data region
    ``[len(magic), data_end)``. Metadata arithmetic only; no data page is read."""
    for rg in range(metadata.num_row_groups):
        group = metadata.row_group(rg)
        for col in range(group.num_columns):
            chunk = group.column(col)
            starts = [o for o in (chunk.dictionary_page_offset, chunk.data_page_offset)
                      if _is_int(o) and o > 0]
            if not starts:
                continue
            start = min(starts)
            total = chunk.total_compressed_size
            if start < len(PARQUET_MAGIC):
                return True
            if _is_int(total) and total >= 0 and start + total > data_end:
                return True
    return False


def _parse_footer(window: bytes, size: int, footer_len: int):
    """Parse captured footer bytes from memory. Returns ``(status, reasons, facts)``.

    PyArrow's parse of an in-memory buffer cannot perform I/O, so its ``OSError`` (thrift failures)
    and ``ArrowException`` family are content evidence.
    """
    try:
        parsed = pq.ParquetFile(
            pa.BufferReader(window),
            thrift_string_size_limit=footer_len,
            thrift_container_size_limit=footer_len,
        )
        metadata = parsed.metadata
        num_rows, num_rgs, num_cols = metadata.num_rows, metadata.num_row_groups, metadata.num_columns
        rows_sum = sum(metadata.row_group(i).num_rows for i in range(num_rgs))
        out_of_bounds = _chunk_out_of_bounds(metadata, size - _TRAILER_BYTES - footer_len)
    except pa.ArrowNotImplementedError:
        return ParquetMetadataStatus.UNSUPPORTED_METADATA, ("PARQUET_METADATA_UNSUPPORTED",), None
    except (OSError, pa.ArrowException):
        return ParquetMetadataStatus.INVALID_PARQUET, ("PARQUET_FOOTER_PARSE_FAILED",), None
    problems = []
    if rows_sum != num_rows:
        problems.append("PARQUET_ROW_GROUP_ROWS_SUM_MISMATCH")
    if out_of_bounds:
        problems.append("PARQUET_COLUMN_CHUNK_OUT_OF_BOUNDS")
    if problems:
        return ParquetMetadataStatus.INVALID_PARQUET, tuple(problems), None
    return ParquetMetadataStatus.VALID_PARQUET, ("PARQUET_METADATA_VALID",), (num_rows, num_rgs, num_cols)


def _inspect_open_file(
    stream_fd: int,
    filename: str,
    rec: SourceRecord,
    prior: SegmentByteVerification,
    evidence: Optional[DeclaredMarkerEvidence],
    file_flags: int,
    limit: int,
) -> ParquetMetadataVerification:
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
        size = first.st_size
        if size < 0:
            return _unknown(rec, "SEGMENT_SIZE_INVALID_AT_OPEN")
        if size != prior.observed_size:
            # Cheap, exact: the file is already known not to be the one that was hashed.
            return _unknown(
                rec, "BYTES_CHANGED_SINCE_BYTE_VERIFICATION", "BYTE_VERIFICATION_SIZE_DIFFERS",
                observed_size=size, byte_evidence_matches_prior=False,
            )

        # Decide the capture window from the trailer, so that no footer byte is buffered before the
        # declared length has been checked against the limit.
        window_len = min(size, _TRAILER_BYTES)
        footer_len: Optional[int] = None
        pre_trailer: Optional[bytes] = None
        oversized = False
        if size >= _MIN_PARQUET_BYTES:
            try:
                pre_trailer = _read_trailer(fd, size)
            except OSError as exc:
                return _unknown(rec, "SEGMENT_READ_FAILED", f"ERRNO_{_errno_name(exc)}",
                                observed_size=size)
            if pre_trailer is None:
                return _unknown(rec, "SEGMENT_SHORT_READ", observed_size=size)
            declared = int.from_bytes(pre_trailer[:4], "little")
            if pre_trailer[4:] == PARQUET_MAGIC and 0 < declared <= size - _MIN_PARQUET_BYTES:
                footer_len = declared
                if declared > limit:
                    # A resource decision about the footer, not a reason to skip byte identity: keep
                    # the capture window at the 8-byte trailer (the footer is never buffered or
                    # parsed) but still run the full hashing pass below, and only report
                    # FOOTER_TOO_LARGE once that pass has proved the bytes equal the prior evidence.
                    oversized = True
                else:
                    window_len = _TRAILER_BYTES + declared

        # One bounded sequential pass over the single descriptor: hash everything, capture only the
        # leading magic and the trailing window.
        digest = hashlib.sha256()
        head = bytearray()
        window = bytearray()
        window_start = size - window_len
        offset = 0
        while offset < size:
            try:
                chunk = os.read(fd, min(READ_CHUNK_BYTES, size - offset))
            except OSError as exc:
                return _unknown(rec, "SEGMENT_READ_FAILED", f"ERRNO_{_errno_name(exc)}",
                                observed_size=size, bytes_hashed=total)
            if not chunk:
                return _unknown(rec, "SEGMENT_SHORT_READ", observed_size=size, bytes_hashed=total)
            digest.update(chunk)
            if offset < len(PARQUET_MAGIC):
                head += chunk[: len(PARQUET_MAGIC) - offset]
            end = offset + len(chunk)
            if end > window_start:
                window += chunk[max(0, window_start - offset):]
            offset = end
            total = end

        try:
            extra = os.read(fd, 1)
            second = os.fstat(fd)
            same_path = _path_still_names(stream_fd, filename, first)
        except FileNotFoundError:
            return _unknown(rec, "SEGMENT_REPLACED_DURING_READ", observed_size=size, bytes_hashed=total)
        except OSError as exc:
            return _unknown(rec, "SEGMENT_STABILITY_CHECK_FAILED", f"ERRNO_{_errno_name(exc)}",
                            observed_size=size, bytes_hashed=total)
        if extra:
            return _unknown(rec, "SEGMENT_GREW_DURING_READ", observed_size=size, bytes_hashed=total)
        if not same_path:
            return _unknown(rec, "SEGMENT_REPLACED_DURING_READ", observed_size=size, bytes_hashed=total)
        if (_stable_identity(first) != _stable_identity(second) or total != size
                or second.st_size != size):
            return _unknown(rec, "SEGMENT_CHANGED_DURING_READ", observed_size=size, bytes_hashed=total)

        actual_sha = digest.hexdigest()
        observed = dict(observed_size=size, observed_sha256=actual_sha, bytes_hashed=total)
        if actual_sha.lower() != prior.observed_sha256.lower():
            return _unknown(
                rec, "BYTES_CHANGED_SINCE_BYTE_VERIFICATION", "BYTE_VERIFICATION_SHA256_DIFFERS",
                byte_evidence_matches_prior=False, **observed,
            )
        if pre_trailer is not None and bytes(window[-_TRAILER_BYTES:]) != pre_trailer:
            # Cannot happen for a stable file; fail closed rather than trust either observation.
            return _unknown(rec, "TRAILER_CHANGED_DURING_READ",
                            byte_evidence_matches_prior=True, **observed)

        # Marker comparison facts about the bytes (independent of the earlier stage's copy).
        size_match = sha_match = None
        if evidence is not None:
            size_match = size == evidence.size_bytes
            sha_match = actual_sha.lower() == evidence.sha256.lower()
        base = dict(observed, byte_evidence_matches_prior=True,
                    declared_size_matches=size_match, declared_sha256_matches=sha_match)
        mismatch_reasons = []
        if size_match is False:
            mismatch_reasons.append("DECLARED_SIZE_MISMATCH")
        if sha_match is False:
            mismatch_reasons.append("DECLARED_SHA256_MISMATCH")
        if evidence is not None and evidence.legacy:
            mismatch_reasons.append("DECLARED_MARKER_LEGACY")

        # Precedence (all after the stable pass and the SHA-256/size match against the prior
        # byte-verification evidence): structural errors provable from the captured leading magic
        # and trailer come first, and only a structurally plausible file can be reported as having
        # an oversized footer. The oversized footer is never buffered, so these checks use only the
        # 4-byte head and the 8-byte trailer, which are captured in every case.
        structure = _structure_reasons(size, bytes(head), bytes(window))
        if structure:
            declared_len = (int.from_bytes(window[-_TRAILER_BYTES:][:4], "little")
                            if size >= _MIN_PARQUET_BYTES else None)
            return _result(rec, ParquetMetadataStatus.INVALID_PARQUET, *structure, *mismatch_reasons,
                           footer_length=declared_len, **base)

        if oversized:
            # Basic structure is plausible and the bytes equal the prior evidence. No footer byte
            # was captured, so nothing footer-derived is reported.
            return _result(
                rec, ParquetMetadataStatus.FOOTER_TOO_LARGE, "FOOTER_EXCEEDS_LIMIT", *mismatch_reasons,
                footer_length=footer_len, **base,
            )

        if footer_len is None:  # unreachable: structure passed, so the pre-read trailer was plausible
            return _unknown(rec, "TRAILER_CHANGED_DURING_READ", **base)

        status, reasons, facts = _parse_footer(bytes(window), size, footer_len)
        if status is not ParquetMetadataStatus.VALID_PARQUET:
            return _result(rec, status, *reasons, *mismatch_reasons, footer_length=footer_len, **base)

        num_rows, num_rgs, num_cols = facts
        corroborated: Optional[bool] = None
        count_match: Optional[bool] = None
        if evidence is not None:
            corroborated = bool(size_match and sha_match)
            if corroborated:
                count_match = _is_int(evidence.record_count) and num_rows == evidence.record_count
                if not count_match:
                    mismatch_reasons.append("DECLARED_RECORD_COUNT_MISMATCH")
            else:
                mismatch_reasons.append("RECORD_COUNT_COMPARISON_SKIPPED_BYTES_UNCORROBORATED")
        return _result(
            rec, ParquetMetadataStatus.VALID_PARQUET, *reasons, *mismatch_reasons,
            footer_length=footer_len, num_rows=num_rows, num_row_groups=num_rgs, num_columns=num_cols,
            declared_record_count_matches=count_match,
            marker_claims_corroborated_by_bytes=corroborated, **base,
        )
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def verify_parquet_metadata(
    data_root: Union[str, os.PathLike[str]],
    discovery: DiscoveryResult,
    marker_validations: Sequence[MarkerValidation],
    byte_verifications: Sequence[SegmentByteVerification],
    *,
    max_footer_bytes: int = DEFAULT_MAX_FOOTER_BYTES,
) -> Tuple[ParquetMetadataVerification, ...]:
    """Validate Parquet footers without writes, recovery, or trust-state promotion.

    ``marker_validations`` and ``byte_verifications`` must be the ordered results of
    ``validate_markers`` and ``verify_segment_bytes`` for the same ``discovery``. Only ``VALID_V2``
    markers supply declarations; unmarked sources and legacy hourly Parquet are inspected with no
    marker evidence inferred. A record is opened only if the byte-verification stage produced
    ``HASHED_STABLE`` evidence for it; otherwise it is ``NOT_CHECKED``.

    Raises ``ValueError``, before any filesystem access and without partial results, if the inputs are
    misaligned or hold an inconsistent or fabricated entry, or if ``max_footer_bytes`` is out of range.
    These checks establish internal consistency only, not authenticity.

    Observation, not a snapshot transaction (see the module docstring for the binding argument and the
    resource bound).
    """
    limit = _validate_limit(max_footer_bytes)
    markers = tuple(marker_validations)
    priors = tuple(byte_verifications)
    records = discovery.records
    if len(markers) != len(records) or len(priors) != len(records):
        raise ValueError(
            "marker_validations and byte_verifications must each contain one result per discovery record")

    evidence_by_path = {}
    for rec, marker, prior in zip(records, markers, priors):
        if marker.relative_path != rec.relative_path:
            raise ValueError("marker_validations must match discovery order and paths")
        evidence = _marker_evidence(rec, marker)
        _check_prior(rec, prior, evidence)
        if _skip_record(rec) is not None and prior.status is SegmentVerificationStatus.HASHED_STABLE:
            raise ValueError(
                f"byte verification for {rec.relative_path!r} hashed a record discovery marked unusable")
        evidence_by_path[rec.relative_path] = evidence

    root = _resolve_root(data_root)
    candidates = []
    for rec, prior in zip(records, priors):
        skipped = _skip_record(rec)
        if skipped is not None:
            candidates.append(_not_checked(rec, *skipped.reasons))
        elif prior.status is not SegmentVerificationStatus.HASHED_STABLE:
            candidates.append(_not_checked(
                rec, "BYTE_VERIFICATION_NOT_HASHED_STABLE", f"PRIOR_STATUS_{prior.status.value}"))
        else:
            candidates.append(None)

    if _SAFE_FLAGS is None:
        return tuple(
            c if c is not None else _unknown(rec, "SAFE_OPEN_UNAVAILABLE")
            for rec, c in zip(records, candidates)
        )

    dir_flags, file_flags = _SAFE_FLAGS
    output: list[ParquetMetadataVerification] = []
    open_fds: list[int] = []
    root_fd: Optional[Union[int, Tuple[str, ...]]] = None
    raw_fd: Optional[Union[int, Tuple[str, ...]]] = None
    stream_fds: dict[str, Union[int, Tuple[str, ...]]] = {}

    try:
        for rec, prior, candidate in zip(records, priors, candidates):
            if candidate is not None:
                output.append(candidate)
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
            output.append(_inspect_open_file(
                stream_fd, filename, rec, prior, evidence_by_path.get(rec.relative_path),
                file_flags, limit,
            ))
    finally:
        for fd in open_fds:
            try:
                os.close(fd)
            except OSError:
                pass
    return tuple(output)
