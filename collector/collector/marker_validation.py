"""Bounded, read-only validation of publication-marker *contents* (Source Catalog, Phase 2A).

Evidence pipeline (each stage stays distinguishable, and this module is only the third):

    source discovery -> marker-path classification -> **bounded marker validation**
        -> segment byte verification -> Parquet metadata verification -> final trust classification

What a result here means
------------------------
A marker is the writer's *claim* about publication. :attr:`MarkerValidationStatus.VALID_V2` says only
that the observed marker bytes parse as a schema-2 marker naming exactly this segment. It does NOT
say that the declared ``sha256`` / ``size_bytes`` match the segment, that the segment is readable
Parquet, or that ``record_count`` agrees with it: none of that is read here. Consequently this module

* never returns, changes or promotes a :class:`~collector.collector.source_catalog.TrustState`
  (it cannot yield ``CONFIRMED`` / ``CONFIRMED_RETROACTIVE`` / ``CORRUPT``),
* never touches the segment file, and
* never converts an I/O failure, a race, or an unsafe filesystem object into "marker absent",
  "marker invalid" or "segment corrupt": those are :attr:`~MarkerValidationStatus.UNKNOWN_IO`.

``MarkerPresence`` (what discovery saw at the marker *path*), :class:`MarkerValidationStatus` (what
the marker *contents* showed) and ``TrustState`` (the source's overall state) are three separate
vocabularies and are kept in three separate places. Discovery's records are not modified.

Why this is a separate module from ``source_catalog``
-----------------------------------------------------
``source_catalog`` guarantees (and its tests enforce) that it never opens a file. Validation must
open one, so it lives here and imports the discovery types, rather than weakening that guarantee.

No schema drift
---------------
The content rules are not re-implemented. Bytes are classified by
:func:`collector.collector.publication.parse_marker_bytes`, the same pure function
``publication.read_marker`` uses. ``read_marker`` itself is deliberately NOT used: it reads the whole
file with ``read_bytes`` (unbounded) and maps every ``OSError`` to ``INVALID``, which is a
conservative *recovery* verdict, not evidence about the marker.

Access discipline
-----------------
* Only a record whose marker discovery observed as a regular file is opened; everything else is
  ``NOT_CHECKED`` (or ``NOT_APPLICABLE`` for legacy hourly Parquet).
* ``<data_root>`` is resolved and opened as a directory descriptor; ``raw/``, the stream directory
  and finally the marker are then opened *relative to the previous descriptor* with ``O_NOFOLLOW``
  (and ``O_DIRECTORY`` for directories). A symlink anywhere below the data root, including one swapped
  in after discovery, is therefore refused rather than followed. The marker is opened with
  ``O_NONBLOCK`` so that a FIFO swapped in after discovery cannot block the process.
* The object that was actually opened is re-examined with ``fstat`` (type and size); the early
  ``lstat``-level probe made by discovery is not trusted.
* At most ``max_marker_bytes + 1`` bytes are ever requested. A marker whose opened size already
  exceeds the limit is rejected from ``fstat`` alone, without reading it.
* After reading, the descriptor is re-``fstat``-ed and the path is re-``stat``-ed (no-follow) relative
  to the stream directory. A change in identity, size, mtime or ctime, a short read, or a path that now
  refers to a different object, is ``UNKNOWN_IO``.
* If the platform cannot provide ``O_NOFOLLOW`` / ``O_DIRECTORY`` / ``dir_fd`` / no-follow ``stat``, every
  marker is ``UNKNOWN_IO`` (``SAFE_OPEN_UNAVAILABLE``): there is no weaker fallback.

Honest limits: the ancestors of the *resolved* ``data_root`` path itself are trusted (they are
resolved once with ``realpath`` and then opened; only the components below the root are protected
against substitution). Validation re-observes the filesystem after discovery; it can detect a marker
that changed type or content between the two moments, not a whole tree swapped out and back.
Diagnostics are fixed codes plus an errno *name*; raw ``OSError`` text (which embeds paths) is never
copied into a result.
"""
from __future__ import annotations

import errno
import os
import stat as _stat
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Tuple, Union

from .publication import MarkerStatus, parse_marker_bytes
from .source_catalog import (
    RAW_DIRNAME,
    DiscoveryResult,
    MarkerPresence,
    SourceRecord,
    TrustState,
    _resolve_root,
)

MARKER_VALIDATION_SCHEMA_VERSION = 1

#: Default marker size limit. Real schema-2 markers are a few hundred bytes; this is a resource
#: boundary, not a statement that anything up to it is plausible.
DEFAULT_MAX_MARKER_BYTES = 64 * 1024
#: Hard ceiling for the configurable limit, so the bound itself cannot be configured away.
MAX_ALLOWED_MARKER_BYTES = 1024 * 1024

_READ_CHUNK = 64 * 1024
_MARKER_SUFFIX = ".meta.json"


class MarkerValidationStatus(str, Enum):
    """What was established about a marker's *contents*. Not a trust state."""

    #: Marker content was not inspected (see ``reasons`` for why).
    NOT_CHECKED = "NOT_CHECKED"
    #: Contents pass the schema-2 rules for this exact segment name. A claim only: nothing about
    #: the segment's bytes, size, Parquet footer or row count has been verified.
    VALID_V2 = "VALID_V2"
    #: A pre-F1 ``meta.json`` hint. Historical metadata, not a publication confirmation.
    LEGACY_HINT_V1 = "LEGACY_HINT_V1"
    #: The observed bytes violate the accepted format or schema (evidence about the marker only).
    INVALID_CONTENT = "INVALID_CONTENT"
    #: The marker exceeds the size limit; it was not read.
    TOO_LARGE = "TOO_LARGE"
    #: Opening, reading or stable inspection failed or was unsafe: validity could not be established.
    UNKNOWN_IO = "UNKNOWN_IO"
    #: This source format does not use the marker convention (legacy hourly Parquet).
    NOT_APPLICABLE = "NOT_APPLICABLE"


@dataclass(frozen=True)
class DeclaredMarkerEvidence:
    """Fields a schema-2 marker *declares*. Every value is the writer's assertion, preserved with its
    native type; none has been checked against the segment. Optional fields that were missing (or not
    of the expected type) are ``None``; nothing is inferred or synthesised."""

    segment: str
    schema: int
    state: str
    confirmed_by: str
    legacy: bool
    size_bytes: int
    sha256: str
    record_count: int
    first_record_ts: Optional[int]
    last_record_ts: Optional[int]
    #: Provenance only (never a trust condition); not validated beyond being a string.
    confirmed_at_utc: Optional[str] = None
    boot_id: Optional[str] = None
    writer_schema: Optional[str] = None


@dataclass(frozen=True)
class MarkerValidation:
    #: The discovery record this result belongs to (``raw/<stream>/<segment>``).
    relative_path: str
    #: What discovery observed at the marker path. Copied through, never overwritten.
    marker_presence: MarkerPresence
    status: MarkerValidationStatus
    #: Stable machine-readable codes. Never raw exception text, never an absolute path.
    reasons: Tuple[str, ...]
    #: Fixed parser wording for ``INVALID_CONTENT`` (e.g. ``"schema != 2; sha256 invalid"``).
    detail: str = ""
    #: Only for ``VALID_V2``.
    evidence: Optional[DeclaredMarkerEvidence] = None
    #: Size of the opened object per ``fstat``; ``None`` if it was never opened.
    observed_size: Optional[int] = None
    #: Bytes actually read (0 when nothing was read, e.g. ``TOO_LARGE``). Never above limit + 1.
    bytes_read: int = 0
    max_marker_bytes: int = DEFAULT_MAX_MARKER_BYTES
    validation_schema_version: int = MARKER_VALIDATION_SCHEMA_VERSION


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _validate_limit(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_ALLOWED_MARKER_BYTES:
        raise ValueError(
            f"max_marker_bytes must be an int in 1..{MAX_ALLOWED_MARKER_BYTES}, got {value!r}")
    return value


def _errno_name(exc: OSError) -> str:
    code = exc.errno
    return errno.errorcode.get(code, "UNKNOWN") if isinstance(code, int) else "UNKNOWN"


def _compute_safe_flags() -> Optional[Tuple[int, int]]:
    """``(directory_flags, file_flags)``, or ``None`` when safe no-follow descriptor-relative access
    cannot be established on this platform (fail closed: there is no weaker fallback)."""
    needed = ("O_NOFOLLOW", "O_DIRECTORY", "O_NONBLOCK")
    if not all(hasattr(os, name) for name in needed):
        return None
    if os.open not in os.supports_dir_fd or os.stat not in os.supports_dir_fd:
        return None
    if os.stat not in os.supports_follow_symlinks:
        return None
    common = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    return (common | os.O_DIRECTORY | os.O_NOFOLLOW,
            common | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_NOCTTY", 0))


#: Evaluated once, at import, from the real ``os`` module (so that wrapping ``os.open`` later, for
#: instrumentation, cannot be mistaken for a platform that lacks ``dir_fd`` support).
_SAFE_FLAGS: Optional[Tuple[int, int]] = _compute_safe_flags()


def _unknown(rec: SourceRecord, limit: int, *reasons: str, observed: Optional[int] = None,
             read: int = 0) -> MarkerValidation:
    return MarkerValidation(rec.relative_path, rec.marker_presence, MarkerValidationStatus.UNKNOWN_IO,
                            tuple(reasons), observed_size=observed, bytes_read=read, max_marker_bytes=limit)


def _skip(rec: SourceRecord, limit: int) -> Optional[MarkerValidation]:
    """Records whose marker contents must not (or need not) be opened. ``None`` -> go inspect it.

    Precedence mirrors discovery: a legacy hourly file has no marker convention whatever its trust
    state; a structurally INVALID or colliding (AMBIGUOUS) source is not inspected at all; and only a
    marker discovery saw as a regular file is ever opened."""
    def made(status: MarkerValidationStatus, reason: str) -> MarkerValidation:
        return MarkerValidation(rec.relative_path, rec.marker_presence, status, (reason,),
                                max_marker_bytes=limit)

    if rec.marker_presence is MarkerPresence.NOT_APPLICABLE:
        return made(MarkerValidationStatus.NOT_APPLICABLE, "MARKER_CONVENTION_NOT_APPLICABLE")
    if rec.trust_state is TrustState.INVALID:
        return made(MarkerValidationStatus.NOT_CHECKED, "SOURCE_STATE_INVALID")
    if rec.trust_state is TrustState.AMBIGUOUS:
        return made(MarkerValidationStatus.NOT_CHECKED, "SOURCE_STATE_AMBIGUOUS")
    if "ENTRY_METADATA_FAILED" in rec.reasons:
        return made(MarkerValidationStatus.NOT_CHECKED, "SOURCE_ENTRY_METADATA_FAILED")
    if rec.marker_presence is MarkerPresence.ABSENT:
        return made(MarkerValidationStatus.NOT_CHECKED, "MARKER_ABSENT_AT_DISCOVERY")
    if rec.marker_presence is MarkerPresence.NON_REGULAR:
        return made(MarkerValidationStatus.NOT_CHECKED, "MARKER_NOT_REGULAR_AT_DISCOVERY")
    if rec.marker_presence is MarkerPresence.UNKNOWN:
        return made(MarkerValidationStatus.NOT_CHECKED, "MARKER_PRESENCE_UNKNOWN_AT_DISCOVERY")
    if rec.marker_presence is not MarkerPresence.REGULAR_FILE:      # defensive: an unforeseen member
        return made(MarkerValidationStatus.NOT_CHECKED, "MARKER_PRESENCE_UNRECOGNIZED")
    return None


def _split_record_path(rec: SourceRecord) -> Optional[Tuple[str, str]]:
    """``(stream, segment_name)`` from ``raw/<stream>/<segment>.seg``, or ``None`` if unusable."""
    parts = rec.relative_path.split("/")
    if len(parts) != 3 or parts[0] != RAW_DIRNAME or parts[1] != rec.stream:
        return None
    seg = parts[2]
    if not seg.endswith(".seg") or "\x00" in seg or seg in (".", ".."):
        return None
    return parts[1], seg


def _stable_identity(st: os.stat_result) -> Tuple[int, int, int, int, int]:
    return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)


def _inspect_marker(stream_fd: int, seg_name: str, rec: SourceRecord, limit: int,
                    file_flags: int) -> MarkerValidation:
    marker_name = seg_name + _MARKER_SUFFIX
    try:
        fd = os.open(marker_name, file_flags, dir_fd=stream_fd)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            return _unknown(rec, limit, "MARKER_SYMLINK_AT_OPEN")
        if isinstance(exc, FileNotFoundError):
            # Discovery listed it; it is gone now. That is a change, never proof of absence.
            return _unknown(rec, limit, "MARKER_MISSING_AT_OPEN")
        return _unknown(rec, limit, "MARKER_OPEN_FAILED", f"ERRNO_{_errno_name(exc)}")
    try:
        try:
            first = os.fstat(fd)
        except OSError as exc:
            return _unknown(rec, limit, "MARKER_FSTAT_FAILED", f"ERRNO_{_errno_name(exc)}")
        if not _stat.S_ISREG(first.st_mode):
            return _unknown(rec, limit, "MARKER_NOT_REGULAR_AT_OPEN")
        if first.st_size > limit:
            # Rejected from the opened object's own size. Nothing is read.
            return MarkerValidation(rec.relative_path, rec.marker_presence, MarkerValidationStatus.TOO_LARGE,
                                    ("MARKER_EXCEEDS_SIZE_LIMIT",), observed_size=first.st_size,
                                    max_marker_bytes=limit)
        chunks = []
        total = 0
        try:
            while total < limit + 1:                       # never request more than limit + 1 bytes
                chunk = os.read(fd, min(_READ_CHUNK, limit + 1 - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
            second = os.fstat(fd)
            path_now = os.stat(marker_name, dir_fd=stream_fd, follow_symlinks=False)
        except FileNotFoundError:
            return _unknown(rec, limit, "MARKER_REPLACED_DURING_READ", observed=first.st_size, read=total)
        except OSError as exc:
            return _unknown(rec, limit, "MARKER_READ_FAILED", f"ERRNO_{_errno_name(exc)}",
                            observed=first.st_size, read=total)
        if not _stat.S_ISREG(path_now.st_mode) or (path_now.st_dev, path_now.st_ino) != (first.st_dev, first.st_ino):
            return _unknown(rec, limit, "MARKER_REPLACED_DURING_READ", observed=first.st_size, read=total)
        if (_stable_identity(first) != _stable_identity(second) or total != first.st_size
                or total != second.st_size or total > limit):
            return _unknown(rec, limit, "MARKER_CHANGED_DURING_READ", observed=first.st_size, read=total)
        raw = b"".join(chunks)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass            # the content was already read; a close failure is not evidence about it

    try:
        parsed = parse_marker_bytes(raw, seg_name)
    except RecursionError:
        # json.loads on pathologically nested input raises RecursionError (not ValueError). It is a
        # statement about the marker's bytes, so it is INVALID_CONTENT here, never an I/O result.
        return MarkerValidation(rec.relative_path, rec.marker_presence, MarkerValidationStatus.INVALID_CONTENT,
                                ("MARKER_CONTENT_INVALID",), detail="not valid JSON: RecursionError",
                                observed_size=first.st_size, bytes_read=total, max_marker_bytes=limit)
    common = dict(observed_size=first.st_size, bytes_read=total, max_marker_bytes=limit)
    if parsed.status is MarkerStatus.VALID:
        pub = parsed.publication
        data = parsed.data or {}

        def opt_str(key: str) -> Optional[str]:
            value = pub.get(key)
            return value if isinstance(value, str) else None

        evidence = DeclaredMarkerEvidence(
            segment=pub["segment"], schema=pub["schema"], state=pub["state"],
            confirmed_by=pub["confirmed_by"], legacy=pub["legacy"], size_bytes=pub["size_bytes"],
            sha256=pub["sha256"], record_count=data["record_count"],
            first_record_ts=data.get("first_record_ts"), last_record_ts=data.get("last_record_ts"),
            confirmed_at_utc=opt_str("confirmed_at_utc"), boot_id=opt_str("boot_id"),
            writer_schema=opt_str("writer_schema"))
        return MarkerValidation(rec.relative_path, rec.marker_presence, MarkerValidationStatus.VALID_V2,
                                ("MARKER_SCHEMA_2_VALID", "SEGMENT_BYTES_NOT_VERIFIED"),
                                evidence=evidence, **common)
    if parsed.status is MarkerStatus.ABSENT and parsed.v1:
        return MarkerValidation(rec.relative_path, rec.marker_presence, MarkerValidationStatus.LEGACY_HINT_V1,
                                ("MARKER_LEGACY_V1_HINT", "NOT_PUBLICATION_CONFIRMATION"), **common)
    if parsed.status is MarkerStatus.INVALID:
        return MarkerValidation(rec.relative_path, rec.marker_presence, MarkerValidationStatus.INVALID_CONTENT,
                                ("MARKER_CONTENT_INVALID",), detail=parsed.reason, **common)
    return _unknown(rec, limit, "MARKER_PARSER_RESULT_UNEXPECTED", observed=first.st_size, read=total)


# --------------------------------------------------------------------------
# public entry point
# --------------------------------------------------------------------------

def validate_markers(data_root: Union[str, "os.PathLike[str]"], discovery: DiscoveryResult, *,
                     max_marker_bytes: int = DEFAULT_MAX_MARKER_BYTES) -> Tuple[MarkerValidation, ...]:
    """Validate the contents of the markers of the sources in ``discovery``, read-only and bounded.

    Returns one :class:`MarkerValidation` per ``discovery.records`` entry, in the same order. The
    records themselves, their ``marker_presence`` and their ``trust_state`` are untouched: a
    ``VALID_V2`` result does not make any source more trusted.

    Raises :class:`~collector.collector.source_catalog.SourceRootError` for an unusable root and
    ``ValueError`` for a bad ``max_marker_bytes``. Every filesystem problem below the root becomes an
    ``UNKNOWN_IO`` result for the affected records instead.
    """
    limit = _validate_limit(max_marker_bytes)
    root = _resolve_root(data_root)
    flags = _SAFE_FLAGS
    results: list[MarkerValidation] = []
    open_fds: list[int] = []
    #: stream -> directory fd, or the reason codes for which it could not be opened safely.
    streams: Dict[str, Union[int, Tuple[str, ...]]] = {}
    raw_fd: Optional[Union[int, Tuple[str, ...]]] = None
    root_fd: Optional[Union[int, Tuple[str, ...]]] = None

    def open_dir(name: str, dir_fd: int, dir_flags: int) -> Union[int, Tuple[str, ...]]:
        try:
            fd = os.open(name, dir_flags, dir_fd=dir_fd)
        except OSError as exc:
            if exc.errno in (errno.ELOOP, errno.ENOTDIR):
                # With O_DIRECTORY | O_NOFOLLOW Linux reports a symlink (even to a directory) as
                # ENOTDIR, exactly like a regular file; both mean "not a plain directory".
                return ("PATH_COMPONENT_NOT_PLAIN_DIRECTORY",)
            if isinstance(exc, FileNotFoundError):
                return ("PATH_COMPONENT_MISSING_AT_OPEN",)
            return ("PATH_COMPONENT_OPEN_FAILED", f"ERRNO_{_errno_name(exc)}")
        open_fds.append(fd)
        return fd

    try:
        for rec in discovery.records:
            skipped = _skip(rec, limit)
            if skipped is not None:
                results.append(skipped)
                continue
            if flags is None:
                results.append(_unknown(rec, limit, "SAFE_OPEN_UNAVAILABLE"))
                continue
            dir_flags, file_flags = flags
            split = _split_record_path(rec)
            if split is None:
                results.append(_unknown(rec, limit, "RECORD_PATH_UNUSABLE"))
                continue
            stream, seg_name = split

            if root_fd is None:
                try:
                    fd = os.open(str(root), dir_flags)
                    open_fds.append(fd)
                    root_fd = fd
                except OSError as exc:
                    root_fd = ("DATA_ROOT_OPEN_FAILED", f"ERRNO_{_errno_name(exc)}")
            if isinstance(root_fd, tuple):
                results.append(_unknown(rec, limit, *root_fd))
                continue
            if raw_fd is None:
                raw_fd = open_dir(RAW_DIRNAME, root_fd, dir_flags)
            if isinstance(raw_fd, tuple):
                results.append(_unknown(rec, limit, *raw_fd))
                continue
            if stream not in streams:
                streams[stream] = open_dir(stream, raw_fd, dir_flags)
            stream_fd = streams[stream]
            if isinstance(stream_fd, tuple):
                results.append(_unknown(rec, limit, *stream_fd))
                continue
            results.append(_inspect_marker(stream_fd, seg_name, rec, limit, file_flags))
    finally:
        for fd in open_fds:
            try:
                os.close(fd)
            except OSError:
                pass
    return tuple(results)
