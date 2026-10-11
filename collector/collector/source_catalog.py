"""Read-only discovery of raw collector source files (Source Catalog, Phase 1 Part 2).

Scope of this module (and nothing more): enumerate ``<data_root>/raw/<stream>/``
and say, for each *candidate source file*, what it is called, where it lives, and
how little can be claimed about it from names and ``lstat`` alone.

What this module deliberately does NOT do
-----------------------------------------
* It never opens, reads, hashes or parses a file. Publication markers are only
  detected by name (``<seg>.meta.json`` exists or it does not); their content is
  not read, so nothing here can ever be ``CONFIRMED``.
* It never creates, renames, deletes, rewrites or locks anything. In particular
  it never opens ``.writer.lock``, never opens the dedup sqlite state, and never
  calls any recovery helper (``confirm_unmarked``, ``preserve_invalid``,
  ``orphan_marker``, ``quarantine_segment``, ...).
* It never follows symlinks and never descends into ``quarantine/``.
* It never resolves a legacy-versus-segment collision. Both members are reported
  as ``AMBIGUOUS``; ``storage_layout.iter_segments`` is intentionally not used
  because it raises mid-iteration or silently drops one side.
* It never claims a file type it could not establish: if ``is_symlink()`` / ``is_dir()`` /
  ``is_file()`` raises, a source-like name is kept as ``UNKNOWN`` (``ENTRY_METADATA_FAILED``),
  with its size unknown, and the entry is not stat()ed, opened or followed.
* It asserts no completeness, instrument identity, unit or timestamp semantics.

Identity is only what repository naming conventions support
(``docs/STORAGE_NAMESPACES.md``): the stream is the directory name; the venue is
derived from the directory-name prefix registry, or is explicitly unknown for the
legacy shared streams whose rows carry their own venue column.
"""
from __future__ import annotations

import os
import re
import stat as _stat
from dataclasses import dataclass
from datetime import date as _date
from enum import Enum
from pathlib import Path
from typing import Iterable, Optional, Sequence, Union

from .storage_layout import LEGACY_SHARED_STREAMS, SegmentKind, parse_segment_name, stream_owner

CATALOG_SCHEMA_VERSION = 1

RAW_DIRNAME = "raw"
#: Mirrors ``publication.QUARANTINE_DIRNAME`` and ``parquet_writer.WRITER_LOCK_FILENAME``.
#: Duplicated on purpose so this module imports neither pyarrow nor the writer;
#: tests assert the values stay equal.
QUARANTINE_DIRNAME = "quarantine"
WRITER_LOCK_FILENAME = ".writer.lock"

_STREAM_NAME = re.compile(r"^[A-Za-z0-9_]+$")
_DATE_ARG = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_NAME_DATE_HOUR_PREFIX = re.compile(r"^(\d{4}-\d{2}-\d{2})-(\d{2})(?!\d)")
_MARKER_SIDECAR = re.compile(r"\.meta\.json\.(?:invalid|orphan)\.")
_TEMP_SUFFIXES = (".seg.tmp", ".parquet.tmp", ".meta.json.tmp")


class SourceRootError(ValueError):
    """The data root is unusable or unsafe. Raised instead of guessing."""


class TrustState(str, Enum):
    """Vocabulary from the approved design. Discovery only ever emits
    ``UNCONFIRMED``, ``LEGACY``, ``INVALID``, ``AMBIGUOUS`` and ``UNKNOWN``:
    the other values need marker/hash evidence that this module does not read."""

    CONFIRMED = "CONFIRMED"
    CONFIRMED_RETROACTIVE = "CONFIRMED_RETROACTIVE"
    UNCONFIRMED = "UNCONFIRMED"
    LEGACY = "LEGACY"
    CORRUPT = "CORRUPT"
    INVALID = "INVALID"
    AMBIGUOUS = "AMBIGUOUS"
    UNKNOWN = "UNKNOWN"


class IdentityStatus(str, Enum):
    #: venue derived from a registered, non-empty prefix (``bybit_``, ``okx_``, ``spot_``).
    NAME_PREFIX_REGISTERED = "NAME_PREFIX_REGISTERED"
    #: unprefixed stream: Binance by repository convention only (weaker evidence).
    UNPREFIXED_CONVENTION = "UNPREFIXED_CONVENTION"
    #: legacy shared stream: rows carry their own venue; the directory proves nothing.
    SHARED_UNVERIFIED = "SHARED_UNVERIFIED"


class IgnoredCategory(str, Enum):
    LOCK_FILE = "LOCK_FILE"
    TEMP_FILE = "TEMP_FILE"
    COUNT_SIDECAR = "COUNT_SIDECAR"
    MARKER_FILE = "MARKER_FILE"                    # marker belonging to a listed segment
    MARKER_ORPHAN = "MARKER_ORPHAN"                # marker whose segment is absent
    MARKER_SIDECAR = "MARKER_SIDECAR"              # ``.meta.json.invalid.N`` / ``.orphan.<utc>``
    MARKER_UNRECOGNIZED = "MARKER_UNRECOGNIZED"
    QUARANTINE_DIR = "QUARANTINE_DIR"
    UNRECOGNIZED_DIRECTORY = "UNRECOGNIZED_DIRECTORY"
    SYMLINK = "SYMLINK"
    NON_REGULAR = "NON_REGULAR"
    UNRELATED = "UNRELATED"


@dataclass(frozen=True)
class DiscoveryWarning:
    code: str
    relative_path: str
    detail: str = ""


@dataclass(frozen=True)
class IgnoredEntry:
    relative_path: str
    category: IgnoredCategory


@dataclass(frozen=True)
class SourceRecord:
    #: ``raw/<stream>/<name>``, posix, built from components (never from the absolute root).
    relative_path: str
    stream: str
    venue: Optional[str]
    identity_status: IdentityStatus
    #: ``None`` when the name matched the pattern but is not a real date/hour.
    date: Optional[str]
    hour: Optional[int]
    sequence: Optional[int]
    kind: Optional[SegmentKind]
    file_format: str                      # "seg" | "parquet"
    size_bytes: Optional[int]             # lstat size at observation; None if lstat failed or the type is unverified
    marker_file_present: bool             # name-level only: the marker's content is NOT read
    trust_state: TrustState
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    collision_group: Optional[str] = None
    legacy_kind: Optional[str] = None     # "hourly_parquet" for legacy hourly files
    catalog_schema_version: int = CATALOG_SCHEMA_VERSION


@dataclass(frozen=True)
class DiscoveryResult:
    records: tuple[SourceRecord, ...]
    ignored: tuple[IgnoredEntry, ...]
    warnings: tuple[DiscoveryWarning, ...]
    #: Streams whose directory could not be listed. Their absence from ``records``
    #: means UNKNOWN, never "empty".
    unreadable_streams: tuple[str, ...]
    streams_scanned: tuple[str, ...]
    catalog_schema_version: int = CATALOG_SCHEMA_VERSION


# --------------------------------------------------------------------------
# input validation
# --------------------------------------------------------------------------

def _validate_streams(streams: Optional[Iterable[str]]) -> Optional[tuple[str, ...]]:
    if streams is None:
        return None
    if isinstance(streams, (str, bytes)):
        raise ValueError("streams must be an iterable of stream names, not a single string")
    out: list[str] = []
    for name in streams:
        if not isinstance(name, str) or not _STREAM_NAME.match(name):
            raise ValueError(f"invalid stream name {name!r}: must match {_STREAM_NAME.pattern}")
        out.append(name)
    return tuple(sorted(set(out), key=os.fsencode))


def _validate_date(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or not _DATE_ARG.match(value):
        raise ValueError(f"date must be YYYY-MM-DD, got {value!r}")
    try:
        _date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"date {value!r} is not a real calendar date") from exc
    return value


def _validate_hour(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 23:
        raise ValueError(f"hour must be an int in 0..23, got {value!r}")
    return value


def _resolve_root(data_root: Union[str, "os.PathLike[str]"]) -> Path:
    try:
        raw = os.fspath(data_root)
    except TypeError as exc:
        raise SourceRootError(f"data_root must be a path, got {type(data_root).__name__}") from exc
    if not isinstance(raw, str) or not raw.strip():
        raise SourceRootError("data_root must be a non-empty path")
    if "\x00" in raw:
        raise SourceRootError("data_root contains a NUL byte")
    try:
        resolved = os.path.realpath(raw, strict=True)
    except (OSError, ValueError) as exc:
        raise SourceRootError(f"data_root {raw!r} cannot be resolved: {exc}") from exc
    if not os.path.isdir(resolved):
        raise SourceRootError(f"data_root {raw!r} is not a directory")
    return Path(resolved)


# --------------------------------------------------------------------------
# identity (naming convention only)
# --------------------------------------------------------------------------

def _identity(stream: str) -> tuple[Optional[str], IdentityStatus]:
    if stream in LEGACY_SHARED_STREAMS:
        return None, IdentityStatus.SHARED_UNVERIFIED
    owner = stream_owner(stream)
    if owner is not None:
        return owner, IdentityStatus.NAME_PREFIX_REGISTERED
    return "BINANCE", IdentityStatus.UNPREFIXED_CONVENTION


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _byte_key(path: str) -> bytes:
    return os.fsencode(path)


def _valid_logical_hour(date_str: str, hour: int) -> bool:
    if not 0 <= hour <= 23:
        return False
    try:
        _date.fromisoformat(date_str)
    except ValueError:
        return False
    return True


def _in_filter(name: str, date: Optional[str], hour: Optional[int]) -> bool:
    """Date/hour filters apply to any name that carries a date-hour prefix.
    Names without one (lock file, unrelated files, directories) are always kept."""
    if date is None and hour is None:
        return True
    match = _NAME_DATE_HOUR_PREFIX.match(name)
    if match is None:
        return True
    if date is not None and match.group(1) != date:
        return False
    if hour is not None and int(match.group(2)) != hour:
        return False
    return True


# --------------------------------------------------------------------------
# per-stream scan
# --------------------------------------------------------------------------

@dataclass
class _Candidate:
    name: str
    parsed: tuple  # (date, hour, sequence, kind) from storage_layout.parse_segment_name
    size: Optional[int]
    stat_failed: bool
    symlink: bool
    non_regular: bool
    #: ``is_symlink()`` / ``is_dir()`` / ``is_file()`` raised, so the file type is NOT established.
    meta_failed: bool = False


def _scan_stream(raw_dir: Path, stream: str, date: Optional[str], hour: Optional[int],
                 records: list, ignored: list, warnings: list, unreadable: list) -> None:
    rel_dir = f"{RAW_DIRNAME}/{stream}"
    try:
        with os.scandir(raw_dir / stream) as it:
            entries = sorted(it, key=lambda e: _byte_key(e.name))
    except OSError as exc:
        unreadable.append(stream)
        warnings.append(DiscoveryWarning("STREAM_UNREADABLE", rel_dir, f"{type(exc).__name__}: {exc}"))
        return

    candidates: list[_Candidate] = []
    markers: set[str] = set()              # segment names that have a ``.meta.json`` file
    temp_count = 0

    def ignore(name: str, category: IgnoredCategory) -> None:
        ignored.append(IgnoredEntry(f"{rel_dir}/{name}", category))

    for entry in entries:
        name = entry.name
        if not _in_filter(name, date, hour):
            continue
        parsed = parse_segment_name(name)          # pure name parsing; no filesystem access
        probe = "is_symlink"
        try:
            is_symlink = entry.is_symlink()
            probe = "is_dir"
            is_dir = (not is_symlink) and entry.is_dir(follow_symlinks=False)
            probe = "is_file"
            is_regular = (not is_symlink) and (not is_dir) and entry.is_file(follow_symlinks=False)
        except OSError as exc:
            # The file type could not be established, so the entry is never classified as a
            # symlink, directory, regular file or unrelated *because of* what it is. It is
            # not stat()ed, opened, hashed or followed either: nothing more is attempted.
            warnings.append(DiscoveryWarning(
                "ENTRY_METADATA_FAILED", f"{rel_dir}/{name}", f"{probe}: {type(exc).__name__}: {exc}"))
            if parsed is not None:
                # A source-like name stays in the inventory (UNKNOWN), never silently dropped.
                candidates.append(_Candidate(name, parsed, None, False, False, False, meta_failed=True))
            else:
                ignore(name, IgnoredCategory.UNRELATED)   # unverified type: generic bucket only
            continue

        if is_symlink:
            warnings.append(DiscoveryWarning("SYMLINK_NOT_FOLLOWED", f"{rel_dir}/{name}"))
            if parsed is not None:
                candidates.append(_Candidate(name, parsed, None, False, True, False))
            else:
                ignore(name, IgnoredCategory.SYMLINK)
            continue
        if is_dir:
            ignore(name, IgnoredCategory.QUARANTINE_DIR if name == QUARANTINE_DIRNAME
                   else IgnoredCategory.UNRECOGNIZED_DIRECTORY)
            continue
        if not is_regular:
            if parsed is not None:
                candidates.append(_Candidate(name, parsed, None, False, False, True))
            else:
                ignore(name, IgnoredCategory.NON_REGULAR)
            continue

        if name == WRITER_LOCK_FILENAME:
            ignore(name, IgnoredCategory.LOCK_FILE)
        elif name.endswith(_TEMP_SUFFIXES):
            temp_count += 1
            ignore(name, IgnoredCategory.TEMP_FILE)
        elif name.endswith(".count.json"):
            ignore(name, IgnoredCategory.COUNT_SIDECAR)
        elif _MARKER_SIDECAR.search(name):
            ignore(name, IgnoredCategory.MARKER_SIDECAR)
        elif name.endswith(".seg.meta.json"):
            markers.add(name[: -len(".meta.json")])      # existence only; content is never read
        elif name.endswith(".meta.json"):
            ignore(name, IgnoredCategory.MARKER_UNRECOGNIZED)
        elif parsed is not None:
            try:
                size: Optional[int] = entry.stat(follow_symlinks=False).st_size
                stat_failed = False
            except OSError:
                size, stat_failed = None, True
            candidates.append(_Candidate(name, parsed, size, stat_failed, False, False))
        else:
            ignore(name, IgnoredCategory.UNRELATED)

    if temp_count:
        warnings.append(DiscoveryWarning(
            "TEMP_RESIDUE", rel_dir,
            f"{temp_count} temporary file(s): live writer or crash residue; not source data"))

    # -- collisions: group valid logical hours; never pick a side -----------
    by_hour: dict[tuple[str, int], list[_Candidate]] = {}
    for cand in candidates:
        c_date, c_hour, _seq, _kind = cand.parsed
        # Only entries *proven* to be symlinks / non-regular are kept out of a group. A candidate
        # whose type could not be established (meta_failed) stays in: a name-level collision is
        # never resolved in favour of the member that happened to be verifiable.
        if not (cand.symlink or cand.non_regular) and _valid_logical_hour(c_date, c_hour):
            by_hour.setdefault((c_date, c_hour), []).append(cand)
    colliding: dict[str, str] = {}           # candidate name -> collision group id
    for (c_date, c_hour), group in sorted(by_hour.items()):
        kinds = {c.parsed[3] for c in group}
        if SegmentKind.LEGACY_HOURLY in kinds and SegmentKind.SEGMENT in kinds:
            group_id = f"{rel_dir}/{c_date}-{c_hour:02d}"
            for member in group:
                colliding[member.name] = group_id
            warnings.append(DiscoveryWarning(
                "COLLISION", rel_dir,
                f"{group_id}: legacy hourly and sequenced segment(s) coexist: "
                + ", ".join(sorted(m.name for m in group))))

    venue, identity_status = _identity(stream)
    candidate_names = {c.name for c in candidates}

    for cand in candidates:
        c_date, c_hour, c_seq, c_kind = cand.parsed
        rel = f"{rel_dir}/{cand.name}"
        file_format = "seg" if cand.name.endswith(".seg") else "parquet"
        marker_present = cand.name in markers
        valid_name = _valid_logical_hour(c_date, c_hour)
        reasons: list[str] = []
        rec_warnings: list[str] = []
        #: Filename-derived identity: kept whatever the trust state, so an inconclusive
        #: observation (e.g. STAT_FAILED) never erases that this is a legacy hourly file.
        legacy_kind: Optional[str] = (
            "hourly_parquet" if valid_name and c_kind is SegmentKind.LEGACY_HOURLY else None)

        # Precedence: structural INVALID > name-level AMBIGUOUS > inconclusive UNKNOWN
        # (STAT_FAILED / ENTRY_METADATA_FAILED) > LEGACY / UNCONFIRMED / marker-based UNKNOWN.
        # A failed lstat or type probe means the observation is incomplete, so no state that
        # implies a successful one is assigned, and no file type is claimed.
        if cand.symlink:
            state = TrustState.INVALID
            reasons.append("SYMLINK_NOT_FOLLOWED")
        elif cand.non_regular:
            state = TrustState.INVALID
            reasons.append("NOT_A_REGULAR_FILE")
        elif not valid_name:
            state = TrustState.INVALID
            reasons.append("INVALID_DATE_OR_HOUR_IN_NAME")
        elif cand.name in colliding:
            state = TrustState.AMBIGUOUS
            reasons.append("LEGACY_SEGMENT_COLLISION")
            rec_warnings.append("COLLISION")
        elif cand.stat_failed or cand.meta_failed:
            state = TrustState.UNKNOWN
        elif c_kind is SegmentKind.LEGACY_HOURLY:
            state = TrustState.LEGACY
            reasons.append("LEGACY_HOURLY_PARQUET")
        elif not marker_present:
            state = TrustState.UNCONFIRMED
            reasons.append("NO_MARKER_FILE")
        else:
            state = TrustState.UNKNOWN
            reasons.append("MARKER_PRESENT_CONTENT_NOT_READ")

        if cand.meta_failed:
            # Recorded for every state (INVALID / AMBIGUOUS keep their structural reason first).
            rec_warnings.append("ENTRY_METADATA_FAILED")
            reasons.append("ENTRY_METADATA_FAILED")
        if cand.stat_failed:
            # Recorded for every state (INVALID / AMBIGUOUS keep their structural reason first).
            rec_warnings.append("STAT_FAILED")
            reasons.append("STAT_FAILED")

        records.append(SourceRecord(
            relative_path=rel, stream=stream, venue=venue, identity_status=identity_status,
            date=c_date if valid_name else None, hour=c_hour if valid_name else None,
            sequence=c_seq if valid_name else None, kind=c_kind if valid_name else None,
            file_format=file_format, size_bytes=cand.size, marker_file_present=marker_present,
            trust_state=state, reasons=tuple(reasons), warnings=tuple(sorted(rec_warnings)),
            collision_group=colliding.get(cand.name), legacy_kind=legacy_kind))

    # -- markers: matched ones are listed as ignored; orphans are warned about --
    for seg_name in sorted(markers, key=_byte_key):
        marker_name = seg_name + ".meta.json"
        if seg_name in candidate_names:
            ignore(marker_name, IgnoredCategory.MARKER_FILE)
        else:
            ignore(marker_name, IgnoredCategory.MARKER_ORPHAN)
            warnings.append(DiscoveryWarning(
                "ORPHAN_MARKER", f"{rel_dir}/{marker_name}",
                "marker file without a listed segment (absent, filtered out, or not a regular segment name)"))


# --------------------------------------------------------------------------
# public entry point
# --------------------------------------------------------------------------

def discover_sources(data_root: Union[str, "os.PathLike[str]"], *,
                     streams: Optional[Sequence[str]] = None,
                     date: Optional[str] = None,
                     hour: Optional[int] = None) -> DiscoveryResult:
    """Enumerate candidate source files under ``<data_root>/raw/`` without touching them.

    ``streams`` restricts the scan to the named stream directories (names are
    validated; separators and ``..`` are rejected). ``date``/``hour`` filter by
    the name prefix of date-hour-named entries.

    Raises :class:`SourceRootError` for an unusable root and ``ValueError`` for a
    bad argument. Everything else becomes a record or a warning.
    """
    wanted = _validate_streams(streams)
    date = _validate_date(date)
    hour = _validate_hour(hour)
    root = _resolve_root(data_root)
    raw_dir = root / RAW_DIRNAME

    records: list[SourceRecord] = []
    ignored: list[IgnoredEntry] = []
    warnings: list[DiscoveryWarning] = []
    unreadable: list[str] = []
    scanned: list[str] = []

    try:
        raw_mode = os.lstat(raw_dir).st_mode
    except FileNotFoundError:
        warnings.append(DiscoveryWarning("RAW_DIR_MISSING", RAW_DIRNAME, "no raw/ directory under data_root"))
        return DiscoveryResult((), (), tuple(warnings), (), ())
    except OSError as exc:
        raise SourceRootError(f"cannot stat {RAW_DIRNAME}/ under data_root: {exc}") from exc
    if _stat.S_ISLNK(raw_mode):
        raise SourceRootError("raw/ under data_root is a symlink; refusing to follow it")
    if not _stat.S_ISDIR(raw_mode):
        raise SourceRootError("raw/ under data_root is not a directory")

    # Stream directories directly under raw/. Only real directories with a plain
    # stream name are scanned; everything else is reported, never followed.
    present: dict[str, str] = {}             # stream name -> "dir"
    try:
        with os.scandir(raw_dir) as it:
            top = sorted(it, key=lambda e: _byte_key(e.name))
    except OSError as exc:
        raise SourceRootError(f"cannot list {RAW_DIRNAME}/ under data_root: {exc}") from exc
    for entry in top:
        rel = f"{RAW_DIRNAME}/{entry.name}"
        try:
            is_symlink = entry.is_symlink()
            is_dir = (not is_symlink) and entry.is_dir(follow_symlinks=False)
        except OSError:
            ignored.append(IgnoredEntry(rel, IgnoredCategory.UNRELATED))
            continue
        if is_symlink:
            ignored.append(IgnoredEntry(rel, IgnoredCategory.SYMLINK))
            warnings.append(DiscoveryWarning("SYMLINK_NOT_FOLLOWED", rel))
        elif not is_dir:
            ignored.append(IgnoredEntry(rel, IgnoredCategory.UNRELATED))
        elif not _STREAM_NAME.match(entry.name):
            ignored.append(IgnoredEntry(rel, IgnoredCategory.UNRECOGNIZED_DIRECTORY))
        else:
            present[entry.name] = "dir"

    if wanted is None:
        to_scan = sorted(present, key=_byte_key)
    else:
        to_scan = []
        for name in wanted:
            if name in present:
                to_scan.append(name)
            else:
                warnings.append(DiscoveryWarning(
                    "STREAM_NOT_FOUND", f"{RAW_DIRNAME}/{name}",
                    "no such stream directory (or it is a symlink / not a directory)"))

    for stream in to_scan:
        scanned.append(stream)
        _scan_stream(raw_dir, stream, date, hour, records, ignored, warnings, unreadable)

    records.sort(key=lambda r: _byte_key(r.relative_path))
    ignored.sort(key=lambda i: (_byte_key(i.relative_path), i.category.value))
    warnings.sort(key=lambda w: (w.code, _byte_key(w.relative_path), w.detail))
    return DiscoveryResult(
        records=tuple(records), ignored=tuple(ignored), warnings=tuple(warnings),
        unreadable_streams=tuple(sorted(unreadable, key=_byte_key)),
        streams_scanned=tuple(scanned))
