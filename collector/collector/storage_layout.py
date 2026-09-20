"""Immutable raw-segment naming and discovery.

Raw collector output has used both legacy hourly ``.parquet`` files and the
crash-bounded ``.seg`` format. Readers must use this module rather than
constructing filenames themselves so that old data remains readable.

Venue namespaces
----------------
Every venue's raw streams live in their own stream directory, named by
:func:`venue_stream`. Segment sequence numbers, ``.tmp`` files and orphan
recovery are all scoped to one stream directory, so a directory shared by two
venues (or two processes) is a shared sequence namespace. Binance keeps the
historical unprefixed names because its recorded history lives there;
Bybit and OKX are prefixed. See ``docs/STORAGE_NAMESPACES.md``.
"""
from __future__ import annotations

import re
from enum import Enum
from pathlib import Path
from typing import Iterator


class SegmentKind(str, Enum):
    LEGACY_HOURLY = "legacy_hourly"
    SEGMENT = "segment"


SEGMENT_SUFFIXES = (".seg", ".parquet")
_NAME = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})-(?P<hour>\d{2})(?:-(?P<seq>\d{6}))?(?P<suffix>\.seg|\.parquet)$"
)


class StorageCollisionError(RuntimeError):
    """Raised when legacy and sequenced files represent the same logical hour."""


class StorageNamespaceError(RuntimeError):
    """Raised when a stream name and the venue writing to it disagree."""


#: Prefix that namespaces each venue's stream directories. BINANCE is empty on
#: purpose: it was the only venue when the unprefixed names were chosen, its
#: recorded history lives under them, and renaming would strand that history.
#: BINANCE_SPOT is a distinct storage-namespace key from BINANCE, even though
#: P5 canonical events use exchange="BINANCE" (market_type="spot" is the
#: differentiator P6's cross-exchange identity uses -- see canonical.py and
#: docs/EXECUTION_STATUS.md, "P5"). Storage namespace identity and P6
#: cross-exchange identity are deliberately two different keyspaces: this one
#: exists so Spot's segments/sequence/orphan-recovery can never share a
#: directory with USD-M futures' (the exact PR #19 collision class this
#: registry exists to prevent), while P6 alignment still sees both as the
#: same exchange with a different market_type, which is what it's supposed to.
VENUE_STREAM_PREFIX: dict[str, str] = {"BINANCE": "", "BYBIT": "bybit_", "OKX": "okx_", "BINANCE_SPOT": "spot_"}

#: Unprefixed stream names that PR #13 let OKX share with Binance. Legacy OKX
#: frames and quality events may still sit in these directories; every row in
#: them carries its own venue/exchange column, which is how readers separate
#: them. New OKX data never goes here.
LEGACY_SHARED_STREAMS = frozenset({"raw_wire", "raw_rest", "quality_events"})


def _known_venue(venue: str) -> str:
    key = str(venue).upper()
    if key not in VENUE_STREAM_PREFIX:
        raise ValueError(
            f"unknown venue {venue!r}; register it in VENUE_STREAM_PREFIX so its "
            f"streams get their own namespace (known: {sorted(VENUE_STREAM_PREFIX)})"
        )
    return key


def venue_stream(venue: str, stream: str) -> str:
    """Return the stream directory name ``venue`` writes ``stream`` to.

    Raises for an unregistered venue rather than defaulting: a silent default
    is exactly how two venues came to share ``raw_wire``.
    """
    return VENUE_STREAM_PREFIX[_known_venue(venue)] + stream


def read_streams(venue: str, stream: str) -> tuple[str, ...]:
    """Stream directories that may hold ``venue``'s rows for ``stream``.

    The venue's own directory first. OKX additionally reads the legacy
    unprefixed directory, where its pre-namespace captures live. Callers must
    still filter the rows by their venue column: the legacy directory is
    shared history, not OKX's.
    """
    key = _known_venue(venue)
    own = venue_stream(key, stream)
    if key == "OKX" and stream in LEGACY_SHARED_STREAMS:
        return (own, stream)
    return (own,)


def stream_owner(stream_name: str) -> str | None:
    """Venue whose non-empty prefix ``stream_name`` carries, if any."""
    for venue, prefix in VENUE_STREAM_PREFIX.items():
        if prefix and stream_name.startswith(prefix):
            return venue
    return None


def check_stream_namespace(venue: str, stream_name: str) -> None:
    """Refuse a writer whose declared venue contradicts its stream name.

    Two rules, both of which are the misconfigurations that create shared
    namespaces or misattribute a venue's storage faults:

    * a prefixed venue (Bybit, OKX) must write to its own prefixed streams;
    * a stream carrying another venue's prefix must not be written by a
      writer declared as a different venue (including the BINANCE default).

    A venue that is not registered is not policed here; the writer's own
    stream lock still prevents it from sharing a directory concurrently.
    """
    key = str(venue).upper()
    prefix = VENUE_STREAM_PREFIX.get(key)
    if prefix and not stream_name.startswith(prefix):
        raise StorageNamespaceError(
            f"{key} writer must use a {prefix!r}-prefixed stream, got {stream_name!r}; "
            f"use venue_stream({key!r}, ...) so venues never share a sequence namespace"
        )
    owner = stream_owner(stream_name)
    if owner is not None and owner != key:
        raise StorageNamespaceError(
            f"stream {stream_name!r} belongs to {owner}, but the writer is declared as {key}"
        )


def segment_path(base_dir: str | Path, stream: str, hour_str: str, seq: int) -> Path:
    """Return the canonical, crash-bounded segment pathname."""
    return Path(base_dir) / "raw" / stream / f"{hour_str}-{seq:06d}.seg"


def parse_segment_name(path: str | Path) -> tuple[str, int, int | None, SegmentKind] | None:
    """Return ``(date, hour, sequence, kind)`` for a published segment."""
    match = _NAME.match(Path(path).name)
    if not match:
        return None
    suffix = match["suffix"]
    if suffix == ".parquet" and match["seq"] is None:
        return match["date"], int(match["hour"]), None, SegmentKind.LEGACY_HOURLY
    if suffix != ".seg" or match["seq"] is None:
        return None
    return match["date"], int(match["hour"]), int(match["seq"]), SegmentKind.SEGMENT


def iter_segments(
    base_dir: str | Path,
    stream: str,
    *,
    date: str | None = None,
    hour: int | None = None,
    on_collision: str = "raise",
) -> Iterator[Path]:
    """Yield one unambiguous representation for each logical hour.

    The safe default raises on legacy/segment ambiguity. Offline migration
    readers may explicitly request ``prefer_segments`` or ``prefer_legacy``.
    """
    if on_collision not in {"raise", "prefer_segments", "prefer_legacy"}:
        raise ValueError("on_collision must be raise, prefer_segments, or prefer_legacy")
    stream_dir = Path(base_dir) / "raw" / stream
    if not stream_dir.exists():
        return

    if date is not None and hour is not None:
        candidates = [
            stream_dir / f"{date}-{hour:02d}.parquet",
            *sorted(stream_dir.glob(f"{date}-{hour:02d}-*.seg")),
        ]
    elif date is not None:
        candidates = [
            *stream_dir.glob(f"{date}-*.parquet"),
            *stream_dir.glob(f"{date}-*-*.seg"),
        ]
    elif hour is not None:
        candidates = [
            *stream_dir.glob(f"*-{hour:02d}.parquet"),
            *stream_dir.glob(f"*-{hour:02d}-*.seg"),
        ]
    else:
        candidates = list(stream_dir.iterdir())

    found: dict[tuple[str, int], list[tuple[SegmentKind, Path, int | None]]] = {}
    for path in candidates:
        if not path.is_file():
            continue
        parsed = parse_segment_name(path)
        if parsed is None:
            continue
        row_date, row_hour, sequence, kind = parsed
        if date is not None and row_date != date:
            continue
        if hour is not None and row_hour != hour:
            continue
        found.setdefault((row_date, row_hour), []).append((kind, path, sequence))

    for logical_hour in sorted(found):
        entries = found[logical_hour]
        legacy = [entry for entry in entries if entry[0] is SegmentKind.LEGACY_HOURLY]
        segments = [entry for entry in entries if entry[0] is SegmentKind.SEGMENT]
        if legacy and segments:
            paths = [entry[1] for entry in entries]
            if on_collision == "raise":
                raise StorageCollisionError(
                    f"legacy/segment storage collision for {logical_hour}: "
                    + ", ".join(str(path) for path in paths)
                )
            chosen = segments if on_collision == "prefer_segments" else legacy
        else:
            chosen = entries
        for _, path, _ in sorted(chosen, key=lambda entry: (entry[2] is None, entry[2] or -1, str(entry[1]))):
            yield path
