"""Immutable raw-segment naming and discovery.

Raw collector output has used both legacy hourly ``.parquet`` files and the
crash-bounded ``.seg`` format. Readers must use this module rather than
constructing filenames themselves so that old data remains readable.
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
