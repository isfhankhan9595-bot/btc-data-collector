from datetime import datetime, timezone

import pyarrow as pa
import pytest

from collector.collector.parquet_writer import ParquetWriter
from collector.collector.storage_layout import (
    SegmentKind,
    StorageCollisionError,
    iter_segments,
    parse_segment_name,
    segment_path,
)


def _schema() -> pa.Schema:
    return pa.schema([("timestamp", pa.timestamp("ms", tz="UTC")), ("value", pa.int64())])


def test_storage_layout_roundtrip(tmp_path):
    writer = ParquetWriter("example", _schema(), base_dir=str(tmp_path), segment_rows=1)
    writer.write({"timestamp": datetime.now(timezone.utc), "value": 7})
    writer.close()

    paths = list(iter_segments(tmp_path, "example"))
    assert len(paths) == 1
    parsed = parse_segment_name(paths[0])
    assert parsed is not None
    date, hour, sequence, kind = parsed
    assert kind is SegmentKind.SEGMENT
    assert sequence == 0
    assert paths[0] == segment_path(tmp_path, "example", f"{date}-{hour:02d}", sequence)


def test_parse_legacy_has_no_sequence(tmp_path):
    path = tmp_path / "raw" / "example" / "2026-06-03-00.parquet"
    path.parent.mkdir(parents=True)
    path.touch()
    assert parse_segment_name(path) == (
        "2026-06-03", 0, None, SegmentKind.LEGACY_HOURLY
    )


def test_storage_layout_reads_legacy_and_never_temporary_files(tmp_path):
    raw = tmp_path / "raw" / "example"
    raw.mkdir(parents=True)
    (raw / "2026-06-03-01.parquet").touch()
    (raw / "2026-06-03-00-000002.seg").touch()
    (raw / "2026-06-03-00-000003.seg.tmp").touch()
    assert [path.name for path in iter_segments(tmp_path, "example", date="2026-06-03")] == [
        "2026-06-03-00-000002.seg", "2026-06-03-01.parquet"
    ]


def test_iter_segments_raises_on_legacy_segment_collision(tmp_path):
    raw = tmp_path / "raw" / "example"
    raw.mkdir(parents=True)
    (raw / "2026-06-03-01.parquet").touch()
    (raw / "2026-06-03-01-000001.seg").touch()
    with pytest.raises(StorageCollisionError, match="2026-06-03-01"):
        list(iter_segments(tmp_path, "example", date="2026-06-03", hour=1))


def test_iter_segments_explicit_collision_precedence(tmp_path):
    raw = tmp_path / "raw" / "example"
    raw.mkdir(parents=True)
    legacy = raw / "2026-06-03-01.parquet"
    segment = raw / "2026-06-03-01-000001.seg"
    legacy.touch()
    segment.touch()
    assert list(iter_segments(tmp_path, "example", date="2026-06-03", hour=1, on_collision="prefer_segments")) == [segment]
    assert list(iter_segments(tmp_path, "example", date="2026-06-03", hour=1, on_collision="prefer_legacy")) == [legacy]


def test_writer_never_creates_colliding_sequence(tmp_path, monkeypatch):
    raw = tmp_path / "raw" / "example"
    raw.mkdir(parents=True)
    hour = "2026-06-03-01"
    (raw / f"{hour}.parquet").touch()
    events = []
    monkeypatch.setattr(ParquetWriter, "_get_current_hour_str", lambda self: hour)
    writer = ParquetWriter("example", _schema(), base_dir=str(tmp_path), segment_rows=1, quality_event_sink=events.append)
    assert writer._seq == 1
    assert any(event["event_type"] == "STORAGE_MIGRATION" for event in events)
    writer.write({"timestamp": datetime.now(timezone.utc), "value": 7})
    writer.close()
    assert (raw / f"{hour}-000001.seg").exists()
    assert not (raw / f"{hour}-000000.seg").exists()


def test_writer_refuses_legacy_segment_collision(tmp_path, monkeypatch):
    raw = tmp_path / "raw" / "example"
    raw.mkdir(parents=True)
    hour = "2026-06-03-01"
    (raw / f"{hour}.parquet").touch()
    (raw / f"{hour}-000001.seg").touch()
    monkeypatch.setattr(ParquetWriter, "_get_current_hour_str", lambda self: hour)
    with pytest.raises(StorageCollisionError):
        ParquetWriter("example", _schema(), base_dir=str(tmp_path))
