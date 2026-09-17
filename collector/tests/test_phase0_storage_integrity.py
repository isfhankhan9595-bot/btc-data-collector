"""Phase 0 regression tests: baseline storage integrity.

These target defects that were live on ``main``:

1. A schema-legal ``datetime`` timestamp crashed segment publication.
2. Sequence allocation rescanned the stream directory on every hour rollover.
3. A metadata sidecar failure killed the ingest task *after* the segment had
   already been durably published.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from collector.collector.parquet_writer import ParquetWriter, _epoch_ms
from collector.collector.storage_layout import StorageCollisionError


def _schema() -> pa.Schema:
    return pa.schema([("timestamp", pa.timestamp("ms", tz="UTC")), ("value", pa.int64())])


# ---------------------------------------------------------------- timestamps

def test_datetime_timestamp_does_not_break_publication(tmp_path):
    """A datetime is schema-legal for pa.timestamp columns; it must publish."""
    writer = ParquetWriter("dt", _schema(), base_dir=str(tmp_path), segment_rows=1)
    moment = datetime(2026, 6, 3, 13, 31, 11, tzinfo=timezone.utc)
    writer.write({"timestamp": moment, "value": 7})
    writer.close()

    segments = sorted((tmp_path / "raw" / "dt").glob("*.seg"))
    assert len(segments) == 1, "segment was not published"

    sidecar = json.loads((segments[0].parent / (segments[0].name + ".meta.json")).read_text())
    # Metadata must be epoch ms, not a stringified datetime.
    assert sidecar["first_record_ts"] == int(moment.timestamp() * 1000)
    assert isinstance(sidecar["first_record_ts"], int)


def test_naive_datetime_is_interpreted_as_utc():
    naive = datetime(2026, 6, 3, 13, 0, 0)
    aware = datetime(2026, 6, 3, 13, 0, 0, tzinfo=timezone.utc)
    assert _epoch_ms(naive) == _epoch_ms(aware)


@pytest.mark.parametrize(
    "value",
    [None, float("nan"), True, object(), "not-a-timestamp"],
)
def test_unconvertible_timestamps_degrade_to_none_not_exceptions(value):
    """Metadata is a hint. Losing it must never cost us the segment."""
    assert _epoch_ms(value) is None


def test_int_timestamps_pass_through_unchanged():
    assert _epoch_ms(1770000000000) == 1770000000000


# ------------------------------------------------------------ sequence cache

def test_single_scan_populates_every_hour(tmp_path, monkeypatch):
    """Hour rollover must not trigger a fresh directory scan."""
    raw = tmp_path / "raw" / "trades"
    raw.mkdir(parents=True)
    for name in ("2026-06-03-01-000001.seg", "2026-06-03-01-000002.seg", "2026-06-03-02-000099.seg"):
        (raw / name).touch()

    writer = object.__new__(ParquetWriter)
    writer.stream_name = "trades"
    writer.base_dir = str(tmp_path)
    writer._sequence_cache = {}

    assert writer._next_sequence("2026-06-03-01") == 3

    def explode(*args, **kwargs):
        raise AssertionError("directory was rescanned")

    monkeypatch.setattr("collector.collector.parquet_writer.iter_segments", explode)
    monkeypatch.setattr(type(raw), "iterdir", lambda self: explode())

    assert writer._next_sequence("2026-06-03-02") == 100
    assert writer._next_sequence("2026-06-03-01") == 3


def test_unrelated_hour_collision_does_not_block_current_hour(tmp_path):
    """A corrupt old hour must not stop the collector writing the live hour."""
    raw = tmp_path / "raw" / "trades"
    raw.mkdir(parents=True)
    # Ambiguous legacy/segment pair in an old hour.
    (raw / "2026-06-03-01.parquet").touch()
    (raw / "2026-06-03-01-000001.seg").touch()
    # Clean current hour.
    (raw / "2026-06-03-05-000003.seg").touch()

    writer = object.__new__(ParquetWriter)
    writer.stream_name = "trades"
    writer.base_dir = str(tmp_path)
    writer._sequence_cache = {}

    assert writer._next_sequence("2026-06-03-05") == 4


def test_requested_hour_collision_still_raises(tmp_path):
    """Collision detection must not be weakened by the single-scan rewrite."""
    raw = tmp_path / "raw" / "trades"
    raw.mkdir(parents=True)
    (raw / "2026-06-03-01.parquet").touch()
    (raw / "2026-06-03-01-000001.seg").touch()

    writer = object.__new__(ParquetWriter)
    writer.stream_name = "trades"
    writer.base_dir = str(tmp_path)
    writer._sequence_cache = {}

    with pytest.raises(StorageCollisionError):
        writer._next_sequence("2026-06-03-01")


def test_empty_hour_starts_at_zero(tmp_path):
    writer = object.__new__(ParquetWriter)
    writer.stream_name = "trades"
    writer.base_dir = str(tmp_path)
    writer._sequence_cache = {}
    assert writer._next_sequence("2026-06-03-01") == 0


# ------------------------------------------------------- metadata durability

def test_metadata_failure_does_not_lose_published_segment(tmp_path, monkeypatch):
    """The segment is published before metadata; a sidecar failure is reported,
    not fatal, and never unpublishes the data."""
    events: list[dict] = []
    writer = ParquetWriter(
        "meta", _schema(), base_dir=str(tmp_path), segment_rows=1,
        quality_event_sink=events.append,
    )

    real_open = type(tmp_path).open

    def fail_on_meta_tmp(self, *args, **kwargs):
        if self.name.endswith(".meta.json.tmp"):
            raise OSError("simulated sidecar failure")
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(type(tmp_path), "open", fail_on_meta_tmp)

    writer.write({"timestamp": 1770000000000, "value": 7})
    writer.close()

    segments = sorted((tmp_path / "raw" / "meta").glob("*.seg"))
    assert len(segments) == 1, "published segment was lost to a metadata failure"
    assert any(e["event_type"] == "STORAGE_METADATA_FAILED" for e in events), \
        "metadata failure was silent"
    # No orphan temporary left behind.
    assert not list((tmp_path / "raw" / "meta").glob("*.meta.json.tmp"))
