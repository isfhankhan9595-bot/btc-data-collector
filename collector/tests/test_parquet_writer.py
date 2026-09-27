import pytest
import json
import os
import shutil
import pyarrow as pa
import pyarrow.parquet as pa_parquet
import pandas as pd
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.instrument import BINANCE_USDM_BTCUSDT

@pytest.fixture
def temp_dir(tmp_path):
    yield str(tmp_path)

def test_parquet_writer(temp_dir):
    schema = pa.schema([
        ("timestamp", pa.int64()),
        ("value", pa.float64())
    ], metadata={"schema_version": "1.0"})

    writer = ParquetWriter("test_stream", schema, base_dir=temp_dir)

    writer.write({"timestamp": 1000, "value": 1.5})
    writer.write({"timestamp": 2000, "value": 2.5})

    assert len(writer.buffer) == 2

    writer.flush()
    assert len(writer.buffer) == 0

    writer.close()

    file_path = writer._get_filename(writer.current_hour)
    assert os.path.exists(file_path)

    df = pd.read_parquet(file_path)
    assert len(df) == 2
    assert df["value"].iloc[0] == 1.5
    assert df["value"].iloc[1] == 2.5

def test_parquet_writer_rotation(temp_dir, monkeypatch):
    schema = pa.schema([
        ("timestamp", pa.int64()),
        ("value", pa.float64())
    ])

    # Mock datetime to control time
    import datetime

    class MockDatetime:
        @classmethod
        def now(cls, tz=None):
            return datetime.datetime(2026, 6, 3, 10, tzinfo=datetime.timezone.utc)
        @classmethod
        def utcnow(cls):
            return datetime.datetime(2026, 6, 3, 10)

    # Init writer at hour 10
    monkeypatch.setattr("collector.collector.parquet_writer.datetime", MockDatetime)
    writer = ParquetWriter("test_stream2", schema, base_dir=temp_dir)

    assert writer.current_hour == "2026-06-03-10"

    writer.write({"timestamp": 1000, "value": 1.5})

    class MockDatetime11:
        @classmethod
        def now(cls, tz=None):
            return datetime.datetime(2026, 6, 3, 11, tzinfo=datetime.timezone.utc)
        @classmethod
        def utcnow(cls):
            return datetime.datetime(2026, 6, 3, 11)

    monkeypatch.setattr("collector.collector.parquet_writer.datetime", MockDatetime11)

    writer.write({"timestamp": 2000, "value": 2.5})

    assert writer.current_hour == "2026-06-03-11"

    writer.close()

    files = os.listdir(os.path.join(temp_dir, "raw", "test_stream2"))
    parquet_files = [name for name in files if name.endswith(".seg")]
    sidecar_files = [name for name in files if name.endswith(".seg.meta.json")]
    assert len(parquet_files) == 2
    assert len(sidecar_files) == 2


def _trade_record(timestamp_ms=1770000000000):
    return {
        "timestamp": timestamp_ms,
        "exchange_timestamp": timestamp_ms + 1,
        "local_timestamp": timestamp_ms + 2,
        "trade_id": 123,
        "price": 50000.0,
        "quantity": 0.25,
        "is_buyer_maker": False,
        "side_sign": 1,
        "signed_qty": 0.25,
        "instrument_key": BINANCE_USDM_BTCUSDT.key,
    }


def test_trades_schema_casts_integer_ms_to_utc_timestamps():
    from collector.collector.config import TRADES_SCHEMA

    table = pa.Table.from_pydict(
        {key: [value] for key, value in _trade_record().items()},
        schema=TRADES_SCHEMA,
    )

    expected_type = pa.timestamp("ms", tz="UTC")
    assert table.schema.field("timestamp").type == expected_type
    assert table.schema.field("exchange_timestamp").type == expected_type
    assert table.schema.field("local_timestamp").type == expected_type
    assert table.column("timestamp").type == expected_type


def test_trades_parquet_reads_timestamps_as_timezone_aware_datetimes(temp_dir):
    from collector.collector.config import TRADES_SCHEMA

    writer = ParquetWriter("trades", TRADES_SCHEMA, base_dir=temp_dir)
    writer.write(_trade_record())
    writer.close()

    file_path = writer._get_filename(writer.current_hour)
    df = pd.read_parquet(file_path)

    assert str(df["timestamp"].dtype) == "datetime64[ms, UTC]"
    assert isinstance(df["timestamp"].dtype, pd.DatetimeTZDtype)

    decoded = pd.to_datetime(df["timestamp"])
    assert decoded.dt.year.iloc[0] == 2026
    assert decoded.dt.year.iloc[0] != 1970



def _openinterest_record(timestamp_ms=1770000000000):
    return {
        "timestamp": timestamp_ms,
        "exchange_timestamp": timestamp_ms + 1,
        "local_timestamp": timestamp_ms + 2,
        "open_interest": 123.45,
        "instrument_key": BINANCE_USDM_BTCUSDT.key,
    }


def _liquidation_record(timestamp_ms=1770000000000):
    return {
        "timestamp": timestamp_ms,
        "exchange_timestamp": timestamp_ms + 1,
        "local_timestamp": timestamp_ms + 2,
        "side": 1,
        "price": 50000.0,
        "quantity": 0.5,
        "signed_qty": 0.5,
        "order_status": "FILLED",
        "time_in_force": "IOC",
        "instrument_key": BINANCE_USDM_BTCUSDT.key,
    }


def test_openinterest_schema_casts_integer_ms_to_utc_timestamps():
    from collector.collector.config import OPENINTEREST_SCHEMA

    table = pa.Table.from_pydict(
        {key: [value] for key, value in _openinterest_record().items()},
        schema=OPENINTEREST_SCHEMA,
    )

    expected_type = pa.timestamp("ms", tz="UTC")
    assert table.schema.field("timestamp").type == expected_type
    assert table.schema.field("exchange_timestamp").type == expected_type
    assert table.schema.field("local_timestamp").type == expected_type
    assert table.column("timestamp").type == expected_type


def test_liquidation_schema_casts_integer_ms_to_utc_timestamps():
    from collector.collector.config import LIQUIDATION_SCHEMA

    table = pa.Table.from_pydict(
        {key: [value] for key, value in _liquidation_record().items()},
        schema=LIQUIDATION_SCHEMA,
    )

    expected_type = pa.timestamp("ms", tz="UTC")
    assert table.schema.field("timestamp").type == expected_type
    assert table.schema.field("exchange_timestamp").type == expected_type
    assert table.schema.field("local_timestamp").type == expected_type
    assert table.column("timestamp").type == expected_type


def test_openinterest_parquet_reads_timestamps_as_timezone_aware_datetimes(temp_dir):
    from collector.collector.config import OPENINTEREST_SCHEMA

    writer = ParquetWriter("openinterest", OPENINTEREST_SCHEMA, base_dir=temp_dir)
    writer.write(_openinterest_record())
    writer.close()

    file_path = writer._get_filename(writer.current_hour)
    df = pd.read_parquet(file_path)

    assert str(df["timestamp"].dtype) == "datetime64[ms, UTC]"
    assert isinstance(df["timestamp"].dtype, pd.DatetimeTZDtype)

    decoded = pd.to_datetime(df["timestamp"])
    assert decoded.dt.year.iloc[0] == 2026
    assert decoded.dt.year.iloc[0] != 1970

def test_parquet_sidecar_uses_first_record_timestamp(temp_dir, monkeypatch):
    schema = pa.schema([
        ("timestamp", pa.int64()),
        ("value", pa.float64())
    ])

    import datetime

    class MockDatetime:
        @classmethod
        def now(cls, tz=None):
            return datetime.datetime(2026, 6, 3, 13, tzinfo=tz)

        @classmethod
        def fromtimestamp(cls, timestamp, tz=None):
            return datetime.datetime.fromtimestamp(timestamp, tz)

    monkeypatch.setattr("collector.collector.parquet_writer.datetime", MockDatetime)

    writer = ParquetWriter("test_stream_sidecar", schema, base_dir=temp_dir)
    first_timestamp = int(datetime.datetime(2026, 6, 3, 13, 31, 11).timestamp() * 1000)
    writer.write({"timestamp": first_timestamp, "value": 1.5})
    writer.write({"timestamp": first_timestamp + 1000, "value": 2.5})
    writer.close()

    file_path = writer._get_filename(writer.current_hour)
    sidecar_path = file_path + ".meta.json"

    assert os.path.exists(sidecar_path)
    with open(sidecar_path, encoding="utf-8") as f:
        sidecar = json.load(f)

    assert sidecar["first_record_ts"] == first_timestamp
    assert sidecar["last_record_ts"] == first_timestamp + 1000
    assert sidecar["record_count"] == 2


def test_empty_parquet_sidecar_falls_back_to_declared_start(temp_dir, monkeypatch):
    schema = pa.schema([
        ("timestamp", pa.int64()),
        ("value", pa.float64())
    ])

    import datetime

    class MockDatetime:
        @classmethod
        def now(cls, tz=None):
            return datetime.datetime(2026, 6, 3, 13, tzinfo=tz)

        @classmethod
        def fromtimestamp(cls, timestamp, tz=None):
            return datetime.datetime.fromtimestamp(timestamp, tz)

    monkeypatch.setattr("collector.collector.parquet_writer.datetime", MockDatetime)

    writer = ParquetWriter("test_stream_empty_sidecar", schema, base_dir=temp_dir)
    writer.close()

    # Empty segments are intentionally never published: an empty .seg would
    # be indistinguishable from a real but useless collection interval.
    # The writer lock is a permanent, non-segment file by design; what must
    # be absent is any segment, temporary segment or sidecar.
    assert [name for name in os.listdir(os.path.join(temp_dir, "raw", "test_stream_empty_sidecar"))
            if name != ".writer.lock"] == []


def test_parquet_sidecar_created_next_to_parquet_file(temp_dir, monkeypatch):
    schema = pa.schema([
        ("timestamp", pa.int64()),
        ("value", pa.float64())
    ])

    import datetime

    class MockDatetime:
        @classmethod
        def now(cls, tz=None):
            return datetime.datetime(2026, 6, 3, 13, tzinfo=tz)

        @classmethod
        def fromtimestamp(cls, timestamp, tz=None):
            return datetime.datetime.fromtimestamp(timestamp, tz)

    monkeypatch.setattr("collector.collector.parquet_writer.datetime", MockDatetime)

    writer = ParquetWriter("test_stream_sidecar_location", schema, base_dir=temp_dir)
    timestamp = int(datetime.datetime(2026, 6, 3, 13, 31, 11).timestamp() * 1000)
    writer.write({"timestamp": timestamp, "value": 1.5})
    writer.close()

    file_path = writer._get_filename(writer.current_hour)
    sidecar_path = file_path + ".meta.json"

    assert os.path.exists(file_path)
    assert os.path.exists(sidecar_path)
    assert os.path.dirname(sidecar_path) == os.path.dirname(file_path)


def test_writer_reports_its_own_exchange_in_quality_events(temp_dir):
    """A non-Binance writer's own storage failures must not be attributed
    to Binance.

    Every writer in this codebase was a Binance writer until Bybit's; the
    quality-event emitters (`_emit_quality`, `_emit_drop`) hardcoded
    ``"exchange": "BINANCE"` rather than reading it from the instance. A
    Bybit writer using either method unmodified would durably misattribute
    its own data drops and crashed-segment events to a venue that did not
    cause them -- exactly the kind of quality-record falsification this
    project's rules forbid, and easy to miss because nothing before Bybit
    ever constructed a ``ParquetWriter`` for a second venue.
    """
    schema = pa.schema([("timestamp", pa.int64()), ("value", pa.float64())])
    events = []
    writer = ParquetWriter("bybit_test_stream", schema, base_dir=temp_dir,
                           exchange="BYBIT", quality_event_sink=events.append)

    writer._emit_quality("SEQUENCE_GAP", "test_reason")
    writer._emit_drop(rows_lost=3)

    assert len(events) == 2
    assert all(e["exchange"] == "BYBIT" for e in events), (
        f"expected every event attributed to BYBIT, got {[e['exchange'] for e in events]}"
    )


def test_writer_exchange_defaults_to_binance_for_backward_compatibility(temp_dir):
    schema = pa.schema([("timestamp", pa.int64()), ("value", pa.float64())])
    events = []
    writer = ParquetWriter("test_stream_default_exchange", schema, base_dir=temp_dir,
                           quality_event_sink=events.append)
    writer._emit_quality("SEQUENCE_GAP", "test_reason")
    assert events[0]["exchange"] == "BINANCE"


# --------------------------------------------------------------------------
# Quality-event tiny-file explosion fix (P0-3 scope note: this fixes the
# real, independently-verified problem -- segment_rows=1/segment_seconds=1
# for every live runner's quality writer, confirmed by grepping the actual
# repository -- using QUALITY_SEGMENT_ROWS/QUALITY_SEGMENT_SECONDS from
# config.py. It does NOT build the WAL-backed architecture a prior
# specification assumed already existed: no write-ahead log exists
# anywhere in this repository (confirmed by search), so that assumption
# was false and is not fabricated here. See docs/QUALITY_EVENT_STORAGE.md.)
# --------------------------------------------------------------------------

def test_burst_of_quality_sized_events_produces_far_fewer_segments_than_events(temp_dir):
    """10,000 tiny events must not become 10,000 files. With
    QUALITY_SEGMENT_ROWS=500, a clean 10,000-row burst produces exactly 20
    segments -- this pins the actual reduction, not just "fewer than N"."""
    from collector.collector.config import QUALITY_SEGMENT_ROWS, QUALITY_SEGMENT_SECONDS
    schema = pa.schema([("timestamp", pa.int64()), ("value", pa.float64())])
    writer = ParquetWriter("burst_test_stream", schema, base_dir=temp_dir,
                           segment_rows=QUALITY_SEGMENT_ROWS, segment_seconds=QUALITY_SEGMENT_SECONDS)
    for i in range(10_000):
        writer.write({"timestamp": i, "value": float(i)})
    writer.close()

    segment_dir = os.path.join(temp_dir, "raw", "burst_test_stream")
    segments = [f for f in os.listdir(segment_dir) if f.endswith(".seg")]
    assert len(segments) == 10_000 // QUALITY_SEGMENT_ROWS, (
        f"expected {10_000 // QUALITY_SEGMENT_ROWS} segments for a clean "
        f"{QUALITY_SEGMENT_ROWS}-row-boundary burst, got {len(segments)}"
    )
    assert len(segments) <= 20, "burst file count must be dramatically reduced from one-per-event"


def test_old_default_would_have_produced_one_segment_per_event_for_comparison(temp_dir):
    """Pins the actual before/after contrast so this fix cannot silently
    regress back toward the old behavior without a test noticing the
    ratio has collapsed."""
    schema = pa.schema([("timestamp", pa.int64()), ("value", pa.float64())])
    writer = ParquetWriter("old_behavior_stream", schema, base_dir=temp_dir,
                           segment_rows=1, segment_seconds=1)
    for i in range(50):
        writer.write({"timestamp": i, "value": float(i)})
    writer.close()
    segment_dir = os.path.join(temp_dir, "raw", "old_behavior_stream")
    segments = [f for f in os.listdir(segment_dir) if f.endswith(".seg")]
    assert len(segments) == 50, "confirms segment_rows=1 really did mean one file per event"


def test_partial_batch_is_flushed_on_graceful_shutdown(temp_dir):
    """A partial batch (well under the row threshold) must not be lost when
    the writer is closed -- shutdown must not require reaching the row
    threshold first."""
    from collector.collector.config import QUALITY_SEGMENT_ROWS
    schema = pa.schema([("timestamp", pa.int64()), ("value", pa.float64())])
    writer = ParquetWriter("shutdown_test_stream", schema, base_dir=temp_dir,
                           segment_rows=QUALITY_SEGMENT_ROWS, segment_seconds=30)
    for i in range(7):   # far short of QUALITY_SEGMENT_ROWS
        writer.write({"timestamp": i, "value": float(i)})
    assert len(writer.buffer) == 7   # confirmed still buffered, not yet flushed
    writer.close()

    segment_dir = os.path.join(temp_dir, "raw", "shutdown_test_stream")
    segments = [f for f in os.listdir(segment_dir) if f.endswith(".seg")]
    assert len(segments) == 1, "a partial batch must still be published on close()"
    table = pa_parquet.read_table(os.path.join(segment_dir, segments[0]))
    assert table.num_rows == 7


def test_batched_events_preserve_write_order_within_a_segment(temp_dir):
    """Batching rows together must not reorder them -- forensic
    reconstruction depends on the persisted order matching write order,
    not a different timestamp-based resort."""
    schema = pa.schema([("timestamp", pa.int64()), ("value", pa.float64())])
    writer = ParquetWriter("ordering_test_stream", schema, base_dir=temp_dir,
                           segment_rows=100, segment_seconds=30)
    written_order = list(range(37))
    for i in written_order:
        writer.write({"timestamp": 1_000_000 - i, "value": float(i)})   # timestamps deliberately descending
    writer.close()

    segment_dir = os.path.join(temp_dir, "raw", "ordering_test_stream")
    (segment,) = [f for f in os.listdir(segment_dir) if f.endswith(".seg")]
    table = pa_parquet.read_table(os.path.join(segment_dir, segment))
    persisted_values = table.column("value").to_pylist()
    assert persisted_values == [float(i) for i in written_order], (
        "persisted row order must match write order, not be resorted by timestamp"
    )


def test_quality_segment_constants_are_shared_not_duplicated_per_runner():
    """Regression: every live runner's quality writer must reference the
    same authoritative config constants, not a locally hardcoded number
    that could silently drift out of sync (this was the actual state
    before this fix: five runners each independently wrote
    segment_rows=1, segment_seconds=1)."""
    import ast
    import inspect
    runner_modules = [
        "collector.run_bybit_collector", "collector.run_okx_capture",
        "collector.run_collector", "collector.run_okx_collector",
        "collector.run_binance_spot_collector",
    ]
    for module_name in runner_modules:
        module = __import__(module_name, fromlist=["_"])
        source = inspect.getsource(module)
        assert "QUALITY_SEGMENT_ROWS" in source and "QUALITY_SEGMENT_SECONDS" in source, (
            f"{module_name} does not reference the shared quality segment config"
        )
        assert "segment_rows=1, segment_seconds=1" not in source, (
            f"{module_name} still hardcodes the old one-event-per-file sizing"
        )
