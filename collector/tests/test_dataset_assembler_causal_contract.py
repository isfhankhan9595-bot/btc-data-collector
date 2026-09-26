"""P0-6 adversarial tests for the research-dataset causal time contract.

These prove ``dataset_assembler.assemble_dataset`` aligns on the causal
*availability* clock (``local_timestamp`` / receive time) and never on
processing time (``timestamp``) or the venue's own clock
(``exchange_timestamp``). See ``dataset_assembler.py``'s module docstring
for the full contract.
"""
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.pipeline.dataset_assembler import assemble_dataset

_TS_COLS = ("timestamp", "local_timestamp", "exchange_timestamp")
DATE_STR = "2026-06-03"


def _write_parquet(df: pd.DataFrame, path: str) -> None:
    """Write ``df`` to a parquet segment, casting any of the three
    well-known timestamp columns present to ``pa.timestamp("ms", tz="UTC")``,
    matching the real collector schemas (ORDERBOOK_SCHEMA/TRADES_SCHEMA/
    MARKPRICE_SCHEMA in config.py)."""
    df = df.copy()
    for col in _TS_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="ms", utc=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    schema = pa.schema([
        pa.field(field.name, pa.timestamp("ms", tz="UTC")) if field.name in _TS_COLS else field
        for field in table.schema
    ])
    pq.write_table(table.cast(schema), path)


@pytest.fixture
def data_dir(tmp_path):
    d = str(tmp_path)
    for stream in ("orderbook", "trades", "markprice"):
        os.makedirs(os.path.join(d, "raw", stream), exist_ok=True)
    return d


def _seg(data_dir, stream):
    return os.path.join(data_dir, "raw", stream, f"{DATE_STR}-00-000000.seg")


def _start_ts():
    return int(pd.Timestamp(f"{DATE_STR} 00:00:00", tz="UTC").timestamp() * 1000)


def _write_minimal_markprice(data_dir, start_ts):
    """assemble_dataset bails out entirely if markprice is empty; every test
    below is about orderbook/trades causality, so give it one harmless row
    far from anything else being tested."""
    mark_df = pd.DataFrame({
        "timestamp": [start_ts],
        "local_timestamp": [start_ts],
        "exchange_timestamp": [start_ts],
        "mark_price": [100.0],
    })
    _write_parquet(mark_df, _seg(data_dir, "markprice"))


def _assemble(data_dir):
    assemble_dataset(DATE_STR, grid_ms=100, data_dir=data_dir)
    return pd.read_parquet(os.path.join(data_dir, "aligned", f"{DATE_STR}.parquet"))


# ---------------------------------------------------------------------------
# Section 9: adversarial leakage tests
# ---------------------------------------------------------------------------

def test_future_receive_does_not_leak_early_despite_old_exchange_timestamp(data_dir):
    """exchange_event_ts=+1000, local_receive_ts=+5000, process_ts=+6000.
    At T=+4000 the row MUST NOT be visible (not yet received), even though
    exchange_event_ts (1000) <= 4000. At T=+5000 it becomes eligible."""
    start_ts = _start_ts()
    ob_df = pd.DataFrame({
        "timestamp": [start_ts + 6000],
        "local_timestamp": [start_ts + 5000],
        "exchange_timestamp": [start_ts + 1000],
        "best_bid": [100.0], "best_ask": [101.0], "mid_price": [100.5],
    })
    _write_parquet(ob_df, _seg(data_dir, "orderbook"))
    _write_minimal_markprice(data_dir, start_ts)

    df = _assemble(data_dir)

    idx_4000 = 40  # start_ts + 4000
    idx_5000 = 50  # start_ts + 5000
    assert pd.isna(df.loc[idx_4000, "mid_price"]), (
        "row leaked before its local_receive_ts: exchange timestamp being "
        "old must never grant early causal availability"
    )
    assert df.loc[idx_4000, "orderbook_gap"]
    assert df.loc[idx_5000, "mid_price"] == 100.5
    assert not df.loc[idx_5000, "orderbook_gap"]


def test_future_exchange_timestamp_does_not_delay_already_received_event(data_dir):
    """exchange_event_ts=+90000 (claims a "future" event), local_receive_ts
    =+30000, process_ts=+30300. At T=+30000 the event IS eligible: it was
    already received, regardless of what the venue's own clock claims."""
    start_ts = _start_ts()
    ob_df = pd.DataFrame({
        "timestamp": [start_ts + 30300],
        "local_timestamp": [start_ts + 30000],
        "exchange_timestamp": [start_ts + 90000],
        "best_bid": [200.0], "best_ask": [201.0], "mid_price": [200.5],
    })
    _write_parquet(ob_df, _seg(data_dir, "orderbook"))
    _write_minimal_markprice(data_dir, start_ts)

    df = _assemble(data_dir)

    idx_30000 = 300
    assert df.loc[idx_30000, "mid_price"] == 200.5
    assert not df.loc[idx_30000, "orderbook_gap"]


def test_future_information_never_becomes_available_before_it_was_received(data_dir):
    """A single orderbook row received at +1000ms, observed at T=+700ms
    (300ms *before* receipt, well within the 500ms orderbook tolerance).
    The row MUST NOT be visible: it had not been received yet. This is the
    case a "nearest" (rather than strictly backward/causal) join would get
    wrong -- with no earlier candidate to match, "nearest" would pick this
    future row simply because it is the closest one within tolerance,
    which is exactly the leakage the causal contract forbids."""
    start_ts = _start_ts()
    ob_df = pd.DataFrame({
        "timestamp": [start_ts + 1000],
        "local_timestamp": [start_ts + 1000],
        "exchange_timestamp": [start_ts + 1000],
        "best_bid": [500.0], "best_ask": [501.0], "mid_price": [500.5],
    })
    _write_parquet(ob_df, _seg(data_dir, "orderbook"))
    _write_minimal_markprice(data_dir, start_ts)

    df = _assemble(data_dir)

    idx_700 = 7
    idx_1000 = 10
    assert pd.isna(df.loc[idx_700, "mid_price"]), (
        "information from the future leaked into an earlier observation row"
    )
    assert df.loc[idx_700, "orderbook_gap"]
    assert df.loc[idx_1000, "mid_price"] == 500.5


# ---------------------------------------------------------------------------
# Section 10: processing-delay test
# ---------------------------------------------------------------------------

def test_processing_delay_does_not_delay_causal_availability(data_dir):
    """receive=+50000, process=+55000 (5s processing lag), exchange=+49900.
    At T=+50400 (400ms after receive, within the 500ms orderbook tolerance)
    the event MUST be available. An assembler that (incorrectly) used
    processing time would find no eligible row yet, since 55000 > 50400."""
    start_ts = _start_ts()
    ob_df = pd.DataFrame({
        "timestamp": [start_ts + 55000],
        "local_timestamp": [start_ts + 50000],
        "exchange_timestamp": [start_ts + 49900],
        "best_bid": [400.0], "best_ask": [401.0], "mid_price": [400.5],
    })
    _write_parquet(ob_df, _seg(data_dir, "orderbook"))
    _write_minimal_markprice(data_dir, start_ts)

    df = _assemble(data_dir)

    idx_49000 = 490  # before receive: must not be available
    idx_50400 = 504  # after receive, within tolerance: must be available
    assert pd.isna(df.loc[idx_49000, "mid_price"])
    assert df.loc[idx_50400, "mid_price"] == 400.5


# ---------------------------------------------------------------------------
# Section 11: cross-stream consistency
# ---------------------------------------------------------------------------

def test_cross_stream_consistency_uses_receive_time_not_processing_time(data_dir):
    """trade: receive=+1000 process=+1100. book: receive=+1900 process=+4000.
    mark: receive=+1950 process=+3900. All three must be visible by
    observation T=+2000 (using receive time); a processing-time-based
    assembler would incorrectly exclude book and mark (4000, 3900 > 2000),
    and would bin the trade into the wrong grid cell (+1100 instead of
    +1000)."""
    start_ts = _start_ts()

    trades_df = pd.DataFrame({
        "timestamp": [start_ts + 1100],
        "local_timestamp": [start_ts + 1000],
        "exchange_timestamp": [start_ts + 1000],
        "trade_id": [501], "price": [100.0], "quantity": [1.0],
        "is_buyer_maker": [False], "signed_qty": [1.0],
    })
    _write_parquet(trades_df, _seg(data_dir, "trades"))

    ob_df = pd.DataFrame({
        "timestamp": [start_ts + 4000],
        "local_timestamp": [start_ts + 1900],
        "exchange_timestamp": [start_ts + 1900],
        "best_bid": [100.0], "best_ask": [101.0], "mid_price": [100.5],
    })
    _write_parquet(ob_df, _seg(data_dir, "orderbook"))

    mark_df = pd.DataFrame({
        "timestamp": [start_ts + 3900],
        "local_timestamp": [start_ts + 1950],
        "exchange_timestamp": [start_ts + 1950],
        "mark_price": [100.5],
    })
    _write_parquet(mark_df, _seg(data_dir, "markprice"))

    df = _assemble(data_dir)

    idx_1000 = 10   # trade's receive-derived bin: MUST have the trade
    idx_1100 = 11   # trade's processing-derived bin: MUST NOT have it
    idx_2000 = 20   # observation point for book/mark

    assert df.loc[idx_1000, "trade_count"] == 1
    assert df.loc[idx_1100, "trade_count"] == 0
    assert df.loc[idx_2000, "mid_price"] == 100.5
    assert df.loc[idx_2000, "mark_price"] == 100.5
    assert not df.loc[idx_2000, "orderbook_gap"]
    assert not df.loc[idx_2000, "markprice_gap"]


# ---------------------------------------------------------------------------
# Legacy / missing receive-time data: never silently treated as safe
# ---------------------------------------------------------------------------

def test_missing_local_timestamp_falls_back_but_is_flagged_unknown(data_dir):
    """A segment with only ``timestamp`` (no ``local_timestamp`` at all --
    e.g. an older segment written before this contract existed) must still
    be processable, using ``timestamp`` as the availability clock -- but
    every such row must be flagged, never silently presented as causally
    safe."""
    start_ts = _start_ts()
    ob_df = pd.DataFrame({
        "timestamp": [start_ts],
        "best_bid": [100.0], "best_ask": [101.0], "mid_price": [100.5],
    })
    _write_parquet(ob_df, _seg(data_dir, "orderbook"))
    _write_minimal_markprice(data_dir, start_ts)

    df = _assemble(data_dir)

    assert df.loc[0, "mid_price"] == 100.5
    assert bool(df.loc[0, "orderbook_time_unknown"]) is True
    # markprice fixture DOES carry local_timestamp, so it must not be flagged.
    assert bool(df.loc[0, "markprice_time_unknown"]) is False


def test_missing_local_timestamp_never_fabricates_a_new_value(data_dir):
    """The fallback must use exactly the stored ``timestamp`` value -- never
    a freshly computed wall-clock time or any other derived value."""
    start_ts = _start_ts()
    exact_ts = start_ts + 12345
    # 12345 isn't grid-aligned; the row becomes available only once the grid
    # reaches/passes it, so round UP to the next 100ms grid point.
    grid_ts = -(-exact_ts // 100) * 100
    ob_df = pd.DataFrame({
        "timestamp": [exact_ts],
        "best_bid": [100.0], "best_ask": [101.0], "mid_price": [100.5],
    })
    _write_parquet(ob_df, _seg(data_dir, "orderbook"))
    _write_minimal_markprice(data_dir, start_ts)

    df = _assemble(data_dir)

    idx = (grid_ts - start_ts) // 100
    assert df.loc[idx, "timestamp"] == grid_ts
    assert df.loc[idx, "mid_price"] == 100.5
    assert bool(df.loc[idx, "orderbook_time_unknown"]) is True


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

def test_assembly_is_deterministic_across_repeated_runs(data_dir):
    start_ts = _start_ts()
    ob_df = pd.DataFrame({
        "timestamp": [start_ts + 300, start_ts + 100, start_ts + 200],
        "local_timestamp": [start_ts + 300, start_ts + 100, start_ts + 200],
        "exchange_timestamp": [start_ts + 300, start_ts + 100, start_ts + 200],
        "best_bid": [102.0, 100.0, 101.0],
        "best_ask": [103.0, 101.0, 102.0],
        "mid_price": [102.5, 100.5, 101.5],
    })
    _write_parquet(ob_df, _seg(data_dir, "orderbook"))
    _write_minimal_markprice(data_dir, start_ts)

    df_first = _assemble(data_dir)
    df_second = _assemble(data_dir)

    pd.testing.assert_frame_equal(df_first, df_second)


def test_equal_receive_timestamps_are_resolved_deterministically(data_dir):
    """Two rows sharing the exact same local_receive_ts must not crash the
    assembler and must resolve to a single, stable, reproducible value
    across runs (stable-sort tie-break on input order, matching
    cross_exchange_alignment.causally_align()'s documented rule)."""
    start_ts = _start_ts()
    ob_df = pd.DataFrame({
        "timestamp": [start_ts + 1000, start_ts + 1001],
        "local_timestamp": [start_ts + 1000, start_ts + 1000],
        "exchange_timestamp": [start_ts + 1000, start_ts + 1000],
        "best_bid": [100.0, 200.0],
        "best_ask": [101.0, 201.0],
        "mid_price": [100.5, 200.5],
    })
    _write_parquet(ob_df, _seg(data_dir, "orderbook"))
    _write_minimal_markprice(data_dir, start_ts)

    df_first = _assemble(data_dir)
    df_second = _assemble(data_dir)

    pd.testing.assert_frame_equal(df_first, df_second)
    idx_1000 = 10
    assert df_first.loc[idx_1000, "mid_price"] in (100.5, 200.5)
