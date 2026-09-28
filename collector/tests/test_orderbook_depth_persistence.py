"""P0-8: downstream behavior of truthful (unpadded) order-book depth.

Covers the parts of the contract that live beyond ``compute_orderbook_features``
itself: the real ParquetWriter round trip (variable-length list columns,
nullable ``obi_level_N`` and ``bid_depth``/``ask_depth``), the research
dataset assembler consuming such rows, and the statelessness that keeps this
path free of forward-filled/stale book levels.
See ``docs/ORDERBOOK_LEVEL_TRUTH.md``.
"""
import glob
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from collector.collector.config import ORDERBOOK_SCHEMA
from collector.collector.feature_computer import compute_orderbook_features
from collector.collector.parquet_writer import ParquetWriter
from collector.pipeline.dataset_assembler import assemble_dataset


def _thin_book_features(levels=3):
    msg = {
        "E": 1_700_000_000_000,
        "b": [[str(100.0 - i), "1.0"] for i in range(levels)],
        "a": [[str(101.0 + i), "2.0"] for i in range(levels)],
    }
    return compute_orderbook_features(msg)


def test_thin_book_round_trips_through_the_real_writer_without_padding(tmp_path):
    features = _thin_book_features(levels=3)
    writer = ParquetWriter("orderbook", ORDERBOOK_SCHEMA, base_dir=str(tmp_path))
    writer.write(features)
    writer.close()

    path = glob.glob(str(tmp_path / "raw" / "orderbook" / "*.seg"))[0]
    row = pd.read_parquet(path).to_dict("records")[0]

    assert list(row["bids_price"]) == [100.0, 99.0, 98.0]
    assert list(row["asks_price"]) == [101.0, 102.0, 103.0]
    assert len(row["bids_qty"]) == 3
    assert row["bid_depth"] == 3
    assert row["ask_depth"] == 3
    assert row["obi_level_3"] is not None and not pd.isna(row["obi_level_3"])
    # Level-5 was not observed: persisted as a true null, never 0.0.
    assert pd.isna(row["obi_level_5"])


def test_persisted_schema_declares_depth_columns_as_nullable():
    for name in ("bid_depth", "ask_depth", "obi_level_3", "obi_level_5"):
        assert ORDERBOOK_SCHEMA.field(name).nullable
    assert ORDERBOOK_SCHEMA.metadata[b"schema_version"] == b"1.2"


def test_assembler_propagates_null_level_obi_as_missing_not_zero(tmp_path):
    """The research assembler must carry a null obi_level_5 through as NaN
    (its Fisher transform included) and pass bid_depth/ask_depth through."""
    date_str = "2026-06-05"
    start_ms = int(pd.Timestamp(f"{date_str} 00:00:00", tz="UTC").timestamp() * 1000)
    data_dir = str(tmp_path)
    for stream in ("orderbook", "markprice"):
        os.makedirs(os.path.join(data_dir, "raw", stream), exist_ok=True)

    def ts(ms):
        return pa.array([pd.Timestamp(ms, unit="ms", tz="UTC")], type=pa.timestamp("ms", tz="UTC"))

    ob = pa.table({
        "timestamp": ts(start_ms), "local_timestamp": ts(start_ms), "exchange_timestamp": ts(start_ms),
        "best_bid": pa.array([100.0]), "best_ask": pa.array([101.0]), "mid_price": pa.array([100.5]),
        "obi": pa.array([0.1]), "obi_level_1": pa.array([0.1]),
        "obi_level_3": pa.array([0.2]), "obi_level_5": pa.array([None], type=pa.float64()),
        "bid_depth": pa.array([3], type=pa.int32()), "ask_depth": pa.array([3], type=pa.int32()),
    })
    pq.write_table(ob, os.path.join(data_dir, "raw", "orderbook", f"{date_str}-00-000000.seg"))
    mark = pa.table({
        "timestamp": ts(start_ms), "local_timestamp": ts(start_ms), "exchange_timestamp": ts(start_ms),
        "mark_price": pa.array([100.0]),
    })
    pq.write_table(mark, os.path.join(data_dir, "raw", "markprice", f"{date_str}-00-000000.seg"))

    assemble_dataset(date_str, grid_ms=100, data_dir=data_dir)
    df = pd.read_parquet(os.path.join(data_dir, "aligned", f"{date_str}.parquet"))

    assert df.loc[0, "bid_depth"] == 3
    assert df.loc[0, "ask_depth"] == 3
    assert df.loc[0, "obi_level_3"] == 0.2
    assert pd.isna(df.loc[0, "obi_level_5"])
    assert pd.isna(df.loc[0, "obi_level_5_fisher"])
    assert df.loc[0, "obi_level_5"] != 0.0


def test_feature_computation_is_stateless_no_carry_over_between_updates():
    """Causality/staleness: a thin update after a deep one must not inherit
    levels from the earlier call (no forward-fill, no cached book)."""
    deep = _thin_book_features(levels=10)
    thin = _thin_book_features(levels=2)
    assert deep["bid_depth"] == 10
    assert thin["bid_depth"] == 2
    assert thin["bids_price"] == [100.0, 99.0]
    assert thin["obi_level_3"] is None
    # And repeating the identical input is deterministic.
    again = _thin_book_features(levels=2)
    for key in ("bids_price", "bids_qty", "asks_price", "asks_qty", "bid_depth",
                "ask_depth", "obi", "obi_level_1", "obi_level_3", "obi_level_5"):
        assert thin[key] == again[key]
