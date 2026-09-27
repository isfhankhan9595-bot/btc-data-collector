"""P0-7 adversarial tests: open interest and liquidation in the research
dataset assembler. See ``dataset_assembler.py``'s module docstring
("P0-7: open interest and liquidations") for the contract these enforce.
"""
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.pipeline.dataset_assembler import assemble_dataset

_TS_COLS = ("timestamp", "local_timestamp", "exchange_timestamp")
DATE_STR = "2026-06-04"


def _write_parquet(df: pd.DataFrame, path: str) -> None:
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
    for stream in ("orderbook", "trades", "markprice", "openinterest", "liquidation"):
        os.makedirs(os.path.join(d, "raw", stream), exist_ok=True)
    return d


def _seg(data_dir, stream):
    return os.path.join(data_dir, "raw", stream, f"{DATE_STR}-00-000000.seg")


def _start_ts():
    return int(pd.Timestamp(f"{DATE_STR} 00:00:00", tz="UTC").timestamp() * 1000)


def _write_minimal_orderbook_and_mark(data_dir, start_ts):
    """assemble_dataset requires orderbook+markprice to proceed at all; every
    test below is about OI/liquidation, so give both one harmless row."""
    ob_df = pd.DataFrame({
        "timestamp": [start_ts], "local_timestamp": [start_ts], "exchange_timestamp": [start_ts],
        "best_bid": [100.0], "best_ask": [101.0], "mid_price": [100.5],
    })
    _write_parquet(ob_df, _seg(data_dir, "orderbook"))
    mark_df = pd.DataFrame({
        "timestamp": [start_ts], "local_timestamp": [start_ts], "exchange_timestamp": [start_ts],
        "mark_price": [100.0],
    })
    _write_parquet(mark_df, _seg(data_dir, "markprice"))


def _assemble(data_dir):
    assemble_dataset(DATE_STR, grid_ms=100, data_dir=data_dir)
    return pd.read_parquet(os.path.join(data_dir, "aligned", f"{DATE_STR}.parquet"))


INSTRUMENT_KEY = "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT"


# ---------------------------------------------------------------------------
# 1. OI exists and is correctly assembled
# ---------------------------------------------------------------------------

def test_oi_is_assembled_using_receive_time(data_dir):
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    oi_df = pd.DataFrame({
        "timestamp": [start_ts + 6000], "local_timestamp": [start_ts + 5000],
        "exchange_timestamp": [start_ts + 1000],
        "open_interest": [12345.678], "instrument_key": [INSTRUMENT_KEY],
    })
    _write_parquet(oi_df, _seg(data_dir, "openinterest"))

    df = _assemble(data_dir)

    idx_4000, idx_5000 = 40, 50
    assert pd.isna(df.loc[idx_4000, "open_interest"])
    assert df.loc[idx_4000, "openinterest_gap"]
    assert df.loc[idx_5000, "open_interest"] == 12345.678
    assert not df.loc[idx_5000, "openinterest_gap"]


# ---------------------------------------------------------------------------
# 2. Liquidation exists and is correctly assembled
# ---------------------------------------------------------------------------

def test_liquidation_is_assembled_with_buy_sell_net_and_notional(data_dir):
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    liq_df = pd.DataFrame({
        "timestamp": [start_ts + 1100, start_ts + 1150],
        "local_timestamp": [start_ts + 1000, start_ts + 1050],
        "exchange_timestamp": [start_ts + 900, start_ts + 950],
        "side": [1, -1],           # BUY (short liquidated), SELL (long liquidated)
        "price": [100.0, 200.0],
        "quantity": [2.0, 1.0],
        "signed_qty": [2.0, -1.0],
        "order_status": ["FILLED", "FILLED"],
        "time_in_force": ["IOC", "IOC"],
        "instrument_key": [INSTRUMENT_KEY, INSTRUMENT_KEY],
    })
    _write_parquet(liq_df, _seg(data_dir, "liquidation"))

    df = _assemble(data_dir)

    idx_1000 = 10  # first liquidation's receive-derived bin
    assert df.loc[idx_1000, "liquidation_count"] == 1
    assert df.loc[idx_1000, "liquidation_buy_volume"] == 2.0
    assert df.loc[idx_1000, "liquidation_sell_volume"] == 0.0
    assert df.loc[idx_1000, "liquidation_net_volume"] == 2.0
    assert df.loc[idx_1000, "liquidation_notional"] == 200.0
    assert df.loc[idx_1000, "liquidation_stream_available"]

    idx_1050 = 11  # second liquidation's own receive-derived bin (different!)
    assert df.loc[idx_1050, "liquidation_count"] == 1
    assert df.loc[idx_1050, "liquidation_sell_volume"] == 1.0
    assert df.loc[idx_1050, "liquidation_net_volume"] == -1.0
    assert df.loc[idx_1050, "liquidation_notional"] == 200.0


# ---------------------------------------------------------------------------
# 3. Missing OI does not become zero
# ---------------------------------------------------------------------------

def test_missing_oi_is_nan_never_zero(data_dir):
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    # no openinterest segment written at all for this day

    df = _assemble(data_dir)

    assert df["open_interest"].isna().all()
    assert (df["open_interest"] == 0).sum() == 0
    assert df["openinterest_gap"].all()


# ---------------------------------------------------------------------------
# 4. Missing liquidation does not automatically become zero
# ---------------------------------------------------------------------------

def test_missing_liquidation_stream_is_flagged_unavailable_not_silently_zero(data_dir):
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    # no liquidation segment written at all for this day

    df = _assemble(data_dir)

    # The numeric columns are 0 (nothing to sum), but the dedicated flag
    # makes explicit that this 0 carries no evidentiary weight -- it is NOT
    # the same claim as "we watched and confirmed zero liquidations".
    assert (df["liquidation_count"] == 0).all()
    assert not df["liquidation_stream_available"].any()


def test_liquidation_present_but_bin_empty_is_a_confident_zero(data_dir):
    """Contrast case for the test above: when the stream WAS collected that
    day, an empty bin is a confidently observed zero, and is flagged as
    such (available=True)."""
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    liq_df = pd.DataFrame({
        "timestamp": [start_ts + 50100], "local_timestamp": [start_ts + 50000],
        "exchange_timestamp": [start_ts + 50000],
        "side": [1], "price": [100.0], "quantity": [1.0], "signed_qty": [1.0],
        "order_status": ["FILLED"], "time_in_force": ["IOC"],
        "instrument_key": [INSTRUMENT_KEY],
    })
    _write_parquet(liq_df, _seg(data_dir, "liquidation"))

    df = _assemble(data_dir)

    idx_far_from_any_liquidation = 5  # T=+500ms, nowhere near the one event
    assert df.loc[idx_far_from_any_liquidation, "liquidation_count"] == 0
    assert df.loc[idx_far_from_any_liquidation, "liquidation_stream_available"]


# ---------------------------------------------------------------------------
# 5. Stale OI is marked stale
# ---------------------------------------------------------------------------

def test_stale_oi_is_flagged_gap_beyond_tolerance(data_dir):
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    oi_df = pd.DataFrame({
        "timestamp": [start_ts + 1000], "local_timestamp": [start_ts + 1000],
        "exchange_timestamp": [start_ts + 1000],
        "open_interest": [500.0], "instrument_key": [INSTRUMENT_KEY],
    })
    _write_parquet(oi_df, _seg(data_dir, "openinterest"))

    df = _assemble(data_dir)

    idx_6001 = 61  # 5100ms after the OI reading -> beyond the 5000ms tolerance
    assert pd.isna(df.loc[idx_6001, "open_interest"])
    assert df.loc[idx_6001, "openinterest_gap"]


# ---------------------------------------------------------------------------
# 6/8/9. Future OI receive timestamp cannot leak; exchange/processing time
# cannot substitute for receive time
# ---------------------------------------------------------------------------

def test_oi_future_receive_does_not_leak_early_despite_old_exchange_timestamp(data_dir):
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    oi_df = pd.DataFrame({
        "timestamp": [start_ts + 6000],       # processing time
        "local_timestamp": [start_ts + 5000],  # receive time (availability)
        "exchange_timestamp": [start_ts + 1000],  # venue clock, old
        "open_interest": [777.0], "instrument_key": [INSTRUMENT_KEY],
    })
    _write_parquet(oi_df, _seg(data_dir, "openinterest"))

    df = _assemble(data_dir)

    idx_4000 = 40
    idx_5000 = 50
    assert pd.isna(df.loc[idx_4000, "open_interest"]), (
        "OI leaked before its local_receive_ts despite an old exchange timestamp"
    )
    assert df.loc[idx_5000, "open_interest"] == 777.0


def test_oi_processing_delay_does_not_delay_availability(data_dir):
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    oi_df = pd.DataFrame({
        "timestamp": [start_ts + 50000],       # processing time, 5s lag
        "local_timestamp": [start_ts + 45000],  # actual receive time
        "exchange_timestamp": [start_ts + 44900],
        "open_interest": [999.0], "instrument_key": [INSTRUMENT_KEY],
    })
    _write_parquet(oi_df, _seg(data_dir, "openinterest"))

    df = _assemble(data_dir)

    idx_45400 = 454  # 400ms after receive, well within the 5000ms tolerance
    assert df.loc[idx_45400, "open_interest"] == 999.0


# ---------------------------------------------------------------------------
# 7. Future liquidation receive timestamp cannot leak backward
# ---------------------------------------------------------------------------

def test_liquidation_future_receive_does_not_leak_into_earlier_bin(data_dir):
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    liq_df = pd.DataFrame({
        "timestamp": [start_ts + 10100], "local_timestamp": [start_ts + 10000],
        "exchange_timestamp": [start_ts + 1000],  # old venue clock
        "side": [1], "price": [50.0], "quantity": [3.0], "signed_qty": [3.0],
        "order_status": ["FILLED"], "time_in_force": ["IOC"],
        "instrument_key": [INSTRUMENT_KEY],
    })
    _write_parquet(liq_df, _seg(data_dir, "liquidation"))

    df = _assemble(data_dir)

    idx_9000 = 90  # before the receive-derived bin
    idx_10000 = 100  # the receive-derived bin itself
    assert df.loc[idx_9000, "liquidation_count"] == 0
    assert df.loc[idx_10000, "liquidation_count"] == 1


# ---------------------------------------------------------------------------
# 10/11. Different instruments/venues cannot cross-contaminate
# ---------------------------------------------------------------------------

def test_mismatched_instrument_key_is_dropped_not_blended(data_dir):
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    oi_df = pd.DataFrame({
        "timestamp": [start_ts + 1000, start_ts + 2000],
        "local_timestamp": [start_ts + 1000, start_ts + 2000],
        "exchange_timestamp": [start_ts + 1000, start_ts + 2000],
        "open_interest": [111.0, 222.0],
        "instrument_key": [INSTRUMENT_KEY, "BYBIT|linear_perpetual|BTC-USDT|BTCUSDT"],
    })
    _write_parquet(oi_df, _seg(data_dir, "openinterest"))

    df = _assemble(data_dir)

    idx_2000 = 20
    # The mismatched-venue row must never appear: this series stays exactly
    # what the earlier (matching) row established, not the later foreign one.
    assert df.loc[idx_2000, "open_interest"] == 111.0


# ---------------------------------------------------------------------------
# 12. OI unit conversion cannot silently change magnitude
# ---------------------------------------------------------------------------

def test_oi_value_passes_through_unconverted(data_dir):
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    oi_df = pd.DataFrame({
        "timestamp": [start_ts], "local_timestamp": [start_ts], "exchange_timestamp": [start_ts],
        "open_interest": [54321.987654], "instrument_key": [INSTRUMENT_KEY],
    })
    _write_parquet(oi_df, _seg(data_dir, "openinterest"))

    df = _assemble(data_dir)

    assert df.loc[0, "open_interest"] == 54321.987654


# ---------------------------------------------------------------------------
# 13. Duplicate liquidation events cannot double-count
# ---------------------------------------------------------------------------

def test_duplicate_liquidation_event_is_not_double_counted(data_dir):
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    # Same underlying event redelivered by the WS (identical exchange_ts,
    # side, price, quantity) with a different local receive stamp -- as a
    # reconnect-triggered redelivery might arrive at a slightly later wall
    # clock read.
    liq_df = pd.DataFrame({
        "timestamp": [start_ts + 1100, start_ts + 1300],
        "local_timestamp": [start_ts + 1000, start_ts + 1200],
        "exchange_timestamp": [start_ts + 900, start_ts + 900],
        "side": [1, 1], "price": [100.0, 100.0], "quantity": [5.0, 5.0],
        "signed_qty": [5.0, 5.0],
        "order_status": ["FILLED", "FILLED"], "time_in_force": ["IOC", "IOC"],
        "instrument_key": [INSTRUMENT_KEY, INSTRUMENT_KEY],
    })
    _write_parquet(liq_df, _seg(data_dir, "liquidation"))

    df = _assemble(data_dir)

    total_count = df["liquidation_count"].sum()
    total_volume = df["liquidation_buy_volume"].sum()
    assert total_count == 1, "redelivered duplicate was double-counted"
    assert total_volume == 5.0, "redelivered duplicate volume was double-counted"


# ---------------------------------------------------------------------------
# 14. Duplicate OI observations cannot create false aggregates
# ---------------------------------------------------------------------------

def test_duplicate_oi_observation_does_not_create_a_false_aggregate(data_dir):
    """OI is a point-in-time merge_asof value, never summed -- two readings
    at the identical receive time must resolve to a single, stable value,
    never a sum/average that would misrepresent the true reading."""
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    oi_df = pd.DataFrame({
        "timestamp": [start_ts + 1000, start_ts + 1001],
        "local_timestamp": [start_ts + 1000, start_ts + 1000],
        "exchange_timestamp": [start_ts + 1000, start_ts + 1000],
        "open_interest": [1000.0, 1000.0],
        "instrument_key": [INSTRUMENT_KEY, INSTRUMENT_KEY],
    })
    _write_parquet(oi_df, _seg(data_dir, "openinterest"))

    df = _assemble(data_dir)

    idx_1000 = 10
    # Must be exactly one of the readings, never a summed/doubled value.
    assert df.loc[idx_1000, "open_interest"] == 1000.0


# ---------------------------------------------------------------------------
# 15. Empty liquidation interval has explicitly defined semantics
# ---------------------------------------------------------------------------
# Covered directly by test_missing_liquidation_stream_is_flagged_unavailable_
# not_silently_zero and test_liquidation_present_but_bin_empty_is_a_confident_
# zero above: the two cases are distinguished by liquidation_stream_available.


# ---------------------------------------------------------------------------
# 17. Existing P0-6 causal tests remain passing: exercised by running
# test_dataset_assembler.py / test_dataset_assembler_causal_contract.py
# alongside this file (not duplicated here).
# ---------------------------------------------------------------------------
