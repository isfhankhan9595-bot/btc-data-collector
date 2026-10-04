"""Research-integrity audit: trades must carry a day-level availability
signal distinguishing "no qualifying trades segment was found for this day"
from "qualifying trade data was found somewhere in this day" -- the same
contract liquidations already had via ``liquidation_stream_available`` (see
``tests/test_dataset_assembler_oi_liquidation.py``). Before this fix,
``trades_stream_available`` did not exist: a day with no trades collector
output and a real zero-trade day produced byte-identical aggregate columns
with no distinguishing signal anywhere in the schema.

IMPORTANT -- what the flag does and does not prove: ``trades_stream_
available=True`` means the assembler found qualifying trade data for that
day. It does NOT mean every grid bin in that day had continuous stream
coverage, and an empty bin under ``True`` is not a "confident" or "proven"
zero at the bin level -- it only means the day is not in the data-absent
category. A mid-day WS reconnect gap within an otherwise-``True`` day is
indistinguishable from a genuine lull; that is a real, unresolved
limitation this test file does not attempt to close (see
docs/RESEARCH_DATASET_TIME_CONTRACT.md).
"""
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.pipeline.dataset_assembler import assemble_dataset

_TS_COLS = ("timestamp", "local_timestamp", "exchange_timestamp")
DATE_STR = "2026-06-04"
INSTRUMENT_KEY = "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT"


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


# ---------------------------------------------------------------------------
# The core distinction this fix adds
# ---------------------------------------------------------------------------

def test_missing_trades_stream_is_flagged_unavailable_not_silently_zero(data_dir):
    """No trades segment file at all for this day: trade_count is 0
    everywhere (unavoidable, schema needs a value), but
    trades_stream_available must be False everywhere -- the signal that
    those zeros carry no evidentiary weight."""
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    # Deliberately no trades segment written at all.

    df = _assemble(data_dir)

    assert (df["trade_count"] == 0).all()
    assert not df["trades_stream_available"].any()


def test_trades_present_somewhere_in_day_sets_day_level_available(data_dir):
    """Qualifying trade data WAS found somewhere in the day (one real
    trade); a bin far from that trade has trade_count=0 but
    trades_stream_available=True. This only means the day is not in the
    data-absent category -- it does not by itself certify that this
    specific far-away bin had continuous stream coverage (see the module
    docstring)."""
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    trades_df = pd.DataFrame({
        "timestamp": [start_ts + 1000], "local_timestamp": [start_ts + 1000],
        "exchange_timestamp": [start_ts + 1000],
        "trade_id": [1], "price": [100.0], "quantity": [1.0],
        "is_buyer_maker": [False], "signed_qty": [1.0],
        "instrument_key": [INSTRUMENT_KEY],
    })
    _write_parquet(trades_df, _seg(data_dir, "trades"))

    df = _assemble(data_dir)

    idx_far_from_any_trade = 50_000  # 5000s into an 86400s day, nowhere near t=1s
    assert df.loc[idx_far_from_any_trade, "trade_count"] == 0
    assert df.loc[idx_far_from_any_trade, "trades_stream_available"]

    idx_1000 = 10  # the grid bin the one real trade falls into
    assert df.loc[idx_1000, "trade_count"] == 1
    assert df.loc[idx_1000, "trades_stream_available"]


def test_missing_and_present_trades_produce_the_same_zero_but_different_flags(data_dir):
    """The exact regression this fix closes: before it, a day with no
    trades collector output at all, and a bin far from a real trade on a
    day the stream genuinely ran, were byte-identical in every aggregate
    column. Confirms the aggregate columns alone are genuinely ambiguous --
    only the new flag disambiguates them -- which is the whole point of
    adding it. (Note: a segment file that exists but has zero rows is a
    separate edge case, not exercised here -- it currently reads as
    "unavailable" too, the same pre-existing behavior the already-reviewed
    liquidation flag has, inherited unchanged by this fix rather than
    silently reinterpreted.)"""
    start_ts = _start_ts()

    # Scenario A: no trades stream at all.
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    df_missing = _assemble(data_dir)

    # Scenario B: trades stream genuinely collected (one real trade early in
    # the day); a bin far from it is day-level "available" but is not
    # itself individually proven to have had continuous coverage.
    import shutil
    data_dir_b = data_dir + "-b"
    shutil.copytree(data_dir, data_dir_b)
    trades_df = pd.DataFrame({
        "timestamp": [start_ts + 1000], "local_timestamp": [start_ts + 1000],
        "exchange_timestamp": [start_ts + 1000],
        "trade_id": [1], "price": [100.0], "quantity": [1.0],
        "is_buyer_maker": [False], "signed_qty": [1.0],
        "instrument_key": [INSTRUMENT_KEY],
    })
    _write_parquet(trades_df, _seg(data_dir_b, "trades"))
    assemble_dataset(DATE_STR, grid_ms=100, data_dir=data_dir_b)
    df_present = pd.read_parquet(os.path.join(data_dir_b, "aligned", f"{DATE_STR}.parquet"))

    idx_far_from_any_trade = 50_000
    # The aggregate column is identical at this bin in both scenarios --
    # genuinely ambiguous without the flag.
    assert df_missing.loc[idx_far_from_any_trade, "trade_count"] == 0
    assert df_present.loc[idx_far_from_any_trade, "trade_count"] == 0

    # The flag is what tells them apart.
    assert not df_missing["trades_stream_available"].any()
    assert df_present.loc[idx_far_from_any_trade, "trades_stream_available"]


def test_day_level_flag_does_not_detect_an_intra_day_outage(data_dir):
    """Explicit demonstration of the documented limitation: this flag is
    day-level only. A single trade early in the day sets
    trades_stream_available=True for the WHOLE day, including bins many
    hours later that could equally represent a genuine lull or an
    undetected mid-day collector outage -- the flag cannot and does not
    distinguish them. This test does not attempt to fix that (would require
    joining the quality-event stream against the grid); it exists only to
    pin the limitation so it cannot be silently "fixed" into an overclaim
    later without a test noticing."""
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    # One trade in the first grid bin, nothing else all day -- indistinguishable,
    # from this flag alone, from "the trades stream went down right after."
    trades_df = pd.DataFrame({
        "timestamp": [start_ts], "local_timestamp": [start_ts], "exchange_timestamp": [start_ts],
        "trade_id": [1], "price": [100.0], "quantity": [1.0],
        "is_buyer_maker": [False], "signed_qty": [1.0],
        "instrument_key": [INSTRUMENT_KEY],
    })
    _write_parquet(trades_df, _seg(data_dir, "trades"))

    df = _assemble(data_dir)

    late_in_day_idx = len(df) - 1  # the last grid bin of the day
    assert df.loc[late_in_day_idx, "trade_count"] == 0
    # Still True: the flag has no mechanism to detect that the stream could
    # have gone silent hours ago. This is the known, accepted limitation,
    # not a bug -- see docs/RESEARCH_DATASET_TIME_CONTRACT.md.
    assert df.loc[late_in_day_idx, "trades_stream_available"]


def test_trades_stream_available_matches_liquidation_stream_available_semantics(data_dir):
    """Symmetry check: both flags must agree on the same day, same
    missing-vs-present-but-empty scenarios -- confirms this fix actually
    mirrors the existing, already-reviewed liquidation pattern rather than
    inventing a new, possibly-inconsistent one."""
    start_ts = _start_ts()
    _write_minimal_orderbook_and_mark(data_dir, start_ts)
    # Neither trades nor liquidation segments written.

    df = _assemble(data_dir)

    assert not df["trades_stream_available"].any()
    assert not df["liquidation_stream_available"].any()
    assert df["trades_stream_available"].dtype == df["liquidation_stream_available"].dtype
