"""P1 research-dataset assembler integrity audit: adversarial regressions.

Each test pins a demonstrated defect (or a contract the audit confirmed) in
``collector/pipeline/dataset_assembler.py``. Grid is 1000 ms to keep the
864 000-row-per-day fixtures small; boundary offsets scale with it.
"""
import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.pipeline.dataset_assembler import assemble_dataset

DATE = "2026-06-03"
START = int(pd.Timestamp(f"{DATE} 00:00:00", tz="UTC").timestamp() * 1000)
END = START + 86_400_000
G = 1000


def _write(data_dir, stream, df, hour=0, seq=0):
    d = os.path.join(data_dir, "raw", stream)
    os.makedirs(d, exist_ok=True)
    df = df.copy()
    for c in ("timestamp", "local_timestamp", "exchange_timestamp"):
        if c in df:
            df[c] = pd.to_datetime(df[c], unit="ms", utc=True).astype("datetime64[ms, UTC]")
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                   os.path.join(d, f"{DATE}-{hour:02d}-{seq:06d}.seg"))


def _base(d, ob=None, mark=None):
    ob = [START] if ob is None else ob
    mark = [START] if mark is None else mark
    _write(d, "orderbook", pd.DataFrame({"timestamp": ob, "local_timestamp": ob, "best_bid": 1.0,
                                         "best_ask": 2.0, "spread": 1.0}))
    _write(d, "markprice", pd.DataFrame({"timestamp": mark, "local_timestamp": mark, "mark_price": 1.5}))


def _trades(ts, recv=None, **kw):
    n = len(ts)
    df = {"timestamp": ts, "local_timestamp": ts if recv is None else recv,
          "trade_id": list(range(1, n + 1)), "price": 100.0, "quantity": 1.0,
          "is_buyer_maker": False, "signed_qty": 1.0}
    df.update(kw)
    return pd.DataFrame(df)


def _liq(local, exch=None, side=1, qty=5.0, price=100.0):
    n = len(local)
    return pd.DataFrame({"timestamp": local, "local_timestamp": local,
                         "exchange_timestamp": local if exch is None else exch,
                         "side": np.array([side] * n, dtype="int8"), "price": price,
                         "quantity": qty, "signed_qty": float(qty) * side})


def _run(d):
    assemble_dataset(DATE, grid_ms=G, data_dir=d)
    return pd.read_parquet(os.path.join(d, "aligned", f"{DATE}.parquet"))


# 1/2. Day boundary -----------------------------------------------------------

@pytest.mark.parametrize("offset", [-G, -G + 1, -1])
def test_previous_day_trade_cannot_enter_first_bin(tmp_path, offset):
    d = str(tmp_path)
    _base(d)
    _write(d, "trades", _trades([START + offset]))
    out = _run(d)
    assert out["trade_count"].sum() == 0
    assert out.loc[out["timestamp"] == START, "trade_count"].iloc[0] == 0


@pytest.mark.parametrize("offset", [-G + 1, -1])
def test_previous_day_liquidation_cannot_enter_first_bin(tmp_path, offset):
    d = str(tmp_path)
    _base(d)
    _write(d, "liquidation", _liq([START + offset]))
    out = _run(d)
    assert out["liquidation_count"].sum() == 0
    assert out["liquidation_notional"].sum() == 0.0


def test_start_ts_is_inclusive_and_binned_to_first_row(tmp_path):
    d = str(tmp_path)
    _base(d)
    _write(d, "trades", _trades([START, START + 1]))
    out = _run(d).set_index("timestamp")
    assert out.loc[START, "trade_count"] == 1
    assert out.loc[START + G, "trade_count"] == 1  # START+1 -> bin (START, START+G]


@pytest.mark.parametrize("offset", [0, 1, 1000])
def test_next_day_events_never_appear(tmp_path, offset):
    d = str(tmp_path)
    _base(d)
    _write(d, "trades", _trades([END + offset]))
    _write(d, "liquidation", _liq([END + offset]))
    out = _run(d)
    assert out["trade_count"].sum() == 0 and out["liquidation_count"].sum() == 0
    assert out["timestamp"].max() == END - G


def test_trailing_partial_bin_events_are_excluded_and_counted(tmp_path, capsys):
    """An in-day event after the last grid instant has no row to live in. It
    must be reported, not silently lost."""
    d = str(tmp_path)
    _base(d)
    _write(d, "trades", _trades([END - 1, END - G + 1, END - G]))
    out = _run(d).set_index("timestamp")
    log = capsys.readouterr().out
    assert out["trade_count"].sum() == 1 and out.loc[END - G, "trade_count"] == 1
    assert "2 in the trailing partial bin" in log


def test_all_events_outside_day_keeps_stream_available(tmp_path):
    d = str(tmp_path)
    _base(d)
    _write(d, "trades", _trades([START - 5]))
    _write(d, "liquidation", _liq([END + 5]))
    out = _run(d)
    assert out["trades_stream_available"].all() and out["liquidation_stream_available"].all()
    assert out["trade_count"].sum() == 0 and out["liquidation_count"].sum() == 0


# 3/4. Receive time beats exchange and processing time ------------------------

def test_receive_time_beats_exchange_timestamp_for_trades(tmp_path):
    d = str(tmp_path)
    _base(d)
    # exchange/processing say 0.5 s in; the collector only RECEIVED it at 5.5 s
    _write(d, "trades", _trades([START + 500], recv=[START + 5500],
                                exchange_timestamp=[START + 100]))
    out = _run(d).set_index("timestamp")
    assert out.loc[START, "trade_count"] == 0 and out.loc[START + 1000, "trade_count"] == 0
    assert out.loc[START + 6000, "trade_count"] == 1


def test_receive_time_beats_processing_timestamp_for_liquidations(tmp_path):
    d = str(tmp_path)
    _base(d)
    liq = _liq([START + 5500])
    liq["timestamp"] = pd.to_datetime([START + 100], unit="ms", utc=True)  # processing "earlier"
    _write(d, "liquidation", liq)
    out = _run(d).set_index("timestamp")
    assert out.loc[START + 1000, "liquidation_count"] == 0
    assert out.loc[START + 6000, "liquidation_count"] == 1


# 5/13. Determinism, duplicate availability timestamps -----------------------

def test_duplicate_availability_timestamps_are_deterministic(tmp_path):
    d = str(tmp_path)
    ob = pd.DataFrame({"timestamp": [START + 10, START + 20, START + 30],
                       "local_timestamp": [START + 500] * 3, "best_bid": [1.0, 2.0, 3.0],
                       "best_ask": [2.0, 3.0, 4.0], "spread": [1.0] * 3})
    _write(d, "orderbook", ob)
    _write(d, "markprice", pd.DataFrame({"timestamp": [START], "local_timestamp": [START], "mark_price": 1.5}))
    first = _run(d)
    second = _run(d)
    pd.testing.assert_frame_equal(first, second)
    # documented tie rule: stable sort, so the last-written row at an equal receive time wins
    assert first.loc[first["timestamp"] == START + 1000, "best_bid"].iloc[0] == 3.0


def test_repeated_assembly_is_byte_identical(tmp_path):
    d = str(tmp_path)
    _base(d, ob=[START, START + 400], mark=[START])
    _write(d, "trades", _trades([START + 50, START + 150]))
    _write(d, "liquidation", _liq([START + 60, START + 70], exch=[START + 55, START + 55]))
    out_file = os.path.join(d, "aligned", f"{DATE}.parquet")
    _run(d)
    a = open(out_file, "rb").read()
    _run(d)
    assert a == open(out_file, "rb").read()


# 6. Absent stream is not an observed zero ------------------------------------

def test_absent_trades_stream_is_distinguishable_from_observed_empty_bin(tmp_path):
    absent, present = str(tmp_path / "a"), str(tmp_path / "b")
    _base(absent)
    _base(present)
    _write(present, "trades", _trades([START + 5500]))
    out_absent, out_present = _run(absent), _run(present)
    assert not out_absent["trades_stream_available"].any()
    assert out_present["trades_stream_available"].all()
    # both show trade_count == 0 in an empty bin; only the flag tells them apart
    assert out_absent["trade_count"].iloc[0] == 0 and out_present["trade_count"].iloc[0] == 0


def test_absent_liquidation_stream_flag_unchanged(tmp_path):
    d = str(tmp_path)
    _base(d)
    out = _run(d)
    assert not out["liquidation_stream_available"].any()
    assert (out["liquidation_dup_dropped"] == 0).all()


# 7. OI outage never becomes zero ---------------------------------------------

def test_oi_outage_is_nan_and_gap_not_zero(tmp_path):
    d = str(tmp_path)
    _base(d)
    _write(d, "openinterest", pd.DataFrame({"timestamp": [START, START + 20000],
                                            "local_timestamp": [START, START + 20000],
                                            "open_interest": [0.0, 10.0]}))
    out = _run(d).set_index("timestamp")
    assert out.loc[START, "open_interest"] == 0.0 and not out.loc[START, "openinterest_gap"]
    assert np.isnan(out.loc[START + 10000, "open_interest"]) and out.loc[START + 10000, "openinterest_gap"]


# 8/9. Legacy / null availability ---------------------------------------------

def test_legacy_fallback_rows_are_flagged_not_silently_trusted(tmp_path):
    d = str(tmp_path)
    _base(d)
    legacy = _trades([START + 500]).drop(columns=["local_timestamp"])
    _write(d, "trades", legacy)
    out = _run(d).set_index("timestamp")
    assert out.loc[START + 1000, "trades_time_unknown"]
    assert not out.loc[START + 2000, "trades_time_unknown"]


def test_null_availability_row_is_dropped_loudly_not_given_a_sentinel_time(tmp_path, capsys):
    d = str(tmp_path)
    _base(d)
    df = _trades([START + 500, START + 1500])
    df["local_timestamp"] = pd.to_datetime([START + 500, None], unit="ms", utc=True).astype("datetime64[ms, UTC]")
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).astype("datetime64[ms, UTC]")
    os.makedirs(os.path.join(d, "raw", "trades"))
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                   os.path.join(d, "raw", "trades", f"{DATE}-00-000000.seg"))
    out = _run(d)
    assert out["trade_count"].sum() == 1
    assert "1 trades row(s) with a null availability timestamp" in capsys.readouterr().out


# 10. Unknown trade side is never guessed -------------------------------------

def test_unknown_trade_side_fails_loudly_instead_of_being_guessed(tmp_path):
    d = str(tmp_path)
    _base(d)
    df = _trades([START + 500, START + 600])
    df["is_buyer_maker"] = pd.array([False, None], dtype="boolean")
    _write(d, "trades", df)
    with pytest.raises((TypeError, ValueError)):
        _run(d)


def test_unknown_liquidation_side_is_not_assigned_to_a_side(tmp_path):
    d = str(tmp_path)
    _base(d)
    _write(d, "liquidation", _liq([START + 500], side=0))
    out = _run(d).set_index("timestamp")
    assert out.loc[START + 1000, "liquidation_buy_volume"] == 0.0
    assert out.loc[START + 1000, "liquidation_sell_volume"] == 0.0


# 11. Liquidation dedup is a heuristic and must say so ------------------------

def test_liquidation_heuristic_dedup_is_measurable_in_the_dataset(tmp_path):
    d = str(tmp_path)
    _base(d)
    # two receipts sharing (exchange_ts, side, price, qty): indistinguishable
    # from a redelivery with the stored evidence, so one is collapsed -- and
    # the collapse is recorded, not hidden.
    _write(d, "liquidation", _liq([START + 500, START + 600], exch=[START + 450, START + 450]))
    out = _run(d).set_index("timestamp")
    assert out.loc[START + 1000, "liquidation_count"] == 1
    assert out.loc[START + 1000, "liquidation_dup_dropped"] == 1
    assert out["liquidation_dup_dropped"].sum() == 1


def test_liquidations_with_null_exchange_timestamp_are_never_collapsed(tmp_path):
    d = str(tmp_path)
    _base(d)
    df = _liq([START + 500, START + 600])
    df["exchange_timestamp"] = pd.to_datetime([None, None], unit="ms", utc=True).astype("datetime64[ms, UTC]")
    os.makedirs(os.path.join(d, "raw", "liquidation"))
    for c in ("timestamp", "local_timestamp"):
        df[c] = pd.to_datetime(df[c], unit="ms", utc=True).astype("datetime64[ms, UTC]")
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                   os.path.join(d, "raw", "liquidation", f"{DATE}-00-000000.seg"))
    out = _run(d).set_index("timestamp")
    assert out.loc[START + 1000, "liquidation_count"] == 2
    assert out["liquidation_dup_dropped"].sum() == 0


def test_liquidation_dedup_keeps_earliest_received_copy_regardless_of_file_order(tmp_path):
    """Which copy survives decides which bin the event lands in. It must be
    the earliest-received one, not whichever segment file was read first."""
    d = str(tmp_path)
    _base(d)
    _write(d, "liquidation", _liq([START + 5500], exch=[START + 450]), hour=0, seq=0)  # later receipt, earlier file
    _write(d, "liquidation", _liq([START + 500], exch=[START + 450]), hour=1, seq=0)   # earlier receipt, later file
    out = _run(d).set_index("timestamp")
    assert out.loc[START + 1000, "liquidation_count"] == 1
    assert out.loc[START + 6000, "liquidation_count"] == 0
