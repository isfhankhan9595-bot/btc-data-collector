"""Research-dataset availability truth: observed zero vs no evidence.

The assembler used to carry ONE day-level flag per stream
(``trades_stream_available`` / ``liquidation_stream_available``: "a segment
exists for this day"). A mid-day collector disconnect therefore produced rows
that were bit-for-bit identical to a genuinely quiet bin, so an empty bin could
be read as "zero activity" while the collector was not listening. These tests
pin the per-bin proof (``trades_bin_observed`` / ``liquidation_bin_observed``)
that closes that gap, using the semantic result -- never just "no exception".

Grid is 1000 ms (86 400 rows/day). With ``G = 1000`` and a 5 000 ms evidence
tolerance an observation at ``t`` covers ``[t, t + 5000)``; a grid row ``T``
owns the receive-time bin ``(T - G, T]``, observed only if EVERY millisecond of
it is covered. All expected bin sets below are derived from that rule.
"""
import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.pipeline.dataset_assembler import _bin_observed, assemble_dataset

DATE = "2026-06-03"
START = int(pd.Timestamp(f"{DATE} 00:00:00", tz="UTC").timestamp() * 1000)
H = 3_600_000
END = START + 24 * H
G = 1000
TOL = 5000


# -- fixtures ----------------------------------------------------------------

def _write(data_dir, stream, df, hour=0, seq=0):
    d = os.path.join(data_dir, "raw", stream)
    os.makedirs(d, exist_ok=True)
    df = df.copy()
    for c in ("timestamp", "local_timestamp", "exchange_timestamp"):
        if c in df:
            df[c] = pd.to_datetime(df[c], unit="ms", utc=True).astype("datetime64[ms, UTC]")
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                   os.path.join(d, f"{DATE}-{hour:02d}-{seq:06d}.seg"))


def _write_by_hour(data_dir, stream, df, ts_col="local_timestamp"):
    hour = (df[ts_col] - START) // H
    for h, part in df.groupby(hour):
        _write(data_dir, stream, part.reset_index(drop=True), hour=int(h))


def _ob(d):
    _write(d, "orderbook", pd.DataFrame({"timestamp": [START], "local_timestamp": [START],
                                         "best_bid": 1.0, "best_ask": 2.0, "spread": 1.0}))


def _mark_one(d):
    _write(d, "markprice", pd.DataFrame({"timestamp": [START], "local_timestamp": [START],
                                         "mark_price": 1.5}))


def _base(d):
    _ob(d)
    _mark_one(d)


def _every_second(skip=()):
    t = np.arange(START, END, 1000, dtype=np.int64)
    keep = np.ones(len(t), dtype=bool)
    for a, b in skip:
        keep &= ~((t >= a) & (t < b))
    return t[keep]


def _mark_dense(d, skip=()):
    t = _every_second(skip)
    _write_by_hour(d, "markprice", pd.DataFrame({"timestamp": t, "local_timestamp": t, "mark_price": 1.5}))


def _trades(ts, recv=None, **kw):
    ts = np.asarray(ts, dtype=np.int64)
    n = len(ts)
    df = {"timestamp": ts, "local_timestamp": ts if recv is None else np.asarray(recv, dtype=np.int64),
          "trade_id": np.arange(1, n + 1), "price": 100.0, "quantity": 1.0,
          "is_buyer_maker": False, "signed_qty": 1.0}
    df.update(kw)
    return pd.DataFrame(df)


def _liq(local, side=1, qty=5.0, price=100.0):
    local = np.asarray(local, dtype=np.int64)
    n = len(local)
    return pd.DataFrame({"timestamp": local, "local_timestamp": local, "exchange_timestamp": local,
                         "side": np.array([side] * n, dtype="int8"), "price": price,
                         "quantity": qty, "signed_qty": float(qty) * side})


def _run(d):
    assemble_dataset(DATE, grid_ms=G, data_dir=d)
    return pd.read_parquet(os.path.join(d, "aligned", f"{DATE}.parquet")).set_index("timestamp")


def _observed_ts(out, col):
    return out.index[out[col].to_numpy()].to_numpy()


def _expected_observed(unobserved_windows):
    """All grid rows except those whose T lies in an inclusive (lo, hi) window."""
    grid = np.arange(START, END, G)
    keep = np.ones(len(grid), dtype=bool)
    for lo, hi in unobserved_windows:
        keep &= ~((grid >= lo) & (grid <= hi))
    return grid[keep]


# -- 1. the headline defect: intra-day outage vs observed zero ----------------

def test_intraday_trade_outage_is_not_an_observed_zero(tmp_path):
    d = str(tmp_path)
    _base(d)
    outage = (START + 5 * H, START + 5 * H + 600_000)          # 05:00-05:10 collector down
    quiet = START + 12 * H + 10_000                            # alive, simply nothing traded
    t = _every_second(skip=[outage, (quiet, quiet + 1000)])
    _write_by_hour(d, "trades", _trades(t))
    out = _run(d)

    mid_outage = out.loc[START + 5 * H + 300_000]
    quiet_row = out.loc[quiet]
    # Same stored zeros ...
    assert mid_outage["trade_count"] == 0 and quiet_row["trade_count"] == 0
    # ... and the day-level flag cannot tell them apart (this is WHY it is not enough) ...
    assert mid_outage["trades_stream_available"] and quiet_row["trades_stream_available"]
    # ... but the per-bin proof does.
    assert not mid_outage["trades_bin_observed"]
    assert quiet_row["trades_bin_observed"]

    # Exact bin set: last trade 04:59:59 covers to 05:00:04; first resumed trade
    # at 05:10:00 only covers its own bin onward (the ms before it are a gap).
    unobserved = (outage[0] + 4000, outage[1])
    assert np.array_equal(_observed_ts(out, "trades_bin_observed"), _expected_observed([unobserved]))

    # A trade that arrives in a partly-unobserved bin is real, but the count is a
    # lower bound: positive count with observed == False is allowed and honest.
    recovery = out.loc[outage[1]]
    assert recovery["trade_count"] == 1 and not recovery["trades_bin_observed"]
    assert out.loc[outage[1] + G, "trades_bin_observed"]

    # per-bin proof implies the day-level flag, never the reverse
    assert (out["trades_stream_available"] | ~out["trades_bin_observed"]).all()
    assert out["trades_bin_observed"].dtype == bool


def test_intraday_liquidation_outage_uses_shared_connection_evidence(tmp_path):
    d = str(tmp_path)
    _ob(d)
    outage = (START + 8 * H, START + 8 * H + 1_200_000)        # market socket down 08:00-08:20
    _mark_dense(d, skip=[outage])
    inside = START + 8 * H + 600_000                           # a liquidation frame DOES arrive at 08:10:00
    liq_t = [START + 3 * H + 7000, inside, START + 15 * H + 9000]
    _write_by_hour(d, "liquidation", _liq(liq_t))
    out = _run(d)

    dead, quiet = out.loc[START + 8 * H + 300_000], out.loc[START + 12 * H]
    assert dead["liquidation_count"] == 0 and quiet["liquidation_count"] == 0
    assert dead["liquidation_stream_available"] and quiet["liquidation_stream_available"]
    assert not dead["liquidation_bin_observed"] and quiet["liquidation_bin_observed"]

    # a liquidation frame is itself proof the connection was delivering, from its
    # own receive time forward -- and only forward
    expected = _expected_observed([(outage[0] + 4000, inside),
                                   (inside + TOL, outage[1])])
    assert np.array_equal(_observed_ts(out, "liquidation_bin_observed"), expected)
    at_inside = out.loc[inside]
    assert at_inside["liquidation_count"] == 1 and not at_inside["liquidation_bin_observed"]
    assert out.loc[inside + G, "liquidation_bin_observed"]


def test_no_segment_means_no_proof_even_when_other_streams_are_dense(tmp_path):
    """A fully-alive order book + markPrice cannot manufacture proof for a stream
    that has no segment at all: no evidence is never "all observed"."""
    d = str(tmp_path)
    _mark_dense(d)
    t = _every_second()
    _write_by_hour(d, "orderbook", pd.DataFrame({"timestamp": t, "local_timestamp": t,
                                                 "best_bid": 1.0, "best_ask": 2.0, "spread": 1.0}))
    out = _run(d)
    assert not out["liquidation_stream_available"].any()
    assert not out["liquidation_bin_observed"].any()
    assert not out["trades_stream_available"].any()
    assert not out["trades_bin_observed"].any()


def test_orderbook_socket_frames_are_not_evidence_for_the_market_socket(tmp_path):
    """Order book rides the separate "public" socket. A dense order book must not
    vouch for liquidations; only market-socket frames (trades/markPrice/forceOrder)
    do. Here markPrice is a single row, so only that row and the liquidation's own
    receipt provide proof."""
    d = str(tmp_path)
    t = _every_second()
    _write_by_hour(d, "orderbook", pd.DataFrame({"timestamp": t, "local_timestamp": t,
                                                 "best_bid": 1.0, "best_ask": 2.0, "spread": 1.0}))
    _mark_one(d)
    _write(d, "liquidation", _liq([START + 12 * H]))
    out = _run(d)
    expected = [START + k * G for k in range(0, 5)] + [START + 12 * H + k * G for k in range(1, 5)]
    assert list(_observed_ts(out, "liquidation_bin_observed")) == expected
    assert not out["orderbook_gap"].any()                   # the book itself IS dense and fresh


# -- 2. coverage shape: leading / trailing / isolated / absent / partial -------

def test_leading_outage_is_unobserved(tmp_path):
    d = str(tmp_path)
    _base(d)
    first = START + 600_000
    t = np.arange(first, END, 1000, dtype=np.int64)
    _write_by_hour(d, "trades", _trades(t))
    out = _run(d)
    assert np.array_equal(_observed_ts(out, "trades_bin_observed"),
                          _expected_observed([(START, first)]))
    assert out["trades_stream_available"].all()          # day-level flag stays True: not proof


def test_trailing_outage_is_unobserved(tmp_path):
    d = str(tmp_path)
    _base(d)
    last = START + 18 * H
    t = np.arange(START, last + 1000, 1000, dtype=np.int64)
    _write_by_hour(d, "trades", _trades(t))
    out = _run(d)
    assert np.array_equal(_observed_ts(out, "trades_bin_observed"),
                          _expected_observed([(last + TOL, END)]))


def test_isolated_observation_covers_only_its_tolerance(tmp_path):
    d = str(tmp_path)
    _base(d)
    _write(d, "trades", _trades([START + 12 * H]))
    out = _run(d)
    seen = _observed_ts(out, "trades_bin_observed")
    assert list(seen) == [START + 12 * H + k * G for k in (1, 2, 3, 4)]
    assert out["trade_count"].sum() == 1


def test_partial_day_capture_does_not_vouch_for_the_rest_of_the_day(tmp_path):
    d = str(tmp_path)
    _base(d)
    t = np.arange(START, START + 6 * H, 1000, dtype=np.int64)       # hours 00-05 only
    _write_by_hour(d, "trades", _trades(t))
    out = _run(d)
    assert out["trades_stream_available"].all()
    assert np.array_equal(_observed_ts(out, "trades_bin_observed"),
                          _expected_observed([(START + 6 * H - 1000 + TOL, END)]))


def test_stream_absent_or_files_outside_the_day_prove_nothing(tmp_path):
    absent, outside = str(tmp_path / "a"), str(tmp_path / "b")
    for d in (absent, outside):
        _base(d)
    # a segment exists for the day, but every event in it is outside the grid
    _write(outside, "trades", _trades([START - 60_000, END + 60_000]))
    for d in (absent, outside):
        out = _run(d)
        assert not out["trades_bin_observed"].any()
        assert out["trade_count"].sum() == 0
    assert not _run(absent)["trades_stream_available"].any()
    assert _run(outside)["trades_stream_available"].all()      # day-level only; per-bin says no


# -- 3. causality / clocks ----------------------------------------------------

def test_future_observation_cannot_backfill_earlier_bins(tmp_path):
    d = str(tmp_path)
    _base(d)
    _write(d, "trades", _trades([START + 12 * H, START + 12 * H + 1000]))
    out = _run(d)
    seen = _observed_ts(out, "trades_bin_observed")
    assert seen.min() > START + 12 * H                     # nothing before the first receipt
    assert not out.loc[START:START + 12 * H, "trades_bin_observed"].any()


def test_only_the_receive_clock_is_evidence_not_exchange_or_processing_time(tmp_path):
    d = str(tmp_path)
    _base(d)
    recv = START + 12 * H
    df = _trades([START], recv=[recv])                     # processing ts = day start ...
    df["exchange_timestamp"] = END - 1000                  # ... exchange ts = day end (future)
    _write(d, "trades", df)
    out = _run(d)
    assert list(_observed_ts(out, "trades_bin_observed")) == [recv + k * G for k in (1, 2, 3, 4)]


def test_null_receive_time_row_is_not_evidence(tmp_path):
    d = str(tmp_path)
    _base(d)
    df = _trades([START + 12 * H, START + 13 * H])
    df["local_timestamp"] = pd.to_datetime([START + 12 * H, None], unit="ms", utc=True).astype("datetime64[ms, UTC]")
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).astype("datetime64[ms, UTC]")
    os.makedirs(os.path.join(d, "raw", "trades"))
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                   os.path.join(d, "raw", "trades", f"{DATE}-12-000000.seg"))
    out = _run(d)
    assert list(_observed_ts(out, "trades_bin_observed")) == [START + 12 * H + k * G for k in (1, 2, 3, 4)]


def test_out_of_order_and_duplicate_rows_do_not_change_the_proof(tmp_path):
    base_t = _every_second(skip=[(START + 2 * H, START + 2 * H + 90_000)])
    rng = np.random.default_rng(7)
    shuffled = np.concatenate([base_t, base_t[:500], base_t[-500:]])      # duplicates
    rng.shuffle(shuffled)                                                  # out of order
    results = []
    for name, t in (("sorted", base_t), ("messy", shuffled)):
        d = str(tmp_path / name)
        _base(d)
        # two segment files in the same hour, deliberately different row order
        half = len(t) // 2
        _write(d, "trades", _trades(t[:half]), hour=0, seq=0)
        _write(d, "trades", _trades(t[half:]), hour=0, seq=1)
        results.append(_run(d)["trades_bin_observed"].to_numpy())
    assert np.array_equal(results[0], results[1])
    assert not results[0].all() and results[0].any()


# -- 4. day boundaries --------------------------------------------------------

def test_pre_day_receipt_in_todays_segment_is_causal_carry_in_evidence_only(tmp_path):
    d = str(tmp_path)
    _base(d)
    _write(d, "trades", _trades([START - 1]))              # received 1 ms before midnight
    out = _run(d)
    assert out["trade_count"].sum() == 0                   # never aggregated into today
    assert list(_observed_ts(out, "trades_bin_observed")) == [START + k * G for k in range(0, 5)]


def test_stale_pre_day_receipt_is_not_evidence(tmp_path):
    d = str(tmp_path)
    _base(d)
    _write(d, "trades", _trades([START - TOL - 1]))
    assert not _run(d)["trades_bin_observed"].any()


@pytest.mark.parametrize("offset", [0, 1, 10])
def test_next_day_receipt_proves_nothing_for_today(tmp_path, offset):
    d = str(tmp_path)
    _base(d)
    _write(d, "trades", _trades([END + offset]))
    assert not _run(d)["trades_bin_observed"].any()


def test_trailing_partial_interval_and_last_bin(tmp_path):
    d = str(tmp_path)
    _base(d)
    t = np.arange(END - 6000, END, 1000, dtype=np.int64)
    _write(d, "trades", _trades(t))
    out = _run(d)
    last_grid = END - G
    assert list(_observed_ts(out, "trades_bin_observed")) == [END - 5000 + k * G for k in range(0, 5)]
    assert out.index.max() == last_grid and out.loc[last_grid, "trades_bin_observed"]


# -- 5. unreadable / malformed sources fail closed ----------------------------

def test_unreadable_trades_source_fails_loudly_and_writes_no_dataset(tmp_path):
    d = str(tmp_path)
    _base(d)
    os.makedirs(os.path.join(d, "raw", "trades"))
    with open(os.path.join(d, "raw", "trades", f"{DATE}-00-000000.seg"), "wb") as fh:
        fh.write(b"this is not parquet")
    with pytest.raises(Exception):
        assemble_dataset(DATE, grid_ms=G, data_dir=d)
    assert not os.path.exists(os.path.join(d, "aligned", f"{DATE}.parquet"))


def test_unreadable_liquidation_source_does_not_become_an_observed_zero(tmp_path):
    d = str(tmp_path)
    _base(d)
    os.makedirs(os.path.join(d, "raw", "liquidation"))
    with open(os.path.join(d, "raw", "liquidation", f"{DATE}-00-000000.seg"), "wb") as fh:
        fh.write(b"\x00\x01\x02")
    with pytest.raises(Exception):
        assemble_dataset(DATE, grid_ms=G, data_dir=d)
    assert not os.path.exists(os.path.join(d, "aligned", f"{DATE}.parquet"))


# -- 6. the helper against brute force, and causality as a property ----------

def _brute(ts, start, end, g, tol):
    covered = np.zeros(end - start, dtype=bool)
    for t in ts:
        a, b = max(t, start) - start, min(t + tol, end) - start
        if b > a:
            covered[a:b] = True
    grid = np.arange(start, end, g)
    return np.array([covered[max(T - g + 1, start) - start:T + 1 - start].all() for T in grid])


@pytest.mark.parametrize("seed", range(40))
def test_bin_observed_matches_per_millisecond_brute_force(seed):
    rng = np.random.default_rng(seed)
    start, end = 10_000, 10_000 + 240
    g, tol = int(rng.integers(1, 12)), int(rng.integers(1, 30))
    ts = rng.integers(start - 50, end + 50, size=int(rng.integers(0, 40)))
    grid = np.arange(start, end, g)
    got = _bin_observed(ts, grid, start, end, g, tol, "t")
    assert np.array_equal(got, _brute(ts, start, end, g, tol))


@pytest.mark.parametrize("seed", range(40))
def test_bin_observed_depends_only_on_receipts_at_or_before_the_bin(seed):
    rng = np.random.default_rng(1000 + seed)
    start, end, g, tol = 0, 600, 5, int(rng.integers(2, 25))
    ts = rng.integers(-30, end + 30, size=int(rng.integers(1, 60)))
    grid = np.arange(start, end, g)
    full = _bin_observed(ts, grid, start, end, g, tol, "t")
    for T in rng.choice(grid, size=8, replace=False):
        truncated = _bin_observed(ts[ts <= T], grid, start, end, g, tol, "t")
        assert truncated[grid == T][0] == full[grid == T][0]
        assert np.array_equal(truncated[grid <= T], full[grid <= T])


def test_no_evidence_is_never_all_observed():
    grid = np.arange(0, 100, 10)
    assert not _bin_observed([], grid, 0, 100, 10, 50, "t").any()
    assert not _bin_observed([np.nan, None], grid, 0, 100, 10, 50, "t").any()


# -- 7. existing flag semantics are unchanged ---------------------------------

def test_day_level_flags_are_unchanged_and_documented_as_not_per_bin(tmp_path):
    d = str(tmp_path)
    _base(d)
    out_absent = _run(d)
    assert not out_absent["trades_stream_available"].any()
    assert not out_absent["liquidation_stream_available"].any()
    d2 = str(tmp_path / "x")
    _base(d2)
    _write(d2, "trades", _trades([START + 5500]))
    _write(d2, "liquidation", _liq([START + 5500]))
    out = _run(d2)
    assert out["trades_stream_available"].all() and out["liquidation_stream_available"].all()
    # and both bin-level proofs are stricter than the day-level flags
    assert not out["trades_bin_observed"].all() and not out["liquidation_bin_observed"].all()


# -- 8. receive-time ordering / no future backfill (pins previously unpinned mutants)

def test_future_markprice_cannot_backfill_an_earlier_grid_row(tmp_path):
    d = str(tmp_path)
    _ob(d)
    _write(d, "markprice", pd.DataFrame({"timestamp": [START + 3000], "local_timestamp": [START + 3000],
                                         "mark_price": 9.0}))
    out = _run(d)
    for t in (START, START + 1000, START + 2000):           # all BEFORE the only receipt
        assert np.isnan(out.loc[t, "mark_price"]) and out.loc[t, "markprice_gap"]
    assert out.loc[START + 3000, "mark_price"] == 9.0


def test_orderbook_and_trades_order_by_receive_time_not_processing_time(tmp_path):
    d = str(tmp_path)
    _mark_one(d)
    # row A is received first but processed LAST; row B the reverse.
    ob = pd.DataFrame({"timestamp": [START + 990, START + 100], "local_timestamp": [START + 800, START + 900],
                       "best_bid": [1.0, 2.0], "best_ask": [3.0, 4.0], "spread": [2.0, 2.0]})
    _write(d, "orderbook", ob)
    tr = _trades([START + 100, START + 200])
    tr["timestamp"] = [START + 900, START + 100]             # processing order reversed
    tr["price"] = [10.0, 20.0]
    _write(d, "trades", tr)
    out = _run(d)
    assert out.loc[START + 1000, "best_bid"] == 2.0          # latest RECEIVED book wins
    assert out.loc[START + 1000, "last_price"] == 20.0       # latest RECEIVED trade is "last"
