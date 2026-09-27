"""Assemble the research-aligned dataset on a fixed time grid.

P0-6 causal research-time contract
-----------------------------------
A row observed at grid time T may only reflect information the collector
could actually have known by T. Every canonical feature row this module
reads carries (up to) three distinct clocks:

* ``exchange_timestamp`` -- the venue's own clock. Descriptive only. NEVER
  used to decide alignment eligibility, for the same reason
  ``pipeline/cross_exchange_alignment.py.causally_align()`` never uses it
  at the canonical-event level: exchanges are not synchronised with each
  other or with us, and a venue timestamp says nothing about when the
  collector actually received the information.
* ``local_timestamp`` -- the collector's *receive* time: the local instant
  the information became available. This is the availability/eligibility
  clock. It is what ``local_receive_ts`` is on ``CanonicalEvent`` and on the
  raw-wire/raw-REST storage layer (see ``canonical.py``, ``raw_capture.py``,
  ``replay.py``) -- same underlying clock, carried through unmodified into
  the feature-computed schemas this module reads.
* ``timestamp`` -- historically ambiguous, and NEVER safe to use for causal
  alignment. In the feature schemas this module reads (ORDERBOOK_SCHEMA,
  TRADES_SCHEMA, MARKPRICE_SCHEMA) it is the local *processing* timestamp:
  when application code got around to handling the event, which can lag
  receive time by an arbitrary, non-deterministic amount (book-reconstruction
  work, lock contention, GC pauses, ...). Retained as a descriptive/
  diagnostic field only.

For every stream this module aligns, the causal join is therefore:

    local_timestamp (availability) <= observation_ts

never ``exchange_timestamp <= observation_ts`` and never
``timestamp (processing) <= observation_ts``.

Legacy data / minimal fixtures
-------------------------------
Older segments (and some test fixtures) carry only ``timestamp`` with no
``local_timestamp`` column at all. Nothing here fabricates a receive time
for such rows: ``<stream>_ts`` falls back to ``timestamp`` for that stream,
but the fallback is never silent -- every such row is flagged in the
aligned output via ``<stream>_time_unknown`` = True, so ambiguous legacy
timestamp semantics are never presented as causally safe.

Ties: rows are ordered with a stable sort on the availability clock, so two
rows sharing one ``<stream>_ts`` keep their original (input) order --
matching the documented tie rule in
``pipeline/cross_exchange_alignment.py.causally_align()``. Determinism does
not depend on filesystem iteration order or a non-stable sort algorithm.

P0-7: open interest and liquidations
--------------------------------------
Open interest (``open_interest``) and liquidations (``liquidation_*``) are
aligned with the exact same rule as every other stream: their own
``local_timestamp`` (receive time), never ``exchange_timestamp`` or
``timestamp``. Two points specific to these two streams:

* Open interest is REST-polled (``OI_POLL_INTERVAL_S`` in ``run_collector.py``,
  currently 3s), not event-driven, so it is naturally coarser than book/mark
  data. It is joined with ``merge_asof`` exactly like markprice (backward,
  with a staleness tolerance), and a missing/stale reading is represented as
  NaN plus ``openinterest_gap`` -- NEVER as ``open_interest = 0``. A poll
  failure or an OI value of zero are not interchangeable, and this module
  never invents the latter to paper over the former. Binance OI IS
  replayable at the canonical-event layer (``replay.py``'s
  ``FrameKind.REST_OI`` / ``_handle_rest_oi`` route a recorded response
  through the same ``normalize_binance_oi()`` live uses -- see
  ``docs/REPLAY.md``), but nothing currently writes replayed OI back into
  the flattened ``OPENINTEREST_SCHEMA`` parquet this module reads, so in
  practice this module only aligns whatever OI segments were actually
  collected live.
* Liquidations are sparse, irregular events. They are aggregated per grid
  bin the same way trades are (count/volumes/notional, keyed on the
  availability clock, never fabricated for a bin with no events) -- see
  ``docs/RESEARCH_DATASET_TIME_CONTRACT.md`` for the explicit, documented
  limitation this shares with the trades aggregation: an empty bin means
  "no liquidation was received in this window", which is only as trustworthy
  as the forceOrder stream's own connection coverage for that window (the
  same real-time WS capture as trades, with no separate per-bin coverage
  record persisted to prove it). This is not new to P0-7 -- it is the same
  assumption the trades aggregation already makes -- but it is called out
  explicitly here rather than silently inherited.

Neither stream's absence blocks assembly: like trades, both are optional
inputs (a quiet day with zero liquidations, or an OI poll outage, does not
prevent the rest of the dataset from being built).
"""
import calendar
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collector.collector.storage_layout import iter_segments
from collector.collector.instrument import BINANCE_USDM_BTCUSDT

#: This assembler is intentionally single-venue, single-instrument
#: (Binance USD-M BTCUSDT). ``instrument_key`` is validated at write time
#: (a payload symbol that contradicts the configured instrument is rejected
#: before it ever reaches a writer -- see run_collector.py's
#: ``_binance_usdm_instrument_key``/``normalize_binance_oi``), so this is a
#: defense-in-depth check, not the primary guard: any row whose
#: ``instrument_key`` contradicts the expected instrument is dropped here
#: too, never silently merged into the aligned series.
_EXPECTED_INSTRUMENT_KEY = BINANCE_USDM_BTCUSDT.key


def _normalize_ms(frame: pd.DataFrame, col: str) -> None:
    """In place: force ``col`` to integer epoch milliseconds if datetime-typed.

    Arrow/Pandas may preserve a millisecond physical timestamp as a
    datetime64 column; this coerces it to nanosecond resolution first so the
    floor-division to milliseconds is exact, never a silent unit error.
    """
    if col in frame and pd.api.types.is_datetime64_any_dtype(frame[col]):
        frame[col] = (
            pd.to_datetime(frame[col], utc=True)
            .astype("datetime64[ns, UTC]")
            .astype("int64")
            // 1_000_000
        )


def _availability_series(frame: pd.DataFrame) -> tuple[pd.Series, bool]:
    """The causal availability clock for this frame: local receive time.

    Returns ``(series, used_fallback)``. ``local_timestamp`` is the
    collector's receive time and is authoritative whenever present. Its
    absence is a distinct condition from being present with a fabricated
    value: it means this row's timing semantics are unknown, so the
    fallback to ``timestamp`` (processing time) is flagged, never silently
    trusted as causally safe (see module docstring).
    """
    if "local_timestamp" in frame.columns:
        return frame["local_timestamp"], False
    return frame["timestamp"], True


def _enforce_single_instrument(frame: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Defense-in-depth instrument isolation (see module-level constant).

    A row with no ``instrument_key`` at all (legacy data predating the
    column) is kept -- absence is not a contradiction, matching the same
    principle already applied to missing ``local_timestamp``. A row whose
    ``instrument_key`` is present and does NOT match the expected single
    instrument is dropped and reported; it must never silently blend into
    this single-instrument series.
    """
    if "instrument_key" not in frame.columns:
        return frame
    mismatched = frame["instrument_key"].notna() & (frame["instrument_key"] != _EXPECTED_INSTRUMENT_KEY)
    if mismatched.any():
        print(f"WARNING: dropping {int(mismatched.sum())} {prefix} row(s) with "
              f"unexpected instrument_key (expected {_EXPECTED_INSTRUMENT_KEY})")
        frame = frame.loc[~mismatched]
    return frame


def _prepare_stream_frame(frame: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Attach the causal availability clock and make every clock's role explicit.

    Adds/renames columns so that, after this call:

    * ``<prefix>_ts``           -- causal availability time (local receive
                                    time, or the flagged legacy fallback).
                                    The ONLY column eligibility/alignment
                                    logic may read.
    * ``<prefix>_process_ts``   -- processing time (was ``timestamp``).
                                    Descriptive/diagnostic only.
    * ``<prefix>_exchange_ts``  -- venue clock (was ``exchange_timestamp``).
                                    Descriptive only.
    * ``<prefix>_time_unknown`` -- True iff ``local_timestamp`` was absent
                                    and ``<prefix>_ts`` had to fall back to
                                    the ambiguous processing timestamp.

    Also enforces single-instrument isolation (see
    ``_enforce_single_instrument``) and drops the now-redundant
    ``instrument_key`` column -- this assembler covers exactly one
    venue/instrument by construction.
    """
    frame = _enforce_single_instrument(frame, prefix)
    avail, used_fallback = _availability_series(frame)
    out = frame.copy()
    out[f"{prefix}_ts"] = avail.astype("int64")
    out[f"{prefix}_time_unknown"] = used_fallback
    rename = {}
    if "timestamp" in out.columns:
        rename["timestamp"] = f"{prefix}_process_ts"
    if "exchange_timestamp" in out.columns:
        rename["exchange_timestamp"] = f"{prefix}_exchange_ts"
    out = out.rename(columns=rename)
    return out.drop(columns=["local_timestamp", "instrument_key"], errors="ignore")


def _read_stream(data_dir: str, stream: str, date_str: str) -> list[pd.DataFrame]:
    """Read one unambiguous representation for each raw logical hour."""
    frames = []
    for path in iter_segments(data_dir, stream, date=date_str, on_collision="prefer_segments"):
        frame = pd.read_parquet(path)
        _normalize_ms(frame, "timestamp")
        _normalize_ms(frame, "local_timestamp")
        frames.append(frame)
    return frames

def assemble_dataset(date_str: str, grid_ms: int = 100, data_dir: str = "data"):
    print(f"Assembling dataset for {date_str} with grid {grid_ms}ms")

    ob_dfs = _read_stream(data_dir, "orderbook", date_str)
    trades_dfs = _read_stream(data_dir, "trades", date_str)
    mark_dfs = _read_stream(data_dir, "markprice", date_str)
    oi_dfs = _read_stream(data_dir, "openinterest", date_str)
    liq_dfs = _read_stream(data_dir, "liquidation", date_str)

    if not ob_dfs or not mark_dfs:
        print(f"Insufficient data for {date_str}")
        return

    df_ob = _prepare_stream_frame(pd.concat(ob_dfs).reset_index(drop=True), "ob")
    df_ob = df_ob.sort_values("ob_ts", kind="stable").reset_index(drop=True)

    if trades_dfs:
        df_trades = _prepare_stream_frame(pd.concat(trades_dfs).reset_index(drop=True), "trades")
        df_trades = df_trades.sort_values("trades_ts", kind="stable").reset_index(drop=True)
    else:
        df_trades = pd.DataFrame()

    df_mark = _prepare_stream_frame(pd.concat(mark_dfs).reset_index(drop=True), "mark")
    df_mark = df_mark.sort_values("mark_ts", kind="stable").reset_index(drop=True)

    # OI and liquidation are optional inputs, like trades: a quiet day with
    # zero liquidations, or an OI REST-poll outage, must not block assembly
    # of the rest of the dataset (see module docstring).
    if oi_dfs:
        df_oi = _prepare_stream_frame(pd.concat(oi_dfs).reset_index(drop=True), "oi")
        df_oi = df_oi.sort_values("oi_ts", kind="stable").reset_index(drop=True)
    else:
        df_oi = pd.DataFrame()

    if liq_dfs:
        df_liq = _prepare_stream_frame(pd.concat(liq_dfs).reset_index(drop=True), "liq")
        # Liquidations have no venue-assigned unique event id (unlike
        # trades' trade_id in TRADES_SCHEMA) -- forceOrder gives no such
        # field, and nothing in this repository establishes that this
        # tuple is guaranteed unique. This is a heuristic, not exact
        # deduplication: two rows sharing the exact
        # (exchange_timestamp, side, price, quantity) tuple are treated as
        # one WS-redelivered event and never summed twice, but two
        # genuinely distinct liquidations that happen to share all four
        # fields would be incorrectly collapsed into one. See
        # docs/RESEARCH_DATASET_TIME_CONTRACT.md for the full tradeoff.
        before = len(df_liq)
        df_liq = df_liq.drop_duplicates(subset=["liq_exchange_ts", "side", "price", "quantity"], keep="first")
        if len(df_liq) < before:
            print(f"WARNING: dropped {before - len(df_liq)} duplicate liquidation row(s) "
                  f"(identical exchange_timestamp/side/price/quantity)")
        df_liq = df_liq.sort_values("liq_ts", kind="stable").reset_index(drop=True)
    else:
        df_liq = pd.DataFrame()

    # Create common time grid
    start_dt = datetime.strptime(date_str, "%Y-%m-%d")
    end_dt = start_dt + timedelta(days=1)

    start_ts = calendar.timegm(start_dt.timetuple()) * 1000
    end_ts = calendar.timegm(end_dt.timetuple()) * 1000

    grid_ts = np.arange(start_ts, end_ts, grid_ms)
    df_grid = pd.DataFrame({"timestamp": grid_ts})

    # Causal joins have explicit freshness limits.  Never present old market
    # state as current during an outage.
    df_ob = df_ob.drop(columns=["bids_price", "bids_qty", "asks_price", "asks_qty"], errors="ignore")

    # Causal merges: eligibility is decided by the availability clock
    # (<prefix>_ts, i.e. local receive time), never by processing or
    # exchange time. See module docstring.
    df_aligned = pd.merge_asof(df_grid, df_ob, left_on="timestamp", right_on="ob_ts",
                                direction="backward", tolerance=500)
    df_aligned = pd.merge_asof(df_aligned, df_mark, left_on="timestamp", right_on="mark_ts",
                                direction="backward", tolerance=5000)

    df_aligned["orderbook_gap"] = df_aligned["ob_ts"].isna() | ((df_aligned["timestamp"] - df_aligned["ob_ts"]) > 500)
    df_aligned["markprice_gap"] = df_aligned["mark_ts"].isna() | ((df_aligned["timestamp"] - df_aligned["mark_ts"]) > 5000)

    df_aligned = df_aligned.drop(columns=["ob_ts", "mark_ts"])
    df_aligned = df_aligned.rename(columns={
        "ob_time_unknown": "orderbook_time_unknown",
        "mark_time_unknown": "markprice_time_unknown",
    })

    # Open interest: a REST poll every OI_POLL_INTERVAL_S (currently 3s, see
    # run_collector.py), so a generous but explicit staleness tolerance --
    # never a fabricated 0 for a poll outage or a not-yet-collected value.
    OI_STALE_MS = 5000
    if not df_oi.empty:
        df_aligned = pd.merge_asof(df_aligned, df_oi, left_on="timestamp", right_on="oi_ts",
                                    direction="backward", tolerance=OI_STALE_MS)
        df_aligned["openinterest_gap"] = df_aligned["oi_ts"].isna() | (
            (df_aligned["timestamp"] - df_aligned["oi_ts"]) > OI_STALE_MS)
        df_aligned = df_aligned.drop(columns=["oi_ts"])
        df_aligned = df_aligned.rename(columns={"oi_time_unknown": "openinterest_time_unknown"})
    else:
        df_aligned["open_interest"] = np.nan
        df_aligned["oi_process_ts"] = np.nan
        df_aligned["oi_exchange_ts"] = np.nan
        df_aligned["openinterest_gap"] = True
        df_aligned["openinterest_time_unknown"] = False

    # Preserve stress observations.  This flag is descriptive only; no spread
    # value is masked or forward-filled.
    SPREAD_SPIKE_THRESHOLD = 1.0
    if "spread" in df_aligned.columns:
        df_aligned["spread_spike_flag"] = df_aligned["spread"] > SPREAD_SPIKE_THRESHOLD
    else:
        df_aligned["spread_spike_flag"] = False
    df_aligned["spread_spike_flag"] = df_aligned["spread_spike_flag"].astype(bool)

    # De-saturate orderbook imbalance features for downstream linear models while
    # preserving the raw OBI columns.
    OBI_COLS = ["obi", "obi_level_1", "obi_level_3", "obi_level_5"]
    CLIP = 0.9999
    for col in OBI_COLS:
        if col in df_aligned.columns:
            clipped = df_aligned[col].clip(-CLIP, CLIP)
            df_aligned[f"{col}_fisher"] = np.arctanh(clipped).astype("float64")

    # Aggregate trades
    if not df_trades.empty:
        # Bin trades by grid timestamp, using the causal availability clock
        # (trades_ts), never the processing timestamp. A trade at t falls
        # into the bin (t_grid-grid_ms, t_grid]. We can achieve this by
        # ceiling the trade's availability timestamp to the nearest grid point.
        df_trades["grid_ts"] = np.ceil((df_trades["trades_ts"] - start_ts) / grid_ms) * grid_ms + start_ts
        df_trades["grid_ts"] = df_trades["grid_ts"].astype(np.int64)

        df_trades["is_buyer"] = ~df_trades["is_buyer_maker"]
        df_trades["buy_vol"] = np.where(df_trades["is_buyer"], df_trades["quantity"], 0)
        df_trades["sell_vol"] = np.where(~df_trades["is_buyer"], df_trades["quantity"], 0)
        df_trades["vol_x_price"] = df_trades["quantity"] * df_trades["price"]

        trade_aggs = df_trades.groupby("grid_ts").agg(
            trade_count=("trade_id", "count"),
            buy_volume=("buy_vol", "sum"),
            sell_volume=("sell_vol", "sum"),
            net_volume=("signed_qty", "sum"),
            vol_x_price_sum=("vol_x_price", "sum"),
            last_price=("price", "last"),
            trades_time_unknown=("trades_time_unknown", "any"),
        ).reset_index()

        trade_aggs["trade_flow_imbalance"] = np.where(
            (trade_aggs["buy_volume"] + trade_aggs["sell_volume"]) > 0,
            trade_aggs["net_volume"] / (trade_aggs["buy_volume"] + trade_aggs["sell_volume"]),
            0
        )
        trade_aggs["vwap"] = trade_aggs["vol_x_price_sum"] / (trade_aggs["buy_volume"] + trade_aggs["sell_volume"])
        trade_aggs = trade_aggs.drop(columns=["vol_x_price_sum"])

        df_aligned = pd.merge(df_aligned, trade_aggs, left_on="timestamp", right_on="grid_ts", how="left")
        df_aligned = df_aligned.drop(columns=["grid_ts"])
    else:
        df_aligned["trade_count"] = 0
        df_aligned["buy_volume"] = 0.0
        df_aligned["sell_volume"] = 0.0
        df_aligned["net_volume"] = 0.0
        df_aligned["trade_flow_imbalance"] = 0.0
        df_aligned["vwap"] = np.nan
        df_aligned["last_price"] = np.nan
        df_aligned["trades_time_unknown"] = False

    # Fill NaNs for trades where appropriate
    df_aligned["trade_count"] = df_aligned["trade_count"].fillna(0).astype(np.int32)
    df_aligned["buy_volume"] = df_aligned["buy_volume"].fillna(0.0)
    df_aligned["sell_volume"] = df_aligned["sell_volume"].fillna(0.0)
    df_aligned["net_volume"] = df_aligned["net_volume"].fillna(0.0)
    df_aligned["trade_flow_imbalance"] = df_aligned["trade_flow_imbalance"].fillna(0.0)
    # A grid bin with no trades has nothing ambiguous to flag: absence of a
    # trade is not a legacy/unknown-timestamp condition.
    df_aligned["trades_time_unknown"] = df_aligned["trades_time_unknown"].fillna(False).astype(bool)

    # Aggregate liquidations: same causal binning as trades (ceil to the
    # availability clock, never the processing timestamp), see module
    # docstring for the shared "empty bin = zero" limitation this inherits
    # from the trades aggregation.
    if not df_liq.empty:
        df_liq["grid_ts"] = np.ceil((df_liq["liq_ts"] - start_ts) / grid_ms) * grid_ms + start_ts
        df_liq["grid_ts"] = df_liq["grid_ts"].astype(np.int64)

        # side: +1 == BUY (forced buy => a short position was liquidated),
        # -1 == SELL (forced sell => a long position was liquidated). See
        # feature_computer.compute_liquidation_features.
        df_liq["liq_buy_vol"] = np.where(df_liq["side"] > 0, df_liq["quantity"], 0.0)
        df_liq["liq_sell_vol"] = np.where(df_liq["side"] < 0, df_liq["quantity"], 0.0)
        df_liq["liq_notional"] = df_liq["quantity"] * df_liq["price"]

        liq_aggs = df_liq.groupby("grid_ts").agg(
            liquidation_count=("price", "count"),
            liquidation_buy_volume=("liq_buy_vol", "sum"),
            liquidation_sell_volume=("liq_sell_vol", "sum"),
            liquidation_net_volume=("signed_qty", "sum"),
            liquidation_notional=("liq_notional", "sum"),
            liquidation_time_unknown=("liq_time_unknown", "any"),
        ).reset_index()

        df_aligned = pd.merge(df_aligned, liq_aggs, left_on="timestamp", right_on="grid_ts", how="left")
        df_aligned = df_aligned.drop(columns=["grid_ts"])
        # The stream was collected for this day: a bin with no matching
        # aggregate genuinely had zero liquidations (not "unknown"),
        # distinct from the whole-stream-absent case below (see
        # docs/RESEARCH_DATASET_TIME_CONTRACT.md, "empty liquidation
        # interval" semantics).
        df_aligned["liquidation_stream_available"] = True
    else:
        df_aligned["liquidation_count"] = 0
        df_aligned["liquidation_buy_volume"] = 0.0
        df_aligned["liquidation_sell_volume"] = 0.0
        df_aligned["liquidation_net_volume"] = 0.0
        df_aligned["liquidation_notional"] = 0.0
        df_aligned["liquidation_time_unknown"] = False
        # No liquidation segment exists for this day at all: a count of 0
        # here is NOT the same causal claim as "we watched and saw zero" --
        # it means we have no evidence either way. Never collapse this into
        # the same signal as a genuinely observed quiet interval.
        df_aligned["liquidation_stream_available"] = False

    df_aligned["liquidation_count"] = df_aligned["liquidation_count"].fillna(0).astype(np.int32)
    df_aligned["liquidation_buy_volume"] = df_aligned["liquidation_buy_volume"].fillna(0.0)
    df_aligned["liquidation_sell_volume"] = df_aligned["liquidation_sell_volume"].fillna(0.0)
    df_aligned["liquidation_net_volume"] = df_aligned["liquidation_net_volume"].fillna(0.0)
    df_aligned["liquidation_notional"] = df_aligned["liquidation_notional"].fillna(0.0)
    df_aligned["liquidation_time_unknown"] = df_aligned["liquidation_time_unknown"].fillna(False).astype(bool)

    # Save aligned dataset
    out_dir = os.path.join(data_dir, "aligned")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"{date_str}.parquet")

    df_aligned.to_parquet(out_file, compression="snappy")
    print(f"Saved aligned dataset to {out_file}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        assemble_dataset(sys.argv[1])
    else:
        print("Usage: python dataset_assembler.py YYYY-MM-DD")
