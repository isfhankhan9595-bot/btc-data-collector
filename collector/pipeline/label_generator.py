"""Forward-looking labels, with the realised horizon verified rather than assumed.

Labels may legitimately use future information -- that is what a label is.
What they may not do is silently measure a horizon other than the one named,
which is what the previous implementation could do.

Defect this file fixes
----------------------
Horizons were applied as a **row shift**::

    shift_rows = int((h * 1000) / grid_ms)
    future_mid = df["mid_price"].shift(-shift_rows)

That equals ``h`` seconds only if every row is exactly ``grid_ms`` apart. The
aligned grid is built from market data with real outages -- the coverage model
exists precisely because intervals go missing -- so on a grid with a gap,
``shift(-10)`` can span an hour while the column is still called
``return_1s``. Every conditional statistic computed from it would then be
answering a different question than the one asked, with nothing anywhere
saying so.

Two further silent misnamings are closed:

* ``int()`` truncation. With ``grid_ms=300`` and ``h=1``, ``shift_rows`` was 3,
  so ``return_1s`` measured 900ms. The horizon must divide the grid exactly,
  or the realised horizon is recorded instead of the requested one.
* Row shifting across a gap is now detected when a timestamp column is
  available, and the grid is reported as unverified when it is not.
"""
from __future__ import annotations

import glob
import os
from typing import Any, List, Optional, Tuple

import numpy as np
import pandas as pd

DEFAULT_HORIZONS_S: tuple[int, ...] = (1, 5, 15, 60, 300)


class LabelHorizonError(ValueError):
    """The requested horizon cannot be realised on this grid."""


def _verify_grid(
    df: pd.DataFrame, grid_ms: int, timestamp_column: Optional[str]
) -> tuple[bool, int, Optional[int]]:
    """Return ``(verified, irregular_steps, observed_step_ms)``.

    ``verified`` is True only when a timestamp column exists *and* every
    consecutive step equals ``grid_ms``. Absence of timestamps is reported as
    unverified rather than assumed regular.
    """
    if timestamp_column is None or timestamp_column not in df.columns or len(df) < 2:
        return False, 0, None
    steps = pd.to_numeric(df[timestamp_column], errors="coerce").diff().dropna()
    if steps.empty:
        return False, 0, None
    irregular = int((steps != grid_ms).sum())
    observed = int(steps.mode().iloc[0]) if not steps.mode().empty else None
    return irregular == 0, irregular, observed


def _generate_label_columns(
    df: pd.DataFrame,
    grid_ms: int,
    threshold: float,
    horizons_s: List[int],
    *,
    timestamp_column: Optional[str] = "timestamp",
    strict: bool = False,
) -> Tuple[pd.DataFrame, int, dict[str, Any]]:
    """Attach forward return/direction labels.

    Returns ``(labeled_df, rows_dropped, meta)``. ``meta`` carries the facts a
    researcher needs in order to trust the columns: whether the grid was
    verified regular, how many irregular steps were seen, and the horizon each
    column *actually* realises.

    The input frame is not mutated.
    """
    if grid_ms <= 0:
        raise ValueError("grid_ms must be positive")
    df = df.copy()

    grid_verified, irregular_steps, observed_step_ms = _verify_grid(
        df, grid_ms, timestamp_column)

    if strict and not grid_verified:
        raise LabelHorizonError(
            "refusing to shift by rows on an unverified grid: "
            f"irregular_steps={irregular_steps} observed_step_ms={observed_step_ms}; "
            "a row shift only equals the named horizon on a regular grid"
        )

    realised: dict[str, int] = {}
    for horizon in horizons_s:
        requested_ms = horizon * 1000
        if requested_ms % grid_ms != 0:
            message = (
                f"horizon {horizon}s is not a whole multiple of grid_ms={grid_ms}; "
                "the column would be named for a horizon it does not measure"
            )
            if strict:
                raise LabelHorizonError(message)
        shift_rows = int(requested_ms / grid_ms)
        if shift_rows <= 0:
            raise LabelHorizonError(
                f"horizon {horizon}s is shorter than one grid step ({grid_ms}ms)")

        future_mid = df["mid_price"].shift(-shift_rows)
        ret_col = f"return_{horizon}s"
        df[ret_col] = (future_mid - df["mid_price"]) / df["mid_price"]

        dir_col = f"direction_{horizon}s"
        direction = np.zeros(len(df), dtype=np.int8)
        direction[(df[ret_col] > threshold).to_numpy(na_value=False)] = 1
        direction[(df[ret_col] < -threshold).to_numpy(na_value=False)] = -1
        df[dir_col] = direction

        # The horizon this column actually spans, which is what a researcher
        # must condition on -- not the one that was asked for.
        realised[ret_col] = shift_rows * grid_ms

    label_cols = ([f"return_{h}s" for h in horizons_s]
                  + [f"direction_{h}s" for h in horizons_s])
    rows_before = len(df)
    df = df.dropna(subset=label_cols).reset_index(drop=True)
    rows_dropped = rows_before - len(df)
    drop_pct = rows_dropped / rows_before * 100 if rows_before else 0
    print(f"Dropped {rows_dropped} rows with NaN labels ({drop_pct:.2f}%)")

    meta = {
        "grid_ms": grid_ms,
        "grid_verified": grid_verified,
        "irregular_steps": irregular_steps,
        "observed_step_ms": observed_step_ms,
        "requested_horizons_s": list(horizons_s),
        "realised_horizon_ms": realised,
        "max_label_horizon_s": max(horizons_s) if horizons_s else 0,
        "rows_dropped": rows_dropped,
        "threshold": threshold,
    }
    if not grid_verified:
        print("WARNING: grid regularity unverified; realised horizons may differ "
              "from the requested ones")
    return df, rows_dropped, meta


def generate_labels(
    date_str: str,
    grid_ms: int = 100,
    threshold: float = 0.00005,
    data_dir: str = "data",
    *,
    horizons_s: Optional[List[int]] = None,
    strict: bool = True,
):
    print(f"Generating labels for {date_str}...")

    in_file = os.path.join(data_dir, "aligned", f"{date_str}.parquet")
    if not os.path.exists(in_file):
        print(f"Input file not found: {in_file}")
        return None

    df = pd.read_parquet(in_file)
    horizons = list(horizons_s or DEFAULT_HORIZONS_S)
    df, _, meta = _generate_label_columns(
        df, grid_ms, threshold, horizons, strict=strict)

    out_dir = os.path.join(data_dir, "aligned", "labeled")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"{date_str}.parquet")
    df.to_parquet(out_file, compression="snappy")
    print(f"Saved labeled data to {out_file}")
    return meta


def max_label_horizon_s(data_dir: str = "data") -> int:
    """Longest horizon present in the labeled set, read from the columns.

    The split generator needs this to size its purge. Deriving it from the
    data rather than from a constant keeps the two in step if the label set
    changes.
    """
    files = sorted(glob.glob(os.path.join(data_dir, "aligned", "labeled", "*.parquet")))
    if not files:
        return 0

    # Every file, not just the newest. If the label set changed part-way
    # through a run, sizing the purge from whichever day happens to be last
    # reintroduces the under-purge this function exists to prevent -- one step
    # removed and harder to see. The longest horizon anywhere in the set is
    # the only safe figure.
    #
    # Schema metadata only: reading the frames would cost a full parquet load
    # per day to answer a question about column names.
    import pyarrow.parquet as pq

    horizons: list[int] = []
    for path in files:
        try:
            names = pq.read_schema(path).names
        except Exception:  # noqa: BLE001 - an unreadable day must not be silent
            print(f"WARNING: could not read label schema from {path}; "
                  "its horizons are not represented in the purge")
            continue
        for name in names:
            if name.startswith("return_") and name.endswith("s"):
                digits = name[len("return_"):-1]
                if digits.isdigit():
                    horizons.append(int(digits))
    return max(horizons) if horizons else 0
