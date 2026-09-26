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

import os
import stat as stat_module
from typing import Any, List, Optional, Tuple


class LabelDiscoveryError(RuntimeError):
    """Label file discovery could not be completed reliably.

    Distinct from the labeled-data directory existing and legitimately
    containing zero files (a fresh pipeline with nothing labeled yet, which
    genuinely needs no purge). A directory that cannot be listed --
    permission denied, a transient filesystem error -- must not be silently
    read as "no files found": ``glob.glob`` swallows most listing errors and
    simply returns no matches, which is exactly the ambiguity this class
    exists to remove.
    """


class LabelSchemaError(RuntimeError):
    """A label file that was expected to participate in horizon discovery
    could not be read or its schema could not be inspected.

    Raised rather than warned-past. ``max_label_horizon_s`` exists to prove
    the *maximum* horizon across every labeled file; a file it could not
    inspect might carry a longer horizon than every file it could read, so
    continuing past it does not produce a smaller-but-still-valid answer --
    it produces an unproven one that looks proven.
    """

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


def discover_labeled_files(data_dir: str = "data") -> List[str]:
    """The one authoritative, fail-closed listing of labeled parquet files.

    Both this module's :func:`max_label_horizon_s` and
    ``split_generator.generate_splits`` use this rather than each rolling
    its own discovery call, so the two can never disagree about what "no
    files" versus "listing failed" means -- which is exactly how the
    previous fix's end-to-end contract had a gap: ``max_label_horizon_s``
    was fixed to use ``os.listdir`` (raises on failure), but
    ``generate_splits`` still discovered its own file list with
    ``glob.glob`` (silently returns ``[]`` on a listing failure), so a
    directory-listing failure there was reported as "No labeled files
    found" rather than the fail-closed error this contract requires.

    Three filesystem states are distinguished on purpose:

    1. The labeled path does not exist at all -- a fresh pipeline with
       nothing labeled yet. Returns ``[]``, not an error.
    2. The labeled path exists but is not a directory (a stray file where
       a directory was expected). Never the same as "nothing labeled yet"
       -- raises :class:`LabelDiscoveryError`.
    3. The labeled path is a directory that cannot be listed (permission
       denied, a transient I/O error). Also raises
       :class:`LabelDiscoveryError` -- ``glob.glob`` would silently return
       ``[]`` for this exact case, which is the defect this function exists
       to close.

    Returns:
        Sorted full paths to every ``*.parquet`` file directly in the
        labeled directory. An empty list means state 1 or "directory
        exists, genuinely has no parquet files in it" -- both legitimate,
        neither an error.

    Correctness note (audited): ``os.path.exists``/``os.path.isdir`` are
    deliberately NOT used for the initial check, because both catch
    ``OSError`` broadly and return ``False`` for *any* stat failure, not
    only "does not exist" (this is CPython's own implementation --
    ``genericpath.exists``/``isdir`` wrap ``os.stat`` in
    ``except (OSError, ValueError): return False``). A ``PermissionError``
    raised by a parent-directory access failure while statting a labeled
    directory that genuinely exists would therefore be indistinguishable
    from the directory never having existed at all -- silently converting
    "cannot inspect" into "empty", exactly the forbidden transition this
    function exists to prevent. Verified directly: monkeypatching
    ``os.stat`` to raise ``PermissionError`` for an existing directory's
    path previously made this function return ``[]``; see
    ``tests/test_label_horizon_fail_closed.py``'s
    ``test_stat_failure_on_an_existing_path_is_not_silently_absent`` for the
    regression test. ``os.stat`` is called directly instead, and only
    ``FileNotFoundError`` specifically resolves to "does not exist".
    """
    labeled_dir = os.path.join(data_dir, "aligned", "labeled")
    try:
        st = os.stat(labeled_dir)
    except FileNotFoundError:
        return []
    except OSError as exc:
        # Any other stat failure -- permission denied on this path or a
        # parent directory, a transient I/O error, a mount problem -- is
        # "cannot inspect", never "does not exist".
        raise LabelDiscoveryError(
            f"could not inspect labeled-data path: path={labeled_dir} reason={exc}"
        ) from exc
    if not stat_module.S_ISDIR(st.st_mode):
        raise LabelDiscoveryError(
            f"labeled-data path exists but is not a directory: path={labeled_dir}"
        )
    try:
        entries = os.listdir(labeled_dir)
    except OSError as exc:
        # os.listdir raises on permission errors and similar; glob.glob
        # does not, and would have silently returned no matches for the
        # exact same failure. Listing explicitly is what makes "permission
        # denied" distinguishable from "genuinely empty".
        raise LabelDiscoveryError(
            f"could not list labeled-data directory: path={labeled_dir} reason={exc}"
        ) from exc
    return sorted(os.path.join(labeled_dir, name) for name in entries if name.endswith(".parquet"))


def max_label_horizon_s(data_dir: str = "data") -> int:
    """Longest horizon present in the labeled set, read from the columns.

    The split generator needs this to size its purge. Deriving it from the
    data rather than from a constant keeps the two in step if the label set
    changes.

    Fail-closed contract: if the labeled directory cannot be listed
    (:class:`LabelDiscoveryError`) or any participating file's schema
    cannot be read (:class:`LabelSchemaError`), this function raises rather
    than computing a horizon from whatever it could read. A caller that
    needs a leakage-safe split must not treat either exception as "purge
    with what we have" -- see split_generator.py, which lets both propagate
    uncaught so no manifest is written.
    """
    files = discover_labeled_files(data_dir)
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
        except Exception as exc:  # noqa: BLE001 - any read/schema failure must abort, not degrade
            raise LabelSchemaError(
                f"cannot determine label horizon: file={path} "
                f"reason=parquet schema read failed ({exc}); split generation "
                "must abort rather than compute a purge horizon from the "
                "remaining files, since this file's own horizon is unknown "
                "and may exceed every file that was readable"
            ) from exc
        for name in names:
            if name.startswith("return_") and name.endswith("s"):
                digits = name[len("return_"):-1]
                if digits.isdigit():
                    horizons.append(int(digits))
    return max(horizons) if horizons else 0
