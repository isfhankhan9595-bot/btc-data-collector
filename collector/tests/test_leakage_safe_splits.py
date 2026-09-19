"""Leakage safety for labels and splits.

Two classes of silent dishonesty are pinned here, because both produce a
research dataset that looks clean and is not:

1. A **label column named for a horizon it does not measure.** Horizons are
   applied as a row shift, which equals the named horizon only on a perfectly
   regular grid. The aligned grid is built from market data with real
   outages, so `shift(-10)` can span an hour while the column is still called
   `return_1s`.

2. A **manifest claiming a purge/embargo it did not achieve.** A split that is
   shorter than the requested gap must not silently collapse it while still
   recording the requested number.

Both were real defects. These tests fail if either returns.
"""
from __future__ import annotations

import json
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from collector.pipeline.label_generator import (
    LabelHorizonError,
    _generate_label_columns,
    max_label_horizon_s,
)
from collector.pipeline.split_generator import (
    LeakageError,
    SplitManifest,
    build_manifest,
    generate_splits,
    verify_manifest,
)

DATES = [f"2026-01-{day:02d}" for day in range(1, 21)]


def _write_labeled(base_dir, columns, count=20):
    """Write `count` labelled day files carrying `columns`."""
    labeled_dir = base_dir / "aligned" / "labeled"
    labeled_dir.mkdir(parents=True, exist_ok=True)
    dates = [f"2026-01-{day:02d}" for day in range(1, count + 1)]
    for date in dates:
        frame = pd.DataFrame({"date": [date], **{c: [0.0] for c in columns}})
        frame.to_parquet(labeled_dir / f"{date}.parquet")
    return dates


def _manifest_json(base_dir):
    return json.loads((base_dir / "splits" / "split_manifest.json").read_text())


# --------------------------------------------------------------------------
# The purge must be measured off the labels, never assumed.
# --------------------------------------------------------------------------

def test_purge_is_derived_from_label_columns(tmp_path):
    """A 300s label forces a one-day purge even with a zero embargo.

    Purge and embargo are different things. Purge is dictated by the label
    horizon: a training row whose forward label reaches past the boundary
    contaminates the later split regardless of what embargo was asked for.
    """
    _write_labeled(tmp_path, ["return_300s"])

    generate_splits(str(tmp_path), embargo_days=0)
    manifest = _manifest_json(tmp_path)

    assert manifest["max_label_horizon_s"] == 300
    assert manifest["max_label_horizon_source"] == "derived_from_labeled_columns"
    assert manifest["purge_days"] == 1
    assert manifest["embargo_days"] == 0
    assert manifest["requested_gap_days"] == 1
    assert manifest["achieved_gap_train_val"] >= 1
    assert manifest["leakage_safe"] is True


def test_label_horizon_longer_than_a_day_widens_the_purge(tmp_path):
    """Regression: the purge was sized from a constant, not from the data.

    `generate_splits` defaulted to a 300s constant and never consulted the
    labelled columns, even though `max_label_horizon_s()` exists for exactly
    that. A two-day label horizon therefore received a one-day purge -- the
    later split was contaminated while the manifest still reported
    `leakage_safe: true` and a rationale quoting 300s. Silent under-purge is
    the worst failure this module can have, because everything downstream
    looks clean.
    """
    two_days_s = 2 * 86_400
    _write_labeled(tmp_path, [f"return_{two_days_s}s"])

    generate_splits(str(tmp_path), embargo_days=0)
    manifest = _manifest_json(tmp_path)

    assert manifest["max_label_horizon_s"] == two_days_s
    assert manifest["purge_days"] == 2, "purge must cover the real label horizon"
    assert manifest["achieved_gap_train_val"] >= 2
    assert str(two_days_s) in manifest["rationale"]


def test_absence_of_label_columns_is_recorded_not_assumed(tmp_path):
    """No forward labels genuinely needs no purge -- but say so explicitly."""
    _write_labeled(tmp_path, [])

    generate_splits(str(tmp_path), embargo_days=0)
    manifest = _manifest_json(tmp_path)

    assert manifest["purge_days"] == 0
    assert manifest["max_label_horizon_source"] == "no_return_columns_found:purge_0"


def test_caller_supplied_horizon_is_labelled_as_such(tmp_path):
    _write_labeled(tmp_path, ["return_300s"])

    generate_splits(str(tmp_path), embargo_days=0, max_label_horizon_s=0)
    assert _manifest_json(tmp_path)["max_label_horizon_source"] == "caller_supplied"


def test_max_label_horizon_reads_the_longest_return_column(tmp_path):
    _write_labeled(tmp_path, ["return_1s", "return_60s", "return_300s", "direction_1s"])
    assert max_label_horizon_s(str(tmp_path)) == 300


# --------------------------------------------------------------------------
# A manifest may never claim a gap it did not achieve.
# --------------------------------------------------------------------------

def test_under_purge_is_refused_in_strict_mode():
    with pytest.raises(LeakageError, match="does not cover the longest label"):
        build_manifest(DATES, purge_days=0, max_label_horizon_s=300, strict=True)


def test_under_purge_is_recorded_when_not_strict():
    manifest = build_manifest(DATES, purge_days=0, max_label_horizon_s=300, strict=False)
    assert manifest.leakage_safe is False
    assert any("does not cover the longest label" in w for w in manifest.warnings)


def test_unachievable_gap_is_refused_rather_than_collapsed():
    """The original defect: too few dates silently dropped the embargo.

    `dates[:train_end - embargo] if train_end > embargo else dates[:train_end]`
    fell back to *no* gap whenever a split was shorter than the embargo, and
    still recorded the requested number in the manifest. Strict mode must now
    refuse; the specific reason (here the gap consumes the train split) is
    asserted by the dedicated tests below.
    """
    with pytest.raises(LeakageError):
        build_manifest(DATES[:4], embargo_days=3, max_label_horizon_s=0, strict=True)


def test_collapsed_gap_is_never_reported_as_achieved():
    """Non-strict mode still may not overstate what it delivered.

    The requested gap is recorded because it was requested; what must never
    happen is `leakage_safe: true`, or an achieved figure that the dates do
    not support.
    """
    manifest = build_manifest(DATES[:4], embargo_days=3, max_label_horizon_s=0,
                              strict=False)
    assert manifest.requested_gap_days == 3
    assert manifest.leakage_safe is False
    for achieved in (manifest.achieved_gap_train_val, manifest.achieved_gap_val_test):
        assert achieved is None or achieved >= manifest.requested_gap_days


def test_short_dataset_never_reports_itself_leakage_safe():
    manifest = build_manifest(DATES[:4], embargo_days=3, max_label_horizon_s=0,
                              strict=False)
    assert manifest.leakage_safe is False
    assert manifest.warnings


def test_embargo_stacks_on_top_of_the_purge():
    # 60 real calendar dates: val is 15% = 9 days, wide enough to survive a
    # 3-day gap. With only 20 dates a 3-day gap consumes val entirely, which
    # strict mode now refuses outright -- see
    # test_gap_that_consumes_a_split_is_refused_in_strict_mode.
    start = date(2026, 1, 1)
    dates = [(start + timedelta(days=i)).isoformat() for i in range(60)]
    manifest = build_manifest(dates, embargo_days=2, max_label_horizon_s=300)
    assert manifest.purge_days == 1
    assert manifest.embargo_days == 2
    assert manifest.requested_gap_days == 3
    assert manifest.achieved_gap_train_val >= 3


def test_gap_that_consumes_a_split_is_refused_in_strict_mode():
    """An empty split is the most severe form of an unachievable gap.

    It was previously only a warning, so strict mode wrote a manifest with no
    train set and emitted test.parquet beside it.
    """
    with pytest.raises(LeakageError, match="split is empty"):
        build_manifest(DATES, embargo_days=3, max_label_horizon_s=300, strict=True)


def test_verify_manifest_rejects_an_empty_split():
    """The verifier had the same blind spot as the builder.

    Two checks that share an assumption are one check. `verify_manifest`
    skipped boundaries where either side was empty, so a manifest with no
    train set passed independent verification.
    """
    manifest = build_manifest(DATES, embargo_days=3, max_label_horizon_s=300,
                              strict=False)
    problems = verify_manifest(manifest)
    assert any("empty" in p for p in problems)


@pytest.mark.parametrize("kwargs", [
    {"embargo_days": -1},
    {"purge_days": -1},
    {"max_label_horizon_s": -1},
])
def test_negative_parameters_are_rejected(kwargs):
    with pytest.raises(ValueError):
        build_manifest(DATES, **{"max_label_horizon_s": 0, **kwargs})


# --------------------------------------------------------------------------
# verify_manifest is an independent second check, not a restatement.
# --------------------------------------------------------------------------

def _clean_manifest():
    return build_manifest(DATES, embargo_days=1, max_label_horizon_s=0)


def test_verify_manifest_passes_a_clean_manifest():
    assert verify_manifest(_clean_manifest()) == []


def test_verify_manifest_detects_overlap():
    manifest = _clean_manifest()
    manifest.val = [manifest.train[-1]] + manifest.val
    assert any("overlap" in p for p in verify_manifest(manifest))


def test_verify_manifest_detects_a_claimed_gap_that_the_dates_contradict():
    """Tamper with the claim, not the data: the dates must win."""
    manifest = _clean_manifest()
    manifest.requested_gap_days = 99
    problems = verify_manifest(manifest)
    assert any("claims a 99-day" in p for p in problems)


def test_verify_manifest_detects_non_chronological_split():
    manifest = _clean_manifest()
    manifest.train = list(reversed(manifest.train))
    assert any("chronologically ordered" in p for p in verify_manifest(manifest))


def test_verify_manifest_detects_splits_out_of_order():
    manifest = _clean_manifest()
    manifest.train, manifest.test = manifest.test, manifest.train
    assert verify_manifest(manifest)


def test_generate_splits_refuses_to_write_an_unverifiable_manifest(tmp_path):
    _write_labeled(tmp_path, ["return_300s"], count=4)
    with pytest.raises(LeakageError):
        generate_splits(str(tmp_path), embargo_days=3)


# --------------------------------------------------------------------------
# A label column must not be named for a horizon it does not measure.
# --------------------------------------------------------------------------

def _regular(rows=100, step_ms=100):
    return pd.DataFrame({
        "timestamp": np.arange(rows) * step_ms,
        "mid_price": np.arange(100.0, 100.0 + rows),
    })


def test_realised_horizon_is_recorded_and_matches_on_a_regular_grid():
    _, _, meta = _generate_label_columns(
        _regular(), grid_ms=100, threshold=0.0, horizons_s=[1, 2])

    assert meta["grid_verified"] is True
    assert meta["irregular_steps"] == 0
    assert meta["realised_horizon_ms"] == {"return_1s": 1000, "return_2s": 2000}
    assert meta["max_label_horizon_s"] == 2


def test_missing_timestamp_column_is_unverified_not_assumed_regular():
    """Absence of evidence is not evidence of a regular grid."""
    frame = pd.DataFrame({"mid_price": np.arange(100.0, 200.0)})
    _, _, meta = _generate_label_columns(
        frame, grid_ms=100, threshold=0.0, horizons_s=[1])

    assert meta["grid_verified"] is False
    assert meta["observed_step_ms"] is None


def test_irregular_grid_is_detected_and_refused_in_strict_mode():
    frame = _regular(rows=50)
    frame.loc[25:, "timestamp"] += 3_600_000     # a one-hour outage

    _, _, meta = _generate_label_columns(
        frame, grid_ms=100, threshold=0.0, horizons_s=[1], strict=False)
    assert meta["grid_verified"] is False
    assert meta["irregular_steps"] >= 1

    with pytest.raises(LabelHorizonError, match="unverified grid"):
        _generate_label_columns(frame, grid_ms=100, threshold=0.0,
                                horizons_s=[1], strict=True)


def test_horizon_not_divisible_by_the_grid_is_refused_in_strict_mode():
    """`int(1000/300)` is 3, so `return_1s` would measure 900ms."""
    frame = _regular(step_ms=300)
    with pytest.raises(LabelHorizonError, match="whole multiple"):
        _generate_label_columns(frame, grid_ms=300, threshold=0.0,
                                horizons_s=[1], strict=True)


def test_non_divisible_horizon_records_the_realised_span_when_permitted():
    frame = _regular(step_ms=300)
    _, _, meta = _generate_label_columns(
        frame, grid_ms=300, threshold=0.0, horizons_s=[1], strict=False)
    # Named 1s, actually 900ms -- recorded rather than hidden.
    assert meta["realised_horizon_ms"]["return_1s"] == 900


def test_horizon_shorter_than_one_grid_step_is_rejected():
    with pytest.raises(LabelHorizonError, match="shorter than one grid step"):
        _generate_label_columns(_regular(step_ms=2000), grid_ms=2000,
                                threshold=0.0, horizons_s=[1], strict=False)


def test_labels_do_not_mutate_the_caller_frame():
    frame = _regular()
    before = list(frame.columns)
    _generate_label_columns(frame, grid_ms=100, threshold=0.0, horizons_s=[1])
    assert list(frame.columns) == before


def test_zero_grid_ms_is_rejected():
    with pytest.raises(ValueError, match="grid_ms must be positive"):
        _generate_label_columns(_regular(), grid_ms=0, threshold=0.0, horizons_s=[1])


# --------------------------------------------------------------------------
# Causality: a feature may not see the future; a label must.
# --------------------------------------------------------------------------

def test_label_uses_future_price_and_drops_rows_without_one():
    """Labels are allowed to look forward. Rows with no future are dropped,
    not filled -- a forward-filled label is a fabricated outcome."""
    frame = _regular(rows=20)
    labeled, dropped, _ = _generate_label_columns(
        frame, grid_ms=100, threshold=0.0, horizons_s=[1])

    assert dropped == 10                       # 1s / 100ms = 10 rows
    assert len(labeled) == 10
    assert labeled["return_1s"].isna().sum() == 0
    # mid_price rises by 1 per row, so a 10-row lookahead is +10 on the base.
    expected = 10.0 / labeled["mid_price"].iloc[0]
    assert labeled["return_1s"].iloc[0] == pytest.approx(expected)


def test_splits_are_chronological_and_never_shuffled():
    manifest = build_manifest(DATES, embargo_days=1, max_label_horizon_s=0)
    for split in (manifest.train, manifest.val, manifest.test):
        assert split == sorted(split)
    assert manifest.train[-1] < manifest.val[0] < manifest.test[0]


# --------------------------------------------------------------------------
# Provenance of the purge size, closed during self-review.
# --------------------------------------------------------------------------

def test_max_label_horizon_scans_every_day_not_just_the_newest(tmp_path):
    """Schema drift must not shrink the purge.

    The horizon was read from `files[-1]` alone. If the label set changed
    part-way through a run, the purge would be sized from whichever day
    happened to sort last -- the same silent under-purge, one step removed
    and considerably harder to notice.
    """
    labeled_dir = tmp_path / "aligned" / "labeled"
    labeled_dir.mkdir(parents=True)
    long_horizon = 3 * 86_400
    # The long horizon lives on the FIRST day; the last day has only a short one.
    pd.DataFrame({"date": ["2026-01-01"], f"return_{long_horizon}s": [0.0]}).to_parquet(
        labeled_dir / "2026-01-01.parquet")
    pd.DataFrame({"date": ["2026-01-02"], "return_60s": [0.0]}).to_parquet(
        labeled_dir / "2026-01-02.parquet")

    assert max_label_horizon_s(str(tmp_path)) == long_horizon


def test_build_manifest_requires_the_horizon_to_be_stated():
    """No hidden constant. A purge sized from an assumption is not evidence.

    `build_manifest` previously defaulted to a 300s module constant while
    recording the provenance as "caller_supplied", which was untrue.
    """
    with pytest.raises(TypeError):
        build_manifest(DATES, embargo_days=1)
