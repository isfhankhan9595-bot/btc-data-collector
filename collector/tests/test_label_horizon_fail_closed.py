"""P0-5: label/schema failure handling must fail closed.

Previously, max_label_horizon_s() caught any schema-read exception, printed
a warning, and continued -- computing the horizon from whichever files it
could read. A split generated from that under-counted horizon could still
be reported leakage_safe=True. These tests pin the fail-closed replacement:
an unreadable/corrupt file or an unlistable labeled directory must abort
horizon discovery entirely, and generate_splits() must let that propagate
rather than writing any manifest.
"""
from __future__ import annotations

import json
import os
import stat

import pandas as pd
import pytest

from collector.pipeline.label_generator import (
    LabelDiscoveryError,
    LabelSchemaError,
    max_label_horizon_s,
)
from collector.pipeline.split_generator import generate_splits


def _write_labeled_day(data_dir, date_str, columns):
    labeled_dir = os.path.join(data_dir, "aligned", "labeled")
    os.makedirs(labeled_dir, exist_ok=True)
    df = pd.DataFrame({col: [1.0, 2.0, 3.0] for col in columns})
    df.to_parquet(os.path.join(labeled_dir, f"{date_str}.parquet"))


# ---------------------------------------------------------------------------
# Core fail-closed contract
# ---------------------------------------------------------------------------

def test_no_labeled_directory_is_not_a_discovery_failure(tmp_path):
    """A fresh pipeline with nothing labeled yet needs no purge -- this is
    the one case that must NOT raise."""
    assert max_label_horizon_s(str(tmp_path)) == 0


def test_empty_but_listable_directory_returns_zero(tmp_path):
    os.makedirs(os.path.join(tmp_path, "aligned", "labeled"))
    assert max_label_horizon_s(str(tmp_path)) == 0


def test_corrupt_parquet_file_aborts_horizon_discovery(tmp_path):
    """Test 2 from the task: a corrupted file must FAIL, not be skipped."""
    _write_labeled_day(tmp_path, "2026-01-01", ["mid_price", "return_60s"])
    labeled_dir = os.path.join(tmp_path, "aligned", "labeled")
    with open(os.path.join(labeled_dir, "2026-01-02.parquet"), "wb") as f:
        f.write(b"this is not a parquet file")

    with pytest.raises(LabelSchemaError, match="2026-01-02"):
        max_label_horizon_s(str(tmp_path))


def test_larger_horizon_in_any_file_is_the_reported_maximum(tmp_path):
    """Test 5 from the task: A=60s, B=300s, C=60s -> max horizon = 300s."""
    _write_labeled_day(tmp_path, "2026-01-01", ["mid_price", "return_60s"])
    _write_labeled_day(tmp_path, "2026-01-02", ["mid_price", "return_300s"])
    _write_labeled_day(tmp_path, "2026-01-03", ["mid_price", "return_60s"])
    assert max_label_horizon_s(str(tmp_path)) == 300


def test_one_unreadable_file_among_readable_ones_still_fails(tmp_path):
    """The exact scenario in the task's motivating example: A readable,
    B unreadable, C readable -- must not silently compute from A+C."""
    _write_labeled_day(tmp_path, "2026-01-01", ["mid_price", "return_60s"])
    _write_labeled_day(tmp_path, "2026-01-03", ["mid_price", "return_300s"])
    labeled_dir = os.path.join(tmp_path, "aligned", "labeled")
    with open(os.path.join(labeled_dir, "2026-01-02.parquet"), "wb") as f:
        f.write(b"garbage")

    with pytest.raises(LabelSchemaError):
        max_label_horizon_s(str(tmp_path))


def test_files_with_no_return_columns_is_not_an_error(tmp_path):
    """A readable file that simply has no forward-label columns is not a
    failure -- it legitimately contributes no horizon. Distinguishing
    'unreadable' from 'readable but no return_Ns columns' is deliberate,
    not a gap: only the former is a proof failure."""
    _write_labeled_day(tmp_path, "2026-01-01", ["mid_price"])
    assert max_label_horizon_s(str(tmp_path)) == 0


@pytest.mark.skipif(os.name == "nt" or os.geteuid() == 0,
                     reason="permission bits are not enforced for root or on Windows")
def test_permission_denied_directory_raises_not_treated_as_empty(tmp_path):
    """Test 7 from the task: permission denied must not become 'no files
    found'. glob.glob silently returns [] on a listing failure; this pins
    that os.listdir is used instead, which raises."""
    labeled_dir = os.path.join(tmp_path, "aligned", "labeled")
    os.makedirs(labeled_dir)
    _write_labeled_day(tmp_path, "2026-01-01", ["mid_price", "return_60s"])
    original_mode = os.stat(labeled_dir).st_mode
    os.chmod(labeled_dir, 0o000)
    try:
        with pytest.raises(LabelDiscoveryError):
            max_label_horizon_s(str(tmp_path))
    finally:
        os.chmod(labeled_dir, original_mode)  # restore so tmp_path cleanup can run


# ---------------------------------------------------------------------------
# generate_splits() must let these propagate -- no manifest on failure
# ---------------------------------------------------------------------------

def test_generate_splits_writes_no_manifest_when_horizon_discovery_fails(tmp_path):
    """Test 1/Test 6 from the task, end to end: split generation must fail,
    and no valid leakage-safe manifest may exist afterward."""
    for i, date in enumerate(["2026-01-0%d" % d for d in range(1, 8)]):
        cols = ["mid_price", "return_60s"] if i != 3 else None
        if cols is None:
            labeled_dir = os.path.join(tmp_path, "aligned", "labeled")
            os.makedirs(labeled_dir, exist_ok=True)
            with open(os.path.join(labeled_dir, f"{date}.parquet"), "wb") as f:
                f.write(b"not parquet")
        else:
            _write_labeled_day(tmp_path, date, cols)

    with pytest.raises(LabelSchemaError):
        generate_splits(str(tmp_path))

    manifest_path = os.path.join(tmp_path, "splits", "split_manifest.json")
    assert not os.path.exists(manifest_path)


def test_generate_splits_succeeds_normally_for_a_valid_dataset(tmp_path):
    """Test 12 from the task: a normal valid dataset must still work --
    the fail-closed change must not break the happy path."""
    dates = [f"2026-01-{d:02d}" for d in range(1, 11)]
    for date in dates:
        _write_labeled_day(tmp_path, date, ["mid_price", "return_60s"])

    manifest = generate_splits(str(tmp_path), strict=False)
    assert manifest is not None
    assert os.path.exists(os.path.join(tmp_path, "splits", "split_manifest.json"))
    assert manifest.max_label_horizon_s == 60
    assert manifest.max_label_horizon_source == "derived_from_labeled_columns"


# ---------------------------------------------------------------------------
# Atomic publish: a failed regeneration must never destroy or shadow a
# previously-valid manifest (task's Phase 18 / acceptance criterion)
# ---------------------------------------------------------------------------

def test_existing_valid_manifest_survives_a_failed_regeneration(tmp_path):
    dates = [f"2026-01-{d:02d}" for d in range(1, 11)]
    for date in dates:
        _write_labeled_day(tmp_path, date, ["mid_price", "return_60s"])
    good_manifest = generate_splits(str(tmp_path), strict=False)
    assert good_manifest is not None
    manifest_path = os.path.join(tmp_path, "splits", "split_manifest.json")
    with open(manifest_path) as f:
        good_contents = f.read()
    assert good_contents  # sanity

    # Corrupt one more label file and try to regenerate -- must fail.
    labeled_dir = os.path.join(tmp_path, "aligned", "labeled")
    with open(os.path.join(labeled_dir, "2026-01-11.parquet"), "wb") as f:
        f.write(b"corrupt")
    with pytest.raises(LabelSchemaError):
        generate_splits(str(tmp_path), strict=False)

    # The previously-published, valid manifest must be untouched.
    with open(manifest_path) as f:
        after_failed_attempt = f.read()
    assert after_failed_attempt == good_contents
    parsed = json.loads(after_failed_attempt)
    assert parsed["leakage_safe"] in (True, False)  # still a coherent, previously-valid manifest


def test_pending_directory_is_cleaned_up_after_a_successful_publish(tmp_path):
    dates = [f"2026-01-{d:02d}" for d in range(1, 11)]
    for date in dates:
        _write_labeled_day(tmp_path, date, ["mid_price", "return_60s"])
    generate_splits(str(tmp_path), strict=False)
    pending_dir = os.path.join(tmp_path, "splits", ".pending")
    assert not os.path.exists(pending_dir) or not os.listdir(pending_dir)


# ---------------------------------------------------------------------------
# Determinism (task's Test 9)
# ---------------------------------------------------------------------------

def test_horizon_discovery_is_deterministic_across_repeated_runs(tmp_path):
    _write_labeled_day(tmp_path, "2026-01-01", ["mid_price", "return_60s"])
    _write_labeled_day(tmp_path, "2026-01-02", ["mid_price", "return_300s"])
    results = {max_label_horizon_s(str(tmp_path)) for _ in range(5)}
    assert results == {300}
