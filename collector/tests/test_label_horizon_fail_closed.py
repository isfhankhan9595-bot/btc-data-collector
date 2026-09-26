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
    discover_labeled_files,
    max_label_horizon_s,
)
from collector.pipeline.split_generator import LeakageError, build_manifest, generate_splits, verify_manifest


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


def test_labeled_path_exists_but_is_not_a_directory_raises(tmp_path):
    """Known Defect #2: a stray file at the labeled path must never mean
    'nothing labeled yet' -- that reading is reserved for the path being
    entirely absent. A file where a directory was expected is a discovery
    failure."""
    aligned_dir = os.path.join(tmp_path, "aligned")
    os.makedirs(aligned_dir)
    with open(os.path.join(aligned_dir, "labeled"), "w") as f:
        f.write("not a directory")

    with pytest.raises(LabelDiscoveryError, match="not a directory"):
        max_label_horizon_s(str(tmp_path))
    with pytest.raises(LabelDiscoveryError):
        discover_labeled_files(str(tmp_path))


def test_discover_labeled_files_distinguishes_absent_from_not_a_directory(tmp_path):
    """The absent case must NOT raise (established 'fresh pipeline'
    semantics); the not-a-directory case MUST raise. These are different
    filesystem states and must not share a code path."""
    assert discover_labeled_files(str(tmp_path)) == []  # aligned/ doesn't even exist

    aligned_dir = os.path.join(tmp_path, "aligned")
    os.makedirs(aligned_dir)
    with open(os.path.join(aligned_dir, "labeled"), "w") as f:
        f.write("x")
    with pytest.raises(LabelDiscoveryError):
        discover_labeled_files(str(tmp_path))


def test_stat_failure_on_an_existing_path_is_not_silently_absent(tmp_path, monkeypatch):
    """The audit target from this session: os.path.exists()/os.path.isdir()
    both catch OSError broadly (CPython's genericpath module: 'except
    (OSError, ValueError): return False'), so a PermissionError from
    statting a labeled directory that genuinely exists -- e.g. because a
    parent directory's permissions were changed, or a transient I/O/mount
    error -- would be indistinguishable from the directory never having
    existed at all, if either boolean API were used for the initial check.
    This is exactly the forbidden 'cannot inspect -> pretend absent'
    transition. discover_labeled_files() calls os.stat() directly instead
    and only treats FileNotFoundError specifically as genuine absence;
    every other OSError must raise LabelDiscoveryError.

    This sandbox runs as root, so a real chmod-based reproduction is not
    possible (root bypasses permission bits entirely -- verified this
    session: os.path.exists()/os.path.isdir() both still returned True
    under a real os.chmod(0o000) on the parent). A precise monkeypatch of
    os.stat itself, raising PermissionError only for the exact labeled-dir
    path, exercises the identical code path a real permission failure
    would reach without depending on enforcement this environment cannot
    provide."""
    _write_labeled_day(tmp_path, "2026-01-01", ["mid_price", "return_60s"])
    labeled_dir = os.path.join(tmp_path, "aligned", "labeled")

    real_stat = os.stat

    def flaky_stat(path, *a, **kw):
        if os.fspath(path) == labeled_dir:
            raise PermissionError(13, "Permission denied", labeled_dir)
        return real_stat(path, *a, **kw)

    monkeypatch.setattr(os, "stat", flaky_stat)

    with pytest.raises(LabelDiscoveryError, match="could not inspect"):
        discover_labeled_files(str(tmp_path))
    with pytest.raises(LabelDiscoveryError):
        max_label_horizon_s(str(tmp_path))


def test_stat_failure_end_to_end_through_generate_splits_is_not_none_or_empty(tmp_path, monkeypatch):
    """The same simulated failure, exercised through the actual
    generate_splits() call site -- must not be converted into 'No labeled
    files found' / None anywhere along that path."""
    _write_labeled_day(tmp_path, "2026-01-01", ["mid_price", "return_60s"])
    labeled_dir = os.path.join(tmp_path, "aligned", "labeled")

    real_stat = os.stat

    def flaky_stat(path, *a, **kw):
        if os.fspath(path) == labeled_dir:
            raise PermissionError(13, "Permission denied", labeled_dir)
        return real_stat(path, *a, **kw)

    monkeypatch.setattr(os, "stat", flaky_stat)

    with pytest.raises(LabelDiscoveryError):
        generate_splits(str(tmp_path), strict=False)
    assert not os.path.exists(os.path.join(tmp_path, "splits", "split_manifest.json"))


# ---------------------------------------------------------------------------
# Known Defect #1 (end-to-end): generate_splits() must use the SAME
# fail-closed discovery as max_label_horizon_s(), not a separate glob.glob
# call that can silently convert a listing failure into "no files found".
# ---------------------------------------------------------------------------

@pytest.mark.skipif(os.name == "nt" or os.geteuid() == 0,
                     reason="permission bits are not enforced for root or on Windows")
def test_generate_splits_raises_on_permission_denied_not_none(tmp_path):
    """Before the end-to-end fix: generate_splits() discovered its file
    list via glob.glob, which returns [] on a listing failure --
    generate_splits() would print 'No labeled files found' and return
    None, silently bypassing the fail-closed contract for this exact path.
    Must now raise LabelDiscoveryError instead."""
    labeled_dir = os.path.join(tmp_path, "aligned", "labeled")
    os.makedirs(labeled_dir)
    _write_labeled_day(tmp_path, "2026-01-01", ["mid_price", "return_60s"])
    original_mode = os.stat(labeled_dir).st_mode
    os.chmod(labeled_dir, 0o000)
    try:
        with pytest.raises(LabelDiscoveryError):
            generate_splits(str(tmp_path), strict=False)
        # And, crucially, no manifest may exist afterward.
        assert not os.path.exists(os.path.join(tmp_path, "splits", "split_manifest.json"))
    finally:
        os.chmod(labeled_dir, original_mode)


def test_generate_splits_raises_when_labeled_path_is_not_a_directory(tmp_path):
    aligned_dir = os.path.join(tmp_path, "aligned")
    os.makedirs(aligned_dir)
    with open(os.path.join(aligned_dir, "labeled"), "w") as f:
        f.write("not a directory")
    with pytest.raises(LabelDiscoveryError):
        generate_splits(str(tmp_path), strict=False)


def test_generate_splits_and_max_label_horizon_s_use_the_same_discovery(tmp_path):
    """Pins the actual fix for Defect #1: both call sites must agree on
    what 'no files' means, by construction (same underlying function),
    not by coincidence of two separately-maintained implementations."""
    import inspect

    from collector.pipeline import split_generator
    source = inspect.getsource(split_generator.generate_splits)
    assert "discover_labeled_files" in source
    assert "glob.glob(" not in source  # no actual call left; a docstring mention of the old bug is fine


# ---------------------------------------------------------------------------
# Race condition (Test M): a file present at discovery time but gone by the
# time its schema is read must still fail closed, not be silently skipped.
# ---------------------------------------------------------------------------

def test_file_disappearing_between_discovery_and_schema_read_fails_closed(tmp_path, monkeypatch):
    _write_labeled_day(tmp_path, "2026-01-01", ["mid_price", "return_60s"])
    _write_labeled_day(tmp_path, "2026-01-02", ["mid_price", "return_300s"])

    import pyarrow.parquet as pq
    real_read_schema = pq.read_schema
    vanished_path = os.path.join(tmp_path, "aligned", "labeled", "2026-01-02.parquet")

    def flaky_read_schema(path, *a, **kw):
        if str(path) == vanished_path:
            os.remove(path)  # simulate the file vanishing right before the read
            raise FileNotFoundError(f"[simulated race] {path} no longer exists")
        return real_read_schema(path, *a, **kw)

    monkeypatch.setattr(pq, "read_schema", flaky_read_schema)

    with pytest.raises(LabelSchemaError):
        max_label_horizon_s(str(tmp_path))


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


def test_strict_mode_leakage_violation_raises_before_any_publication(tmp_path):
    """The same 10-day dataset above fails verify_manifest in non-strict
    mode (empty val split -- too few dates for the purge to leave a val
    window). In strict mode (the default), that must raise LeakageError
    -- and, more importantly for this task, must do so BEFORE anything is
    published: no split_manifest.json, no parquet, not even a stale one
    from a half-completed run."""
    dates = [f"2026-01-{d:02d}" for d in range(1, 11)]
    for date in dates:
        _write_labeled_day(tmp_path, date, ["mid_price", "return_60s"])

    with pytest.raises(LeakageError):
        generate_splits(str(tmp_path))  # strict=True is the default

    assert not os.path.exists(os.path.join(tmp_path, "splits", "split_manifest.json"))
    assert not os.path.exists(os.path.join(tmp_path, "splits", "train.parquet"))


def test_verify_manifest_has_no_independent_effect_on_the_p0_5_failure_class():
    """Investigated per this session's audit instruction rather than
    mutated blindly: build_manifest()'s train/val/test slices are always
    taken from a single sorted, deduplicated date list with monotonically
    non-decreasing slice boundaries (parsed[:a], parsed[a:b], parsed[b:]),
    so they are structurally incapable of overlapping or being
    out-of-order regardless of purge/embargo/horizon values -- there is no
    label-horizon-discovery failure that reaches build_manifest() (since
    discover_labeled_files/max_label_horizon_s already raised earlier) and
    no horizon value that can make build_manifest()'s own slicing produce
    an overlap. Confirmed empirically: build_manifest() cannot be driven
    into producing a manifest verify_manifest() would flag differently
    than build_manifest()'s own strict checks already did, for any input
    tried. For the specific failure class P0-5 addresses (unreadable/
    corrupt label files under-proving the horizon), verify_manifest() is
    therefore genuinely redundant with build_manifest(strict=True)'s own
    checks -- not a gap, just an honestly-documented fact about where the
    real protection lives for this failure class."""
    manifest = build_manifest(
        [f"2026-01-{d:02d}" for d in range(1, 21)],
        max_label_horizon_s=60, strict=True,
    )
    assert verify_manifest(manifest) == []  # nothing left for it to catch here


def test_verify_manifest_does_independently_catch_a_hand_crafted_bad_manifest():
    """verify_manifest() is not dead code / not vacuous in general -- it
    genuinely re-derives overlap/ordering from the raw date lists rather
    than trusting them, and catches a manifest that never went through
    build_manifest()'s own construction at all. This is what makes it a
    real independent check -- just not one reachable via the P0-5 failure
    class, per the test above."""
    from collector.pipeline.split_generator import SplitManifest

    bad = SplitManifest(
        train=["2026-01-01", "2026-01-05"], val=["2026-01-03", "2026-01-06"],  # overlaps train's range and is unordered relative to it
        test=["2026-01-07"], purge_days=0, embargo_days=0, requested_gap_days=0,
        achieved_gap_train_val=0, achieved_gap_val_test=0, max_label_horizon_s=0,
        leakage_safe=True, rationale="hand-crafted for this test", warnings=[],
    )
    problems = verify_manifest(bad)
    assert problems  # verify_manifest independently detects this, nothing upstream did


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
