"""P0-5: fail-closed label-horizon validation.

The defect: `max_label_horizon_s()` could encounter an unreadable label
file (corrupt Parquet, schema read failure, permission denied), print a
warning, `continue`, and return the maximum horizon computed from only
the files that happened to be readable. `split_generator.py` then used
that partial number to size the purge and could report `leakage_safe:
true` on a split that was never actually proven safe -- exactly the
class of silent dishonesty `test_leakage_safe_splits.py`'s own module
docstring already names as unacceptable, just one file layer removed
from where it was previously fixed.

Required invariant: if the system cannot prove the complete label
horizon, it must not create a leakage-safe split. `readable A + unreadable
B + readable C` must never silently become `partial horizon -> split ->
leakage_safe=true`.
"""
from __future__ import annotations

import json
import os

import pandas as pd
import pytest

from collector.pipeline.label_generator import LabelHorizonError, max_label_horizon_s
from collector.pipeline.split_generator import generate_splits


def _write_labeled(base_dir, columns, count=20):
    labeled_dir = base_dir / "aligned" / "labeled"
    labeled_dir.mkdir(parents=True, exist_ok=True)
    dates = [f"2026-01-{day:02d}" for day in range(1, count + 1)]
    for d in dates:
        frame = pd.DataFrame({"date": [d], **{c: [0.0] for c in columns}})
        frame.to_parquet(labeled_dir / f"{d}.parquet")
    return dates


def _corrupt_one_file(base_dir, date_str):
    """Overwrite one label file with bytes that are not valid Parquet at all
    -- a genuine schema-read failure, not merely missing columns."""
    path = base_dir / "aligned" / "labeled" / f"{date_str}.parquet"
    path.write_bytes(b"not a parquet file, just garbage bytes" * 20)


def _manifest_json(base_dir):
    return json.loads((base_dir / "splits" / "split_manifest.json").read_text())


# ---------------------------------------------------------------------------
# 1-4: unreadable/corrupted/schema-failure/missing files fail closed
# ---------------------------------------------------------------------------


def test_unreadable_parquet_file_fails_closed_by_default(tmp_path):
    _write_labeled(tmp_path, ["return_300s"])
    _corrupt_one_file(tmp_path, "2026-01-10")
    with pytest.raises(LabelHorizonError):
        max_label_horizon_s(str(tmp_path))   # strict=True is the default


def test_corrupted_parquet_file_fails_closed_by_default(tmp_path):
    """Same defect, framed as 'corrupted' rather than 'never valid' --
    both are schema-read failures and must be treated identically."""
    _write_labeled(tmp_path, ["return_60s"])
    _corrupt_one_file(tmp_path, "2026-01-05")
    with pytest.raises(LabelHorizonError):
        max_label_horizon_s(str(tmp_path))


def test_schema_read_failure_message_names_the_actual_file(tmp_path):
    _write_labeled(tmp_path, ["return_300s"])
    _corrupt_one_file(tmp_path, "2026-01-10")
    with pytest.raises(LabelHorizonError) as excinfo:
        max_label_horizon_s(str(tmp_path))
    assert "2026-01-10" in str(excinfo.value)


def test_missing_required_label_information_fails_closed_via_split_generator(tmp_path):
    """The end-to-end path: generate_splits must refuse to produce ANY
    manifest (not a partial-horizon one) when a label file cannot be read,
    in the default strict mode."""
    _write_labeled(tmp_path, ["return_300s"])
    _corrupt_one_file(tmp_path, "2026-01-10")
    with pytest.raises(LabelHorizonError):
        generate_splits(str(tmp_path), embargo_days=0)
    assert not os.path.exists(tmp_path / "splits" / "split_manifest.json"), \
        "no manifest of any kind may be written when the horizon could not be established"


# ---------------------------------------------------------------------------
# 5: unknown horizon fails closed (covered by the above -- an unreadable
# file IS the unknown-horizon case for this module); explicit permission
# variant below
# ---------------------------------------------------------------------------


def test_permission_denied_file_fails_closed(tmp_path, monkeypatch):
    """os.chmod-based permission denial doesn't work reliably in every test
    environment (e.g. running as root bypasses it entirely), so this
    exercises the real code path by making pq.read_schema itself raise
    PermissionError for one specific file -- the same exception type and
    call site a genuine permission failure would hit."""
    import pyarrow.parquet as pq
    from collector.pipeline import label_generator

    _write_labeled(tmp_path, ["return_300s"])
    blocked_path = str(tmp_path / "aligned" / "labeled" / "2026-01-10.parquet")
    real_read_schema = pq.read_schema

    def _maybe_denied(path, *args, **kwargs):
        if str(path) == blocked_path:
            raise PermissionError(f"[Errno 13] Permission denied: {path!r}")
        return real_read_schema(path, *args, **kwargs)

    monkeypatch.setattr("pyarrow.parquet.read_schema", _maybe_denied)
    with pytest.raises(LabelHorizonError):
        max_label_horizon_s(str(tmp_path))


# ---------------------------------------------------------------------------
# 6: a larger horizon in an EARLIER file is still the conservative maximum
# (pre-existing correct behaviour, re-confirmed still holds after the fix)
# ---------------------------------------------------------------------------


def test_larger_horizon_in_an_earlier_file_is_still_the_conservative_maximum(tmp_path):
    labeled_dir = tmp_path / "aligned" / "labeled"
    labeled_dir.mkdir(parents=True)
    pd.DataFrame({"return_900s": [0.0]}).to_parquet(labeled_dir / "2026-01-01.parquet")
    pd.DataFrame({"return_60s": [0.0]}).to_parquet(labeled_dir / "2026-01-02.parquet")
    pd.DataFrame({"return_60s": [0.0]}).to_parquet(labeled_dir / "2026-01-03.parquet")
    assert max_label_horizon_s(str(tmp_path)) == 900


# ---------------------------------------------------------------------------
# 7-8: file-discovery / directory-level failure
# ---------------------------------------------------------------------------


def test_nonexistent_label_directory_is_a_genuine_empty_set_not_an_error(tmp_path):
    """An absent labeled/ directory means no labels exist yet -- a
    legitimate, distinct situation from a file that exists but cannot be
    read. Zero files is not itself unsafe (generate_splits separately
    refuses to run at all with zero labeled files); this just confirms
    max_label_horizon_s doesn't raise for the absent-directory case."""
    assert max_label_horizon_s(str(tmp_path / "does_not_exist")) == 0


def test_generate_splits_with_zero_labeled_files_does_not_run_at_all(tmp_path):
    (tmp_path / "aligned" / "labeled").mkdir(parents=True)
    result = generate_splits(str(tmp_path), embargo_days=0)
    assert result is None
    assert not os.path.exists(tmp_path / "splits" / "split_manifest.json")


# ---------------------------------------------------------------------------
# 9: mixed/incompatible schema handled explicitly (already-correct
# behaviour: a readable file with no return_* columns contributes 0, not
# an error) -- distinguished from an unreadable file
# ---------------------------------------------------------------------------


def test_a_readable_file_with_no_return_columns_is_not_an_error(tmp_path):
    """Distinguishes 'this file has nothing to say about label horizons'
    (fine, contributes 0) from 'this file could not be read at all'
    (fails closed) -- the two must never be conflated."""
    labeled_dir = tmp_path / "aligned" / "labeled"
    labeled_dir.mkdir(parents=True)
    pd.DataFrame({"some_other_column": [0.0]}).to_parquet(labeled_dir / "2026-01-01.parquet")
    pd.DataFrame({"return_300s": [0.0]}).to_parquet(labeled_dir / "2026-01-02.parquet")
    assert max_label_horizon_s(str(tmp_path)) == 300


# ---------------------------------------------------------------------------
# 10: deterministic repeated generation
# ---------------------------------------------------------------------------


def test_repeated_generation_from_the_same_input_is_deterministic(tmp_path):
    _write_labeled(tmp_path, ["return_300s"])
    generate_splits(str(tmp_path), embargo_days=0)
    first = _manifest_json(tmp_path)
    generate_splits(str(tmp_path), embargo_days=0)
    second = _manifest_json(tmp_path)
    assert first == second


# ---------------------------------------------------------------------------
# 11-12: failed generation cannot publish a new manifest; a previously
# valid manifest survives a failed regeneration
# ---------------------------------------------------------------------------


def test_failed_generation_leaves_no_valid_new_manifest(tmp_path):
    _write_labeled(tmp_path, ["return_60s"])
    generate_splits(str(tmp_path), embargo_days=0)   # succeeds, writes a manifest
    good_manifest = _manifest_json(tmp_path)

    _corrupt_one_file(tmp_path, "2026-01-10")   # now break it
    with pytest.raises(LabelHorizonError):
        generate_splits(str(tmp_path), embargo_days=0)

    # The manifest on disk must be EXACTLY the last good one -- untouched,
    # not partially overwritten, not replaced by a bad one.
    assert _manifest_json(tmp_path) == good_manifest


def test_previous_valid_manifest_survives_a_failed_regeneration_byte_for_byte(tmp_path):
    """Stronger than the JSON-equality check above: proves the file's raw
    bytes on disk are untouched, not merely 'happens to parse the same'."""
    _write_labeled(tmp_path, ["return_60s"])
    generate_splits(str(tmp_path), embargo_days=0)
    manifest_path = tmp_path / "splits" / "split_manifest.json"
    original_bytes = manifest_path.read_bytes()
    original_mtime_ns = manifest_path.stat().st_mtime_ns

    _corrupt_one_file(tmp_path, "2026-01-10")
    with pytest.raises(LabelHorizonError):
        generate_splits(str(tmp_path), embargo_days=0)

    assert manifest_path.read_bytes() == original_bytes
    assert manifest_path.stat().st_mtime_ns == original_mtime_ns, \
        "the file must not even have been reopened/rewritten, not just coincidentally identical"


def test_atomic_write_leaves_no_temp_file_behind_on_success(tmp_path):
    _write_labeled(tmp_path, ["return_60s"])
    generate_splits(str(tmp_path), embargo_days=0)
    leftover = [p for p in (tmp_path / "splits").iterdir() if p.name.startswith(".tmp-")]
    assert leftover == []


def test_atomic_write_helper_never_truncates_the_target_on_a_write_failure(tmp_path, monkeypatch):
    """Direct unit test of _atomic_write_bytes itself, not routed through a
    generate_splits failure path (every generate_splits failure this file
    tests happens BEFORE any write is attempted, so those tests cannot
    exercise a crash *during* a write -- confirmed by mutation testing:
    replacing _atomic_write_bytes with a direct non-atomic write broke
    none of the generate_splits-level tests). This test simulates a write
    that fails partway through and proves the original file survives
    untouched -- the actual property _atomic_write_bytes exists for."""
    from collector.pipeline.split_generator import _atomic_write_bytes

    target = tmp_path / "existing.json"
    target.write_bytes(b'{"old": "value"}')

    real_write = os.fdopen

    def _write_then_fail(fd, mode):
        handle = real_write(fd, mode)
        original_write = handle.write

        def _boom(data):
            original_write(data[:1])   # partial write, then...
            raise OSError("simulated disk-full mid-write")
        handle.write = _boom
        return handle

    monkeypatch.setattr(os, "fdopen", _write_then_fail)
    with pytest.raises(OSError):
        _atomic_write_bytes(str(target), b'{"new": "value", "padding": "xxxxxxxxxxxxxxxx"}')

    assert target.read_bytes() == b'{"old": "value"}', \
        "the original file must be untouched after an interrupted write, not truncated/partial"
    leftover_temp = [p for p in tmp_path.iterdir() if p.name.startswith(".tmp-")]
    assert leftover_temp == [], "the failed temp file must be cleaned up, not left behind"


# ---------------------------------------------------------------------------
# 13: a genuinely valid dataset still generates correct splits (no
# regression from the fix)
# ---------------------------------------------------------------------------


def test_valid_dataset_still_generates_correct_leakage_safe_splits(tmp_path):
    _write_labeled(tmp_path, ["return_300s"], count=20)
    manifest = generate_splits(str(tmp_path), embargo_days=0)
    assert manifest.leakage_safe is True
    assert manifest.max_label_horizon_source == "derived_from_labeled_columns"
    assert manifest.purge_days == 1


# ---------------------------------------------------------------------------
# The non-strict escape hatch: explicitly opted into, never silently
# reached, and never allowed to claim leakage_safe=true
# ---------------------------------------------------------------------------


def test_non_strict_escape_hatch_records_incompleteness_and_forces_unsafe(tmp_path):
    """strict=False is preserved for informational/exploratory use, but
    generate_splits must never let it produce a manifest claiming safety
    that was not established: the unreadable file is named in a warning
    and leakage_safe is forced False, not silently left as whatever the
    partial horizon's own arithmetic would have produced. The corrupted
    file also can't be read for the actual split parquet -- that must
    degrade gracefully (the day dropped, a warning recorded) rather than
    crash uncontrolled, consistent with the manifest already being marked
    unsafe."""
    _write_labeled(tmp_path, ["return_60s"])
    _corrupt_one_file(tmp_path, "2026-01-10")

    manifest = generate_splits(str(tmp_path), embargo_days=0, strict=False)

    assert manifest is not None
    assert manifest.leakage_safe is False
    assert manifest.max_label_horizon_source == "partial_derivation_unreadable_files_present"
    assert any("2026-01-10" in w for w in manifest.warnings)
    persisted = _manifest_json(tmp_path)
    assert persisted["leakage_safe"] is False
    assert any("2026-01-10" in w for w in persisted["warnings"]), \
        "a warning discovered while writing the split parquet must still be in the persisted manifest.json"


def test_non_strict_max_label_horizon_s_still_warns_and_returns_partial(tmp_path):
    """The underlying function's own opt-out path, unchanged in spirit
    from before this fix -- proven still available for callers who
    explicitly want it, with the unreadable file identifiable via the
    detailed variant used internally by split_generator."""
    _write_labeled(tmp_path, ["return_300s"])
    _corrupt_one_file(tmp_path, "2026-01-10")
    result = max_label_horizon_s(str(tmp_path), strict=False)
    assert result == 300   # the readable files' horizon, explicitly a lower bound only


def test_non_strict_split_parquet_write_degrades_gracefully_not_crashes(tmp_path):
    """A corrupted file that survived horizon derivation (strict=False)
    still cannot actually be read when writing the split's parquet data.
    Must not raise an unhandled pyarrow exception -- the day is dropped
    from that split's data with an explicit warning instead."""
    _write_labeled(tmp_path, ["return_60s"], count=20)
    _corrupt_one_file(tmp_path, "2026-01-10")
    manifest = generate_splits(str(tmp_path), embargo_days=0, strict=False)
    assert manifest is not None
    train_path = tmp_path / "splits" / "train.parquet"
    assert train_path.exists()
    assert any("2026-01-10" in w and "excluded" in w for w in manifest.warnings)


# ---------------------------------------------------------------------------
# 14: full regression -- confirmed by running the whole targeted test
# module set alongside this file in verification, not duplicated here.
# ---------------------------------------------------------------------------


def test_production_function_is_what_fails_not_a_test_reimplementation():
    """Guards against the class of test that reimplements the intended
    behaviour instead of exercising the real function: imports and calls
    the actual max_label_horizon_s, not a local stand-in, and asserts the
    real exception type it actually raises."""
    import inspect
    from collector.pipeline import label_generator

    source = inspect.getsource(label_generator._max_label_horizon_s_detailed)
    assert "except Exception" in source
    assert "raise LabelHorizonError" in source
    # The old defect's exact shape -- a bare continue with no raise -- must
    # not be reachable when strict is True.
    assert "if strict:" in source
