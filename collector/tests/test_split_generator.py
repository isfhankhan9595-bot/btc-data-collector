import json

import pandas as pd

from collector.pipeline.split_generator import generate_splits


def _write_labeled_dates(base_dir, count=20):
    labeled_dir = base_dir / "aligned" / "labeled"
    labeled_dir.mkdir(parents=True)
    dates = [f"2026-01-{day:02d}" for day in range(1, count + 1)]
    for date in dates:
        pd.DataFrame({"date": [date], "value": [1]}).to_parquet(labeled_dir / f"{date}.parquet")
    return dates


def _manifest(base_dir):
    return json.loads((base_dir / "splits" / "split_manifest.json").read_text())


def test_generate_splits_embargo_reduces_boundaries_and_prevents_train_val_overlap(tmp_path):
    _write_labeled_dates(tmp_path, count=20)

    generate_splits(str(tmp_path), embargo_days=1)
    manifest = _manifest(tmp_path)

    assert manifest["embargo_days"] == 1
    assert len(manifest["train"]) <= 13
    assert len(manifest["val"]) <= 2
    assert set(manifest["train"]).isdisjoint(manifest["val"])


def test_generate_splits_gap_is_taken_from_the_earlier_split_only(tmp_path):
    """The gap is removed from the end of the earlier split, not both sides.

    This test previously asserted that the first date of each later split was
    also dropped, i.e. that the gap was carved out of both sides of every
    boundary. That encodes the wrong model. Contamination here is
    directional: labels look *forward*, so it is the tail of the earlier
    split whose label windows reach across the boundary. The head of the
    later split is not contaminated by the earlier split's labels, and
    dropping it as well double-counts the gap -- discarding sound data and
    overstating the separation actually required.

    The property that matters is asserted directly instead: a real gap of at
    least the requested size exists at both boundaries, the splits are
    disjoint and strictly chronological, and the dropped tail dates appear
    nowhere.
    """
    dates = _write_labeled_dates(tmp_path, count=20)

    generate_splits(str(tmp_path), embargo_days=1)
    manifest = _manifest(tmp_path)

    n = len(dates)
    train_end = int(n * 0.7)
    val_end = int(n * 0.85)
    requested = manifest["requested_gap_days"]

    # The tail of each earlier split is what gets purged.
    purged_tail = set(dates[train_end - requested:train_end]) | set(
        dates[val_end - requested:val_end])
    split_dates = set(manifest["train"] + manifest["val"] + manifest["test"])
    assert split_dates.isdisjoint(purged_tail)

    # The head of the later split is retained -- it is not contaminated.
    assert dates[train_end] in manifest["val"]

    # And the separation is real, measured from the dates themselves.
    assert manifest["achieved_gap_train_val"] >= requested
    assert manifest["achieved_gap_val_test"] >= requested
    assert set(manifest["train"]).isdisjoint(manifest["val"])
    assert set(manifest["val"]).isdisjoint(manifest["test"])
    assert manifest["train"][-1] < manifest["val"][0] < manifest["test"][0]


def test_generate_splits_zero_embargo_matches_original_boundaries(tmp_path):
    """No forward labels in this fixture, so no purge is required either.

    The boundaries match the raw fractions only because the purge is measured
    from the labelled columns and this fixture has none. With real labels a
    zero embargo still yields a non-zero gap -- see
    test_purge_is_derived_from_label_columns.
    """
    dates = _write_labeled_dates(tmp_path, count=20)

    generate_splits(str(tmp_path), embargo_days=0)
    manifest = _manifest(tmp_path)

    n = len(dates)
    train_end = int(n * 0.7)
    val_end = int(n * 0.85)
    assert manifest["train"] == dates[:train_end]
    assert manifest["val"] == dates[train_end:val_end]
    assert manifest["test"] == dates[val_end:]
    assert manifest["embargo_days"] == 0
