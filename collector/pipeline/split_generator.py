"""Chronological, leakage-safe train/validation/test splits.

Why this file was rewritten
---------------------------
The previous implementation could write a manifest claiming an embargo it had
not applied. Its guards were of the form::

    dates[:train_end - embargo_days] if train_end > embargo_days else dates[:train_end]

so whenever a split was shorter than the requested embargo, the embargo was
**silently dropped** and the splits became adjacent -- while the manifest
still recorded ``"embargo_days": N``. Any downstream statistic computed from
those splits would be contaminated, and the artifact itself asserted it was
safe. The master rules prohibit exactly this ("do not silently skip requested
embargo periods; if an embargo is requested, verify programmatically that it
actually exists").

It also applied the gap on both sides of every boundary -- end of the earlier
split *and* start of the later one -- so a requested ``embargo_days=1``
produced a two-day gap while reporting one, and discarded twice the intended
data.

Model used here
---------------
Splits are day-granular and strictly chronological: ``train < val < test``.
With forward-looking labels the only leak direction is an **earlier** split's
label window reaching into a **later** split. So:

``purge_days``
    Dropped from the **end** of the earlier split. This is the leak-directional
    defence, and it must cover the longest label horizon. Derived from
    ``max_label_horizon_s`` unless given explicitly.

``embargo_days``
    An **additional** gap on top of the purge. Not required for a one-pass
    chronological split (no training resumes after the test period), but
    supported for walk-forward use where it is, and for deliberate extra
    conservatism.

The achieved gap is measured from the resulting date lists and written to the
manifest. If it does not match what was requested, the run fails instead of
producing a mislabelled artifact.
"""
from __future__ import annotations

import glob
import json
import math

from collector.pipeline.label_generator import (
    max_label_horizon_s as observed_max_label_horizon_s,
)
import os
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from typing import Optional

SECONDS_PER_DAY = 86_400

#: Longest label horizon produced by ``label_generator`` today.

TRAIN_FRACTION = 0.70
VAL_FRACTION = 0.85


class LeakageError(RuntimeError):
    """A split could not be produced without contaminating a later split."""


@dataclass
class SplitManifest:
    train: list[str]
    val: list[str]
    test: list[str]
    purge_days: int
    embargo_days: int
    requested_gap_days: int
    achieved_gap_train_val: Optional[int]
    achieved_gap_val_test: Optional[int]
    max_label_horizon_s: int
    leakage_safe: bool
    rationale: str
    #: Where the horizon came from: measured from the labels, or supplied.
    #: A purge sized from an assumed constant is not the same evidence as one
    #: measured off the label columns, so the distinction is persisted.
    max_label_horizon_source: str = "caller_supplied"
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def _parse_dates(files: list[str]) -> list[tuple[date, str]]:
    """Parse each file stem as an ISO date.

    ``sorted(glob(...))`` is a *lexical* sort. It happens to be chronological
    for zero-padded ISO stems and is wrong for anything else -- a single
    ``2026-1-5`` among ``2026-01-05`` style names reorders the timeline and
    puts later data into an earlier split. Parsing makes the ordering real and
    makes a bad name an error rather than a silent reordering.
    """
    parsed: list[tuple[date, str]] = []
    bad: list[str] = []
    for path in files:
        stem = os.path.basename(path)[: -len(".parquet")]
        try:
            parsed.append((datetime.strptime(stem, "%Y-%m-%d").date(), stem))
        except ValueError:
            bad.append(stem)
    if bad:
        raise LeakageError(
            "labeled files must be named YYYY-MM-DD.parquet so the split is "
            f"provably chronological; unparseable: {sorted(bad)[:10]}"
        )
    parsed.sort(key=lambda item: item[0])
    duplicates = {s for _, s in parsed if [n for _, n in parsed].count(s) > 1}
    if duplicates:
        raise LeakageError(f"duplicate dates in labeled set: {sorted(duplicates)}")
    return parsed


def _gap_days(earlier: list[tuple[date, str]], later: list[tuple[date, str]]) -> Optional[int]:
    """Calendar days strictly between the two split boundaries.

    Measured from the data itself rather than inferred from the slicing
    arithmetic, so the number in the manifest is an observation.
    """
    if not earlier or not later:
        return None
    return (later[0][0] - earlier[-1][0]).days - 1


def _required_purge_days(max_label_horizon_s: int) -> int:
    """Whole days needed to cover the longest forward label.

    Splits are day-granular, so a label horizon of any length below one day
    still requires a full day of purge: the last row of a retained day sits
    moments before the boundary and its label reaches across it.
    """
    if max_label_horizon_s <= 0:
        return 0
    return max(1, math.ceil(max_label_horizon_s / SECONDS_PER_DAY))


def build_manifest(
    dates: list[str],
    *,
    purge_days: Optional[int] = None,
    embargo_days: int = 0,
    max_label_horizon_s: int,
    strict: bool = True,
) -> SplitManifest:
    """Compute the split manifest without touching the filesystem.

    Separated from :func:`generate_splits` so the leakage properties are
    testable without writing parquet.
    """
    if embargo_days < 0:
        raise ValueError("embargo_days must be non-negative")
    if purge_days is not None and purge_days < 0:
        raise ValueError("purge_days must be non-negative")
    if max_label_horizon_s < 0:
        raise ValueError("max_label_horizon_s must be non-negative")

    required = _required_purge_days(max_label_horizon_s)
    effective_purge = required if purge_days is None else purge_days
    requested_gap = effective_purge + embargo_days

    warnings: list[str] = []
    if effective_purge < required:
        message = (
            f"purge_days={effective_purge} does not cover the longest label "
            f"horizon ({max_label_horizon_s}s needs {required} day(s)); "
            "an earlier split's labels will reach into a later split"
        )
        if strict:
            raise LeakageError(message)
        warnings.append(message)

    parsed = _parse_dates([f"{d}.parquet" for d in dates])
    total = len(parsed)
    train_end = int(total * TRAIN_FRACTION)
    val_end = int(total * VAL_FRACTION)

    # The gap is taken from the END of the earlier split only. Forward-looking
    # labels leak forward, so that is the side that contaminates; removing
    # from the start of the later split as well double-counts the gap.
    train = parsed[: max(train_end - requested_gap, 0)]
    val = parsed[train_end: max(val_end - requested_gap, train_end)]
    test = parsed[val_end:]

    achieved_train_val = _gap_days(train, val)
    achieved_val_test = _gap_days(val, test)

    # An empty split is checked first, and is fatal in strict mode. It was
    # previously only a warning, which meant the one case that escaped the
    # strict guarantee was the *worst* one: a requested gap wide enough to
    # consume a whole split produced a manifest with no train set at all,
    # still written to disk, with test.parquet emitted beside it. "The gap
    # could not be achieved" and "the split no longer exists" are the same
    # failure, and the second is not the milder of the two.
    for name, split in (("train", train), ("val", val), ("test", test)):
        if not split:
            message = (
                f"{name} split is empty: a {requested_gap}-day gap "
                f"(purge={effective_purge} + embargo={embargo_days}) cannot be "
                f"taken from {total} dates at the {name} boundary"
            )
            if strict:
                raise LeakageError(message)
            warnings.append(message)

    # Never claim a gap that was not achieved. A short split cannot silently
    # collapse the embargo the way the previous implementation did.
    for name, achieved, earlier, later in (
        ("train->val", achieved_train_val, train, val),
        ("val->test", achieved_val_test, val, test),
    ):
        if not earlier or not later:
            warnings.append(f"{name}: one side is empty, no gap is defined")
            continue
        if achieved is None or achieved < requested_gap:
            message = (
                f"{name}: requested a {requested_gap}-day gap "
                f"(purge={effective_purge} + embargo={embargo_days}) but only "
                f"{achieved} day(s) could be achieved from {total} dates"
            )
            if strict:
                raise LeakageError(message)
            warnings.append(message)

    leakage_safe = not warnings and effective_purge >= required

    rationale = (
        f"purge_days={effective_purge} chosen to cover the longest label "
        f"horizon of {max_label_horizon_s}s at day granularity "
        f"({required} day(s) required); embargo_days={embargo_days} added on "
        f"top. Gap is removed from the end of the earlier split only, because "
        f"forward-looking labels leak forward. Achieved gaps are measured from "
        f"the resulting dates, not inferred."
    )

    return SplitManifest(
        train=[s for _, s in train],
        val=[s for _, s in val],
        test=[s for _, s in test],
        purge_days=effective_purge,
        embargo_days=embargo_days,
        requested_gap_days=requested_gap,
        achieved_gap_train_val=achieved_train_val,
        achieved_gap_val_test=achieved_val_test,
        max_label_horizon_s=max_label_horizon_s,
        leakage_safe=leakage_safe,
        rationale=rationale,
        warnings=warnings,
    )


def verify_manifest(manifest: SplitManifest) -> list[str]:
    """Re-derive the leakage properties from the manifest alone.

    A second, independent check: ordering, disjointness and the achieved gaps
    are all recomputed from the recorded date lists rather than trusted.
    """
    problems: list[str] = []
    sets = {"train": set(manifest.train), "val": set(manifest.val), "test": set(manifest.test)}
    for a, b in (("train", "val"), ("train", "test"), ("val", "test")):
        overlap = sets[a] & sets[b]
        if overlap:
            problems.append(f"{a} and {b} overlap: {sorted(overlap)}")

    def as_dates(names: list[str]) -> list[date]:
        return [datetime.strptime(n, "%Y-%m-%d").date() for n in names]

    for name, names in (("train", manifest.train), ("val", manifest.val), ("test", manifest.test)):
        parsed = as_dates(names)
        if parsed != sorted(parsed):
            problems.append(f"{name} is not chronologically ordered")

    for name, names in (("train", manifest.train), ("val", manifest.val),
                        ("test", manifest.test)):
        if not names:
            problems.append(f"{name} split is empty")

    for label, earlier, later in (
        ("train->val", manifest.train, manifest.val),
        ("val->test", manifest.val, manifest.test),
    ):
        if not earlier or not later:
            problems.append(f"{label}: one side is empty, no gap can be verified")
            continue
        if as_dates(earlier)[-1] >= as_dates(later)[0]:
            problems.append(f"{label}: splits are not strictly chronological")
        gap = (as_dates(later)[0] - as_dates(earlier)[-1]).days - 1
        if gap < manifest.requested_gap_days:
            problems.append(
                f"{label}: manifest claims a {manifest.requested_gap_days}-day "
                f"gap but the recorded dates give {gap}")
    return problems


def generate_splits(
    data_dir: str = "data",
    embargo_days: int = 0,
    *,
    purge_days: Optional[int] = None,
    max_label_horizon_s: Optional[int] = None,
    strict: bool = True,
    write_parquet: bool = True,
) -> Optional[SplitManifest]:
    """Write a leakage-verified split manifest for the labelled data.

    ``max_label_horizon_s`` is **derived from the labelled columns** when it is
    not supplied. A constant would silently decouple the purge from the labels:
    add a label horizon longer than the one assumed and every later split
    would be under-purged while the manifest still reported itself
    leakage-safe, which is the precise failure this module exists to prevent.
    The origin of the number is recorded in ``max_label_horizon_source`` so a
    researcher can see whether the purge was measured or assumed.
    """
    labeled_dir = os.path.join(data_dir, "aligned", "labeled")
    files = glob.glob(os.path.join(labeled_dir, "*.parquet"))
    if not files:
        print("No labeled files found.")
        return None

    if max_label_horizon_s is None:
        derived = observed_max_label_horizon_s(data_dir)
        if derived > 0:
            horizon_source = "derived_from_labeled_columns"
        else:
            # Files exist but carry no recognisable ``return_<N>s`` column. A
            # dataset with no forward labels genuinely needs no purge, so this
            # is not treated as unsafe -- but it is never assumed silently.
            horizon_source = "no_return_columns_found:purge_0"
        max_label_horizon_s = derived
    else:
        horizon_source = "caller_supplied"
    print(f"max_label_horizon_s={max_label_horizon_s} ({horizon_source})")

    parsed = _parse_dates(files)
    manifest = build_manifest(
        [stem for _, stem in parsed],
        purge_days=purge_days,
        embargo_days=embargo_days,
        max_label_horizon_s=max_label_horizon_s,
        strict=strict,
    )
    manifest.max_label_horizon_source = horizon_source

    problems = verify_manifest(manifest)
    if problems:
        message = "split manifest failed verification: " + "; ".join(problems)
        if strict:
            raise LeakageError(message)
        manifest.warnings.append(message)
        manifest.leakage_safe = False

    out_dir = os.path.join(data_dir, "splits")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "split_manifest.json"), "w") as handle:
        json.dump(manifest.to_dict(), handle, indent=2)

    print(f"Saved split_manifest.json (leakage_safe={manifest.leakage_safe})")
    print(f"purge={manifest.purge_days}d embargo={manifest.embargo_days}d "
          f"requested_gap={manifest.requested_gap_days}d "
          f"achieved train->val={manifest.achieved_gap_train_val} "
          f"val->test={manifest.achieved_gap_val_test}")
    print(f"Train: {len(manifest.train)} days, Val: {len(manifest.val)} days, "
          f"Test: {len(manifest.test)} days")
    for warning in manifest.warnings:
        print(f"WARNING: {warning}")

    if write_parquet:
        import pandas as pd

        for split_name in ("train", "val", "test"):
            split_dates = getattr(manifest, split_name)
            if not split_dates:
                continue
            frames = [pd.read_parquet(os.path.join(labeled_dir, f"{d}.parquet"))
                      for d in split_dates]
            combined = pd.concat(frames, ignore_index=True)
            combined.to_parquet(os.path.join(out_dir, f"{split_name}.parquet"),
                                compression="snappy")
            print(f"Saved {split_name}.parquet")

    return manifest


if __name__ == "__main__":
    generate_splits()
