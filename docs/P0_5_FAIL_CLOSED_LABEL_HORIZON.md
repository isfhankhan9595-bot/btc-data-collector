# P0-5: fail-closed label-horizon validation

## The defect

`max_label_horizon_s()` (`collector/pipeline/label_generator.py`) computed
the purge horizon by scanning every labeled Parquet file's schema for
`return_<N>s` columns and taking the maximum `N`. If a file's schema could
not be read (corrupt Parquet, permission denied, genuinely malformed), the
old code printed a warning and `continue`d — returning the maximum horizon
from only the files that happened to be readable. `split_generator.py`
then sized the purge from that number and could report `leakage_safe:
true` on a split whose safety was never actually established: `readable A
+ unreadable B + readable C` silently became `partial horizon -> split ->
leakage_safe=true`.

## The fix

`max_label_horizon_s(data_dir, *, strict=True)` now fails closed by
default: any file whose schema cannot be read raises `LabelHorizonError`
immediately, naming the exact file and the underlying exception, rather
than silently proceeding. `strict=False` remains available for
non-leakage-relevant, informational use, but `split_generator.py` never
uses it to justify safety — when explicitly opted into, the unreadable
files are recorded as an explicit warning, `leakage_safe` is forced
`False`, and (new) the actual split-parquet-writing step degrades
gracefully rather than crashing on the same unreadable file, dropping
just that day's data with a named warning instead of an unhandled
exception.

A second, related gap was fixed alongside it: `generate_splits` wrote
`split_manifest.json` and each split's `.parquet` file with a plain
truncating `open(path, "w")` / `to_parquet(path)`. A crash mid-write
(disk full, process killed) could leave a partially-written file in place
of a previously-valid one. `_atomic_write_bytes` now writes to a temp file
in the same directory, `fsync`s it, and only then `os.replace`s the
target — the previously-valid manifest or split file is either kept fully
intact or fully replaced, never left partial.

## What counts as "unreadable" vs. "legitimately empty"

Distinguished explicitly, not conflated: a file whose schema **cannot be
read at all** (`pq.read_schema` raises) fails closed. A file whose schema
**reads fine but has no `return_<N>s` columns** contributes `0` to the
horizon and is not an error — a dataset with no forward labels genuinely
needs no purge. Both are tested (`test_a_readable_file_with_no_return_columns_is_not_an_error`
vs. the unreadable-file tests).

## Horizon rule (unchanged, re-confirmed correct)

The maximum across every file wins, regardless of which file it appears
in — a larger horizon in an *earlier* file is still the conservative
maximum (`test_larger_horizon_in_an_earlier_file_is_still_the_conservative_maximum`,
pre-existing behavior, re-verified still holds after the fix).

## Manifest lineage

`SplitManifest.max_label_horizon_source` already distinguished
`"derived_from_labeled_columns"`, `"caller_supplied"`, and
`"no_return_columns_found:purge_0"` before this phase. A fourth value,
`"partial_derivation_unreadable_files_present"`, is added for the
non-strict escape hatch specifically — so a manifest can never claim
`"derived_from_labeled_columns"` (implying a complete derivation) when the
derivation was actually partial. No other lineage fields were added:
`purge_days`, `embargo_days`, `requested_gap_days`, `achieved_gap_*`,
`rationale`, and `warnings` already provide sufficient evidence for
reproducibility; fields were not added "for appearance."

## Determinism (re-confirmed, unchanged)

File discovery is `sorted(glob.glob(...))`, already deterministic for
zero-padded ISO date stems; unaffected by this phase.
`test_repeated_generation_from_the_same_input_is_deterministic` confirms
the full pipeline (horizon derivation, split boundaries, manifest
contents) is byte-identical across repeated runs on the same input.

## Real mutation testing (source mutated, suite run, restored, diff confirmed byte-identical)

| Mutation | Result |
|---|---|
| Reintroduce the exact original defect (bare `except Exception: continue`, no `raise`) in `label_generator.py` | 9 failures |
| Bypass `_atomic_write_bytes`'s atomicity (direct truncating `open(...).write()`, no temp+replace) | **0 failures against the `generate_splits`-level tests** — see below |

**The zero-failure result was investigated, not hidden.** Every
`generate_splits`-level failure test in this suite fails *before* any
write is ever attempted (validation-first design: horizon derivation
raises inside `_max_label_horizon_s_detailed`, well before
`_atomic_write_bytes` is called) — so those tests cannot exercise a crash
*during* a write, and correctly don't notice when atomicity is bypassed.
The atomicity protection is real and exists for a different failure
mode (an interruption mid-write, e.g. disk full), not for
"validation failed before writing" — those are already handled by
failing before any write happens at all. A direct unit test of
`_atomic_write_bytes` itself
(`test_atomic_write_helper_never_truncates_the_target_on_a_write_failure`,
simulating a partial write followed by an `OSError`) was added
specifically to close this gap, and the same atomicity-bypass mutation
was re-run against it: **1 failure**, confirming the fix.

## Tests

19 new tests in `test_p0_5_fail_closed_label_horizon.py`: unreadable/
corrupted/permission-denied files fail closed (3), the end-to-end
`generate_splits` refusal with no manifest of any kind written (1),
conservative-maximum-across-files re-confirmation (1), file-discovery
edge cases (2), readable-but-columnless files are not errors (1),
determinism (1), failed-generation-leaves-no-new-manifest +
byte-for-byte survival of the previous valid manifest (2), no leftover
temp files on success (1), the direct atomicity unit test (1), valid-
dataset regression (1), the non-strict escape hatch's warning/
`leakage_safe=False`/graceful-degradation behavior (3), and a guard
confirming the real production function (not a test reimplementation) is
what actually fails (1).

Existing tests: `test_label_generator.py` (2), `test_split_generator.py`,
`test_leakage_safe_splits.py` — **39 passed unchanged**, no weakening, no
deletions.

Full suite: **1260 passed** (1241 baseline + 19 new). `compileall` clean.
`git diff --check` clean. Standalone `CollectorApp()` construction check:
passes (P0-5 touches only the label/split pipeline, unrelated to the live
collector — confirmed, not assumed).

## Scope discipline

Files changed: `collector/pipeline/label_generator.py`,
`collector/pipeline/split_generator.py`,
`collector/tests/test_p0_5_fail_closed_label_horizon.py`, this document.
Not touched: P0-1 (not found anywhere in the repository — see below),
P0-2's WAL (`p0-2-quality-event-wal` branch, untouched, not merged into
this branch since P0-5 has no file dependency on it), P0-4's dedup index
(`p0-4-bounded-trade-dedup` branch/PR #57, untouched), any adapter,
replay, CVD, or order-book code, AWS/deployment configuration.

## P0-1 status (branch-state finding, not fabricated)

A full search (`git log --all --grep`, `git branch -a`, `git stash list`,
history of `websocket_client.py` across all branches) found **no trace of
P0-1** ("WebSocket receive/processing decoupling") anywhere in this
repository — no branch, no commit, no stash. It is reported here as
genuinely **NOT STARTED**, not recovered from memory and not
reconstructed, per this task's own explicit instruction not to recreate
it if it cannot be found. Implementing it was out of this task's scope
(P0-5 only).

## P0-2 / P0-4 branch topology (recorded, not merged)

- P0-2: branch `p0-2-quality-event-wal`, HEAD `a9fe2ab`, based on
  `main@790df62`. No PR opened yet (checked via the GitHub API). Untouched
  by this session.
- P0-4: branch `p0-4-bounded-trade-dedup`, HEAD `0816f03`, PR #57 (open,
  draft, not merged), based on `main@790df62`. **Production
  `adapters/base.py` remains exactly as before P0-4** — the SQLite index
  is a tested, ready component, deliberately not wired into
  `ExchangeAdapter._dedupe_trades`. Deployment benchmark and the
  crash-ordering decision remain the two concrete pieces of evidence
  needed before production adoption. This distinction is preserved here
  exactly as it must be in every future handoff: P0-4 is **not**
  "production dedup fixed."
- P0-2 and P0-4 touch disjoint files (`quality_wal.py`/`run_collector.py`/
  `config.py` vs. a new standalone `persistent_trade_id_index.py`) — no
  conflict exists between them. P0-5 has no file dependency on either, so
  it was branched directly from `main@790df62` rather than from a merged
  "cumulative" branch, per this task's own instruction to reason about
  dependency/conflict/scope rather than auto-merging.
