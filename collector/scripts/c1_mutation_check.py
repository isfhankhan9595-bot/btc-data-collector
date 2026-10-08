"""C1 mutation harness: does the test suite actually notice when a C1 protection is removed?

Usage (from the repository root)::

    python collector/scripts/c1_mutation_check.py            # all mutants
    python collector/scripts/c1_mutation_check.py NAME ...   # selected mutants

Each mutant is an exact-text patch of ``collector/collector/segment_dedup.py`` applied to a
THROW-AWAY copy of the working tree (the real files are never modified). A patch whose
anchor text is not found exactly once aborts the run (a mutation that silently did nothing
would be a false "survivor"/"kill"). The unmutated copy must pass first; then every mutant
must make an INTENDED test fail (see INTENDED), not merely any test. Exit status is 0 only if the baseline passes and every
mutant is killed. Stdlib only.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGET = Path("collector/collector/segment_dedup.py")
TESTS = ["tests/test_c1_dedup_index_audit.py", "tests/test_segment_dedup.py", "tests/test_f1_durable_publication.py"]

# name -> (what is removed, [(anchor, replacement), ...])
INTENDED = {'skip_membership_digest_compare': ['test_1_deleted', 'test_2_fake', 'test_3_swapped', 'test_6_identity'], 'membership_compare_count_only': ['test_3_swapped', 'test_6b'], 'skip_missing_identity_audit': ['test_1b'], 'skip_extra_identity_audit': ['test_orphan'], 'skip_identity_digest_check': ['test_4_wrong_recorded'], 'skip_identity_count_check': ['test_4_wrong_recorded'], 'sqlite_is_its_own_reference': ['test_self_consistent'], 'accept_duplicate_identity_across_segments_at_commit': ['test_commit_segment_refuses', 'test_genuine_cross'], 'skip_ownership_audit_of_duplicates': ['test_5b'], 'accept_identity_conflict_on_equal_evidence': ['test_commit_segment_refuses'], 'bypass_schema_version_rebuild': ['test_current_shape_with_a_different'], 'bypass_schema_shape_check': ['test_orphan_membership_rows_and_dropped'], 'skip_encoding_check_of_rows': ['test_15a'], 'drop_encoding_canary': ['test_15b'], 'evidence_not_bound_to_segment_bytes': ['test_evidence_bound'], 'skip_deep_verify': ['test_7_', 'test_8_', 'test_forged_evidence'], 'skip_evidence_rederivation': ['test_missing_or_garbage', 'test_lost_evidence'], 'skip_marker_evidence_check': ['test_14a'], 'evidence_conflict_not_checked_before_files': ['test_14b'], 'reset_not_atomic': ['test_11a'], 'no_rebuild_on_divergence': ['test_1_deleted'], 'skip_indexing_unreconciled_segments': ['test_11c', 'test_10_'], 'ownership_written_to_wrong_segment': ['test_normal_run_membership']}

MUTANTS = {
    "skip_membership_digest_compare": (
        "per-segment membership vs evidence comparison (missing/extra/swapped/mis-owned identities)",
        [("elif segment_id in expected and (n, digest) != expected[segment_id]:",
          "elif False and segment_id in expected and (n, digest) != expected[segment_id]:")]),
    "membership_compare_count_only": (
        "membership digest check weakened to a row count (a swap keeps the count)",
        [("elif segment_id in expected and (n, digest) != expected[segment_id]:",
          "elif segment_id in expected and n != expected[segment_id][0]:")]),
    "skip_missing_identity_audit": (
        "segment whose identities are ALL gone has no membership entry to compare",
        [("if segment_id not in membership and count != 0:", "if False:")]),
    "skip_extra_identity_audit": (
        "identity rows owned by an unknown segment_id",
        [("if segment_id not in by_id:", "if False:")]),
    "skip_identity_digest_check": (
        "recorded identity_digest vs evidence",
        [("if (row.identity_count, row.identity_digest) != held:", "if row.identity_count != held[0]:")]),
    "skip_identity_count_check": (
        "recorded identity_count vs evidence",
        [("if (row.identity_count, row.identity_digest) != held:", "if row.identity_digest != held[1]:")]),
    "sqlite_is_its_own_reference": (
        "independent evidence: expected values taken from SQLite's own recorded row",
        [("if (row.identity_count, row.identity_digest) != held:", "if False:"),
         ("expected[row.segment_id] = held", "expected[row.segment_id] = (row.identity_count, row.identity_digest)")]),
    "accept_duplicate_identity_across_segments_at_commit": (
        "commit refuses an identity owned by another segment",
        [("if owned is not None:", "if False:")]),
    "skip_ownership_audit_of_duplicates": (
        "audit flags an identity owned by several segments",
        [("for identity, segment_ids in duplicates:", "for identity, segment_ids in []:")]),
    "accept_identity_conflict_on_equal_evidence": (
        "equal evidence but a different identity set is a conflict, not a no-op",
        [("if (row[2], row[3], row[4]) == (count, digest, encoding):", "if True:")]),
    "bypass_schema_version_rebuild": (
        "older user_version (previous F1 schema) forces a rebuild",
        [("        if version != INDEX_SCHEMA_VERSION:\n", "        if False:\n")]),
    "bypass_schema_shape_check": (
        "table/index shape must match the current layout",
        [("if have != want or extra:", "if False:")]),
    "skip_encoding_check_of_rows": (
        "identity-key encoding recorded per segment row",
        [("if row.identity_encoding != fingerprint:", "if False:")]),
    "drop_encoding_canary": (
        "content canary inside the encoding fingerprint (a forgotten version bump)",
        [("{hashlib.sha256(probe.encode('utf-8')).hexdigest()[:16]}", "x")]),
    "evidence_not_bound_to_segment_bytes": (
        "evidence file bound to the marker's sha256/size",
        [('obj.get("segment_sha256") == sha and obj.get("segment_size") == size', "True")]),
    "skip_deep_verify": (
        "DEDUP_DEEP_VERIFY re-derives identity sets from segment bytes",
        [('deep = os.environ.get(DEEP_VERIFY_ENV) == "1"', "deep = False")]),
    "skip_evidence_rederivation": (
        "missing/stale evidence is re-derived from the bytes",
        [('if state != "ok" or deep:', "if False:")]),
    "skip_marker_evidence_check": (
        "indexed sha256/size must equal the marker",
        [('if row.sha256 != pub["sha256"] or row.size != pub["size_bytes"]:', "if False:")]),
    "evidence_conflict_not_checked_before_files": (
        "stale index row is refused before any evidence file is touched",
        [("if held is not None and (held[0], held[1]) != (sha, size):", "if False:")]),
    "reset_not_atomic": (
        "rebuild/migration is ONE transaction",
        [('        self._conn.execute("BEGIN IMMEDIATE")\n        try:\n            if drop:\n'
          '                self._conn.execute("DROP TABLE IF EXISTS seen")\n'
          '                self._conn.execute("DROP TABLE IF EXISTS reconciled_segments")\n',
          '        if drop:\n            self._conn.execute("DROP TABLE IF EXISTS seen")\n'
          '            self._conn.execute("DROP TABLE IF EXISTS reconciled_segments")\n'
          '        self._conn.execute("BEGIN IMMEDIATE")\n        try:\n')]),
    "ownership_written_to_wrong_segment": (
        "commit attributes identities to the segment they were read from",
        [("segment_id = cur.lastrowid", "segment_id = max(cur.lastrowid - 1, 1)")]),
    "no_rebuild_on_divergence": (
        "divergence triggers the rebuild",
        [("            self.index.rebuild_reset()\n\n        for path in confirmed:",
          "            pass\n\n        for path in confirmed:")]),
    "skip_indexing_unreconciled_segments": (
        "confirmed-but-unreconciled segments are indexed before ingestion",
        [("            self._index_segment(path, read_marker(path))\n            report.indexed += 1",
          "            report.indexed += 1")]),
}


def run_pytest(tree: Path, tests=None, stop_first=True) -> subprocess.CompletedProcess:
    env = dict(os.environ, PYTHONPATH=f"{tree}:{tree / 'collector'}", PYTHONDONTWRITEBYTECODE="1")
    env.pop("DEDUP_DEEP_VERIFY", None)
    return subprocess.run([sys.executable, "-m", "pytest", *(["-x"] if stop_first else []), "-q", "-p", "no:cacheprovider",
                           *(tests or TESTS)],
                          cwd=tree / "collector", env=env, capture_output=True, text=True, timeout=900)


def main(argv: list[str]) -> int:
    wanted = argv or list(MUTANTS)
    unknown = [n for n in wanted if n not in MUTANTS]
    if unknown:
        print(f"unknown mutants: {unknown}\nknown: {sorted(MUTANTS)}")
        return 2
    results = []
    with tempfile.TemporaryDirectory(prefix="c1_mut_") as tmp:
        tree = Path(tmp) / "tree"
        shutil.copytree(ROOT, tree, ignore=shutil.ignore_patterns(".git", "__pycache__", ".pytest_cache"))
        base = run_pytest(tree)
        print(f"baseline (unmutated): {'PASS' if base.returncode == 0 else 'FAIL'}")
        if base.returncode != 0:
            print(base.stdout[-3000:])
            return 1
        original = (tree / TARGET).read_text()
        for name in wanted:
            what, patches = MUTANTS[name]
            text = original
            for anchor, replacement in patches:
                if text.count(anchor) != 1:
                    print(f"ABORT: anchor for {name!r} found {text.count(anchor)} times (need exactly 1): {anchor[:70]!r}")
                    return 2
                text = text.replace(anchor, replacement)
            (tree / TARGET).write_text(text)
            proc = run_pytest(tree, tests=[TESTS[0]], stop_first=False)      # the C1 file, every failure listed
            failing = [ln.split("::", 1)[1].split(" ")[0] for ln in proc.stdout.splitlines() if ln.startswith("FAILED")]
            hit = [f for f in failing if any(f.startswith(i) or i in f for i in INTENDED[name])]
            killed = bool(hit)                      # killed BY THE INTENDED test, not by any stray failure
            note = f"intended={hit[0]}" if hit else f"NOT killed by intended {INTENDED[name]}; other failures: {failing[:3]}"
            results.append((name, killed, what, note))
            print(f"{'KILLED  ' if killed else 'SURVIVED'}  {name:52s} {note[:80]}  (+{len(failing) - len(hit)} other failing)")
        (tree / TARGET).write_text(original)
    survivors = [r[0] for r in results if not r[1]]
    print(f"\n{len(results) - len(survivors)}/{len(results)} mutants killed" + (f"; SURVIVORS: {survivors}" if survivors else ""))
    return 1 if survivors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
