"""C1 mutation harness: does the test suite actually notice when a C1 protection is removed?

Usage (from the repository root)::

    python collector/scripts/c1_mutation_check.py                     # every mutant of every suite
    python collector/scripts/c1_mutation_check.py --suite c1         # the C1 production code only
    python collector/scripts/c1_mutation_check.py --suite rehearsal  # the migration/rehearsal tool only
    python collector/scripts/c1_mutation_check.py NAME ...           # selected mutants (suite inferred)

Two suites. ``c1`` mutates ``segment_dedup.py`` (attribution: a failing test whose name starts with / contains an
INTENDED entry). ``rehearsal`` mutates ``scripts/c1_migration_rehearsal.py`` and attributes EXACTLY: a mutant is killed
only when one of its named INTENDED test functions (a parametrized id counts as its function) fails -- a collateral
failure elsewhere does not count.

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
INTENDED = {
    'digest_domain_changed': ['test_identity_set_digest_known_answer'],
    'digest_framing_removed': ['test_identity_set_digest_known_answer'],
    'ignore_non_text_identity_type': ['test_9b_non_text'],
    'future_schema_not_checked_at_startup': ['test_10b_future_schema'],
    'reset_schema_ignores_future_schema': ['test_10c_rebuild_reset'],
    'schema_problem_treats_future_as_old': ['test_10b_future_schema', 'test_10c_rebuild_reset'],
    'skip_membership_digest_compare': ['test_1_deleted', 'test_2_fake', 'test_3_swapped', 'test_6_identity'], 'membership_compare_count_only': ['test_3_swapped', 'test_6b'], 'skip_missing_identity_audit': ['test_1b'], 'skip_extra_identity_audit': ['test_orphan'], 'skip_identity_digest_check': ['test_4_wrong_recorded'], 'skip_identity_count_check': ['test_4_wrong_recorded'], 'sqlite_is_its_own_reference': ['test_self_consistent'], 'accept_duplicate_identity_across_segments_at_commit': ['test_commit_segment_refuses', 'test_genuine_cross'], 'skip_ownership_audit_of_duplicates': ['test_5b'], 'accept_identity_conflict_on_equal_evidence': ['test_commit_segment_refuses'], 'bypass_schema_version_rebuild': ['test_current_shape_with_a_different'], 'bypass_schema_shape_check': ['test_orphan_membership_rows_and_dropped'], 'skip_encoding_check_of_rows': ['test_15a'], 'drop_encoding_canary': ['test_15b'], 'evidence_not_bound_to_segment_bytes': ['test_evidence_bound'], 'skip_deep_verify': ['test_7_', 'test_8_', 'test_forged_evidence'], 'skip_evidence_rederivation': ['test_missing_or_garbage', 'test_lost_evidence'], 'skip_marker_evidence_check': ['test_14a'], 'evidence_conflict_not_checked_before_files': ['test_14b'], 'reset_not_atomic': ['test_11a'], 'no_rebuild_on_divergence': ['test_1_deleted'], 'skip_indexing_unreconciled_segments': ['test_11c', 'test_10_'], 'ownership_written_to_wrong_segment': ['test_normal_run_membership']}

MUTANTS = {
    "digest_domain_changed": (
        "identity-set digest domain tag changed (every stored digest would silently change meaning)",
        [('_DIGEST_DOMAIN = b"btc-collector/dedup-identity-set/1\\x00"', '_DIGEST_DOMAIN = b"btc-collector/dedup-identity-set/2\\x00"')]),
    "digest_framing_removed": (
        "identity-set digest loses its length framing (ambiguous concatenation)",
        [("    h.update(_LEN(len(key_bytes)) + key_bytes)", "    h.update(key_bytes)"),
         ("                entry[1].update(_LEN(len(kb)) + kb)", "                entry[1].update(kb)")]),
    "ignore_non_text_identity_type": (
        "a non-TEXT stored identity_key is normalised into the TEXT digest (contains() would still miss it)",
        [('                if kind != "text":', "                if False:")]),
    "future_schema_not_checked_at_startup": (
        "startup does not refuse a future index schema before touching markers",
        [("        if future is not None:        # before ANY marker is renamed or written: an unknown layout is never rebuilt",
          "        if False:        # before ANY marker is renamed or written: an unknown layout is never rebuilt")]),
    "reset_schema_ignores_future_schema": (
        "rebuild/reset will drop and downgrade a future-versioned index",
        [("            if future is not None:                       # never drop/downgrade a layout this code does not know",
          "            if False:                       # never drop/downgrade a layout this code does not know")]),
    "schema_problem_treats_future_as_old": (
        "a future schema is described as an old one to rebuild",
        [("        future = future_schema_message(version)\n        if future is not None:\n            return future\n", "")]),
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
        "separately stored evidence is bypassed: expected values taken from SQLite's own recorded row",
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
        [('        self._conn.execute("BEGIN IMMEDIATE")\n        try:\n'
          '            future = self._future_schema_problem_in_txn()\n'
          '            if future is not None:                       # never drop/downgrade a layout this code does not know\n'
          '                raise DedupStateError(future)\n'
          '            if drop:\n'
          '                self._conn.execute("DROP TABLE IF EXISTS seen")\n'
          '                self._conn.execute("DROP TABLE IF EXISTS reconciled_segments")\n',
          '        if drop:\n            self._conn.execute("DROP TABLE IF EXISTS seen")\n'
          '            self._conn.execute("DROP TABLE IF EXISTS reconciled_segments")\n'
          '        self._conn.execute("BEGIN IMMEDIATE")\n        try:\n'
          '            future = self._future_schema_problem_in_txn()\n'
          '            if future is not None:                       # never drop/downgrade a layout this code does not know\n'
          '                raise DedupStateError(future)\n')]),
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


REHEARSAL_TARGET = Path("collector/scripts/c1_migration_rehearsal.py")
REHEARSAL_TESTS = ["tests/test_c1_migration_rehearsal.py"]
PRESET = "test_preset_matches_the_production_runner_wiring"
CHANGES = "test_input_changes_during_the_run_are_detected"
SYMLINKS = "test_symlinks_into_the_input_tree_cannot_hide_scratch_or_outputs"
# name -> (what, [(anchor, replacement)], [INTENDED test function names])
REHEARSAL_MUTANTS = {
    "preset_bybit_wrong_exchange": ("Bybit preset names the wrong exchange",
        [('dict(stream="bybit_trades", exchange="BYBIT",', 'dict(stream="bybit_trades", exchange="BINANCE",')], [PRESET]),
    "preset_okx_trades_all_wrong_event_stream": ("OKX trades-all preset uses the plain trades event stream",
        [('event_stream="trades-all", trade_id_field="trade_id"),', 'event_stream="trades", trade_id_field="trade_id"),')], [PRESET]),
    "preset_binance_spot_wrong_market_type": ("Binance spot preset is a perpetual",
        [('market_type="spot",', 'market_type="linear_perpetual",')], [PRESET]),
    "preset_binance_spot_wrong_stream": ("Binance spot preset reads the wrong stream directory",
        [('dict(stream="spot_trades",', 'dict(stream="binance_spot_trades",')], [PRESET]),
    "preset_binance_usdm_wrong_trade_id_field": ("Binance USD-M preset reads the wrong trade-id column",
        [('trade_id_field="native_trade_id"),', 'trade_id_field="trade_id"),')], [PRESET]),
    "preset_okx_trades_wrong_stream": ("OKX trades preset reads the wrong stream directory",
        [('dict(stream="okx_trades",', 'dict(stream="okx_trades_x",')], [PRESET]),
    "read_only_check_hardcoded_true": ("read_only_check.unchanged no longer reflects the comparison",
        [('"unchanged": not changed,', '"unchanged": True,')], [CHANGES]),
    "snapshot_ignores_ctime": ("same-size in-place rewrite with a restored mtime goes unnoticed",
        [("st.st_mtime_ns, st.st_ctime_ns, target)", "st.st_mtime_ns, 0, target)")], [CHANGES]),
    "snapshot_ignores_directories": ("directory additions are not recorded",
        [('                snap[f"{label}:{rel_dir}"] = _stat_entry(dirpath)', "                pass")], [CHANGES]),
    "snapshot_ignores_index_sidecars": ("-wal/-shm/-journal created beside the index are not watched",
        [('f"siblings:{self.index_path.name}"))', '"file"))')], [CHANGES]),
    "snapshot_ignores_evidence_dir": ("evidence directory is not watched",
        [('            watch.append(("evidence", self.evidence_dir, "tree"))', "            pass")], [CHANGES]),
    "evidence_size_binding_removed": ("evidence is no longer bound to the segment size",
        [('obj["segment_sha256"] != sha or obj["segment_size"] != size', 'obj["segment_sha256"] != sha')],
        ["test_evidence_is_bound_to_segment_size_not_only_sha"]),
    "unsorted_segment_discovery": ("segments are processed in directory-listing order",
        [('segs = sorted(self.stream_dir.glob("*.seg"))', 'segs = list(self.stream_dir.glob("*.seg"))')],
        ["test_report_does_not_depend_on_directory_listing_order"]),
    "unsorted_orphan_evidence": ("orphan evidence examples depend on directory-listing order",
        [('for p in sorted(self.evidence_dir.glob("*" + sd.EVIDENCE_SUFFIX)):', 'for p in list(self.evidence_dir.glob("*" + sd.EVIDENCE_SUFFIX)):')],
        ["test_report_does_not_depend_on_directory_listing_order"]),
    "marker_reason_leaks_path": ("an unreadable-marker OSError text (with the absolute path) enters the report",
        [("_marker_reason(marker.reason)[:100]", "marker.reason[:100]")],
        ["test_identical_damaged_datasets_at_different_paths_have_the_same_report_digest"]),
    "default_scratch_inside_input_not_checked": ("a default scratch directory (TMPDIR) inside the input is accepted",
        [("    if _inside(parent, protected):", "    if False:")], ["test_tmpdir_inside_the_input_tree_is_refused"]),
    "tmpdir_env_probed_before_check": ("TMPDIR inside the input is only noticed after tempfile has probed it",
        [("            if value and _inside(Path(value), protected):", "            if False:")], ["test_tmpdir_inside_the_input_tree_is_refused"]),
    "explicit_scratch_inside_input_not_checked": ("--scratch-dir inside the input is accepted",
        [("        if _inside(explicit, protected):", "        if False:")], [SYMLINKS]),
    "outputs_inside_input_not_checked": ("--json-out/--detail-out inside the input are accepted",
        [("            if out is not None and _inside(out, protected):", "            if False:")],
        ["test_outputs_inside_the_input_tree_are_refused", SYMLINKS]),
    "inside_check_does_not_resolve_symlinks": ("a symlink into the input hides a scratch/output location",
        [("    p = Path(os.path.realpath(path))", "    p = Path(os.path.abspath(path))")], [SYMLINKS]),
    "data_root_not_protected": ("a scratch directory elsewhere under --data-root is accepted",
        [("        if self.data_root is not None:\n            roots.append(self.data_root)\n", "")],
        ["test_tmpdir_inside_the_input_tree_is_refused"]),
    "cross_only_counts_all_occurrences": ("cross_only_extra_rows counts intra-segment repetitions too",
        [('a["cross_only"] += len(group) - 1', 'a["cross_only"] += total - 1')], ["test_cross_only_extra_rows_metric"]),
    "legacy_overlap_not_reported": ("legacy/.seg overlap hours are never found",
        [("both = sorted(set(self._legacy_files) & set(self._seg_hours))", "both = []")],
        ["test_legacy_parquet_overlap_by_hour_is_reported_and_never_merged"]),
    "legacy_overlap_is_union": ("every legacy-or-seg hour is called an overlap",
        [("both = sorted(set(self._legacy_files) & set(self._seg_hours))", "both = sorted(set(self._legacy_files) | set(self._seg_hours))")],
        ["test_legacy_parquet_overlap_by_hour_is_reported_and_never_merged"]),
    "future_schema_treated_as_old": ("a future index schema is compared/rebuilt like an old one",
        [("        if version > cur_version:", "        if False:")], ["test_future_index_schema_is_reported_as_unsupported_not_as_a_rebuild"]),
    "non_text_key_not_reported": ("the non-TEXT identity_key count is hard-wired to zero",
        [('"identity_rows_with_non_text_storage_type": idx.non_text_identity_rows(),', '"identity_rows_with_non_text_storage_type": 0,')],
        ["test_non_text_identity_key_is_reported_as_divergence_by_the_tool"]),
    "preflight_never_errors": ("a clearly-too-small scratch volume does not stop the run",
        [("        if free < expected:", "        if False:")], ["test_scratch_preflight_refuses_when_clearly_too_small_and_creates_nothing"]),
    "preflight_estimate_is_a_constant": ("the scratch estimate uses a fixed bytes-per-row instead of a measurement",
        [('dense, scattered = measured["dense"], measured["scattered"] * (max_len / avg_len)', "dense, scattered = 1.0, 1.0")],
        ["test_scratch_preflight_estimate_is_measured_and_not_below_actual_use"]),
}


def _suites():
    return {
        "c1": dict(target=TARGET, tests=TESTS, run_tests=[TESTS[0]], mutants={n: (w, p) for n, (w, p) in MUTANTS.items()},
                   intended=INTENDED, exact=False),
        "rehearsal": dict(target=REHEARSAL_TARGET, tests=REHEARSAL_TESTS, run_tests=REHEARSAL_TESTS,
                          mutants={n: (w, p) for n, (w, p, _i) in REHEARSAL_MUTANTS.items()},
                          intended={n: i for n, (_w, _p, i) in REHEARSAL_MUTANTS.items()}, exact=True),
    }


def run_pytest(tree: Path, tests=None, stop_first=True) -> subprocess.CompletedProcess:
    env = dict(os.environ, PYTHONPATH=f"{tree}:{tree / 'collector'}", PYTHONDONTWRITEBYTECODE="1")
    env.pop("DEDUP_DEEP_VERIFY", None)
    return subprocess.run([sys.executable, "-m", "pytest", *(["-x"] if stop_first else []), "-q", "-p", "no:cacheprovider",
                           *(tests or TESTS)],
                          cwd=tree / "collector", env=env, capture_output=True, text=True, timeout=900)


def _attributed(failing: list[str], intended: list[str], exact: bool) -> list[str]:
    if exact:
        return [f for f in failing if f.split("[", 1)[0] in intended]
    return [f for f in failing if any(f.startswith(i) or i in f for i in intended)]


def main(argv: list[str]) -> int:
    suites = _suites()
    chosen = list(suites)
    if "--suite" in argv:
        i = argv.index("--suite")
        chosen, argv = [argv[i + 1]], argv[:i] + argv[i + 2:]
        if chosen[0] not in suites:
            print(f"unknown suite {chosen[0]!r}; known: {sorted(suites)}")
            return 2
    owner = {n: s for s in suites for n in suites[s]["mutants"]}
    unknown = [n for n in argv if n not in owner]
    if unknown:
        print(f"unknown mutants: {unknown}\nknown: {sorted(owner)}")
        return 2
    plan = {s: [n for n in suites[s]["mutants"] if (not argv or n in argv)] for s in chosen}
    results = []
    with tempfile.TemporaryDirectory(prefix="c1_mut_") as tmp:
        tree = Path(tmp) / "tree"
        shutil.copytree(ROOT, tree, ignore=shutil.ignore_patterns(".git", "__pycache__", ".pytest_cache"))
        for s in chosen:
            if not plan[s]:
                continue
            suite = suites[s]
            base = run_pytest(tree, tests=suite["tests"])
            print(f"[{s}] baseline (unmutated): {'PASS' if base.returncode == 0 else 'FAIL'}")
            if base.returncode != 0:
                print(base.stdout[-3000:])
                return 1
            original = (tree / suite["target"]).read_text()
            for name in plan[s]:
                what, patches = suite["mutants"][name]
                text = original
                for anchor, replacement in patches:
                    if text.count(anchor) != 1:
                        print(f"ABORT: anchor for {name!r} found {text.count(anchor)} times (need exactly 1): {anchor[:70]!r}")
                        return 2
                    text = text.replace(anchor, replacement)
                (tree / suite["target"]).write_text(text)
                proc = run_pytest(tree, tests=suite["run_tests"], stop_first=False)      # every failure listed
                failing = [ln.split("::", 1)[1].split(" ")[0] for ln in proc.stdout.splitlines() if ln.startswith("FAILED")]
                hit = _attributed(failing, suite["intended"][name], suite["exact"])
                killed = bool(hit)                  # killed BY THE INTENDED test, not by any stray failure
                note = f"intended={hit[0]}" if hit else f"NOT killed by intended {suite['intended'][name]}; other failures: {failing[:3]}"
                results.append((name, killed, what, note))
                print(f"{'KILLED  ' if killed else 'SURVIVED'}  [{s}] {name:46s} {note[:70]}  (+{len(failing) - len(hit)} other failing)")
            (tree / suite["target"]).write_text(original)
    survivors = [r[0] for r in results if not r[1]]
    print(f"\n{len(results) - len(survivors)}/{len(results)} mutants killed" + (f"; SURVIVORS: {survivors}" if survivors else ""))
    return 1 if survivors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
