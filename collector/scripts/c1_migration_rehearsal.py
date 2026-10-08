"""C1 migration rehearsal: a READ-ONLY census of a dataset's dedup identities.

Run it against a COPY of a production dataset. It answers, from the authoritative parquet
segment bytes, the questions that must be answered before the cross-segment duplicate policy
is decided: how many identities exist, how many repeat inside one segment, how many repeat across
segments (and where), how many segments are unreadable / unmarked / carry stale evidence, and how
much work a rebuild or migration would be.

It reports. It never decides, repairs or deduplicates:

* it does NOT choose which segment "wins" a cross-segment duplicate (segments are listed in
  lexicographic name order, which carries no meaning), quarantines nothing and deletes nothing;
* it does NOT call ``startup_reconcile`` / ``confirm_unmarked`` / ``_ensure_evidence`` /
  ``commit_segment`` / ``rebuild_reset`` or any marker/evidence writer (the startup path renames
  invalid markers, writes markers and writes evidence -- all forbidden here);
* the SQLite dedup index is only ever read from a private COPY in the scratch directory (opening a
  WAL database in place can create ``-shm``/``-wal`` files);
* the reference result comes from the segment bytes. The marker, the evidence files and the index
  are MEASURED and COMPARED against it, never trusted as the source of truth;
* it snapshots every input file (size + mtime) before and after and reports
  ``read_only_check.unchanged``; it refuses to write its own outputs or scratch inside the
  inputs it reads.

Usage (repo root; the script puts the repo root on ``sys.path`` itself)::

    python collector/scripts/c1_migration_rehearsal.py --preset binance-usdm \\
        --data-root /copy/of/prod/data --json-out /tmp/c1_report.json

    python collector/scripts/c1_migration_rehearsal.py --stream-dir /copy/data/raw/okx_trades \\
        --exchange OKX --market-type linear_perpetual --event-stream trades \\
        --detail-out /tmp/c1_dups.jsonl

Run it on a quiesced copy (no live writer, index not being modified). One run audits one stream.

Report layout: ``report`` is a pure function of the input bytes (deterministic; ``report_digest``
is the sha256 of its canonical JSON); ``run`` holds the non-deterministic measurements (elapsed
time, peak RSS, host). Identities are NOT dumped by default: the summary carries counters and a few
samples; every duplicate identity, with provenance, goes to ``--detail-out`` (JSON lines) only
when asked for.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import sqlite3
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:          # runnable from a clean checkout without PYTHONPATH
    sys.path.insert(0, str(_REPO_ROOT))

import pyarrow as pa  # noqa: E402

from collector.collector import publication as pub  # noqa: E402
from collector.collector import segment_dedup as sd  # noqa: E402
from collector.collector.storage_layout import parse_segment_name  # noqa: E402

REPORT_SCHEMA = "c1_migration_rehearsal/1"

#: Production wiring of each trade stream (run_*.py ``ParquetWriter`` + ``StreamSpec``), so an operator
#: cannot silently derive the wrong identity (e.g. Binance USD-M keys on ``native_trade_id``).
PRESETS: Dict[str, Dict[str, str]] = {
    "binance-usdm": dict(stream="binance_trades_raw", exchange="BINANCE", market_type="linear_perpetual",
                         event_stream="trades", trade_id_field="native_trade_id"),
    "bybit": dict(stream="bybit_trades", exchange="BYBIT", market_type="linear_perpetual",
                  event_stream="trades", trade_id_field="trade_id"),
    "okx-trades": dict(stream="okx_trades", exchange="OKX", market_type="linear_perpetual",
                       event_stream="trades", trade_id_field="trade_id"),
    "okx-trades-all": dict(stream="okx_trades_all", exchange="OKX", market_type="linear_perpetual",
                           event_stream="trades-all", trade_id_field="trade_id"),
    "binance-spot": dict(stream="spot_trades", exchange="BINANCE", market_type="spot",
                         event_stream="spot_trades", trade_id_field="trade_id"),
}

_HIST_CAP = 10        # segments-per-identity histogram: exact up to this, then "11+"


class Tally:
    """A counter that remembers the first few examples (processing order is sorted => deterministic)."""

    __slots__ = ("count", "examples", "_limit")

    def __init__(self, limit: int) -> None:
        self.count, self.examples, self._limit = 0, [], limit

    def add(self, example: str) -> None:
        self.count += 1
        if len(self.examples) < self._limit:
            self.examples.append(example)

    def to_json(self) -> Dict[str, Any]:
        return {"count": self.count, "examples": list(self.examples)}


class SegInfo:
    __slots__ = ("ordinal", "key", "name", "size", "rows", "rows_with_identity", "distinct", "intra_identities",
                 "intra_extra_rows", "cross_identities", "cross_rows")

    def __init__(self, ordinal: int, key: str, name: str, size: int) -> None:
        self.ordinal, self.key, self.name, self.size = ordinal, key, name, size
        self.rows = self.rows_with_identity = self.distinct = 0
        self.intra_identities = self.intra_extra_rows = self.cross_identities = self.cross_rows = 0

    def affected(self) -> bool:
        return bool(self.intra_identities or self.cross_identities)

    def to_json(self) -> Dict[str, Any]:
        return {"segment": self.key, "rows": self.rows, "distinct_identities": self.distinct,
                "intra_duplicate_identities": self.intra_identities, "intra_duplicate_extra_rows": self.intra_extra_rows,
                "cross_duplicate_identities": self.cross_identities, "cross_duplicate_rows": self.cross_rows}


def decode_identity_key(key: str) -> Optional[List[str]]:
    """Inverse of ``dedup_identity_key`` (length-prefixed parts); None if the key is not in that form."""
    parts, i = [], 0
    try:
        while i < len(key):
            j = key.index(":", i)
            n = int(key[i:j])
            parts.append(key[j + 1:j + 1 + n])
            i = j + 1 + n
    except ValueError:
        return None
    return parts if i == len(key) else None


def _trade_id_of(key: str) -> Optional[str]:
    parts = decode_identity_key(key)
    return parts[-1] if parts else None


def _canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _peak_rss_bytes() -> Optional[int]:
    try:
        import resource
    except ImportError:                                       # not POSIX: no reliable figure
        return None
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if sys.platform == "darwin" else peak * 1024)


def _snapshot(roots: Iterable[Path]) -> Dict[str, Tuple[int, int]]:
    snap: Dict[str, Tuple[int, int]] = {}
    for root in roots:
        if root.is_file():
            st = root.stat()
            snap[str(root)] = (st.st_size, st.st_mtime_ns)
        elif root.is_dir():
            for dirpath, dirnames, filenames in os.walk(root):
                dirnames.sort()
                for fname in sorted(filenames):
                    p = Path(dirpath) / fname
                    try:
                        st = p.stat()
                    except OSError:
                        continue
                    snap[str(p)] = (st.st_size, st.st_mtime_ns)
    return snap


def _inside(path: Path, roots: Iterable[Path]) -> bool:
    p = path.resolve()
    for r in roots:
        rr = r.resolve()
        if p == rr or rr in p.parents:
            return True
    return False


def _evidence_file(evidence_dir: Path, segment_key: str) -> Path:
    return evidence_dir / (segment_key.replace("/", "%2F") + sd.EVIDENCE_SUFFIX)


def classify_evidence(evidence_dir: Path, segment_key: str, sha: str, size: int,
                      derived: Tuple[int, str]) -> str:
    """Read-only classification of one identity-evidence file against the bytes-derived identity set.

    ``ok`` | ``missing`` | ``invalid`` (unreadable / not JSON / wrong shape or fields) |
    ``stale`` (well formed but bound to other segment bytes or another identity-key encoding) |
    ``mismatch`` (bound to THESE bytes, but its count/digest differ from what the bytes yield: forged/wrong)."""
    try:
        raw = _evidence_file(evidence_dir, segment_key).read_bytes()
    except FileNotFoundError:
        return "missing"
    except OSError:
        return "invalid"
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return "invalid"
    if not isinstance(obj, dict):
        return "invalid"
    count, digest = obj.get("identity_count"), obj.get("identity_digest")
    shape_ok = (obj.get("schema") == sd.EVIDENCE_SCHEMA and obj.get("segment_key") == segment_key
                and isinstance(obj.get("segment_sha256"), str) and isinstance(obj.get("segment_size"), int)
                and not isinstance(obj.get("segment_size"), bool) and isinstance(obj.get("identity_encoding"), str)
                and isinstance(count, int) and not isinstance(count, bool) and count >= 0
                and isinstance(digest, str) and len(digest) == 64)
    if not shape_ok:
        return "invalid"
    if (obj["segment_sha256"] != sha or obj["segment_size"] != size
            or obj["identity_encoding"] != sd.identity_encoding_fingerprint()):
        return "stale"
    return "ok" if (count, digest) == derived else "mismatch"


class Rehearsal:
    def __init__(self, stream_dir: Path, *, exchange: str, market_type: str, event_stream: str,
                 trade_id_field: str, index_path: Optional[Path], evidence_dir: Optional[Path],
                 sample_limit: int, top_segments: int, example_limit: int, detail_out: Optional[Path],
                 scratch_dir: Optional[Path]) -> None:
        self.stream_dir = Path(stream_dir)
        self.stream_name = self.stream_dir.name
        self.spec = sd.StreamSpec(event_stream, None, exchange, market_type, trade_id_field=trade_id_field)
        self.ident = dict(exchange=exchange, market_type=market_type, event_stream=event_stream,
                          trade_id_field=trade_id_field)
        self.index_path = Path(index_path) if index_path else None
        if evidence_dir is not None:
            self.evidence_dir: Optional[Path] = Path(evidence_dir)
        elif self.index_path is not None:
            self.evidence_dir = sd.SegmentDedupCoordinator.default_evidence_dir(SimpleNamespace(path=str(self.index_path)))
        else:
            self.evidence_dir = None
        self.sample_limit, self.top_segments, self.example_limit = sample_limit, top_segments, example_limit
        self.detail_out = Path(detail_out) if detail_out else None
        self.scratch_parent = Path(scratch_dir) if scratch_dir else None
        ex = example_limit
        self.malformed, self.derivation_failed = Tally(ex), Tally(ex)
        self.missing_trade_id_col = Tally(ex)
        self.marker_tallies = {k: Tally(ex) for k in (
            "valid", "absent", "absent_v1_hint", "invalid", "size_mismatch", "sha256_mismatch", "record_count_mismatch")}
        self.marker_tallies["valid"] = Tally(0)
        self.evidence_tallies = {k: Tally(ex) for k in ("ok", "missing", "invalid", "stale", "mismatch", "not_evaluated")}
        self.segs: List[SegInfo] = []
        self.derived: Dict[str, Tuple[str, int, int, str]] = {}     # key -> (sha, size, identity count, digest)
        self.empty_segments = 0
        self.warnings: List[str] = []

    # ------------------------------------------------------------------ per segment
    def _inspect(self, path: Path, ordinal: int, conn: sqlite3.Connection) -> None:
        key = f"{self.stream_name}/{path.name}"
        marker = pub.read_marker(path)
        try:
            data = path.read_bytes()
        except OSError as exc:
            info = SegInfo(ordinal, key, path.name, 0)
            self.segs.append(info)
            self.malformed.add(f"{key}: unreadable file: {type(exc).__name__}")
            self.evidence_tallies["not_evaluated"].add(key)
            self._tally_marker(marker, key, None, None, None)
            return
        size, sha = len(data), pub.sha256_bytes(data)
        info = SegInfo(ordinal, key, path.name, size)
        self.segs.append(info)
        table = None
        try:
            table = pub.read_segment_table(data, path.name)
        except pub.PublicationError as exc:
            self.malformed.add(f"{key}: {str(exc)[:160]}")
        del data
        self._tally_marker(marker, key, size, sha, table.num_rows if table is not None else None)
        if table is None:
            self.evidence_tallies["not_evaluated"].add(key)
            return
        info.rows = table.num_rows
        if info.rows == 0:
            self.empty_segments += 1
        keys: List[str] = []
        if self.spec.trade_id_field not in table.column_names:
            self.missing_trade_id_col.add(key)
        else:
            cols = [c for c in ("instrument_key", self.spec.trade_id_field) if c in table.column_names]
            try:
                for row in table.select(cols).to_pylist():
                    k = self.spec.row_identity(row)
                    if k is not None:
                        keys.append(k)
                counts = Counter(keys)
                derived = sd.identity_set_digest(counts.keys())
                conn.execute("BEGIN")
                try:
                    conn.executemany("INSERT INTO occ (identity_key, seg, n) VALUES (?,?,?)",
                                     ((k, ordinal, n) for k, n in counts.items()))
                    conn.execute("COMMIT")
                except BaseException:
                    conn.execute("ROLLBACK")
                    raise
            except Exception as exc:  # noqa: BLE001 - production would raise here; we report and carry on
                self.derivation_failed.add(f"{key}: {type(exc).__name__}: {str(exc)[:120]}")
                self.evidence_tallies["not_evaluated"].add(key)
                return
            info.rows_with_identity = len(keys)
            info.distinct = len(counts)
            info.intra_identities = sum(1 for n in counts.values() if n > 1)
            info.intra_extra_rows = sum(n - 1 for n in counts.values() if n > 1)
            self.derived[key] = (sha, size, derived[0], derived[1])
        if key not in self.derived:                       # no trade-id column => zero identities, still evidence-checkable
            digest = sd.identity_set_digest([])
            self.derived[key] = (sha, size, digest[0], digest[1])
        self._tally_evidence(key, sha, size)

    def _tally_marker(self, marker: pub.MarkerResult, key: str, size: Optional[int], sha: Optional[str],
                      decoded_rows: Optional[int]) -> None:
        t = self.marker_tallies
        if marker.status is pub.MarkerStatus.ABSENT:
            (t["absent_v1_hint"] if marker.v1 else t["absent"]).add(key)
        elif marker.status is pub.MarkerStatus.INVALID:
            t["invalid"].add(f"{key}: {marker.reason[:100]}")
        else:
            t["valid"].add(key)
            p = marker.publication
            if size is not None and size != p["size_bytes"]:
                t["size_mismatch"].add(key)
            elif sha is not None and sha != p["sha256"]:
                t["sha256_mismatch"].add(key)
            if decoded_rows is not None and decoded_rows != marker.data["record_count"]:
                t["record_count_mismatch"].add(key)

    def _tally_evidence(self, key: str, sha: str, size: int) -> None:
        if self.evidence_dir is None:
            self.evidence_tallies["not_evaluated"].add(key)
            return
        _, _, count, digest = self.derived[key]
        self.evidence_tallies[classify_evidence(self.evidence_dir, key, sha, size, (count, digest))].add(key)

    # ------------------------------------------------------------------ index (read from a private copy)
    def _index_section(self, scratch: Path, all_keys: List[str]) -> Dict[str, Any]:
        cur_version = sd.INDEX_SCHEMA_VERSION
        if self.index_path is None:
            return {"checked": False, "current_schema_version": cur_version, "reason": "no index path"}
        if not self.index_path.is_file():
            return {"checked": True, "present": False, "current_schema_version": cur_version}
        copy_dir = scratch / "index_copy"
        copy_dir.mkdir()
        copy = copy_dir / self.index_path.name
        shutil.copy2(self.index_path, copy)
        wal = Path(str(self.index_path) + "-wal")
        if wal.is_file():
            shutil.copy2(wal, Path(str(copy) + "-wal"))
        out: Dict[str, Any] = {"checked": True, "present": True, "current_schema_version": cur_version}
        try:
            idx = sd.SegmentDedupIndex(str(copy))
        except sd.DedupStateError as exc:
            out.update(readable=False, error=str(exc)[:200])
            return out
        try:
            out["readable"] = True
            out["user_version"] = idx.user_version()
            problem = idx.schema_problem()
            out["schema_problem"] = problem
            out["needs_rebuild_for_schema"] = problem is not None
            try:
                out["identity_rows"] = idx.identity_count()
            except sd.DedupStateError:
                out["identity_rows"] = None
            if problem is not None:
                out["reconciled_segments"] = None
                return out
            rows = idx.reconciled_rows()
            out["reconciled_segments"] = len(rows)
            membership, _dups = idx.membership_digests()
            fingerprint = sd.identity_encoding_fingerprint()
            ex = self.example_limit
            not_indexed, gone = Tally(ex), Tally(ex)
            evid, ident, memb, enc = Tally(ex), Tally(ex), Tally(ex), Tally(ex)
            on_disk = set(all_keys)
            for key in sorted(self.derived):
                if key not in rows:
                    not_indexed.add(key)
            for key in sorted(rows):
                row = rows[key]
                if key not in on_disk:
                    gone.add(key)
                    continue
                if key not in self.derived:
                    continue
                sha, size, count, digest = self.derived[key]
                if (row.sha256, row.size) != (sha, size):
                    evid.add(key)
                if row.identity_encoding != fingerprint:
                    enc.add(key)
                if (row.identity_count, row.identity_digest) != (count, digest):
                    ident.add(key)
                held = membership.get(row.segment_id)
                if held is None:
                    if count != 0:
                        memb.add(key)
                elif held != (count, digest):
                    memb.add(key)
            known = {r.segment_id for r in rows.values()}
            out["comparison"] = {
                "segments_on_disk_not_indexed": not_indexed.to_json(),
                "indexed_segments_missing_on_disk": gone.to_json(),
                "indexed_evidence_differs_from_bytes": evid.to_json(),
                "indexed_identity_set_differs_from_bytes": ident.to_json(),
                "indexed_membership_differs_from_bytes": memb.to_json(),
                "indexed_encoding_differs_from_current": enc.to_json(),
                "identity_rows_owned_by_unknown_segment": sum(n for sid, (n, _h) in membership.items() if sid not in known),
            }
        finally:
            idx.close()
        return out

    # ------------------------------------------------------------------ whole run
    def run(self) -> Dict[str, Any]:
        t_start = time.perf_counter()
        protected = [self.stream_dir] + ([self.evidence_dir] if self.evidence_dir else []) + (
            [self.index_path.parent] if self.index_path else [])
        for label, p in (("--detail-out", self.detail_out), ("--scratch-dir", self.scratch_parent)):
            if p is not None and _inside(p, protected):
                raise SystemExit(f"{label} {p} is inside an input directory; refusing to write there")
        if not self.stream_dir.is_dir():
            raise SystemExit(f"stream directory {self.stream_dir} does not exist")
        watch = [self.stream_dir] + ([self.evidence_dir] if self.evidence_dir else [])
        if self.index_path:
            watch += [self.index_path] + [Path(str(self.index_path) + s) for s in ("-wal", "-shm")]
        before = _snapshot(watch)

        segs = sorted(self.stream_dir.glob("*.seg"))
        present = {p.name for p in segs}
        extras = {
            "legacy_hourly_parquet_files_not_audited": sum(1 for _ in self.stream_dir.glob("*.parquet")),
            "dangling_markers": sum(1 for m in self.stream_dir.glob("*.seg.meta.json")
                                    if m.name[: -len(".meta.json")] not in present),
            "preserved_invalid_or_orphan_markers": sum(1 for _ in self.stream_dir.glob("*.meta.json.invalid.*"))
                                                   + sum(1 for _ in self.stream_dir.glob("*.meta.json.orphan.*")),
            "tmp_files": sum(1 for _ in self.stream_dir.glob("*.seg.tmp")) + sum(1 for _ in self.stream_dir.glob("*.meta.json.tmp")),
        }
        t_discover = time.perf_counter()

        scratch = Path(tempfile.mkdtemp(prefix="c1_rehearsal_", dir=str(self.scratch_parent) if self.scratch_parent else None))
        try:
            conn = sqlite3.connect(str(scratch / "occ.sqlite3"), isolation_level=None)
            conn.execute("PRAGMA journal_mode=OFF")
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute("CREATE TABLE occ (identity_key TEXT NOT NULL, seg INTEGER NOT NULL, n INTEGER NOT NULL, "
                         "PRIMARY KEY (identity_key, seg)) WITHOUT ROWID")
            for ordinal, path in enumerate(segs):
                self._inspect(path, ordinal, conn)
            t_read = time.perf_counter()
            agg = self._aggregate(conn)
            t_agg = time.perf_counter()
            conn.close()
            index = self._index_section(scratch, [s.key for s in self.segs])
            t_index = time.perf_counter()
        finally:
            shutil.rmtree(scratch, ignore_errors=True)

        evidence_section = self._evidence_section()
        after = _snapshot(watch)
        changed = sorted(p for p in set(before) | set(after) if before.get(p) != after.get(p))
        report = self._build_report(agg, index, evidence_section, extras, len(before), changed)
        total = time.perf_counter() - t_start
        read_s = t_read - t_discover
        total_rows = sum(s.rows for s in self.segs)
        total_bytes = sum(s.size for s in self.segs)
        run = {
            "elapsed_seconds": round(total, 3),
            "phase_seconds": {"discover": round(t_discover - t_start, 3), "read_and_derive": round(read_s, 3),
                              "aggregate": round(t_agg - t_read, 3), "index_compare": round(t_index - t_agg, 3)},
            "read_throughput": {"rows_per_second": round(total_rows / read_s, 1) if read_s > 0 else None,
                                "megabytes_per_second": round(total_bytes / 1e6 / read_s, 2) if read_s > 0 else None},
            "peak_rss_bytes": _peak_rss_bytes(),
            "peak_rss_scope": "process lifetime (ru_maxrss)",
            "started_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "host": {"platform": platform.platform(), "python": platform.python_version(), "pyarrow": pa.__version__},
            "inputs": {"stream_dir": str(self.stream_dir), "index": str(self.index_path) if self.index_path else None,
                       "evidence_dir": str(self.evidence_dir) if self.evidence_dir else None},
            "detail_out": {"path": str(self.detail_out), "lines": agg["detail_lines"]} if self.detail_out else None,
        }
        return {"schema": REPORT_SCHEMA, "report": report, "report_digest": hashlib.sha256(
            _canonical(report).encode("utf-8")).hexdigest(), "run": run}

    # ------------------------------------------------------------------ aggregation pass over the scratch table
    def _aggregate(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        by_ord = self.segs
        a: Dict[str, Any] = dict(distinct=0, cross_ids=0, cross_rows=0, cross_extra=0, intra_ids=0, intra_extra=0,
                                 overlap=0, hist=Counter(), dist=Counter(), cal=Counter(), cross_samples=[],
                                 intra_samples=[], detail_lines=0)
        detail = None
        if self.detail_out is not None:
            self.detail_out.parent.mkdir(parents=True, exist_ok=True)
            detail = open(self.detail_out, "w", encoding="utf-8")

        def emit(obj: Dict[str, Any]) -> None:
            detail.write(_canonical(obj) + "\n")        # type: ignore[union-attr]
            a["detail_lines"] += 1

        def flush(key: str, group: List[Tuple[int, int]]) -> None:
            a["distinct"] += 1
            total = sum(n for _s, n in group)
            multi_seg = len(group) > 1
            intra = [(s, n) for s, n in group if n > 1]
            if multi_seg:
                a["cross_ids"] += 1
                a["cross_rows"] += total
                a["cross_extra"] += total - 1
                a["hist"][len(group) if len(group) <= _HIST_CAP else _HIST_CAP + 1] += 1
                for s, n in group:
                    by_ord[s].cross_identities += 1
                    by_ord[s].cross_rows += n
                lo, hi = group[0][0], group[-1][0]
                d = hi - lo
                a["dist"]["adjacent" if d == 1 else "2-10" if d <= 10 else "11-100" if d <= 100 else ">100"] += 1
                a["cal"][self._calendar_bucket(by_ord[lo].name, by_ord[hi].name)] += 1
                rec = {"class": "cross_segment", "identity": key, "trade_id": _trade_id_of(key), "occurrences": total,
                       "segments": [{"segment": by_ord[s].key, "count": n} for s, n in group]}
                if len(a["cross_samples"]) < self.sample_limit:
                    a["cross_samples"].append(rec)
                if detail is not None:
                    emit(rec)
            if intra:
                a["intra_ids"] += 1
                a["intra_extra"] += sum(n - 1 for _s, n in intra)
                if multi_seg:
                    a["overlap"] += 1
                rec = {"class": "intra_segment", "identity": key, "trade_id": _trade_id_of(key),
                       "occurrences": sum(n for _s, n in intra),
                       "segments": [{"segment": by_ord[s].key, "count": n} for s, n in intra]}
                if len(a["intra_samples"]) < self.sample_limit:
                    a["intra_samples"].append(rec)
                if detail is not None:
                    emit(rec)

        try:
            prev: Optional[str] = None
            group: List[Tuple[int, int]] = []
            for key, seg, n in conn.execute("SELECT identity_key, seg, n FROM occ ORDER BY identity_key, seg"):
                if key != prev:
                    if prev is not None:
                        flush(prev, group)
                    prev, group = key, []
                group.append((seg, n))
            if prev is not None:
                flush(prev, group)
            if detail is not None:
                for s in sorted(self.segs, key=lambda s: s.key):
                    emit({"class": "segment", **s.to_json()})
        finally:
            if detail is not None:
                detail.close()
        return a

    @staticmethod
    def _calendar_bucket(lo_name: str, hi_name: str) -> str:
        lo, hi = parse_segment_name(lo_name), parse_segment_name(hi_name)
        if lo is None or hi is None:
            return "unparsed_name"
        if (lo[0], lo[1]) == (hi[0], hi[1]):
            return "same_hour"
        return "same_date_different_hour" if lo[0] == hi[0] else "different_date"

    # ------------------------------------------------------------------ report assembly
    def _evidence_section(self) -> Dict[str, Any]:
        if self.evidence_dir is None:
            return {"checked": False, "reason": "no evidence directory (no index path given)"}
        present = self.evidence_dir.is_dir()
        expected = {_evidence_file(self.evidence_dir, s.key).name for s in self.segs}
        orphans = Tally(self.example_limit)
        if present:
            for p in sorted(self.evidence_dir.glob("*" + sd.EVIDENCE_SUFFIX)):
                if p.name not in expected:
                    orphans.add(p.name)
        return {"checked": True, "directory_present": present,
                "per_segment": {k: t.to_json() for k, t in sorted(self.evidence_tallies.items())},
                "orphan_evidence_files": orphans.to_json()}

    def _build_report(self, a: Dict[str, Any], index: Dict[str, Any], evidence: Dict[str, Any],
                      extras: Dict[str, Any], files_snapshotted: int, changed: List[str]) -> Dict[str, Any]:
        segs = self.segs
        rows_total = sum(s.rows for s in segs)
        rows_with_id = sum(s.rows_with_identity for s in segs)
        affected = [s for s in segs if s.affected()]
        affected.sort(key=lambda s: (-s.cross_identities, -s.intra_identities, s.key))
        cross_segs = sum(1 for s in segs if s.cross_identities)
        intra_segs = sum(1 for s in segs if s.intra_identities)
        hist = {(str(k) if k <= _HIST_CAP else f"{_HIST_CAP + 1}+"): v for k, v in sorted(a["hist"].items())}
        if rows_total and not rows_with_id and not self.missing_trade_id_col.count:
            self.warnings.append("rows were read but no identity could be derived: check --trade-id-field / --preset")
        if self.missing_trade_id_col.count:
            self.warnings.append(f"{self.missing_trade_id_col.count} segment(s) lack the trade-id column "
                                 f"{self.spec.trade_id_field!r}: check --trade-id-field / --preset")
        if evidence.get("checked") and not evidence.get("directory_present"):
            self.warnings.append("identity-evidence directory not found: every segment reports evidence 'missing' "
                                 "(expected for a pre-C1 dataset)")
        if index.get("checked") and not index.get("present"):
            self.warnings.append("dedup index file not found: a startup would build it from scratch")
        readable = [s for s in segs if s.key in self.derived]
        read_bytes = sum(s.size for s in readable)
        schema_stale = bool(index.get("needs_rebuild_for_schema")) or index.get("present") is False \
            or index.get("readable") is False
        cmp_ = index.get("comparison") or {}
        diverged = [k for k in ("indexed_evidence_differs_from_bytes", "indexed_identity_set_differs_from_bytes",
                                "indexed_membership_differs_from_bytes", "indexed_encoding_differs_from_current",
                                "indexed_segments_missing_on_disk") if cmp_.get(k, {}).get("count")]
        if cmp_.get("identity_rows_owned_by_unknown_segment"):
            diverged.append("identity_rows_owned_by_unknown_segment")
        rebuild_reasons = (["index missing, unreadable or older/other schema"] if schema_stale else []) + diverged
        markers_to_write = sum(t.count for k, t in self.marker_tallies.items() if k in ("absent", "absent_v1_hint", "invalid"))
        not_indexed = (cmp_.get("segments_on_disk_not_indexed") or {}).get("count") if cmp_ else None
        evid_bad = sum(t.count for k, t in self.evidence_tallies.items() if k in ("missing", "invalid", "stale", "mismatch"))
        mt = self.marker_tallies
        return {
            "stream": self.stream_name,
            "identity_spec": self.ident,
            "segments": {
                "total_seg_files": len(segs), "readable": len(readable), "total_bytes": sum(s.size for s in segs),
                "empty": self.empty_segments, "malformed_or_unreadable": self.malformed.to_json(),
                "identity_derivation_failed": self.derivation_failed.to_json(),
                "missing_trade_id_column": self.missing_trade_id_col.to_json(),
                "other_files": extras,
            },
            "rows": {"physical_total": rows_total, "with_identity": rows_with_id,
                     "without_identity": rows_total - rows_with_id},
            "identities": {"distinct_total": a["distinct"],
                           "sum_of_per_segment_distinct": sum(s.distinct for s in segs)},
            "duplicates": {
                "note": ("reported only; no policy applied. Segments are listed in lexicographic name order, which "
                         "carries no winner/loser meaning"),
                "intra_segment": {"identities": a["intra_ids"], "extra_rows": a["intra_extra"],
                                  "segments_affected": intra_segs, "samples": a["intra_samples"]},
                "cross_segment": {"identities": a["cross_ids"], "rows_total": a["cross_rows"],
                                  "extra_rows_beyond_one_per_identity": a["cross_extra"],
                                  "segments_affected": cross_segs,
                                  "segments_per_identity_histogram": hist,
                                  "ordinal_distance_histogram": dict(sorted(a["dist"].items())),
                                  "calendar_span_histogram": dict(sorted(a["cal"].items())),
                                  "samples": a["cross_samples"]},
                "identities_both_intra_and_cross": a["overlap"],
            },
            "affected_segments": {"total": len(affected), "shown": min(len(affected), self.top_segments),
                                  "sorted_by": "cross_duplicate_identities desc, intra_duplicate_identities desc, name",
                                  "segments": [s.to_json() for s in affected[: self.top_segments]]},
            "markers": {"valid": mt["valid"].count, "absent": mt["absent"].to_json(),
                        "absent_with_v1_hint": mt["absent_v1_hint"].to_json(), "invalid": mt["invalid"].to_json(),
                        "size_mismatch": mt["size_mismatch"].to_json(), "sha256_mismatch": mt["sha256_mismatch"].to_json(),
                        "record_count_mismatch": mt["record_count_mismatch"].to_json()},
            "identity_evidence": evidence,
            "dedup_index": index,
            "fail_closed_conditions": {
                "note": ("conditions under which current code raises when the segment is read/indexed (a rebuild or "
                         "migration reads every segment); a normal startup of an already-indexed segment does not "
                         "re-read its bytes unless DEDUP_DEEP_VERIFY=1 or its evidence file is not ok"),
                "cross_segment_duplicate_identities": a["cross_ids"],
                "valid_marker_but_size_differs": mt["size_mismatch"].count,
                "valid_marker_but_sha256_differs": mt["sha256_mismatch"].count,
                "valid_marker_but_record_count_differs": mt["record_count_mismatch"].count,
                "unreadable_parquet": self.malformed.count,
                "identity_derivation_failed": self.derivation_failed.count},
            "estimated_work": {
                "assumption": "per segment: read bytes, decode parquet, derive identities (what a rebuild does)",
                "full_rebuild": {"segments": len(readable), "bytes_read": read_bytes, "rows_decoded": rows_total,
                                 "identity_rows_to_insert": sum(s.distinct for s in readable)},
                "index_rebuild_expected": bool(rebuild_reasons), "index_rebuild_reasons": rebuild_reasons,
                "segments_to_index_incrementally": (len(readable) if (not_indexed is None) else not_indexed),
                "markers_to_write_by_startup_refsync": markers_to_write,
                "evidence_files_to_write": evid_bad,
                "measured_rate": "see run.read_throughput (this run performed an equivalent read of every segment)"},
            "warnings": self.warnings,
            "read_only_check": {"files_snapshotted": files_snapshotted, "unchanged": not changed,
                                "changed_examples": changed[:10]},
        }


def format_summary(result: Dict[str, Any]) -> str:
    r, run = result["report"], result["run"]
    seg, ci, ca = r["segments"], r["duplicates"]["cross_segment"], r["duplicates"]["intra_segment"]
    ev = r["identity_evidence"]
    idx = r["dedup_index"]
    lines = [
        f"C1 migration rehearsal  stream={r['stream']}  digest={result['report_digest'][:16]}",
        f"  segments: {seg['total_seg_files']} files, {seg['readable']} readable, {seg['malformed_or_unreadable']['count']} malformed, "
        f"{seg['empty']} empty, {seg['total_bytes']} bytes",
        f"  rows: {r['rows']['physical_total']} physical, {r['rows']['without_identity']} without identity; "
        f"distinct identities: {r['identities']['distinct_total']}",
        f"  intra-segment duplicates: {ca['identities']} identities ({ca['extra_rows']} extra rows) in {ca['segments_affected']} segments",
        f"  CROSS-segment duplicates: {ci['identities']} identities ({ci['extra_rows_beyond_one_per_identity']} extra rows) "
        f"in {ci['segments_affected']} segments",
        f"  markers: valid={r['markers']['valid']} absent={r['markers']['absent']['count']} "
        f"v1-hint={r['markers']['absent_with_v1_hint']['count']} invalid={r['markers']['invalid']['count']} "
        f"size-mismatch={r['markers']['size_mismatch']['count']} sha-mismatch={r['markers']['sha256_mismatch']['count']}",
    ]
    if ev.get("checked"):
        e = ev["per_segment"]
        lines.append("  evidence: " + " ".join(f"{k}={e[k]['count']}" for k in ("ok", "missing", "invalid", "stale", "mismatch"))
                     + f" orphans={ev['orphan_evidence_files']['count']}")
    if idx.get("present"):
        lines.append(f"  index: user_version={idx.get('user_version')} (current {idx['current_schema_version']}) "
                     f"schema_problem={idx.get('schema_problem')!r}")
    elif idx.get("checked"):
        lines.append("  index: not found")
    w = r["estimated_work"]
    lines.append(f"  rebuild work: {w['full_rebuild']['segments']} segments, {w['full_rebuild']['bytes_read']} bytes, "
                 f"{w['full_rebuild']['rows_decoded']} rows; index rebuild expected={w['index_rebuild_expected']}")
    peak = run["peak_rss_bytes"]
    lines.append(f"  elapsed {run['elapsed_seconds']}s, peak RSS {'n/a' if peak is None else f'{peak / 2**20:.0f} MiB'}, "
                 f"source files unchanged={r['read_only_check']['unchanged']}")
    lines += [f"  WARNING: {m}" for m in r["warnings"]]
    return "\n".join(lines)


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="READ-ONLY C1 dedup-identity census of a COPY of a dataset.")
    ap.add_argument("--preset", choices=sorted(PRESETS), help="production wiring of a trade stream")
    ap.add_argument("--data-root", help="dataset root containing raw/ and dedup_state/ (with --preset or --stream)")
    ap.add_argument("--stream", help="stream directory name under <data-root>/raw")
    ap.add_argument("--stream-dir", help="stream directory holding the *.seg files (alternative to --data-root)")
    ap.add_argument("--exchange")
    ap.add_argument("--market-type")
    ap.add_argument("--event-stream")
    ap.add_argument("--trade-id-field")
    ap.add_argument("--index", help="dedup index (default: <data-root>/dedup_state/<stream>.sqlite3)")
    ap.add_argument("--no-index", action="store_true", help="skip index and evidence comparison")
    ap.add_argument("--evidence-dir", help="identity-evidence directory (default: beside the index)")
    ap.add_argument("--json-out", help="write the full JSON result here")
    ap.add_argument("--detail-out", help="write EVERY duplicate identity (+ per-segment table) as JSON lines here")
    ap.add_argument("--sample-limit", type=int, default=10)
    ap.add_argument("--top-segments", type=int, default=25)
    ap.add_argument("--example-limit", type=int, default=5)
    ap.add_argument("--scratch-dir", help="parent for the temporary scratch (default: system temp)")
    ap.add_argument("--json", action="store_true", help="print the JSON result instead of the text summary")
    return ap


def main(argv: Optional[List[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    preset = PRESETS.get(args.preset or "", {})
    stream = args.stream or preset.get("stream")
    if args.stream_dir:
        stream_dir = Path(args.stream_dir)
    elif args.data_root and stream:
        stream_dir = Path(args.data_root) / "raw" / stream
    else:
        raise SystemExit("give --stream-dir, or --data-root with --stream/--preset")
    spec = {k: getattr(args, k) or preset.get(k) for k in ("exchange", "market_type", "event_stream", "trade_id_field")}
    spec["event_stream"] = spec["event_stream"] or "trades"
    spec["trade_id_field"] = spec["trade_id_field"] or "trade_id"
    if not spec["exchange"] or not spec["market_type"]:
        raise SystemExit("--exchange and --market-type are required (or use --preset)")
    index_path: Optional[Path] = None
    if not args.no_index:
        if args.index:
            index_path = Path(args.index)
        elif args.data_root:
            index_path = Path(args.data_root) / "dedup_state" / f"{stream_dir.name}.sqlite3"
        elif stream_dir.parent.name == "raw":
            index_path = stream_dir.parent.parent / "dedup_state" / f"{stream_dir.name}.sqlite3"
    out = Path(args.json_out) if args.json_out else None
    evidence_dir = Path(args.evidence_dir) if args.evidence_dir else None
    if out is not None and _inside(out, [stream_dir] + ([index_path.parent] if index_path else [])
                                   + ([evidence_dir] if evidence_dir else [])):
        raise SystemExit(f"--json-out {out} is inside an input directory; refusing to write there")
    result = Rehearsal(
        stream_dir, index_path=index_path, evidence_dir=evidence_dir,
        sample_limit=args.sample_limit, top_segments=args.top_segments, example_limit=args.example_limit,
        detail_out=Path(args.detail_out) if args.detail_out else None,
        scratch_dir=Path(args.scratch_dir) if args.scratch_dir else None, **spec).run()
    if out is not None:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True, indent=2) if args.json else format_summary(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
