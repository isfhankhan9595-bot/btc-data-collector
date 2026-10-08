"""Dedup startup / rebuild timing at a chosen scale (reproducible; prints only what it measured).

Usage (repo root, PYTHONPATH=<repo>:<repo>/collector)::

    python collector/scripts/c1_dedup_bench.py --segments 1000 --rows 5000
    python collector/scripts/c1_dedup_bench.py --segments 10000 --rows 100 --keep /tmp/bench

Builds N confirmed segments (real parquet + real publication markers) in a scratch
directory, then times, with the public coordinator API only (so the same script also runs
against older revisions):

  cold_index      no index file at all  -> every segment read and indexed (= rebuild = legacy migration)
  normal_startup  valid index           -> the audit that runs on EVERY start
  deep_startup    DEDUP_DEEP_VERIFY=1   -> every segment re-read and re-derived

The filesystem guard is bypassed (COLLECTOR_ALLOW_UNVERIFIED_FS=1) because this measures CPU/IO of the
audit, not durability. Numbers depend on the machine; the script prints the platform.
"""
from __future__ import annotations

import argparse
import os
import platform
import shutil
import sys
import tempfile
import time
from pathlib import Path

os.environ["COLLECTOR_ALLOW_UNVERIFIED_FS"] = "1"
os.environ.pop("DEDUP_DEEP_VERIFY", None)

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from collector.collector import publication as pub  # noqa: E402
from collector.collector.segment_dedup import SegmentDedupCoordinator, SegmentDedupIndex, dedup_identity_key  # noqa: E402

SCHEMA = pa.schema([("timestamp", pa.int64()), ("instrument_key", pa.string()), ("trade_id", pa.string())])
INSTR = "BINANCE|linear_perpetual|BTC-USDT|BTCUSDT"


def row_identity(row):
    return None if row["trade_id"] is None else dedup_identity_key("BINANCE", "linear_perpetual", INSTR, "trades", row["trade_id"])


def build(stream_dir: Path, segments: int, rows: int) -> None:
    stream_dir.mkdir(parents=True, exist_ok=True)
    for s in range(segments):
        path = stream_dir / f"2026-01-01-00-{s:06d}.seg"
        ids = [f"{s}-{i}" for i in range(rows)]
        pq.write_table(pa.table({"timestamp": list(range(rows)), "instrument_key": [INSTR] * rows, "trade_id": ids},
                                schema=SCHEMA), path)
        sha, size = pub.sha256_file(path)
        pub.write_marker_atomic(path, record_count=rows, first_ts=0, last_ts=rows - 1, sha256=sha, size_bytes=size,
                                confirmed_by=pub.CONFIRMED_BY_WRITER, legacy=False, fsync_dir_after=False)


def du(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file()) if path.is_dir() else 0


def start(base: Path, stream_dir: Path):
    index = SegmentDedupIndex(str(base / "dedup.sqlite3"))
    co = SegmentDedupCoordinator(index, row_identity)
    t0 = time.perf_counter()
    co.startup_reconcile(stream_dir)
    dt = time.perf_counter() - t0
    return index, co, dt


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--segments", type=int, default=1000)
    ap.add_argument("--rows", type=int, default=5000)
    ap.add_argument("--keep", default=None, help="scratch dir (kept); default: temporary, removed")
    args = ap.parse_args()
    base = Path(args.keep) if args.keep else Path(tempfile.mkdtemp(prefix="c1_bench_"))
    shutil.rmtree(base, ignore_errors=True)
    stream_dir = base / "raw" / "bench_trades"
    print(f"platform={platform.platform()} python={platform.python_version()} pyarrow={pa.__version__}")
    t0 = time.perf_counter()
    build(stream_dir, args.segments, args.rows)
    total = args.segments * args.rows
    print(f"segments={args.segments} rows/segment={args.rows} identities={total} "
          f"segment_bytes={du(stream_dir)} (built in {time.perf_counter() - t0:.1f}s)")

    index, co, dt = start(base, stream_dir)
    rep = co.last_report
    print(f"cold_index      {dt:9.2f}s  indexed={rep.indexed} rebuilt={rep.rebuilt}")
    index.close()
    for label, env in (("normal_startup", None), ("normal_startup", None), ("deep_startup", "1")):
        if env:
            os.environ["DEDUP_DEEP_VERIFY"] = env
        index, co, dt = start(base, stream_dir)
        os.environ.pop("DEDUP_DEEP_VERIFY", None)
        rep = co.last_report
        print(f"{label:15s} {dt:9.2f}s  indexed={rep.indexed} rebuilt={rep.rebuilt} "
              f"regenerated={getattr(rep, 'evidence_regenerated', 'n/a')} identities={index.identity_count()}")
        index.close()
    idx_bytes = sum(os.path.getsize(base / n) for n in os.listdir(base) if n.startswith("dedup.sqlite3"))
    ev = base / "dedup.identity_evidence"
    print(f"index_bytes={idx_bytes} evidence_files_bytes={du(ev)} evidence_files={len(list(ev.glob('*'))) if ev.exists() else 0}")
    if not args.keep:
        shutil.rmtree(base, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
