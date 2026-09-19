"""Deterministic replay CLI.

Replays recorded raw wire data through the production reconstruction path
and reports the resulting book, quality transitions and a digest.

``--verify-determinism`` runs the same input twice and compares digests,
which is the check that actually proves replay is reproducible.

Replay never contacts the exchange. If the recorded data is insufficient,
that is reported rather than filled in.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from collector.collector.replay import ReplayEngine, ReplaySource
from collector.collector.storage_layout import StorageCollisionError


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("date", nargs="?", default=None, help="UTC date, YYYY-MM-DD")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--venue", default="BINANCE", type=str.upper,
                        help="Venue whose recorded stream to replay (BINANCE, BYBIT, OKX).")
    parser.add_argument("--verify-determinism", action="store_true",
                        help="Replay twice and require identical digests.")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    try:
        source = ReplaySource.from_directory(args.data_dir, date=args.date, venue=args.venue)
    except (StorageCollisionError, ValueError) as exc:  # ambiguous storage / unknown venue
        print(f"[FAIL] {exc}")
        return 1

    if len(source) == 0:
        # An empty replay is not a clean replay.
        print(f"[FAIL] no recorded raw frames under {args.data_dir}"
              + (f" for {args.date}" if args.date else ""))
        return 1

    try:
        result = ReplayEngine(venue=args.venue).run(source)
    except ValueError as exc:  # unsupported venue: say so, do not traceback
        print(f"[FAIL] {exc}")
        return 1
    summary = result.summary()

    if args.verify_determinism:
        second = ReplayEngine(venue=args.venue).run(source)
        summary["deterministic"] = second.digest == result.digest
        if not summary["deterministic"]:
            summary["second_digest"] = second.digest

    if args.as_json:
        print(json.dumps(summary, indent=2))
    else:
        width = max(len(key) for key in summary)
        for key, value in summary.items():
            print(f"{key:<{width}} : {value}")

    ok = True
    if args.verify_determinism and not summary.get("deterministic", True):
        print("\n[FAIL] replay is not deterministic: digests differ")
        ok = False
    if source.skipped_rows:
        print(f"[WARN] excluded rows from other venues: {source.skipped_rows}")
    if result.frames_undecodable:
        print(f"\n[WARN] {result.frames_undecodable} frame(s) were undecodable when recorded")
    if result.snapshots_rejected:
        print(f"[WARN] {result.snapshots_rejected} snapshot(s) rejected")
    if result.final_state != "VALID":
        print(f"[WARN] book ended in {result.final_state}, not VALID")

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
