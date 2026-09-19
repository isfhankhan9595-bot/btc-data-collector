"""Report the field structure actually observed in captured OKX frames.

This is the second half of the D11 unblock. ``run_okx_capture`` records raw
frames; this script reads them back and reports, per channel, which keys
appeared inside ``data[]``, how often, and with what value types.

It reports **observation, not interpretation**. It will tell you that a
``funding-rate`` payload carries a key named ``fundingRate`` whose values look
like decimal strings. It will not tell you whether that is a period rate or an
annualised one, which is a semantic question that only documentation or an
authoritative SDK can answer. Field *names* can be read off the wire safely;
field *meanings* cannot, and a parser needs both.

Usage
-----
    python -m collector.scripts.okx_schema_report --data-dir data
    python -m collector.scripts.okx_schema_report --data-dir data --json

Exit codes
----------
0  frames were found and a report was produced
1  no OKX data-push frames were found (nothing to report)
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from typing import Any

from collector.collector.storage_layout import StorageCollisionError, iter_segments, read_streams

#: Envelope keys, documented and already known. Excluded from the payload
#: report so the output is only the part that is actually unverified.
ENVELOPE_KEYS = frozenset({"arg", "data", "event", "code", "msg", "connId", "id", "action"})

#: Truncate example values so a deep order book does not dominate the report.
EXAMPLE_MAX_CHARS = 120


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, str):
        # Distinguish numeric-looking strings: OKX sends numbers as strings,
        # and knowing that is necessary to avoid float coercion bugs.
        stripped = value.strip()
        if stripped == "":
            return "str(empty)"
        try:
            float(stripped)
        except ValueError:
            return "str"
        return "str(numeric)"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, list):
        return f"list[{_type_name(value[0])}]" if value else "list(empty)"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def collect(data_dir: str, date: str | None = None) -> dict[str, Any]:
    import pandas as pd

    channels: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "frames": 0,
            "data_elements": 0,
            "keys": Counter(),
            "key_types": defaultdict(Counter),
            "key_examples": {},
            "action_values": Counter(),
        })
    totals = {
        "raw_wire_rows": 0, "okx_rows": 0, "decode_failed": 0,
        "control_frames": 0, "data_push_frames": 0, "non_push_frames": 0,
        "unparseable_payloads": 0,
    }

    try:
        # OKX's own stream first, then the legacy unprefixed ``raw_wire`` where
        # its pre-namespace captures share a directory with Binance's frames.
        # The venue filter below is what separates them; nothing is assumed.
        paths = [path for name in read_streams("OKX", "raw_wire")
                 for path in sorted(iter_segments(data_dir, name, date=date))]
    except StorageCollisionError as exc:
        # Ambiguous storage means the observed frame set cannot be trusted to
        # be the frame set that was captured. Report it instead of guessing.
        return {"totals": totals, "segments_read": 0, "channels": {},
                "storage_collision": str(exc)}
    for path in paths:
        frame = pd.read_parquet(path)
        for row in frame.to_dict("records"):
            totals["raw_wire_rows"] += 1
            if str(row.get("venue")) != "OKX":
                continue
            totals["okx_rows"] += 1
            if not row.get("decode_ok", True):
                totals["decode_failed"] += 1
                continue
            if str(row.get("channel")) == "__control__":
                totals["control_frames"] += 1
                continue
            try:
                parsed = json.loads(row.get("payload") or "")
            except (json.JSONDecodeError, TypeError, ValueError):
                totals["unparseable_payloads"] += 1
                continue
            if not isinstance(parsed, dict) or "data" not in parsed:
                totals["non_push_frames"] += 1
                continue

            arg = parsed.get("arg") or {}
            channel = arg.get("channel") if isinstance(arg, dict) else None
            channel = channel or row.get("channel") or "<unknown>"
            bucket = channels[str(channel)]
            bucket["frames"] += 1
            totals["data_push_frames"] += 1
            if parsed.get("action") is not None:
                bucket["action_values"][str(parsed["action"])] += 1

            elements = parsed.get("data")
            if not isinstance(elements, list):
                elements = [elements]
            for element in elements:
                if not isinstance(element, dict):
                    continue
                bucket["data_elements"] += 1
                for key, value in element.items():
                    if key in ENVELOPE_KEYS:
                        continue
                    bucket["keys"][key] += 1
                    bucket["key_types"][key][_type_name(value)] += 1
                    if key not in bucket["key_examples"]:
                        bucket["key_examples"][key] = json.dumps(value)[:EXAMPLE_MAX_CHARS]

    report = {"totals": totals, "segments_read": len(paths), "channels": {}}
    for channel, bucket in sorted(channels.items()):
        elements = bucket["data_elements"] or 1
        report["channels"][channel] = {
            "frames": bucket["frames"],
            "data_elements": bucket["data_elements"],
            "action_values": dict(bucket["action_values"]),
            "fields": {
                key: {
                    "present_in": count,
                    "presence_ratio": round(count / elements, 4),
                    "types": dict(bucket["key_types"][key]),
                    "example": bucket["key_examples"].get(key),
                }
                for key, count in sorted(bucket["keys"].items(), key=lambda kv: -kv[1])
            },
        }
    return report


def render(report: dict[str, Any]) -> str:
    lines: list[str] = []
    totals = report["totals"]
    if report.get("storage_collision"):
        lines.append(f"STORAGE COLLISION: {report['storage_collision']}")
        lines.append("Refusing to report field structure from ambiguous storage.")
        return "\n".join(lines)
    lines.append(f"segments read: {report['segments_read']}")
    lines.append(
        "rows: raw_wire={raw_wire_rows} okx={okx_rows} data_push={data_push_frames} "
        "control={control_frames} other_envelope={non_push_frames} "
        "decode_failed={decode_failed} unparseable={unparseable_payloads}".format(**totals))
    if not report["channels"]:
        lines.append("")
        lines.append("No OKX data-push frames found. Nothing can be verified from this data.")
        return "\n".join(lines)
    for channel, info in report["channels"].items():
        lines.append("")
        lines.append(f"== {channel} ==")
        lines.append(f"   frames={info['frames']} data_elements={info['data_elements']}"
                     + (f" action={info['action_values']}" if info["action_values"] else ""))
        for key, field in info["fields"].items():
            types = ",".join(f"{t}x{c}" for t, c in field["types"].items())
            lines.append(
                f"   {key:<24} present={field['presence_ratio']:<7} "
                f"types={types:<24} e.g. {field['example']}")
    lines.append("")
    lines.append("Field NAMES above are observed on the wire and safe to rely on.")
    lines.append("Field MEANINGS (units, sign conventions, period vs annualised,")
    lines.append("side encoding) are NOT established by this report and must come")
    lines.append("from official documentation before any parser is written.")
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD; default all")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    report = collect(args.data_dir, args.date)
    print(json.dumps(report, indent=2, default=str) if args.as_json else render(report))
    return 0 if report["channels"] else 1


if __name__ == "__main__":
    sys.exit(main())
