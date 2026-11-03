"""Recompute resource/stage summaries from the archived, extracted evidence."""

import collections
import datetime
import json
from pathlib import Path
import re
import sys


def resources(rows, hz, start, end):
    selected = [row for row in rows if start <= row["wall_ns"] <= end]
    intervals = []
    total_ticks = 0
    total_ns = 0
    for a, b in zip(selected, selected[1:]):
        before = {p["pid"]: p["ticks"] for p in a["processes"]}
        after = {p["pid"]: p["ticks"] for p in b["processes"]}
        if before.keys() != after.keys() or not before:
            continue
        ticks = sum(after[pid] - value for pid, value in before.items())
        ns = b["monotonic_ns"] - a["monotonic_ns"]
        assert ticks >= 0 and ns > 0
        total_ticks += ticks
        total_ns += ns
        intervals.append(ticks / hz / (ns / 1e9))
    return {
        "samples": len(selected),
        "complete_intervals": len(intervals),
        "sampled_interval_seconds": total_ns / 1e9,
        "cpu_average_cores": total_ticks / hz / (total_ns / 1e9) if total_ns else None,
        "cpu_peak_interval_cores": max(intervals, default=None),
        "simultaneous_sum_rss_peak_mib": max(
            (sum(p["rss_bytes"] for p in r["processes"]) / 1048576 for r in selected),
            default=None,
        ),
        "first_sample_wall_ns": selected[0]["wall_ns"] if selected else None,
        "last_sample_wall_ns": selected[-1]["wall_ns"] if selected else None,
    }


def main(root):
    report = json.loads((root / "aggregate.json").read_text())
    start = report["settings"]["start_unix_ms"] * 1000000
    drained = start + round(report["global_ramp_and_drain_seconds"] * 1e9)
    ping = min(w["ping_started_unix_ms"] for w in report["workers"]) * 1000000
    server = [json.loads(line) for line in (root / "server/resources.jsonl").read_text().splitlines()]
    client = json.loads((root / "client/client-resources.json").read_text())
    summary = {
        "method": "Sum process CPU ticks over complete adjacent same-PID intervals inside each phase; monotonic duration; no boundary interpolation. RSS is simultaneous sampled sum. Broker/client phase alignment uses wall time, without measured cross-host clock offset.",
        "boundaries_ns": {"start": start, "drained": drained, "ping": ping},
    }
    for name, rows, hz in [("broker", server, server[0]["cpu_hz"]), ("client", client["samples"], client["cpu_hz"])]:
        summary[name] = {
            phase: resources(rows, hz, a, b)
            for phase, a, b in [("ramp", start, drained), ("hold_before_ping", drained, ping), ("ramp_and_hold", start, ping)]
        }
    (root / "resource-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    stages = collections.defaultdict(list)
    errors = []
    for path in sorted((root / "server").glob("node*/process.log")):
        for line in path.read_text().splitlines():
            try:
                when = round(datetime.datetime.fromisoformat(line.split()[0]).timestamp() * 1e9)
            except (ValueError, IndexError):
                continue
            if " ERROR " in line or 'outcome=Failure' in line:
                errors.append({"node": path.parent.name, "phase": "before" if when < start else "ramp" if when < drained else "hold" if when < ping else "cleanup", "line": line})
            if not start <= when < drained:
                continue
            fields = dict(re.findall(r"(\w+)=(\S+)", line))
            if "elapsed_us" in fields and "stage" in fields:
                key = (path.parent.name, fields["stage"], fields.get("kind", ""), fields.get("outcome", ""))
                stages[key].append(int(fields["elapsed_us"]))
    stage_summary = {
        "note": "Sparse diagnostic samples, not population percentiles or paired request stages.",
        "ramp_stages": [{"node": k[0], "stage": k[1], "kind": k[2], "outcome": k[3], "samples": len(v), "max_elapsed_us": max(v)} for k, v in sorted(stages.items())],
        "errors": errors,
    }
    (root / "stage-summary.json").write_text(json.dumps(stage_summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main(Path(sys.argv[1]))
