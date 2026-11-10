"""Alternate two release pipeline binaries on identical offline workloads (Windows only)."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
from datetime import datetime, timezone
from measure_process import once


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--passes", type=int, default=96)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if not 1 <= args.pairs <= 10 or not 1 <= args.passes <= 4096:
        parser.error("pairs must be 1..10 and passes 1..4096")
    binaries = {name: Path(getattr(args, name)).resolve(strict=True) for name in ("baseline", "candidate")}
    runs = {name: [] for name in binaries}
    order = []
    for index in range(args.pairs):
        for name in (["baseline", "candidate"] if index % 2 == 0 else ["candidate", "baseline"]):
            result = once([str(binaries[name]), str(args.passes)], 120, False, False)
            if result["exit_code"] != 0:
                raise RuntimeError("pipeline benchmark failed")
            runs[name].append(result)
            order.append(name)
    contracts = {(r["child_report"]["frames"], r["child_report"]["events"], r["child_report"]["serialized_bytes"])
        for group in runs.values() for r in group}
    if len(contracts) != 1:
        raise RuntimeError("workload or output counts changed")
    summary = {}
    for name, group in runs.items():
        summary[name] = {"version": group[0]["child_report"]["version"],
            "binary_sha256": hashlib.sha256(binaries[name].read_bytes()).hexdigest(),
            "median_events_per_second": statistics.median(r["child_report"]["events_per_second"] for r in group),
            "median_peak_working_set_mib": statistics.median(r["peak_working_set_bytes"] for r in group) / 1048576}
    output = {"recorded_at_utc": datetime.now(timezone.utc).isoformat(), "pairs": args.pairs,
        "passes": args.passes, "execution_order": order, "output_counts_match": True,
        "frames_events_bytes_per_run": list(contracts.pop()), "summary": summary,
        "throughput_change_percent": (summary["candidate"]["median_events_per_second"] / summary["baseline"]["median_events_per_second"] - 1) * 100,
        "limits": "Synthetic local codec/normalization/JSON throughput; not TikTok delivery speed. OS working-set peak includes prepared inputs and runtime. Runs are sequential, not simultaneous.",
        "runs": runs}
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in output.items() if k != "runs"}, indent=2))


if __name__ == "__main__":
    main()
