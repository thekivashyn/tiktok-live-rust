"""Paired offline engine-loop benchmark; never connects to TikTok."""
import argparse
import hashlib
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from measure_process import once


def validate(result):
    child = result["child_report"]
    frames = child["frames"]
    expected = {kind: frames for kind in ("chat", "like", "follow", "share", "join", "viewers")}
    expected.update(gift=frames * 2, stream_end=1)
    assert result["exit_code"] == 0
    assert child["event_counts"] == expected
    assert child["gift_quantity"] == frames * 3
    assert child["events"] == frames * 8 + 1
    assert child["report"]["error"] is None
    assert child["report"]["events_unqueued"] == 0
    assert child["server_tasks_after_stop"] == 0
    assert child["peak_server_tasks"] == 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--frames", type=int, default=50000)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if not 1 <= args.pairs <= 10 or not 1000 <= args.frames <= 2000000:
        parser.error("pairs 1..10 and frames 1000..2000000 required")
    binaries = {name: Path(getattr(args, name)).resolve(strict=True) for name in ("baseline", "candidate")}
    modes = {}
    for mode in ("plain", "mixed", "gzip"):
        runs = {name: [] for name in binaries}
        order = []
        for pair in range(args.pairs):
            for name in (["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]):
                result = once([str(binaries[name]), str(args.frames), mode], 120, False, False)
                validate(result)
                assert result["child_report"]["frames"] == args.frames
                runs[name].append(result)
                order.append(name)
        summary = {}
        for name, group in runs.items():
            summary[name] = {
                "version": group[0]["child_report"]["version"],
                "events_per_second": statistics.median(r["child_report"]["events_per_second"] for r in group),
                "cpu_seconds": statistics.median(r["cpu_seconds"] for r in group),
                "peak_working_set_mib": statistics.median(r["peak_working_set_bytes"] for r in group) / 1048576,
                "peak_private_mib_sampled": statistics.median(r["peak_private_bytes_sampled"] for r in group) / 1048576,
                "peak_threads_sampled": statistics.median(r["peak_threads_sampled"] for r in group),
                "ack_p50_us": statistics.median(r["child_report"]["ack_latency_us_upper"]["p50"] for r in group),
                "ack_p95_us": statistics.median(r["child_report"]["ack_latency_us_upper"]["p95"] for r in group),
                "ack_p99_us": statistics.median(r["child_report"]["ack_latency_us_upper"]["p99"] for r in group),
            }
        modes[mode] = {"order": order, "summary": summary, "runs": runs}
        print(json.dumps({"mode": mode, "summary": summary}), flush=True)
    output = {"recorded_at_utc": datetime.now(timezone.utc).isoformat(), "pairs": args.pairs,
              "frames_per_run": args.frames, "event_counts_and_gift_totals_verified": True,
              "binary_sha256": {n: hashlib.sha256(p.read_bytes()).hexdigest() for n, p in binaries.items()},
              "limits": "Sequential same-host synthetic fixture, not TikTok network latency. Fixed 1us histogram with overflow mapped to observed max. Process RAM includes fixture/runtime. CPU is OS process CPU; private RAM/threads sampled every 250ms.",
              "modes": modes}
    Path(args.output).write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
