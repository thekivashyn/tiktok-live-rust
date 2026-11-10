"""Validate and summarize two timed offline engine soak reports."""
import argparse
import json
import statistics
from pathlib import Path
from compare_engine import validate


def summarize(path):
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    run, = document["runs"]
    validate(run)
    child = run["child_report"]
    elapsed = child["elapsed_seconds"]
    assert elapsed >= 600
    assert child["forced_cuts"] >= 1
    assert child["report"]["reconnects"] == child["forced_cuts"]
    assert child["report"]["decode_errors"] == 0
    samples = run["samples"]
    first = [s for s in samples if 60 <= s["seconds"] < 120]
    last = [s for s in samples if elapsed - 60 <= s["seconds"] <= elapsed]
    assert len(first) >= 100 and len(last) >= 100
    memory = {}
    for field in ("working_set_bytes", "private_bytes"):
        before = statistics.median(s[field] for s in first) / 1048576
        after = statistics.median(s[field] for s in last) / 1048576
        after_warmup = [s[field] / 1048576 for s in samples if s["seconds"] >= 60]
        memory[field] = {"minute_2_median_mib": before, "last_minute_median_mib": after,
                         "median_delta_mib": after - before,
                         "after_warmup_min_mib": min(after_warmup), "after_warmup_max_mib": max(after_warmup)}
    return {"version": child["version"], "binary_sha256": document["binary_sha256"],
            "elapsed_seconds": elapsed, "frames": child["frames"], "events": child["events"],
            "gift_quantity": child["gift_quantity"], "forced_cuts": child["forced_cuts"],
            "connections": child["report"]["connections"], "decode_errors": 0,
            "events_unqueued": 0, "server_tasks_after_stop": 0, "peak_server_tasks": 1,
            "peak_working_set_mib": document["median_peak_working_set_mib"],
            "peak_threads_sampled": run["peak_threads_sampled"], "cpu_seconds": run["cpu_seconds"],
            "memory": memory}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = {"baseline": summarize(args.baseline), "candidate": summarize(args.candidate),
              "event_counts_sequence_gift_totals_and_cleanup_verified": True,
              "limits": "Each run is 600 real seconds with 5ms requested pacing and a cut before ACK every 10000 fresh frames. Frame totals differ with scheduling and reconnect backoff. Local in-memory fixture, no network or 24-hour claim. RAM is sampled OS process memory, includes fixture. Builds and other verification could overlap; use paired fixed-frame benchmark for throughput."}
    Path(args.output).write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
