"""Explicit live smoke test; consumes NDJSON without storing chat or sender data."""
import argparse
from collections import Counter
import json
import hashlib
from pathlib import Path
import subprocess
import tempfile
import threading


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("username")
    parser.add_argument("--seconds", type=int, default=180)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 600:
        parser.error("smoke duration must be 1..600")
    root = Path(__file__).resolve().parents[1]
    report_path = root / "reports" / "live-v0.7.json"
    report_path.parent.mkdir(exist_ok=True)
    binary = root / "target" / "release" / "tiktok-live-rust.exe"
    counts = Counter()
    sequence = 0
    with tempfile.TemporaryFile() as stderr:
        process = subprocess.Popen([str(binary), "--user", args.username, "--seconds", str(args.seconds),
            "--max-reconnects", "2", "--events", "--report", str(report_path)], stdout=subprocess.PIPE, stderr=stderr)
        watchdog = threading.Timer(args.seconds + 20, process.kill)
        watchdog.start()
        try:
            while line := process.stdout.readline(65537):
                if len(line) > 65536:
                    raise RuntimeError("event line exceeded bound")
                event = json.loads(line)
                sequence += 1
                if event["sequence"] != sequence:
                    raise RuntimeError("event sequence discontinuity")
                counts[event["kind"]] += 1
            returncode = process.wait(timeout=5)
            stderr.seek(0)
            report = json.loads(stderr.read(65536))
            consistent = sequence == report["events_written"] == report["events_enqueued"] and dict(counts) == report["events"]
            result = {"username": args.username, "requested_seconds": args.seconds,
                "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "exit_code": returncode, "events_received": sequence, "event_counts": dict(counts),
                "sequence_contiguous": True, "report_matches_output": consistent,
                "engine_report": report, "raw_events_saved": False}
            (root / "reports" / "live-v0.7-output-check.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
            print(json.dumps(result, indent=2))
            if returncode != 0 or not consistent:
                raise SystemExit(1)
        finally:
            watchdog.cancel()
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


if __name__ == "__main__":
    main()
