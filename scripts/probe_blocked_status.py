"""Explicit live probe: leave stderr unread, consume/discard events, verify bounded exit.

Only the child pipe is blocked. No system networking or TikTok settings are changed.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess
import threading
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("username")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    binary = root / "target" / "release" / "tiktok-live-rust.exe"
    report_path = root / "reports" / "blocked-status-v0.7-engine.json"
    report_path.parent.mkdir(exist_ok=True)
    seconds = 15
    counts = Counter()
    errors = []
    started = time.monotonic()
    started_unix_ms = time.time_ns() // 1_000_000
    process = subprocess.Popen([str(binary), "--user", args.username, "--seconds", str(seconds),
        "--events", "--status", "--report", str(report_path)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))

    def drain_events():
        sequence = 0
        try:
            while line := process.stdout.readline(65537):
                if len(line) > 65536:
                    raise ValueError("oversized event")
                event = json.loads(line)
                sequence += 1
                if event["sequence"] != sequence:
                    raise ValueError("local sequence gap")
                counts[event["kind"]] += 1
        except Exception:
            errors.append("event output validation failed")
            process.kill()

    reader = threading.Thread(target=drain_events, daemon=True)
    reader.start()
    timed_out = False
    try:
        try:
            code = process.wait(timeout=seconds + 15)
        except subprocess.TimeoutExpired:
            timed_out = True
            process.kill()
            code = process.wait(timeout=5)
        reader.join(timeout=3)
        elapsed = time.monotonic() - started
        # File timestamp must belong to this run; never treat an older report as fresh evidence.
        report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {}
        failure = report.get("error") or {}
        passed = (not timed_out and not reader.is_alive() and not errors and code == 1
            and failure.get("code") == "consumer_slow" and failure.get("stage") == "status_output"
            and report.get("events_written") == report.get("events_enqueued") == sum(counts.values())
            and sum(counts.values()) > 0 and elapsed < seconds + 10
            and report.get("started_at_unix_ms", 0) >= started_unix_ms)
        result = {"passed": passed, "username": args.username, "requested_seconds": seconds,
            "wall_seconds": elapsed, "exit_code": code, "watchdog_fired": timed_out,
            "events_consumed": sum(counts.values()), "event_counts": dict(counts),
            "raw_events_saved": False, "stderr_intentionally_unread": True,
            "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "report": report}
        (root / "reports" / "blocked-status-v0.7.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2))
        if not passed:
            raise SystemExit(1)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.stdout.close()
        process.stderr.close()


if __name__ == "__main__":
    main()
