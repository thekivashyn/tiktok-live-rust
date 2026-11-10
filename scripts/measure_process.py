"""Windows-only process measurements; standard library, no raw event recording.

Example: python scripts/measure_process.py --runs 3 --output reports/bench.json -- target/release/examples/bench_pipeline.exe 32
Default: child outputs one aggregate JSON document. With --event-stream, NDJSON stdout
is consumed without recording raw events and the aggregate JSON is read from stderr.
Add --status-stream when the child also uses --status (tagged stderr NDJSON).
"""
import argparse
import ctypes as c
from ctypes import wintypes as w
import hashlib
import json
import os
from pathlib import Path
import statistics
from collections import Counter
import subprocess
import tempfile
import time
import threading


class Memory(c.Structure):
    _fields_ = [("cb", w.DWORD), ("faults", w.DWORD)] + [(name, c.c_size_t) for name in (
        "peak_working_set", "working_set", "peak_paged", "paged", "peak_nonpaged", "nonpaged",
        "pagefile", "peak_pagefile", "private_bytes")]


class Thread(c.Structure):
    _fields_ = [("size", w.DWORD), ("usage", w.DWORD), ("thread_id", w.DWORD), ("owner", w.DWORD),
        ("priority", w.LONG), ("delta", w.LONG), ("flags", w.DWORD)]


kernel = c.WinDLL("kernel32", use_last_error=True)
psapi = c.WinDLL("psapi", use_last_error=True)
kernel.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
kernel.OpenProcess.restype = w.HANDLE
kernel.CloseHandle.argtypes = [w.HANDLE]
kernel.GetProcessTimes.argtypes = [w.HANDLE] + [c.POINTER(w.FILETIME)] * 4
psapi.GetProcessMemoryInfo.argtypes = [w.HANDLE, c.POINTER(Memory), w.DWORD]
kernel.CreateToolhelp32Snapshot.argtypes = [w.DWORD, w.DWORD]
kernel.CreateToolhelp32Snapshot.restype = w.HANDLE
kernel.Thread32First.argtypes = [w.HANDLE, c.POINTER(Thread)]
kernel.Thread32Next.argtypes = [w.HANDLE, c.POINTER(Thread)]


def threads(pid):
    snapshot = kernel.CreateToolhelp32Snapshot(4, 0)
    if snapshot == c.c_void_p(-1).value:
        return 0
    try:
        item = Thread()
        item.size = c.sizeof(item)
        ok = kernel.Thread32First(snapshot, c.byref(item))
        count = 0
        while ok:
            count += item.owner == pid
            ok = kernel.Thread32Next(snapshot, c.byref(item))
        return count
    finally:
        kernel.CloseHandle(snapshot)


def once(command, timeout, event_stream, status_stream, diagnostic_path=None):
    started = time.perf_counter()
    samples = []
    handle = None
    with tempfile.TemporaryFile() as output, (open(diagnostic_path, "w+b") if diagnostic_path else tempfile.TemporaryFile()) as errors:
        child = subprocess.Popen(command, stdout=subprocess.PIPE if event_stream else output, stderr=errors, creationflags=subprocess.CREATE_NO_WINDOW)
        consumed = Counter()
        stream_errors = []
        def drain():
            sequence = 0
            try:
                while line := child.stdout.readline(65537):
                    if len(line) > 65536:
                        raise ValueError("event line exceeds bound")
                    event = json.loads(line)
                    sequence += 1
                    if event["sequence"] != sequence:
                        raise ValueError("event sequence gap")
                    consumed[event["kind"]] += 1
            except Exception:
                stream_errors.append("event stream validation failed")
                child.kill()
        reader = threading.Thread(target=drain, daemon=True) if event_stream else None
        if reader:
            reader.start()
        try:
            handle = kernel.OpenProcess(0x410, False, child.pid)
            if not handle:
                raise OSError(c.get_last_error(), "OpenProcess failed")
            while child.poll() is None:
                elapsed = time.perf_counter() - started
                if elapsed > timeout:
                    raise TimeoutError("measurement deadline exceeded")
                mem = Memory()
                mem.cb = c.sizeof(mem)
                if psapi.GetProcessMemoryInfo(handle, c.byref(mem), c.sizeof(mem)):
                    samples.append({"seconds": round(elapsed, 3), "working_set_bytes": mem.working_set,
                        "os_peak_working_set_bytes": mem.peak_working_set, "private_bytes": mem.private_bytes,
                        "threads": threads(child.pid)})
                time.sleep(0.25)
            times = [w.FILETIME() for _ in range(4)]
            if not kernel.GetProcessTimes(handle, *(c.byref(t) for t in times)):
                raise OSError(c.get_last_error(), "GetProcessTimes failed")
            seconds = lambda t: ((t.dwHighDateTime << 32) | t.dwLowDateTime) / 10_000_000
            if reader:
                reader.join(timeout=3)
                if reader.is_alive() or stream_errors:
                    raise ValueError("event reader failed to finish cleanly")
            document = errors if event_stream else output
            document.seek(0)
            raw = document.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise ValueError("aggregate JSON exceeded limit")
            status_summary = None
            if status_stream:
                records = [json.loads(line) for line in raw.splitlines()]
                if len(records) < 2 or records[-1]["type"] != "report":
                    raise ValueError("missing final diagnostic report")
                report = records[-1]["data"]
                statuses = records[:-1]
                if any(r["type"] != "status" for r in statuses):
                    raise ValueError("unexpected diagnostic type")
                final = statuses[-1]["data"]
                if final["state"] != "stopped" or final["events_enqueued"] != report["events_enqueued"]:
                    raise ValueError("final status and report differ")
                states = []
                for record in statuses:
                    state = record["data"]["state"]
                    if not states or states[-1] != state:
                        states.append(state)
                status_summary = {"snapshots": len(statuses), "transitions": states, "last_status": final}
            else:
                report = json.loads(raw)
            if event_stream and (sum(consumed.values()) != report["events_written"] or dict(consumed) != report["events"]):
                raise ValueError("event stream and report counts differ")
            return {"exit_code": child.returncode, "wall_seconds": time.perf_counter() - started,
                "event_stream": event_stream, "events_consumed": sum(consumed.values()), "raw_events_saved": False,
                "cpu_seconds": seconds(times[2]) + seconds(times[3]),
                "peak_working_set_bytes": max(s["os_peak_working_set_bytes"] for s in samples),
                "peak_private_bytes_sampled": max(s["private_bytes"] for s in samples),
                "peak_threads_sampled": max(s["threads"] for s in samples),
                "samples": samples, "status_summary": status_summary, "child_report": report}
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
            if handle:
                kernel.CloseHandle(handle)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--output", required=True)
    parser.add_argument("--event-stream", action="store_true", help="drain child NDJSON stdout and read its aggregate report from stderr")
    parser.add_argument("--status-stream", action="store_true", help="parse the child's --status stderr records; requires --event-stream")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command and args.command[0] == "--" else args.command
    if not command or not 1 <= args.runs <= 10 or not 1 <= args.timeout <= 900:
        parser.error("provide a command, runs 1..10 and timeout 1..900")
    if args.status_stream and not args.event_stream:
        parser.error("--status-stream requires --event-stream")
    binary = Path(command[0]).resolve(strict=True)
    command[0] = str(binary)
    runs = [once(command, args.timeout, args.event_stream, args.status_stream) for _ in range(args.runs)]
    summary = {"logical_cpus": os.cpu_count(), "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "poll_seconds": 0.25, "runs": runs,
        "median_peak_working_set_mib": statistics.median(r["peak_working_set_bytes"] for r in runs) / 1048576,
        "median_peak_threads": statistics.median(r["peak_threads_sampled"] for r in runs),
        "limits": "OS working-set peak observed during polling; private memory and thread peaks are sampled. Not whole-machine RAM or network latency."}
    rates = [r["child_report"].get("events_per_second") for r in runs]
    if all(rate is not None for rate in rates):
        summary["median_events_per_second"] = statistics.median(rates)
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if k != "runs"}, indent=2))
    print(json.dumps({"exit_codes": [r["exit_code"] for r in runs], "child_reports": [r["child_report"] for r in runs]}, indent=2))
    if any(r["exit_code"] != 0 for r in runs):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
