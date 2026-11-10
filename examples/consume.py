"""Python 3.11+ subprocess consumer. Drains both pipes; prints aggregates only."""
import argparse
import asyncio
from collections import Counter
import json
import os
from pathlib import Path

KINDS = {"chat", "gift", "like", "follow", "share", "social", "join", "viewers", "stream_end"}
ROOT = Path(__file__).resolve().parents[1]


async def consume(command, timeout=75):
    process = await asyncio.create_subprocess_exec(
        *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        limit=512 * 1024)
    counts = Counter()
    health = {"last_state": None, "stop_reason": None, "error_code": None}

    async def events():
        async for line in process.stdout:
            event = json.loads(line)
            kind = event.get("kind")
            if kind not in KINDS:
                raise ValueError("unexpected event kind")
            counts[kind] += 1
            # Your application handles the event here. Keep work quick; do not
            # print/store raw chats or profiles unless explicitly needed.
            # Gift quantity_delta=None means unknown, never a known total.

    async def diagnostics():
        async for line in process.stderr:
            record = json.loads(line)
            data = record.get("data", {})
            if record.get("type") == "status":
                health["last_state"] = data.get("state")
            elif record.get("type") == "report":
                health["stop_reason"] = data.get("stop_reason")
                health["error_code"] = (data.get("error") or {}).get("code")

    try:
        async with asyncio.timeout(timeout):
            async with asyncio.TaskGroup() as group:
                group.create_task(events())
                group.create_task(diagnostics())
            code = await process.wait()
        return {"exit_code": code, "events": dict(counts), **health}
    finally:
        if process.returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=3)
            except TimeoutError:
                process.kill()
                await process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--user")
    target.add_argument("--demo", action="store_true", help="Read the built offline_demo example")
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--binary", type=Path, help="Override the executable path")
    args = parser.parse_args()
    if not 1 <= args.seconds <= 86400:
        parser.error("seconds must be 1..86400")
    name = "offline_demo" if args.demo else "tiktok-live-rust"
    binary = args.binary or ROOT / "target" / "release" / ("examples" if args.demo else "") / (name + (".exe" if os.name == "nt" else ""))
    if not binary.is_file():
        parser.error("executable not found; build the CLI or offline_demo first")
    command = [str(binary.resolve())]
    if not args.demo:
        command += ["--user", args.user, "--seconds", str(args.seconds), "--events", "--status"]
    try:
        summary = asyncio.run(consume(command, timeout=10 if args.demo else args.seconds + 15))
    except KeyboardInterrupt:
        return 130
    except (ExceptionGroup, ValueError, TimeoutError) as error:
        print(json.dumps({"error": "invalid or incomplete child output", "kind": type(error).__name__}))
        return 1
    print(json.dumps(summary, indent=2))
    return 0 if summary["exit_code"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
