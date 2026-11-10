"""Check exact registry versions in Cargo.lock against OSV; standard library only.

Sends public crate names/versions to https://api.osv.dev. Does not upload source,
local paths, environment variables, account data, or engine events.
"""
import datetime
import hashlib
import json
from pathlib import Path
import sys
import tomllib
import urllib.request


def main():
    root = Path(__file__).resolve().parents[1]
    lock = (root / "Cargo.lock").read_bytes()
    packages = [p for p in tomllib.loads(lock.decode())["package"] if p.get("source", "").startswith("registry+")]
    findings = {f'{p["name"]}@{p["version"]}': set() for p in packages}
    queries = [{"package": {"ecosystem": "crates.io", "name": p["name"]}, "version": p["version"]} for p in packages]
    pending = list(enumerate(queries))
    for _ in range(20):
        if not pending:
            break
        next_pending = []
        for start in range(0, len(pending), 200):
            batch = pending[start:start + 200]
            request = urllib.request.Request("https://api.osv.dev/v1/querybatch",
                data=json.dumps({"queries": [query for _, query in batch]}).encode(),
                headers={"Content-Type": "application/json", "User-Agent": "tiktok-live-dependency-check/0.2"})
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read(5 * 1024 * 1024 + 1)
            if len(body) > 5 * 1024 * 1024:
                raise RuntimeError("OSV response exceeded limit")
            results = json.loads(body)["results"]
            if len(results) != len(batch):
                raise RuntimeError("OSV returned an incomplete result set")
            for (index, query), result in zip(batch, results):
                p = packages[index]
                findings[f'{p["name"]}@{p["version"]}'].update(v["id"] for v in result.get("vulns", []))
                if result.get("next_page_token"):
                    next_pending.append((index, {**query, "page_token": result["next_page_token"]}))
        pending = next_pending
    if pending:
        raise RuntimeError("OSV pagination incomplete")
    affected = {package: sorted(ids) for package, ids in findings.items() if ids}
    report = {
        "checked_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source": "https://api.osv.dev/v1/querybatch",
        "cargo_lock_sha256": hashlib.sha256(lock).hexdigest(),
        "scope": "All registry packages in Cargo.lock, including other platform dependencies",
        "package_versions_checked": len(packages), "complete": True,
        "affected_packages": affected,
        "limits": "Known advisory lookup only; not a source-code audit, penetration test, or absence-of-vulnerabilities guarantee.",
    }
    path = root / "reports" / "dependency-audit.json"
    path.parent.mkdir(exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
    print(json.dumps(report, indent=2))
    return 1 if affected else 0


if __name__ == "__main__":
    sys.exit(main())
