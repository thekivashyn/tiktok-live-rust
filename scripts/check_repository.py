"""Validate the source manifest and local Markdown targets; no network access."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import tomllib
from urllib.parse import unquote

from prepare_public_repo import ROOT, selected_files


def expected_manifest():
    return {
        "engine_version": tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))["package"]["version"],
        "content_normalization": "LF newlines for selected text files",
        "files": [{"path": p.relative_to(ROOT).as_posix(),
                   "sha256": hashlib.sha256(p.read_bytes().replace(b"\r\n", b"\n")).hexdigest()}
                  for p in selected_files()],
    }


def broken_links():
    failures = []
    for path in selected_files():
        if path.suffix != ".md":
            continue
        text = re.sub(r"```[\s\S]*?```|~~~[\s\S]*?~~~", "", path.read_text(encoding="utf-8"))
        for target in re.findall(r"\]\(([^)]+)\)", text):
            if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", target) or target.startswith("#"):
                continue
            local = unquote(target.split("#")[0])
            resolved = (path.parent / local).resolve()
            if ROOT not in resolved.parents or not resolved.is_file():
                failures.append(f"{path.relative_to(ROOT)}: {target}")
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--update-manifest", action="store_true",
                        help="Explicitly refresh manifest after intended source changes")
    args = parser.parse_args()
    manifest = expected_manifest()
    destination = ROOT / "PUBLIC_SOURCE_MANIFEST.json"
    if args.update_manifest:
        destination.write_bytes((json.dumps(manifest, indent=2) + "\n").encode())
    failures = broken_links()
    if not destination.is_file() or json.loads(destination.read_text(encoding="utf-8")) != manifest:
        failures.append("source manifest differs; review changes, then use --update-manifest")
    print(json.dumps({"source_files": len(manifest["files"]), "failures": failures}, indent=2))
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
