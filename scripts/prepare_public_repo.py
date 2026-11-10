"""Create a fresh, local standalone source copy. Never initialize Git or upload."""

import argparse
import hashlib
import json
from pathlib import Path
import tomllib


ROOT = Path(__file__).resolve().parents[1]
ROOT_FILES = (
    ".gitattributes", ".gitignore", "AGENTS.md", "Cargo.toml", "Cargo.lock", "README.md",
    "LICENSE", "THIRD_PARTY_NOTICES.md", "CHANGELOG.md", "DEVELOPMENT.md",
    "CONTRIBUTING.md", "SECURITY.md", "run.ps1", "dlltool.cmd",
)
# Directory + permitted suffixes: unknown extensions never enter the export.
TREES = {
    "src": {".rs"}, "tests": {".rs", ".py"}, "examples": {".rs", ".py"},
    "docs": {".md"}, "scripts": {".py", ".md"},
    "docs/assets": {".svg"},
    "benchmarks/connector": {".py", ".mjs", ".json", ".md"},
    ".github": {".yml", ".yaml", ".md"},
}
SKIP = {"node_modules", "__pycache__", ".git", "target", "reports"}


def selected_files():
    files = [ROOT / name for name in ROOT_FILES]
    for directory, suffixes in TREES.items():
        base = ROOT / directory
        for path in sorted(base.rglob("*")):
            relative = path.relative_to(base)
            if any(part in SKIP or part.startswith(".") for part in relative.parts):
                continue
            if path.is_symlink():
                raise ValueError(f"symlink in export tree: {path.relative_to(ROOT)}")
            if path.is_file() and path.suffix in suffixes:
                files.append(path)
    return sorted(files)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, required=True,
                        help="New directory; an existing directory is never replaced")
    args = parser.parse_args()
    destination = args.destination.resolve()
    if destination.exists():
        parser.error("destination already exists; choose a new directory")
    if destination == ROOT or destination in ROOT.parents:
        parser.error("destination must not contain the engine source")
    if ROOT in destination.parents and ROOT / ".public-export" not in destination.parents:
        parser.error("inside the engine, destination must be under .public-export/")
    files = selected_files()
    # Resolve all inputs before creating output; never follow links outside the engine.
    for path in files:
        if not path.is_file() or path.is_symlink() or ROOT not in path.resolve().parents:
            parser.error(f"missing or unsafe input: {path.relative_to(ROOT)}")
    destination.mkdir(parents=True)
    manifest = []
    for path in files:
        relative = path.relative_to(ROOT)
        output = destination / relative
        output.parent.mkdir(parents=True, exist_ok=True)
        # Every selected format is text; stable bytes across Windows and Git clones.
        output.write_bytes(path.read_bytes().replace(b"\r\n", b"\n"))
        manifest.append({"path": relative.as_posix(),
                         "sha256": hashlib.sha256(output.read_bytes()).hexdigest()})
    (destination / "PUBLIC_SOURCE_MANIFEST.json").write_text(
        json.dumps({"engine_version": tomllib.loads(
            (ROOT / "Cargo.toml").read_text(encoding="utf-8"))["package"]["version"],
            "content_normalization": "LF newlines for selected text files",
            "files": manifest}, indent=2) + "\n",
        encoding="utf-8")
    print(json.dumps({"destination": str(destination), "source_files": len(manifest),
                      "git_initialized": False, "uploaded": False}, indent=2))


if __name__ == "__main__":
    main()
