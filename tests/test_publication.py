"""Publication checks exercise a temporary synthetic repository."""
import contextlib
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import check_repository as checker
import prepare_public_repo as exporter


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve() / "source"
        self.root.mkdir()
        for name in exporter.ROOT_FILES:
            (self.root / name).write_bytes(b"synthetic\r\n")
        (self.root / "Cargo.toml").write_text('[package]\nname="fixture"\nversion="0.8.0"\n')
        (self.root / "README.md").write_text('[License](LICENSE)\n')
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(exporter, "ROOT", self.root).start()
        mock.patch.object(checker, "ROOT", self.root).start()

    def invoke(self, function, args):
        with mock.patch.object(sys, "argv", ["check", *args]), contextlib.redirect_stdout(io.StringIO()):
            return function()

    def test_export_excludes_runtime_data_and_keeps_examples_and_assets(self):
        for name in [".env", "reports/private.json", "target/private.rs", "examples/consume.py", "docs/assets/hero.svg"]:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"synthetic\r\n")
        destination = self.root / ".public-export" / "copy"
        self.invoke(exporter.main, ["--destination", str(destination)])
        self.assertFalse((destination / ".env").exists())
        self.assertFalse((destination / "reports").exists())
        self.assertFalse((destination / "target").exists())
        self.assertTrue((destination / "examples/consume.py").is_file())
        self.assertTrue((destination / "docs/assets/hero.svg").is_file())
        manifest = json.loads((destination / "PUBLIC_SOURCE_MANIFEST.json").read_text())
        for item in manifest["files"]:
            content = (destination / item["path"]).read_bytes()
            self.assertNotIn(b"\r\n", content)
            self.assertEqual(hashlib.sha256(content).hexdigest(), item["sha256"])

    def test_manifest_detects_unrecorded_changes(self):
        self.assertFalse(self.invoke(checker.main, ["--update-manifest"]))
        (self.root / "LICENSE").write_text("changed\n")
        self.assertTrue(self.invoke(checker.main, []))

    def test_links_reject_missing_files_and_paths_outside_the_repository(self):
        (self.root / "README.md").write_text('[Missing](missing.md)\n[Outside](../outside.md)\n')
        (self.root.parent / "outside.md").write_text("exists outside\n")
        self.assertEqual(len(checker.broken_links()), 2)


if __name__ == "__main__":
    unittest.main()
