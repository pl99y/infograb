import json
import tempfile
import unittest
from pathlib import Path
from scripts.export_status import telegram_preview, publish_exports


class PreviewTests(unittest.TestCase):
    def test_preview_preserves_order_and_all_channels(self):
        rows = [{"id": i, "source_name": str(i % 7), "fetched_at": str(i).zfill(3)} for i in range(100)]
        result = telegram_preview(rows)
        self.assertEqual(result["items"], rows[:50])
        self.assertEqual(len(result["sources"]), 7)
        self.assertEqual(result["snapshot_at"], "099")
        self.assertEqual(telegram_preview([])["items"], [])

    def test_failed_collection_preview_matches_retained_full(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            root = Path(directory)
            rows = [{"id": 1, "source_name": "retained"}]
            (root / "telegram.json").write_text(json.dumps(rows), encoding="utf-8")
            def must_not_load():
                raise AssertionError("failed collection should retain snapshot")
            publish_exports(root, "15m", {"telegram.json": must_not_load}, {"telegram.json": {"status": "failed", "last_attempt_at": "2026-11-24T00:00:00Z"}})
            preview = json.loads((root / "telegram_preview.json").read_text(encoding="utf-8"))
            self.assertEqual(preview["items"], rows)
            self.assertEqual(preview["sources"], ["retained"])
            self.assertEqual(json.loads((root / "export_meta.json").read_text(encoding="utf-8"))["profiles"]["15m"]["datasets"]["telegram.json"]["status"], "failed")
