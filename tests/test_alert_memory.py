from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bkk_collector.storage.parquet_store import DurableParquetSpool, check_alert_spool_size


class AlertMemoryTests(unittest.TestCase):
    def test_expansion_guard_checks_repeated_string_bytes_without_serializing(self):
        shared = "x" * (1024 * 1024)
        rows = [{"entity_id": str(index), "informed_entities_json": shared} for index in range(3421)]
        with patch("bkk_collector.storage.parquet_store.json.dumps", side_effect=AssertionError("must not allocate JSON")):
            with self.assertRaisesRegex(ValueError, "raw fallback"):
                check_alert_spool_size(rows)

    def test_oversized_alert_stage_preserves_existing_durable_segments(self):
        with tempfile.TemporaryDirectory() as directory:
            spool = DurableParquetSpool(Path(directory), 0)
            previous = spool.stage("alerts", "2026-10-19", "small", [{"entity_id": "keep"}])
            before = previous.read_bytes()
            shared = "x" * (1024 * 1024)
            rows = [{"informed_entities_json": shared} for _ in range(3421)]
            with self.assertRaisesRegex(ValueError, "raw fallback"):
                spool.stage("alerts", "2026-10-19", "large", rows)
            self.assertEqual(before, previous.read_bytes())
            self.assertEqual([previous], spool.pending_segments())


if __name__ == "__main__":
    unittest.main()
