from __future__ import annotations

import gzip
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
from google.transit import gtfs_realtime_pb2 as pb

from bkk_collector.realtime.gtfs_rt_parse import parse_alerts
from bkk_collector.storage.atomic_io import atomic_write_bytes
from bkk_collector.storage.parquet_compact import compact_partition
from bkk_collector.storage.parquet_store import (
    DurableParquetSpool, check_alert_spool_size, parquet_schema, rows_to_table, write_parquet_atomic,
)


def wide_alert_rows():
    feed = pb.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add(id="wide-alert")
    entity.alert.header_text.translation.add(text='Árvíz – "all stops"', language="hu")
    for index in range(3421):
        entity.alert.informed_entity.add(stop_id=f"S{index:05d}")
    return parse_alerts(feed, "2026-10-19T06:00:00+00:00")


def exercise_wide_pipeline():
    """Runs in a memory-limited child: the equivalent plain JSON exceeds 3 GiB."""
    import resource

    rows = wide_alert_rows()
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        spool = DurableParquetSpool(root, 0)
        segment = spool.stage("alerts", "2026-10-18", "wide", rows)
        decoded = spool._read_segment(segment)
        assert decoded == rows
        assert decoded[0]["informed_entities_json"] is decoded[-1]["informed_entities_json"]
        with gzip.open(segment, "rt") as handle:
            assert json.load(handle)["version"] == 2
        result = spool.flush(force=True)
        assert result.ok, result.errors
        assert result.rows_written == 3421
        assert not spool.pending_segments()
        path = result.files_written[0]
        assert pq.ParquetFile(path).schema_arrow.equals(parquet_schema("alerts"), check_metadata=True)
        projected = pq.ParquetFile(path).read(columns=["affected_stop_id"]).to_pylist()
        assert projected == [{"affected_stop_id": row["affected_stop_id"]} for row in rows]
        dictionary_read = pq.ParquetFile(path, read_dictionary=["informed_entities_json"]).read()
        assert dictionary_read.nbytes < 16 * 1024 * 1024
        assert dictionary_read["informed_entities_json"].chunk(0).dictionary[0].as_py() == rows[0]["informed_entities_json"]
        # A pre-fix, ordinary string-schema file must compact with new files.
        legacy = path.with_name("legacy.parquet")
        pq.write_table(rows_to_table("alerts", [{"entity_id": "old"}]), legacy)
        compacted = compact_partition(root, "alerts", "2026-10-18", min_files=2)
        assert compacted is not None
        assert pq.ParquetFile(compacted).metadata.num_rows == 3422
        assert pq.ParquetFile(compacted).schema_arrow.equals(parquet_schema("alerts").remove_metadata(), check_metadata=True)
        count = sum(batch.num_rows for batch in pq.ParquetFile(compacted, read_dictionary=["informed_entities_json"]).iter_batches())
        assert count == 3422
        return {"rows": 3421, "compacted_rows": count, "parquet_bytes": compacted.stat().st_size,
                "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}


class AlertMemoryTests(unittest.TestCase):
    def test_expansion_guard_checks_repeated_string_bytes_without_serializing(self):
        shared = "x" * (1024 * 1024)
        rows = [{"entity_id": str(index), "informed_entities_json": shared} for index in range(3421)]
        with patch("bkk_collector.storage.parquet_store.json.dumps", side_effect=AssertionError("must not allocate JSON")):
            with self.assertRaisesRegex(ValueError, "raw fallback"):
                check_alert_spool_size(rows)

    def test_failed_wide_alert_stage_preserves_input_and_existing_durable_segments(self):
        with tempfile.TemporaryDirectory() as directory:
            spool = DurableParquetSpool(Path(directory), 0)
            previous = spool.stage("alerts", "2026-10-19", "small", [{"entity_id": "keep"}])
            before = previous.read_bytes()
            shared = "x" * (1024 * 1024)
            rows = [{"informed_entities_json": shared} for _ in range(3421)]
            with patch("bkk_collector.storage.parquet_store.atomic_write_bytes", side_effect=OSError("disk full")):
                with self.assertRaisesRegex(OSError, "disk full"):
                    spool.stage("alerts", "2026-10-19", "large", rows)
            self.assertEqual(before, previous.read_bytes())
            self.assertEqual([previous], spool.pending_segments())
            self.assertTrue(all(row["informed_entities_json"] is shared for row in rows))

    @unittest.skipUnless(sys.platform == "linux", "uses the production Linux address-space limit")
    def test_wide_alert_spool_parquet_and_compaction_under_one_gib(self):
        program = (
            # Bound thread stacks too, independent of the CI host's CPU count.
            "import resource, json, pyarrow as pa; pa.set_cpu_count(2); pa.set_io_thread_count(2); "
            "resource.setrlimit(resource.RLIMIT_AS, (1024**3, 1024**3)); "
            "from tests.test_alert_memory import exercise_wide_pipeline; "
            "print(json.dumps(exercise_wide_pipeline()))"
        )
        result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True, timeout=60)
        self.assertEqual(0, result.returncode, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(3422, report["compacted_rows"])
        self.assertLess(report["peak_rss_kib"], 512 * 1024)

    def test_legacy_v1_spools_still_recover(self):
        with tempfile.TemporaryDirectory() as directory:
            spool = DurableParquetSpool(Path(directory), 0)
            path = spool.spool_dir / "alerts/date=2026-10-18/batch-old.json.gz"
            rows = [{"entity_id": "old", "header_text": "Árvíz", "cause": None}]
            atomic_write_bytes(path, gzip.compress(json.dumps({"version": 1, "rows": rows}).encode()))
            self.assertEqual(rows, spool._read_segment(path))
            result = spool.flush(force=True)
            self.assertTrue(result.ok, result.errors)
            self.assertEqual(1, result.rows_written)

    def test_invalid_shared_references_refuse_without_consuming_sources(self):
        for strings, reference in [(["x"], -1), (["x"], 1), (["x"], True), (["x"], 0.5), (None, 0), ([42], 0)]:
            with self.subTest(strings=strings, reference=reference), tempfile.TemporaryDirectory() as directory:
                spool = DurableParquetSpool(Path(directory), 0)
                path = spool.spool_dir / "alerts/date=2026-10-18/batch-corrupt.json.gz"
                content = gzip.compress(json.dumps({"version": 2, "shared_strings": strings,
                    "rows": [{"header_text": {"$shared_string": reference}}]}).encode())
                atomic_write_bytes(path, content)
                result = spool.flush(force=True)
                self.assertFalse(result.ok)
                self.assertEqual(content, path.read_bytes())
                self.assertEqual([], result.files_written)

    def test_large_dictionary_value_above_default_parquet_page_limit_stays_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            large = "ő" * (1024 * 1024)
            path = Path(directory) / "wide.parquet"
            rows = [{"entity_id": str(index), "informed_entities_json": large} for index in range(64)]
            write_parquet_atomic("alerts", rows, path)
            table = pq.ParquetFile(path, read_dictionary=["informed_entities_json"]).read()
            self.assertEqual(64, table.num_rows)
            self.assertLess(table.nbytes, 4 * 1024 * 1024)
            self.assertEqual(large, table["informed_entities_json"].chunk(0).dictionary[0].as_py())

    def test_restart_after_wide_parquet_publication_recovers_shared_spool(self):
        from bkk_collector.storage import parquet_store
        with tempfile.TemporaryDirectory() as directory:
            spool = DurableParquetSpool(Path(directory), 0)
            source = spool.stage("alerts", "2026-10-18", "wide", wide_alert_rows())
            writer = parquet_store.write_parquet_atomic
            def interrupted(*args):
                writer(*args)
                raise SystemExit("crash after durable publication")
            with patch.object(parquet_store, "write_parquet_atomic", side_effect=interrupted):
                with self.assertRaises(SystemExit):
                    spool.flush(force=True)
            self.assertTrue(source.exists())
            recovered = DurableParquetSpool(Path(directory), 0)
            self.assertTrue(recovered.flush(force=True).ok)
            self.assertFalse(recovered.pending_segments())
            outputs = list((Path(directory) / "parquet/alerts/date=2026-10-18").glob("*.parquet"))
            self.assertEqual(1, len(outputs))
            self.assertEqual(3421, pq.ParquetFile(outputs[0]).metadata.num_rows)


if __name__ == "__main__":
    unittest.main()
