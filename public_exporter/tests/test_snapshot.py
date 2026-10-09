import gzip
import hashlib
import json
import tempfile
import tracemalloc
import unittest
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from google.transit import gtfs_realtime_pb2 as pb

from bkk_collector.storage.atomic_io import atomic_write_json
from public_exporter.__main__ import PublishError, main, publish
from public_exporter.snapshot import FEEDS, SnapshotBuilder, health, statistics
from public_exporter.source import HEADER, LatestVehicleFrame, RouteCatalog, mode_for
from bkk_collector.storage.raw_log import append_record


NOW = datetime(2026, 10, 2, 12, tzinfo=timezone.utc).timestamp()


def vehicle_feed(route="10", latitude=47.5, longitude=19.05):
    feed = pb.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(NOW)
    entity = feed.entity.add(id="private-entity")
    vehicle = entity.vehicle
    vehicle.trip.route_id = route
    vehicle.vehicle.id = "private-vehicle"
    vehicle.vehicle.license_plate = "NOT-PUBLIC"
    vehicle.position.latitude, vehicle.position.longitude = latitude, longitude
    vehicle.timestamp = int(NOW - 3)
    return feed.SerializeToString()


class PublicSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.path = self.root / "raw/date=2026-10-02/vehiclepositions.rawlog"

    def tearDown(self):
        self.directory.cleanup()

    def append(self, timestamp=NOW, payload=None, path=None):
        append_record(path or self.path, timestamp, payload or vehicle_feed())

    def catalog(self):
        path = self.root / "static/versions/example.zip"
        path.parent.mkdir(parents=True)
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("routes.txt", "route_id,route_short_name,route_type,route_color\n10,10,3,009fe3\n")
            archive.writestr("stop_times.txt", "NEVER READ THIS")
        atomic_write_json(self.root / "static/state.json", {
            "latest_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "latest_version_path": "versions/example.zip",
        })
        return path

    def test_only_latest_committed_frame_is_decompressed_and_cache_is_not_new_observation(self):
        for offset in range(20):
            self.append(NOW + offset)
        before = self.path.read_bytes()
        reader = LatestVehicleFrame(self.root / "raw")
        with patch("public_exporter.source.gzip.GzipFile", wraps=gzip.GzipFile) as decoder:
            first = reader.read(date(2026, 10, 2))
            self.assertEqual(decoder.call_count, 1)
            self.assertEqual(reader.read(date(2026, 10, 2)), first)
            self.assertEqual(decoder.call_count, 1)
            self.append(NOW + 30)
            self.assertEqual(reader.read(date(2026, 10, 2))[0], NOW + 30)
            self.assertEqual(decoder.call_count, 2)
        self.assertTrue(self.path.read_bytes().startswith(before))

    def test_uncommitted_crash_tail_is_untouched(self):
        self.append()
        with self.path.open("ab") as handle:
            handle.write(b"torn-tail")
        before = self.path.read_bytes()
        self.assertEqual(LatestVehicleFrame(self.root / "raw").read(date(2026, 10, 2))[0], NOW)
        self.assertEqual(before, self.path.read_bytes())

    def test_invalid_checkpoint_refuses_instead_of_inventing_freshness(self):
        self.append()
        atomic_write_json(self.path.with_name(self.path.name + ".checkpoint.json"), {"version": 1, "valid_bytes": self.path.stat().st_size + 1})
        with self.assertRaises(ValueError):
            LatestVehicleFrame(self.root / "raw").read(date(2026, 10, 2))

    def test_midnight_and_inode_replacement_reset_the_cursor(self):
        yesterday = self.root / "raw/date=2026-10-01/vehiclepositions.rawlog"
        self.append(NOW - 3600, path=yesterday)
        reader = LatestVehicleFrame(self.root / "raw")
        self.assertEqual(reader.read(date(2026, 10, 2))[0], NOW - 3600)
        self.append()
        self.assertEqual(reader.read(date(2026, 10, 2))[0], NOW)
        replacement = self.path.with_name("replacement")
        append_record(replacement, NOW + 10, vehicle_feed())
        replacement.replace(self.path)
        replacement.with_name(replacement.name + ".checkpoint.json").replace(self.path.with_name(self.path.name + ".checkpoint.json"))
        self.assertEqual(reader.read(date(2026, 10, 2))[0], NOW + 10)

    def test_decompression_bomb_is_bounded(self):
        self.append(payload=b"x" * (8 * 1024 * 1024 + 1))
        with self.assertRaises(ValueError):
            LatestVehicleFrame(self.root / "raw").read(date(2026, 10, 2))

    def test_public_projection_is_closed_and_excludes_operational_secrets(self):
        self.append()
        self.catalog()
        atomic_write_json(self.root / "health/status.json", {
            "updated_timestamp": NOW, "healthy": False, "reasons": ["tripupdates:http_failed", "private:secret"],
            "disk_free_bytes": 12345, "last_error": "TOKEN=secret", "feeds": {
                "tripupdates": {"last_error": "private/path?key=secret", "header_timestamp": NOW, "last_success_timestamp": NOW},
            },
        })
        snapshot = SnapshotBuilder(self.root).build(NOW)
        text = json.dumps(snapshot)
        for sensitive in ("NOT-PUBLIC", "private-vehicle", "private-entity", "TOKEN", "private/path", "disk_free_bytes", "secret"):
            self.assertNotIn(sensitive, text)
        feature = snapshot["vehicles"]["features"][0]
        self.assertEqual(feature["properties"]["mode"], "bus")
        self.assertEqual(feature["properties"]["route_label"], "10")
        self.assertEqual(snapshot["health"]["feeds"]["tripupdates"]["state"], "degraded")
        self.assertEqual(set(snapshot), {"schema_version", "generated_at", "vehicles", "health", "statistics", "public_layer_issues"})

    def test_unknown_external_routes_are_preserved_without_static_join(self):
        self.append(payload=vehicle_feed("S70"))
        result = SnapshotBuilder(self.root).build(NOW)
        self.assertEqual(result["vehicles"]["features"][0]["properties"]["route_label"], "S70")
        self.assertEqual(result["vehicles"]["features"][0]["properties"]["mode"], "other")

    def test_invalid_coordinate_and_deleted_entity_are_not_plotted(self):
        self.append(payload=vehicle_feed(latitude=float("nan")))
        result = SnapshotBuilder(self.root).build(NOW)
        self.assertEqual(result["vehicles"]["features"], [])
        self.assertEqual(result["vehicles"]["omitted_records"], 1)

    def test_stale_public_metadata_is_unknown_but_alert_advisory_is_not_failure(self):
        atomic_write_json(self.root / "health/status.json", {
            "updated_timestamp": NOW, "healthy": True, "warnings": ["alerts:source_timestamp_unchanged_warning"],
            "feeds": {"alerts": {"header_timestamp": NOW - 86400, "last_success_timestamp": NOW}},
        })
        self.assertEqual(health(self.root, NOW)["feeds"]["alerts"]["state"], "healthy")
        self.assertEqual(health(self.root, NOW + 181)["state"], "unknown")

    def test_private_failure_names_still_project_generic_feed_failure(self):
        atomic_write_json(self.root / "health/status.json", {
            "updated_timestamp": NOW, "healthy": False,
            "reasons": ["tripupdates:private-diagnostic/path?key=secret"],
            "feeds": {"tripupdates": {"change_tracking_ok": False}},
        })
        result = health(self.root, NOW)
        self.assertEqual(result["feeds"]["tripupdates"]["state"], "degraded")
        self.assertEqual(result["feeds"]["tripupdates"]["issues"], ["change_tracker_failed"])
        self.assertEqual(result["feeds"]["vehiclepositions"]["state"], "unknown")
        self.assertNotIn("private-diagnostic", json.dumps(result))
        self.assertNotIn("secret", json.dumps(result))

    def test_static_catalog_checks_hash_and_avoids_stop_times_and_rehashing(self):
        self.catalog()
        catalog = RouteCatalog(self.root / "static")
        original_open = zipfile.ZipFile.open
        with patch("zipfile.ZipFile.open", autospec=True, side_effect=original_open) as opened:
            self.assertEqual(catalog.read()["10"]["mode"], "bus")
            self.assertEqual([call.args[1].filename for call in opened.call_args_list], ["routes.txt"])
        with patch("zipfile.ZipFile", side_effect=AssertionError("should be cached")):
            self.assertEqual(catalog.read()["10"]["label"], "10")
        atomic_write_json(self.root / "static/state.json", {"latest_sha256": "0" * 64, "latest_version_path": "versions/example.zip"})
        with self.assertRaises(ValueError):
            catalog.read()

    def test_static_traversal_and_extended_modes(self):
        atomic_write_json(self.root / "static/state.json", {"latest_sha256": "a" * 64, "latest_version_path": "../private.zip"})
        with self.assertRaises(ValueError):
            RouteCatalog(self.root / "static").read()
        self.assertEqual([mode_for(value) for value in ("109", "800", "11", "1000", "3", "1")], ["rail", "trolleybus", "trolleybus", "ferry", "bus", "metro"])

    def test_statistics_do_not_call_missing_legacy_evidence_uptime_or_complete(self):
        feeds = {name: {"attempted_polls": 10, "expected_polls": 20, "raw_snapshots": 2, "parquet_rows": 100,
                        "first_success_at": "2026-10-01T12:00:00+00:00"} for name in FEEDS}
        atomic_write_json(self.root / "manifests/date=2026-10-01.json", {"date": "2026-10-01", "quality_ok": False, "feeds": feeds})
        atomic_write_json(self.root / "manifests/date=2026-08-18.json", {"date": "2026-08-18", "complete": True, "feeds": {}})
        atomic_write_json(self.root / "receipts/date=2026-10-01.json", {"version": 2, "date": "2026-10-01", "remote_verified": True})
        atomic_write_json(self.root / "receipts/date=2026-08-18.json", {"version": 1, "date": "2026-08-18", "remote_verified": True})
        result = statistics(self.root, NOW)
        self.assertEqual(result["evidenced_days"], 1)
        self.assertEqual(result["verified_days"], 1)
        self.assertEqual(result["polls_recorded"], 30)
        self.assertEqual(result["event_rows"], 300)
        self.assertIsNone(result["latest_day"]["feeds"]["tripupdates"]["data_polls"])
        self.assertEqual(result["latest_day"]["quality"], "flagged")

    def test_publisher_is_bounded_no_redirects_and_no_remote_error_disclosure(self):
        session = Mock()
        session.put.return_value.status_code = 302
        with self.assertRaisesRegex(RuntimeError, "snapshot publish refused"):
            publish(session, "https://example.invalid/api/publish", "secret", {"schema_version": 1})
        self.assertEqual(session.put.call_args.kwargs["timeout"], (3, 7))
        self.assertFalse(session.put.call_args.kwargs["allow_redirects"])

    def test_publish_error_reports_only_http_status_without_reading_remote_body(self):
        session = Mock()
        session.put.return_value.status_code = 500
        session.put.return_value.text = "remote private/path?token=secret"
        with self.assertRaises(PublishError) as caught:
            publish(session, "https://example.invalid/api/publish", "secret", {"schema_version": 1})
        self.assertEqual(caught.exception.status_code, 500)
        self.assertEqual(str(caught.exception), "snapshot publish refused (HTTP 500)")
        session.put.return_value.json.assert_not_called()

    def test_failed_publish_logs_status_and_marks_public_layer_unhealthy_without_secrets(self):
        output = self.root / "output"
        with patch("sys.argv", ["public-exporter", "--once", "--output", str(output)]), \
             patch.dict("os.environ", {"PUBLIC_PUBLISH_URL": "https://example.invalid/api/publish", "PUBLIC_PUBLISH_TOKEN": "secret-" * 8}), \
             patch("public_exporter.__main__.signal.signal"), \
             patch("public_exporter.__main__.SnapshotBuilder") as builder, \
             patch("public_exporter.__main__.publish", side_effect=PublishError(500)), \
             self.assertLogs(level="ERROR") as captured:
            builder.return_value.build.return_value = {"schema_version": 1}
            self.assertEqual(main(), 1)
        self.assertEqual(captured.output, ["ERROR:root:Public snapshot failed (HTTP 500); retrying next tick"])
        self.assertFalse(json.loads((output / "status.json").read_text())["healthy"])

    def test_network_error_log_does_not_disclose_endpoint_or_token(self):
        with patch("sys.argv", ["public-exporter", "--once", "--output", str(self.root / "output")]), \
             patch.dict("os.environ", {"PUBLIC_PUBLISH_URL": "https://example.invalid/api/publish", "PUBLIC_PUBLISH_TOKEN": "secret-" * 8}), \
             patch("public_exporter.__main__.signal.signal"), \
             patch("public_exporter.__main__.SnapshotBuilder") as builder, \
             patch("public_exporter.__main__.publish", side_effect=ConnectionError("private/path?token=secret")), \
             self.assertLogs(level="ERROR") as captured:
            builder.return_value.build.return_value = {"schema_version": 1}
            self.assertEqual(main(), 1)
        self.assertEqual(captured.output, ["ERROR:root:Public snapshot failed (ConnectionError); retrying next tick"])

    def test_statistics_memory_does_not_accumulate_private_artifact_lists(self):
        for day in range(1, 31):
            atomic_write_json(self.root / f"manifests/date=2026-09-{day:02d}.json", {})
        def load(path):
            return {"date": path.stem.removeprefix("date="), "feeds": {name: {"attempted_polls": 1} for name in FEEDS},
                    "artifacts": [{"irrelevant_metadata": "x" * 1024 * 1024}]}
        tracemalloc.start()
        try:
            with patch("public_exporter.snapshot.load_optional", side_effect=load):
                result = statistics(self.root, NOW)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(result["evidenced_days"], 30)
        self.assertLess(peak, 8 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
