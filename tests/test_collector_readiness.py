from __future__ import annotations
import io
import logging
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from google.transit import gtfs_realtime_pb2 as pb
from urllib3.exceptions import NewConnectionError
import rebuild_parquet
from atomic_io import sha256_file
from collector import Collector, FetchResult, _new_session
from config import CollectorConfig
from gtfs_rt_parse import parse_trip_updates
from monitoring import utc_iso
from parquet_store import parquet_row_count, write_parquet_atomic
from raw_log import append_record


def sample(feed_name: str, timestamp: int, sequences=(1, 2)) -> pb.FeedMessage:
    feed = pb.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = timestamp
    if feed_name == "vehiclepositions":
        vehicle = feed.entity.add(id="vehicle").vehicle
        vehicle.position.latitude, vehicle.position.longitude = 47.5, 19.0
        vehicle.timestamp = timestamp
    elif feed_name == "tripupdates":
        trip = feed.entity.add(id="update").trip_update
        trip.trip.trip_id, trip.trip.start_date, trip.trip.start_time = "trip", "20261001", "08:00:00"
        for sequence in sequences:
            stop = trip.stop_time_update.add(stop_sequence=sequence, stop_id="REPEATED")
            stop.arrival.time = timestamp + 300
    elif feed_name == "alerts":
        feed.entity.add(id="alert").alert.header_text.translation.add(text="Notice", language="en")
    return feed



def response(feed_name: str, feed: pb.FeedMessage | None, poll_id: str, timestamp: float) -> FetchResult:
    return FetchResult(feed_name, poll_id, utc_iso(timestamp - .01), timestamp - .01,
                       utc_iso(timestamp), timestamp, 10.0, 200 if feed is not None else None,
                       feed.SerializeToString() if feed is not None else None,
                       None if feed is not None else "ConnectionError: unavailable")



def wait_until(predicate, timeout=5.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        threading.Event().wait(.01)
    raise AssertionError("condition was not satisfied before timeout")



class ReadinessFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.data = Path(self.temporary.name)
        self.now = time.time()
        self.date = datetime.fromtimestamp(self.now, timezone.utc).date().isoformat()

    def collector(self, **kwargs):
        value = Collector(CollectorConfig(api_key="synthetic-key", data_dir=self.data, **kwargs))
        self.addCleanup(lambda: [session.close() for session in value.sessions.values()])
        self.addCleanup(lambda: value.executor.shutdown(wait=True))
        return value

    def context(self, poll: str):
        return {"poll_id": poll, "request_started_at": utc_iso(self.now - .01),
                "response_received_at": utc_iso(self.now), "feed_header_timestamp": int(self.now),
                "feed_incrementality": 0}

    def rows(self, sequences=(1, 2)):
        return parse_trip_updates(sample("tripupdates", int(self.now), sequences), self.context("source"))




class RetryRedactionTests(ReadinessFixture):
    def test_actual_urllib3_connection_retry_warning_does_not_leak_key(self):
        canary = "not-a-real-BKK-key-CANARY"
        session = _new_session(CollectorConfig(api_key=canary, data_dir=self.data, http_connect_retries=1))
        session.trust_env = False
        self.addCleanup(session.close)
        captured = io.StringIO()
        handler = logging.StreamHandler(captured)
        logger = logging.getLogger("urllib3.connectionpool")
        logger.addHandler(handler)
        self.addCleanup(lambda: logger.removeHandler(handler))
        with patch("urllib3.connectionpool.HTTPConnectionPool._make_request", side_effect=NewConnectionError(None, "offline")):
            with self.assertRaises(Exception):
                session.get("https://example.invalid/feed?key=" + canary, timeout=(.1, .1))
        text = captured.getvalue()
        self.assertIn("Retrying", text, "exercise the library's real warning path")
        self.assertIn("key=<redacted>", text)
        self.assertNotIn(canary, text)

    def test_debug_and_exception_logging_are_also_redacted(self):
        canary = "not-a-real-key-DEBUG-CANARY"
        session = _new_session(CollectorConfig(api_key=canary, data_dir=self.data))
        self.addCleanup(session.close)
        logger = logging.getLogger("urllib3.connectionpool")
        previous_level = logger.level
        logger.setLevel(logging.DEBUG)
        self.addCleanup(lambda: logger.setLevel(previous_level))
        with self.assertLogs(logger, logging.DEBUG) as captured:
            logger.debug("request %s", "/feed?key=" + canary)
            try:
                raise IOError("failed /feed?key=" + canary)
            except IOError:
                logger.exception("failed request")
        self.assertNotIn(canary, "\n".join(captured.output))
        self.assertIn("<redacted>", "\n".join(captured.output))



class RebuildSafetyTests(ReadinessFixture):
    def setUp(self):
        super().setUp()
        self.date = "2026-09-24"
        for name, value in (("DATA_DIR", self.data), ("RAW_DIR", self.data / "raw"), ("PARQUET_DIR", self.data / "parquet")):
            patcher = patch.object(rebuild_parquet, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.raw = self.data / "raw" / "vehiclepositions" / f"date={self.date}" / "vehiclepositions.rawlog"
        for index in range(3):
            append_record(self.raw, self.now + index * 10, sample("vehiclepositions", int(self.now)).SerializeToString())
        self.original = self.data / "parquet" / "vehiclepositions" / f"date={self.date}" / "part-original.parquet"
        write_parquet_atomic("vehiclepositions", [{"entity_id": "original"}], self.original)

    def test_write_error_cannot_be_dismissed_as_parse_error_or_replace_original(self):
        raw_hash, original_hash = sha256_file(self.raw), sha256_file(self.original)
        real_write = write_parquet_atomic
        for allow in (False, True):
            calls = []

            def fail_late(feed, rows, path):
                calls.append(path)
                if len(calls) == 2:
                    raise OSError("disk full on second output")
                return real_write(feed, rows, path)

            with self.subTest(allow_parse_errors=allow), patch("rebuild_parquet.write_parquet_atomic", side_effect=fail_late):
                with self.assertRaisesRegex(OSError, "second output"):
                    rebuild_parquet.rebuild_one("vehiclepositions", self.date, chunk_rows=1, allow_parse_errors=allow)
            self.assertEqual(raw_hash, sha256_file(self.raw))
            self.assertEqual(original_hash, sha256_file(self.original))
            self.assertEqual([], list((self.data / "parquet" / ".rebuild-staging").glob("*/date=*")))

    def test_explicitly_allowed_genuine_parse_error_still_preserves_previous_partition(self):
        append_record(self.raw, self.now + 30, b"not valid protobuf")
        with self.assertRaises(RuntimeError):
            rebuild_parquet.rebuild_one("vehiclepositions", self.date, chunk_rows=1)
        result = rebuild_parquet.rebuild_one("vehiclepositions", self.date, chunk_rows=1, allow_parse_errors=True)
        self.assertEqual(1, result["parse_failures"])
        self.assertEqual(3, result["rows"])
        previous = Path(result["previous_partition"])
        self.assertTrue((previous / "part-original.parquet").is_file())
        self.assertEqual(3, sum(map(parquet_row_count, self.original.parent.glob("*.parquet"))))

    def test_cli_returns_failure_and_continues_remaining_dates(self):
        result = {"raw_records": 1, "rows": 1, "previous_partition": None}
        with patch("sys.argv", ["rebuild_parquet.py", "--feed", "vehiclepositions"]), \
                patch("rebuild_parquet.all_dates_for", return_value=["2026-09-23", "2026-09-24"]), \
                patch("rebuild_parquet.rebuild_one", side_effect=[OSError("disk full"), result]) as run, \
                patch("sys.stdout", io.StringIO()):
            self.assertEqual(1, rebuild_parquet.main())
            self.assertEqual(2, run.call_count)

    def test_cli_default_skips_current_and_future_dates(self):
        tomorrow = (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat()
        with patch("sys.argv", ["rebuild_parquet.py", "--feed", "vehiclepositions"]), \
                patch("rebuild_parquet.all_dates_for", return_value=[self.date, utc_iso()[:10], tomorrow]), \
                patch("rebuild_parquet.rebuild_one", return_value={"raw_records": 1, "rows": 1, "previous_partition": None}) as run, \
                patch("sys.stdout", io.StringIO()):
            self.assertEqual(0, rebuild_parquet.main())
            self.assertEqual(1, run.call_count)
            self.assertEqual(self.date, run.call_args.args[1])



if __name__ == "__main__":
    unittest.main()
