from __future__ import annotations
import io
import json
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
from config import CollectorConfig, FEED_NAMES
from gtfs_rt_parse import parse_trip_updates
from monitoring import poll_journal_path, utc_iso
from parquet_store import parquet_row_count, write_parquet_atomic
from poll_journal import append_poll_jsonl, recent_partition_files, repair_jsonl_tail
from raw_log import append_record, scan_raw_log


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




class JournalRecoveryTests(ReadinessFixture):
    def test_journal_failure_stays_unhealthy_until_same_feed_append_recovers(self):
        value = self.collector()
        for feed in FEED_NAMES:
            value.process_result(response(feed, sample(feed, int(self.now)), feed, self.now), [])
        with patch("collector.append_poll_event", side_effect=OSError("journal unavailable")):
            value.process_result(response("tripupdates", sample("tripupdates", int(self.now)), "missing", self.now + 10), [])
        value.process_result(response("vehiclepositions", sample("vehiclepositions", int(self.now)), "other-feed", self.now + 11), [])
        status = value._commit_and_write_status("status", [])
        self.assertFalse(status["healthy"])
        self.assertIn("tripupdates:poll_journal_failed", status["reasons"])
        raw = self.data / "raw" / "tripupdates" / f"date={self.date}" / "tripupdates.rawlog"
        self.assertEqual(2, scan_raw_log(raw).complete_records, "journal loss also forces raw fallback")
        value.process_result(response("tripupdates", sample("tripupdates", int(self.now)), "recovered", self.now + 20), [])
        status = value._commit_and_write_status("recovered-status", [])
        self.assertTrue(status["healthy"], status["reasons"])

    def test_torn_final_line_is_detached_and_all_valid_records_survive(self):
        path = self.data / "polls.jsonl"
        append_poll_jsonl(path, {"poll_id": "one"})
        original = path.read_bytes()
        torn = b'{"poll_id":"two","text":"\xf0\x9f'
        with path.open("ab") as handle:
            handle.write(torn)
        recovered = repair_jsonl_tail(path)
        self.assertEqual(torn, recovered.read_bytes())
        self.assertEqual(original, path.read_bytes())
        append_poll_jsonl(path, {"poll_id": "three"})
        self.assertEqual(["one", "three"], [json.loads(line)["poll_id"] for line in path.read_text().splitlines()])
        self.assertIsNone(repair_jsonl_tail(path))

    def test_legacy_complete_json_without_separator_is_preserved(self):
        path = self.data / "polls.jsonl"
        path.write_bytes(b'{"poll_id":"one"}\n{"poll_id":"two"}')
        self.assertIsNone(repair_jsonl_tail(path))
        append_poll_jsonl(path, {"poll_id": "three"})
        self.assertEqual(["one", "two", "three"], [json.loads(line)["poll_id"] for line in path.read_text().splitlines()])

    def test_checkpoint_failure_preserves_complete_append(self):
        path = self.data / "polls.jsonl"
        append_poll_jsonl(path, {"poll_id": "one"})
        with patch("poll_journal.atomic_write_json", side_effect=OSError("checkpoint full")):
            with self.assertRaises(OSError):
                append_poll_jsonl(path, {"poll_id": "two"})
        append_poll_jsonl(path, {"poll_id": "three"})
        self.assertEqual(["one", "two", "three"], [json.loads(line)["poll_id"] for line in path.read_text().splitlines()])

    def test_interior_corruption_is_not_destructively_repaired(self):
        path = self.data / "polls.jsonl"
        append_poll_jsonl(path, {"poll_id": "one"})
        with path.open("ab") as handle:
            handle.write(b'bad interior line\n{"poll_id":"three"}\n')
        original = path.read_bytes()
        with self.assertRaisesRegex(ValueError, "interior corruption"):
            repair_jsonl_tail(path)
        self.assertEqual(original, path.read_bytes())
        self.assertEqual([], list(self.data.glob("*.corrupt-tail-*")))

    def test_restart_after_midnight_repairs_today_and_latest_prior_only(self):
        value = self.collector()
        yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
        older = (datetime.now(timezone.utc).date() - timedelta(days=2)).isoformat()
        for feed in FEED_NAMES:
            for date in (older, yesterday, self.date):
                path = poll_journal_path(self.data, feed, date)
                path.parent.mkdir(parents=True)
                path.write_bytes(b'{"poll_id":"valid"}\n{"torn":')
        value.recover_recent_journal_tails()
        for feed in FEED_NAMES:
            root = self.data / "metadata" / "polls" / feed
            self.assertEqual([yesterday, self.date], [p.parent.name[5:] for p in recent_partition_files(root, "polls.jsonl", self.date)])
            for date in (yesterday, self.date):
                path = poll_journal_path(self.data, feed, date)
                self.assertEqual(b'{"poll_id":"valid"}\n', path.read_bytes())
                self.assertEqual(1, len(list(path.parent.glob("*.corrupt-tail-*"))))
            self.assertTrue(poll_journal_path(self.data, feed, older).read_bytes().endswith(b'{"torn":'))
        self.assertEqual([], recent_partition_files(self.data / "missing", "polls.jsonl", self.date))



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
