from __future__ import annotations
import io
import logging
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from google.transit import gtfs_realtime_pb2 as pb
from urllib3.exceptions import NewConnectionError
from collector import Collector, FetchResult, _new_session
from config import CollectorConfig
from gtfs_rt_parse import parse_trip_updates
from monitoring import utc_iso


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



if __name__ == "__main__":
    unittest.main()
