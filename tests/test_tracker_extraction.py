import random
import unittest

from dedup import ChangeTracker


class TrackerExtractionTests(unittest.TestCase):
    def test_sparse_rows_match_generator_reference_through_retries_and_heartbeats(self):
        keys, exact, numeric = ("id", "optional_key"), ("state", "optional_flag"), ("arrival", "departure")
        tracker = ChangeTracker(keys, exact, numeric_tolerance_fields=numeric, tolerance=2,
                                required_signal_fields=numeric, heartbeat_seconds=30)
        reference = {}
        rng = random.Random(23)
        for now in range(120):
            rows = []
            for _ in range(8):
                row = {"id": rng.randrange(5), "state": rng.choice(("running", "stopped")),
                       "arrival": rng.choice((None, 100, 101, 102, 103, 130))}
                if rng.randrange(2):
                    row.update(optional_key=None, optional_flag=False, departure=100)
                rows.append(row)
            prospective = dict(reference)
            expected = []
            for row in rows:
                key = tuple(row.get(field) for field in keys)
                categorical = tuple(row.get(field) for field in exact)
                values = tuple(row.get(field) for field in numeric)
                previous = prospective.get(key)
                changed = previous is None
                if previous is not None:
                    old_exact, old_values, emitted_at = previous
                    changed = old_exact != categorical or now - emitted_at >= 30 or any(
                        (a is None) != (b is None) or (a is not None and b is not None and abs(a - b) > 2)
                        for a, b in zip(old_values, values))
                if changed:
                    expected.append(row)
                    prospective[key] = (categorical, values, now)
            selected = tracker.filter(rows, "same-day", now, update=False)
            self.assertEqual(expected, selected)
            if now % 7:  # A failed durable stage must not advance either baseline.
                tracker.commit(selected, "same-day", now)
                reference = prospective
