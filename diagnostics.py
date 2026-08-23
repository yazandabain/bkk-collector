"""Read-only operational diagnostics; never writes to the collection dataset."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import requests

import realcity
from config import FEED_NAMES, feed_urls
from gtfs_rt_parse import PARSERS, parse_feed
from quality_diagnostics import prediction_revision_report, tripupdate_static_join_report


BKK_FIELDS = {
    "vehiclepositions": (
        "bkk_vehicle_model", "bkk_deviated", "bkk_vehicle_type", "bkk_door_open", "bkk_stop_distance",
    ),
    "tripupdates": (
        "bkk_vehicle_model", "bkk_deviated", "bkk_vehicle_type", "bkk_door_open", "bkk_stop_distance",
        "bkk_scheduled_arrival_delay", "bkk_scheduled_arrival_time",
        "bkk_scheduled_arrival_uncertainty", "bkk_scheduled_arrival_scheduled_time",
        "bkk_scheduled_departure_delay", "bkk_scheduled_departure_time",
        "bkk_scheduled_departure_uncertainty", "bkk_scheduled_departure_scheduled_time",
    ),
    "alerts": (
        "bkk_start_text_json", "bkk_end_text_json", "bkk_modified_time", "bkk_route_details_json",
    ),
}


def _has_extension(message: Any, extension: Any) -> bool:
    try:
        return bool(message.HasExtension(extension))
    except (KeyError, TypeError, ValueError):
        return False


def summarize_live_sample(feed_name: str, feed: Any, rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "entities": len(feed.entity),
        "parsed_rows": len(rows),
        "bkk_non_null_parsed_rows": {},
    }
    if feed_name == "vehiclepositions":
        descriptors = [
            entity.vehicle.vehicle
            for entity in feed.entity
            if entity.HasField("vehicle") and entity.vehicle.HasField("vehicle")
        ]
        summary["vehicle_descriptors"] = len(descriptors)
        summary["realcity_vehicle_extension_present"] = sum(
            _has_extension(value, realcity.vehicle) for value in descriptors
        )
    elif feed_name == "tripupdates":
        updates = [entity.trip_update for entity in feed.entity if entity.HasField("trip_update")]
        descriptors = [update.vehicle for update in updates if update.HasField("vehicle")]
        stops = [stop for update in updates for stop in update.stop_time_update]
        summary.update(
            {
                "trip_updates": len(updates),
                "vehicle_descriptors": len(descriptors),
                "realcity_vehicle_extension_present": sum(
                    _has_extension(value, realcity.vehicle) for value in descriptors
                ),
                "stop_time_updates": len(stops),
                "realcity_stop_extension_present": sum(
                    _has_extension(value, realcity.stop_time_update) for value in stops
                ),
            }
        )
    else:
        alerts = [entity.alert for entity in feed.entity if entity.HasField("alert")]
        summary["alerts"] = len(alerts)
        summary["realcity_alert_extension_present"] = sum(
            _has_extension(value, realcity.alert) for value in alerts
        )

    counts = Counter()
    for row in rows:
        for field in BKK_FIELDS[feed_name]:
            value = row.get(field)
            # Empty JSON arrays mean the extension field had no populated
            # values and should not be reported as non-null population.
            if value is not None and value != "[]":
                counts[field] += 1
    summary["bkk_non_null_parsed_rows"] = {
        field: counts[field] for field in BKK_FIELDS[feed_name]
    }
    return summary


def live_schema(api_key: str) -> dict[str, Any]:
    session = requests.Session()
    session.headers.update({"User-Agent": "bkk-collector-live-schema-diagnostic/2"})
    report: dict[str, Any] = {
        "checked_at_unix": time.time(),
        "extension_descriptors": {
            "vehicle": {"number": realcity.vehicle.number, "type": realcity.vehicle.message_type.full_name},
            "stop_time_update": {
                "number": realcity.stop_time_update.number,
                "type": realcity.stop_time_update.message_type.full_name,
            },
            "alert": {"number": realcity.alert.number, "type": realcity.alert.message_type.full_name},
        },
        "feeds": {},
    }
    try:
        for feed_name, url in feed_urls(api_key).items():
            response = session.get(url, timeout=(5, 20))
            response.raise_for_status()
            feed = parse_feed(response.content)
            context = {
                "poll_id": "live-schema-diagnostic",
                "request_started_at": None,
                "response_received_at": None,
            }
            rows = PARSERS[feed_name](feed, context)
            report["feeds"][feed_name] = summarize_live_sample(feed_name, feed, rows)
    finally:
        session.close()
    return report


def _print_human(report: dict[str, Any]) -> None:
    descriptors = report["extension_descriptors"]
    print("Registered realCity extensions:")
    for name, value in descriptors.items():
        print(f"  {name}: field {value['number']}, type {value['type']}")
    for feed_name in FEED_NAMES:
        summary = report["feeds"][feed_name]
        print(f"\n{feed_name}:")
        for key, value in summary.items():
            if key == "bkk_non_null_parsed_rows":
                print("  parsed-row non-null BKK fields:")
                for field, count in value.items():
                    print(f"    {field}: {count}")
            else:
                print(f"  {key}: {value}")


def _print_revision_report(report: dict[str, Any]) -> None:
    print(f"raw log: {report['raw_path']}")
    print(f"snapshots analyzed: {report['snapshots_analyzed']}")
    print(f"note: {report['snapshot_interval_note']}")
    for event_name in ("arrival", "departure"):
        values = report[event_name]
        print(f"\n{event_name} prediction revisions:")
        print(f"  comparisons: {values['comparisons']}")
        for percentile in ("p50_seconds", "p75_seconds", "p90_seconds", "p95_seconds", "p99_seconds", "max_seconds"):
            print(f"  {percentile}: {values[percentile]}")
        for threshold, result in values["above_threshold"].items():
            percent = result["percent"]
            rendered = "n/a" if percent is None else f"{percent:.2f}%"
            print(f"  >{threshold}s: {result['count']} ({rendered})")
        print("  most common seconds:")
        for seconds, count in values["most_common_seconds"]:
            print(f"    {seconds}: {count}")


def _print_join_report(report: dict[str, Any]) -> None:
    print(f"date: {report['date']}")
    print(f"snapshots analyzed: {report['snapshots_analyzed']}")
    print("static versions used:")
    for version in report["static_versions_used"]:
        print(
            f"  {version['sha256']} {version['version_path']} "
            f"({version['applicability_confidence']}, {version['snapshots']} snapshots)"
        )
    overall = report["overall"]
    print("overall:")
    print(f"  stop updates: {overall['stop_updates']}")
    print(f"  trip ID match: {overall['trip_id_matched']} ({_percent(overall['trip_id_match_rate'])})")
    print(f"  exact stop match: {overall['exact_stop_matched']} ({_percent(overall['exact_stop_match_rate'])})")
    print("by mode:")
    for mode, values in report["by_mode"].items():
        print(
            f"  {mode}: {values['exact_stop_matched']}/{values['stop_updates']} "
            f"({_percent(values['exact_stop_match_rate'])})"
        )
    if report["unmatched_realtime_route_ids"]:
        print("unmatched/external realtime route IDs (valid observations, not malformed rows):")
        for route_id, count in report["unmatched_realtime_route_ids"].items():
            print(f"  {route_id}: {count}")
    print("quality warnings:")
    if report["quality_warnings"]:
        for warning in report["quality_warnings"]:
            print(f"  {warning}")
    else:
        print("  none")


def _percent(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2%}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    live = subparsers.add_parser("live-schema", help="aggregate-only live realCity schema probe")
    live.add_argument("--json", action="store_true", help="print aggregate JSON")
    revisions = subparsers.add_parser(
        "prediction-revisions", help="measure absolute TripUpdate prediction revisions in a raw log"
    )
    revisions.add_argument("--date", required=True, help="UTC date (YYYY-MM-DD)")
    revisions.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "/data"))
    revisions.add_argument("--start-hour", type=int, default=0)
    revisions.add_argument("--end-hour", type=int, default=24)
    revisions.add_argument("--max-snapshots", type=int)
    revisions.add_argument("--json", action="store_true")
    static_join = subparsers.add_parser(
        "static-join", help="report TripUpdate joins to the static version observed at each timestamp"
    )
    static_join.add_argument("--date", required=True, help="UTC date (YYYY-MM-DD)")
    static_join.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "/data"))
    static_join.add_argument("--warning-rate", type=float, default=0.90)
    static_join.add_argument("--max-snapshots", type=int)
    static_join.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "live-schema":
        api_key = os.environ.get("BKK_API_KEY", "").strip()
        if not api_key:
            print("BKK_API_KEY is not set; run this read-only command on the VPS with its normal environment.", file=sys.stderr)
            return 2
        try:
            report = live_schema(api_key)
        except Exception as error:
            # Never stringify requests errors: their request URL can contain
            # the API key. Type-only output is sufficient operationally.
            print(f"live schema probe failed: {type(error).__name__}", file=sys.stderr)
            return 1
        if args.json:
            print(json.dumps(report, indent=2, sort_keys=True))
        else:
            _print_human(report)
        return 0
    if args.command == "prediction-revisions":
        raw_path = Path(args.data_dir) / "raw" / "tripupdates" / f"date={args.date}" / "tripupdates.rawlog"
        try:
            report = prediction_revision_report(
                raw_path,
                start_hour=args.start_hour,
                end_hour=args.end_hour,
                max_snapshots=args.max_snapshots,
            )
        except Exception as error:
            print(f"prediction revision diagnostic failed: {type(error).__name__}: {error}", file=sys.stderr)
            return 1
        if args.json:
            print(json.dumps(report, indent=2, sort_keys=True))
        else:
            _print_revision_report(report)
        return 0
    if args.command == "static-join":
        try:
            report = tripupdate_static_join_report(
                Path(args.data_dir),
                args.date,
                warning_rate=args.warning_rate,
                max_snapshots=args.max_snapshots,
            )
        except Exception as error:
            print(f"static join diagnostic failed: {type(error).__name__}: {error}", file=sys.stderr)
            return 1
        if args.json:
            print(json.dumps(report, indent=2, sort_keys=True))
        else:
            _print_join_report(report)
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
