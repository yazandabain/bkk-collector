"""
Parses BKK's three GTFS-Realtime feeds (VehiclePositions, TripUpdates, Alerts)
from raw protobuf bytes into flat lists of dicts, ready for a Parquet writer.

This is deliberately the ONLY place that knows about the protobuf schema.
Both collector.py (live polling) and rebuild_parquet.py (rebuilding Parquet
from archived raw files) import this module, so a schema fix only has to
happen once.

Reference: https://opendata.bkk.hu/docs/gtfs-realtime.proto
           https://developers.google.com/transit/gtfs-realtime/reference
"""

from __future__ import annotations

from google.transit import gtfs_realtime_pb2 as pb


def parse_feed(raw_bytes: bytes) -> pb.FeedMessage:
    """Parse raw protobuf bytes into a FeedMessage. Raises on malformed input --
    callers are responsible for catching and logging, never for silently dropping."""
    feed = pb.FeedMessage()
    feed.ParseFromString(raw_bytes)
    return feed


def _trip_desc(trip) -> dict:
    return {
        "trip_id": trip.trip_id or None,
        "route_id": trip.route_id or None,
        "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
        "start_time": trip.start_time or None,
        "start_date": trip.start_date or None,
        "schedule_relationship": trip.schedule_relationship if trip.HasField("schedule_relationship") else None,
    }


def parse_vehicle_positions(feed: pb.FeedMessage, fetched_at: str) -> list[dict]:
    """One row per vehicle. fetched_at is the ISO-8601 UTC timestamp of the poll,
    NOT the feed's own timestamp field -- we keep both."""
    rows = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        row = {
            "fetched_at": fetched_at,
            "entity_id": entity.id or None,
            "vehicle_timestamp": v.timestamp if v.HasField("timestamp") else None,
            "vehicle_id": v.vehicle.id if v.HasField("vehicle") else None,
            "vehicle_label": v.vehicle.label if v.HasField("vehicle") and v.vehicle.label else None,
            "latitude": v.position.latitude if v.HasField("position") else None,
            "longitude": v.position.longitude if v.HasField("position") else None,
            "bearing": v.position.bearing if v.HasField("position") and v.position.HasField("bearing") else None,
            "speed": v.position.speed if v.HasField("position") and v.position.HasField("speed") else None,
            "current_stop_sequence": v.current_stop_sequence if v.HasField("current_stop_sequence") else None,
            "stop_id": v.stop_id or None,
            "current_status": v.current_status if v.HasField("current_status") else None,
            "congestion_level": v.congestion_level if v.HasField("congestion_level") else None,
            "occupancy_status": v.occupancy_status if v.HasField("occupancy_status") else None,
        }
        if v.HasField("trip"):
            row.update(_trip_desc(v.trip))
        else:
            row.update({k: None for k in ("trip_id", "route_id", "direction_id", "start_time", "start_date", "schedule_relationship")})
        rows.append(row)
    return rows


def parse_trip_updates(feed: pb.FeedMessage, fetched_at: str) -> list[dict]:
    """One row PER STOP-TIME UPDATE (not per trip) -- this is the target-variable
    table. A trip with predictions for 8 upcoming stops produces 8 rows."""
    rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip_fields = _trip_desc(tu.trip) if tu.HasField("trip") else {}
        vehicle_id = tu.vehicle.id if tu.HasField("vehicle") else None
        base = {
            "fetched_at": fetched_at,
            "entity_id": entity.id or None,
            "trip_update_timestamp": tu.timestamp if tu.HasField("timestamp") else None,
            "trip_delay": tu.delay if tu.HasField("delay") else None,
            "vehicle_id": vehicle_id,
            **trip_fields,
        }
        if not tu.stop_time_update:
            # Trip-level update with no per-stop detail -- still keep the row,
            # every other column will be null.
            row = dict(base)
            row.update({
                "stop_sequence": None, "stop_id": None,
                "arrival_delay": None, "arrival_time": None,
                "departure_delay": None, "departure_time": None,
            })
            rows.append(row)
            continue
        for stu in tu.stop_time_update:
            row = dict(base)
            row.update({
                "stop_sequence": stu.stop_sequence if stu.HasField("stop_sequence") else None,
                "stop_id": stu.stop_id or None,
                "arrival_delay": stu.arrival.delay if stu.HasField("arrival") and stu.arrival.HasField("delay") else None,
                "arrival_time": stu.arrival.time if stu.HasField("arrival") and stu.arrival.HasField("time") else None,
                "departure_delay": stu.departure.delay if stu.HasField("departure") and stu.departure.HasField("delay") else None,
                "departure_time": stu.departure.time if stu.HasField("departure") and stu.departure.HasField("time") else None,
            })
            rows.append(row)
    return rows


def parse_alerts(feed: pb.FeedMessage, fetched_at: str) -> list[dict]:
    """One row per (alert, informed_entity) pair -- an alert can affect several
    routes/stops, and you want to be able to filter by route_id later."""
    rows = []
    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue
        a = entity.alert
        header = a.header_text.translation[0].text if a.header_text.translation else None
        desc = a.description_text.translation[0].text if a.description_text.translation else None
        active_start = a.active_period[0].start if a.active_period and a.active_period[0].HasField("start") else None
        active_end = a.active_period[0].end if a.active_period and a.active_period[0].HasField("end") else None
        base = {
            "fetched_at": fetched_at,
            "entity_id": entity.id or None,
            "cause": a.cause if a.HasField("cause") else None,
            "effect": a.effect if a.HasField("effect") else None,
            "header_text": header,
            "description_text": desc,
            "active_period_start": active_start,
            "active_period_end": active_end,
        }
        if not a.informed_entity:
            row = dict(base)
            row.update({"affected_route_id": None, "affected_stop_id": None, "affected_trip_id": None})
            rows.append(row)
            continue
        for sel in a.informed_entity:
            row = dict(base)
            row.update({
                "affected_route_id": sel.route_id or None,
                "affected_stop_id": sel.stop_id or None,
                "affected_trip_id": sel.trip.trip_id if sel.HasField("trip") else None,
            })
            rows.append(row)
    return rows


PARSERS = {
    "vehiclepositions": parse_vehicle_positions,
    "tripupdates": parse_trip_updates,
    "alerts": parse_alerts,
}
