"""Loss-conscious GTFS-Realtime parsing for BKK feeds.

The raw protobuf is always the authoritative representation. These rows keep
the common research fields typed/scalar and preserve repeated or evolving
structures as canonical JSON instead of flattening away information.
"""

from __future__ import annotations

import json
from typing import Any

from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2 as pb

import realcity


SCHEMA_VERSION = 2


def parse_feed(raw_bytes: bytes) -> pb.FeedMessage:
    feed = pb.FeedMessage()
    feed.ParseFromString(raw_bytes)
    if not feed.IsInitialized():
        raise ValueError("GTFS-RT message is missing required fields")
    return feed


def _has(message: Any, field: str) -> bool:
    if field not in message.DESCRIPTOR.fields_by_name:
        return False
    try:
        return message.HasField(field)
    except ValueError:
        return False


def _value(message: Any, field: str, default: Any = None) -> Any:
    return getattr(message, field) if _has(message, field) else default


def _enum_name(message: Any, field: str) -> str | None:
    if not _has(message, field):
        return None
    descriptor = message.DESCRIPTOR.fields_by_name[field]
    enum_value = descriptor.enum_type.values_by_number.get(getattr(message, field))
    return enum_value.name if enum_value else str(getattr(message, field))


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _message_json(message: Any) -> str | None:
    if message is None:
        return None
    return _json(MessageToDict(message, preserving_proto_field_name=True))


def _translations(value: Any) -> list[dict[str, str | None]]:
    if value is None:
        return []
    return [
        {
            "text": translation.text,
            "language": translation.language if _has(translation, "language") and translation.language else None,
        }
        for translation in value.translation
    ]


def _first_translation(value: Any) -> str | None:
    translations = _translations(value)
    return translations[0]["text"] if translations else None


def _poll_context(feed: pb.FeedMessage, context: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(context, str):
        context = {
            "poll_id": None,
            "request_started_at": context,
            "response_received_at": context,
        }
    received = context.get("response_received_at") or context.get("fetched_at")
    return {
        "schema_version": SCHEMA_VERSION,
        "poll_id": context.get("poll_id"),
        "request_started_at": context.get("request_started_at") or received,
        "response_received_at": received,
        "fetched_at": received,  # backward-compatible alias
        "feed_header_timestamp": _value(feed.header, "timestamp"),
        "feed_header_version": _value(feed.header, "feed_version"),
        "feed_incrementality": _value(feed.header, "incrementality"),
        "feed_incrementality_name": _enum_name(feed.header, "incrementality"),
    }


def _entity_context(entity: pb.FeedEntity) -> dict[str, Any]:
    return {
        "entity_id": entity.id or None,
        "entity_is_deleted": _value(entity, "is_deleted"),
    }


def _trip_desc(trip: Any) -> dict[str, Any]:
    modified = getattr(trip, "modified_trip", None) if _has(trip, "modified_trip") else None
    return {
        "trip_id": trip.trip_id or None,
        "route_id": trip.route_id or None,
        "direction_id": _value(trip, "direction_id"),
        "start_time": trip.start_time or None,
        "start_date": trip.start_date or None,
        "schedule_relationship": _value(trip, "schedule_relationship"),
        "schedule_relationship_name": _enum_name(trip, "schedule_relationship"),
        "modified_trip_json": _message_json(modified),
    }


TRIP_FIELDS = (
    "trip_id", "route_id", "direction_id", "start_time", "start_date",
    "schedule_relationship", "schedule_relationship_name", "modified_trip_json",
)


def _realcity_vehicle(vehicle: Any) -> dict[str, Any]:
    empty = {
        "bkk_vehicle_model": None,
        "bkk_deviated": None,
        "bkk_vehicle_type": None,
        "bkk_door_open": None,
        "bkk_stop_distance": None,
    }
    try:
        if not vehicle.HasExtension(realcity.vehicle):
            return empty
        extension = vehicle.Extensions[realcity.vehicle]
    except (KeyError, TypeError):
        return empty
    return {
        "bkk_vehicle_model": _value(extension, "vehicle_model"),
        "bkk_deviated": _value(extension, "deviated", False),
        "bkk_vehicle_type": _value(extension, "vehicle_type"),
        "bkk_door_open": _value(extension, "door_open"),
        "bkk_stop_distance": _value(extension, "stop_distance"),
    }


def _vehicle_desc(vehicle: Any | None) -> dict[str, Any]:
    if vehicle is None:
        return {
            "vehicle_id": None,
            "vehicle_label": None,
            "vehicle_license_plate": None,
            "vehicle_wheelchair_accessible": None,
            "vehicle_wheelchair_accessible_name": None,
            **_realcity_vehicle(pb.VehicleDescriptor()),
        }
    return {
        "vehicle_id": vehicle.id or None,
        "vehicle_label": vehicle.label or None,
        "vehicle_license_plate": _value(vehicle, "license_plate"),
        "vehicle_wheelchair_accessible": _value(vehicle, "wheelchair_accessible"),
        "vehicle_wheelchair_accessible_name": _enum_name(vehicle, "wheelchair_accessible"),
        **_realcity_vehicle(vehicle),
    }


def _stop_event(event: Any | None, prefix: str) -> dict[str, Any]:
    return {
        f"{prefix}_delay": _value(event, "delay") if event is not None else None,
        f"{prefix}_time": _value(event, "time") if event is not None else None,
        f"{prefix}_uncertainty": _value(event, "uncertainty") if event is not None else None,
        f"{prefix}_scheduled_time": _value(event, "scheduled_time") if event is not None else None,
    }


def _realcity_stop_update(stop_update: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    extension = None
    try:
        if stop_update.HasExtension(realcity.stop_time_update):
            extension = stop_update.Extensions[realcity.stop_time_update]
    except (KeyError, TypeError):
        pass
    for field, prefix in (("scheduled_arrival", "bkk_scheduled_arrival"), ("scheduled_departure", "bkk_scheduled_departure")):
        event = getattr(extension, field) if extension is not None and _has(extension, field) else None
        result.update(_stop_event(event, prefix))
    return result


def parse_vehicle_positions(feed: pb.FeedMessage, context: str | dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    poll = _poll_context(feed, context)
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            if _value(entity, "is_deleted") is True:
                rows.append({**poll, **_entity_context(entity)})
            continue
        vehicle_position = entity.vehicle
        position = vehicle_position.position if vehicle_position.HasField("position") else None
        descriptor = vehicle_position.vehicle if vehicle_position.HasField("vehicle") else None
        row = {
            **poll,
            **_entity_context(entity),
            "vehicle_timestamp": _value(vehicle_position, "timestamp"),
            **_vehicle_desc(descriptor),
            "latitude": _value(position, "latitude") if position is not None else None,
            "longitude": _value(position, "longitude") if position is not None else None,
            "bearing": _value(position, "bearing") if position is not None else None,
            "odometer": _value(position, "odometer") if position is not None else None,
            "speed": _value(position, "speed") if position is not None else None,
            "current_stop_sequence": _value(vehicle_position, "current_stop_sequence"),
            "stop_id": vehicle_position.stop_id or None,
            "current_status": _value(vehicle_position, "current_status"),
            "current_status_name": _enum_name(vehicle_position, "current_status"),
            "congestion_level": _value(vehicle_position, "congestion_level"),
            "congestion_level_name": _enum_name(vehicle_position, "congestion_level"),
            "occupancy_status": _value(vehicle_position, "occupancy_status"),
            "occupancy_status_name": _enum_name(vehicle_position, "occupancy_status"),
            "occupancy_percentage": _value(vehicle_position, "occupancy_percentage"),
            "multi_carriage_details_json": _json(
                [MessageToDict(item, preserving_proto_field_name=True) for item in getattr(vehicle_position, "multi_carriage_details", [])]
            ),
        }
        row.update(_trip_desc(vehicle_position.trip) if vehicle_position.HasField("trip") else {field: None for field in TRIP_FIELDS})
        rows.append(row)
    return rows


def parse_trip_updates(feed: pb.FeedMessage, context: str | dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    poll = _poll_context(feed, context)
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            if _value(entity, "is_deleted") is True:
                rows.append({**poll, **_entity_context(entity)})
            continue
        update = entity.trip_update
        trip_fields = _trip_desc(update.trip) if update.HasField("trip") else {field: None for field in TRIP_FIELDS}
        descriptor = update.vehicle if update.HasField("vehicle") else None
        base = {
            **poll,
            **_entity_context(entity),
            "trip_update_timestamp": _value(update, "timestamp"),
            "trip_delay": _value(update, "delay"),
            "trip_properties_json": _message_json(update.trip_properties) if _has(update, "trip_properties") else None,
            **_vehicle_desc(descriptor),
            **trip_fields,
        }
        stop_updates = list(update.stop_time_update) or [None]
        for stop_update_index, stop_update in enumerate(stop_updates):
            row = dict(base)
            if stop_update is None:
                row.update({
                    "stop_time_update_index": None, "stop_visit_fallback_index": None,
                    "stop_sequence": None, "stop_id": None,
                    "stop_schedule_relationship": None, "stop_schedule_relationship_name": None,
                    "departure_occupancy_status": None, "departure_occupancy_status_name": None,
                    "stop_time_properties_json": None,
                    **_stop_event(None, "arrival"), **_stop_event(None, "departure"),
                    **_realcity_stop_update(pb.TripUpdate.StopTimeUpdate()),
                })
            else:
                stop_sequence = _value(stop_update, "stop_sequence")
                row.update({
                    "stop_time_update_index": stop_update_index,
                    # Sequence is the stable identity. The ordinal is used only
                    # when an upstream message omits that optional field.
                    "stop_visit_fallback_index": stop_update_index if stop_sequence is None else None,
                    "stop_sequence": stop_sequence,
                    "stop_id": stop_update.stop_id or None,
                    "stop_schedule_relationship": _value(stop_update, "schedule_relationship"),
                    "stop_schedule_relationship_name": _enum_name(stop_update, "schedule_relationship"),
                    "departure_occupancy_status": _value(stop_update, "departure_occupancy_status"),
                    "departure_occupancy_status_name": _enum_name(stop_update, "departure_occupancy_status"),
                    "stop_time_properties_json": _message_json(stop_update.stop_time_properties) if _has(stop_update, "stop_time_properties") else None,
                    **_stop_event(stop_update.arrival if stop_update.HasField("arrival") else None, "arrival"),
                    **_stop_event(stop_update.departure if stop_update.HasField("departure") else None, "departure"),
                    **_realcity_stop_update(stop_update),
                })
            rows.append(row)
    return rows


def _selector(selector: Any) -> dict[str, Any]:
    trip = _trip_desc(selector.trip) if selector.HasField("trip") else {field: None for field in TRIP_FIELDS}
    return {
        "agency_id": selector.agency_id or None,
        "route_id": selector.route_id or None,
        "route_type": _value(selector, "route_type"),
        "stop_id": selector.stop_id or None,
        "direction_id": _value(selector, "direction_id"),
        "trip": trip,
    }


def _realcity_alert(alert: Any) -> dict[str, Any]:
    empty = {"bkk_start_text_json": "[]", "bkk_end_text_json": "[]", "bkk_modified_time": None, "bkk_route_details_json": "[]"}
    try:
        if not alert.HasExtension(realcity.alert):
            return empty
        extension = alert.Extensions[realcity.alert]
    except (KeyError, TypeError):
        return empty
    routes = [{
        "route_id": route.route_id,
        "header_text": _translations(route.header_text) if _has(route, "header_text") else [],
        "cause": _value(route, "cause"), "cause_name": _enum_name(route, "cause"),
        "effect": _value(route, "effect"), "effect_name": _enum_name(route, "effect"),
        "effect_type": _value(route, "effect_type"), "effect_type_name": _enum_name(route, "effect_type"),
    } for route in extension.route]
    return {
        "bkk_start_text_json": _json(_translations(extension.startText) if _has(extension, "startText") else []),
        "bkk_end_text_json": _json(_translations(extension.endText) if _has(extension, "endText") else []),
        "bkk_modified_time": _value(extension, "modifiedTime"),
        "bkk_route_details_json": _json(routes),
    }


def parse_alerts(feed: pb.FeedMessage, context: str | dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    poll = _poll_context(feed, context)
    for entity in feed.entity:
        if not entity.HasField("alert"):
            if _value(entity, "is_deleted") is True:
                rows.append({**poll, **_entity_context(entity)})
            continue
        alert = entity.alert
        periods = [{"start": _value(period, "start"), "end": _value(period, "end")} for period in alert.active_period]
        communication_periods = [{"start": _value(period, "start"), "end": _value(period, "end")} for period in getattr(alert, "communication_period", [])]
        impact_periods = [{"start": _value(period, "start"), "end": _value(period, "end")} for period in getattr(alert, "impact_period", [])]
        selectors = [_selector(value) for value in alert.informed_entity]
        base = {
            **poll, **_entity_context(entity),
            "cause": _value(alert, "cause"), "cause_name": _enum_name(alert, "cause"),
            "effect": _value(alert, "effect"), "effect_name": _enum_name(alert, "effect"),
            "severity_level": _value(alert, "severity_level"), "severity_level_name": _enum_name(alert, "severity_level"),
            "header_text": _first_translation(alert.header_text) if _has(alert, "header_text") else None,
            "description_text": _first_translation(alert.description_text) if _has(alert, "description_text") else None,
            "url": _first_translation(alert.url) if _has(alert, "url") else None,
            "header_text_json": _json(_translations(alert.header_text) if _has(alert, "header_text") else []),
            "description_text_json": _json(_translations(alert.description_text) if _has(alert, "description_text") else []),
            "url_json": _json(_translations(alert.url) if _has(alert, "url") else []),
            "tts_header_text_json": _json(_translations(alert.tts_header_text) if _has(alert, "tts_header_text") else []),
            "tts_description_text_json": _json(_translations(alert.tts_description_text) if _has(alert, "tts_description_text") else []),
            "image_json": _message_json(alert.image) if _has(alert, "image") else None,
            "image_alternative_text_json": _json(_translations(alert.image_alternative_text) if _has(alert, "image_alternative_text") else []),
            "cause_detail_json": _message_json(alert.cause_detail) if _has(alert, "cause_detail") else None,
            "effect_detail_json": _message_json(alert.effect_detail) if _has(alert, "effect_detail") else None,
            "active_period_start": periods[0]["start"] if periods else None,
            "active_period_end": periods[0]["end"] if periods else None,
            "active_periods_json": _json(periods),
            "communication_periods_json": _json(communication_periods),
            "impact_periods_json": _json(impact_periods),
            "informed_entities_json": _json(selectors),
            **_realcity_alert(alert),
        }
        for selector in selectors or [None]:
            row = dict(base)
            trip = selector["trip"] if selector else {field: None for field in TRIP_FIELDS}
            row.update({
                "affected_agency_id": selector["agency_id"] if selector else None,
                "affected_route_id": selector["route_id"] if selector else None,
                "affected_route_type": selector["route_type"] if selector else None,
                "affected_stop_id": selector["stop_id"] if selector else None,
                "affected_direction_id": selector["direction_id"] if selector else None,
                "affected_trip_id": trip["trip_id"],
                "affected_trip_route_id": trip["route_id"],
                "affected_trip_direction_id": trip["direction_id"],
                "affected_trip_start_time": trip["start_time"],
                "affected_trip_start_date": trip["start_date"],
                "affected_trip_schedule_relationship": trip["schedule_relationship"],
                "affected_trip_schedule_relationship_name": trip["schedule_relationship_name"],
            })
            rows.append(row)
    return rows


PARSERS = {
    "vehiclepositions": parse_vehicle_positions,
    "tripupdates": parse_trip_updates,
    "alerts": parse_alerts,
}
