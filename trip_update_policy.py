"""Explicit change-compression policy for parsed TripUpdates rows.

Every TripUpdates Parquet column is deliberately classified here.  Request,
feed-header, and source-message timestamps are retained in emitted rows but do
not themselves cause emission: most change every poll and would disable useful
compression.  Numeric prediction noise and BKK stop-distance movement use the
configured tolerance; every other meaningful mutable field is exact.

The coverage regression test intentionally fails whenever the parser/Parquet
schema gains a field without a corresponding decision here.
"""

TRIP_UPDATE_KEY_FIELDS = (
    "entity_id",
    "trip_id",
    "start_date",
    "start_time",
    "stop_sequence",
    "stop_visit_fallback_index",
    "stop_id",
)

TRIP_UPDATE_EXACT_MUTABLE_FIELDS = (
    "entity_is_deleted",
    "trip_properties_json",
    "vehicle_id",
    "vehicle_label",
    "vehicle_license_plate",
    "vehicle_wheelchair_accessible",
    "bkk_vehicle_model",
    "bkk_deviated",
    "bkk_vehicle_type",
    "bkk_door_open",
    "route_id",
    "direction_id",
    "schedule_relationship",
    "modified_trip_json",
    "stop_schedule_relationship",
    "departure_occupancy_status",
    "stop_time_properties_json",
    "arrival_uncertainty",
    "arrival_scheduled_time",
    "departure_uncertainty",
    "departure_scheduled_time",
    "bkk_scheduled_arrival_delay",
    "bkk_scheduled_arrival_time",
    "bkk_scheduled_arrival_uncertainty",
    "bkk_scheduled_arrival_scheduled_time",
    "bkk_scheduled_departure_delay",
    "bkk_scheduled_departure_time",
    "bkk_scheduled_departure_uncertainty",
    "bkk_scheduled_departure_scheduled_time",
)

TRIP_UPDATE_DELAY_FIELDS = (
    "trip_delay",
    "arrival_delay",
    "departure_delay",
)

# BKK metro commonly supplies no delay fields at all; its useful mutable
# predictions are these absolute Unix timestamps. Live evidence shows the same
# pattern across surface modes, HÉV, and ferry, so this is deliberately general.
# They must be compared as predictions, not ignored as collection metadata or
# compared exactly. For a fixed stop-visit identity, scheduled time is constant,
# therefore delta(predicted - scheduled) == delta(predicted); a static-GTFS
# lookup in the realtime path would not change the emission decision.
TRIP_UPDATE_PREDICTION_TIME_FIELDS = (
    "arrival_time",
    "departure_time",
)

TRIP_UPDATE_DISTANCE_FIELDS = (
    "bkk_stop_distance",
)

TRIP_UPDATE_TOLERANT_NUMERIC_FIELDS = (
    TRIP_UPDATE_DELAY_FIELDS
    + TRIP_UPDATE_PREDICTION_TIME_FIELDS
    + TRIP_UPDATE_DISTANCE_FIELDS
)

# These are deterministic representations of other classified fields, or
# structural ordinals already represented by the identity fallback.
TRIP_UPDATE_IMMUTABLE_OR_REDUNDANT_FIELDS = (
    "schema_version",
    "feed_header_version",
    "feed_incrementality",
    "feed_incrementality_name",
    "vehicle_wheelchair_accessible_name",
    "schedule_relationship_name",
    "stop_time_update_index",
    "stop_schedule_relationship_name",
    "departure_occupancy_status_name",
)

# Retained as provenance on every emitted row.  Including any of these in the
# comparison would manufacture a change on virtually every successful poll.
TRIP_UPDATE_COLLECTION_METADATA_FIELDS = (
    "poll_id",
    "request_started_at",
    "response_received_at",
    "fetched_at",
    "feed_header_timestamp",
    "trip_update_timestamp",
)


def classified_trip_update_fields() -> set[str]:
    return set().union(
        TRIP_UPDATE_KEY_FIELDS,
        TRIP_UPDATE_EXACT_MUTABLE_FIELDS,
        TRIP_UPDATE_TOLERANT_NUMERIC_FIELDS,
        TRIP_UPDATE_IMMUTABLE_OR_REDUNDANT_FIELDS,
        TRIP_UPDATE_COLLECTION_METADATA_FIELDS,
    )
