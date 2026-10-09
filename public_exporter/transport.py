"""Compact private publication; local output and public R2 data stay GeoJSON."""

from __future__ import annotations

import json


COMPACT_MEDIA_TYPE = "application/vnd.bkk-observatory.snapshot.v1+json"


def encode_publication(snapshot: dict) -> bytes:
    """Replace repeated GeoJSON keys with fixed eight-field vehicle tuples.

    The Worker validates every value and reconstructs the original public
    contract. Refuse unexpected feature fields here, rather than silently hide
    a projection bug. Never mutate the independently inspectable local snapshot.
    """
    vehicles = snapshot["vehicles"]
    rows = []
    for feature in vehicles["features"]:
        geometry, properties = feature["geometry"], feature["properties"]
        if (set(feature) != {"type", "id", "geometry", "properties"} or feature["type"] != "Feature"
                or set(geometry) != {"type", "coordinates"} or geometry["type"] != "Point"
                or not isinstance(geometry["coordinates"], list) or len(geometry["coordinates"]) != 2
                or set(properties) != {"route_label", "mode", "color", "bearing", "recorded_at"}):
            raise ValueError("unexpected public vehicle shape")
        rows.append([feature["id"], *geometry["coordinates"], properties["route_label"], properties["mode"],
                     properties["color"], properties["bearing"], properties["recorded_at"]])
    payload = {**snapshot, "vehicles": {**vehicles, "features": rows}}
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
