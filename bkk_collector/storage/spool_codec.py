"""Lossless shared strings for wide internal spool segments, not research rows."""

from __future__ import annotations

from typing import Any


SHARED_STRING_MIN_CHARS = 4096
REFERENCE_KEY = "$shared_string"


def share_large_strings(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    """Keep repeated large selector/text JSON once without changing input rows."""
    strings: list[str] = []
    indices: dict[str, int] = {}
    encoded = []
    for row in rows:
        item = {}
        for field, value in row.items():
            if isinstance(value, str) and len(value) >= SHARED_STRING_MIN_CHARS:
                index = indices.get(value)
                if index is None:
                    index = len(strings)
                    indices[value] = index
                    strings.append(value)
                item[field] = {REFERENCE_KEY: index}
            else:
                item[field] = value
        encoded.append(item)
    return (encoded, strings) if strings else (rows, [])


def restore_shared_strings(rows: list, strings: Any) -> list[dict[str, Any]]:
    if not isinstance(strings, list) or any(not isinstance(value, str) for value in strings):
        raise ValueError("invalid spool shared strings")
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("invalid spool row")
        for field, value in row.items():
            if isinstance(value, dict) and REFERENCE_KEY in value:
                index = value[REFERENCE_KEY]
                if set(value) != {REFERENCE_KEY} or type(index) is not int or not 0 <= index < len(strings):
                    raise ValueError("invalid spool shared-string reference")
                row[field] = strings[index]
    return rows
