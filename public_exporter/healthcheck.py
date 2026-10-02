"""Checks only the isolated publisher, never controls collection."""

import os
import time
from pathlib import Path

from public_exporter.source import read_object


def main() -> int:
    try:
        status = read_object(Path(os.environ.get("PUBLIC_OUTPUT_DIR", "/output")) / "status.json", 4096)
        healthy = status.get("healthy") is True and 0 <= time.time() - status["updated_timestamp"] <= 90
    except (OSError, ValueError, KeyError, TypeError):
        healthy = False
    print("healthy: public publisher" if healthy else "unhealthy: public publisher")
    return 0 if healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
