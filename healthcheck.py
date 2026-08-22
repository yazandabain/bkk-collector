"""Docker healthcheck: fail when collection data is absent, stale, or unsafe."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from atomic_io import read_json


def main() -> int:
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    max_age = float(os.environ.get("HEALTH_STATUS_MAX_AGE_SECONDS", "180"))
    status = read_json(data_dir / "health" / "status.json", {})
    age = time.time() - float(status.get("updated_timestamp", 0))
    if not status or age > max_age:
        print(f"unhealthy: collector health status missing/stale (age={age:.0f}s)")
        return 1
    if not status.get("healthy"):
        print("unhealthy: " + ", ".join(status.get("reasons", ["unknown"])))
        return 1
    print(f"healthy: status age={age:.0f}s, disk_free={status.get('disk_free_bytes')} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
