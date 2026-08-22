"""Docker healthcheck for the independent maintenance worker."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from atomic_io import read_json


def main() -> int:
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    max_age = float(os.environ.get("MAINTENANCE_STATUS_MAX_AGE_SECONDS", "300"))
    status = read_json(data_dir / "maintenance" / "status.json", {})
    age = time.time() - float(status.get("updated_timestamp", 0))
    if not status or age > max_age:
        print(f"unhealthy: maintenance status missing/stale (age={age:.0f}s)")
        return 1
    if not status.get("healthy"):
        print("unhealthy: " + ", ".join(status.get("reasons", ["unknown"])))
        return 1
    print(f"healthy: maintenance status age={age:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
