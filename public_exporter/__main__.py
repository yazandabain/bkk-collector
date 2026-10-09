"""One outbound publisher, no web server and no collector credentials."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit

import requests

from bkk_collector.storage.atomic_io import atomic_write_json
from public_exporter.snapshot import SnapshotBuilder, iso


class PublishError(RuntimeError):
    """A refused upload, exposing only its numeric HTTP status."""

    def __init__(self, status_code: int):
        self.status_code = status_code
        super().__init__(f"snapshot publish refused (HTTP {status_code})")


def publish(session: requests.Session, url: str, token: str, snapshot: dict) -> None:
    payload = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    if len(payload) > 2 * 1024 * 1024:
        raise ValueError("snapshot exceeds publish limit")
    response = session.put(url, data=payload, headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
                           timeout=(3, 7), allow_redirects=False)
    if response.status_code != 204:
        raise PublishError(response.status_code)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--no-publish", action="store_true", help="Create a sanitized snapshot without network access")
    parser.add_argument("--input", type=Path, default=Path(os.environ.get("PUBLIC_INPUT_DIR", "/input")))
    parser.add_argument("--output", type=Path, default=Path(os.environ.get("PUBLIC_OUTPUT_DIR", "/output")))
    args = parser.parse_args()
    interval = float(os.environ.get("PUBLIC_EXPORT_INTERVAL_SECONDS", "10"))
    url, token = os.environ.get("PUBLIC_PUBLISH_URL", ""), os.environ.get("PUBLIC_PUBLISH_TOKEN", "")
    parsed = urlsplit(url)
    if not 10 <= interval <= 300:
        parser.error("PUBLIC_EXPORT_INTERVAL_SECONDS must be between 10 and 300")
    if not args.no_publish and (parsed.scheme != "https" or parsed.username or parsed.password or parsed.query or
                                parsed.fragment or parsed.path != "/api/publish" or len(token) < 32):
        parser.error("Set an HTTPS PUBLIC_PUBLISH_URL ending /api/publish and a token of at least 32 characters")
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    builder = SnapshotBuilder(args.input)
    deadline = time.monotonic()
    with requests.Session() as session:
        # Do not inherit proxy credentials or automatic .netrc authentication.
        session.trust_env = False
        while not stop.is_set():
            success = False
            try:
                snapshot = builder.build(time.time())
                atomic_write_json(args.output / "snapshot.json", snapshot)
                if not args.no_publish:
                    publish(session, url, token, snapshot)
                success = True
            except PublishError as exc:
                logging.error("Public snapshot failed (HTTP %d); retrying next tick", exc.status_code)
            except Exception as exc:
                # requests exceptions may contain authenticated endpoints.
                logging.error("Public snapshot failed (%s); retrying next tick", type(exc).__name__)
            atomic_write_json(args.output / "status.json", {"updated_at": iso(time.time()), "updated_timestamp": time.time(), "healthy": success})
            if args.once:
                return 0 if success else 1
            deadline += interval
            if deadline <= time.monotonic():
                deadline += (int((time.monotonic() - deadline) // interval) + 1) * interval
            stop.wait(max(0, deadline - time.monotonic()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
