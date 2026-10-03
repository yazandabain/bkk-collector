"""One background Parquet commit owner; never submit a work item per poll."""

from __future__ import annotations

import logging
import threading
import time

from bkk_collector.storage.parquet_store import DurableParquetSpool


class ParquetCommitWorker:
    """Ingestion publishes immutable spool files; only this thread commits them.

    No row queue or shared tracker exists. The disk spool is the queue and
    recovery source. A daemon thread permits bounded shutdown even if local
    storage hangs; interruption is recovered by existing commit markers.
    """

    def __init__(self, spool: DurableParquetSpool, logger: logging.Logger):
        self.spool = spool
        self.logger = logger
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._active_started_monotonic = 0.0
        self._state = {"running": False, "errors": [], "last_started_at": None,
                       "last_finished_at": None, "last_duration_seconds": None,
                       "last_rows_written": 0, "last_files_written": 0}

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("Parquet commit worker already started")
        self._thread = threading.Thread(target=self._run, name="bkk-parquet", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            with self._lock:
                self._active_started_monotonic = started
                self._state.update(running=True, last_started_at=time.time())
            try:
                result = self.spool.flush(force=True, should_stop=self._stop.is_set)
                errors = result.errors
                rows, files = result.rows_written, len(result.files_written)
            except Exception as error:
                errors, rows, files = [f"{type(error).__name__}: {error}"], 0, 0
            with self._lock:
                self._state.update(running=False, errors=errors, last_finished_at=time.time(),
                                   last_duration_seconds=time.monotonic() - started,
                                   last_rows_written=rows, last_files_written=files)
            for error in errors:
                self.logger.error("Parquet commit failed; durable spool retained: %s", error)
            if files:
                self.logger.info("Committed %d rows to %d Parquet file(s) in %.3fs", rows, files, time.monotonic() - started)
            # Failure retries are bounded, rather than occurring on every poll.
            self._stop.wait(30.0 if errors else max(1.0, self.spool.flush_seconds))

    def snapshot(self) -> dict:
        with self._lock:
            state = dict(self._state)
            state["errors"] = list(state["errors"])
            state["active_seconds"] = max(0.0, time.monotonic() - self._active_started_monotonic) if state["running"] else 0.0
        state["alive"] = self._thread is not None and self._thread.is_alive()
        return state

    def stop(self, timeout: float = 10.0) -> bool:
        self._stop.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout)
        return not self.snapshot()["alive"]
