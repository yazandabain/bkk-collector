"""Bounded independent scheduling for the three realtime feeds."""

from __future__ import annotations

import queue
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable

from monitoring import utc_iso


@dataclass
class FeedSchedule:
    interval_seconds: float
    next_deadline: float
    future: Future[Any] | None = None
    active_poll_id: str | None = None
    active_scheduled_for_at: str | None = None
    active_scheduler_lag_ms: float = 0.0
    active_missed_before_request: int = 0
    in_flight_started_monotonic: float | None = None
    result_pending: bool = False
    pending_poll_id: str | None = None
    total_submitted: int = 0
    total_completed: int = 0
    total_missed_deadlines: int = 0
    missed_since_last_request: int = 0
    last_scheduler_lag_ms: float | None = None
    last_completed_monotonic: float | None = None
    last_missed_monotonic: float | None = None


@dataclass(frozen=True)
class ScheduledResult:
    feed_name: str
    poll_id: str
    value: Any | None
    worker_error: BaseException | None
    scheduled_for_at: str
    scheduler_lag_ms: float
    missed_deadlines_before_request: int


class IndependentFeedScheduler:
    """One monotonic schedule and one active HTTP request per feed.

    The scheduler thread owns request submission.  The collector's main thread
    consumes the bounded result queue and remains the sole owner of parsing,
    change trackers, the durable spool, and health state.  One completed result
    plus one subsequent active request is the maximum outstanding work per
    feed, so overload coalesces deadlines rather than growing a queue.
    """

    def __init__(
        self,
        fetch: Callable[[str, str], Any],
        intervals: dict[str, float],
        *,
        executor: Any | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        wall_time: Callable[[], float] = time.time,
        start_monotonic: float | None = None,
    ):
        self.fetch = fetch
        self.monotonic = monotonic
        self.wall_time = wall_time
        start = monotonic() if start_monotonic is None else start_monotonic
        self.schedules = {
            feed_name: FeedSchedule(float(interval), start)
            for feed_name, interval in intervals.items()
        }
        self.executor = executor or ThreadPoolExecutor(
            max_workers=len(intervals), thread_name_prefix="bkk-fetch"
        )
        self.results: queue.Queue[ScheduledResult] = queue.Queue(maxsize=len(intervals))
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._accepting = True
        self._thread: threading.Thread | None = None

    def _deadline_wall_iso(self, deadline: float, now: float) -> str:
        return utc_iso(self.wall_time() - max(0.0, now - deadline))

    def step(self, now: float | None = None) -> None:
        """Advance the state machine once; public to allow deterministic tests."""
        current = self.monotonic() if now is None else now
        ready: list[ScheduledResult] = []
        with self._lock:
            # Only move a completed result into the queue after the previous
            # result for that feed was acknowledged by the durability owner.
            for feed_name, state in self.schedules.items():
                if state.future is None or not state.future.done() or state.result_pending:
                    continue
                future = state.future
                poll_id = state.active_poll_id or "unknown"
                try:
                    value = future.result()
                    worker_error = None
                except BaseException as error:  # surfaced and journaled by Collector
                    value = None
                    worker_error = error
                ready.append(
                    ScheduledResult(
                        feed_name=feed_name,
                        poll_id=poll_id,
                        value=value,
                        worker_error=worker_error,
                        scheduled_for_at=state.active_scheduled_for_at or utc_iso(self.wall_time()),
                        scheduler_lag_ms=state.active_scheduler_lag_ms,
                        missed_deadlines_before_request=state.active_missed_before_request,
                    )
                )
                state.future = None
                state.result_pending = True
                state.pending_poll_id = poll_id
                state.in_flight_started_monotonic = None
                state.total_completed += 1
                state.last_completed_monotonic = current

            for feed_name, state in self.schedules.items():
                if not self._accepting:
                    break
                if current < state.next_deadline:
                    continue
                due_count = int((current - state.next_deadline) // state.interval_seconds) + 1
                latest_due = state.next_deadline + (due_count - 1) * state.interval_seconds
                state.next_deadline += due_count * state.interval_seconds
                if state.future is not None:
                    state.total_missed_deadlines += due_count
                    state.missed_since_last_request += due_count
                    state.last_missed_monotonic = current
                    continue

                # A queued result is allowed alongside one subsequent request.
                # Further deadlines are coalesced if that request finishes
                # before its predecessor has been durably processed.
                coalesced = max(0, due_count - 1)
                if coalesced:
                    state.total_missed_deadlines += coalesced
                    state.missed_since_last_request += coalesced
                    state.last_missed_monotonic = current
                poll_id = uuid.uuid4().hex
                lag_ms = max(0.0, current - latest_due) * 1000.0
                missed_before = state.missed_since_last_request
                state.missed_since_last_request = 0
                state.active_poll_id = poll_id
                state.active_scheduled_for_at = self._deadline_wall_iso(latest_due, current)
                state.active_scheduler_lag_ms = lag_ms
                state.active_missed_before_request = missed_before
                state.in_flight_started_monotonic = current
                state.last_scheduler_lag_ms = lag_ms
                state.total_submitted += 1
                state.future = self.executor.submit(self.fetch, feed_name, poll_id)

        # Exactly one result per feed can be pending, so this cannot overflow.
        for item in ready:
            self.results.put_nowait(item)

    def _run(self) -> None:
        while not self._stop.is_set():
            self.step()
            now = self.monotonic()
            with self._lock:
                nearest = min(state.next_deadline for state in self.schedules.values())
                any_future = any(state.future is not None for state in self.schedules.values())
            # Poll Futures reasonably promptly without using callback threads to
            # mutate scheduler state. Deadlines still supply the long timeout.
            timeout = max(0.0, nearest - now)
            if any_future:
                timeout = min(timeout, 0.1)
            timeout = min(timeout, 1.0)
            self._wake.wait(timeout)
            self._wake.clear()

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("scheduler already started")
        self._thread = threading.Thread(target=self._run, name="bkk-scheduler", daemon=False)
        self._thread.start()

    def get(self, timeout: float) -> ScheduledResult | None:
        try:
            return self.results.get(timeout=timeout)
        except queue.Empty:
            return None

    def acknowledge(self, item: ScheduledResult) -> None:
        with self._lock:
            state = self.schedules[item.feed_name]
            if state.result_pending and state.pending_poll_id == item.poll_id:
                state.result_pending = False
                state.pending_poll_id = None
        self._wake.set()

    def snapshot(self, now: float | None = None) -> dict[str, dict[str, Any]]:
        current = self.monotonic() if now is None else now
        with self._lock:
            return {
                feed_name: {
                    "interval_seconds": state.interval_seconds,
                    "in_flight": state.future is not None and not state.future.done(),
                    "completed_request_waiting": state.future is not None and state.future.done(),
                    "result_pending_processing": state.result_pending,
                    "in_flight_seconds": (
                        None
                        if state.in_flight_started_monotonic is None
                        else max(0.0, current - state.in_flight_started_monotonic)
                    ),
                    "next_deadline_in_seconds": state.next_deadline - current,
                    "total_submitted": state.total_submitted,
                    "total_completed": state.total_completed,
                    "total_missed_deadlines": state.total_missed_deadlines,
                    "missed_since_last_request": state.missed_since_last_request,
                    "seconds_since_missed_deadline": (
                        None
                        if state.last_missed_monotonic is None
                        else max(0.0, current - state.last_missed_monotonic)
                    ),
                    "last_scheduler_lag_ms": state.last_scheduler_lag_ms,
                }
                for feed_name, state in self.schedules.items()
            }

    def stop_and_drain(self) -> list[ScheduledResult]:
        """Stop new submissions, finish active requests, and return every result."""
        with self._lock:
            self._accepting = False
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join()
        self.executor.shutdown(wait=True, cancel_futures=False)

        # After executor shutdown all active Futures are terminal. Move each
        # into the bounded queue, draining first if necessary.
        drained: list[ScheduledResult] = []
        while True:
            try:
                drained.append(self.results.get_nowait())
            except queue.Empty:
                break
        with self._lock:
            for state in self.schedules.values():
                state.result_pending = False
                state.pending_poll_id = None
        self.step()
        while True:
            try:
                drained.append(self.results.get_nowait())
            except queue.Empty:
                break
        return drained
