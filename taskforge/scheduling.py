"""Parent-side queue policy shared by both execution backends."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from math import isfinite
from threading import TIMEOUT_MAX, Condition
from time import monotonic
from typing import Any, Generic, ParamSpec, TypeVar
from uuid import UUID

from .errors import QueueFullError, RuntimeClosedError, WorkerLostError
from .job import Job, JobState
from .observability import RuntimeSnapshot, WorkerHealth, WorkerState

Payload = TypeVar("Payload")
P = ParamSpec("P")
T = TypeVar("T")
_log = logging.getLogger("taskforge.runtime")


@dataclass(frozen=True, slots=True)
class Schedule:
    priority: int = 0
    submit_timeout: float | None = None
    start_within: float | None = None
    max_retries: int = 0
    retry_delay: float = 0

    def __post_init__(self) -> None:
        if type(self.priority) is not int:
            raise ValueError("priority must be an integer")
        if self.submit_timeout is not None:
            _validate_duration("submit_timeout", self.submit_timeout)
        if self.start_within is not None:
            _validate_duration("start_within", self.start_within)
        if type(self.max_retries) is not int or self.max_retries < 0:
            raise ValueError("max_retries must be a nonnegative integer")
        _validate_duration("retry_delay", self.retry_delay)


def _validate_duration(name: str, value: float) -> None:
    if (
        type(value) not in (int, float)
        or not 0 <= value <= TIMEOUT_MAX
        or not isfinite(value)
    ):
        raise ValueError(
            f"{name} must be a finite nonnegative duration up to {TIMEOUT_MAX} seconds"
        )


@dataclass(slots=True)
class WorkItem(Generic[Payload]):
    job: Job[Any]
    payload: Payload
    schedule: Schedule
    deadline: float | None
    ready_at: float = 0
    reserved: bool = False
    worker_id: str = ""


class ScheduledPool(ABC, Generic[Payload]):
    _backend: str

    def __init__(self, capacity: int, priority_burst: int) -> None:
        if type(capacity) is not int or capacity < 1:
            raise ValueError("queue_capacity must be a positive integer")
        if type(priority_burst) is not int or priority_burst < 1:
            raise ValueError("priority_burst must be a positive integer")
        self._capacity = capacity
        self._priority_burst = priority_burst
        self._bypasses = 0
        self._changed = Condition()
        self._closed = False
        self._stopped = False
        self._cancel_retries = False
        self._reserved = 0
        self._pending: dict[UUID, WorkItem[Payload]] = {}
        self._health: dict[str, WorkerHealth] = {}
        self._outcomes: Counter[JobState] = Counter()
        self._accepted = self._started = self._attempts = self._retries = 0

    def snapshot(self) -> RuntimeSnapshot:
        """Read counters and worker assignments atomically, without advancing timers."""
        with self._changed:
            workers = tuple(self._health.values())
            return RuntimeSnapshot(
                backend=self._backend,
                closed=self._closed,
                stopped=self._stopped,
                accepted=self._accepted,
                started=self._started,
                attempts=self._attempts,
                retries_scheduled=self._retries,
                completed=self._outcomes[JobState.COMPLETED],
                failed=self._outcomes[JobState.FAILED],
                cancelled=self._outcomes[JobState.CANCELLED],
                expired=self._outcomes[JobState.EXPIRED],
                pending=len(self._pending),
                reserved=self._reserved,
                capacity=self._capacity,
                workers=workers,
            )

    def _event(
        self,
        event: str,
        job: Job[Any] | None = None,
        *,
        worker_id: str | None = None,
        state: str | None = None,
        deadline: float | None = None,
    ) -> None:
        if _log.isEnabledFor(logging.INFO):
            try:
                _log.info(
                    event,
                    extra={
                        "event": event,
                        "backend": self._backend,
                        "worker_id": worker_id,
                        "job_id": str(job.id) if job else None,
                        "attempt": job.attempts if job else None,
                        "state": job.state.value if job else state,
                        "deadline": deadline,
                    },
                )
            except Exception:  # noqa: BLE001, S110
                # Application-owned formatting and handlers cannot decide job outcomes.
                pass

    def _leave_pending(
        self, job: Job[Any], state: JobState, deadline: float | None = None
    ) -> None:
        self._outcomes[state] += 1
        job._leave_queue(state)
        self._event(state.value, job, deadline=deadline)

    def _fail_job(self, job: Job[Any], error: BaseException) -> None:
        self._outcomes[JobState.FAILED] += 1
        job._fail(error)
        self._event("failed", job)

    def _no_workers(self) -> None:
        self._closed = True
        for item in self._pending.values():
            self._fail_job(
                item.job, WorkerLostError("no worker available to run the queued job")
            )
        self._pending.clear()
        self._changed.notify_all()

    def _worker_exit(
        self, worker_id: str, expected: bool, code: int | None = None
    ) -> None:
        state = WorkerState.EXITED if expected else WorkerState.FAILED
        previous = self._health[worker_id].state
        self._health[worker_id] = WorkerHealth(worker_id, state, exit_code=code)
        if previous is not state:
            self._event(f"worker_{state.value}", worker_id=worker_id, state=state.value)

    def _ensure_timer(self) -> None:
        """Thread pools override this when deadlines need an independent timer."""

    def _expire(self, now: float) -> None:
        due = [
            key
            for key, item in self._pending.items()
            if item.deadline is not None and item.deadline <= now
        ]
        if due:
            for key in due:
                item = self._pending.pop(key)
                self._leave_pending(item.job, JobState.EXPIRED, item.deadline)
            self._signal()

    def _expire_due(self) -> None:
        self._expire(monotonic())

    def _deadline_timeout(self) -> float | None:
        earliest = min(
            (
                item.deadline
                for item in self._pending.values()
                if item.deadline is not None
            ),
            default=None,
        )
        return None if earliest is None else max(0, earliest - monotonic())

    @abstractmethod
    def _prepare(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Payload: ...

    def _signal(self) -> None:
        self._changed.notify_all()

    def submit(
        self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs
    ) -> Job[T]:
        return self.submit_scheduled(Schedule(), fn, *args, **kwargs)

    def submit_with_timeout(
        self,
        timeout: float | None,
        fn: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Job[T]:
        return self.submit_scheduled(
            Schedule(submit_timeout=timeout), fn, *args, **kwargs
        )

    def submit_scheduled(
        self,
        schedule: Schedule,
        fn: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Job[T]:
        if not isinstance(schedule, Schedule):
            raise TypeError("schedule must be a Schedule")
        with self._changed:
            if self._closed:
                raise RuntimeClosedError("runtime is closed")
        payload = self._prepare(fn, args, kwargs)
        with self._changed:
            ready = self._changed.wait_for(self._can_admit, schedule.submit_timeout)
            if self._closed:
                raise RuntimeClosedError("runtime is closed")
            if not ready:
                raise QueueFullError("job queue is full")
            job = Job[T](self._cancel)
            deadline = None
            if schedule.start_within is not None:
                if schedule.start_within == 0:
                    self._accepted += 1
                    deadline = monotonic()
                    self._event("accepted", job, deadline=deadline)
                    self._leave_pending(job, JobState.EXPIRED, deadline)
                    return job
                self._ensure_timer()
                deadline = monotonic() + schedule.start_within
            self._pending[job.id] = WorkItem(job, payload, schedule, deadline)
            self._accepted += 1
            self._event("accepted", job, deadline=deadline)
            self._signal()
            return job

    def _can_admit(self) -> bool:
        self._expire_due()
        return self._closed or len(self._pending) + self._reserved < self._capacity

    def _claim(self, worker_id: str) -> WorkItem[Payload] | None:
        """Called under the queue condition. Dict insertion order breaks ties."""
        now = monotonic()
        while True:
            self._expire(now)
            eligible = [item for item in self._pending.values() if item.ready_at <= now]
            if not eligible:
                return None
            oldest = eligible[0]
            item = oldest
            if self._bypasses < self._priority_burst:
                item = max(eligible, key=lambda entry: entry.schedule.priority)
            if item.deadline is None:
                break
            now = monotonic()
            if now < item.deadline:
                break
            # Expire and reselect before consuming a bypass or reserving capacity.
        self._bypasses = self._bypasses + 1 if item is not oldest else 0
        del self._pending[item.job.id]
        item.job._leave_queue(JobState.RUNNING)
        attempts = item.job.attempts
        self._attempts += 1
        self._started += int(attempts == 1)
        item.worker_id = worker_id
        self._health[worker_id] = WorkerHealth(
            worker_id, WorkerState.RUNNING, item.job.id
        )
        item.reserved = attempts <= item.schedule.max_retries
        if item.reserved:
            self._reserved += 1
        self._event("claimed", item.job, worker_id=worker_id, deadline=item.deadline)
        self._changed.notify_all()
        return item

    def _retry_timeout(self) -> float | None:
        earliest = min(
            (item.ready_at for item in self._pending.values() if item.ready_at),
            default=None,
        )
        return None if earliest is None else max(0, earliest - monotonic())

    def _finish(
        self,
        item: WorkItem[Payload],
        value: Any = None,
        error: BaseException | None = None,
        retryable: bool = False,
    ) -> None:
        with self._changed:
            self._health[item.worker_id] = WorkerHealth(
                item.worker_id, WorkerState.IDLE
            )
            if item.reserved:
                self._reserved -= 1
                item.reserved = False
                if retryable and not self._cancel_retries:
                    now = monotonic()
                    if item.deadline is not None and item.deadline <= now:
                        self._leave_pending(item.job, JobState.EXPIRED, item.deadline)
                    else:
                        if item.deadline is not None:
                            try:
                                self._ensure_timer()
                            except RuntimeError as timer_error:
                                self._fail_job(item.job, timer_error)
                                self._signal()
                                return
                        item.ready_at = now + item.schedule.retry_delay
                        item.job._retry(self._cancel)
                        self._pending[item.job.id] = item
                        self._retries += 1
                        self._event(
                            "retry_scheduled",
                            item.job,
                            worker_id=item.worker_id,
                            deadline=item.deadline,
                        )
                    self._signal()
                    return
                self._changed.notify_all()
            if error is None:
                self._outcomes[JobState.COMPLETED] += 1
                item.job._complete(value)
                self._event("completed", item.job, worker_id=item.worker_id)
            else:
                self._fail_job(item.job, error)

    def _cancel(self, job_id: UUID) -> bool:
        with self._changed:
            self._expire(monotonic())
            item = self._pending.pop(job_id, None)
            if item is None:
                return False
            self._leave_pending(item.job, JobState.CANCELLED)
            self._signal()
            return True

    def _close(self, cancel_pending: bool) -> None:
        with self._changed:
            self._expire(monotonic())
            changed = not self._closed or bool(cancel_pending and self._pending)
            self._closed = True
            if cancel_pending:
                self._cancel_retries = True
                discarded, self._pending = self._pending, {}
                for item in discarded.values():
                    self._leave_pending(item.job, JobState.CANCELLED)
            if changed:
                self._signal()
