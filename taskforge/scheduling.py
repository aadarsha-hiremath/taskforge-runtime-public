"""Parent-side queue policy shared by both execution backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from threading import Condition
from typing import Any, Generic, ParamSpec, TypeVar
from uuid import UUID

from .errors import QueueFullError, RuntimeClosedError
from .job import Job, JobState

Payload = TypeVar("Payload")
P = ParamSpec("P")
T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Schedule:
    priority: int = 0
    submit_timeout: float | None = None

    def __post_init__(self) -> None:
        if type(self.priority) is not int:
            raise ValueError("priority must be an integer")


@dataclass(slots=True)
class WorkItem(Generic[Payload]):
    job: Job[Any]
    payload: Payload
    schedule: Schedule


class ScheduledPool(ABC, Generic[Payload]):
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
        self._pending: dict[UUID, WorkItem[Payload]] = {}

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
            ready = self._changed.wait_for(
                lambda: self._closed or len(self._pending) < self._capacity,
                schedule.submit_timeout,
            )
            if self._closed:
                raise RuntimeClosedError("runtime is closed")
            if not ready:
                raise QueueFullError("job queue is full")
            job = Job[T](self._cancel)
            self._pending[job.id] = WorkItem(job, payload, schedule)
            self._signal()
            return job

    def _claim(self) -> WorkItem[Payload] | None:
        """Called under the queue condition. Dict insertion order breaks ties."""
        if not self._pending:
            return None
        oldest = next(iter(self._pending.values()))
        item = oldest
        if self._bypasses < self._priority_burst:
            item = max(
                self._pending.values(), key=lambda entry: entry.schedule.priority
            )
        self._bypasses = self._bypasses + 1 if item is not oldest else 0
        del self._pending[item.job.id]
        item.job._leave_queue(JobState.RUNNING)
        self._changed.notify_all()
        return item

    def _cancel(self, job_id: UUID) -> bool:
        with self._changed:
            item = self._pending.pop(job_id, None)
            if item is None:
                return False
            item.job._leave_queue(JobState.CANCELLED)
            self._signal()
            return True

    def _close(self, cancel_pending: bool) -> None:
        with self._changed:
            changed = not self._closed or bool(cancel_pending and self._pending)
            self._closed = True
            if cancel_pending:
                discarded, self._pending = self._pending, {}
                for item in discarded.values():
                    item.job._leave_queue(JobState.CANCELLED)
            if changed:
                self._signal()
