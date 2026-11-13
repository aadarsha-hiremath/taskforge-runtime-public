from collections import OrderedDict
from collections.abc import Callable
from functools import partial
from threading import Condition, Thread, current_thread
from types import TracebackType
from typing import Any, ParamSpec, Self, TypeVar
from uuid import UUID, uuid4

from .errors import QueueFullError, RuntimeClosedError
from .job import Job, JobState
from .worker import WorkItem, worker_loop

P = ParamSpec("P")
T = TypeVar("T")


class Runtime:
    """Start a fixed pool. Queue capacity counts waiting jobs only."""

    def __init__(self, workers: int = 2, queue_capacity: int = 16) -> None:
        for name, value in (("workers", workers), ("queue_capacity", queue_capacity)):
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        self._capacity = queue_capacity
        self._changed = Condition()
        self._closed = False
        self._pending: OrderedDict[UUID, WorkItem[Any]] = OrderedDict()
        self._workers: list[Thread] = []
        pool_id = uuid4().hex[:8]
        try:
            for index in range(workers):
                thread = Thread(
                    target=worker_loop,
                    args=(self._take,),
                    name=f"taskforge-{pool_id}-{index}",
                    daemon=False,
                )
                thread.start()
                self._workers.append(thread)
        except BaseException:
            self.shutdown()
            raise

    def submit(
        self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs
    ) -> Job[T]:
        """Wait for capacity, then enqueue the call."""
        return self.submit_with_timeout(None, fn, *args, **kwargs)

    def submit_with_timeout(
        self,
        timeout: float | None,
        fn: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Job[T]:
        """Limit the wait for queue space. A zero timeout tries once."""
        if not callable(fn):
            raise TypeError("fn must be callable")
        with self._changed:
            ready = self._changed.wait_for(
                lambda: self._closed or len(self._pending) < self._capacity, timeout
            )
            if self._closed:
                raise RuntimeClosedError("runtime is shutting down")
            if not ready:
                raise QueueFullError("job queue is full")
            job = Job[T](self._cancel)
            self._pending[job.id] = WorkItem(job, partial(fn, *args, **kwargs))
            self._changed.notify_all()
            return job

    def _take(self) -> WorkItem[Any] | None:
        with self._changed:
            self._changed.wait_for(lambda: self._closed or bool(self._pending))
            if not self._pending:
                return None
            _, item = self._pending.popitem(last=False)
            item.job._leave_queue(JobState.RUNNING)
            self._changed.notify_all()
            return item

    def _cancel(self, job_id: UUID) -> bool:
        with self._changed:
            item = self._pending.pop(job_id, None)
            if item is None:
                return False
            item.job._leave_queue(JobState.CANCELLED)
            self._changed.notify_all()
        return True

    def shutdown(self, *, wait: bool = True, cancel_pending: bool = False) -> None:
        """Close admission and drain or cancel queued work. Optionally join workers."""
        if wait and current_thread() in self._workers:
            raise RuntimeError("a worker cannot join its own pool; use wait=False")
        with self._changed:
            self._closed = True
            if cancel_pending:
                discarded, self._pending = self._pending, OrderedDict()
                for item in discarded.values():
                    item.job._leave_queue(JobState.CANCELLED)
            self._changed.notify_all()
        if wait:
            for thread in self._workers:
                thread.join()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.shutdown()
