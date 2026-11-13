from collections.abc import Callable
from functools import partial
from threading import Thread, current_thread
from types import TracebackType
from typing import Any, Self
from uuid import uuid4

from .scheduling import ScheduledPool, WorkItem
from .worker import worker_loop


class Runtime(ScheduledPool[Callable[[], Any]]):
    """Start a fixed pool. Queue capacity counts waiting jobs only."""

    def __init__(
        self, workers: int = 2, queue_capacity: int = 16, *, priority_burst: int = 8
    ) -> None:
        if type(workers) is not int or workers < 1:
            raise ValueError("workers must be a positive integer")
        super().__init__(queue_capacity, priority_burst)
        self._workers: list[Thread] = []
        self._timer: Thread | None = None
        pool_id = uuid4().hex[:8]
        try:
            for index in range(workers):
                thread = Thread(
                    target=worker_loop,
                    args=(self._take, self._finish),
                    name=f"taskforge-{pool_id}-{index}",
                    daemon=False,
                )
                thread.start()
                self._workers.append(thread)
        except BaseException:
            self.shutdown()
            raise

    def _prepare(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Callable[[], Any]:
        if not callable(fn):
            raise TypeError("fn must be callable")
        return partial(fn, *args, **kwargs)

    def _ensure_timer(self) -> None:
        if self._timer is None:
            timer = Thread(
                target=self._watch_deadlines, name="taskforge-deadlines", daemon=False
            )
            timer.start()
            self._timer = timer

    def _watch_deadlines(self) -> None:
        with self._changed:
            while True:
                self._expire_due()
                timeout = self._deadline_timeout()
                if timeout is None:
                    self._timer = None
                    return
                self._changed.wait(timeout)

    def _take(self) -> WorkItem[Callable[[], Any]] | None:
        with self._changed:
            while True:
                item = self._claim()
                if item is not None or (self._closed and not self._pending):
                    return item
                self._changed.wait(self._retry_timeout())

    def shutdown(self, *, wait: bool = True, cancel_pending: bool = False) -> None:
        """Close admission and drain or cancel queued work. Optionally join workers."""
        if wait and current_thread() in self._workers:
            raise RuntimeError("a worker cannot join its own pool; use wait=False")
        self._close(cancel_pending)
        if wait:
            for thread in self._workers:
                thread.join()
            with self._changed:
                timer = self._timer
            if timer is not None:
                timer.join()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.shutdown()
