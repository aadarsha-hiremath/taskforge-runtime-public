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

    def _prepare(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Callable[[], Any]:
        if not callable(fn):
            raise TypeError("fn must be callable")
        return partial(fn, *args, **kwargs)

    def _take(self) -> WorkItem[Callable[[], Any]] | None:
        with self._changed:
            self._changed.wait_for(lambda: self._closed or bool(self._pending))
            return self._claim()

    def shutdown(self, *, wait: bool = True, cancel_pending: bool = False) -> None:
        """Close admission and drain or cancel queued work. Optionally join workers."""
        if wait and current_thread() in self._workers:
            raise RuntimeError("a worker cannot join its own pool; use wait=False")
        self._close(cancel_pending)
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
