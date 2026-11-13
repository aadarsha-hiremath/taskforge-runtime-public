from collections.abc import Callable
from functools import partial
from threading import Thread, current_thread, local
from types import TracebackType
from typing import Any, Self
from uuid import uuid4

from .errors import WorkerLostError
from .observability import WorkerHealth, WorkerState
from .scheduling import ScheduledPool, WorkItem
from .worker import worker_loop


class Runtime(ScheduledPool[Callable[[], Any]]):
    """Start a fixed pool. Queue capacity counts waiting jobs only."""

    _backend = "thread"

    def __init__(
        self, workers: int = 2, queue_capacity: int = 16, *, priority_burst: int = 8
    ) -> None:
        if type(workers) is not int or workers < 1:
            raise ValueError("workers must be a positive integer")
        super().__init__(queue_capacity, priority_burst)
        self._workers: list[Thread] = []
        self._worker_context = local()
        self._timer: Thread | None = None
        pool_id = uuid4().hex[:8]
        try:
            for index in range(workers):
                worker_id = f"taskforge-{pool_id}-{index}"
                thread = Thread(
                    target=self._run_worker,
                    args=(worker_id,),
                    name=worker_id,
                    daemon=False,
                )
                with self._changed:
                    self._health[thread.name] = WorkerHealth(
                        thread.name, WorkerState.IDLE
                    )
                    try:
                        thread.start()
                    except BaseException:
                        del self._health[thread.name]
                        raise
                self._workers.append(thread)
        except BaseException:
            self.shutdown()
            raise

    def _run_worker(self, worker_id: str) -> None:
        self._worker_context.worker_id = worker_id
        expected = False
        try:
            worker_loop(self._take, self._finish)
            expected = True
        except BaseException:  # noqa: BLE001
            # The loop itself failed. Task exceptions are handled inside worker_loop.
            # Never replay an assigned attempt whose effects are uncertain.
            with self._changed:
                item = getattr(self._worker_context, "item", None)
                if item is not None and self._health[worker_id].job_id == item.job.id:
                    self._finish(
                        item,
                        error=WorkerLostError(f"thread worker {worker_id} stopped"),
                    )
        finally:
            with self._changed:
                self._worker_exit(worker_id, expected)
                if all(
                    w.state in (WorkerState.EXITED, WorkerState.FAILED)
                    for w in self._health.values()
                ):
                    if not expected:
                        self._no_workers()
                    self._stopped = True

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
            self._worker_context.item = None
            while True:
                item = self._claim(self._worker_context.worker_id)
                if item is not None or (self._closed and not self._pending):
                    self._worker_context.item = item
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
