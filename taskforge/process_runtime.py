from __future__ import annotations

import multiprocessing as mp
import pickle
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from math import isfinite
from multiprocessing.connection import wait as wait_connections
from multiprocessing.process import BaseProcess
from threading import Condition, Thread, current_thread
from time import monotonic
from types import TracebackType
from typing import Any, ParamSpec, Self, TypeVar
from uuid import UUID

from .errors import (
    QueueFullError,
    RemoteTaskError,
    RuntimeClosedError,
    SerializationError,
    WorkerLostError,
)
from .job import Job, JobState
from .process_worker import (
    CODEC_ERROR,
    READY,
    TASK_ERROR,
    VALUE,
    PipeEnd,
    encode_call,
    process_worker,
)

P = ParamSpec("P")
T = TypeVar("T")


@dataclass
class _Worker:
    process: BaseProcess
    pipe: PipeEnd
    job: Job[Any] | None = None


class ProcessRuntime:
    """Spawn workers and wait for their handshakes before accepting submissions."""

    def __init__(
        self,
        workers: int = 2,
        queue_capacity: int = 16,
        *,
        startup_timeout: float = 30.0,
    ) -> None:
        # Python 3.11 on Windows has a limit on handles in a single wait set.
        if type(workers) is not int or not 1 <= workers <= 60:
            raise ValueError("workers must be an integer between 1 and 60")
        if type(queue_capacity) is not int or queue_capacity < 1:
            raise ValueError("queue_capacity must be a positive integer")
        if not isfinite(startup_timeout) or startup_timeout <= 0:
            raise ValueError("startup_timeout must be finite and positive")
        ctx = mp.get_context("spawn")
        self._capacity = queue_capacity
        self._changed = Condition()
        self._closed = False
        self._pending: OrderedDict[UUID, tuple[Job[Any], bytes]] = OrderedDict()
        self._wake_read, self._wake_write = ctx.Pipe(duplex=False)
        self._wake_pending = False
        self._workers: list[_Worker] = []
        self._controller = Thread(
            target=self._serve, name="taskforge-process-controller", daemon=False
        )
        deadline = monotonic() + startup_timeout
        try:
            for _ in range(workers):
                parent, child = ctx.Pipe()
                process = ctx.Process(
                    target=process_worker, args=(child,), daemon=False
                )
                try:
                    process.start()
                except BaseException:
                    parent.close()
                    process.close()
                    raise
                finally:
                    child.close()
                self._workers.append(_Worker(process, parent))
            for worker in self._workers:
                if not worker.pipe.poll(max(0, deadline - monotonic())):
                    raise TimeoutError("process startup handshake timed out")
                try:
                    handshake = worker.pipe.recv_bytes()
                except (EOFError, OSError) as error:
                    raise WorkerLostError("worker exited during startup") from error
                if handshake != READY:
                    raise WorkerLostError("invalid worker startup handshake")
            self._controller.start()
        except BaseException:
            # Construction has not admitted any user tasks.
            for worker in self._workers:
                if worker.process.is_alive():
                    worker.process.terminate()
            self._cleanup()
            raise

    def submit(
        self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs
    ) -> Job[T]:
        return self.submit_with_timeout(None, fn, *args, **kwargs)

    def submit_with_timeout(
        self,
        timeout: float | None,
        fn: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Job[T]:
        with self._changed:
            if self._closed:
                raise RuntimeClosedError("process runtime is closed")
        request = encode_call(fn, args, kwargs)
        with self._changed:
            ready = self._changed.wait_for(
                lambda: self._closed or len(self._pending) < self._capacity, timeout
            )
            if self._closed:
                raise RuntimeClosedError("process runtime is closed")
            if not ready:
                raise QueueFullError("job queue is full")
            job = Job[T](self._cancel)
            self._pending[job.id] = job, request
            self._wake()
            return job

    def _cancel(self, job_id: UUID) -> bool:
        with self._changed:
            item = self._pending.pop(job_id, None)
            if item is None:
                return False
            item[0]._leave_queue(JobState.CANCELLED)
            self._changed.notify_all()
            return True

    def _wake(self) -> None:
        # Caller holds the condition. At most one byte-sized message is outstanding.
        if not self._wake_pending:
            self._wake_write.send_bytes(b"!")
            self._wake_pending = True

    def _lose(self, worker: _Worker, reason: str) -> None:
        worker.pipe.close()
        if worker.job is not None:
            worker.job._fail(WorkerLostError(f"worker {worker.process.pid}: {reason}"))
            worker.job = None

    def _receive(self, worker: _Worker) -> None:
        try:
            reply = worker.pipe.recv_bytes()
        except (EOFError, OSError) as error:
            self._lose(worker, f"connection lost ({type(error).__name__})")
            return
        job = worker.job
        if job is None or reply[:1] not in (VALUE, TASK_ERROR, CODEC_ERROR):
            self._lose(worker, "invalid worker reply")
            return
        if reply[:1] == VALUE:
            try:
                value = pickle.loads(reply[1:])
            except BaseException as error:  # noqa: BLE001
                job._fail(
                    SerializationError(
                        f"could not decode worker result ({type(error).__name__})"
                    )
                )
            else:
                job._complete(value)
        else:
            detail = reply[1:].decode("utf-8")
            job._fail(
                RemoteTaskError(detail)
                if reply[:1] == TASK_ERROR
                else SerializationError(detail)
            )
        worker.job = None

    def _dispatch(self, worker: _Worker) -> None:
        with self._changed:
            if not self._pending:
                if self._closed:
                    worker.pipe.close()  # EOF asks an idle worker to exit.
                return
            _, (job, request) = self._pending.popitem(last=False)
            worker.job = job
            job._leave_queue(JobState.RUNNING)
            self._changed.notify_all()
        try:
            worker.pipe.send_bytes(request)
        except (EOFError, OSError) as error:
            self._lose(worker, f"dispatch failed ({type(error).__name__})")

    def _serve(self) -> None:
        try:
            while True:
                for worker in self._workers:
                    if worker.job is None and not worker.pipe.closed:
                        self._dispatch(worker)
                live = {w.pipe: w for w in self._workers if not w.pipe.closed}
                if not live:
                    return
                # After EOF, reap the process as soon as the OS reports its exit.
                retiring = {
                    w.process.sentinel: w for w in self._workers if w.pipe.closed
                }
                ready = wait_connections([self._wake_read, *live, *retiring])
                if self._wake_read in ready:
                    with self._changed:
                        self._wake_read.recv_bytes()
                        self._wake_pending = False
                for pipe, worker in live.items():
                    if pipe in ready:
                        self._receive(worker)
                for sentinel, worker in retiring.items():
                    if sentinel in ready:
                        worker.process.join()
                        worker.process.close()
                        self._workers.remove(worker)
        finally:
            with self._changed:
                self._closed = True
                for job, _ in self._pending.values():
                    job._fail(
                        WorkerLostError("no worker available to run the queued job")
                    )
                self._pending.clear()
                self._changed.notify_all()
            for worker in self._workers:
                self._lose(worker, "controller stopped before receiving a result")
            self._cleanup()

    def _cleanup(self) -> None:
        self._wake_read.close()
        self._wake_write.close()
        for worker in self._workers:
            worker.pipe.close()
            worker.process.join()
            worker.process.close()

    def shutdown(
        self,
        *,
        wait: bool = True,
        cancel_pending: bool = False,
        timeout: float | None = None,
    ) -> None:
        if wait and current_thread() is self._controller:
            raise RuntimeError("the controller cannot join itself")
        with self._changed:
            if cancel_pending:
                for job, _ in self._pending.values():
                    job._leave_queue(JobState.CANCELLED)
                self._pending.clear()
            if not self._closed:
                self._closed = True
                self._wake()
            self._changed.notify_all()
        if wait:
            self._controller.join(timeout)
            if self._controller.is_alive():
                raise TimeoutError("process runtime is still shutting down")

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.shutdown()
