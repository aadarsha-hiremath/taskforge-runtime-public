from __future__ import annotations

import multiprocessing as mp
import pickle
import sys
from collections.abc import Callable
from dataclasses import dataclass
from math import ceil, isfinite
from multiprocessing.connection import wait as wait_connections
from multiprocessing.process import BaseProcess
from threading import Thread, current_thread
from time import monotonic
from types import TracebackType
from typing import Any, Self

from .errors import (
    RemoteTaskError,
    SerializationError,
    WorkerLostError,
)
from .observability import WorkerHealth, WorkerState
from .process_worker import (
    CODEC_ERROR,
    READY,
    TASK_ERROR,
    TASK_EXIT,
    VALUE,
    PipeEnd,
    encode_call,
    process_worker,
)
from .scheduling import ScheduledPool, WorkItem

# Connection waits can use signed 32-bit milliseconds on POSIX and Windows.
# Cap each OS wait well below that limit, keeping the original logical deadline.
_MAX_WAIT_SECONDS = 86_400.0


def _connection_timeout(remaining: float) -> float:
    timeout = max(0.0, min(remaining, _MAX_WAIT_SECONDS))
    if sys.platform == "win32":
        return ceil(timeout * 1000) / 1000
    return timeout


@dataclass(slots=True)
class _Request:
    data: bytes
    # Manager proxies use reference-counted server objects. Keep the original
    # inputs alive until every child-side reconstruction has finished.
    inputs: tuple[tuple[Any, ...], dict[str, Any]]


@dataclass
class _Worker:
    process: BaseProcess
    pipe: PipeEnd
    worker_id: str
    item: WorkItem[_Request] | None = None


class ProcessRuntime(ScheduledPool[_Request]):
    """Spawn workers and wait for their handshakes before accepting submissions."""

    _backend = "process"

    def __init__(
        self,
        workers: int = 2,
        queue_capacity: int = 16,
        *,
        priority_burst: int = 8,
        startup_timeout: float = 30.0,
    ) -> None:
        # Python 3.11 on Windows has a limit on handles in a single wait set.
        if type(workers) is not int or not 1 <= workers <= 60:
            raise ValueError("workers must be an integer between 1 and 60")
        super().__init__(queue_capacity, priority_burst)
        if not isfinite(startup_timeout) or startup_timeout <= 0:
            raise ValueError("startup_timeout must be finite and positive")
        ctx = mp.get_context("spawn")
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
                worker_id = str(process.pid)
                self._workers.append(_Worker(process, parent, worker_id))
                self._health[worker_id] = WorkerHealth(worker_id, WorkerState.IDLE)
            for worker in self._workers:
                while not worker.pipe.poll(_connection_timeout(deadline - monotonic())):
                    if monotonic() >= deadline:
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

    def _prepare(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> _Request:
        return _Request(encode_call(fn, args, kwargs), (args, kwargs))

    def _signal(self) -> None:
        super()._signal()
        # Caller holds the condition. At most one byte-sized message is outstanding.
        if not self._wake_pending:
            self._wake_write.send_bytes(b"!")
            self._wake_pending = True

    def _lose(self, worker: _Worker, reason: str) -> None:
        with self._changed:
            if worker.pipe.closed:
                return
            worker.pipe.close()
            if worker.item is not None:
                self._finish(
                    worker.item,
                    error=WorkerLostError(f"worker {worker.worker_id}: {reason}"),
                )
                worker.item = None
            self._worker_exit(worker.worker_id, False)

    def _receive(self, worker: _Worker) -> None:
        try:
            reply = worker.pipe.recv_bytes()
        except (EOFError, OSError) as error:
            self._lose(worker, f"connection lost ({type(error).__name__})")
            return
        item = worker.item
        if item is None or reply[:1] not in (VALUE, TASK_ERROR, TASK_EXIT, CODEC_ERROR):
            self._lose(worker, "invalid worker reply")
            return
        if reply[:1] == VALUE:
            try:
                value = pickle.loads(reply[1:])
            except BaseException as error:  # noqa: BLE001
                self._finish(
                    item,
                    error=SerializationError(
                        f"could not decode worker result ({type(error).__name__})"
                    ),
                )
            else:
                self._finish(item, value)
        else:
            detail = reply[1:].decode("utf-8")
            error_type = (
                SerializationError if reply[:1] == CODEC_ERROR else RemoteTaskError
            )
            self._finish(
                item, error=error_type(detail), retryable=reply[:1] == TASK_ERROR
            )
        worker.item = None

    def _dispatch(self, worker: _Worker) -> None:
        with self._changed:
            item = self._claim(worker.worker_id)
            if item is None:
                if self._closed and not self._pending:
                    self._health[worker.worker_id] = WorkerHealth(
                        worker.worker_id, WorkerState.STOPPING
                    )
                    worker.pipe.close()
                return
            worker.item = item
        try:
            worker.pipe.send_bytes(item.payload.data)
        except (EOFError, OSError) as error:
            self._lose(worker, f"dispatch failed ({type(error).__name__})")

    def _serve(self) -> None:
        try:
            while True:
                with self._changed:
                    self._expire_due()
                for worker in self._workers:
                    if worker.item is None and not worker.pipe.closed:
                        self._dispatch(worker)
                live = {w.pipe: w for w in self._workers if not w.pipe.closed}
                if not live:
                    return
                # After EOF, reap the process as soon as the OS reports its exit.
                retiring = {
                    w.process.sentinel: w for w in self._workers if w.pipe.closed
                }
                with self._changed:
                    idle = any(worker.item is None for worker in live.values())
                    delays = (
                        self._deadline_timeout(),
                        self._retry_timeout() if idle else None,
                    )
                    timeout = min(
                        (delay for delay in delays if delay is not None), default=None
                    )
                ready = wait_connections(
                    [self._wake_read, *live, *retiring],
                    None if timeout is None else _connection_timeout(timeout),
                )
                if self._wake_read in ready:
                    with self._changed:
                        self._wake_read.recv_bytes()
                        self._wake_pending = False
                for pipe, worker in live.items():
                    if pipe in ready:
                        self._receive(worker)
                for sentinel, worker in retiring.items():
                    if sentinel in ready:
                        self._reap(worker)
                        self._workers.remove(worker)
        finally:
            with self._changed:
                self._no_workers()
            for worker in self._workers:
                self._lose(worker, "controller stopped before receiving a result")
            self._cleanup()

    def _reap(self, worker: _Worker) -> None:
        worker.process.join()
        code = worker.process.exitcode
        worker.process.close()
        with self._changed:
            expected = (
                self._health[worker.worker_id].state is WorkerState.STOPPING
                and code == 0
            )
            self._worker_exit(worker.worker_id, expected, code)

    def _cleanup(self) -> None:
        self._wake_read.close()
        self._wake_write.close()
        for worker in self._workers:
            worker.pipe.close()
            self._reap(worker)
        with self._changed:
            self._stopped = True

    def shutdown(
        self,
        *,
        wait: bool = True,
        cancel_pending: bool = False,
        timeout: float | None = None,
    ) -> None:
        if wait and current_thread() is self._controller:
            raise RuntimeError("the controller cannot join itself")
        if wait and timeout is not None and not isfinite(timeout):
            raise ValueError("shutdown timeout must be finite or None")
        self._close(cancel_pending)
        if not wait:
            return
        if timeout is None:
            self._controller.join()
            return
        deadline = monotonic() + timeout
        while self._controller.is_alive():
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError("process runtime is still shutting down")
            self._controller.join(min(remaining, _MAX_WAIT_SECONDS))

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.shutdown()
