"""Immutable observations of the scheduler's last committed state."""

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID


class WorkerState(StrEnum):
    IDLE = "idle"
    RUNNING = "running"
    STOPPING = "stopping"
    EXITED = "exited"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class WorkerHealth:
    worker_id: str
    state: WorkerState
    job_id: UUID | None = None
    exit_code: int | None = None


@dataclass(frozen=True, slots=True)
class RuntimeSnapshot:
    backend: str
    closed: bool
    stopped: bool
    accepted: int
    started: int
    attempts: int
    retries_scheduled: int
    completed: int
    failed: int
    cancelled: int
    expired: int
    pending: int
    reserved: int
    capacity: int
    workers: tuple[WorkerHealth, ...]

    @property
    def running(self) -> int:
        return sum(worker.state is WorkerState.RUNNING for worker in self.workers)

    @property
    def available_workers(self) -> int:
        return sum(worker.state is WorkerState.IDLE for worker in self.workers)

    @property
    def queue_depth(self) -> int:
        return self.pending
