from collections.abc import Callable
from enum import StrEnum
from threading import Condition
from typing import Generic, TypeVar, cast
from uuid import UUID, uuid4

from .errors import DeadlineExpiredError, JobCancelledError

T = TypeVar("T")


class JobState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class Job(Generic[T]):
    """A thread-safe result handle returned by Runtime.submit()."""

    def __init__(self, cancel_pending: Callable[[UUID], bool]) -> None:
        self._id = uuid4()
        self._changed = Condition()
        self._state = JobState.PENDING
        self._cancel_pending: Callable[[UUID], bool] | None = cancel_pending
        self._value: T | None = None
        self._error: BaseException | None = None

    @property
    def id(self) -> UUID:
        return self._id

    @property
    def state(self) -> JobState:
        with self._changed:
            return self._state

    def cancel(self) -> bool:
        """Return True only when this call cancels a queued job."""
        with self._changed:
            cancel_pending = self._cancel_pending
        # Release the job lock before entering the scheduler.
        return cancel_pending is not None and cancel_pending(self._id)

    def result(self, timeout: float | None = None) -> T:
        """Wait for a terminal state, then return the value or raise the error."""
        with self._changed:
            if not self._changed.wait_for(
                lambda: self._state not in (JobState.PENDING, JobState.RUNNING),
                timeout,
            ):
                raise TimeoutError(f"job {self._id} is still {self._state}")
            if self._state is JobState.CANCELLED:
                raise JobCancelledError(f"job {self._id} was cancelled")
            if self._state is JobState.EXPIRED:
                raise DeadlineExpiredError(f"job {self._id} missed its start deadline")
            if self._error is not None:
                raise self._error
            # COMPLETED includes tasks that legitimately returned None.
            return cast(T, self._value)

    def _leave_queue(self, state: JobState) -> None:
        # Called only by the scheduler while it holds the queue lock.
        with self._changed:
            self._cancel_pending = None
            self._state = state
            if state in (JobState.CANCELLED, JobState.EXPIRED):
                self._changed.notify_all()

    def _complete(self, value: T) -> None:
        with self._changed:
            self._value = value
            self._state = JobState.COMPLETED
            self._changed.notify_all()

    def _fail(self, error: BaseException) -> None:
        with self._changed:
            self._cancel_pending = None  # The last process can die with jobs queued.
            self._error = error
            self._state = JobState.FAILED
            self._changed.notify_all()
