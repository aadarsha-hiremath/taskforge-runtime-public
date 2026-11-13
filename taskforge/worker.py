from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from .job import Job

T = TypeVar("T")


@dataclass(slots=True)
class WorkItem(Generic[T]):
    job: Job[T]
    invoke: Callable[[], T]

    def run(self) -> None:
        try:
            value = self.invoke()
        except BaseException as error:  # noqa: BLE001
            # Even a callable raising SystemExit must leave a terminal result.
            self.job._fail(error)
        else:
            self.job._complete(value)


def worker_loop(take: Callable[[], WorkItem[Any] | None]) -> None:
    while (item := take()) is not None:
        item.run()
        del item  # Release the callable and arguments before waiting for work.
