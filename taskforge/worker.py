from collections.abc import Callable
from typing import Any

from .scheduling import WorkItem

__all__ = ["WorkItem", "worker_loop"]


def worker_loop(take: Callable[[], WorkItem[Callable[[], Any]] | None]) -> None:
    while (item := take()) is not None:
        try:
            value = item.payload()
        except BaseException as error:  # noqa: BLE001
            item.job._fail(error)
        else:
            item.job._complete(value)
            del value
        del item
