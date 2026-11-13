from collections.abc import Callable
from typing import Any

from .scheduling import WorkItem

__all__ = ["WorkItem", "worker_loop"]


ThreadItem = WorkItem[Callable[[], Any]]


def worker_loop(
    take: Callable[[], ThreadItem | None],
    finish: Callable[[ThreadItem, Any, BaseException | None, bool], None],
) -> None:
    while (item := take()) is not None:
        try:
            value = item.payload()
        except BaseException as error:  # noqa: BLE001
            finish(item, None, error, isinstance(error, Exception))
        else:
            finish(item, value, None, False)
            del value
        del item
