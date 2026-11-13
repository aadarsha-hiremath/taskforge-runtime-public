"""Wire protocol for a private, trusted parent/child pipe."""

from __future__ import annotations

import pickle
import sys
import traceback
from collections.abc import Callable
from inspect import isbuiltin, isfunction
from typing import Any

from .errors import SerializationError

__all__ = [
    "CODEC_ERROR",
    "READY",
    "TASK_ERROR",
    "VALUE",
    "PipeEnd",
    "encode_call",
    "process_worker",
]

if sys.platform == "win32":
    from multiprocessing.connection import PipeConnection as PipeEnd
else:
    from multiprocessing.connection import Connection as PipeEnd

READY = b"ready"
VALUE, TASK_ERROR, CODEC_ERROR = b"V", b"E", b"S"


def encode_call(
    fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
) -> bytes:
    if (
        not (isfunction(fn) or isbuiltin(fn))
        or fn.__module__ in (None, "__main__", "__mp_main__")
        or "." in fn.__qualname__
        or fn.__name__ == "<lambda>"
    ):
        raise SerializationError(
            "submit a named module-level function from an importable module"
        )
    try:
        return pickle.dumps((fn, args, kwargs), protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as error:
        raise SerializationError("could not serialize the submitted call") from error


def _failure(tag: bytes, error: BaseException) -> bytes:
    return tag + "".join(traceback.format_exception(error)).encode(
        "utf-8", "backslashreplace"
    )


def _execute(request: bytes) -> bytes:
    try:
        fn, args, kwargs = pickle.loads(request)
    except BaseException as error:  # noqa: BLE001
        return _failure(CODEC_ERROR, error)
    try:
        value = fn(*args, **kwargs)
    except BaseException as error:  # noqa: BLE001
        return _failure(TASK_ERROR, error)
    try:
        return VALUE + pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    except BaseException as error:  # noqa: BLE001
        return _failure(CODEC_ERROR, error)


def process_worker(pipe: PipeEnd) -> None:
    with pipe:
        pipe.send_bytes(READY)
        while True:
            try:
                request = pipe.recv_bytes()
            except EOFError:
                return
            pipe.send_bytes(_execute(request))
            del request
