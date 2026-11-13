"""Exceptions raised by the runtime itself."""


class RuntimeClosedError(RuntimeError):
    """Submission was attempted after shutdown began."""


class QueueFullError(TimeoutError):
    """A submission could not obtain a queue slot before its deadline."""


class JobCancelledError(Exception):
    """The job was cancelled before a worker claimed it."""
