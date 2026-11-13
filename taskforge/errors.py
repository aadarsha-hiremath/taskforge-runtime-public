"""Exceptions raised by the runtime itself."""


class RuntimeClosedError(RuntimeError):
    """Submission was attempted after shutdown began."""


class QueueFullError(TimeoutError):
    """A submission could not obtain a queue slot before its deadline."""


class JobCancelledError(Exception):
    """The job was cancelled before a worker claimed it."""


class SerializationError(ValueError):
    """A process task or its result could not cross the serialization boundary."""


class RemoteTaskError(Exception):
    """The worker raised an exception. Its formatted traceback is the message."""


class WorkerLostError(RuntimeError):
    """A worker connection was lost or no surviving worker could run the job."""
