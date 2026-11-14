# Observing a pool

Both backends expose `pool.snapshot()`. To see the final counters and worker records after a real join, run either demo:

```sh
python -m taskforge --stats --workers 2 2 3 4
python -m taskforge --stats --backend process --workers 2 2 3 4
```

This example keeps one thread occupied while taking a snapshot. The gate makes the observation reproducible.

```python
from threading import Event
from taskforge import Runtime

release = Event()
with Runtime(workers=1) as pool:
    entered = Event()
    def task():
        entered.set()
        return release.wait(5)
    job = pool.submit(task)
    try:
        assert entered.wait(5)
        view = pool.snapshot()
        print(view.running, view.available_workers, view.workers[0].job_id)
    finally:
        release.set()
    job.result(timeout=5)
print(pool.snapshot().workers[0].state)  # exited
```

| Snapshot field | Meaning at the instant the scheduler lock was held |
| --- | --- |
| `accepted` | Logical jobs admitted during this pool's lifetime, including immediate expiry. Rejected submissions contribute zero. |
| `started`, `attempts` | Distinct jobs claimed at least once, and total claims including retries. A process dispatch can fail after claim and before entering user code. |
| `retries_scheduled` | Failed attempts returned to the queue. Cancellation or expiry can prevent a scheduled retry from being claimed. The number of retry claims is `attempts - started`. |
| `completed`, `failed`, `cancelled`, `expired` | Mutually exclusive final logical job outcomes. Loss of the last worker also fails unclaimed jobs. A retryable failure increments `failed` only if failure becomes the final outcome. |
| `pending` / `queue_depth`, `running` | Pending entries include delayed retries. Running counts assigned attempts, including process dispatch and result reconstruction. Their sum plus the four terminal counts equals `accepted`. |
| `reserved`, `capacity` | Running attempts with a reserved retry slot, and the configured queue limit. `pending + reserved <= capacity`. |
| `workers`, `available_workers` | Immutable tuple of worker observations and count of idle workers. The ID is the original thread name or decimal PID. A running worker carries its job UUID. |
| `closed`, `stopped` | Admission has closed, and worker lifecycle cleanup has reached its end, respectively. A closed pool can still be draining. Use a joining shutdown to synchronize with thread termination. |

**Worker health.** States are `idle`, `running`, `stopping` (an idle child was asked to exit), `exited` (expected), and `failed` (unexpected worker or connection loss). Process failures become visible when the parent observes them. The exit code can arrive later, after reaping. Failed workers remain in subsequent snapshots even if siblings continue serving jobs. Task exceptions affect job statistics and ordinarily leave the worker usable. A thread executing a blocked callable continues to appear running. Snapshots provide neither a heartbeat nor a hang diagnosis. Failed constructors clean up and raise before returning a pool.

**Structured logging.** TaskForge emits INFO records on `taskforge.runtime`. It installs no handlers and leaves logging levels to the application. Enable diagnostics in the application entry point:

```python
import logging

logger = logging.getLogger("taskforge.runtime")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(backend)s %(event)s job=%(job_id)s worker=%(worker_id)s "
    "state=%(state)s attempt=%(attempt)s deadline=%(deadline)s"
))
logger.addHandler(handler)
logger.setLevel(logging.INFO)
```

Events cover acceptance, claim, scheduled retry, terminal outcome and worker exit or failure. Unavailable fields are `None`. Deadlines use the parent's monotonic clock. Records contain identifiers and state, excluding callable arguments, results and exception text. Logging is synchronous at transition points. Keep handlers short and avoid calling back into the pool or waiting on its jobs. For a slow destination, attach a standard-library `logging.handlers.QueueHandler` and consume records outside the scheduler. Snapshots are independent of the logger.
