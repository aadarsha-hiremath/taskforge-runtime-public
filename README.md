# TaskForge

TaskForge Phase 1 runs Python callables on a fixed pool of threads. Submitting a job returns a handle with a UUID, execution state and result. The runtime uses the Python 3.11+ standard library.

## Install and run

From the repository root in PowerShell, using Python 3.11 or newer:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install .
.\.venv\Scripts\taskforge.exe --workers 2 --queue-capacity 1 2 3 4
```

On macOS or Linux, use `python3 -m venv .venv`, then `.venv/bin/python -m pip install .` and `.venv/bin/taskforge --workers 2 --queue-capacity 1 2 3 4`.

To run directly from the checkout, use `python -m taskforge --workers 2 --queue-capacity 1 2 3 4`. The demo prints one row per input, with its job ID, final state, square and worker name. The squares are 4, 9 and 16. Worker assignments can vary.

## Python API

```python
from taskforge import Runtime

with Runtime(workers=2, queue_capacity=4) as pool:
    job = pool.submit(pow, 2, exp=5)
    print(job.result(timeout=2))  # 32
```

| Operation | Behavior |
| --- | --- |
| `pool.submit(fn, *args, **kwargs)` | Waits for queue space and returns a pending or already-claimed job. All task arguments are forwarded. |
| `pool.submit_with_timeout(seconds, fn, *args, **kwargs)` | Raises `QueueFullError` if space does not become available in time. Zero attempts admission immediately. A timed-out submission never enters the queue. |
| `job.result(timeout=None)` | Waits for completion, returns the value, or raises the task's exception. Cancellation raises `JobCancelledError`. A wait timeout raises `TimeoutError` and the job continues. Multiple callers can retrieve the same outcome. |
| `job.cancel()` | Returns true to the caller that removes a pending job and immediately frees its slot. Returns false once the job is claimed, finished or already cancelled. |
| `pool.shutdown()` | Closes admission, wakes blocked submitters with `RuntimeClosedError`, drains accepted jobs and joins every worker. Context exit does the same, including on exceptional exit. |
| `pool.shutdown(wait=False, cancel_pending=True)` | Cancels queued jobs, wakes their result waiters and returns while claimed jobs finish. The two flags are independent. Call `shutdown()` later to join. |

Pool construction starts the workers. Shutdown is idempotent. After closure, create a fresh pool for another run. A worker may request shutdown using `wait=False`. A joining shutdown from a pool's own worker raises `RuntimeError` before changing admission.

## Architecture

- **Scheduler** (`taskforge/scheduler.py`). A condition protects the admission flag and an ordered dictionary keyed by job ID. Workers claim the oldest item. Cancellation removes an item directly by ID. Both operations publish the state transition under the scheduler lock before another producer or worker can enter. Capacity counts queued jobs, so a pool can have up to `queue_capacity + workers` accepted, unfinished jobs.
- **Worker** (`taskforge/worker.py`). Each non-daemon thread waits on the scheduler condition, claims a job, releases the scheduler lock and calls the task. It publishes either the returned value or the raised exception before taking another job. Once admission closes and the queue empties, it exits.
- **Job** (`taskforge/job.py`). A per-job condition protects the state and outcome. The valid paths are `pending → running → completed`, `pending → running → failed` and `pending → cancelled`. Result readers wait on a terminal-state predicate. Cancellation releases the job lock before calling the scheduler, preserving the scheduler-to-job lock order.
- **Public API** (`taskforge/__init__.py`) exports the runtime, handle, states and exceptions. Runtime-specific exceptions live in `taskforge/errors.py`. The CLI uses the same submission and result APIs.

## Limits

Python threads cannot be forcibly terminated safely. A claimed job runs until its callable returns or raises. A callable that needs early termination can periodically check a caller-supplied `threading.Event`. An indefinitely blocked task can keep a joining shutdown and interpreter exit waiting indefinitely.

FIFO governs claim order among accepted jobs. With multiple workers, the operating system can schedule their callable starts and completions in a different order. Producers competing for space have no fairness guarantee. A task that blocks on another job in the same saturated pool, or waits for space in its own full queue, can deadlock the application. Use an external coordinator or bounded waits for dependent work.

Queue contents and results live in memory. Phase 1 covers thread execution and FIFO scheduling. Process execution, advanced scheduling and benchmarks are deferred.

## Local verification

Run the suite from the repository root with `python -m unittest discover -v`. Tests coordinate with events, barriers and conditions. They cover simultaneous workers, FIFO claims, cancellation, result and admission timeouts, blocked producers, shutdown, partial startup failure and repeated worker cleanup.

For development checks, install `ruff` and `mypy` into the development environment and run:

```sh
python -m ruff format --check .
python -m ruff check .
python -m mypy taskforge tests
```

The [Phase 1 verification record](verification.md) lists the exact local commands, interpreter versions and results.
