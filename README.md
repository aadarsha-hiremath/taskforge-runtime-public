# TaskForge

TaskForge runs Python callables on a thread pool (`Runtime`) or a pool of spawned processes (`ProcessRuntime`). Both return a parent-side job handle with a UUID, execution state and result. Both use the Python 3.11+ standard library.

## Install and run

From the repository root in PowerShell, using Python 3.11 or newer:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install .
.\.venv\Scripts\taskforge.exe --workers 2 --queue-capacity 1 2 3 4
.\.venv\Scripts\taskforge.exe --backend process --workers 2 --queue-capacity 1 2 3 4
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

Pool construction starts the workers. Shutdown is idempotent. After closure, create a fresh pool for another run. In the thread runtime, a worker may request shutdown using `wait=False`. A joining shutdown from a pool's own thread raises `RuntimeError` before changing admission.

## Scheduling

Use `submit_scheduled(schedule, fn, *args, **kwargs)` on either backend. The first two parameters are positional-only, leaving all callable keywords available. Existing submissions use priority zero, one attempt and no deadline.

A thread job can retry an idempotent file read while another task produces the file:

```python
from pathlib import Path
from taskforge import Runtime, Schedule

with Runtime(workers=2, queue_capacity=8, priority_burst=4) as pool:
    read = pool.submit_scheduled(
        Schedule(priority=3, start_within=5, max_retries=2, retry_delay=0.25),
        Path("report.txt").read_text, encoding="utf-8",
    )
    print(read.result(timeout=6))
```

If all three attempts fail, the caller receives the last file error. Inspect `read.attempts` to see how many claims took place.

| Setting | Scope |
| --- | --- |
| `Schedule(submit_timeout=seconds)` | Limits the wait for admission after preparing the call. A full queue raises `QueueFullError` and produces no job. Equivalent to the existing submission timeout. |
| `Schedule(start_within=seconds)` | Sets one monotonic deadline relative to admission. Every attempt must be claimed strictly before it. Zero expires immediately. An expired handle has state `EXPIRED` and raises `DeadlineExpiredError` on retrieval. |
| `job.result(timeout=seconds)` | Bounds this caller's wait for the final outcome. A timeout leaves execution and retry policy intact. |
| `Schedule(max_retries=n, retry_delay=seconds)` | Allows at most `1 + n` attempts. After a retryable failure, the next attempt becomes eligible after a fixed delay measured from failure handling. Zero makes it eligible immediately. |

Deadlines govern the scheduler's claim under its lock. OS scheduling, pipe transfer and argument reconstruction can delay entry into the callable after a timely claim. A running attempt can finish after its deadline. If it needs another attempt and its deadline has passed, the job expires. Expiration frees capacity and wakes result readers and submitters even when all workers are occupied.

### Priority and fairness

Higher integer priorities win among eligible jobs. Equal priorities follow queue insertion order. After `priority_burst` consecutive claims bypass the oldest eligible entry, the next claim serves that entry. The default burst is 8. For example, with a burst of 2, an oldest eligible low-priority job can be bypassed twice before it runs. A retry enters at the tail and participates once its delay has elapsed. All-default jobs are FIFO.

For a continuously eligible job with **k** older entries, at most **(k + 1) × (priority_burst + 1)** further claims are needed to reach it while workers keep making claims and the job stays queued. Later admissions cannot increase that bound. This guarantee concerns accepted jobs and counts claims. It gives no wall-clock bound or guarantee about which blocked producer gets the next slot. Tasks already running retain their workers.

### Retries, backpressure and shutdown

Use retries for idempotent tasks. A failed attempt may already have changed external state. Both runtimes retry ordinary task exceptions only when explicitly configured. Process loss, codec failures and control-flow exceptions such as `SystemExit` are terminal. Thread retries reuse the callable and its arguments. Process retries transmit the original serialized request and can run on a different worker.

`job.attempts` counts claims, including a process dispatch whose outcome becomes uncertain. A failed attempt finishes before its successor is queued. A job waiting to retry is `PENDING` and can be cancelled. An attempt with retries remaining reserves a queue slot while running. This guarantees space for its retry even under full producer load. The final attempt releases that slot on claim. Thus a capacity of one can block new admissions while a retry-enabled job runs. Queue capacity bounds pending entries plus reservations.

Default shutdown drains scheduled retries, including their delays. `shutdown(cancel_pending=True)` cancels queued retries and suppresses future retries from attempts already running. Those attempts publish their own return value or failure. Python threads and arbitrary running process tasks have no safe forced-stop guarantee.

## Processes

Save this example in a Python file and run it as a script. Spawn starts a fresh interpreter that imports the entry module. The main guard keeps pool construction in the parent.

```python
from math import gcd
from taskforge import ProcessRuntime, Schedule

if __name__ == "__main__":
    with ProcessRuntime(workers=2, queue_capacity=4) as pool:
        job = pool.submit_scheduled(Schedule(priority=2, start_within=3), gcd, 84, 30)
        print(job.result(timeout=5))  # 6
```

| | Threads | Processes |
| --- | --- | --- |
| Memory | Workers share Python objects. Callers synchronize shared mutations. | Each worker has a separate heap. Ordinary arguments are snapshots and results are copies. A worker's own globals persist across its jobs. |
| CPU execution | On GIL-enabled CPython, one thread at a time executes Python bytecode per interpreter. Threads can overlap I/O and native work that releases the GIL. | Each worker has its own interpreter and GIL on GIL-enabled builds, allowing CPU-bound Python tasks to run simultaneously on different cores. |
| Cost | Arguments and results pass by reference. | Spawn imports modules in each worker. Pickling, copying and pipe transfers add latency and memory use. |
| Task functions | Accepts any callable. | Accepts importable, named module-level functions, including module-level built-ins such as `math.gcd`. Put custom functions in a separate importable module. Lambdas, closures, methods, callable instances and functions defined in the entry script or REPL are unsupported. |

### Admission and serialization

The process runtime uses pickle over private parent/child pipes. Submit **trusted functions and data**. Pickle reconstruction can execute Python code in either process. Arguments and results must support a complete pickle round trip in the receiving interpreter. Workers must have access to the same modules. For cooperative cancellation, tasks can check a caller-supplied, serializable signal such as a multiprocessing manager event proxy.

The submitting thread serializes the call before waiting for queue capacity. An unsupported function or an ordinary pickling error raises `SerializationError` before admission. The `submit_with_timeout` timeout covers the subsequent wait for space. Capacity includes pending jobs and retry reservations. It does not bound bytes or the memory used by callers preparing submissions.

### Dispatch and failure

One parent controller waits on worker pipes and a wake-up pipe. Each worker has one private connection and at most one assigned job. Under the queue condition, the controller selects an eligible job with the shared scheduling policy and marks it running before writing the request. A pending cancellation that takes the same lock first removes the job and frees the slot. Once the controller claims a job, `cancel()` returns false, including during dispatch.

| Event | Observable outcome |
| --- | --- |
| Task raises, including `SystemExit` | After any eligible retries, the job fails with `RemoteTaskError` containing the child traceback, exception type and message. The worker can accept another job. |
| Task reconstruction, result encoding or parent result decoding fails | The job fails with `SerializationError`. The worker remains available. |
| Worker exits before the controller receives a complete reply | Its assigned job fails with `WorkerLostError`. Side effects may already have occurred. TaskForge never replays a lost worker's assigned job. A complete reply received before EOF is retained. |
| An idle worker exits | The controller detects EOF even without a new submission. Surviving workers continue serving the queue at reduced capacity. |
| The last worker is lost | Admission closes. Unclaimed jobs move directly from pending to failed with `WorkerLostError`. Blocked and subsequent submissions raise `RuntimeClosedError`. Create a fresh pool to restore capacity. |

### Startup, shutdown and timeouts

A process pool supports 1–60 workers and defaults to a 30-second shared startup handshake timeout. The limit on pool size keeps the controller's wait set within the Windows handle limit across supported Python versions. If construction fails, it cleans up the workers it started before admitting user work.

Shutdown drains by default. Pass `cancel_pending=True` to discard queued work, or `wait=False` to let cleanup continue in the controller. A joining `shutdown(timeout=seconds)` raises `TimeoutError` if cleanup is still in progress. Admission stays closed, and another call can finish the join. A result timeout limits only the caller's wait. Neither timeout stops a running task. The controller joins the children and closes the IPC and process handles before a joining shutdown returns. A task that never returns can keep its worker and the parent interpreter alive.

## Scheduler architecture

The scheduler owns the accounting for both backends. Admission, claim, retry, terminal outcome and worker assignment update under the same condition that protects the queue. A snapshot acquires that condition once and copies counters and immutable worker records. It neither polls children nor sends a message. Transition accounting takes constant work per transition with logging disabled. Snapshot cost scales with worker count. The pool retains one health record per worker and aggregate counters, so observations do not keep completed job handles alive.

- **Shared policy** (`taskforge/scheduling.py`). One pool condition protects admission, reservations, the bypass counter and a dictionary of pending entries keyed by job ID. Selection scans the bounded queue in insertion order. Cancellation removes an item directly by ID. Both operations publish the state transition under the scheduler lock before another producer or worker can enter. A pool can have up to `queue_capacity + workers` accepted, unfinished jobs.
- **Timing**. Idle thread workers use timed condition waits for retries. A thread pool lazily starts one deadline timer while there are waiting deadlines, so busy workers can keep executing user code. The timer exits when there are no deadlines left. The existing process controller incorporates deadlines and retry eligibility into its pipe wait. All waiters recompute eligibility under the same lock.
- **Worker** (`taskforge/worker.py`). Each non-daemon thread waits on the scheduler condition, claims a job, releases the scheduler lock and calls the task. It publishes either the returned value or the raised exception before taking another job. Once admission closes and the queue empties, it exits.
- **Job** (`taskforge/job.py`). A per-job condition protects the state and outcome. A claim moves a pending job to running. An attempt may complete, fail, or return to pending for a retry. Waiting jobs may expire, be cancelled, or fail if the last worker is lost. Result readers wait on a terminal-state predicate. Cancellation releases the job lock before calling the scheduler, preserving the scheduler-to-job lock order.
- **Public API** (`taskforge/__init__.py`) exports the runtime, handle, states and exceptions. Runtime-specific exceptions live in `taskforge/errors.py`. The CLI uses the same submission and result APIs.

## Observing a pool

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

## Limits

Python threads cannot be forcibly terminated safely. A claimed job runs until its callable returns or raises. A callable that needs early termination can periodically check a caller-supplied `threading.Event`. An indefinitely blocked task can keep a joining shutdown and interpreter exit waiting indefinitely.

Scheduling governs claim order among eligible attempts. With multiple workers, the operating system can schedule their callable starts and completions in a different order. Producers competing for space have no fairness guarantee. A task that blocks on another job in the same saturated pool, or waits for space in its own full queue, can deadlock the application. Use an external coordinator or bounded waits for dependent work.

Queue contents and results live in memory in both backends. Worker replacement and durable recovery remain outside this phase. Selection and timer recalculation take linear time in queue length. Start windows and retry delays accept finite nonnegative seconds up to the platform's `threading.TIMEOUT_MAX`.

## Local verification

Run the suite from the repository root with `python -m unittest discover -v`. Tests coordinate with events, barriers and conditions. They cover simultaneous workers, FIFO claims, cancellation, result and admission timeouts, blocked producers, shutdown, partial startup failure and repeated worker cleanup.

For development checks, install `ruff` and `mypy` into the development environment and run:

```sh
python -m ruff format --check .
python -m ruff check .
python -m mypy taskforge tests
```

The [Phase 4 verification record](verification-phase-4.md) covers observability, failure handling, stress scenarios and local packaging. Earlier checks are in the [Phase 3](verification-phase-3.md), [Phase 2](verification-phase-2.md) and [Phase 1](verification.md) records.
