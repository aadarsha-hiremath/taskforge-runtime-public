# Scheduling

Use `submit_scheduled(schedule, fn, *args, **kwargs)` on either backend. The first two parameters are positional-only, leaving all callable keywords available. Existing submissions use priority zero, one attempt and no deadline.

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
