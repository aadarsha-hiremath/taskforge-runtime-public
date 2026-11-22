# Local benchmark measurements

From the source checkout, run `python -m benchmarks` for a short JSON report. Use `python -m benchmarks --output results.json` to save it. Optional `--extended` runs 128 jobs per batch, seven measured iterations, and worker counts of 1, 2 and 4. Both modes use only the standard library, CPU work and local waits. The [recorded short run](../benchmarks/windows-short.json) contains the raw elapsed times, median throughput, observed range, worker participation, callable occupancy and lifecycle samples.

## Measurement boundaries

The recorded short run used the Phase 5 runtime represented by public commit `45be4a2`, on Windows 11 build 26300 with an Intel Core i7-14650HX, 24 logical CPUs, and 64-bit CPython 3.15.2. Each backend ran with 1 and 2 workers, capacity 4, the same 32 input values shuffled with seed 731, one warm-up per scenario and three measured batches. Each batch measures from before the first submission through retrieval and verification of every result with `perf_counter()`. Verification of final snapshot deltas is outside the timed interval. Returned value, attempt and terminal counts are checked. For OS-level containment the recorded invocation used a 180-second outer timeout:

```sh
python -c "import subprocess,sys; subprocess.run([sys.executable,'-m','benchmarks','--output','results.json'],timeout=180,check=True)"
```

The CPU workload applies a modular arithmetic recurrence 25,000 times. The wait workload requests a 2 ms local sleep per job. The contention workload uses four concurrent submitters and includes producer executor construction and join. The priority workload assigns four priority levels. The deadline workload gives each job a 20-second start window. The snapshot workload samples after every submission. The failure workload expects an exception from every job, and the retry workload expects two failed attempts for each logical job. Remote exception formatting and IPC are included in both failure measurements.

## Median logical jobs per second

| Scenario | Thread ×1 | Thread ×2 | Process ×1 | Process ×2 |
| --- | ---: | ---: | ---: | ---: |
| Lightweight | 53,174 | 38,661 | 9,924 | 12,626 |
| CPU | 876 | 825 | 957 | 1,584 |
| Local wait | 394 | 801 | 387 | 770 |
| Producer contention | 18,750 | 12,337 | 5,283 | 6,797 |
| Mixed priorities | 47,825 | 33,441 | 11,560 | 10,248 |
| Start deadline | 15,511 | 18,188 | 9,778 | 10,251 |
| Snapshot per submission | 41,650 | 31,611 | 10,404 | 10,981 |
| Terminal failure | 18,851 | 36,998 | 1,556 | 2,570 |
| One retry then failure | 35,810 | 17,617 | 759 | 1,364 |

For the two-worker CPU batches the observed ranges were **808–894** jobs/s for threads and **1,570–1,588** for processes. Both processes participated in every CPU batch. Callable occupancy was 84–86%, calculated as `sum(callable wall seconds) / (batch wall seconds × worker count)`. This includes time spent waiting or descheduled and excludes serialization. The tiny two-thread workload used one thread in each measured batch. Failure scenarios return no occupancy sample.

Median empty-pool startup and joining shutdown were 0.265/0.117 ms for one thread, 0.245/0.112 ms for two threads, 186.564/11.993 ms for one process and 200.816/13.775 ms for two processes. Startup includes the process handshakes. Each lifecycle sample used a fresh pool in the same parent interpreter.

Separate interpreters can execute Python bytecode concurrently on this GIL-enabled CPython build. Shared-interpreter threads can overlap blocking waits. These measurements are consistent with both effects and show the cost of short process jobs. The one-process priority workload measured faster than the lightweight baseline. Three small batches, fixed scenario order, scheduler jitter and a single machine do not isolate the cost of one feature. Use the ranges and rerun with your own task sizes before selecting a backend.
