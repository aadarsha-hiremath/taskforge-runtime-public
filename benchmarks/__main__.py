"""Run with python -m benchmarks [--extended] [--output PATH]."""

import argparse
import json
import os
import platform
import random
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any

from taskforge import Job, ProcessRuntime, RemoteTaskError, Runtime, Schedule

from .tasks import fail, measure

Pool = Runtime | ProcessRuntime
Result = tuple[int, int, int, float]
LIMIT = 20
SCENARIOS = (
    "light",
    "cpu",
    "wait",
    "contention",
    "priority",
    "deadline",
    "snapshot",
    "failure",
    "retry",
)


def batch(
    pool: Pool, workers: int, scenario: str, numbers: list[int], expected: list[int]
) -> tuple[float, float | None, int | None]:
    rounds = 25_000 if scenario == "cpu" else 0
    delay = 0.002 if scenario == "wait" else 0.0
    before = pool.snapshot()
    start = perf_counter()
    active: set[tuple[int, int]] = set()
    busy = 0.0
    if scenario in ("failure", "retry"):
        retries = int(scenario == "retry")
        options = Schedule(max_retries=retries, submit_timeout=LIMIT)
        failed = [pool.submit_scheduled(options, fail, n) for n in numbers]
        for n, failed_job in zip(numbers, failed, strict=True):
            try:
                failed_job.result(LIMIT)
            except (ValueError, RemoteTaskError) as error:
                if (
                    f"expected failure {n}" not in str(error)
                    or failed_job.attempts != retries + 1
                ):
                    raise AssertionError("unexpected failure outcome") from error
            else:
                raise AssertionError("failure workload succeeded")
    else:

        def submit(n: int) -> Job[Result]:
            if scenario == "priority":
                options = Schedule(priority=n % 4, submit_timeout=LIMIT)
            elif scenario == "deadline":
                options = Schedule(start_within=LIMIT, submit_timeout=LIMIT)
            else:
                options = Schedule(submit_timeout=LIMIT)
            job = pool.submit_scheduled(options, measure, n, rounds, delay)
            if scenario == "snapshot":
                pool.snapshot()
            return job

        if scenario == "contention":
            with ThreadPoolExecutor(4) as producers:
                jobs = list(producers.map(submit, numbers, timeout=LIMIT))
        else:
            jobs = [submit(n) for n in numbers]
        for expected_value, job in zip(expected, jobs, strict=True):
            value, pid, tid, seconds = job.result(LIMIT)
            if value != expected_value or job.attempts != 1:
                raise AssertionError("incorrect task result or attempt count")
            busy += seconds
            active.add((pid, tid))
    elapsed = perf_counter() - start
    after = pool.snapshot()
    count = len(numbers)
    failures = count if scenario in ("failure", "retry") else 0
    if (
        after.accepted - before.accepted,
        after.completed - before.completed,
        after.failed - before.failed,
        after.attempts - before.attempts,
        after.pending,
        after.running,
    ) != (
        count,
        count - failures,
        failures,
        count * (2 if scenario == "retry" else 1),
        0,
        0,
    ):
        raise AssertionError("accounting mismatch")
    return elapsed, busy / (workers * elapsed) if active else None, len(active) or None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extended", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    count, repeats = (128, 7) if args.extended else (32, 3)
    numbers = list(range(count))
    random.Random(731).shuffle(numbers)
    cpu_values = [measure(n, 25_000, 0)[0] for n in numbers]
    report: dict[str, Any] = {
        "python": sys.version,
        "platform": platform.platform(),
        "cpu": platform.processor(),
        "logical_cpus": os.cpu_count(),
        "seed": 731,
        "jobs": count,
        "capacity": 4,
        "warmups": 1,
        "repeats": repeats,
        "rows": [],
        "lifecycle": [],
    }
    for backend in (Runtime, ProcessRuntime):
        for workers in (1, 2, 4) if args.extended else (1, 2):
            label = backend.__name__
            startups, shutdowns = [], []
            for iteration in range(repeats + 1):
                start = perf_counter()
                pool = backend(workers, 4)
                started = perf_counter()
                pool.shutdown()
                ended = perf_counter()
                if not pool.snapshot().stopped:
                    raise AssertionError("pool did not stop")
                if iteration:
                    startups.append(started - start)
                    shutdowns.append(ended - started)
            report["lifecycle"].append(
                {
                    "backend": label,
                    "workers": workers,
                    "startup_s": startups,
                    "shutdown_s": shutdowns,
                }
            )
            with backend(workers, 4) as pool:
                for scenario in SCENARIOS:
                    expected = cpu_values if scenario == "cpu" else numbers
                    batch(pool, workers, scenario, numbers, expected)
                    samples = [
                        batch(pool, workers, scenario, numbers, expected)
                        for _ in range(repeats)
                    ]
                    times = [s[0] for s in samples]
                    report["rows"].append(
                        {
                            "backend": label,
                            "workers": workers,
                            "workload": scenario,
                            "seconds": times,
                            "median_jobs_s": count / median(times),
                            "range_jobs_s": [count / max(times), count / min(times)],
                            "occupied_fraction": [s[1] for s in samples],
                            "participating_workers": [s[2] for s in samples],
                        }
                    )
    output = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(output, encoding="utf-8")
    else:
        print(output, end="")


if __name__ == "__main__":
    main()
