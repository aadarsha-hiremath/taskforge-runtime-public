import argparse
from threading import current_thread

from .demo import process_square
from .process_runtime import ProcessRuntime
from .scheduler import Runtime


def _square(number: int) -> tuple[int, str]:
    return number * number, current_thread().name


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute squares on a TaskForge pool.")
    parser.add_argument("numbers", nargs="*", type=int, default=[2, 3, 4])
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--queue-capacity", type=int, default=4)
    parser.add_argument("--backend", choices=("thread", "process"), default="thread")
    args = parser.parse_args()
    if min(args.workers, args.queue_capacity) < 1:
        parser.error("workers and queue capacity must be positive")
    if args.backend == "process" and args.workers > 60:
        parser.error("the process backend supports up to 60 workers")
    pool_type = ProcessRuntime if args.backend == "process" else Runtime
    square_task = process_square if args.backend == "process" else _square
    with pool_type(args.workers, args.queue_capacity) as runtime:
        jobs = [(n, runtime.submit(square_task, n)) for n in args.numbers]
        for number, job in jobs:
            square, worker = job.result()
            print(f"{job.id}  {job.state.value}  {number}^2 = {square}  ({worker})")


if __name__ == "__main__":
    main()
