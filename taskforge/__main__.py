import argparse
from threading import current_thread

from .scheduler import Runtime


def _square(number: int) -> tuple[int, str]:
    return number * number, current_thread().name


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute squares on a TaskForge pool.")
    parser.add_argument("numbers", nargs="*", type=int, default=[2, 3, 4])
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--queue-capacity", type=int, default=4)
    args = parser.parse_args()
    if min(args.workers, args.queue_capacity) < 1:
        parser.error("workers and queue capacity must be positive")
    with Runtime(args.workers, args.queue_capacity) as runtime:
        jobs = [(n, runtime.submit(_square, n)) for n in args.numbers]
        for number, job in jobs:
            square, worker = job.result()
            print(f"{job.id}  {job.state.value}  {number}^2 = {square}  ({worker})")


if __name__ == "__main__":
    main()
