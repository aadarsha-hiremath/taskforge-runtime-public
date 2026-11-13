import unittest
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier, Condition, Event, Lock, Thread, current_thread
from typing import Any
from unittest.mock import patch
from uuid import UUID

from taskforge import (
    Job,
    JobCancelledError,
    JobState,
    QueueFullError,
    Runtime,
    RuntimeClosedError,
)
from taskforge.worker import WorkItem

LIMIT = 5.0


def wait(event: Event) -> None:
    if not event.wait(LIMIT):
        raise AssertionError("timed out waiting for test event")


@contextmanager
def observe_wait(condition: Condition) -> Iterator[Event]:
    """Signal while the waiter owns the lock, just before it goes to sleep."""
    entered = Event()
    original = condition.wait

    def observed(timeout: float | None = None) -> bool:
        entered.set()
        return original(timeout)

    with patch.object(condition, "wait", observed):
        yield entered


class RuntimeTests(unittest.TestCase):
    def pool(self, workers: int = 1, capacity: int = 4) -> Runtime:
        runtime = Runtime(workers, capacity)
        self.addCleanup(runtime.shutdown)
        return runtime

    def occupy_worker(self, runtime: Runtime) -> tuple[Event, Job[None]]:
        started, release = Event(), Event()
        self.addCleanup(release.set)

        def task() -> None:
            started.set()
            wait(release)

        job = runtime.submit(task)
        wait(started)
        return release, job

    def test_submission_arguments_results_and_identifiers(self) -> None:
        def task(a: int, *, timeout: int, fn: int) -> int:
            return a + timeout + fn

        runtime = self.pool()
        first = runtime.submit(task, 2, timeout=3, fn=4)
        second = runtime.submit_with_timeout(LIMIT, task, 5, timeout=6, fn=7)
        self.assertNotEqual(first.id, second.id)
        self.assertEqual(first.result(LIMIT), 9)
        self.assertEqual(first.result(0), 9)
        self.assertEqual(second.result(LIMIT), 18)
        self.assertIs(first.state, JobState.COMPLETED)
        self.assertFalse(first.cancel())

    def test_workers_execute_concurrently(self) -> None:
        rendezvous = Barrier(3, timeout=LIMIT)
        runtime = self.pool(workers=3)

        def task() -> Thread:
            rendezvous.wait()
            return current_thread()

        jobs = [runtime.submit(task) for _ in range(3)]
        self.assertEqual(len({job.result(LIMIT) for job in jobs}), 3)

    def test_fifo_and_cancellation_free_capacity(self) -> None:
        runtime = self.pool(capacity=3)
        release, blocker = self.occupy_worker(runtime)
        order: list[int] = []
        jobs = [runtime.submit(order.append, n) for n in range(3)]
        self.assertTrue(jobs[1].cancel())
        self.assertFalse(jobs[1].cancel())
        self.assertIs(jobs[1].state, JobState.CANCELLED)
        with self.assertRaises(JobCancelledError):
            jobs[1].result(0)
        last = runtime.submit_with_timeout(0, order.append, 3)
        release.set()
        for job in (blocker, jobs[0], jobs[2], last):
            self.assertIsNone(job.result(LIMIT))
        self.assertEqual(order, [0, 2, 3])

    def test_state_transitions_result_timeout_and_multiple_readers(self) -> None:
        runtime = self.pool()
        release_first, _ = self.occupy_worker(runtime)
        started, finish = Event(), Event()
        self.addCleanup(finish.set)

        def task() -> str:
            started.set()
            wait(finish)
            return "done"

        job = runtime.submit(task)
        self.assertIs(job.state, JobState.PENDING)
        with self.assertRaises(TimeoutError):
            job.result(0.02)
        release_first.set()
        wait(started)
        self.assertIs(job.state, JobState.RUNNING)
        self.assertFalse(job.cancel())
        with self.assertRaises(TimeoutError):
            job.result(0)
        with ThreadPoolExecutor(2) as readers, observe_wait(job._changed) as waiting:
            first = readers.submit(job.result, LIMIT)
            wait(waiting)
            waiting.clear()
            second = readers.submit(job.result, LIMIT)
            wait(waiting)
            finish.set()
            self.assertEqual([first.result(LIMIT), second.result(LIMIT)], ["done"] * 2)
        self.assertIs(job.state, JobState.COMPLETED)

    def test_exception_propagation_and_worker_survival(self) -> None:
        runtime = self.pool()
        for error in (ValueError("bad input"), SystemExit(3)):

            def fail(error: BaseException = error) -> None:
                raise error

            job = runtime.submit(fail)
            with self.assertRaises(type(error)) as caught:
                job.result(LIMIT)
            self.assertIs(caught.exception, error)
            self.assertIs(job.state, JobState.FAILED)
            self.assertFalse(job.cancel())
        self.assertEqual(runtime.submit(str, 42).result(LIMIT), "42")

    def test_producer_waits_until_worker_claims_a_job(self) -> None:
        runtime = self.pool(capacity=1)
        release, _ = self.occupy_worker(runtime)
        runtime.submit(str)
        rejected_ran = Event()
        for timeout in (0, 0.02):
            with self.assertRaises(QueueFullError):
                runtime.submit_with_timeout(timeout, rejected_ran.set)
        with (
            ThreadPoolExecutor(1) as producer,
            observe_wait(runtime._changed) as waiting,
        ):
            accepted = producer.submit(runtime.submit_with_timeout, LIMIT, str, 7)
            wait(waiting)
            self.assertFalse(accepted.done())
            release.set()
            self.assertEqual(accepted.result(LIMIT).result(LIMIT), "7")
        runtime.shutdown()
        self.assertFalse(rejected_ran.is_set())

    def test_pending_cancellation_wakes_blocked_producer(self) -> None:
        runtime = self.pool(capacity=1)
        release, _ = self.occupy_worker(runtime)
        pending = runtime.submit(str)
        with (
            ThreadPoolExecutor(1) as producer,
            observe_wait(runtime._changed) as waiting,
        ):
            replacement = producer.submit(runtime.submit_with_timeout, LIMIT, str, 3)
            wait(waiting)
            self.assertTrue(pending.cancel())
            job = replacement.result(LIMIT)
            self.assertIs(job.state, JobState.PENDING)
            release.set()
            self.assertEqual(job.result(LIMIT), "3")

    def test_shutdown_wakes_blocked_submitter_and_drains_queue(self) -> None:
        runtime = self.pool(capacity=1)
        release, running = self.occupy_worker(runtime)
        queued = runtime.submit(str, 1)
        with (
            ThreadPoolExecutor(1) as producer,
            observe_wait(runtime._changed) as waiting,
        ):
            blocked = producer.submit(runtime.submit_with_timeout, LIMIT, str, 2)
            wait(waiting)
            runtime.shutdown(wait=False)
            with self.assertRaises(RuntimeClosedError):
                blocked.result(LIMIT)
        self.assertIs(running.state, JobState.RUNNING)
        self.assertIs(queued.state, JobState.PENDING)
        release.set()
        runtime.shutdown()
        self.assertEqual(queued.result(0), "1")
        with self.assertRaises(RuntimeClosedError):
            runtime.submit(str)

    def test_shutdown_cancel_pending_wakes_result_waiter(self) -> None:
        runtime = self.pool()
        release, running = self.occupy_worker(runtime)
        queued = [runtime.submit(self.fail, "cancelled job ran") for _ in range(3)]
        with (
            ThreadPoolExecutor(1) as reader,
            observe_wait(queued[0]._changed) as waiting,
        ):
            result = reader.submit(queued[0].result, LIMIT)
            wait(waiting)
            runtime.shutdown(wait=False, cancel_pending=True)
            with self.assertRaises(JobCancelledError):
                result.result(LIMIT)
        self.assertTrue(all(job.state is JobState.CANCELLED for job in queued))
        self.assertIs(running.state, JobState.RUNNING)
        release.set()
        runtime.shutdown()
        self.assertIsNone(running.result(0))

    def test_concurrent_cancellers_win_exactly_once(self) -> None:
        runtime = self.pool()
        self.occupy_worker(runtime)
        pending = runtime.submit(self.fail, "cancelled job ran")
        start = Barrier(4, timeout=LIMIT)

        def cancel() -> bool:
            start.wait()
            return pending.cancel()

        with ThreadPoolExecutor(4) as callers:
            attempts = [callers.submit(cancel) for _ in range(4)]
            self.assertEqual(sum(attempt.result(LIMIT) for attempt in attempts), 1)
        self.assertIs(pending.state, JobState.CANCELLED)

    def test_cancel_racing_worker_claim_has_exactly_one_winner(self) -> None:
        for cancel_first in (True, False):
            with self.subTest(cancel_first=cancel_first):
                self.check_claim_race(cancel_first)

    def check_claim_race(self, cancel_first: bool) -> None:
        take, cancel = Runtime._take, Runtime._cancel
        # Pause both real contenders immediately before queue access.
        at_claim, at_cancel = Event(), Event()
        claimed, cancelled = Event(), Event()
        calls: list[int] = []

        def gated_take(pool: Runtime) -> WorkItem[Any] | None:
            at_claim.set()
            wait(at_cancel)
            if cancel_first:
                wait(cancelled)
            item = take(pool)
            claimed.set()
            return item

        def gated_cancel(pool: Runtime, key: UUID) -> bool:
            at_cancel.set()
            wait(at_claim)
            if not cancel_first:
                wait(claimed)
            won = cancel(pool, key)
            cancelled.set()
            return won

        with (
            patch.object(Runtime, "_take", gated_take),
            patch.object(Runtime, "_cancel", gated_cancel),
            Runtime(1, 1) as pool,
        ):
            job = pool.submit(calls.append, 1)
            self.assertEqual(job.cancel(), cancel_first)
            if cancel_first:
                with self.assertRaises(JobCancelledError):
                    job.result(LIMIT)
                self.assertIs(job.state, JobState.CANCELLED)
            else:
                self.assertIsNone(job.result(LIMIT))
                self.assertIs(job.state, JobState.COMPLETED)
        self.assertEqual(calls, [] if cancel_first else [1])
        snapshot = pool.snapshot()
        self.assertEqual(snapshot.accepted, 1)
        self.assertEqual(snapshot.cancelled, int(cancel_first))
        self.assertEqual(snapshot.started, int(not cancel_first))
        self.assertEqual(snapshot.completed, snapshot.started)

    def test_concurrent_producers_no_lost_or_duplicate_work(self) -> None:
        runtime = self.pool(workers=3, capacity=2)
        start = Barrier(4, timeout=LIMIT)
        seen: list[int] = []
        lock = Lock()

        def record(number: int) -> int:
            with lock:
                seen.append(number)
            return number

        def submit_batch(base: int) -> list[Job[int]]:
            start.wait()
            return [
                runtime.submit_with_timeout(LIMIT, record, base + n) for n in range(25)
            ]

        with ThreadPoolExecutor(4) as producers:
            batches = [
                producers.submit(submit_batch, base) for base in range(0, 100, 25)
            ]
            jobs = [job for batch in batches for job in batch.result(LIMIT)]
        self.assertEqual([job.result(LIMIT) for job in jobs], list(range(100)))
        self.assertEqual(len({job.id for job in jobs}), 100)
        self.assertEqual(sorted(seen), list(range(100)))

    def test_worker_initiated_shutdown(self) -> None:
        runtime = self.pool()
        job = runtime.submit(runtime.shutdown)
        with self.assertRaisesRegex(RuntimeError, "cannot join"):
            job.result(LIMIT)
        runtime.submit(runtime.shutdown, wait=False).result(LIMIT)
        with self.assertRaises(RuntimeClosedError):
            runtime.submit(str)
        runtime.shutdown()

    def test_repeated_lifecycle_joins_all_workers(self) -> None:
        def identify(barrier: Barrier) -> Thread:
            barrier.wait()
            return current_thread()

        for _ in range(10):
            with Runtime(2, 1) as runtime:
                barrier = Barrier(2, timeout=LIMIT)
                jobs = [runtime.submit(identify, barrier) for _ in range(2)]
                threads = {job.result(LIMIT) for job in jobs}
            runtime.shutdown()
            self.assertEqual(len(threads), 2)
            self.assertFalse(any(thread.is_alive() for thread in threads))

    def test_partial_startup_failure_joins_started_workers(self) -> None:
        original = Thread.start
        started: list[Thread] = []

        def start(thread: Thread) -> None:
            if started:
                raise RuntimeError("simulated start failure")
            original(thread)
            started.append(thread)

        with (
            patch.object(Thread, "start", start),
            self.assertRaisesRegex(RuntimeError, "simulated"),
        ):
            Runtime(2, 1)
        self.assertEqual(len(started), 1)
        self.assertFalse(started[0].is_alive())

    def test_invalid_configuration(self) -> None:
        for workers, capacity in ((0, 1), (1, 0), (-1, 1), (1, -1)):
            with (
                self.subTest(workers=workers, capacity=capacity),
                self.assertRaises(ValueError),
            ):
                Runtime(workers, capacity)
