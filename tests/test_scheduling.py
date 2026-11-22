from __future__ import annotations

import builtins
import multiprocessing as mp
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.managers import SyncManager
from threading import TIMEOUT_MAX, Event
from typing import Any, ClassVar, cast
from unittest.mock import patch

from taskforge import (
    DeadlineExpiredError,
    Job,
    JobCancelledError,
    JobState,
    ProcessRuntime,
    QueueFullError,
    RemoteTaskError,
    Runtime,
    Schedule,
    SerializationError,
    WorkerLostError,
)
from taskforge.scheduling import WorkItem
from tests.test_process_runtime import (
    LIMIT,
    bad_result,
    exit_task,
    gated_task,
    next_sequence,
)


def fail_once(seen: Event) -> int:
    if not seen.is_set():
        seen.set()
        raise ValueError("first attempt")
    return 42


def fail_after_gate(started: Event, release: Event) -> None:
    gated_task(started, release)
    raise ValueError("persistent failure")


def forwarded(
    *, priority: int, schedule: int, start_within: int, max_retries: int, timeout: int
) -> int:
    return priority + schedule + start_within + max_retries + timeout


class SchedulingCases:
    backend: ClassVar[type[Runtime | ProcessRuntime]]
    manager: ClassVar[SyncManager]

    @classmethod
    def setUpClass(cls) -> None:
        if cls.backend is ProcessRuntime:
            cls.manager = SyncManager(
                address=("127.0.0.1", 0), ctx=mp.get_context("spawn")
            )
            cls.manager.start()
            cast(type[unittest.TestCase], cls).addClassCleanup(cls.manager.shutdown)

    @property
    def case(self) -> unittest.TestCase:
        return cast(unittest.TestCase, self)

    def pool(
        self, workers: int = 1, capacity: int = 8, burst: int = 8
    ) -> Runtime | ProcessRuntime:
        pool = self.backend(workers, capacity, priority_burst=burst)
        if isinstance(pool, ProcessRuntime):
            self.case.addCleanup(pool.shutdown, cancel_pending=True, timeout=LIMIT)
        else:
            self.case.addCleanup(pool.shutdown, cancel_pending=True)
        return pool

    def event(self) -> Event:
        return self.manager.Event() if self.backend is ProcessRuntime else Event()

    def gate(self) -> tuple[Event, Event]:
        started, release = self.event(), self.event()
        self.case.addCleanup(release.set)
        return started, release

    def occupy(self, pool: Runtime | ProcessRuntime) -> Event:
        started, release = self.gate()
        pool.submit(gated_task, started, release)
        assert started.wait(LIMIT)
        return release

    def retry_wait(self, pool: Runtime | ProcessRuntime, job: Job[Any]) -> None:
        with pool._changed:
            assert pool._changed.wait_for(
                lambda: job.state is JobState.PENDING and job.attempts == 1, LIMIT
            )

    def test_priority_and_equal_priority_fifo(self) -> None:
        pool = self.pool()
        release = self.occupy(pool)
        jobs = [
            pool.submit_scheduled(Schedule(priority=p), next_sequence)
            for p in (0, 4, -1, 4)
        ]
        release.set()
        order = [job.result(LIMIT) for job in jobs]
        assert sorted(range(4), key=order.__getitem__) == [1, 3, 0, 2]

    def test_fairness_with_replenished_high_priority_work(self) -> None:
        pool = self.pool(burst=2)
        release = self.occupy(pool)
        low = pool.submit_scheduled(Schedule(priority=-10), next_sequence)
        gates = [self.gate() for _ in range(3)]
        high = Schedule(priority=100)
        pool.submit_scheduled(high, gated_task, *gates[0])
        release.set()
        for i in range(2):
            assert gates[i][0].wait(LIMIT)
            assert low.state is JobState.PENDING
            pool.submit_scheduled(high, gated_task, *gates[i + 1])
            gates[i][1].set()
        assert gates[2][0].wait(LIMIT)
        assert low.result(0) > 0
        gates[2][1].set()

    def test_idle_deadline_wakes_waiter_and_blocked_submitter(self) -> None:
        pool = self.pool(capacity=1)
        release = self.occupy(pool)
        ran = self.event()
        # A real timer expires without a clock tick or another API call.
        job = pool.submit_scheduled(Schedule(start_within=0.1), signal_event, ran)
        with ThreadPoolExecutor(2) as callers:
            result = callers.submit(job.result, LIMIT)
            replacement = callers.submit(pool.submit_with_timeout, LIMIT, os.getpid)
            with self.case.assertRaises(DeadlineExpiredError):
                result.result(LIMIT)
            accepted = replacement.result(LIMIT)
        assert job.attempts == 0 and job.state is JobState.EXPIRED
        release.set()
        assert accepted.result(LIMIT) > 0 and not ran.is_set()

    def test_deadline_claim_boundary_and_running_completion(self) -> None:
        pool = self.pool()
        with patch("taskforge.scheduling.monotonic", return_value=100.0) as clock:
            release = self.occupy(pool)
            started, finish = self.gate()
            winner = pool.submit_scheduled(
                Schedule(start_within=10), gated_task, started, finish
            )
            clock.return_value = 109.0
            release.set()
            assert started.wait(LIMIT)
            loser_ran = self.event()
            loser = pool.submit_scheduled(
                Schedule(start_within=10), signal_event, loser_ran
            )
            clock.return_value = 119.0
            finish.set()
            assert winner.result(LIMIT) > 0
            with self.case.assertRaises(DeadlineExpiredError):
                loser.result(LIMIT)
            assert loser.attempts == 0 and not loser_ran.is_set()

    def test_deadline_rechecked_after_priority_selection(self) -> None:
        pool = self.pool(capacity=3, burst=1)
        release = self.occupy(pool)
        selected, resume = Event(), Event()
        ran = self.event()
        with patch("taskforge.scheduling.monotonic", return_value=10.0) as clock:
            oldest = pool.submit_scheduled(Schedule(priority=0), next_sequence)
            expiring = pool.submit_scheduled(
                Schedule(priority=100, start_within=1, max_retries=1), signal_event, ran
            )
            next_high = pool.submit_scheduled(Schedule(priority=10), next_sequence)

            def choose(*args: Any, **kwargs: Any) -> Any:
                winner = builtins.max(*args, **kwargs)
                if isinstance(winner, WorkItem) and winner.job is expiring:
                    selected.set()
                    assert resume.wait(LIMIT)
                    clock.return_value = 11.0
                return winner

            cancelling, closing = Event(), Event()

            def cancel() -> bool:
                cancelling.set()
                return expiring.cancel()

            def shutdown() -> None:
                closing.set()
                pool.shutdown(wait=False)

            with (
                patch("taskforge.scheduling.max", choose, create=True),
                ThreadPoolExecutor(2) as callers,
            ):
                try:
                    release.set()
                    assert selected.wait(LIMIT)
                    cancellation = callers.submit(cancel)
                    closure = callers.submit(shutdown)
                    assert cancelling.wait(LIMIT) and closing.wait(LIMIT)
                finally:
                    resume.set()
                with self.case.assertRaises(DeadlineExpiredError):
                    expiring.result(LIMIT)
                assert not cancellation.result(LIMIT)
                closure.result(LIMIT)
                # The expired selection doesn't consume the one priority bypass.
                assert next_high.result(LIMIT) < oldest.result(LIMIT)
        view = pool.snapshot()
        assert (view.accepted, view.completed, view.expired) == (4, 3, 1)
        assert (view.pending, view.attempts, view.reserved) == (0, 3, 0)
        assert expiring.attempts == 0 and not ran.is_set()

    def test_retry_deadline_rechecked_after_selection(self) -> None:
        pool = self.pool(capacity=2)
        entered, release = self.gate()
        with patch("taskforge.scheduling.monotonic", return_value=100.0) as clock:
            retry = pool.submit_scheduled(
                Schedule(priority=10, start_within=10, max_retries=1, retry_delay=5),
                fail_once,
                self.event(),
            )
            self.retry_wait(pool, retry)
            occupied = pool.submit(gated_task, entered, release)
            assert entered.wait(LIMIT)
            following = pool.submit(os.getpid)

            def choose(*args: Any, **kwargs: Any) -> Any:
                winner = builtins.max(*args, **kwargs)
                if isinstance(winner, WorkItem) and winner.job is retry:
                    clock.return_value = 111.0
                return winner

            with patch("taskforge.scheduling.max", choose, create=True):
                clock.return_value = 105.0
                release.set()
                with self.case.assertRaises(DeadlineExpiredError):
                    retry.result(LIMIT)
                assert following.result(LIMIT) == occupied.result(LIMIT)
        view = pool.snapshot()
        assert (view.attempts, view.retries_scheduled, view.reserved) == (3, 1, 0)
        assert retry.attempts == 1 and view.expired == 1

    def test_delayed_retry_and_drain_with_controlled_clock(self) -> None:
        pool = self.pool()
        with patch("taskforge.scheduling.monotonic", return_value=50.0) as clock:
            job = pool.submit_scheduled(
                Schedule(max_retries=2, retry_delay=5), fail_once, self.event()
            )
            self.retry_wait(pool, job)
            clock.return_value = 54.99
            assert pool.submit(os.getpid).result(LIMIT) > 0
            assert job.attempts == 1 and job.state is JobState.PENDING
            pool.shutdown(wait=False)
            with pool._changed:
                clock.return_value = 55.0
                pool._signal()
            assert job.result(LIMIT) == 42 and job.attempts == 2

    def test_retry_exhaustion_and_default_no_retry(self) -> None:
        pool = self.pool()
        for retries in (0, 2):
            job = pool.submit_scheduled(
                Schedule(max_retries=retries, retry_delay=0.01), always_fail
            )
            error = RemoteTaskError if self.backend is ProcessRuntime else ValueError
            with self.case.assertRaisesRegex(error, "persistent failure"):
                job.result(LIMIT)
            assert job.attempts == retries + 1 and job.state is JobState.FAILED

    def test_bounded_retry_burst_with_result_readers(self) -> None:
        pool = self.pool(2, capacity=3)
        jobs = [
            pool.submit_scheduled(
                Schedule(max_retries=1, retry_delay=0.001, submit_timeout=LIMIT),
                fail_once,
                self.event(),
            )
            for _ in range(12)
        ]
        with ThreadPoolExecutor(4) as readers:
            futures = [
                readers.submit(job.result, LIMIT) for job in jobs for _ in range(2)
            ]
            assert [future.result(LIMIT) for future in futures] == [42] * 24
        view = pool.snapshot()
        assert (view.accepted, view.started, view.completed) == (12, 12, 12)
        assert (view.attempts, view.retries_scheduled, view.failed, view.reserved) == (
            24,
            12,
            0,
            0,
        )

    def test_long_backoff_drains_on_clock_advance(self) -> None:
        pool = self.pool(capacity=1)
        with patch("taskforge.scheduling.monotonic", return_value=10.0) as clock:
            # Valid Windows duration above signed 32-bit milliseconds.
            delay = 3_000_000
            job = pool.submit_scheduled(
                Schedule(max_retries=1, retry_delay=delay), fail_once, self.event()
            )
            self.retry_wait(pool, job)
            pool.shutdown(wait=False)
            with pool._changed:
                clock.return_value = 10.0 + delay
                pool._signal()
            assert job.result(LIMIT) == 42 and job.attempts == 2

    def test_cancel_retry_and_release_reserved_capacity(self) -> None:
        pool = self.pool(capacity=1)
        started, release = self.gate()
        job = pool.submit_scheduled(
            Schedule(max_retries=2, retry_delay=3600), fail_after_gate, started, release
        )
        assert started.wait(LIMIT)
        with self.case.assertRaises(QueueFullError):
            pool.submit_with_timeout(0, os.getpid)
        release.set()
        self.retry_wait(pool, job)
        assert job.cancel()
        with self.case.assertRaises(JobCancelledError):
            job.result(0)
        assert (
            job.attempts == 1
            and pool.submit_with_timeout(0, os.getpid).result(LIMIT) > 0
        )

    def test_deadline_during_retry_wait(self) -> None:
        pool = self.pool()
        with patch("taskforge.scheduling.monotonic", return_value=1.0) as clock:
            job = pool.submit_scheduled(
                Schedule(start_within=30, max_retries=1, retry_delay=100), always_fail
            )
            self.retry_wait(pool, job)
            with pool._changed:
                clock.return_value = 31.0
                pool._signal()
            with self.case.assertRaises(DeadlineExpiredError):
                job.result(LIMIT)
            assert job.attempts == 1 and not job.cancel()

    def test_cancel_pending_shutdown_covers_running_and_delayed_retries(self) -> None:
        pool = self.pool(2)
        started, release = self.gate()
        options = Schedule(max_retries=1, retry_delay=3600)
        running = pool.submit_scheduled(options, fail_after_gate, started, release)
        assert started.wait(LIMIT)
        delayed = pool.submit_scheduled(options, always_fail)
        self.retry_wait(pool, delayed)
        pool.shutdown(wait=False, cancel_pending=True)
        release.set()
        assert delayed.state is JobState.CANCELLED
        error = RemoteTaskError if self.backend is ProcessRuntime else ValueError
        with self.case.assertRaisesRegex(error, "persistent failure"):
            running.result(LIMIT)
        assert running.attempts == delayed.attempts == 1

    def test_concurrent_scheduled_submissions_and_keyword_forwarding(self) -> None:
        pool = self.pool(2, capacity=2)

        def submit(i: int) -> Job[int]:
            return pool.submit_scheduled(
                Schedule(priority=i, submit_timeout=LIMIT),
                forwarded,
                priority=i,
                schedule=1,
                start_within=2,
                max_retries=3,
                timeout=4,
            )

        with ThreadPoolExecutor(4) as callers:
            jobs = list(callers.map(submit, range(24)))
        assert [job.result(LIMIT) for job in jobs] == list(range(10, 34))
        assert len({job.id for job in jobs}) == 24


def always_fail() -> None:
    raise ValueError("persistent failure")


def signal_event(event: Event) -> None:
    event.set()


class ScheduleValidationTests(unittest.TestCase):
    def test_submit_timeout_rejects_invalid_durations(self) -> None:
        for value in (
            -1,
            float("nan"),
            float("inf"),
            float("-inf"),
            True,
            False,
            TIMEOUT_MAX + 1,
        ):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValueError, "submit_timeout"),
            ):
                Schedule(submit_timeout=value)

    def test_submit_timeout_preserves_supported_durations(self) -> None:
        self.assertIsNone(Schedule().submit_timeout)
        for value in (None, 0, 0.25, 1, TIMEOUT_MAX):
            with self.subTest(value=value):
                self.assertEqual(Schedule(submit_timeout=value).submit_timeout, value)


class ThreadSchedulingTests(SchedulingCases, unittest.TestCase):
    backend = Runtime

    def test_invalid_configuration(self) -> None:
        for value in (True, 1.5, "1", None):
            with self.assertRaises(ValueError):
                Schedule(priority=cast(int, value))
        for value in (-1, float("nan"), float("inf"), True):
            with self.assertRaises(ValueError):
                Schedule(start_within=value)
            with self.assertRaises(ValueError):
                Schedule(retry_delay=value)
        for backend in (Runtime, ProcessRuntime):
            with self.assertRaises(ValueError):
                backend(priority_burst=0)
        with self.assertRaises(ValueError):
            Schedule(max_retries=-1)


class ProcessSchedulingTests(SchedulingCases, unittest.TestCase):
    backend = ProcessRuntime

    def test_crash_and_codec_failures_never_retry(self) -> None:
        pool = self.pool(2)
        schedule = Schedule(max_retries=3)
        for fn, error in (
            (bad_result, SerializationError),
            (exit_task, RemoteTaskError),
        ):
            job = pool.submit_scheduled(schedule, fn)
            with self.assertRaises(error):
                job.result(LIMIT)
            assert job.attempts == 1
        started, release = self.gate()
        crash = pool.submit_scheduled(
            schedule, gated_task, started, release, crash=True
        )
        assert started.wait(LIMIT)
        release.set()
        with self.assertRaises(WorkerLostError):
            crash.result(LIMIT)
        assert crash.attempts == 1 and pool.submit(os.getpid).result(LIMIT) > 0
