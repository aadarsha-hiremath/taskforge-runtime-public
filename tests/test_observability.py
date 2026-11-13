import logging
import multiprocessing as mp
import os
import random
import unittest
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from functools import partial
from multiprocessing.managers import SyncManager
from threading import Barrier, Event, current_thread
from typing import Any
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
    RuntimeClosedError,
    RuntimeSnapshot,
    Schedule,
    WorkerLostError,
    WorkerState,
)
from taskforge.worker import ThreadItem
from tests.test_process_runtime import LIMIT, gated_task
from tests.test_scheduling import fail_once

BACKENDS = (Runtime, ProcessRuntime)


def check_snapshot(view: RuntimeSnapshot) -> None:
    assert (
        view.accepted
        == view.pending
        + view.running
        + view.completed
        + view.failed
        + view.expired
        + view.cancelled
    )
    assert view.started <= view.accepted
    assert view.started <= view.attempts <= view.started + view.retries_scheduled
    assert view.pending + view.reserved <= view.capacity
    assert view.available_workers + view.running <= len(view.workers)
    assert all(
        (w.job_id is not None) == (w.state is WorkerState.RUNNING) for w in view.workers
    )
    assert len({w.worker_id for w in view.workers}) == len(view.workers)


def waiting_to_retry(job: Job[None]) -> bool:
    return job.state is JobState.PENDING and job.attempts == 1


def secret_failure(secret: str) -> None:
    raise ValueError(secret)


class ObservabilityTests(unittest.TestCase):
    manager: SyncManager

    @classmethod
    def setUpClass(cls) -> None:
        cls.manager = SyncManager(address=("127.0.0.1", 0), ctx=mp.get_context("spawn"))
        cls.manager.start()
        cls.addClassCleanup(cls.manager.shutdown)

    def test_snapshots_across_admission_shutdown_and_terminal_states(self) -> None:
        for backend in BACKENDS:
            with self.subTest(backend=backend.__name__), backend(1, 2) as pool:
                before = pool.snapshot()
                started, release = self.manager.Event(), self.manager.Event()
                try:
                    running = pool.submit_scheduled(
                        Schedule(max_retries=1), gated_task, started, release
                    )
                    self.assertTrue(started.wait(LIMIT))
                    expired = pool.submit_scheduled(Schedule(start_within=0), os.getpid)
                    with self.assertRaises(DeadlineExpiredError):
                        expired.result(0)
                    cancelled = pool.submit(os.getpid)
                    with self.assertRaises(QueueFullError):
                        pool.submit_with_timeout(0, os.getpid)
                    live = pool.snapshot()
                    check_snapshot(live)
                    self.assertEqual(
                        (
                            live.accepted,
                            live.started,
                            live.running,
                            live.expired,
                            live.queue_depth,
                            live.reserved,
                        ),
                        (3, 1, 1, 1, 1, 1),
                    )
                    self.assertEqual(live.workers[0].job_id, running.id)
                    self.assertEqual(before.available_workers, 1)
                    self.assertEqual(before.accepted, 0)
                    for obj, field in ((live.workers[0], "state"), (live, "pending")):
                        with self.assertRaises(FrozenInstanceError):
                            setattr(obj, field, 99)
                    pool.shutdown(wait=False, cancel_pending=True)
                    stopping = pool.snapshot()
                    self.assertTrue(stopping.closed)
                    self.assertFalse(stopping.stopped)
                    self.assertEqual(stopping.cancelled, 1)
                    with self.assertRaises(JobCancelledError):
                        cancelled.result(0)
                finally:
                    release.set()
                running.result(LIMIT)
                pool.shutdown()
                final = pool.snapshot()
                check_snapshot(final)
                self.assertTrue(final.stopped)
                self.assertEqual(final.completed, 1)
                self.assertIs(final.workers[0].state, WorkerState.EXITED)
                pool.shutdown()
                self.assertEqual(final, pool.snapshot())

    def test_retry_deadline_and_log_fields(self) -> None:
        secret = "private-argument-never-log"
        for backend in BACKENDS:
            with (
                self.subTest(backend=backend.__name__),
                self.assertLogs("taskforge.runtime", "INFO") as capture,
                backend(1) as pool,
            ):
                successful = pool.submit_scheduled(
                    Schedule(max_retries=1), fail_once, self.manager.Event()
                )
                self.assertEqual(successful.result(LIMIT), 42)
                failed = pool.submit_scheduled(
                    Schedule(max_retries=1), secret_failure, secret
                )
                with self.assertRaises((RemoteTaskError, ValueError)):
                    failed.result(LIMIT)
                with patch("taskforge.scheduling.monotonic", return_value=10) as clock:
                    job = pool.submit_scheduled(
                        Schedule(max_retries=2, retry_delay=100, start_within=30),
                        secret_failure,
                        secret,
                    )
                    with pool._changed:
                        self.assertTrue(
                            pool._changed.wait_for(
                                partial(waiting_to_retry, job),
                                LIMIT,
                            )
                        )
                        clock.return_value = 40
                        pool._signal()
                    with self.assertRaises(DeadlineExpiredError):
                        job.result(LIMIT)
                view = pool.snapshot()
                check_snapshot(view)
                self.assertEqual(
                    (view.started, view.attempts, view.retries_scheduled), (3, 5, 3)
                )
                self.assertEqual((view.completed, view.failed, view.expired), (1, 1, 1))
            records = [r.__dict__ for r in capture.records]
            self.assertNotIn(secret, repr(records))
            self.assertTrue(all(r["backend"] == view.backend for r in records))
            self.assertEqual(
                [r["event"] for r in records if r["job_id"] == str(successful.id)],
                ["accepted", "claimed", "retry_scheduled", "claimed", "completed"],
            )
            end = next(r for r in records if r["event"] == "expired")
            self.assertEqual(
                (end["attempt"], end["deadline"], end["state"]), (1, 40, "expired")
            )

    def test_diagnostic_handler_failure_preserves_job_outcome(self) -> None:
        logger = logging.getLogger("taskforge.runtime")
        with (
            patch.object(logger, "isEnabledFor", return_value=True),
            patch.object(logger, "info", side_effect=RuntimeError("broken sink")),
            Runtime(1) as pool,
        ):
            self.assertEqual(pool.submit(pow, 2, 4).result(LIMIT), 16)
            self.assertEqual(pool.snapshot().completed, 1)

    def test_thread_name_changed_inside_callable(self) -> None:
        def rename() -> None:
            current_thread().name = "application-worker"

        with Runtime(1) as pool:
            original = pool.snapshot().workers[0].worker_id
            pool.submit(rename).result(LIMIT)
            pool.submit(os.getpid).result(LIMIT)
        view = pool.snapshot()
        self.assertEqual(len(view.workers), 1)
        self.assertEqual(view.workers[0].worker_id, original)
        self.assertIs(view.workers[0].state, WorkerState.EXITED)

    def test_thread_loop_failure_resolves_assigned_and_pending_jobs(self) -> None:
        claimed, crash = Event(), Event()

        def broken_loop(take: Callable[[], ThreadItem | None], finish: Any) -> None:
            assert take() is not None
            claimed.set()
            assert crash.wait(LIMIT)
            raise RuntimeError("injected loop failure")

        with (
            patch("taskforge.scheduler.worker_loop", broken_loop),
            Runtime(1, 3) as pool,
        ):
            try:
                active = pool.submit_scheduled(Schedule(max_retries=2), os.getpid)
                self.assertTrue(claimed.wait(LIMIT))
                pending = pool.submit(os.getpid)
            finally:
                crash.set()
            for job in (active, pending):
                with self.assertRaises(WorkerLostError):
                    job.result(LIMIT)
        final = pool.snapshot()
        check_snapshot(final)
        self.assertTrue(final.stopped)
        self.assertEqual(
            (final.failed, final.attempts, final.retries_scheduled, final.reserved),
            (2, 1, 0, 0),
        )
        self.assertIs(final.workers[0].state, WorkerState.FAILED)

    def test_process_crash_burst_and_lifecycle(self) -> None:
        children = {p.pid for p in mp.active_children()}
        for cycle in range(3):
            with (
                self.subTest(cycle=cycle),
                self.assertLogs("taskforge.runtime", "INFO") as logs,
                ProcessRuntime(2, 16) as pool,
            ):
                gates = [(self.manager.Event(), self.manager.Event()) for _ in range(2)]
                try:
                    crashes = [
                        pool.submit_scheduled(
                            Schedule(max_retries=2),
                            gated_task,
                            start,
                            release,
                            crash=True,
                        )
                        for start, release in gates
                    ]
                    for start, _ in gates:
                        self.assertTrue(start.wait(LIMIT))
                    waiting = [
                        pool.submit_with_timeout(LIMIT, os.getpid) for _ in range(12)
                    ]
                finally:
                    for _, release in gates:
                        release.set()
                for job in [*crashes, *waiting]:
                    with self.assertRaises(WorkerLostError):
                        job.result(LIMIT)
                pool.shutdown(timeout=LIMIT)
                pool.shutdown(timeout=LIMIT)
                final = pool.snapshot()
                check_snapshot(final)
                self.assertTrue(final.stopped)
                self.assertEqual(
                    (final.failed, final.attempts, final.retries_scheduled), (14, 2, 0)
                )
                self.assertTrue(
                    all(
                        w.state is WorkerState.FAILED and w.exit_code == 23
                        for w in final.workers
                    )
                )
                with self.assertRaises(RuntimeClosedError):
                    pool.submit(os.getpid)
            self.assertEqual(sum(r.msg == "worker_failed" for r in logs.records), 2)
        self.assertEqual({p.pid for p in mp.active_children()}, children)

    def test_crashed_process_history_survives_remaining_workers_shutdown(self) -> None:
        with ProcessRuntime(2, 3) as pool:
            failed_start, failed_release = self.manager.Event(), self.manager.Event()
            live_start, live_release = self.manager.Event(), self.manager.Event()
            live = pool.submit(gated_task, live_start, live_release)
            lost = pool.submit(gated_task, failed_start, failed_release, crash=True)
            try:
                self.assertTrue(live_start.wait(LIMIT) and failed_start.wait(LIMIT))
                failed_release.set()
                with self.assertRaises(WorkerLostError):
                    lost.result(LIMIT)
                snapshot = pool.snapshot()
                check_snapshot(snapshot)
                self.assertFalse(snapshot.closed)
                self.assertCountEqual(
                    [w.state for w in snapshot.workers],
                    [WorkerState.FAILED, WorkerState.RUNNING],
                )
            finally:
                failed_release.set()
                live_release.set()
            live.result(LIMIT)
        self.assertCountEqual([w.exit_code for w in pool.snapshot().workers], [0, 23])

    def test_seeded_contention_with_snapshot_readers(self) -> None:
        for backend in BACKENDS:
            with self.subTest(backend=backend.__name__), backend(3, 3) as pool:
                self.check_contention(pool)

    def check_contention(self, pool: Runtime | ProcessRuntime) -> None:
        start = Barrier(6, timeout=LIMIT)

        def producer(seed: int) -> list[Job[int]]:
            rng = random.Random(seed)
            start.wait()
            jobs = []
            for i in range(32):
                job = pool.submit_scheduled(
                    Schedule(priority=rng.randrange(-3, 4), submit_timeout=LIMIT),
                    abs,
                    -i,
                )
                if rng.randrange(3) == 0:
                    job.cancel()
                jobs.append(job)
            return jobs

        def reader() -> None:
            start.wait()
            previous = 0
            for _ in range(100):
                view = pool.snapshot()
                check_snapshot(view)
                assert view.accepted >= previous
                previous = view.accepted

        with ThreadPoolExecutor(6) as callers:
            producers = [callers.submit(producer, 731 + i) for i in range(4)]
            readers = [callers.submit(reader) for _ in range(2)]
            jobs = [job for future in producers for job in future.result(LIMIT * 2)]
            for future in readers:
                future.result(LIMIT * 2)
        completed = cancelled = 0
        for job in jobs:
            try:
                job.result(LIMIT)
                completed += 1
            except JobCancelledError:
                cancelled += 1
        final = pool.snapshot()
        check_snapshot(final)
        self.assertEqual(
            (final.accepted, final.started, final.completed, final.cancelled),
            (128, completed, completed, cancelled),
        )
