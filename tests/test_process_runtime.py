import multiprocessing as mp
import os
import unittest
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.context import SpawnProcess
from multiprocessing.managers import SyncManager
from threading import Event
from unittest.mock import patch

from taskforge import (
    JobCancelledError,
    JobState,
    ProcessRuntime,
    QueueFullError,
    RemoteTaskError,
    RuntimeClosedError,
    SerializationError,
    WorkerLostError,
)
from taskforge.process_worker import PipeEnd
from tests.test_runtime import observe_wait

LIMIT = 10.0


def gated_task(started: Event, release: Event, *, crash: bool = False) -> int:
    started.set()
    if not release.wait(LIMIT):
        raise TimeoutError("test did not release worker")
    if crash:
        os._exit(23)
    return os.getpid()


def sum_squares(numbers: list[int], *, count: int) -> tuple[int, list[int]]:
    numbers.append(sum(i * i for i in range(count)))
    return os.getpid(), numbers


sequence = 0


def next_sequence() -> int:
    global sequence
    sequence += 1
    return sequence


def fail_task() -> None:
    class LocalError(Exception):
        pass

    raise LocalError("specific remote failure")


def exit_task() -> None:
    raise SystemExit(5)


def bad_result() -> Callable[[], int]:
    return lambda: 1


def cannot_restore() -> None:
    raise ValueError("failed during reconstruction")


class BadRestore:
    def __reduce__(self) -> tuple[Callable[[], None], tuple[()]]:
        return cannot_restore, ()


def identity(value: object) -> object:
    return value


def exit_at_startup(pipe: PipeEnd) -> None:
    os._exit(11)


def stall_at_startup(pipe: PipeEnd) -> None:
    pipe.poll(LIMIT)


class ProcessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manager = SyncManager(address=("127.0.0.1", 0), ctx=mp.get_context("spawn"))
        cls.manager.start()
        cls.addClassCleanup(cls.manager.shutdown)

    # Events are manager proxies so the same tests work under Windows spawn.
    manager: SyncManager

    def pool(self, workers: int = 1, capacity: int = 3) -> ProcessRuntime:
        pool = ProcessRuntime(workers, capacity, startup_timeout=LIMIT)
        self.addCleanup(pool.shutdown, timeout=LIMIT, cancel_pending=True)
        return pool

    def gate(self) -> tuple[Event, Event]:
        started, release = self.manager.Event(), self.manager.Event()
        self.addCleanup(release.set)
        return started, release

    def test_cpu_work_argument_snapshot_and_memory_isolation(self) -> None:
        pool = self.pool()
        started, release = self.gate()
        blocker = pool.submit(gated_task, started, release)
        self.assertTrue(started.wait(LIMIT))
        data = [3]
        job = pool.submit(sum_squares, data, count=100_000)
        data.append(4)
        release.set()
        pid, result = job.result(LIMIT)
        self.assertEqual(pid, blocker.result(LIMIT))
        self.assertNotEqual(pid, os.getpid())
        self.assertNotEqual(job.id, blocker.id)
        self.assertEqual(result, [3, 333_328_333_350_000])
        self.assertEqual(job.result(0), (pid, result))
        self.assertEqual(data, [3, 4])
        self.assertIs(job.state, JobState.COMPLETED)

    def test_two_workers_overlap_and_report_distinct_pids(self) -> None:
        pool = self.pool(2)
        gates = [self.gate() for _ in range(2)]
        jobs = [pool.submit(gated_task, started, release) for started, release in gates]
        for started, _ in gates:
            self.assertTrue(started.wait(LIMIT))
        self.assertTrue(all(job.state is JobState.RUNNING for job in jobs))
        for _, release in gates:
            release.set()
        self.assertEqual(len({job.result(LIMIT) for job in jobs}), 2)

    def test_fifo_cancel_unblocks_producer_and_shutdown_drains(self) -> None:
        pool = self.pool(capacity=2)
        started, release = self.gate()
        running = pool.submit(gated_task, started, release)
        self.assertTrue(started.wait(LIMIT))
        self.assertFalse(running.cancel())
        with self.assertRaises(TimeoutError):
            running.result(0.01)
        first = pool.submit(next_sequence)
        cancelled = pool.submit(next_sequence)
        with self.assertRaises(QueueFullError):
            pool.submit_with_timeout(0.01, next_sequence)
        with ThreadPoolExecutor(1) as submitter, observe_wait(pool._changed) as waiting:
            submission = submitter.submit(
                pool.submit_with_timeout, LIMIT, next_sequence
            )
            self.assertTrue(waiting.wait(LIMIT))
            self.assertTrue(cancelled.cancel())
            second = submission.result(LIMIT)
        self.assertIs(cancelled.state, JobState.CANCELLED)
        with self.assertRaises(JobCancelledError):
            cancelled.result(0)
        pool.shutdown(wait=False)
        release.set()
        pool.shutdown(timeout=LIMIT)
        self.assertEqual([first.result(0), second.result(0)], [1, 2])

    def test_shutdown_cancels_queue_wakes_producer_and_can_be_rejoined(self) -> None:
        pool = self.pool(capacity=1)
        started, release = self.gate()
        running = pool.submit(gated_task, started, release)
        self.assertTrue(started.wait(LIMIT))
        pending = pool.submit(next_sequence)
        with ThreadPoolExecutor(1) as submitter, observe_wait(pool._changed) as waiting:
            blocked = submitter.submit(pool.submit_with_timeout, LIMIT, next_sequence)
            self.assertTrue(waiting.wait(LIMIT))
            with self.assertRaises(TimeoutError):
                pool.shutdown(timeout=0, cancel_pending=True)
            with self.assertRaises(RuntimeClosedError):
                blocked.result(LIMIT)
        with self.assertRaises(JobCancelledError):
            pending.result(0)
        self.assertIs(running.state, JobState.RUNNING)
        release.set()
        pool.shutdown(timeout=LIMIT)
        self.assertIs(running.state, JobState.COMPLETED)

    def test_remote_exception_and_codec_failures_leave_worker_usable(self) -> None:
        pool = self.pool()
        pid = pool.submit(os.getpid).result(LIMIT)
        with self.assertRaises(SerializationError):
            pool.submit(lambda: 1)
        with self.assertRaises(SerializationError):
            pool.submit(identity, lambda: 1)
        failed = pool.submit(fail_task)
        with self.assertRaisesRegex(
            RemoteTaskError, "LocalError: specific remote failure"
        ):
            failed.result(LIMIT)
        self.assertIs(failed.state, JobState.FAILED)
        with self.assertRaisesRegex(RemoteTaskError, "SystemExit: 5"):
            pool.submit(exit_task).result(LIMIT)
        for job in (
            pool.submit(identity, BadRestore()),
            pool.submit(bad_result),
            pool.submit(make_bad_restore),
        ):
            with self.assertRaises(SerializationError):
                job.result(LIMIT)
            self.assertIs(job.state, JobState.FAILED)
        self.assertEqual(pool.submit(os.getpid).result(LIMIT), pid)

    def test_crash_fails_assigned_job_survivor_drains_queue(self) -> None:
        pool = self.pool(2)
        start1, release1 = self.gate()
        start2, release2 = self.gate()
        survivor = pool.submit(gated_task, start1, release1)
        crash = pool.submit(gated_task, start2, release2, crash=True)
        self.assertTrue(start1.wait(LIMIT))
        self.assertTrue(start2.wait(LIMIT))
        pending = pool.submit(os.getpid)
        release2.set()
        with self.assertRaises(WorkerLostError):
            crash.result(LIMIT)
        self.assertIs(crash.state, JobState.FAILED)
        release1.set()
        self.assertEqual(pending.result(LIMIT), survivor.result(LIMIT))
        self.assertEqual(pool.submit(os.getpid).result(LIMIT), survivor.result(0))

    def test_loss_of_last_worker_fails_queue_and_wakes_blocked_submission(self) -> None:
        pool = self.pool(capacity=1)
        started, release = self.gate()
        crashed = pool.submit(gated_task, started, release, crash=True)
        self.assertTrue(started.wait(LIMIT))
        pending = pool.submit(next_sequence)
        with ThreadPoolExecutor(1) as submitter, observe_wait(pool._changed) as waiting:
            blocked = submitter.submit(pool.submit_with_timeout, LIMIT, next_sequence)
            self.assertTrue(waiting.wait(LIMIT))
            release.set()
            with self.assertRaises(RuntimeClosedError):
                blocked.result(LIMIT)
        for job in (crashed, pending):
            with self.assertRaises(WorkerLostError):
                job.result(LIMIT)
            self.assertIs(job.state, JobState.FAILED)
            self.assertFalse(job.cancel())
        pool.shutdown(timeout=LIMIT)

    def test_idle_worker_exit_detected_without_submitting_work(self) -> None:
        pool = self.pool()
        pool._workers[0].process.terminate()
        pool._controller.join(LIMIT)
        self.assertFalse(pool._controller.is_alive())
        with self.assertRaises(RuntimeClosedError):
            pool.submit(os.getpid)

    def test_handles_and_children_released_on_repeated_shutdown(self) -> None:
        before = {child.pid for child in mp.active_children()}
        for _ in range(3):
            pool = self.pool(2)
            self.assertGreater(pool.submit(os.getpid).result(LIMIT), 0)
            workers = list(pool._workers)
            pool.shutdown(wait=False)
            pool.shutdown(timeout=LIMIT)
            self.assertFalse(pool._controller.is_alive())
            self.assertTrue(pool._wake_read.closed and pool._wake_write.closed)
            for worker in workers:
                self.assertTrue(worker.pipe.closed)
                with self.assertRaises(ValueError):
                    worker.process.is_alive()  # Process.close() released the OS handle.
        self.assertEqual({child.pid for child in mp.active_children()}, before)

    def test_partial_startup_and_handshake_failure_reap_children(self) -> None:
        before = {child.pid for child in mp.active_children()}
        original = SpawnProcess.start
        started: list[SpawnProcess] = []

        def start(process: SpawnProcess) -> None:
            if started:
                raise OSError("simulated spawn failure")
            original(process)
            started.append(process)

        with (
            patch.object(SpawnProcess, "start", start),
            self.assertRaisesRegex(OSError, "simulated spawn"),
        ):
            ProcessRuntime(2)
        with (
            patch("taskforge.process_runtime.process_worker", exit_at_startup),
            self.assertRaises(WorkerLostError),
        ):
            ProcessRuntime(1, startup_timeout=LIMIT)
        self.assertEqual({child.pid for child in mp.active_children()}, before)

    def test_startup_timeout_releases_workers(self) -> None:
        before = {child.pid for child in mp.active_children()}
        with (
            patch("taskforge.process_runtime.process_worker", stall_at_startup),
            self.assertRaises(TimeoutError),
        ):
            ProcessRuntime(2, startup_timeout=0.1)
        self.assertEqual({child.pid for child in mp.active_children()}, before)

    def test_worker_claim_releases_blocked_submitter(self) -> None:
        pool = self.pool(capacity=1)
        first_started, release_first = self.gate()
        second_started, release_second = self.gate()
        pool.submit(gated_task, first_started, release_first)
        self.assertTrue(first_started.wait(LIMIT))
        second = pool.submit(gated_task, second_started, release_second)
        with ThreadPoolExecutor(1) as submitter, observe_wait(pool._changed) as waiting:
            blocked = submitter.submit(pool.submit_with_timeout, LIMIT, os.getpid)
            self.assertTrue(waiting.wait(LIMIT))
            release_first.set()
            third = blocked.result(LIMIT)
            self.assertTrue(second_started.wait(LIMIT))
            self.assertIs(third.state, JobState.PENDING)
            release_second.set()
        self.assertEqual(third.result(LIMIT), second.result(LIMIT))

    def test_large_payload_survives_pipe_backpressure(self) -> None:
        pool = self.pool(2, 1)
        payload = bytes(range(256)) * 8192
        jobs = [pool.submit_with_timeout(LIMIT, identity, payload) for _ in range(4)]
        pool.shutdown(timeout=LIMIT)
        for job in jobs:
            self.assertEqual(job.result(0), payload)


def make_bad_restore() -> BadRestore:
    return BadRestore()
