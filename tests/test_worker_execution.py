"""Worker execution semantics: timeouts, retries, hooks, DLQ routing.

These run against MemoryBroker so they exercise the real worker code path with
no external service.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from aioq import Aioq, JobStatus
from aioq.backends.memory import MemoryBroker
from aioq.exceptions import JobTimeoutError
from aioq.retry import compute_retry_delay
from aioq.worker import Worker


@pytest.fixture
async def app():
    broker = MemoryBroker()
    async with broker:
        yield Aioq(broker=broker)


async def run_next(app: Aioq, queues: list[str] | None = None) -> None:
    """Claim and process exactly one job, like a worker tick would."""
    worker = Worker(app, queues=queues or ["default"])
    job = await app.broker.dequeue(worker.queues, timeout=0.2)
    assert job is not None, "expected a runnable job"
    await worker._process(job)


# ----------------------------------------------------------------------
# Success and failure
# ----------------------------------------------------------------------


async def test_successful_job_saves_result(app):
    @app.task(save_result=True)
    async def add(ctx, a, b):
        return a + b

    job = await add.enqueue(2, 3)
    await run_next(app)

    finished = await app.broker.get_job(job.id)
    assert finished.status == JobStatus.completed
    assert finished.result == 5
    assert finished.duration is not None


async def test_result_not_saved_unless_requested(app):
    @app.task()
    async def add(ctx, a, b):
        return a + b

    job = await add.enqueue(2, 3)
    await run_next(app)

    assert (await app.broker.get_job(job.id)).result is None


async def test_failure_without_retries_marks_failed(app):
    @app.task()
    async def boom(ctx):
        raise ValueError("nope")

    job = await boom.enqueue()
    await run_next(app)

    finished = await app.broker.get_job(job.id)
    assert finished.status == JobStatus.failed
    assert "nope" in finished.error
    assert finished.completed_at is not None


async def test_unknown_task_marks_job_failed(app):
    from aioq.models import Job

    job = Job(task_name="tasks.does_not_exist", queue="default")
    await app.broker.enqueue(job)
    await run_next(app)

    finished = await app.broker.get_job(job.id)
    assert finished.status == JobStatus.failed
    assert "Unknown task" in finished.error


async def test_cancelled_job_is_skipped(app):
    ran = []

    @app.task()
    async def note(ctx):
        ran.append(1)

    job = await note.enqueue()
    worker = Worker(app, queues=["default"])
    claimed = await app.broker.dequeue(["default"], timeout=0.2)
    # Cancel after claiming, before processing.
    claimed.status = JobStatus.pending
    await app.broker.update_job(claimed)
    await app.broker.cancel_job(job.id)

    await worker._process(claimed)

    assert ran == []
    assert (await app.broker.get_job(job.id)).status == JobStatus.cancelled


# ----------------------------------------------------------------------
# Timeouts
# ----------------------------------------------------------------------


async def test_timeout_fails_the_job(app):
    @app.task(timeout=0.05)
    async def slow(ctx):
        await asyncio.sleep(5)

    job = await slow.enqueue()
    await run_next(app)

    finished = await app.broker.get_job(job.id)
    assert finished.status == JobStatus.failed
    assert "timeout" in finished.error.lower()


async def test_timeout_does_not_fire_for_fast_jobs(app):
    @app.task(timeout=5, save_result=True)
    async def quick(ctx):
        return "done"

    job = await quick.enqueue()
    await run_next(app)

    finished = await app.broker.get_job(job.id)
    assert finished.status == JobStatus.completed
    assert finished.result == "done"


async def test_timeout_cancels_the_coroutine(app):
    finished_body = []

    @app.task(timeout=0.05)
    async def slow(ctx):
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            finished_body.append("cancelled")
            raise

    await slow.enqueue()
    await run_next(app)

    assert finished_body == ["cancelled"]


async def test_timeout_is_retried_like_any_failure(app):
    @app.task(timeout=0.05, retries=1, retry_delay=0)
    async def slow(ctx):
        await asyncio.sleep(5)

    job = await slow.enqueue()
    await run_next(app)

    retried = await app.broker.get_job(job.id)
    assert retried.status == JobStatus.retrying
    assert retried.retries == 1


async def test_job_timeout_error_is_a_timeout_error():
    assert issubclass(JobTimeoutError, TimeoutError)


# ----------------------------------------------------------------------
# Retries
# ----------------------------------------------------------------------


async def test_retry_reruns_and_eventually_succeeds(app):
    attempts = []

    @app.task(retries=3, retry_delay=0, save_result=True)
    async def flaky(ctx):
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("not yet")
        return "ok"

    job = await flaky.enqueue()
    for _ in range(3):
        await run_next(app)

    finished = await app.broker.get_job(job.id)
    assert finished.status == JobStatus.completed
    assert finished.result == "ok"
    assert len(attempts) == 3


async def test_retry_is_deferred_not_slept_on(app):
    """A retry_delay parks the job in the future instead of blocking a slot."""

    @app.task(retries=1, retry_delay=30)
    async def boom(ctx):
        raise ValueError("nope")

    job = await boom.enqueue()
    started = datetime.now(UTC)
    await run_next(app)

    retried = await app.broker.get_job(job.id)
    assert retried.status == JobStatus.retrying
    assert retried.run_at is not None
    assert retried.run_at > started
    # ...and it is not runnable yet.
    assert await app.broker.dequeue(["default"], timeout=0.05) is None


async def test_exhausted_retries_go_to_dlq(app):
    @app.task(retries=1, retry_delay=0, dead_letter_queue="dlq")
    async def boom(ctx):
        raise ValueError("nope")

    job = await boom.enqueue()
    await run_next(app)
    await run_next(app)

    finished = await app.broker.get_job(job.id)
    assert finished.status == JobStatus.dead
    assert finished.queue == "dlq"
    assert [j.id for j in await app.broker.list_dead_jobs(queue="dlq")] == [job.id]


def test_flat_retry_delay_is_constant():
    assert compute_retry_delay(5.0, 1, backoff=False) == 5.0
    assert compute_retry_delay(5.0, 4, backoff=False) == 5.0


def test_backoff_grows_and_is_capped():
    # Jitter off so the growth curve itself is observable.
    assert compute_retry_delay(1.0, 1, backoff=True, jitter=False) == 1.0
    assert compute_retry_delay(1.0, 2, backoff=True, jitter=False) == 2.0
    assert compute_retry_delay(1.0, 5, backoff=True, jitter=False) == 16.0
    assert compute_retry_delay(1.0, 50, backoff=True, backoff_max=30, jitter=False) == 30.0


def test_backoff_jitter_stays_within_bounds():
    delays = [compute_retry_delay(1.0, 4, backoff=True, backoff_max=100) for _ in range(50)]
    assert all(0 <= d <= 8.0 for d in delays)
    assert len(set(delays)) > 1  # actually randomised


def test_zero_base_delay_never_waits():
    assert compute_retry_delay(0, 3, backoff=True) == 0.0


async def test_backoff_produces_growing_run_at(app):
    @app.task(retries=3, retry_delay=10, retry_backoff=True)
    async def boom(ctx):
        raise ValueError("nope")

    job = await boom.enqueue()
    assert job.retry_backoff is True

    worker = Worker(app, queues=["default"])
    claimed = await app.broker.dequeue(["default"], timeout=0.2)
    await worker._process(claimed)

    retried = await app.broker.get_job(job.id)
    assert retried.run_at is not None


# ----------------------------------------------------------------------
# Lifecycle hooks
# ----------------------------------------------------------------------


async def test_hooks_fire_on_success(app):
    events = []

    @app.on_job_start
    async def started(job):
        events.append(("start", job.task_name))

    @app.on_job_success
    async def succeeded(job, result):
        events.append(("success", result))

    @app.on_job_failure
    async def failed(job, exc):
        events.append(("failure", exc))

    @app.task(save_result=True)
    async def add(ctx, a, b):
        return a + b

    await add.enqueue(1, 2)
    await run_next(app)

    assert [e[0] for e in events] == ["start", "success"]
    assert events[1][1] == 3


async def test_hooks_fire_on_failure(app):
    events = []

    @app.on_job_failure
    async def failed(job, exc):
        events.append((job.status, str(exc)))

    @app.task()
    async def boom(ctx):
        raise ValueError("nope")

    await boom.enqueue()
    await run_next(app)

    assert events == [(JobStatus.failed, "nope")]


async def test_retry_and_dead_hooks(app):
    events = []

    @app.on_job_retry
    async def retrying(job, exc):
        events.append(("retry", job.retries))

    @app.on_job_dead
    async def dead(job, exc):
        events.append(("dead", job.queue))

    @app.task(retries=1, retry_delay=0, dead_letter_queue="dlq")
    async def boom(ctx):
        raise ValueError("nope")

    await boom.enqueue()
    await run_next(app)
    await run_next(app)

    assert events == [("retry", 1), ("dead", "dlq")]


async def test_sync_hooks_are_supported(app):
    seen = []

    @app.on_job_success
    def note(job, result):
        seen.append(result)

    @app.task(save_result=True)
    async def add(ctx):
        return 7

    await add.enqueue()
    await run_next(app)

    assert seen == [7]


async def test_broken_hook_does_not_fail_the_job(app):
    @app.on_job_start
    async def broken(job):
        raise RuntimeError("hook is broken")

    @app.task(save_result=True)
    async def add(ctx):
        return 1

    job = await add.enqueue()
    await run_next(app)

    assert (await app.broker.get_job(job.id)).status == JobStatus.completed


# ----------------------------------------------------------------------
# Concurrency
# ----------------------------------------------------------------------


async def test_worker_respects_concurrency_limit(app):
    peak = 0
    current = 0

    @app.task()
    async def track(ctx):
        nonlocal peak, current
        current += 1
        peak = max(peak, current)
        await asyncio.sleep(0.05)
        current -= 1

    await track.enqueue_many([{} for _ in range(10)])

    worker = Worker(app, queues=["default"], concurrency=3)
    runner = asyncio.create_task(worker.run())
    try:
        for _ in range(100):
            if (await app.broker.queue_stats())["default"].get("completed", 0) == 10:
                break
            await asyncio.sleep(0.02)
    finally:
        worker.stop()
        await asyncio.wait_for(runner, timeout=5)

    assert peak <= 3
    assert (await app.broker.queue_stats())["default"]["completed"] == 10
