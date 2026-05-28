"""Tests for the dead letter queue (DLQ) feature."""

from __future__ import annotations

import fakeredis.aioredis
import pytest
import redis.asyncio as aioredis
from src.aioq.app import Aarq
from src.aioq.backends.redis import RedisBroker
from src.aioq.models import Job, JobStatus
from src.aioq.worker import Worker


@pytest.fixture
async def broker(monkeypatch):
    """RedisBroker backed by fakeredis with Lua deferred-promotion stubbed out."""
    fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(aioredis, "from_url", lambda *a, **kw: fake)

    b = RedisBroker()
    await b.connect()

    async def _noop_promote(queue: str, now: float) -> None:
        pass

    monkeypatch.setattr(b, "_promote_deferred", _noop_promote)
    yield b
    await b.disconnect()


# ---------------------------------------------------------------------------
# Broker-level DLQ tests (no worker needed)
# ---------------------------------------------------------------------------


async def test_dead_job_marked_dead(broker):
    """A job manually set to dead with a DLQ queue is retrievable via list_dead_jobs."""
    job = Job(
        task_name="tasks.send_email",
        queue="default",
        max_retries=3,
        dead_letter_queue="my-dlq",
    )
    await broker.enqueue(job)

    # Simulate exhausted retries: mark job dead and move to DLQ queue
    job.status = JobStatus.dead
    job.queue = "my-dlq"
    job.error = "permanent failure"
    await broker.update_job(job)

    fetched = await broker.get_job(job.id)
    assert fetched is not None
    assert fetched.status == JobStatus.dead
    assert fetched.queue == "my-dlq"
    assert fetched.error == "permanent failure"


async def test_list_dead_jobs_returns_only_dead(broker):
    """list_dead_jobs returns only jobs with status=dead."""
    dead_job = Job(task_name="tasks.a", queue="default", dead_letter_queue="dlq")
    failed_job = Job(task_name="tasks.b", queue="default")
    pending_job = Job(task_name="tasks.c", queue="default")

    await broker.enqueue(dead_job)
    await broker.enqueue(failed_job)
    await broker.enqueue(pending_job)

    # Mark one dead (moved to DLQ queue), one failed
    dead_job.status = JobStatus.dead
    dead_job.queue = "dlq"
    await broker.update_job(dead_job)

    failed_job.status = JobStatus.failed
    await broker.update_job(failed_job)

    dead_jobs = await broker.list_dead_jobs()
    assert len(dead_jobs) == 1
    assert dead_jobs[0].id == dead_job.id


async def test_list_dead_jobs_filter_by_queue(broker):
    """list_dead_jobs(queue=...) filters by DLQ queue name."""
    job_a = Job(task_name="tasks.a", queue="default", dead_letter_queue="dlq-a")
    job_b = Job(task_name="tasks.b", queue="default", dead_letter_queue="dlq-b")

    await broker.enqueue(job_a)
    await broker.enqueue(job_b)

    job_a.status = JobStatus.dead
    job_a.queue = "dlq-a"
    await broker.update_job(job_a)

    job_b.status = JobStatus.dead
    job_b.queue = "dlq-b"
    await broker.update_job(job_b)

    dlq_a_jobs = await broker.list_dead_jobs(queue="dlq-a")
    assert len(dlq_a_jobs) == 1
    assert dlq_a_jobs[0].id == job_a.id

    dlq_b_jobs = await broker.list_dead_jobs(queue="dlq-b")
    assert len(dlq_b_jobs) == 1
    assert dlq_b_jobs[0].id == job_b.id


async def test_replay_dead_job(broker):
    """replay_dead_job re-enqueues a dead job as pending with retries reset."""
    job = Job(
        task_name="tasks.send_email",
        queue="my-dlq",
        retries=3,
        max_retries=3,
        dead_letter_queue="my-dlq",
        error="all retries exhausted",
    )
    await broker.enqueue(job)

    # Mark as dead
    job.status = JobStatus.dead
    await broker.update_job(job)

    # Replay it
    result = await broker.replay_dead_job(job.id)
    assert result is True

    fetched = await broker.get_job(job.id)
    assert fetched is not None
    assert fetched.status == JobStatus.pending
    assert fetched.retries == 0
    assert fetched.error is None
    assert fetched.started_at is None
    assert fetched.completed_at is None


async def test_replay_non_dead_job_returns_false(broker):
    """replay_dead_job returns False when the job is not in dead status."""
    job = Job(task_name="tasks.add", queue="default")
    await broker.enqueue(job)

    # Job is still pending
    result = await broker.replay_dead_job(job.id)
    assert result is False

    # Job marked failed (not dead)
    job.status = JobStatus.failed
    await broker.update_job(job)
    result = await broker.replay_dead_job(job.id)
    assert result is False


async def test_replay_nonexistent_job_returns_false(broker):
    """replay_dead_job returns False for non-existent job IDs."""
    result = await broker.replay_dead_job("nonexistent-id")
    assert result is False


async def test_cancel_dead_job_fails(broker):
    """cancel_job should not be able to cancel a dead job."""
    job = Job(task_name="tasks.add", queue="default", dead_letter_queue="dlq")
    await broker.enqueue(job)

    job.status = JobStatus.dead
    job.queue = "dlq"
    await broker.update_job(job)

    result = await broker.cancel_job(job.id)
    assert result is False


async def test_retry_dead_job_fails(broker):
    """retry_job should not work on a dead job (use replay_dead_job instead)."""
    job = Job(task_name="tasks.add", queue="default", dead_letter_queue="dlq")
    await broker.enqueue(job)

    job.status = JobStatus.dead
    job.queue = "dlq"
    await broker.update_job(job)

    result = await broker.retry_job(job.id)
    assert result is False


# ---------------------------------------------------------------------------
# Worker-level DLQ integration test
# ---------------------------------------------------------------------------


async def test_worker_moves_job_to_dlq_on_final_failure(broker, monkeypatch):
    """Worker marks a job dead and moves it to the DLQ after all retries fail."""
    app = Aarq(broker)

    @app.task(queue="default", retries=1, dead_letter_queue="my-dlq")
    async def always_fails(ctx):
        raise ValueError("always fails")

    # Enqueue the job
    job = await always_fails.enqueue()
    assert job.dead_letter_queue == "my-dlq"
    assert job.max_retries == 1

    worker = Worker(app, queues=["default", "my-dlq"])

    # First attempt: retries < max_retries → status becomes retrying
    dequeued = await broker.dequeue(["default"], timeout=1.0)
    assert dequeued is not None
    await worker._process(dequeued)

    refreshed = await broker.get_job(job.id)
    assert refreshed.status == JobStatus.retrying
    assert refreshed.retries == 1

    # Second attempt (retry): retries == max_retries → should go to DLQ
    retry_job = await broker.dequeue(["default"], timeout=1.0)
    assert retry_job is not None
    await worker._process(retry_job)

    final = await broker.get_job(job.id)
    assert final.status == JobStatus.dead
    assert final.queue == "my-dlq"
    assert final.error is not None

    # Verify it shows up in list_dead_jobs
    dead_jobs = await broker.list_dead_jobs(queue="my-dlq")
    assert any(j.id == job.id for j in dead_jobs)
