"""Integration tests against a real Redis instance (redis://localhost:6380).

Run with:
    uv run --extra dev pytest tests/test_integration_redis.py -v
"""
from __future__ import annotations

import asyncio

import pytest

from aioq.app import Aarq
from aioq.backends.redis import RedisBroker
from aioq.models import Job, JobStatus
from aioq.worker import Worker

REDIS_URL = "redis://localhost:6380"


@pytest.fixture
async def broker():
    b = RedisBroker(url=REDIS_URL)
    await b.connect()
    # Flush test keys only
    keys = await b.redis.keys("aioq:*")
    if keys:
        await b.redis.delete(*keys)
    yield b
    keys = await b.redis.keys("aioq:*")
    if keys:
        await b.redis.delete(*keys)
    await b.disconnect()


@pytest.fixture
def app(broker):
    return Aarq(broker=broker)


# ------------------------------------------------------------------
# Basic enqueue / dequeue
# ------------------------------------------------------------------


async def test_enqueue_and_dequeue(broker):
    job = Job(task_name="test.task", queue="default")
    await broker.enqueue(job)

    fetched = await broker.dequeue(["default"], timeout=2.0)
    assert fetched is not None
    assert fetched.id == job.id
    # Redis dequeue returns the job as-is (status update is done by the worker)
    assert fetched.status == JobStatus.pending


async def test_dequeue_returns_none_on_empty(broker):
    result = await broker.dequeue(["empty-queue"], timeout=0.5)
    assert result is None


# ------------------------------------------------------------------
# Priority
# ------------------------------------------------------------------


async def test_priority_ordering(broker):
    low = Job(task_name="t", queue="pq", priority=0)
    high = Job(task_name="t", queue="pq", priority=10)
    mid = Job(task_name="t", queue="pq", priority=5)

    await broker.enqueue(low)
    await broker.enqueue(high)
    await broker.enqueue(mid)

    first = await broker.dequeue(["pq"], timeout=2.0)
    second = await broker.dequeue(["pq"], timeout=2.0)
    third = await broker.dequeue(["pq"], timeout=2.0)

    assert first.priority == 10
    assert second.priority == 5
    assert third.priority == 0


# ------------------------------------------------------------------
# Job status lifecycle
# ------------------------------------------------------------------


async def test_update_job_status(broker):
    job = Job(task_name="t", queue="default")
    await broker.enqueue(job)
    job.status = JobStatus.running
    await broker.update_job(job)

    fetched = await broker.get_job(job.id)
    assert fetched.status == JobStatus.running


async def test_cancel_job(broker):
    job = Job(task_name="t", queue="default")
    await broker.enqueue(job)

    cancelled = await broker.cancel_job(job.id)
    assert cancelled is True

    fetched = await broker.get_job(job.id)
    assert fetched.status == JobStatus.cancelled


async def test_retry_job(broker):
    job = Job(task_name="t", queue="default")
    job.status = JobStatus.failed
    await broker.enqueue(job)
    await broker.update_job(job)

    retried = await broker.retry_job(job.id)
    assert retried is True

    fetched = await broker.get_job(job.id)
    assert fetched.status == JobStatus.pending


# ------------------------------------------------------------------
# Batch enqueue
# ------------------------------------------------------------------


async def test_enqueue_many(broker, app):
    @app.task(queue="batch")
    async def batch_task(ctx, n: int):
        return n

    jobs = await batch_task.enqueue_many([{"n": i} for i in range(5)])
    assert len(jobs) == 5

    stats = await broker.queue_stats()
    assert stats["batch"]["pending"] == 5


# ------------------------------------------------------------------
# Dead letter queue
# ------------------------------------------------------------------


async def test_dead_letter_queue(broker, app):
    results = []

    @app.task(queue="default", retries=0, dead_letter_queue="dlq")
    async def failing_task(ctx):
        raise ValueError("boom")

    job = await failing_task.enqueue()

    worker = Worker(app, queues=["default"], concurrency=1)
    worker._semaphore = asyncio.Semaphore(1)

    dequeued = await broker.dequeue(["default"], timeout=2.0)
    assert dequeued is not None
    dequeued.status = JobStatus.running
    await worker._process(dequeued)

    fetched = await broker.get_job(job.id)
    assert fetched.status == JobStatus.dead
    assert fetched.queue == "dlq"

    dead_jobs = await broker.list_dead_jobs(queue="dlq")
    assert any(j.id == job.id for j in dead_jobs)


# ------------------------------------------------------------------
# Job dependencies
# ------------------------------------------------------------------


async def test_job_dependencies(broker):
    dep = Job(task_name="t", queue="default")
    await broker.enqueue(dep)

    child = Job(task_name="t", queue="default", depends_on=[dep.id])
    await broker.enqueue(child)

    # child should be waiting
    fetched_child = await broker.get_job(child.id)
    assert fetched_child.status == JobStatus.waiting

    # complete the dependency
    dep.status = JobStatus.completed
    await broker.update_job(dep)

    # child should now be pending
    fetched_child = await broker.get_job(child.id)
    assert fetched_child.status == JobStatus.pending


# ------------------------------------------------------------------
# Worker registration
# ------------------------------------------------------------------


async def test_worker_registration(broker):
    await broker.register_worker("worker-1", ["default"])
    workers = await broker.list_workers()
    ids = [w["worker_id"] for w in workers]
    assert "worker-1" in ids

    await broker.heartbeat_worker("worker-1")
    await broker.deregister_worker("worker-1")

    workers = await broker.list_workers()
    ids = [w["worker_id"] for w in workers]
    assert "worker-1" not in ids


# ------------------------------------------------------------------
# Dashboard toggle
# ------------------------------------------------------------------


async def test_dashboard_disabled(broker):
    disabled_app = Aarq(broker=broker, dashboard_enabled=False)
    from aioq.dashboard.app import create_dashboard

    with pytest.raises(RuntimeError, match="disabled"):
        create_dashboard(disabled_app)


async def test_dashboard_enabled_by_default(broker):
    enabled_app = Aarq(broker=broker)
    from aioq.dashboard.app import create_dashboard

    dash = create_dashboard(enabled_app)
    assert dash is not None
