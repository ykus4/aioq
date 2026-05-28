"""Tests for job dependency support in RedisBroker."""

from __future__ import annotations

import fakeredis.aioredis
import pytest
import redis.asyncio as aioredis
from src.aioq.backends.redis import RedisBroker
from src.aioq.models import Job, JobStatus


@pytest.fixture
async def broker(monkeypatch):
    """RedisBroker backed by fakeredis."""
    fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(aioredis, "from_url", lambda *a, **kw: fake)

    b = RedisBroker()
    await b.connect()

    # fakeredis does not support Lua eval; stub out the deferred-promotion step
    async def _noop_promote(queue: str, now: float) -> None:
        pass

    monkeypatch.setattr(b, "_promote_deferred", _noop_promote)
    yield b
    await b.disconnect()


async def test_job_without_deps_runs_immediately(broker):
    """A job with no dependencies is enqueued and dequeued normally."""
    job = Job(task_name="tasks.add", queue="default")
    await broker.enqueue(job)

    dequeued = await broker.dequeue(["default"], timeout=1.0)
    assert dequeued is not None
    assert dequeued.id == job.id
    assert dequeued.status == JobStatus.pending


async def test_dependent_job_waits(broker):
    """Job B with depends_on=[A.id] is stored as 'waiting' and NOT dequeued until A completes."""
    job_a = Job(task_name="tasks.step_a", queue="default")
    await broker.enqueue(job_a)

    job_b = Job(task_name="tasks.step_b", queue="default", depends_on=[job_a.id])
    await broker.enqueue(job_b)

    # Dequeue job A (should be available immediately)
    dequeued_a = await broker.dequeue(["default"], timeout=1.0)
    assert dequeued_a is not None
    assert dequeued_a.id == job_a.id

    # Job B should NOT be dequeued yet (A is not completed)
    dequeued_b = await broker.dequeue(["default"], timeout=0.1)
    assert dequeued_b is None

    # Verify B is stored with waiting status
    stored_b = await broker.get_job(job_b.id)
    assert stored_b is not None
    assert stored_b.status == JobStatus.waiting


async def test_dependent_job_runs_after_dep_completes(broker):
    """After A completes, B is automatically promoted to pending and can be dequeued."""
    job_a = Job(task_name="tasks.step_a", queue="default")
    await broker.enqueue(job_a)

    job_b = Job(task_name="tasks.step_b", queue="default", depends_on=[job_a.id])
    await broker.enqueue(job_b)

    # Dequeue and complete job A
    dequeued_a = await broker.dequeue(["default"], timeout=1.0)
    assert dequeued_a is not None

    dequeued_a.status = JobStatus.completed
    await broker.update_job(dequeued_a)

    # Now B should be available
    dequeued_b = await broker.dequeue(["default"], timeout=1.0)
    assert dequeued_b is not None
    assert dequeued_b.id == job_b.id


async def test_chain_dependency(broker):
    """A -> B -> C chain: each job only runs after its predecessor completes."""
    job_a = Job(task_name="tasks.step_a", queue="default")
    await broker.enqueue(job_a)

    job_b = Job(task_name="tasks.step_b", queue="default", depends_on=[job_a.id])
    await broker.enqueue(job_b)

    job_c = Job(task_name="tasks.step_c", queue="default", depends_on=[job_b.id])
    await broker.enqueue(job_c)

    # Only A is available initially
    dequeued_a = await broker.dequeue(["default"], timeout=1.0)
    assert dequeued_a is not None
    assert dequeued_a.id == job_a.id

    # B and C are not available
    nothing = await broker.dequeue(["default"], timeout=0.1)
    assert nothing is None

    # Complete A -> B becomes available
    dequeued_a.status = JobStatus.completed
    await broker.update_job(dequeued_a)

    dequeued_b = await broker.dequeue(["default"], timeout=1.0)
    assert dequeued_b is not None
    assert dequeued_b.id == job_b.id

    # C is still waiting
    nothing = await broker.dequeue(["default"], timeout=0.1)
    assert nothing is None

    # Complete B -> C becomes available
    dequeued_b.status = JobStatus.completed
    await broker.update_job(dequeued_b)

    dequeued_c = await broker.dequeue(["default"], timeout=1.0)
    assert dequeued_c is not None
    assert dequeued_c.id == job_c.id


async def test_waiting_job_is_cancellable(broker):
    """A waiting job (unmet deps) can be cancelled."""
    job_a = Job(task_name="tasks.step_a", queue="default")
    await broker.enqueue(job_a)

    job_b = Job(task_name="tasks.step_b", queue="default", depends_on=[job_a.id])
    await broker.enqueue(job_b)

    # B is waiting
    stored_b = await broker.get_job(job_b.id)
    assert stored_b.status == JobStatus.waiting

    # Cancel B
    result = await broker.cancel_job(job_b.id)
    assert result is True

    stored_b = await broker.get_job(job_b.id)
    assert stored_b.status == JobStatus.cancelled


async def test_dep_already_completed_runs_immediately(broker):
    """If dep A is already completed when B is enqueued, B runs immediately."""
    job_a = Job(task_name="tasks.step_a", queue="default")
    await broker.enqueue(job_a)

    # Complete job A
    job_a.status = JobStatus.completed
    await broker.update_job(job_a)

    # Drain the pending queue (A was pushed to pending when first enqueued)
    dequeued_a = await broker.dequeue(["default"], timeout=1.0)
    assert dequeued_a is not None
    assert dequeued_a.id == job_a.id

    # Now enqueue B — A is already completed
    job_b = Job(task_name="tasks.step_b", queue="default", depends_on=[job_a.id])
    await broker.enqueue(job_b)

    # B should be immediately available
    dequeued_b = await broker.dequeue(["default"], timeout=1.0)
    assert dequeued_b is not None
    assert dequeued_b.id == job_b.id
