import json

import fakeredis.aioredis
import pytest

from aioq.backends.redis import RedisBroker, _job_key
from aioq.models import Job, JobStatus


@pytest.fixture
async def broker(monkeypatch):
    """RedisBroker backed by fakeredis."""
    import redis.asyncio as aioredis

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
    """A job with no dependencies should be enqueued as pending."""
    job = Job(task_name="tasks.noop", queue="default")
    await broker.enqueue(job)

    fetched = await broker.get_job(job.id)
    assert fetched is not None
    assert fetched.status == JobStatus.pending

    # It should be dequeue-able immediately
    dequeued = await broker.dequeue(["default"], timeout=1.0)
    assert dequeued is not None
    assert dequeued.id == job.id


async def test_dependent_job_waits(broker):
    """A job whose dependency is not yet completed should be status=waiting."""
    dep = Job(task_name="tasks.first", queue="default")
    await broker.enqueue(dep)

    child = Job(task_name="tasks.second", queue="default", depends_on=[dep.id])
    await broker.enqueue(child)

    fetched = await broker.get_job(child.id)
    assert fetched is not None
    assert fetched.status == JobStatus.waiting

    # The child should NOT be dequeue-able (it's waiting, not in the pending list)
    dequeued = await broker.dequeue(["default"], timeout=0.2)
    # Only the parent should be dequeue-able
    assert dequeued is not None
    assert dequeued.id == dep.id


async def test_dependent_job_runs_after_dep_completes(broker):
    """When the dependency completes, the waiting job is promoted to pending."""
    dep = Job(task_name="tasks.first", queue="default")
    await broker.enqueue(dep)

    child = Job(task_name="tasks.second", queue="default", depends_on=[dep.id])
    await broker.enqueue(child)

    # Confirm child is waiting
    assert (await broker.get_job(child.id)).status == JobStatus.waiting  # type: ignore[union-attr]

    # Drain the dep job from the queue first (simulate worker picking it up)
    popped = await broker.dequeue(["default"], timeout=1.0)
    assert popped is not None
    assert popped.id == dep.id

    # Mark dep as completed — this should promote child to pending
    dep.status = JobStatus.completed
    await broker.update_job(dep)

    # Child should now be pending
    fetched_child = await broker.get_job(child.id)
    assert fetched_child is not None
    assert fetched_child.status == JobStatus.pending

    # And dequeue-able
    dequeued = await broker.dequeue(["default"], timeout=1.0)
    assert dequeued is not None
    assert dequeued.id == child.id


async def test_chain_dependency(broker):
    """A -> B -> C: C stays waiting until both A and B complete."""
    job_a = Job(task_name="tasks.a", queue="default")
    await broker.enqueue(job_a)

    job_b = Job(task_name="tasks.b", queue="default", depends_on=[job_a.id])
    await broker.enqueue(job_b)

    job_c = Job(task_name="tasks.c", queue="default", depends_on=[job_b.id])
    await broker.enqueue(job_c)

    assert (await broker.get_job(job_b.id)).status == JobStatus.waiting  # type: ignore[union-attr]
    assert (await broker.get_job(job_c.id)).status == JobStatus.waiting  # type: ignore[union-attr]

    # Complete A -> B becomes pending, C stays waiting
    job_a.status = JobStatus.completed
    await broker.update_job(job_a)

    assert (await broker.get_job(job_b.id)).status == JobStatus.pending  # type: ignore[union-attr]
    assert (await broker.get_job(job_c.id)).status == JobStatus.waiting  # type: ignore[union-attr]

    # Complete B -> C becomes pending
    job_b.status = JobStatus.completed
    await broker.update_job(job_b)

    assert (await broker.get_job(job_c.id)).status == JobStatus.pending  # type: ignore[union-attr]


async def test_waiting_job_is_cancellable(broker):
    """A waiting job can be cancelled before its dependency completes."""
    dep = Job(task_name="tasks.first", queue="default")
    await broker.enqueue(dep)

    child = Job(task_name="tasks.second", queue="default", depends_on=[dep.id])
    await broker.enqueue(child)

    assert (await broker.get_job(child.id)).status == JobStatus.waiting  # type: ignore[union-attr]

    cancelled = await broker.cancel_job(child.id)
    assert cancelled is True

    fetched = await broker.get_job(child.id)
    assert fetched is not None
    assert fetched.status == JobStatus.cancelled


async def test_dep_already_completed_runs_immediately(broker):
    """If all dependencies are already completed, the job is enqueued as pending right away."""
    dep = Job(task_name="tasks.first", queue="default")
    dep.status = JobStatus.completed
    # Store the completed dep directly in Redis
    await broker.redis.set(_job_key(dep.id), json.dumps(dep.model_dump_json_safe()))
    await broker.redis.sadd("aioq:jobs:all", dep.id)
    await broker.redis.sadd(f"aioq:jobs:status:{JobStatus.completed.value}", dep.id)

    child = Job(task_name="tasks.second", queue="default", depends_on=[dep.id])
    await broker.enqueue(child)

    fetched = await broker.get_job(child.id)
    assert fetched is not None
    assert fetched.status == JobStatus.pending

    dequeued = await broker.dequeue(["default"], timeout=1.0)
    assert dequeued is not None
    assert dequeued.id == child.id
