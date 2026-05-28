from __future__ import annotations

import fakeredis.aioredis
import pytest
from src.aioq.backends.redis import RedisBroker
from src.aioq.models import Job, JobStatus


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


async def test_enqueue_many_all_enqueued(broker):
    """All 5 jobs enqueued via enqueue_many can be dequeued."""
    jobs = [Job(task_name="tasks.add", queue="default", kwargs={"n": i}) for i in range(5)]
    await broker.enqueue_many(jobs)

    dequeued = []
    for _ in range(5):
        job = await broker.dequeue(["default"], timeout=1.0)
        assert job is not None
        dequeued.append(job)

    assert len(dequeued) == 5
    original_ids = {j.id for j in jobs}
    assert {j.id for j in dequeued} == original_ids


async def test_enqueue_many_returns_jobs(broker):
    """enqueue_many returns Job objects with the correct ids."""
    jobs = [Job(task_name="tasks.noop", queue="default") for _ in range(3)]
    await broker.enqueue_many(jobs)

    # All returned jobs should appear in list_jobs
    listed = await broker.list_jobs(status=JobStatus.pending)
    listed_ids = {j.id for j in listed}
    for job in jobs:
        assert job.id in listed_ids


async def test_enqueue_many_empty_list(broker):
    """Calling enqueue_many with an empty list is a no-op."""
    await broker.enqueue_many([])
    listed = await broker.list_jobs()
    assert listed == []


async def test_enqueue_many_kwargs_preserved(broker):
    """Kwargs are stored and retrieved correctly for each job."""
    jobs = [Job(task_name="tasks.add", queue="default", kwargs={"value": i}) for i in range(3)]
    await broker.enqueue_many(jobs)

    for original in jobs:
        fetched = await broker.get_job(original.id)
        assert fetched is not None
        assert fetched.kwargs == original.kwargs


async def test_task_def_enqueue_many(monkeypatch):
    """TaskDef.enqueue_many creates and enqueues jobs for each item."""
    import redis.asyncio as aioredis

    fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(aioredis, "from_url", lambda *a, **kw: fake)

    from src.aioq.app import Aarq
    from src.aioq.backends.redis import RedisBroker

    broker = RedisBroker()
    await broker.connect()

    async def _noop_promote(queue: str, now: float) -> None:
        pass

    monkeypatch.setattr(broker, "_promote_deferred", _noop_promote)

    app = Aarq(broker)

    @app.task(queue="default")
    async def my_task(ctx, x: int) -> int:
        return x * 2

    items = [{"x": i} for i in range(4)]
    returned_jobs = await my_task.enqueue_many(items)

    assert len(returned_jobs) == 4
    listed = await broker.list_jobs(status=JobStatus.pending)
    listed_ids = {j.id for j in listed}
    for job in returned_jobs:
        assert job.id in listed_ids

    await broker.disconnect()
