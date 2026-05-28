"""Tests for priority queue support in RedisBroker."""

import fakeredis.aioredis
import pytest
from src.aioq.backends.redis import RedisBroker
from src.aioq.models import Job


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


async def test_high_priority_dequeued_first(broker):
    """A high-priority job enqueued after a low-priority job should come out first."""
    low = Job(task_name="tasks.low", queue="default", priority=0)
    high = Job(task_name="tasks.high", queue="default", priority=10)

    await broker.enqueue(low)
    await broker.enqueue(high)

    first = await broker.dequeue(["default"], timeout=1.0)
    assert first is not None
    assert first.id == high.id, "High priority job should be dequeued first"

    second = await broker.dequeue(["default"], timeout=1.0)
    assert second is not None
    assert second.id == low.id, "Low priority job should be dequeued second"


async def test_mid_priority_ordering(broker):
    """Mid-priority (5) jobs should come out before low (0) but after high (10)."""
    low = Job(task_name="tasks.low", queue="default", priority=0)
    mid = Job(task_name="tasks.mid", queue="default", priority=5)
    high = Job(task_name="tasks.high", queue="default", priority=10)

    await broker.enqueue(low)
    await broker.enqueue(mid)
    await broker.enqueue(high)

    order = []
    for _ in range(3):
        job = await broker.dequeue(["default"], timeout=1.0)
        assert job is not None
        order.append(job.priority)

    assert order == [10, 5, 0], f"Expected [10, 5, 0] but got {order}"


async def test_same_priority_fifo(broker):
    """Jobs with the same priority should be dequeued in FIFO order."""
    j1 = Job(task_name="tasks.t1", queue="default", priority=5)
    j2 = Job(task_name="tasks.t2", queue="default", priority=5)
    j3 = Job(task_name="tasks.t3", queue="default", priority=5)

    await broker.enqueue(j1)
    await broker.enqueue(j2)
    await broker.enqueue(j3)

    ids = []
    for _ in range(3):
        job = await broker.dequeue(["default"], timeout=1.0)
        assert job is not None
        ids.append(job.id)

    assert ids == [j1.id, j2.id, j3.id], f"Expected FIFO order but got {ids}"


async def test_priority_preserved_in_job_data(broker):
    """The priority field should be stored and retrieved correctly."""
    job = Job(task_name="tasks.add", queue="default", priority=10)
    await broker.enqueue(job)

    fetched = await broker.get_job(job.id)
    assert fetched is not None
    assert fetched.priority == 10


async def test_default_priority_zero(broker):
    """Jobs without an explicit priority default to 0."""
    job = Job(task_name="tasks.add", queue="default")
    await broker.enqueue(job)

    fetched = await broker.get_job(job.id)
    assert fetched is not None
    assert fetched.priority == 0
