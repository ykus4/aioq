import fakeredis.aioredis
import pytest
import redis.asyncio as aioredis

from aioq.backends.redis import RedisBroker
from aioq.models import Job, JobStatus


@pytest.fixture
async def broker(monkeypatch):
    fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(aioredis, "from_url", lambda *a, **kw: fake)
    b = RedisBroker()
    await b.connect()
    yield b
    await b.disconnect()


async def test_cancel_pending_job(broker):
    job = Job(task_name="tasks.add", queue="default")
    await broker.enqueue(job)

    result = await broker.cancel_job(job.id)
    assert result is True

    fetched = await broker.get_job(job.id)
    assert fetched.status == JobStatus.cancelled


async def test_cancel_running_job_fails(broker):
    job = Job(task_name="tasks.add", queue="default")
    await broker.enqueue(job)

    job.status = JobStatus.running
    await broker.update_job(job)

    result = await broker.cancel_job(job.id)
    assert result is False

    fetched = await broker.get_job(job.id)
    assert fetched.status == JobStatus.running


async def test_cancel_nonexistent_job(broker):
    result = await broker.cancel_job("nonexistent-id")
    assert result is False
