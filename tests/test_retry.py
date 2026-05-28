import fakeredis.aioredis
import pytest
import redis.asyncio as aioredis
from src.aioq.backends.redis import RedisBroker
from src.aioq.models import Job, JobStatus


@pytest.fixture
async def broker(monkeypatch):
    fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(aioredis, "from_url", lambda *a, **kw: fake)
    b = RedisBroker()
    await b.connect()
    yield b
    await b.disconnect()


async def test_retry_failed_job(broker):
    job = Job(task_name="tasks.add", queue="default", retries=2, max_retries=3)
    await broker.enqueue(job)

    job.status = JobStatus.failed
    job.error = "something went wrong"
    await broker.update_job(job)

    result = await broker.retry_job(job.id)
    assert result is True

    fetched = await broker.get_job(job.id)
    assert fetched.status == JobStatus.pending
    assert fetched.retries == 0
    assert fetched.error is None


async def test_retry_cancelled_job(broker):
    job = Job(task_name="tasks.add", queue="default")
    await broker.enqueue(job)

    await broker.cancel_job(job.id)
    fetched = await broker.get_job(job.id)
    assert fetched.status == JobStatus.cancelled

    result = await broker.retry_job(job.id)
    assert result is True

    fetched = await broker.get_job(job.id)
    assert fetched.status == JobStatus.pending


async def test_retry_running_job_fails(broker):
    job = Job(task_name="tasks.add", queue="default")
    await broker.enqueue(job)

    job.status = JobStatus.running
    await broker.update_job(job)

    result = await broker.retry_job(job.id)
    assert result is False
