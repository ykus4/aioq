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


async def test_enqueue_dequeue(broker):
    job = Job(task_name="tasks.add", queue="default", kwargs={"a": 1, "b": 2})
    await broker.enqueue(job)

    dequeued = await broker.dequeue(["default"], timeout=1.0)
    assert dequeued is not None
    assert dequeued.id == job.id
    assert dequeued.kwargs == {"a": 1, "b": 2}


async def test_update_status(broker):
    job = Job(task_name="tasks.add", queue="default")
    await broker.enqueue(job)

    job.status = JobStatus.running
    await broker.update_job(job)

    fetched = await broker.get_job(job.id)
    assert fetched.status == JobStatus.running


async def test_list_jobs_by_status(broker):
    j1 = Job(task_name="tasks.add", queue="default")
    j2 = Job(task_name="tasks.add", queue="default")
    await broker.enqueue(j1)
    await broker.enqueue(j2)

    j1.status = JobStatus.completed
    await broker.update_job(j1)

    completed = await broker.list_jobs(status=JobStatus.completed)
    pending = await broker.list_jobs(status=JobStatus.pending)

    assert any(j.id == j1.id for j in completed)
    assert any(j.id == j2.id for j in pending)


async def test_worker_registration(broker):
    await broker.register_worker("w1", ["default"])
    workers = await broker.list_workers()
    assert any(w["worker_id"] == "w1" for w in workers)

    await broker.deregister_worker("w1")
    workers = await broker.list_workers()
    assert not any(w["worker_id"] == "w1" for w in workers)
