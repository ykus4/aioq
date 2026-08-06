"""Integration tests against a real Redis instance (redis://localhost:6380).

Run with:
    uv run --extra dev pytest tests/test_integration_redis.py -v

These tests are skipped automatically if no Redis is reachable at REDIS_URL.
In CI the redis service is mapped to port 6380.
"""

from __future__ import annotations

import asyncio

import pytest
import redis.asyncio as aioredis

from aioq import Aioq
from aioq.backends.redis import RedisBroker
from aioq.models import Job, JobStatus
from aioq.worker import Worker

REDIS_URL = "redis://localhost:6380"

pytestmark = pytest.mark.integration


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: requires a live external service")


@pytest.fixture(scope="session", autouse=True)
async def require_redis():
    """Skip the entire module if Redis is not reachable."""
    try:
        r = aioredis.from_url(REDIS_URL, socket_connect_timeout=1)
        await r.ping()
        await r.aclose()
    except Exception:
        pytest.skip("Redis not available at " + REDIS_URL, allow_module_level=True)


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
    return Aioq(broker=broker)


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
    disabled_app = Aioq(broker=broker, dashboard_enabled=False)
    from aioq.dashboard.app import create_dashboard

    with pytest.raises(RuntimeError, match="disabled"):
        create_dashboard(disabled_app)


async def test_dashboard_enabled_by_default(broker):
    enabled_app = Aioq(broker=broker)
    from aioq.dashboard.app import create_dashboard

    dash = create_dashboard(enabled_app)
    assert dash is not None


# ------------------------------------------------------------------
# Deferred jobs and the Lua promotion script
# ------------------------------------------------------------------


async def test_deferred_job_is_not_immediately_runnable(broker):
    from datetime import UTC, datetime, timedelta

    job = Job(task_name="t", queue="deferred-q", run_at=datetime.now(UTC) + timedelta(hours=1))
    await broker.enqueue(job)

    assert await broker.dequeue(["deferred-q"], timeout=0.5) is None


async def test_deferred_job_runs_once_due(broker):
    from datetime import UTC, datetime, timedelta

    job = Job(task_name="t", queue="deferred-q", run_at=datetime.now(UTC) + timedelta(seconds=1))
    await broker.enqueue(job)
    assert await broker.dequeue(["deferred-q"], timeout=0.2) is None

    await asyncio.sleep(1.1)
    dequeued = await broker.dequeue(["deferred-q"], timeout=2.0)

    assert dequeued is not None
    assert dequeued.id == job.id


async def test_deferred_job_keeps_its_priority(broker):
    """Exercises the Lua promotion script against a real Redis."""
    from datetime import UTC, datetime, timedelta

    from aioq.backends.redis import _pending_key

    run_at = datetime.now(UTC) + timedelta(minutes=5)
    urgent = Job(task_name="t", queue="tier-q", priority=10, run_at=run_at)
    await broker.enqueue(urgent)

    await broker._promote_deferred(["tier-q"], run_at.timestamp() + 1)

    assert await broker.redis.llen(_pending_key("tier-q", 10)) == 1
    assert await broker.redis.llen(_pending_key("tier-q", 0)) == 0


async def test_promotion_handles_a_large_batch(broker):
    """The Lua script chunks unpack(); a big batch must not overflow the stack."""
    from datetime import UTC, datetime, timedelta

    from aioq.backends.redis import _pending_key

    run_at = datetime.now(UTC) + timedelta(minutes=5)
    jobs = [Job(task_name="t", queue="bulk-q", run_at=run_at) for _ in range(1500)]
    await broker.enqueue_many(jobs)

    await broker._promote_deferred(["bulk-q"], run_at.timestamp() + 1)

    assert await broker.redis.llen(_pending_key("bulk-q", 0)) == 1500


async def test_priority_beats_queue_order(broker):
    """An urgent job in a later queue still wins."""
    normal = Job(task_name="t", queue="q-first", priority=0)
    urgent = Job(task_name="t", queue="q-second", priority=10)
    await broker.enqueue(normal)
    await broker.enqueue(urgent)

    first = await broker.dequeue(["q-first", "q-second"], timeout=2.0)

    assert first.id == urgent.id


# ------------------------------------------------------------------
# Retries
# ------------------------------------------------------------------


async def test_retry_is_deferred_and_then_runs(broker, app):
    attempts = []

    @app.task(queue="retry-q", retries=1, retry_delay=1.0)
    async def flaky(ctx):
        attempts.append(1)
        if len(attempts) == 1:
            raise ValueError("first attempt fails")

    job = await flaky.enqueue()
    worker = Worker(app, queues=["retry-q"])

    await worker._process(await broker.dequeue(["retry-q"], timeout=2.0))

    retried = await broker.get_job(job.id)
    assert retried.status == JobStatus.retrying
    assert retried.run_at is not None
    # Not runnable yet — the delay parks it in the deferred set.
    assert await broker.dequeue(["retry-q"], timeout=0.2) is None

    await asyncio.sleep(1.1)
    second = await broker.dequeue(["retry-q"], timeout=2.0)
    assert second is not None
    await worker._process(second)

    assert (await broker.get_job(job.id)).status == JobStatus.completed
    assert len(attempts) == 2


async def test_requeued_job_is_counted_under_one_status(broker, app):
    @app.task(queue="count-q", retries=1, retry_delay=60)
    async def boom(ctx):
        raise ValueError("nope")

    await boom.enqueue()
    worker = Worker(app, queues=["count-q"])
    await worker._process(await broker.dequeue(["count-q"], timeout=2.0))

    assert (await broker.queue_stats())["count-q"] == {"retrying": 1}


# ------------------------------------------------------------------
# Timeouts
# ------------------------------------------------------------------


async def test_timeout_fails_the_job(broker, app):
    @app.task(queue="timeout-q", timeout=0.2)
    async def slow(ctx):
        await asyncio.sleep(10)

    job = await slow.enqueue()
    worker = Worker(app, queues=["timeout-q"])
    await worker._process(await broker.dequeue(["timeout-q"], timeout=2.0))

    fetched = await broker.get_job(job.id)
    assert fetched.status == JobStatus.failed
    assert "timeout" in fetched.error.lower()


# ------------------------------------------------------------------
# Cron locks
# ------------------------------------------------------------------


async def test_cron_lock_is_granted_once(broker):
    assert await broker.acquire_cron_lock("integration-cron@1000", ttl=30) is True
    assert await broker.acquire_cron_lock("integration-cron@1000", ttl=30) is False
    assert await broker.acquire_cron_lock("integration-cron@1060", ttl=30) is True


async def test_only_one_worker_fires_a_cron_occurrence(broker, app):
    fired = []

    @app.cron("* * * * *")
    async def tick(ctx):
        fired.append(ctx["worker_id"])

    cron = app.crons[0]
    occurrence = cron.next_run()

    workers = [Worker(app), Worker(app)]
    for worker in workers:
        await worker._maybe_fire_cron(cron, occurrence)
    await asyncio.gather(*(t for w in workers for t in w._cron_tasks))

    assert len(fired) == 1


# ------------------------------------------------------------------
# Result TTL and purging
# ------------------------------------------------------------------


async def test_saved_result_gets_a_ttl(broker):
    from aioq.backends.redis import _job_key

    job = Job(task_name="t", queue="ttl-q", save_result=True, result_ttl=60)
    await broker.enqueue(job)

    job.status = JobStatus.completed
    job.result = {"answer": 42}
    await broker.update_job(job)

    assert 0 < await broker.redis.ttl(_job_key(job.id)) <= 60
    assert (await broker.get_job(job.id)).result == {"answer": 42}


async def test_purge_removes_old_finished_jobs(broker):
    from datetime import UTC, datetime, timedelta

    old = Job(task_name="t", queue="purge-q")
    fresh = Job(task_name="t", queue="purge-q")
    await broker.enqueue(old)
    await broker.enqueue(fresh)

    old.status = JobStatus.completed
    old.completed_at = datetime.now(UTC) - timedelta(days=2)
    await broker.update_job(old)

    removed = await broker.purge(older_than=3600)

    assert removed == 1
    assert await broker.get_job(old.id) is None
    assert await broker.get_job(fresh.id) is not None


async def test_workers_hash_has_no_ttl(broker):
    """A TTL on the hash would drop every worker at once."""
    await broker.register_worker("ttl-check-1", ["default"])
    assert await broker.redis.ttl("aioq:workers") == -1
    await broker.deregister_worker("ttl-check-1")
