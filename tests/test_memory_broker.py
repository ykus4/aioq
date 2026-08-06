"""Tests for the in-process MemoryBroker."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from aioq.backends.memory import MemoryBroker
from aioq.models import Job, JobStatus


@pytest.fixture
async def broker():
    async with MemoryBroker() as b:
        yield b


def make_job(**kwargs) -> Job:
    kwargs.setdefault("task_name", "tasks.add")
    kwargs.setdefault("queue", "default")
    return Job(**kwargs)


async def test_enqueue_then_dequeue(broker):
    job = make_job()
    await broker.enqueue(job)

    dequeued = await broker.dequeue(["default"], timeout=0.1)
    assert dequeued is not None
    assert dequeued.id == job.id


async def test_dequeue_returns_none_when_empty(broker):
    assert await broker.dequeue(["default"], timeout=0.05) is None


async def test_dequeue_ignores_other_queues(broker):
    await broker.enqueue(make_job(queue="emails"))
    assert await broker.dequeue(["default"], timeout=0.05) is None


async def test_higher_priority_dequeues_first(broker):
    low = make_job(priority=0)
    high = make_job(priority=10)
    mid = make_job(priority=5)
    await broker.enqueue(low)
    await broker.enqueue(high)
    await broker.enqueue(mid)

    order = [
        (await broker.dequeue(["default"], timeout=0.1)).id,
        (await broker.dequeue(["default"], timeout=0.1)).id,
        (await broker.dequeue(["default"], timeout=0.1)).id,
    ]
    assert order == [high.id, mid.id, low.id]


async def test_priority_beats_queue_order(broker):
    """An urgent job in a later queue still wins over a normal one in the first."""
    normal = make_job(queue="default", priority=0)
    urgent = make_job(queue="high", priority=10)
    await broker.enqueue(normal)
    await broker.enqueue(urgent)

    first = await broker.dequeue(["default", "high"], timeout=0.1)
    assert first.id == urgent.id


async def test_deferred_job_is_not_immediately_runnable(broker):
    job = make_job(run_at=datetime.now(UTC) + timedelta(seconds=60))
    await broker.enqueue(job)
    assert await broker.dequeue(["default"], timeout=0.05) is None


async def test_deferred_job_runs_once_due(broker):
    job = make_job(run_at=datetime.now(UTC) - timedelta(seconds=1))
    await broker.enqueue(job)
    dequeued = await broker.dequeue(["default"], timeout=0.1)
    assert dequeued is not None and dequeued.id == job.id


async def test_enqueue_many(broker):
    jobs = [make_job() for _ in range(3)]
    await broker.enqueue_many(jobs)
    stats = await broker.queue_stats()
    assert stats["default"]["pending"] == 3


async def test_cancel_removes_job_from_queue(broker):
    job = make_job()
    await broker.enqueue(job)

    assert await broker.cancel_job(job.id) is True
    assert (await broker.get_job(job.id)).status == JobStatus.cancelled
    assert await broker.dequeue(["default"], timeout=0.05) is None


async def test_cancel_running_job_fails(broker):
    job = make_job()
    await broker.enqueue(job)
    job.status = JobStatus.running
    await broker.update_job(job)

    assert await broker.cancel_job(job.id) is False


async def test_retry_job_resets_state(broker):
    job = make_job(retries=2, max_retries=3)
    await broker.enqueue(job)
    job.status = JobStatus.failed
    job.error = "boom"
    await broker.update_job(job)

    assert await broker.retry_job(job.id) is True
    refreshed = await broker.get_job(job.id)
    assert refreshed.status == JobStatus.pending
    assert refreshed.retries == 0
    assert refreshed.error is None


async def test_waiting_job_runs_after_dependency_completes(broker):
    dep = make_job()
    await broker.enqueue(dep)
    child = make_job(depends_on=[dep.id])
    await broker.enqueue(child)

    assert (await broker.get_job(child.id)).status == JobStatus.waiting

    dep.status = JobStatus.completed
    await broker.update_job(dep)

    assert (await broker.get_job(child.id)).status == JobStatus.pending


async def test_dependency_already_completed_enqueues_immediately(broker):
    dep = make_job()
    await broker.enqueue(dep)
    dep.status = JobStatus.completed
    await broker.update_job(dep)

    child = make_job(depends_on=[dep.id])
    await broker.enqueue(child)

    assert (await broker.get_job(child.id)).status == JobStatus.pending


async def test_list_jobs_filters(broker):
    a = make_job(queue="default")
    b = make_job(queue="emails")
    await broker.enqueue(a)
    await broker.enqueue(b)
    b.status = JobStatus.completed
    await broker.update_job(b)

    assert {j.id for j in await broker.list_jobs(queue="emails")} == {b.id}
    assert {j.id for j in await broker.list_jobs(status=JobStatus.pending)} == {a.id}
    assert len(await broker.list_jobs()) == 2


async def test_list_dead_jobs(broker):
    dead = make_job(dead_letter_queue="dlq")
    await broker.enqueue(dead)
    dead.status = JobStatus.dead
    dead.queue = "dlq"
    await broker.update_job(dead)

    assert [j.id for j in await broker.list_dead_jobs(queue="dlq")] == [dead.id]


async def test_replay_dead_job(broker):
    job = make_job(dead_letter_queue="dlq", retries=3)
    await broker.enqueue(job)
    job.status = JobStatus.dead
    await broker.update_job(job)

    assert await broker.replay_dead_job(job.id) is True
    refreshed = await broker.get_job(job.id)
    assert refreshed.status == JobStatus.pending
    assert refreshed.retries == 0


async def test_worker_registration_and_liveness(broker):
    await broker.register_worker("w1", ["default"])
    workers = await broker.list_workers()
    assert len(workers) == 1
    assert workers[0]["alive"] is True

    await broker.heartbeat_worker("w1")
    await broker.deregister_worker("w1")
    assert await broker.list_workers() == []


async def test_cron_lock_is_exclusive(broker):
    assert await broker.acquire_cron_lock("job@100", ttl=60) is True
    assert await broker.acquire_cron_lock("job@100", ttl=60) is False
    # A different occurrence is a different lock.
    assert await broker.acquire_cron_lock("job@160", ttl=60) is True


async def test_cron_lock_expires(broker):
    assert await broker.acquire_cron_lock("job@100", ttl=-1) is True
    assert await broker.acquire_cron_lock("job@100", ttl=60) is True


async def test_purge_removes_only_old_terminal_jobs(broker):
    old = make_job()
    recent = make_job()
    pending = make_job()
    for job in (old, recent, pending):
        await broker.enqueue(job)

    old.status = JobStatus.completed
    old.completed_at = datetime.now(UTC) - timedelta(hours=2)
    await broker.update_job(old)

    recent.status = JobStatus.completed
    recent.completed_at = datetime.now(UTC)
    await broker.update_job(recent)

    removed = await broker.purge(older_than=3600)

    assert removed == 1
    assert await broker.get_job(old.id) is None
    assert await broker.get_job(recent.id) is not None
    assert await broker.get_job(pending.id) is not None


async def test_purge_respects_status_filter(broker):
    completed = make_job()
    failed = make_job()
    for job in (completed, failed):
        await broker.enqueue(job)

    for job, status in ((completed, JobStatus.completed), (failed, JobStatus.failed)):
        job.status = status
        job.completed_at = datetime.now(UTC) - timedelta(hours=2)
        await broker.update_job(job)

    removed = await broker.purge(older_than=3600, statuses=[JobStatus.failed])

    assert removed == 1
    assert await broker.get_job(failed.id) is None
    assert await broker.get_job(completed.id) is not None


async def test_queue_stats(broker):
    await broker.enqueue(make_job(queue="a"))
    done = make_job(queue="a")
    await broker.enqueue(done)
    done.status = JobStatus.completed
    await broker.update_job(done)

    stats = await broker.queue_stats()
    assert stats["a"] == {"pending": 1, "completed": 1}


async def test_clear(broker):
    await broker.enqueue(make_job())
    await broker.register_worker("w1", ["default"])
    await broker.clear()

    assert await broker.list_jobs() == []
    assert await broker.list_workers() == []
