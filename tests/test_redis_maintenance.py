"""Redis broker: result TTLs, purging, cron locks and stats edge cases."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta

from aioq.backends.redis import _ALL_JOBS_KEY, _job_key, _status_set
from aioq.models import Job, JobStatus


def make_job(**kwargs) -> Job:
    kwargs.setdefault("task_name", "tasks.add")
    kwargs.setdefault("queue", "default")
    return Job(**kwargs)


async def complete(broker, job: Job, *, finished_at: datetime | None = None) -> None:
    job.status = JobStatus.completed
    job.completed_at = finished_at or datetime.now(UTC)
    await broker.update_job(job)


# ----------------------------------------------------------------------
# result_ttl
# ----------------------------------------------------------------------


async def test_saved_result_gets_a_ttl(broker):
    job = make_job(save_result=True, result_ttl=120)
    await broker.enqueue(job)
    assert await broker.redis.ttl(_job_key(job.id)) == -1  # no TTL while running

    job.result = 42
    await complete(broker, job)

    assert 0 < await broker.redis.ttl(_job_key(job.id)) <= 120


async def test_result_ttl_zero_keeps_the_record_forever(broker):
    job = make_job(save_result=True, result_ttl=0)
    await broker.enqueue(job)
    await complete(broker, job)

    assert await broker.redis.ttl(_job_key(job.id)) == -1


async def test_jobs_without_saved_results_are_not_expired(broker):
    job = make_job(save_result=False)
    await broker.enqueue(job)
    await complete(broker, job)

    assert await broker.redis.ttl(_job_key(job.id)) == -1


async def test_expired_result_is_reaped_from_the_indexes(broker):
    """Once the record expires, its id must not linger in the index sets."""
    job = make_job(save_result=True, result_ttl=1)
    await broker.enqueue(job)
    await complete(broker, job)

    # Simulate the TTL elapsing: drop the record, then reap as of the future.
    await broker.redis.delete(_job_key(job.id))
    assert await broker.redis.sismember(_ALL_JOBS_KEY, job.id) == 1

    await broker._reap_expired(now=time.time() + 60)

    assert await broker.redis.sismember(_ALL_JOBS_KEY, job.id) == 0
    assert await broker.redis.sismember(_status_set(JobStatus.completed), job.id) == 0
    assert await broker.queue_stats() == {}


async def test_reaping_is_a_noop_when_nothing_expired(broker):
    job = make_job(save_result=True, result_ttl=3600)
    await broker.enqueue(job)
    await complete(broker, job)

    await broker._reap_expired()

    assert await broker.get_job(job.id) is not None


# ----------------------------------------------------------------------
# purge
# ----------------------------------------------------------------------


async def test_purge_deletes_old_finished_jobs(broker):
    old = make_job()
    await broker.enqueue(old)
    await complete(broker, old, finished_at=datetime.now(UTC) - timedelta(days=2))

    removed = await broker.purge(older_than=3600)

    assert removed == 1
    assert await broker.get_job(old.id) is None
    assert await broker.queue_stats() == {}


async def test_purge_keeps_recent_and_unfinished_jobs(broker):
    recent = make_job()
    pending = make_job()
    await broker.enqueue(recent)
    await broker.enqueue(pending)
    await complete(broker, recent)

    removed = await broker.purge(older_than=3600)

    assert removed == 0
    assert await broker.get_job(recent.id) is not None
    assert await broker.get_job(pending.id) is not None


async def test_purge_respects_status_filter(broker):
    failed = make_job()
    completed = make_job()
    await broker.enqueue(failed)
    await broker.enqueue(completed)

    long_ago = datetime.now(UTC) - timedelta(days=2)
    failed.status = JobStatus.failed
    failed.completed_at = long_ago
    await broker.update_job(failed)
    await complete(broker, completed, finished_at=long_ago)

    removed = await broker.purge(older_than=3600, statuses=[JobStatus.failed])

    assert removed == 1
    assert await broker.get_job(failed.id) is None
    assert await broker.get_job(completed.id) is not None


async def test_purge_removes_dependents_key(broker):
    dep = make_job()
    await broker.enqueue(dep)
    await broker.enqueue(make_job(depends_on=[dep.id]))
    await complete(broker, dep, finished_at=datetime.now(UTC) - timedelta(days=2))

    await broker.purge(older_than=3600)

    assert await broker.redis.exists(f"aioq:job:{dep.id}:dependents") == 0


# ----------------------------------------------------------------------
# Cron locks
# ----------------------------------------------------------------------


async def test_cron_lock_is_exclusive(broker):
    assert await broker.acquire_cron_lock("tick@100", ttl=60) is True
    assert await broker.acquire_cron_lock("tick@100", ttl=60) is False
    assert await broker.acquire_cron_lock("tick@160", ttl=60) is True


# ----------------------------------------------------------------------
# Stats
# ----------------------------------------------------------------------


async def test_queue_stats_counts_by_status(broker):
    await broker.enqueue(make_job(queue="a"))
    done = make_job(queue="a")
    await broker.enqueue(done)
    await complete(broker, done)

    assert await broker.queue_stats() == {"a": {"pending": 1, "completed": 1}}


async def test_queue_stats_handles_colons_in_queue_names(broker):
    """Queue names are opaque strings — splitting the key on ':' truncated them."""
    await broker.enqueue(make_job(queue="tenant:42:emails"))

    stats = await broker.queue_stats()

    assert stats == {"tenant:42:emails": {"pending": 1}}


async def test_dequeue_drops_a_job_whose_record_vanished(broker):
    job = make_job()
    await broker.enqueue(job)
    await broker.redis.delete(_job_key(job.id))

    assert await broker.dequeue(["default"], timeout=0.2) is None
    # The dangling index entries are cleaned up rather than left behind.
    assert await broker.redis.sismember(_ALL_JOBS_KEY, job.id) == 0


# ----------------------------------------------------------------------
# Workers
# ----------------------------------------------------------------------


async def test_registering_a_worker_does_not_expire_the_others(broker):
    """The workers hash must never carry a TTL — that dropped every worker."""
    await broker.register_worker("w1", ["default"])
    await broker.register_worker("w2", ["default"])

    assert await broker.redis.ttl("aioq:workers") == -1
    assert len(await broker.list_workers()) == 2


async def test_long_dead_workers_are_reaped(broker):
    stale = {
        "worker_id": "zombie",
        "queues": ["default"],
        "registered_at": (datetime.now(UTC) - timedelta(days=1)).isoformat(),
        "last_heartbeat": (datetime.now(UTC) - timedelta(days=1)).isoformat(),
    }
    await broker.redis.hset("aioq:workers", "zombie", json.dumps(stale))
    await broker.register_worker("healthy", ["default"])

    workers = await broker.list_workers()

    assert [w["worker_id"] for w in workers] == ["healthy"]
    assert await broker.redis.hexists("aioq:workers", "zombie") == 0


async def test_recently_missed_heartbeat_marks_worker_not_alive(broker):
    info = {
        "worker_id": "w1",
        "queues": ["default"],
        "registered_at": datetime.now(UTC).isoformat(),
        "last_heartbeat": (datetime.now(UTC) - timedelta(seconds=60)).isoformat(),
    }
    await broker.redis.hset("aioq:workers", "w1", json.dumps(info))

    workers = await broker.list_workers()

    assert len(workers) == 1
    assert workers[0]["alive"] is False
