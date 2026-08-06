"""Deferred-job promotion in the Redis broker.

These exercise the real Lua promotion script (fakeredis is installed with the
``[lua]`` extra), including which priority tier a job lands back in.

Jobs are enqueued with a *future* ``run_at`` so they genuinely go into the
deferred sorted set — a past ``run_at`` is pushed straight onto the pending list
and would never reach the script. Promotion is then driven with an explicit
timestamp instead of a sleep.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from aioq.backends.redis import _deferred_key, _pending_key
from aioq.models import Job, JobStatus


def make_job(**kwargs) -> Job:
    kwargs.setdefault("task_name", "tasks.add")
    kwargs.setdefault("queue", "default")
    return Job(**kwargs)


def in_(seconds: float) -> datetime:
    return datetime.now(UTC) + timedelta(seconds=seconds)


async def test_future_job_is_not_dequeued(broker):
    await broker.enqueue(make_job(run_at=in_(3600)))

    assert await broker.dequeue(["default"], timeout=0.1) is None
    assert await broker.redis.zcard(_deferred_key("default", 0)) == 1


async def test_due_job_is_promoted_and_dequeued(broker):
    job = make_job(run_at=in_(0.1))
    await broker.enqueue(job)
    assert await broker.redis.zcard(_deferred_key("default", 0)) == 1

    await asyncio.sleep(0.15)
    dequeued = await broker.dequeue(["default"], timeout=0.5)

    assert dequeued is not None
    assert dequeued.id == job.id
    assert await broker.redis.zcard(_deferred_key("default", 0)) == 0


async def test_promotion_preserves_priority_tier(broker):
    """A deferred high-priority job must not come back as priority 0."""
    run_at = in_(300)
    await broker.enqueue(make_job(priority=10, run_at=run_at))

    assert await broker.redis.zcard(_deferred_key("default", 10)) == 1

    await broker._promote_deferred(["default"], run_at.timestamp() + 1)

    assert await broker.redis.llen(_pending_key("default", 10)) == 1
    assert await broker.redis.llen(_pending_key("default", 0)) == 0


async def test_deferred_urgent_job_beats_pending_normal_job(broker):
    normal = make_job(priority=0)
    urgent = make_job(priority=10, run_at=in_(0.1))
    await broker.enqueue(normal)
    await broker.enqueue(urgent)

    await asyncio.sleep(0.15)
    first = await broker.dequeue(["default"], timeout=0.5)

    assert first.id == urgent.id


async def test_promotion_only_moves_due_jobs(broker):
    due = make_job(run_at=in_(60))
    later = make_job(run_at=in_(3600))
    await broker.enqueue(due)
    await broker.enqueue(later)
    assert await broker.redis.zcard(_deferred_key("default", 0)) == 2

    await broker._promote_deferred(["default"], due.run_at.timestamp() + 1)

    assert await broker.redis.llen(_pending_key("default", 0)) == 1
    assert await broker.redis.zcard(_deferred_key("default", 0)) == 1


async def test_promotion_handles_many_jobs(broker):
    """The Lua script chunks its unpack() calls; make sure a big batch works."""
    run_at = in_(60)
    await broker.enqueue_many([make_job(run_at=run_at) for _ in range(1200)])
    assert await broker.redis.zcard(_deferred_key("default", 0)) == 1200

    await broker._promote_deferred(["default"], run_at.timestamp() + 1)

    assert await broker.redis.llen(_pending_key("default", 0)) == 1200
    assert await broker.redis.zcard(_deferred_key("default", 0)) == 0


async def test_promotion_spans_queues_and_tiers(broker):
    run_at = in_(60)
    await broker.enqueue(make_job(queue="a", run_at=run_at))
    await broker.enqueue(make_job(queue="b", run_at=run_at, priority=5))

    await broker._promote_deferred(["a", "b"], run_at.timestamp() + 1)

    assert await broker.redis.llen(_pending_key("a", 0)) == 1
    assert await broker.redis.llen(_pending_key("b", 5)) == 1


async def test_promotion_on_empty_queues_is_a_noop(broker):
    await broker._promote_deferred(["nothing-here"], datetime.now(UTC).timestamp())
    assert await broker.dequeue(["nothing-here"], timeout=0.05) is None


async def test_retry_reschedules_into_deferred_set(broker):
    """A retry with a delay is parked in the deferred set, not run immediately."""
    job = make_job(max_retries=1, retry_delay=30)
    await broker.enqueue(job)
    await broker.dequeue(["default"], timeout=0.5)

    job.retries = 1
    job.status = JobStatus.retrying
    job.run_at = in_(30)
    await broker.enqueue(job)

    assert await broker.redis.zcard(_deferred_key("default", 0)) == 1
    assert await broker.dequeue(["default"], timeout=0.1) is None


async def test_requeued_job_is_indexed_under_one_status_only(broker):
    """Re-enqueueing must not leave the job counted under its old status."""
    job = make_job(max_retries=1, retry_delay=0)
    await broker.enqueue(job)
    claimed = await broker.dequeue(["default"], timeout=0.5)

    claimed.status = JobStatus.running
    await broker.update_job(claimed)
    claimed.status = JobStatus.retrying
    claimed.retries = 1
    await broker.enqueue(claimed)

    stats = await broker.queue_stats()
    assert stats["default"] == {"retrying": 1}
