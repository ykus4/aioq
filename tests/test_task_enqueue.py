"""TaskDef.enqueue / enqueue_many semantics."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from aioq import Aioq
from aioq.backends.memory import MemoryBroker
from aioq.models import JobStatus


@pytest.fixture
async def app():
    broker = MemoryBroker()
    async with broker:
        yield Aioq(broker=broker)


@pytest.fixture
def task(app):
    @app.task(queue="work", retries=2, retry_delay=1.5, timeout=7, priority=5)
    async def sample(ctx, a=None, b=None):
        return a, b

    return sample


# ----------------------------------------------------------------------
# enqueue
# ----------------------------------------------------------------------


async def test_enqueue_copies_task_config_onto_the_job(task):
    job = await task.enqueue(1, b=2)

    assert job.queue == "work"
    assert job.args == [1]
    assert job.kwargs == {"b": 2}
    assert job.max_retries == 2
    assert job.retry_delay == 1.5
    assert job.timeout == 7
    assert job.priority == 5
    assert job.status == JobStatus.pending


async def test_enqueue_persists_the_job(app, task):
    job = await task.enqueue()
    assert await app.broker.get_job(job.id) is not None


async def test_priority_can_be_overridden_per_call(task):
    assert (await task.enqueue(priority=10)).priority == 10
    assert (await task.enqueue()).priority == 5


async def test_defer_by_sets_a_future_run_at(task):
    before = datetime.now(UTC)
    job = await task.enqueue(defer_by=60)

    assert job.run_at is not None
    assert before + timedelta(seconds=59) <= job.run_at <= before + timedelta(seconds=61)


async def test_defer_until_is_used_verbatim(task):
    when = datetime.now(UTC) + timedelta(hours=3)
    assert (await task.enqueue(defer_until=when)).run_at == when


async def test_no_defer_means_no_run_at(task):
    assert (await task.enqueue()).run_at is None


async def test_defer_by_and_defer_until_together_is_an_error(task):
    with pytest.raises(ValueError, match="not both"):
        await task.enqueue(defer_by=1, defer_until=datetime.now(UTC))


async def test_depends_on_marks_the_job_waiting(app, task):
    parent = await task.enqueue()
    child = await task.enqueue(depends_on=[parent.id])

    assert (await app.broker.get_job(child.id)).status == JobStatus.waiting


# ----------------------------------------------------------------------
# enqueue_many
# ----------------------------------------------------------------------


async def test_enqueue_many_with_kwargs_dicts(task):
    jobs = await task.enqueue_many([{"a": 1}, {"a": 2}])

    assert [j.kwargs for j in jobs] == [{"a": 1}, {"a": 2}]
    assert all(j.args == [] for j in jobs)


async def test_enqueue_many_with_positional_tuples(task):
    jobs = await task.enqueue_many([(1, 2), (3, 4)])

    assert [j.args for j in jobs] == [[1, 2], [3, 4]]
    assert all(j.kwargs == {} for j in jobs)


async def test_enqueue_many_mixed_items(task):
    jobs = await task.enqueue_many([(1,), {"b": 2}])

    assert jobs[0].args == [1]
    assert jobs[1].kwargs == {"b": 2}


async def test_enqueue_many_persists_every_job(app, task):
    jobs = await task.enqueue_many([{} for _ in range(4)])

    for job in jobs:
        assert await app.broker.get_job(job.id) is not None


async def test_enqueue_many_applies_defer_and_priority(task):
    jobs = await task.enqueue_many([{}, {}], defer_by=30, priority=10)

    assert all(j.run_at is not None for j in jobs)
    assert all(j.priority == 10 for j in jobs)


async def test_enqueue_many_supports_defer_until(task):
    when = datetime.now(UTC) + timedelta(minutes=5)
    jobs = await task.enqueue_many([{}], defer_until=when)
    assert jobs[0].run_at == when


async def test_enqueue_many_supports_dependencies(app, task):
    parent = await task.enqueue()
    jobs = await task.enqueue_many([{}, {}], depends_on=[parent.id])

    for job in jobs:
        assert (await app.broker.get_job(job.id)).status == JobStatus.waiting


async def test_enqueue_many_with_no_items(task):
    assert await task.enqueue_many([]) == []


async def test_jobs_get_unique_ids(task):
    jobs = await task.enqueue_many([{} for _ in range(10)])
    assert len({j.id for j in jobs}) == 10


# ----------------------------------------------------------------------
# Direct invocation
# ----------------------------------------------------------------------


async def test_task_object_is_still_directly_callable(task):
    """The decorator returns a TaskDef, but calling it runs the function."""
    assert await task({}, 1, b=2) == (1, 2)
