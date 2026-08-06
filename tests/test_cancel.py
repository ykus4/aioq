from aioq.models import Job, JobStatus


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
