from aioq.models import Job, JobStatus


def test_job_defaults():
    job = Job(task_name="my_task", queue="default")
    assert job.status == JobStatus.pending
    assert job.retries == 0
    assert job.id is not None


def test_job_serialization():
    job = Job(task_name="my_task", queue="default", kwargs={"x": 1})
    d = job.model_dump_json_safe()
    assert d["task_name"] == "my_task"
    assert isinstance(d["enqueued_at"], str)
    assert d["kwargs"] == {"x": 1}
