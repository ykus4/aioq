"""CLI tests.

Each command runs against a module-level MemoryBroker app that the CLI loads by
dotted path, exactly as a user would point it at their own tasks module.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from click.testing import CliRunner

from aioq import Aioq, JobStatus
from aioq.backends.memory import MemoryBroker
from aioq.cli import cli

# The CLI resolves "tests.test_cli:app", so these must be module-level.
broker = MemoryBroker()
app = Aioq(broker=broker)
disabled_app = Aioq(broker=MemoryBroker(), dashboard_enabled=False)
not_an_app = "just a string"

APP_PATH = "tests.test_cli:app"


@app.task(save_result=True)
async def sample_task(ctx, value: int = 1):
    return value


@pytest.fixture
def runner():
    asyncio.run(broker.clear())
    return CliRunner()


def enqueue(**kwargs):
    """Enqueue one job outside the CLI and return it."""
    return asyncio.run(_enqueue(**kwargs))


async def _enqueue(status: JobStatus | None = None, **kwargs):
    job = await sample_task.enqueue(**kwargs)
    if status is not None:
        job.status = status
        job.completed_at = datetime.now(UTC)
        await broker.update_job(job)
    return job


# ----------------------------------------------------------------------
# App loading
# ----------------------------------------------------------------------


def test_missing_colon_is_reported(runner):
    result = runner.invoke(cli, ["status", "tests.test_cli"])
    assert result.exit_code != 0
    assert "module:attribute" in result.output


def test_unimportable_module_is_reported(runner):
    result = runner.invoke(cli, ["status", "no.such.module:app"])
    assert result.exit_code != 0
    assert "Could not import" in result.output


def test_missing_attribute_is_reported(runner):
    result = runner.invoke(cli, ["status", "tests.test_cli:nope"])
    assert result.exit_code != 0
    assert "has no attribute" in result.output


def test_wrong_object_type_is_reported(runner):
    result = runner.invoke(cli, ["status", "tests.test_cli:not_an_app"])
    assert result.exit_code != 0
    assert "expected an Aioq instance" in result.output


# ----------------------------------------------------------------------
# status
# ----------------------------------------------------------------------


def test_status_on_empty_broker(runner):
    result = runner.invoke(cli, ["status", APP_PATH])
    assert result.exit_code == 0
    assert "No queues have any jobs yet." in result.output


def test_status_shows_counts_and_workers(runner):
    enqueue()
    enqueue(status=JobStatus.completed)
    asyncio.run(broker.register_worker("worker-1", ["default"]))

    result = runner.invoke(cli, ["status", APP_PATH])

    assert result.exit_code == 0
    assert "default" in result.output
    assert "pending" in result.output
    assert "completed" in result.output
    assert "1 alive / 1 registered" in result.output


# ----------------------------------------------------------------------
# jobs / show
# ----------------------------------------------------------------------


def test_jobs_lists_enqueued_jobs(runner):
    job = enqueue()
    result = runner.invoke(cli, ["jobs", APP_PATH])
    assert result.exit_code == 0
    assert job.id[:8] in result.output


def test_jobs_reports_when_empty(runner):
    result = runner.invoke(cli, ["jobs", APP_PATH])
    assert "No matching jobs." in result.output


def test_jobs_filters_by_status(runner):
    pending = enqueue()
    done = enqueue(status=JobStatus.completed)

    result = runner.invoke(cli, ["jobs", APP_PATH, "--status", "completed"])

    assert done.id[:8] in result.output
    assert pending.id[:8] not in result.output


def test_jobs_rejects_unknown_status(runner):
    result = runner.invoke(cli, ["jobs", APP_PATH, "--status", "banana"])
    assert result.exit_code != 0


def test_jobs_honours_limit(runner):
    for _ in range(5):
        enqueue()
    result = runner.invoke(cli, ["jobs", APP_PATH, "--limit", "2"])
    assert len([line for line in result.output.splitlines() if line.strip()]) == 2


def test_show_prints_job_fields(runner):
    job = enqueue()
    result = runner.invoke(cli, ["show", APP_PATH, job.id])
    assert result.exit_code == 0
    assert job.id in result.output
    assert "task_name:" in result.output


def test_show_unknown_job_errors(runner):
    result = runner.invoke(cli, ["show", APP_PATH, "does-not-exist"])
    assert result.exit_code != 0
    assert "No job with id" in result.output


# ----------------------------------------------------------------------
# retry / cancel
# ----------------------------------------------------------------------


def test_retry_reenqueues_failed_job(runner):
    job = enqueue(status=JobStatus.failed)

    result = runner.invoke(cli, ["retry", APP_PATH, job.id])

    assert result.exit_code == 0
    assert "Re-enqueued" in result.output
    assert asyncio.run(broker.get_job(job.id)).status == JobStatus.pending


def test_retry_replays_dead_job(runner):
    job = enqueue(status=JobStatus.dead)

    result = runner.invoke(cli, ["retry", APP_PATH, job.id])

    assert result.exit_code == 0
    assert "Replayed dead job" in result.output
    assert asyncio.run(broker.get_job(job.id)).status == JobStatus.pending


def test_retry_rejects_completed_job(runner):
    job = enqueue(status=JobStatus.completed)
    result = runner.invoke(cli, ["retry", APP_PATH, job.id])
    assert result.exit_code != 0


def test_cancel_pending_job(runner):
    job = enqueue()

    result = runner.invoke(cli, ["cancel", APP_PATH, job.id])

    assert result.exit_code == 0
    assert asyncio.run(broker.get_job(job.id)).status == JobStatus.cancelled


def test_cancel_running_job_errors(runner):
    job = enqueue(status=JobStatus.running)
    result = runner.invoke(cli, ["cancel", APP_PATH, job.id])
    assert result.exit_code != 0


# ----------------------------------------------------------------------
# purge
# ----------------------------------------------------------------------


def make_old_job(status: JobStatus) -> str:
    async def build():
        job = await sample_task.enqueue()
        job.status = status
        job.completed_at = datetime.now(UTC) - timedelta(days=2)
        await broker.update_job(job)
        return job.id

    return asyncio.run(build())


def test_purge_requires_confirmation(runner):
    job_id = make_old_job(JobStatus.completed)

    result = runner.invoke(cli, ["purge", APP_PATH], input="n\n")

    assert result.exit_code != 0  # aborted
    assert asyncio.run(broker.get_job(job_id)) is not None


def test_purge_with_yes_deletes_old_jobs(runner):
    job_id = make_old_job(JobStatus.completed)
    fresh = enqueue()

    result = runner.invoke(cli, ["purge", APP_PATH, "--yes"])

    assert result.exit_code == 0
    assert "Purged 1 job(s)." in result.output
    assert asyncio.run(broker.get_job(job_id)) is None
    assert asyncio.run(broker.get_job(fresh.id)) is not None


def test_purge_status_filter(runner):
    completed = make_old_job(JobStatus.completed)
    failed = make_old_job(JobStatus.failed)

    result = runner.invoke(cli, ["purge", APP_PATH, "--yes", "--status", "failed"])

    assert result.exit_code == 0
    assert asyncio.run(broker.get_job(failed)) is None
    assert asyncio.run(broker.get_job(completed)) is not None


def test_purge_older_than_protects_recent_jobs(runner):
    job = enqueue(status=JobStatus.completed)

    result = runner.invoke(cli, ["purge", APP_PATH, "--yes", "--older-than", "3600"])

    assert "Purged 0 job(s)." in result.output
    assert asyncio.run(broker.get_job(job.id)) is not None


# ----------------------------------------------------------------------
# dashboard guard
# ----------------------------------------------------------------------


def test_dashboard_refuses_when_disabled(runner):
    result = runner.invoke(cli, ["dashboard", "tests.test_cli:disabled_app"])
    assert result.exit_code != 0
    assert "Dashboard is disabled" in result.output
