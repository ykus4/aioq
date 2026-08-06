"""Unit tests for PostgresBroker with asyncpg mocked out.

No live PostgreSQL is needed: the pool is a mock, so these assert the SQL and
parameter shapes the broker sends rather than the database's behaviour.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from aioq.backends.postgres import PostgresBroker
from aioq.models import Job, JobStatus


def make_pool(*, fetchrow=None, fetch=None, execute="UPDATE 1", fetchval=0):
    """Build a mock asyncpg pool whose acquire() yields a mock connection."""
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=execute)
    conn.fetchrow = AsyncMock(return_value=fetchrow)
    conn.fetch = AsyncMock(return_value=fetch or [])
    conn.fetchval = AsyncMock(return_value=fetchval)

    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock(return_value=None)
    transaction.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=transaction)

    acquire = MagicMock()
    acquire.__aenter__ = AsyncMock(return_value=conn)
    acquire.__aexit__ = AsyncMock(return_value=False)

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acquire)
    pool.close = AsyncMock()
    return pool, conn


@pytest.fixture
def broker():
    b = PostgresBroker(dsn="postgresql://localhost/aioq")
    pool, conn = make_pool()
    b._pool = pool
    b.mock_conn = conn  # convenience handle for assertions
    return b


def make_job(**kwargs) -> Job:
    kwargs.setdefault("task_name", "tasks.add")
    kwargs.setdefault("queue", "default")
    return Job(**kwargs)


def full_row(**overrides) -> dict:
    now = datetime.now(UTC)
    row = {
        "id": "job-1",
        "task_name": "tasks.send_email",
        "queue": "emails",
        "status": "pending",
        "args": "[1, 2]",
        "kwargs": '{"to": "a@b.com"}',
        "retries": 1,
        "max_retries": 3,
        "retry_delay": 5.0,
        "retry_backoff": True,
        "retry_backoff_max": 120.0,
        "timeout": 30.0,
        "enqueued_at": now,
        "started_at": None,
        "completed_at": None,
        "run_at": None,
        "result": None,
        "result_ttl": 60,
        "error": None,
        "worker_id": None,
        "priority": 10,
        "save_result": True,
        "dead_letter_queue": "emails-dlq",
        "depends_on": '["dep-1"]',
    }
    row.update(overrides)
    return row


# ----------------------------------------------------------------------
# Connection
# ----------------------------------------------------------------------


def test_pool_property_raises_before_connect():
    with pytest.raises(RuntimeError, match="connect()"):
        _ = PostgresBroker(dsn="postgresql://localhost/aioq").pool


async def test_disconnect_closes_the_pool(broker):
    pool = broker._pool
    await broker.disconnect()
    pool.close.assert_awaited_once()
    assert broker._pool is None


# ----------------------------------------------------------------------
# enqueue
# ----------------------------------------------------------------------


async def test_enqueue_upserts_with_every_column(broker):
    job = make_job(
        timeout=30,
        retry_backoff=True,
        dead_letter_queue="dlq",
        save_result=True,
        result_ttl=60,
    )
    await broker.enqueue(job)

    sql, *params = broker.mock_conn.execute.call_args[0]
    assert "INSERT INTO aioq_jobs" in sql
    assert "ON CONFLICT (id) DO UPDATE" in sql
    # The columns that used to be silently dropped are now persisted.
    for column in ("dead_letter_queue", "timeout", "retry_backoff", "result_ttl"):
        assert column in sql
    assert "dlq" in params
    assert 30.0 in params or 30 in params


async def test_enqueue_placeholder_count_matches_columns(broker):
    await broker.enqueue(make_job())

    sql, *params = broker.mock_conn.execute.call_args[0]
    columns = sql.split("INSERT INTO aioq_jobs (")[1].split(")")[0]
    assert len(columns.split(",")) == len(params)
    assert f"${len(params)}" in sql
    assert f"${len(params) + 1}" not in sql


async def test_enqueue_serialises_json_columns(broker):
    await broker.enqueue(make_job(args=[1], kwargs={"a": 2}, depends_on=[]))

    _, *params = broker.mock_conn.execute.call_args[0]
    assert "[1]" in params
    assert json.dumps({"a": 2}) in params


# ----------------------------------------------------------------------
# dequeue
# ----------------------------------------------------------------------


async def test_dequeue_claims_pending_and_retrying(broker):
    broker.mock_conn.fetchrow = AsyncMock(return_value=full_row())

    job = await broker.dequeue(["default"], timeout=1.0)

    sql, queues, statuses = broker.mock_conn.fetchrow.call_args[0]
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "ORDER BY priority DESC, enqueued_at" in sql
    # A retry is the same row rescheduled, so it must be claimable too.
    assert set(statuses) == {"pending", "retrying"}
    assert queues == ["default"]
    assert job.id == "job-1"


async def test_dequeue_returns_none_when_nothing_is_ready(broker):
    broker.mock_conn.fetchrow = AsyncMock(return_value=None)
    assert await broker.dequeue(["default"], timeout=0.01) is None


# ----------------------------------------------------------------------
# update / retry / cancel
# ----------------------------------------------------------------------


async def test_update_job_persists_run_at(broker):
    """run_at must be written, or a deferred retry would run immediately."""
    job = make_job(run_at=datetime.now(UTC))
    await broker.update_job(job)

    sql, *_ = broker.mock_conn.execute.call_args[0]
    assert "UPDATE aioq_jobs" in sql
    assert "run_at" in sql


async def test_completed_job_triggers_dependency_resolution(broker):
    broker.mock_conn.fetch = AsyncMock(return_value=[])
    job = make_job(status=JobStatus.completed)

    await broker.update_job(job)

    # The dependents lookup runs in addition to the UPDATE.
    assert broker.mock_conn.fetch.await_count == 1
    assert "status = 'waiting'" in broker.mock_conn.fetch.call_args[0][0]


async def test_retry_job_reports_success_from_rowcount(broker):
    broker.mock_conn.execute = AsyncMock(return_value="UPDATE 1")
    assert await broker.retry_job("job-1") is True

    sql, _ = broker.mock_conn.execute.call_args[0]
    assert "status IN ('failed', 'cancelled')" in sql
    assert "run_at       = NULL" in sql


async def test_retry_job_reports_failure_when_no_row_matched(broker):
    broker.mock_conn.execute = AsyncMock(return_value="UPDATE 0")
    assert await broker.retry_job("job-1") is False


async def test_cancel_job_only_targets_unstarted_jobs(broker):
    broker.mock_conn.execute = AsyncMock(return_value="UPDATE 1")
    assert await broker.cancel_job("job-1") is True

    sql, _ = broker.mock_conn.execute.call_args[0]
    assert "status IN ('pending', 'retrying', 'waiting')" in sql


# ----------------------------------------------------------------------
# Querying
# ----------------------------------------------------------------------


async def test_list_jobs_without_filters(broker):
    await broker.list_jobs()
    sql, *params = broker.mock_conn.fetch.call_args[0]
    assert "WHERE" not in sql
    assert params == [100, 0]


async def test_list_jobs_numbers_placeholders_in_order(broker):
    await broker.list_jobs(queue="emails", status=JobStatus.failed, limit=5, offset=10)

    sql, *params = broker.mock_conn.fetch.call_args[0]
    assert "queue = $1" in sql
    assert "status = $2" in sql
    assert "LIMIT $3 OFFSET $4" in sql
    assert params == ["emails", "failed", 5, 10]


async def test_queue_stats_groups_by_queue_and_status(broker):
    broker.mock_conn.fetch = AsyncMock(
        return_value=[
            {"queue": "default", "status": "pending", "cnt": 2},
            {"queue": "default", "status": "failed", "cnt": 1},
            {"queue": "emails", "status": "pending", "cnt": 5},
        ]
    )

    assert await broker.queue_stats() == {
        "default": {"pending": 2, "failed": 1},
        "emails": {"pending": 5},
    }


# ----------------------------------------------------------------------
# Row mapping
# ----------------------------------------------------------------------


def test_row_to_job_maps_every_column():
    job = PostgresBroker.row_to_job(full_row())

    assert job.id == "job-1"
    assert job.args == [1, 2]
    assert job.kwargs == {"to": "a@b.com"}
    assert job.status == JobStatus.pending
    assert job.priority == 10
    assert job.timeout == 30.0
    assert job.retry_backoff is True
    assert job.retry_backoff_max == 120.0
    assert job.result_ttl == 60
    assert job.dead_letter_queue == "emails-dlq"
    assert job.depends_on == ["dep-1"]
    assert job.enqueued_at.tzinfo is not None


def test_row_to_job_preserves_falsy_results():
    """A stored result of 0 or False must not come back as None."""
    assert PostgresBroker.row_to_job(full_row(result="0")).result == 0
    assert PostgresBroker.row_to_job(full_row(result="false")).result is False
    assert PostgresBroker.row_to_job(full_row(result='""')).result == ""
    assert PostgresBroker.row_to_job(full_row(result=None)).result is None


# ----------------------------------------------------------------------
# Workers, cron locks, purge
# ----------------------------------------------------------------------


async def test_list_workers_computes_liveness(broker):
    now = datetime.now(UTC)
    broker.mock_conn.fetch = AsyncMock(
        return_value=[
            {
                "worker_id": "fresh",
                "queues": '["default"]',
                "registered_at": now,
                "last_heartbeat": now,
            },
            {
                "worker_id": "stale",
                "queues": '["default"]',
                "registered_at": now,
                "last_heartbeat": now.replace(year=now.year - 1),
            },
        ]
    )

    workers = await broker.list_workers()

    assert [w["worker_id"] for w in workers] == ["fresh", "stale"]
    assert workers[0]["alive"] is True
    assert workers[1]["alive"] is False
    assert workers[0]["queues"] == ["default"]


async def test_cron_lock_reports_the_insert_result(broker):
    broker.mock_conn.execute = AsyncMock(return_value="INSERT 0 1")
    assert await broker.acquire_cron_lock("tick@100", ttl=60) is True

    sql, key, ttl = broker.mock_conn.execute.call_args[0]
    assert "aioq_cron_locks" in sql
    assert "expires_at <= now()" in sql  # only an expired lock can be stolen
    assert key == "tick@100"
    assert ttl == "60"


async def test_cron_lock_lost_when_no_row_written(broker):
    broker.mock_conn.execute = AsyncMock(return_value="INSERT 0 0")
    assert await broker.acquire_cron_lock("tick@100", ttl=60) is False


async def test_purge_deletes_terminal_statuses_by_default(broker):
    broker.mock_conn.execute = AsyncMock(return_value="DELETE 7")

    removed = await broker.purge(older_than=3600)

    assert removed == 7
    sql, statuses, cutoff = broker.mock_conn.execute.call_args[0]
    assert "DELETE FROM aioq_jobs" in sql
    assert set(statuses) == {"completed", "failed", "cancelled", "dead"}
    assert cutoff < datetime.now(UTC)


async def test_purge_honours_a_status_filter(broker):
    broker.mock_conn.execute = AsyncMock(return_value="DELETE 1")

    await broker.purge(older_than=60, statuses=[JobStatus.dead])

    _, statuses, _ = broker.mock_conn.execute.call_args[0]
    assert statuses == ["dead"]
