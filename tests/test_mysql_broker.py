"""
Unit tests for MySQLBroker that work without a live MySQL instance.

All aiomysql calls are mocked so these tests run in any environment,
regardless of whether aiomysql is installed.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest


def _make_aiomysql_mock() -> MagicMock:
    """Return a MagicMock that looks enough like the aiomysql module."""
    mod = MagicMock(name="aiomysql")
    mod.DictCursor = object()
    return mod


def _make_pool_mock(cursor_rows=None):
    """
    Build a mock aiomysql pool whose acquire() context-manager yields a
    connection whose cursor() context-manager yields a cursor.
    """
    cursor_mock = AsyncMock()
    cursor_mock.execute = AsyncMock()
    cursor_mock.fetchone = AsyncMock(return_value=None if cursor_rows is None else cursor_rows[0])
    cursor_mock.fetchall = AsyncMock(return_value=[] if cursor_rows is None else cursor_rows)
    cursor_mock.rowcount = 0

    cursor_ctx = MagicMock()
    cursor_ctx.__aenter__ = AsyncMock(return_value=cursor_mock)
    cursor_ctx.__aexit__ = AsyncMock(return_value=False)

    conn_mock = AsyncMock()
    conn_mock.cursor = MagicMock(return_value=cursor_ctx)
    conn_mock.autocommit = AsyncMock()
    conn_mock.commit = AsyncMock()
    conn_mock.rollback = AsyncMock()

    acquire_ctx = MagicMock()
    acquire_ctx.__aenter__ = AsyncMock(return_value=conn_mock)
    acquire_ctx.__aexit__ = AsyncMock(return_value=False)

    pool_mock = MagicMock()
    pool_mock.acquire = MagicMock(return_value=acquire_ctx)
    pool_mock.close = MagicMock()
    pool_mock.wait_closed = AsyncMock()

    return pool_mock, conn_mock, cursor_mock


@pytest.fixture(autouse=True)
def patch_aiomysql(monkeypatch):
    """Inject a fake aiomysql module so imports always succeed."""
    fake = _make_aiomysql_mock()

    for key in list(sys.modules.keys()):
        if "aioq.backends.mysql" in key:
            del sys.modules[key]

    monkeypatch.setitem(sys.modules, "aiomysql", fake)
    yield fake


@pytest.fixture
def broker_cls(patch_aiomysql):
    """Import MySQLBroker after aiomysql has been patched."""
    from aioq.backends.mysql import MySQLBroker  # noqa: PLC0415

    return MySQLBroker


async def test_instantiation(broker_cls):
    """MySQLBroker can be instantiated with keyword args."""
    broker = broker_cls(
        host="db.example.com",
        port=3307,
        user="alice",
        password="secret",
        db="mydb",
        min_size=1,
        max_size=5,
    )

    assert broker.host == "db.example.com"
    assert broker.port == 3307
    assert broker.user == "alice"
    assert broker.password == "secret"
    assert broker.db == "mydb"
    assert broker.min_size == 1
    assert broker.max_size == 5
    assert broker._pool is None


async def test_connect_calls_create_pool(broker_cls, patch_aiomysql):
    """connect() calls aiomysql.create_pool with the right keyword arguments."""
    pool_mock, _, _ = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls(host="localhost", port=3306, user="root", password="pw", db="aioq")
    await broker.connect()

    patch_aiomysql.create_pool.assert_awaited_once_with(
        host="localhost",
        port=3306,
        user="root",
        password="pw",
        db="aioq",
        minsize=2,
        maxsize=10,
        autocommit=True,
    )
    assert broker._pool is pool_mock


async def test_pool_property_raises_before_connect(broker_cls):
    """Accessing .pool before connect() raises RuntimeError."""
    broker = broker_cls()
    with pytest.raises(RuntimeError, match="connect()"):
        _ = broker.pool


async def test_disconnect_closes_pool(broker_cls, patch_aiomysql):
    """disconnect() closes the pool and sets _pool to None."""
    pool_mock, _, _ = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()

    await broker.disconnect()

    pool_mock.close.assert_called_once()
    pool_mock.wait_closed.assert_awaited_once()
    assert broker._pool is None


async def test_enqueue_executes_insert(broker_cls, patch_aiomysql):
    """enqueue() runs an INSERT ... ON DUPLICATE KEY UPDATE statement."""
    pool_mock, _, cursor_mock = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()

    from aioq.models import Job  # noqa: PLC0415

    job = Job(task_name="tasks.add", queue="default", kwargs={"x": 1})
    await broker.enqueue(job)

    cursor_mock.execute.assert_awaited()
    sql, params = cursor_mock.execute.call_args[0]
    assert "INSERT INTO aioq_jobs" in sql
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert job.id in params


async def test_row_to_job_converts_dict(broker_cls):
    """row_to_job() converts a dict row into a Job object.

    The row deliberately omits the columns added after 0.4 to prove a table
    whose migrations have not run yet still maps cleanly.
    """
    from aioq.backends.mysql import MySQLBroker  # noqa: PLC0415
    from aioq.models import Job, JobStatus  # noqa: PLC0415

    now = datetime.now(UTC)
    row = {
        "id": "abc-123",
        "task_name": "tasks.send_email",
        "queue": "emails",
        "status": "pending",
        "args": json.dumps([1, 2]),
        "kwargs": json.dumps({"to": "a@b.com"}),
        "retries": 0,
        "max_retries": 3,
        "retry_delay": 5.0,
        "enqueued_at": now.replace(tzinfo=None),  # naive, as MySQL returns
        "started_at": None,
        "completed_at": None,
        "run_at": None,
        "result": None,
        "error": None,
        "worker_id": None,
        "save_result": 0,
    }

    job = MySQLBroker.row_to_job(row)

    assert isinstance(job, Job)
    assert job.id == "abc-123"
    assert job.task_name == "tasks.send_email"
    assert job.queue == "emails"
    assert job.status == JobStatus.pending
    assert job.args == [1, 2]
    assert job.kwargs == {"to": "a@b.com"}
    assert job.max_retries == 3
    assert job.retry_delay == 5.0
    assert job.enqueued_at.tzinfo is not None
    assert job.save_result is False
    # Columns missing from the row fall back to the model defaults.
    assert job.timeout is None
    assert job.retry_backoff is False
    assert job.dead_letter_queue is None
    assert job.depends_on == []


async def test_row_to_job_reads_new_columns(broker_cls):
    """row_to_job() picks up the timeout/backoff/DLQ columns when present."""
    from aioq.backends.mysql import MySQLBroker  # noqa: PLC0415

    now = datetime.now(UTC)
    job = MySQLBroker.row_to_job(
        {
            "id": "abc-123",
            "task_name": "tasks.send_email",
            "queue": "emails",
            "status": "dead",
            "args": "[]",
            "kwargs": "{}",
            "retries": 3,
            "max_retries": 3,
            "retry_delay": 1.5,
            "retry_backoff": 1,
            "retry_backoff_max": 120.0,
            "timeout": 30.0,
            "enqueued_at": now.replace(tzinfo=None),
            "started_at": None,
            "completed_at": None,
            "run_at": None,
            "result": None,
            "result_ttl": 60,
            "error": "boom",
            "worker_id": None,
            "priority": 10,
            "save_result": 1,
            "dead_letter_queue": "emails-dlq",
            "depends_on": json.dumps(["dep-1"]),
        }
    )

    assert job.timeout == 30.0
    assert job.retry_backoff is True
    assert job.retry_backoff_max == 120.0
    assert job.result_ttl == 60
    assert job.priority == 10
    assert job.dead_letter_queue == "emails-dlq"
    assert job.depends_on == ["dep-1"]


# ----------------------------------------------------------------------
# Schema migrations
# ----------------------------------------------------------------------


async def test_connect_creates_every_table(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()

    statements = [call[0][0] for call in cursor_mock.execute.call_args_list]
    joined = "\n".join(statements)
    for table in ("aioq_jobs", "aioq_workers", "aioq_cron_locks"):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in joined


async def test_connect_never_uses_create_index_if_not_exists(broker_cls, patch_aiomysql):
    """MySQL rejects that syntax — only MariaDB accepts it."""
    pool_mock, _, cursor_mock = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    await broker_cls().connect()

    statements = "\n".join(call[0][0] for call in cursor_mock.execute.call_args_list)
    assert "CREATE INDEX IF NOT EXISTS" not in statements
    assert "ADD COLUMN IF NOT EXISTS" not in statements


async def test_migrations_probe_information_schema(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    await broker_cls().connect()

    statements = "\n".join(call[0][0] for call in cursor_mock.execute.call_args_list)
    assert "information_schema.columns" in statements
    assert "information_schema.statistics" in statements


async def test_missing_columns_are_added(broker_cls, patch_aiomysql):
    """fetchone() -> None means "column absent", so an ALTER must follow."""
    pool_mock, _, cursor_mock = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    await broker_cls().connect()

    statements = "\n".join(call[0][0] for call in cursor_mock.execute.call_args_list)
    for column in ("dead_letter_queue", "timeout", "retry_backoff", "result_ttl"):
        assert f"ADD COLUMN {column}" in statements


async def test_existing_columns_are_left_alone(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    cursor_mock.fetchone = AsyncMock(return_value=(1,))  # everything already there
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    await broker_cls().connect()

    statements = "\n".join(call[0][0] for call in cursor_mock.execute.call_args_list)
    assert "ADD COLUMN" not in statements
    assert "CREATE INDEX" not in statements


# ----------------------------------------------------------------------
# UTC handling
# ----------------------------------------------------------------------


async def test_timestamp_comparisons_use_utc_not_session_time(broker_cls, patch_aiomysql):
    """Timestamps are stored naive-UTC, so NOW() would compare against the
    session time zone and skew every deferred job."""
    pool_mock, _, cursor_mock = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()
    await broker.dequeue(["default"], timeout=0.01)

    dequeue_sql = [c[0][0] for c in cursor_mock.execute.call_args_list if "FOR UPDATE" in c[0][0]]
    assert dequeue_sql, "expected a claim query"
    assert "UTC_TIMESTAMP" in dequeue_sql[0]
    assert "NOW()" not in dequeue_sql[0]


async def test_aware_timestamps_are_converted_to_utc_before_storage(broker_cls, patch_aiomysql):
    from datetime import timedelta, timezone

    pool_mock, _, cursor_mock = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    from aioq.models import Job  # noqa: PLC0415

    broker = broker_cls()
    await broker.connect()

    jst = timezone(timedelta(hours=9))
    run_at = datetime(2026, 1, 1, 21, 0, tzinfo=jst)
    await broker.enqueue(Job(task_name="t", queue="default", run_at=run_at))

    _, params = cursor_mock.execute.call_args[0]
    stored = [p for p in params if isinstance(p, datetime)]
    # 21:00 JST is 12:00 UTC, stored without tzinfo.
    assert any(p.hour == 12 and p.tzinfo is None for p in stored)


# ----------------------------------------------------------------------
# Claiming
# ----------------------------------------------------------------------


async def test_dequeue_claims_pending_and_retrying(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()
    await broker.dequeue(["default", "high"], timeout=0.01)

    claim = next(c for c in cursor_mock.execute.call_args_list if "FOR UPDATE" in c[0][0])
    sql, params = claim[0]
    assert "SKIP LOCKED" in sql
    assert "ORDER BY priority DESC, enqueued_at" in sql
    assert params == ("default", "high", "pending", "retrying")


async def test_dequeue_rolls_back_when_no_row_is_available(broker_cls, patch_aiomysql):
    pool_mock, conn_mock, _ = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()
    assert await broker.dequeue(["default"], timeout=0.01) is None

    conn_mock.rollback.assert_awaited()
    conn_mock.commit.assert_not_awaited()


# ----------------------------------------------------------------------
# Mutations
# ----------------------------------------------------------------------


async def test_update_job_writes_run_at(broker_cls, patch_aiomysql):
    """Without run_at a deferred retry would become runnable immediately."""
    pool_mock, _, cursor_mock = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    from aioq.models import Job  # noqa: PLC0415

    broker = broker_cls()
    await broker.connect()
    await broker.update_job(Job(task_name="t", queue="default"))

    sql, _ = cursor_mock.execute.call_args[0]
    assert "UPDATE aioq_jobs" in sql
    assert "run_at" in sql


async def test_retry_job_uses_rowcount(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    cursor_mock.rowcount = 1
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()

    assert await broker.retry_job("job-1") is True
    sql, _ = cursor_mock.execute.call_args[0]
    assert "status IN ('failed', 'cancelled')" in sql
    assert "run_at       = NULL" in sql


async def test_retry_job_false_when_nothing_matched(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    cursor_mock.rowcount = 0
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()
    assert await broker.retry_job("job-1") is False


async def test_cancel_job_only_targets_unstarted_jobs(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    cursor_mock.rowcount = 1
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()

    assert await broker.cancel_job("job-1") is True
    sql, _ = cursor_mock.execute.call_args[0]
    assert "status IN ('pending', 'retrying', 'waiting')" in sql


# ----------------------------------------------------------------------
# Querying
# ----------------------------------------------------------------------


async def test_list_jobs_builds_filters(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    from aioq.models import JobStatus  # noqa: PLC0415

    broker = broker_cls()
    await broker.connect()
    await broker.list_jobs(queue="emails", status=JobStatus.failed, limit=5, offset=10)

    sql, params = cursor_mock.execute.call_args[0]
    assert "WHERE queue = %s AND status = %s" in sql
    assert "LIMIT %s OFFSET %s" in sql
    assert params == ["emails", "failed", 5, 10]


async def test_queue_stats_groups_rows(broker_cls, patch_aiomysql):
    rows = [
        {"queue": "default", "status": "pending", "cnt": 2},
        {"queue": "default", "status": "dead", "cnt": 1},
    ]
    pool_mock, _, cursor_mock = _make_pool_mock()
    cursor_mock.fetchall = AsyncMock(return_value=rows)
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()

    assert await broker.queue_stats() == {"default": {"pending": 2, "dead": 1}}


async def test_dependency_lookup_uses_json_contains(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()
    await broker._fetch_waiting_dependents("dep-1")

    sql, params = cursor_mock.execute.call_args[0]
    assert "JSON_CONTAINS(depends_on, %s)" in sql
    assert params == (json.dumps("dep-1"),)


# ----------------------------------------------------------------------
# Cron locks and purge
# ----------------------------------------------------------------------


async def test_cron_lock_won_on_insert(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    cursor_mock.rowcount = 1
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()

    assert await broker.acquire_cron_lock("tick@100", ttl=60) is True
    sql, params = cursor_mock.execute.call_args[0]
    assert "aioq_cron_locks" in sql
    assert "expires_at <= UTC_TIMESTAMP(6)" in sql
    assert params == ("tick@100", 60)


async def test_cron_lock_won_when_stealing_an_expired_lock(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    cursor_mock.rowcount = 2  # MySQL reports 2 for a row it actually changed
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()
    assert await broker.acquire_cron_lock("tick@100", ttl=60) is True


async def test_cron_lock_lost_when_held_by_someone_else(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    cursor_mock.rowcount = 0  # the IF() guards left the row untouched
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()
    assert await broker.acquire_cron_lock("tick@100", ttl=60) is False


async def test_purge_targets_terminal_statuses(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    cursor_mock.rowcount = 4
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()
    removed = await broker.purge(older_than=3600)

    assert removed == 4
    sql, params = cursor_mock.execute.call_args[0]
    assert "DELETE FROM aioq_jobs" in sql
    assert set(params[:4]) == {"completed", "failed", "cancelled", "dead"}
    assert params[-1].tzinfo is None  # naive UTC cutoff


async def test_purge_also_clears_expired_cron_locks(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()
    await broker.purge(older_than=3600)

    statements = "\n".join(c[0][0] for c in cursor_mock.execute.call_args_list)
    assert "DELETE FROM aioq_cron_locks" in statements


# ----------------------------------------------------------------------
# Workers
# ----------------------------------------------------------------------


async def test_worker_heartbeats_use_utc(broker_cls, patch_aiomysql):
    pool_mock, _, cursor_mock = _make_pool_mock()
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()
    await broker.register_worker("w1", ["default"])
    await broker.heartbeat_worker("w1")

    statements = "\n".join(c[0][0] for c in cursor_mock.execute.call_args_list)
    assert "UTC_TIMESTAMP(6)" in statements
    assert "NOW(6)" not in statements


async def test_list_workers_maps_rows(broker_cls, patch_aiomysql):
    now = datetime.now(UTC).replace(tzinfo=None)
    pool_mock, _, cursor_mock = _make_pool_mock()
    cursor_mock.fetchall = AsyncMock(
        return_value=[
            {
                "worker_id": "w1",
                "queues": json.dumps(["default"]),
                "registered_at": now,
                "last_heartbeat": now,
            }
        ]
    )
    patch_aiomysql.create_pool = AsyncMock(return_value=pool_mock)

    broker = broker_cls()
    await broker.connect()
    workers = await broker.list_workers()

    assert workers[0]["worker_id"] == "w1"
    assert workers[0]["queues"] == ["default"]
    assert workers[0]["alive"] is True
