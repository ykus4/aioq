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
    from src.aioq.backends.mysql import MySQLBroker  # noqa: PLC0415

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

    from src.aioq.models import Job  # noqa: PLC0415

    job = Job(task_name="tasks.add", queue="default", kwargs={"x": 1})
    await broker.enqueue(job)

    cursor_mock.execute.assert_awaited()
    sql, params = cursor_mock.execute.call_args[0]
    assert "INSERT INTO aioq_jobs" in sql
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert job.id in params


async def test_row_to_job_converts_dict(broker_cls):
    """_row_to_job() correctly converts a dict row into a Job object."""
    from src.aioq.backends.mysql import MySQLBroker  # noqa: PLC0415
    from src.aioq.models import Job, JobStatus  # noqa: PLC0415

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

    job = MySQLBroker._row_to_job(row)

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
