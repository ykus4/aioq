from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Collection
from datetime import UTC, datetime
from typing import Any

try:
    import aiomysql
except ImportError as e:  # pragma: no cover - depends on install extras
    raise ImportError("aiomysql is required for MySQL broker: pip install aiomysql") from e

from ..constants import CRON_LOCK_TTL, DEQUEUE_TIMEOUT, SQL_POLL_INTERVAL
from ..models import Job, JobStatus
from .sql import SQLBroker, parse_json_list

# Statuses that dequeue() will claim. `retrying` is included because a retry is
# stored as the same row rescheduled via run_at, not as a new job.
_RUNNABLE = ("pending", "retrying")

# All timestamps are stored naive-UTC, so every comparison must use
# UTC_TIMESTAMP() rather than NOW(), which follows the session time zone.
_CREATE_TABLES = [
    """
    CREATE TABLE IF NOT EXISTS aioq_jobs (
        id VARCHAR(64) PRIMARY KEY,
        task_name VARCHAR(255) NOT NULL,
        queue VARCHAR(191) NOT NULL DEFAULT 'default',
        status VARCHAR(32) NOT NULL DEFAULT 'pending',
        args JSON NOT NULL,
        kwargs JSON NOT NULL,
        retries INT NOT NULL DEFAULT 0,
        max_retries INT NOT NULL DEFAULT 0,
        retry_delay FLOAT NOT NULL DEFAULT 0,
        retry_backoff TINYINT(1) NOT NULL DEFAULT 0,
        retry_backoff_max FLOAT NOT NULL DEFAULT 600,
        timeout FLOAT NULL,
        enqueued_at DATETIME(6) NOT NULL,
        started_at DATETIME(6) NULL,
        completed_at DATETIME(6) NULL,
        run_at DATETIME(6) NULL,
        result JSON NULL,
        result_ttl INT NOT NULL DEFAULT 3600,
        error TEXT NULL,
        worker_id VARCHAR(64) NULL,
        priority INT NOT NULL DEFAULT 0,
        save_result TINYINT(1) NOT NULL DEFAULT 0,
        dead_letter_queue VARCHAR(191) NULL,
        depends_on JSON NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS aioq_workers (
        worker_id VARCHAR(64) PRIMARY KEY,
        queues JSON NOT NULL,
        registered_at DATETIME(6) NOT NULL,
        last_heartbeat DATETIME(6) NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS aioq_cron_locks (
        lock_key VARCHAR(191) PRIMARY KEY,
        acquired_at DATETIME(6) NOT NULL,
        expires_at DATETIME(6) NOT NULL
    )
    """,
]

# MySQL (unlike MariaDB) has no `ADD COLUMN IF NOT EXISTS` / `CREATE INDEX IF
# NOT EXISTS`, so upgrades are applied only after checking information_schema.
_ADD_COLUMNS = [
    ("retry_backoff", "TINYINT(1) NOT NULL DEFAULT 0"),
    ("retry_backoff_max", "FLOAT NOT NULL DEFAULT 600"),
    ("timeout", "FLOAT NULL"),
    ("result_ttl", "INT NOT NULL DEFAULT 3600"),
    ("dead_letter_queue", "VARCHAR(191) NULL"),
]

_CREATE_INDEXES = [
    ("aioq_jobs_queue_status", "aioq_jobs", "(queue, status)"),
    ("aioq_jobs_run_at", "aioq_jobs", "(run_at)"),
    ("aioq_jobs_claim", "aioq_jobs", "(queue, status, priority, enqueued_at)"),
]

_JOB_COLUMNS = """
    id, task_name, queue, status, args, kwargs,
    retries, max_retries, retry_delay, retry_backoff, retry_backoff_max, timeout,
    enqueued_at, run_at, priority, save_result, result_ttl,
    dead_letter_queue, depends_on
"""


async def _scalar(cur) -> Any:
    """Read the single value of a one-column, one-row result."""
    row = await cur.fetchone()
    if row is None:
        return None
    return row[0] if isinstance(row, tuple | list) else row


def _naive(dt: datetime | None) -> datetime | None:
    """Convert an aware datetime to naive UTC for storage."""
    if dt is None:
        return None
    return dt.astimezone(UTC).replace(tzinfo=None) if dt.tzinfo else dt


class MySQLBroker(SQLBroker):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        db: str = "aioq",
        min_size: int = 2,
        max_size: int = 10,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.min_size = min_size
        self.max_size = max_size
        self._pool: aiomysql.Pool | None = None

    @property
    def pool(self) -> aiomysql.Pool:
        if self._pool is None:
            raise RuntimeError("Broker not connected. Call connect() first.")
        return self._pool

    async def connect(self) -> None:
        self._pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db,
            minsize=self.min_size,
            maxsize=self.max_size,
            autocommit=True,
        )
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            for sql in _CREATE_TABLES:
                await cur.execute(sql)
            await self._apply_migrations(cur)

    async def _apply_migrations(self, cur) -> None:
        """Add columns and indexes that a table from an older aioq is missing."""
        for column, ddl in _ADD_COLUMNS:
            await cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = 'aioq_jobs' "
                "AND column_name = %s",
                (column,),
            )
            if not await _scalar(cur):
                await cur.execute(f"ALTER TABLE aioq_jobs ADD COLUMN {column} {ddl}")

        for name, table, columns in _CREATE_INDEXES:
            await cur.execute(
                "SELECT COUNT(*) FROM information_schema.statistics "
                "WHERE table_schema = DATABASE() AND table_name = %s AND index_name = %s",
                (table, name),
            )
            if not await _scalar(cur):
                await cur.execute(f"CREATE INDEX {name} ON {table} {columns}")

    async def disconnect(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    async def enqueue(self, job: Job) -> None:
        await self._check_dependencies(job)
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(*self._upsert_sql(job))
        if job.status == JobStatus.waiting:
            # A dependency may have completed while we were inserting.
            await self._promote_if_ready(job)

    async def enqueue_many(self, jobs: list[Job]) -> None:
        for job in jobs:
            await self._check_dependencies(job)
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            for job in jobs:
                await cur.execute(*self._upsert_sql(job))
        for job in jobs:
            if job.status == JobStatus.waiting:
                await self._promote_if_ready(job)

    @staticmethod
    def _upsert_sql(job: Job) -> tuple[str, tuple]:
        sql = f"""
            INSERT INTO aioq_jobs ({_JOB_COLUMNS})
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                queue = VALUES(queue),
                retries = VALUES(retries),
                run_at = VALUES(run_at),
                priority = VALUES(priority),
                depends_on = VALUES(depends_on)
        """
        params = (
            job.id,
            job.task_name,
            job.queue,
            job.status.value,
            json.dumps(job.args),
            json.dumps(job.kwargs),
            job.retries,
            job.max_retries,
            job.retry_delay,
            int(job.retry_backoff),
            job.retry_backoff_max,
            job.timeout,
            _naive(job.enqueued_at),
            _naive(job.run_at),
            job.priority,
            int(job.save_result),
            job.result_ttl,
            job.dead_letter_queue,
            json.dumps(job.depends_on),
        )
        return sql, params

    async def dequeue(self, queues: list[str], timeout: float = DEQUEUE_TIMEOUT) -> Job | None:
        """Claim one job.

        ``SELECT ... FOR UPDATE SKIP LOCKED`` ensures each job is picked by one
        worker only. MySQL cannot block on a table, so this polls until
        *timeout*.
        """
        queue_slots = ", ".join(["%s"] * len(queues))
        status_slots = ", ".join(["%s"] * len(_RUNNABLE))
        select_sql = f"""
            SELECT id FROM aioq_jobs
            WHERE queue IN ({queue_slots})
              AND status IN ({status_slots})
              AND (run_at IS NULL OR run_at <= UTC_TIMESTAMP(6))
            ORDER BY priority DESC, enqueued_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """
        params = (*queues, *_RUNNABLE)

        deadline = time.monotonic() + timeout
        while True:
            job_row = await self._claim_one(select_sql, params)
            if job_row:
                return self.row_to_job(job_row)
            if time.monotonic() + SQL_POLL_INTERVAL >= deadline:
                return None
            await asyncio.sleep(SQL_POLL_INTERVAL)

    async def _claim_one(self, select_sql: str, params: tuple) -> dict | None:
        """Lock, claim and read back one runnable row inside a transaction."""
        async with self.pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(select_sql, params)
                    row = await cur.fetchone()
                    if row is None:
                        await conn.rollback()
                        return None
                    await cur.execute(
                        "UPDATE aioq_jobs SET status = 'running', "
                        "started_at = UTC_TIMESTAMP(6) WHERE id = %s",
                        (row["id"],),
                    )
                    await cur.execute("SELECT * FROM aioq_jobs WHERE id = %s", (row["id"],))
                    claimed = await cur.fetchone()
                await conn.commit()
                return claimed
            except BaseException:
                await conn.rollback()
                raise

    async def update_job(self, job: Job) -> None:
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE aioq_jobs SET
                    status       = %s,
                    queue        = %s,
                    retries      = %s,
                    started_at   = %s,
                    completed_at = %s,
                    result       = %s,
                    error        = %s,
                    worker_id    = %s,
                    run_at       = %s
                WHERE id = %s
                """,
                (
                    job.status.value,
                    job.queue,
                    job.retries,
                    _naive(job.started_at),
                    _naive(job.completed_at),
                    json.dumps(job.result) if job.result is not None else None,
                    job.error,
                    job.worker_id,
                    _naive(job.run_at),
                    job.id,
                ),
            )

        if job.status == JobStatus.completed:
            await self._resolve_dependents(job.id)

    async def retry_job(self, job_id: str) -> bool:
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE aioq_jobs SET
                    status       = 'pending',
                    retries      = 0,
                    error        = NULL,
                    result       = NULL,
                    started_at   = NULL,
                    completed_at = NULL,
                    worker_id    = NULL,
                    run_at       = NULL
                WHERE id = %s AND status IN ('failed', 'cancelled')
                """,
                (job_id,),
            )
            return cur.rowcount == 1

    async def cancel_job(self, job_id: str) -> bool:
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "UPDATE aioq_jobs SET status = 'cancelled' "
                "WHERE id = %s AND status IN ('pending', 'retrying', 'waiting')",
                (job_id,),
            )
            return cur.rowcount == 1

    async def get_job(self, job_id: str) -> Job | None:
        async with self.pool.acquire() as conn, conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM aioq_jobs WHERE id = %s", (job_id,))
            row = await cur.fetchone()
        return self.row_to_job(row) if row else None

    async def list_jobs(
        self,
        queue: str | None = None,
        status: JobStatus | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        where, params = self.build_filters(queue, status, lambda _: "%s")
        params += [limit, offset]

        async with self.pool.acquire() as conn, conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                f"SELECT * FROM aioq_jobs {where} ORDER BY enqueued_at DESC LIMIT %s OFFSET %s",
                params,
            )
            rows = await cur.fetchall()
        return [self.row_to_job(r) for r in rows]

    # ------------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------------

    async def _fetch_waiting_dependents(self, job_id: str) -> list[tuple[str, list[str]]]:
        async with self.pool.acquire() as conn, conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT id, depends_on FROM aioq_jobs "
                "WHERE status = 'waiting' AND JSON_CONTAINS(depends_on, %s)",
                (json.dumps(job_id),),
            )
            rows = await cur.fetchall()
        return [(row["id"], parse_json_list(row["depends_on"])) for row in rows]

    async def _count_completed(self, dep_ids: list[str]) -> int:
        slots = ", ".join(["%s"] * len(dep_ids))
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                f"SELECT COUNT(*) FROM aioq_jobs WHERE id IN ({slots}) AND status = 'completed'",
                dep_ids,
            )
            (count,) = await cur.fetchone()
        return int(count)

    async def _mark_waiting_as_pending(self, job_id: str) -> None:
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "UPDATE aioq_jobs SET status = 'pending' WHERE id = %s AND status = 'waiting'",
                (job_id,),
            )

    async def _promote_if_ready(self, job: Job) -> None:
        if await self._all_deps_completed(job.depends_on):
            await self._mark_waiting_as_pending(job.id)
            job.status = JobStatus.pending

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    async def queue_stats(self) -> dict[str, dict[str, int]]:
        async with self.pool.acquire() as conn, conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT queue, status, COUNT(*) AS cnt FROM aioq_jobs GROUP BY queue, status"
            )
            rows = await cur.fetchall()
        stats: dict[str, dict[str, int]] = {}
        for row in rows:
            stats.setdefault(row["queue"], {})[row["status"]] = int(row["cnt"])
        return stats

    # ------------------------------------------------------------------
    # Workers
    # ------------------------------------------------------------------

    async def register_worker(self, worker_id: str, queues: list[str]) -> None:
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO aioq_workers (worker_id, queues, registered_at, last_heartbeat)
                VALUES (%s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                ON DUPLICATE KEY UPDATE
                    queues = VALUES(queues),
                    last_heartbeat = UTC_TIMESTAMP(6)
                """,
                (worker_id, json.dumps(queues)),
            )

    async def heartbeat_worker(self, worker_id: str) -> None:
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "UPDATE aioq_workers SET last_heartbeat = UTC_TIMESTAMP(6) WHERE worker_id = %s",
                (worker_id,),
            )

    async def deregister_worker(self, worker_id: str) -> None:
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute("DELETE FROM aioq_workers WHERE worker_id = %s", (worker_id,))

    async def list_workers(self) -> list[dict]:
        now = datetime.now(UTC).timestamp()
        async with self.pool.acquire() as conn, conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM aioq_workers")
            rows = await cur.fetchall()
        return [self.worker_row_to_dict(row, now) for row in rows]

    # ------------------------------------------------------------------
    # Cron coordination
    # ------------------------------------------------------------------

    async def acquire_cron_lock(self, key: str, ttl: float = CRON_LOCK_TTL) -> bool:
        # MySQL cannot put a WHERE on ON DUPLICATE KEY UPDATE, so the guard goes
        # in the assignments: an unexpired lock keeps its values, leaving
        # rowcount at 0, which tells us somebody else holds it.
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO aioq_cron_locks (lock_key, acquired_at, expires_at)
                VALUES (%s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6) + INTERVAL %s SECOND)
                ON DUPLICATE KEY UPDATE
                    acquired_at = IF(expires_at <= UTC_TIMESTAMP(6),
                                     UTC_TIMESTAMP(6), acquired_at),
                    expires_at  = IF(expires_at <= UTC_TIMESTAMP(6),
                                     VALUES(expires_at), expires_at)
                """,
                (key, int(ttl)),
            )
            return cur.rowcount in (1, 2)

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    async def purge(
        self,
        older_than: float,
        statuses: Collection[JobStatus] | None = None,
    ) -> int:
        cutoff = _naive(self._purge_cutoff(older_than))
        status_values = self._purge_statuses(statuses)
        slots = ", ".join(["%s"] * len(status_values))

        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM aioq_cron_locks WHERE expires_at <= UTC_TIMESTAMP(6)",
            )
            await cur.execute(
                f"DELETE FROM aioq_jobs WHERE status IN ({slots}) "
                "AND COALESCE(completed_at, enqueued_at) <= %s",
                (*status_values, cutoff),
            )
            return cur.rowcount
