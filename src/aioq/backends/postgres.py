from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Collection
from datetime import UTC, datetime

try:
    import asyncpg
except ImportError as e:  # pragma: no cover - depends on install extras
    raise ImportError("asyncpg is required for PostgreSQL broker: pip install asyncpg") from e

from ..constants import CRON_LOCK_TTL, DEQUEUE_TIMEOUT, SQL_POLL_INTERVAL
from ..models import Job, JobStatus
from .sql import SQLBroker

# Statuses that dequeue() will claim. `retrying` is included because a retry is
# stored as the same row rescheduled via run_at, not as a new job.
_RUNNABLE = ("pending", "retrying")

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS aioq_jobs (
    id                TEXT PRIMARY KEY,
    task_name         TEXT NOT NULL,
    queue             TEXT NOT NULL DEFAULT 'default',
    status            TEXT NOT NULL DEFAULT 'pending',
    args              JSONB NOT NULL DEFAULT '[]',
    kwargs            JSONB NOT NULL DEFAULT '{}',
    retries           INT NOT NULL DEFAULT 0,
    max_retries       INT NOT NULL DEFAULT 0,
    retry_delay       FLOAT NOT NULL DEFAULT 0,
    retry_backoff     BOOLEAN NOT NULL DEFAULT FALSE,
    retry_backoff_max FLOAT NOT NULL DEFAULT 600,
    timeout           FLOAT,
    enqueued_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at        TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    run_at            TIMESTAMPTZ,
    result            JSONB,
    result_ttl        INT NOT NULL DEFAULT 3600,
    error             TEXT,
    worker_id         TEXT,
    priority          INT NOT NULL DEFAULT 0,
    save_result       BOOLEAN NOT NULL DEFAULT FALSE,
    dead_letter_queue TEXT,
    depends_on        JSONB NOT NULL DEFAULT '[]'
);

-- Idempotent upgrades for tables created by an older aioq.
ALTER TABLE aioq_jobs ADD COLUMN IF NOT EXISTS retry_backoff BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE aioq_jobs ADD COLUMN IF NOT EXISTS retry_backoff_max FLOAT NOT NULL DEFAULT 600;
ALTER TABLE aioq_jobs ADD COLUMN IF NOT EXISTS timeout FLOAT;
ALTER TABLE aioq_jobs ADD COLUMN IF NOT EXISTS result_ttl INT NOT NULL DEFAULT 3600;
ALTER TABLE aioq_jobs ADD COLUMN IF NOT EXISTS dead_letter_queue TEXT;

CREATE INDEX IF NOT EXISTS aioq_jobs_queue_status ON aioq_jobs (queue, status);
CREATE INDEX IF NOT EXISTS aioq_jobs_run_at ON aioq_jobs (run_at) WHERE run_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS aioq_jobs_claim
    ON aioq_jobs (queue, priority DESC, enqueued_at)
    WHERE status IN ('pending', 'retrying');
CREATE INDEX IF NOT EXISTS aioq_jobs_depends_on ON aioq_jobs USING GIN (depends_on);

CREATE TABLE IF NOT EXISTS aioq_workers (
    worker_id       TEXT PRIMARY KEY,
    queues          JSONB NOT NULL DEFAULT '[]',
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS aioq_cron_locks (
    lock_key    TEXT PRIMARY KEY,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL
);
"""

_JOB_COLUMNS = """
    id, task_name, queue, status, args, kwargs,
    retries, max_retries, retry_delay, retry_backoff, retry_backoff_max, timeout,
    enqueued_at, run_at, priority, save_result, result_ttl,
    dead_letter_queue, depends_on
"""


class PostgresBroker(SQLBroker):
    def __init__(self, dsn: str, min_size: int = 2, max_size: int = 10):
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self._pool: asyncpg.Pool | None = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Broker not connected. Call connect() first.")
        return self._pool

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            self.dsn, min_size=self.min_size, max_size=self.max_size
        )
        async with self._pool.acquire() as conn:
            await conn.execute(_INIT_SQL)

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    async def enqueue(self, job: Job) -> None:
        await self._check_dependencies(job)
        await self._upsert(job)
        if job.status == JobStatus.waiting:
            # A dependency may have completed while we were inserting.
            await self._promote_if_ready(job)

    async def enqueue_many(self, jobs: list[Job]) -> None:
        for job in jobs:
            await self._check_dependencies(job)
        async with self.pool.acquire() as conn, conn.transaction():
            for job in jobs:
                await self._upsert(job, conn=conn)
        for job in jobs:
            if job.status == JobStatus.waiting:
                await self._promote_if_ready(job)

    async def _upsert(self, job: Job, conn: asyncpg.Connection | None = None) -> None:
        sql = f"""
            INSERT INTO aioq_jobs ({_JOB_COLUMNS})
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
            ON CONFLICT (id) DO UPDATE SET
                status     = EXCLUDED.status,
                queue      = EXCLUDED.queue,
                retries    = EXCLUDED.retries,
                run_at     = EXCLUDED.run_at,
                priority   = EXCLUDED.priority,
                depends_on = EXCLUDED.depends_on
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
            job.retry_backoff,
            job.retry_backoff_max,
            job.timeout,
            job.enqueued_at,
            job.run_at,
            job.priority,
            job.save_result,
            job.result_ttl,
            job.dead_letter_queue,
            json.dumps(job.depends_on),
        )
        if conn is not None:
            await conn.execute(sql, *params)
        else:
            async with self.pool.acquire() as own:
                await own.execute(sql, *params)

    async def dequeue(self, queues: list[str], timeout: float = DEQUEUE_TIMEOUT) -> Job | None:
        """Claim one job.

        ``FOR UPDATE SKIP LOCKED`` ensures two workers never claim the same
        row. Postgres cannot block on a table, so this polls until *timeout*.
        """
        deadline = time.monotonic() + timeout
        while True:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    UPDATE aioq_jobs
                    SET status = 'running', started_at = now()
                    WHERE id = (
                        SELECT id FROM aioq_jobs
                        WHERE queue = ANY($1::text[])
                          AND status = ANY($2::text[])
                          AND (run_at IS NULL OR run_at <= now())
                        ORDER BY priority DESC, enqueued_at
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    RETURNING *
                    """,
                    queues,
                    list(_RUNNABLE),
                )
            if row:
                return self.row_to_job(row)
            if time.monotonic() + SQL_POLL_INTERVAL >= deadline:
                return None
            await asyncio.sleep(SQL_POLL_INTERVAL)

    async def update_job(self, job: Job) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE aioq_jobs SET
                    status       = $2,
                    queue        = $3,
                    retries      = $4,
                    started_at   = $5,
                    completed_at = $6,
                    result       = $7,
                    error        = $8,
                    worker_id    = $9,
                    run_at       = $10
                WHERE id = $1
                """,
                job.id,
                job.status.value,
                job.queue,
                job.retries,
                job.started_at,
                job.completed_at,
                json.dumps(job.result) if job.result is not None else None,
                job.error,
                job.worker_id,
                job.run_at,
            )

        if job.status == JobStatus.completed:
            await self._resolve_dependents(job.id)

    async def retry_job(self, job_id: str) -> bool:
        async with self.pool.acquire() as conn:
            result = await conn.execute(
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
                WHERE id = $1 AND status IN ('failed', 'cancelled')
                """,
                job_id,
            )
        return result == "UPDATE 1"

    async def cancel_job(self, job_id: str) -> bool:
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE aioq_jobs SET status = 'cancelled'
                WHERE id = $1 AND status IN ('pending', 'retrying', 'waiting')
                """,
                job_id,
            )
        return result == "UPDATE 1"

    async def get_job(self, job_id: str) -> Job | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM aioq_jobs WHERE id = $1", job_id)
        return self.row_to_job(row) if row else None

    async def list_jobs(
        self,
        queue: str | None = None,
        status: JobStatus | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        where, params = self.build_filters(queue, status, lambda i: f"${i}")
        params += [limit, offset]

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM aioq_jobs {where} "
                f"ORDER BY enqueued_at DESC LIMIT ${len(params) - 1} OFFSET ${len(params)}",
                *params,
            )
        return [self.row_to_job(r) for r in rows]

    # ------------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------------

    async def _fetch_waiting_dependents(self, job_id: str) -> list[tuple[str, list[str]]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, depends_on FROM aioq_jobs
                WHERE status = 'waiting' AND depends_on @> $1::jsonb
                """,
                json.dumps([job_id]),
            )
        return [(r["id"], json.loads(r["depends_on"]) if r["depends_on"] else []) for r in rows]

    async def _count_completed(self, dep_ids: list[str]) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT count(*)::int FROM aioq_jobs "
                "WHERE id = ANY($1::text[]) AND status = 'completed'",
                dep_ids,
            )

    async def _mark_waiting_as_pending(self, job_id: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE aioq_jobs SET status = 'pending' WHERE id = $1 AND status = 'waiting'",
                job_id,
            )

    async def _promote_if_ready(self, job: Job) -> None:
        if await self._all_deps_completed(job.depends_on):
            await self._mark_waiting_as_pending(job.id)
            job.status = JobStatus.pending

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    async def queue_stats(self) -> dict[str, dict[str, int]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT queue, status, count(*)::int AS cnt FROM aioq_jobs GROUP BY queue, status"
            )
        stats: dict[str, dict[str, int]] = {}
        for row in rows:
            stats.setdefault(row["queue"], {})[row["status"]] = row["cnt"]
        return stats

    # ------------------------------------------------------------------
    # Workers
    # ------------------------------------------------------------------

    async def register_worker(self, worker_id: str, queues: list[str]) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO aioq_workers (worker_id, queues, registered_at, last_heartbeat)
                VALUES ($1, $2, now(), now())
                ON CONFLICT (worker_id) DO UPDATE SET
                    queues = EXCLUDED.queues,
                    last_heartbeat = now()
                """,
                worker_id,
                json.dumps(queues),
            )

    async def heartbeat_worker(self, worker_id: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE aioq_workers SET last_heartbeat = now() WHERE worker_id = $1",
                worker_id,
            )

    async def deregister_worker(self, worker_id: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM aioq_workers WHERE worker_id = $1", worker_id)

    async def list_workers(self) -> list[dict]:
        now = datetime.now(UTC).timestamp()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM aioq_workers")
        return [self.worker_row_to_dict(row, now) for row in rows]

    # ------------------------------------------------------------------
    # Cron coordination
    # ------------------------------------------------------------------

    async def acquire_cron_lock(self, key: str, ttl: float = CRON_LOCK_TTL) -> bool:
        async with self.pool.acquire() as conn:
            # Reclaim the row if a previous holder expired, otherwise the
            # insert conflicts and nobody fires.
            result = await conn.execute(
                """
                INSERT INTO aioq_cron_locks (lock_key, acquired_at, expires_at)
                VALUES ($1, now(), now() + ($2 || ' seconds')::interval)
                ON CONFLICT (lock_key) DO UPDATE SET
                    acquired_at = now(),
                    expires_at  = now() + ($2 || ' seconds')::interval
                WHERE aioq_cron_locks.expires_at <= now()
                """,
                key,
                str(int(ttl)),
            )
        return result == "INSERT 0 1"

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    async def purge(
        self,
        older_than: float,
        statuses: Collection[JobStatus] | None = None,
    ) -> int:
        cutoff = self._purge_cutoff(older_than)
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM aioq_cron_locks WHERE expires_at <= now()")
            result = await conn.execute(
                """
                DELETE FROM aioq_jobs
                WHERE status = ANY($1::text[])
                  AND coalesce(completed_at, enqueued_at) <= $2
                """,
                self._purge_statuses(statuses),
                cutoff,
            )
        return int(result.rsplit(" ", 1)[-1])
