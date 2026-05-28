from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from typing import Any

try:
    import asyncpg
except ImportError as e:
    raise ImportError(
        "asyncpg is required for PostgreSQL broker: pip install asyncpg"
    ) from e

from ..models import Job, JobStatus
from .base import BaseBroker

# Table DDL (auto-created on connect if not exists):
#
#   aioq_jobs
#     id            TEXT PRIMARY KEY
#     task_name     TEXT NOT NULL
#     queue         TEXT NOT NULL DEFAULT 'default'
#     status        TEXT NOT NULL DEFAULT 'pending'
#     args          JSONB NOT NULL DEFAULT '[]'
#     kwargs        JSONB NOT NULL DEFAULT '{}'
#     retries       INT NOT NULL DEFAULT 0
#     max_retries   INT NOT NULL DEFAULT 0
#     retry_delay   FLOAT NOT NULL DEFAULT 0
#     enqueued_at   TIMESTAMPTZ NOT NULL DEFAULT now()
#     started_at    TIMESTAMPTZ
#     completed_at  TIMESTAMPTZ
#     run_at        TIMESTAMPTZ
#     result        JSONB
#     error         TEXT
#     worker_id     TEXT
#     save_result   BOOLEAN NOT NULL DEFAULT FALSE
#
#   aioq_workers
#     worker_id       TEXT PRIMARY KEY
#     queues          JSONB NOT NULL DEFAULT '[]'
#     registered_at   TIMESTAMPTZ NOT NULL DEFAULT now()
#     last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now()

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS aioq_jobs (
    id            TEXT PRIMARY KEY,
    task_name     TEXT NOT NULL,
    queue         TEXT NOT NULL DEFAULT 'default',
    status        TEXT NOT NULL DEFAULT 'pending',
    args          JSONB NOT NULL DEFAULT '[]',
    kwargs        JSONB NOT NULL DEFAULT '{}',
    retries       INT NOT NULL DEFAULT 0,
    max_retries   INT NOT NULL DEFAULT 0,
    retry_delay   FLOAT NOT NULL DEFAULT 0,
    enqueued_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    run_at        TIMESTAMPTZ,
    result        JSONB,
    error         TEXT,
    worker_id     TEXT,
    save_result   BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS aioq_jobs_queue_status ON aioq_jobs (queue, status);
CREATE INDEX IF NOT EXISTS aioq_jobs_run_at ON aioq_jobs (run_at) WHERE run_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS aioq_workers (
    worker_id       TEXT PRIMARY KEY,
    queues          JSONB NOT NULL DEFAULT '[]',
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

_WORKER_TTL = 30  # seconds


class PostgresBroker(BaseBroker):
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
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO aioq_jobs
                    (id, task_name, queue, status, args, kwargs,
                     retries, max_retries, retry_delay,
                     enqueued_at, run_at, save_result)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (id) DO UPDATE SET
                    status = EXCLUDED.status,
                    retries = EXCLUDED.retries,
                    run_at = EXCLUDED.run_at
                """,
                job.id,
                job.task_name,
                job.queue,
                job.status.value,
                json.dumps(job.args),
                json.dumps(job.kwargs),
                job.retries,
                job.max_retries,
                job.retry_delay,
                job.enqueued_at,
                job.run_at,
                job.save_result,
            )

    async def dequeue(self, queues: list[str], timeout: float = 2.0) -> Job | None:
        """
        SKIP LOCKED ensures multiple workers don't pick up the same job.
        Polls until a job is available or timeout expires.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    UPDATE aioq_jobs
                    SET status = 'running', started_at = now()
                    WHERE id = (
                        SELECT id FROM aioq_jobs
                        WHERE queue = ANY($1::text[])
                          AND status = 'pending'
                          AND (run_at IS NULL OR run_at <= now())
                        ORDER BY enqueued_at
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    RETURNING *
                    """,
                    queues,
                )
            if row:
                return self._row_to_job(row)
            import asyncio

            await asyncio.sleep(0.5)
        return None

    async def ack(self, job: Job) -> None:
        await self.update_job(job)

    async def nack(self, job: Job, requeue: bool = False) -> None:
        if requeue:
            job.status = JobStatus.pending
        await self.update_job(job)

    async def update_job(self, job: Job) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE aioq_jobs SET
                    status       = $2,
                    retries      = $3,
                    started_at   = $4,
                    completed_at = $5,
                    result       = $6,
                    error        = $7,
                    worker_id    = $8
                WHERE id = $1
                """,
                job.id,
                job.status.value,
                job.retries,
                job.started_at,
                job.completed_at,
                json.dumps(job.result) if job.result is not None else None,
                job.error,
                job.worker_id,
            )

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
                    worker_id    = NULL
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
                WHERE id = $1 AND status IN ('pending', 'retrying')
                """,
                job_id,
            )
        return result == "UPDATE 1"

    async def get_job(self, job_id: str) -> Job | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM aioq_jobs WHERE id = $1", job_id)
        return self._row_to_job(row) if row else None

    async def list_jobs(
        self,
        queue: str | None = None,
        status: JobStatus | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        conditions = []
        params: list[Any] = []

        if queue:
            params.append(queue)
            conditions.append(f"queue = ${len(params)}")
        if status:
            params.append(status.value)
            conditions.append(f"status = ${len(params)}")

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params += [limit, offset]

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM aioq_jobs {where} ORDER BY enqueued_at DESC LIMIT ${len(params) - 1} OFFSET ${len(params)}",
                *params,
            )
        return [self._row_to_job(r) for r in rows]

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
            await conn.execute(
                "DELETE FROM aioq_workers WHERE worker_id = $1", worker_id
            )

    async def list_workers(self) -> list[dict]:
        cutoff = datetime.now(UTC).replace(tzinfo=None).timestamp() - _WORKER_TTL
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM aioq_workers")
        workers = []
        for row in rows:
            hb = row["last_heartbeat"].timestamp()
            workers.append(
                {
                    "worker_id": row["worker_id"],
                    "queues": json.loads(row["queues"]),
                    "registered_at": row["registered_at"].isoformat(),
                    "last_heartbeat": row["last_heartbeat"].isoformat(),
                    "alive": hb > cutoff,
                }
            )
        return workers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_job(row: asyncpg.Record) -> Job:
        return Job(
            id=row["id"],
            task_name=row["task_name"],
            queue=row["queue"],
            status=JobStatus(row["status"]),
            args=json.loads(row["args"]),
            kwargs=json.loads(row["kwargs"]),
            retries=row["retries"],
            max_retries=row["max_retries"],
            retry_delay=row["retry_delay"],
            enqueued_at=row["enqueued_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            run_at=row["run_at"],
            result=json.loads(row["result"]) if row["result"] else None,
            error=row["error"],
            worker_id=row["worker_id"],
            save_result=row["save_result"],
        )
