from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime
from typing import Any

try:
    import aiomysql
except ImportError as e:
    raise ImportError("aiomysql is required for MySQL broker: pip install aiomysql") from e

from ..models import Job, JobStatus
from .base import BaseBroker

_INIT_SQLS = [
    """
    CREATE TABLE IF NOT EXISTS aioq_jobs (
        id VARCHAR(36) PRIMARY KEY,
        task_name VARCHAR(255) NOT NULL,
        queue VARCHAR(255) NOT NULL DEFAULT 'default',
        status VARCHAR(32) NOT NULL DEFAULT 'pending',
        args JSON NOT NULL,
        kwargs JSON NOT NULL,
        retries INT NOT NULL DEFAULT 0,
        max_retries INT NOT NULL DEFAULT 0,
        retry_delay FLOAT NOT NULL DEFAULT 0,
        enqueued_at DATETIME(6) NOT NULL DEFAULT NOW(6),
        started_at DATETIME(6),
        completed_at DATETIME(6),
        run_at DATETIME(6),
        result JSON,
        error TEXT,
        worker_id VARCHAR(36),
        save_result TINYINT(1) NOT NULL DEFAULT 0
    )
    """,
    "CREATE INDEX IF NOT EXISTS aioq_jobs_queue_status ON aioq_jobs (queue, status)",
    "CREATE INDEX IF NOT EXISTS aioq_jobs_run_at ON aioq_jobs (run_at)",
    """
    CREATE TABLE IF NOT EXISTS aioq_workers (
        worker_id VARCHAR(36) PRIMARY KEY,
        queues JSON NOT NULL,
        registered_at DATETIME(6) NOT NULL DEFAULT NOW(6),
        last_heartbeat DATETIME(6) NOT NULL DEFAULT NOW(6)
    )
    """,
]

_WORKER_TTL = 30  # seconds


def _ensure_utc(dt: datetime | None) -> datetime | None:
    """Attach UTC timezone to a naive datetime returned from MySQL."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


class MySQLBroker(BaseBroker):
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
            for sql in _INIT_SQLS:
                await cur.execute(sql)

    async def disconnect(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    async def enqueue(self, job: Job) -> None:
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO aioq_jobs
                    (id, task_name, queue, status, args, kwargs,
                     retries, max_retries, retry_delay,
                     enqueued_at, run_at, save_result)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status),
                    retries = VALUES(retries),
                    run_at = VALUES(run_at)
                """,
                (
                    job.id,
                    job.task_name,
                    job.queue,
                    job.status.value,
                    json.dumps(job.args),
                    json.dumps(job.kwargs),
                    job.retries,
                    job.max_retries,
                    job.retry_delay,
                    job.enqueued_at.replace(tzinfo=None) if job.enqueued_at else None,
                    job.run_at.replace(tzinfo=None) if job.run_at else None,
                    int(job.save_result),
                ),
            )

    async def dequeue(self, queues: list[str], timeout: float = 2.0) -> Job | None:
        """
        SELECT ... FOR UPDATE SKIP LOCKED ensures multiple workers do not pick the same job.
        Polls until a job is available or timeout expires.
        """
        placeholders = ", ".join(["%s"] * len(queues))
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            async with self.pool.acquire() as conn:
                await conn.autocommit(False)
                try:
                    async with conn.cursor(aiomysql.DictCursor) as cur:
                        await cur.execute(
                            f"""
                            SELECT id FROM aioq_jobs
                            WHERE queue IN ({placeholders})
                              AND status = 'pending'
                              AND (run_at IS NULL OR run_at <= NOW())
                            ORDER BY enqueued_at
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                            """,
                            queues,
                        )
                        row = await cur.fetchone()
                        if row:
                            job_id = row["id"]
                            await cur.execute(
                                """
                                UPDATE aioq_jobs
                                SET status = 'running', started_at = NOW(6)
                                WHERE id = %s
                                """,
                                (job_id,),
                            )
                            await conn.commit()
                            await cur.execute("SELECT * FROM aioq_jobs WHERE id = %s", (job_id,))
                            job_row = await cur.fetchone()
                        else:
                            await conn.rollback()
                            job_row = None
                finally:
                    await conn.autocommit(True)

            if job_row:
                return self._row_to_job(job_row)
            await asyncio.sleep(0.5)
        return None

    async def ack(self, job: Job) -> None:
        await self.update_job(job)

    async def nack(self, job: Job, requeue: bool = False) -> None:
        if requeue:
            job.status = JobStatus.pending
        await self.update_job(job)

    async def update_job(self, job: Job) -> None:
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE aioq_jobs SET
                    status       = %s,
                    retries      = %s,
                    started_at   = %s,
                    completed_at = %s,
                    result       = %s,
                    error        = %s,
                    worker_id    = %s
                WHERE id = %s
                """,
                (
                    job.status.value,
                    job.retries,
                    job.started_at.replace(tzinfo=None) if job.started_at else None,
                    job.completed_at.replace(tzinfo=None) if job.completed_at else None,
                    json.dumps(job.result) if job.result is not None else None,
                    job.error,
                    job.worker_id,
                    job.id,
                ),
            )

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
                    worker_id    = NULL
                WHERE id = %s AND status IN ('failed', 'cancelled')
                """,
                (job_id,),
            )
            return cur.rowcount == 1

    async def cancel_job(self, job_id: str) -> bool:
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE aioq_jobs SET status = 'cancelled'
                WHERE id = %s AND status IN ('pending', 'retrying')
                """,
                (job_id,),
            )
            return cur.rowcount == 1

    async def get_job(self, job_id: str) -> Job | None:
        async with self.pool.acquire() as conn, conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM aioq_jobs WHERE id = %s", (job_id,))
            row = await cur.fetchone()
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
            conditions.append("queue = %s")
            params.append(queue)
        if status:
            conditions.append("status = %s")
            params.append(status.value)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params += [limit, offset]

        async with self.pool.acquire() as conn, conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                f"SELECT * FROM aioq_jobs {where} ORDER BY enqueued_at DESC LIMIT %s OFFSET %s",
                params,
            )
            rows = await cur.fetchall()
        return [self._row_to_job(r) for r in rows]

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

    async def register_worker(self, worker_id: str, queues: list[str]) -> None:
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO aioq_workers (worker_id, queues, registered_at, last_heartbeat)
                VALUES (%s, %s, NOW(6), NOW(6))
                ON DUPLICATE KEY UPDATE
                    queues = VALUES(queues),
                    last_heartbeat = NOW(6)
                """,
                (worker_id, json.dumps(queues)),
            )

    async def heartbeat_worker(self, worker_id: str) -> None:
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "UPDATE aioq_workers SET last_heartbeat = NOW(6) WHERE worker_id = %s",
                (worker_id,),
            )

    async def deregister_worker(self, worker_id: str) -> None:
        async with self.pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute("DELETE FROM aioq_workers WHERE worker_id = %s", (worker_id,))

    async def list_workers(self) -> list[dict]:
        cutoff = datetime.now(UTC).timestamp() - _WORKER_TTL
        async with self.pool.acquire() as conn, conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM aioq_workers")
            rows = await cur.fetchall()
        workers = []
        for row in rows:
            hb = _ensure_utc(row["last_heartbeat"])
            registered = _ensure_utc(row["registered_at"])
            workers.append(
                {
                    "worker_id": row["worker_id"],
                    "queues": json.loads(row["queues"]),
                    "registered_at": registered.isoformat() if registered else None,
                    "last_heartbeat": hb.isoformat() if hb else None,
                    "alive": hb.timestamp() > cutoff if hb else False,
                }
            )
        return workers

    @staticmethod
    def _row_to_job(row: dict) -> Job:
        return Job(
            id=row["id"],
            task_name=row["task_name"],
            queue=row["queue"],
            status=JobStatus(row["status"]),
            args=json.loads(row["args"]) if isinstance(row["args"], str) else row["args"],
            kwargs=json.loads(row["kwargs"]) if isinstance(row["kwargs"], str) else row["kwargs"],
            retries=row["retries"],
            max_retries=row["max_retries"],
            retry_delay=float(row["retry_delay"]),
            enqueued_at=_ensure_utc(row["enqueued_at"]),
            started_at=_ensure_utc(row["started_at"]),
            completed_at=_ensure_utc(row["completed_at"]),
            run_at=_ensure_utc(row["run_at"]),
            result=json.loads(row["result"]) if row.get("result") else None,
            error=row["error"],
            worker_id=row["worker_id"],
            save_result=bool(row["save_result"]),
        )
