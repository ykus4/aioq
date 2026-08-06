"""Shared behaviour for the SQL-backed brokers.

PostgreSQL and MySQL differ in placeholder syntax, JSON handling and how they
report affected rows, but the *logic* on top of that — building list filters,
turning a row into a :class:`~aioq.models.Job`, deciding which waiting jobs a
completed job unblocks — is identical. It lives here so the two backends cannot
drift apart.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Collection, Mapping
from datetime import UTC, datetime
from typing import Any

from ..constants import WORKER_TTL
from ..models import Job, JobStatus
from .base import BaseBroker


def parse_json_list(value: Any) -> list:
    """Read a JSON array column, whether the driver decoded it or not."""
    if value is None:
        return []
    if isinstance(value, str | bytes | bytearray):
        return json.loads(value)
    return value


def parse_json_dict(value: Any) -> dict:
    """Read a JSON object column, whether the driver decoded it or not."""
    if value is None:
        return {}
    if isinstance(value, str | bytes | bytearray):
        return json.loads(value)
    return value


def parse_json_value(value: Any) -> Any:
    """Read a free-form JSON column, preserving falsy values like ``0``."""
    if value is None:
        return None
    if isinstance(value, str | bytes | bytearray):
        return json.loads(value)
    return value


def ensure_utc(dt: datetime | None) -> datetime | None:
    """Attach UTC to a naive datetime returned by a driver that drops tzinfo."""
    if dt is None:
        return None
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt


def require_utc(dt: datetime | None) -> datetime:
    """Like :func:`ensure_utc`, for columns declared NOT NULL."""
    if dt is None:
        raise ValueError("Expected a timestamp but the column was NULL")
    return ensure_utc(dt)  # type: ignore[return-value]


class SQLBroker(BaseBroker):
    """Base class for brokers backed by a relational table."""

    #: Table names, overridable if you need a prefix.
    jobs_table = "aioq_jobs"
    workers_table = "aioq_workers"
    cron_locks_table = "aioq_cron_locks"

    # ------------------------------------------------------------------
    # Query building
    # ------------------------------------------------------------------

    @staticmethod
    def build_filters(
        queue: str | None,
        status: JobStatus | None,
        placeholder: Callable[[int], str],
    ) -> tuple[str, list[Any]]:
        """Return ``(where_clause, params)`` for a queue/status filter.

        *placeholder* renders the driver's parameter marker for a 1-based
        position — ``lambda i: f"${i}"`` for asyncpg, ``lambda i: "%s"`` for
        aiomysql.
        """
        conditions: list[str] = []
        params: list[Any] = []

        if queue:
            params.append(queue)
            conditions.append(f"queue = {placeholder(len(params))}")
        if status:
            params.append(JobStatus(status).value)
            conditions.append(f"status = {placeholder(len(params))}")

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        return where, params

    # ------------------------------------------------------------------
    # Row mapping
    # ------------------------------------------------------------------

    @classmethod
    def row_to_job(cls, row: Mapping[str, Any]) -> Job:
        """Map a jobs-table row onto a :class:`~aioq.models.Job`.

        Optional columns are read with ``.get()`` so a table created by an
        older aioq — one whose migrations have not been applied yet, e.g.
        because the connecting user cannot ALTER — still yields usable jobs
        with the model's defaults.
        """
        defaults = Job.model_fields
        timeout = row.get("timeout")
        return Job(
            id=row["id"],
            task_name=row["task_name"],
            queue=row["queue"],
            status=JobStatus(row["status"]),
            args=parse_json_list(row["args"]),
            kwargs=parse_json_dict(row["kwargs"]),
            priority=row.get("priority", 0),
            retries=row["retries"],
            max_retries=row["max_retries"],
            retry_delay=float(row["retry_delay"]),
            retry_backoff=bool(row.get("retry_backoff", False)),
            retry_backoff_max=float(
                row.get("retry_backoff_max") or defaults["retry_backoff_max"].default
            ),
            timeout=float(timeout) if timeout is not None else None,
            enqueued_at=require_utc(row["enqueued_at"]),
            started_at=ensure_utc(row["started_at"]),
            completed_at=ensure_utc(row["completed_at"]),
            run_at=ensure_utc(row["run_at"]),
            result=parse_json_value(row["result"]),
            result_ttl=row.get("result_ttl") or defaults["result_ttl"].default,
            error=row["error"],
            worker_id=row["worker_id"],
            save_result=bool(row["save_result"]),
            dead_letter_queue=row.get("dead_letter_queue"),
            depends_on=parse_json_list(row.get("depends_on")),
        )

    @classmethod
    def worker_row_to_dict(cls, row: Mapping[str, Any], now: float) -> dict:
        heartbeat = ensure_utc(row["last_heartbeat"])
        registered = ensure_utc(row["registered_at"])
        return {
            "worker_id": row["worker_id"],
            "queues": parse_json_list(row["queues"]),
            "registered_at": registered.isoformat() if registered else None,
            "last_heartbeat": heartbeat.isoformat() if heartbeat else None,
            "alive": bool(heartbeat and heartbeat.timestamp() > now - WORKER_TTL),
        }

    # ------------------------------------------------------------------
    # Dependency resolution
    # ------------------------------------------------------------------

    async def _resolve_dependents(self, completed_job_id: str) -> None:
        """Promote every waiting job whose last dependency just completed."""
        for waiting_id, dep_ids in await self._fetch_waiting_dependents(completed_job_id):
            if completed_job_id not in dep_ids:
                continue
            if await self._all_deps_completed(dep_ids):
                await self._mark_waiting_as_pending(waiting_id)

    async def _all_deps_completed(self, dep_ids: Collection[str]) -> bool:
        if not dep_ids:
            return True
        unique = list(dict.fromkeys(dep_ids))
        return await self._count_completed(unique) == len(unique)

    async def _fetch_waiting_dependents(self, job_id: str) -> list[tuple[str, list[str]]]:
        """Return ``(job_id, depends_on)`` for waiting jobs that may depend on *job_id*.

        Implementations may over-return (the caller filters); narrowing this in
        SQL is purely an optimisation.
        """
        raise NotImplementedError

    async def _count_completed(self, dep_ids: list[str]) -> int:
        """Return how many of *dep_ids* name a completed job."""
        raise NotImplementedError

    async def _mark_waiting_as_pending(self, job_id: str) -> None:
        """Flip a job from waiting to pending, if it is still waiting."""
        raise NotImplementedError
