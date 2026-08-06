"""In-process broker.

Everything lives in Python objects, so there is nothing to install and nothing
to clean up. Use it for tests and for running the whole stack — worker,
dashboard, enqueueing code — inside a single process during development.

It is *not* usable across processes: a worker started with ``aioq worker`` gets
its own empty broker. Reach for Redis or a SQL backend the moment more than one
process is involved.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Collection
from datetime import UTC, datetime

from ..constants import CRON_LOCK_TTL, DEQUEUE_TIMEOUT, WORKER_TTL
from ..models import CANCELLABLE_STATUSES, RETRIABLE_STATUSES, Job, JobStatus
from .base import BaseBroker

# How often a blocked dequeue re-checks the queues.
_POLL_INTERVAL = 0.02


class MemoryBroker(BaseBroker):
    """Broker backed by in-process dictionaries."""

    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        # {queue: [job_id, ...]} — kept sorted by (-priority, enqueued_at)
        self._ready: dict[str, list[str]] = {}
        self._workers: dict[str, dict] = {}
        self._cron_locks: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        return None

    async def disconnect(self) -> None:
        return None

    async def clear(self) -> None:
        """Drop all state. Handy between tests."""
        async with self._lock:
            self._jobs.clear()
            self._ready.clear()
            self._workers.clear()
            self._cron_locks.clear()

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    async def enqueue(self, job: Job) -> None:
        await self._check_dependencies(job)
        async with self._lock:
            self._jobs[job.id] = job
            if job.status != JobStatus.waiting:
                self._make_ready(job)
        if job.status == JobStatus.waiting:
            await self._promote_if_ready(job.id)

    def _make_ready(self, job: Job) -> None:
        """Add *job* to its queue's runnable list, keeping priority order."""
        queue = self._ready.setdefault(job.queue, [])
        if job.id not in queue:
            queue.append(job.id)
        queue.sort(key=lambda jid: (-self._jobs[jid].priority, self._jobs[jid].enqueued_at))

    def _pop_runnable(self, queues: list[str]) -> Job | None:
        """Return the highest-priority job that is due, across *queues*."""
        now = datetime.now(UTC)
        best: tuple[int, datetime, str, str] | None = None

        for queue in queues:
            for job_id in self._ready.get(queue, []):
                job = self._jobs.get(job_id)
                if job is None or (job.run_at is not None and job.run_at > now):
                    continue
                candidate = (-job.priority, job.enqueued_at, queue, job_id)
                if best is None or candidate < best:
                    best = candidate

        if best is None:
            return None

        *_, queue, job_id = best
        self._ready[queue].remove(job_id)
        return self._jobs[job_id]

    async def dequeue(self, queues: list[str], timeout: float = DEQUEUE_TIMEOUT) -> Job | None:
        deadline = time.monotonic() + timeout
        while True:
            async with self._lock:
                job = self._pop_runnable(queues)
            if job is not None:
                if job.status == JobStatus.cancelled:
                    continue  # dropped while queued
                return job
            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(_POLL_INTERVAL)

    async def nack(self, job: Job, requeue: bool = False) -> None:
        async with self._lock:
            self._jobs[job.id] = job
            if requeue:
                self._make_ready(job)

    async def update_job(self, job: Job) -> None:
        async with self._lock:
            self._jobs[job.id] = job
        if job.status == JobStatus.completed:
            for dependent_id in [
                other.id for other in list(self._jobs.values()) if job.id in other.depends_on
            ]:
                await self._promote_if_ready(dependent_id)

    async def _promote_if_ready(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if job is None or job.status != JobStatus.waiting:
            return
        if not await self._all_deps_completed(job.depends_on):
            return
        async with self._lock:
            job.status = JobStatus.pending
            self._make_ready(job)

    async def get_job(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    async def cancel_job(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if job is None or job.status not in CANCELLABLE_STATUSES:
            return False
        async with self._lock:
            job.status = JobStatus.cancelled
            queue = self._ready.get(job.queue)
            if queue and job_id in queue:
                queue.remove(job_id)
        return True

    async def retry_job(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if job is None or job.status not in RETRIABLE_STATUSES:
            return False
        async with self._lock:
            job.reset_for_replay()
            job.status = JobStatus.pending
            self._make_ready(job)
        return True

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    async def list_jobs(
        self,
        queue: str | None = None,
        status: JobStatus | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        jobs = [
            job
            for job in self._jobs.values()
            if (queue is None or job.queue == queue) and (status is None or job.status == status)
        ]
        jobs.sort(key=lambda j: j.enqueued_at, reverse=True)
        return jobs[offset : offset + limit]

    async def queue_stats(self) -> dict[str, dict[str, int]]:
        stats: dict[str, dict[str, int]] = {}
        for job in self._jobs.values():
            per_status = stats.setdefault(job.queue, {})
            per_status[job.status.value] = per_status.get(job.status.value, 0) + 1
        return stats

    # ------------------------------------------------------------------
    # Workers
    # ------------------------------------------------------------------

    async def register_worker(self, worker_id: str, queues: list[str]) -> None:
        now = datetime.now(UTC)
        self._workers[worker_id] = {
            "worker_id": worker_id,
            "queues": list(queues),
            "registered_at": now.isoformat(),
            "last_heartbeat": now.isoformat(),
        }

    async def heartbeat_worker(self, worker_id: str) -> None:
        if worker_id in self._workers:
            self._workers[worker_id]["last_heartbeat"] = datetime.now(UTC).isoformat()

    async def deregister_worker(self, worker_id: str) -> None:
        self._workers.pop(worker_id, None)

    async def list_workers(self) -> list[dict]:
        cutoff = time.time() - WORKER_TTL
        workers = []
        for info in self._workers.values():
            heartbeat = datetime.fromisoformat(info["last_heartbeat"]).timestamp()
            workers.append({**info, "alive": heartbeat > cutoff})
        return workers

    # ------------------------------------------------------------------
    # Cron coordination
    # ------------------------------------------------------------------

    async def acquire_cron_lock(self, key: str, ttl: float = CRON_LOCK_TTL) -> bool:
        now = time.time()
        async with self._lock:
            expires_at = self._cron_locks.get(key)
            if expires_at is not None and expires_at > now:
                return False
            self._cron_locks[key] = now + ttl
        return True

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    async def purge(
        self,
        older_than: float,
        statuses: Collection[JobStatus] | None = None,
    ) -> int:
        cutoff = self._purge_cutoff(older_than)
        wanted = set(self._purge_statuses(statuses))
        doomed = [
            job.id
            for job in self._jobs.values()
            if job.status.value in wanted and (job.completed_at or job.enqueued_at) <= cutoff
        ]

        async with self._lock:
            now = time.time()
            for job_id in doomed:
                job = self._jobs.pop(job_id)
                queue = self._ready.get(job.queue)
                if queue and job_id in queue:
                    queue.remove(job_id)
            self._cron_locks = {k: v for k, v in self._cron_locks.items() if v > now}

        return len(doomed)
