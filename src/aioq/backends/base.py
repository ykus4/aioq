from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Collection
from datetime import UTC, datetime, timedelta

from ..constants import CRON_LOCK_TTL, DEQUEUE_TIMEOUT
from ..models import TERMINAL_STATUSES, Job, JobStatus

logger = logging.getLogger("aioq.broker")


class BaseBroker(ABC):
    """Abstract broker interface. Implement this to add a new backend."""

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    async def __aenter__(self) -> BaseBroker:
        await self.connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self.disconnect()

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    async def enqueue(self, job: Job) -> None: ...

    async def enqueue_many(self, jobs: list[Job]) -> None:
        """Enqueue multiple jobs. Default: loop over enqueue()."""
        for job in jobs:
            await self.enqueue(job)

    @abstractmethod
    async def dequeue(self, queues: list[str], timeout: float = DEQUEUE_TIMEOUT) -> Job | None: ...

    @abstractmethod
    async def update_job(self, job: Job) -> None: ...

    @abstractmethod
    async def get_job(self, job_id: str) -> Job | None: ...

    async def ack(self, job: Job) -> None:
        """Mark job as acknowledged by persisting its current state."""
        await self.update_job(job)

    async def nack(self, job: Job, requeue: bool = False) -> None:
        """Negative-acknowledge a job. Requeue it as pending if requeue=True."""
        if requeue:
            job.status = JobStatus.pending
        await self.update_job(job)

    @abstractmethod
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending/retrying/waiting job. Returns True if cancelled."""
        ...

    @abstractmethod
    async def retry_job(self, job_id: str) -> bool:
        """Re-enqueue a failed or cancelled job as a fresh pending job. Returns True if retried."""
        ...

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    @abstractmethod
    async def list_jobs(
        self,
        queue: str | None = None,
        status: JobStatus | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]: ...

    @abstractmethod
    async def queue_stats(self) -> dict[str, dict[str, int]]:
        """Return per-queue counts: {queue: {status: count}}"""
        ...

    async def list_dead_jobs(self, queue: str | None = None) -> list[Job]:
        """Return all jobs with status=dead, optionally filtered by DLQ queue name."""
        return await self.list_jobs(queue=queue, status=JobStatus.dead)

    # ------------------------------------------------------------------
    # Workers
    # ------------------------------------------------------------------

    @abstractmethod
    async def register_worker(self, worker_id: str, queues: list[str]) -> None: ...

    @abstractmethod
    async def heartbeat_worker(self, worker_id: str) -> None: ...

    @abstractmethod
    async def deregister_worker(self, worker_id: str) -> None: ...

    @abstractmethod
    async def list_workers(self) -> list[dict]: ...

    # ------------------------------------------------------------------
    # Cron coordination
    # ------------------------------------------------------------------

    async def acquire_cron_lock(self, key: str, ttl: float = CRON_LOCK_TTL) -> bool:
        """Try to claim one cron occurrence fleet-wide.

        Returns ``True`` for exactly one caller per *key*; every other worker
        gets ``False`` and skips that occurrence. The default implementation
        always returns ``True``, which is only correct with a single worker —
        every bundled broker overrides it.
        """
        logger.warning(
            "%s does not implement acquire_cron_lock(); cron tasks will fire once "
            "per worker. Override it to run more than one worker.",
            type(self).__name__,
        )
        return True

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    @abstractmethod
    async def purge(
        self,
        older_than: float,
        statuses: Collection[JobStatus] | None = None,
    ) -> int:
        """Delete finished jobs older than *older_than* seconds.

        *statuses* defaults to every terminal status. Returns the number of
        jobs removed.
        """
        ...

    # ------------------------------------------------------------------
    # Shared helpers for implementations
    # ------------------------------------------------------------------

    @staticmethod
    def _purge_cutoff(older_than: float) -> datetime:
        return datetime.now(UTC) - timedelta(seconds=older_than)

    @staticmethod
    def _purge_statuses(statuses: Collection[JobStatus] | None) -> list[str]:
        selected = TERMINAL_STATUSES if statuses is None else statuses
        return [JobStatus(s).value for s in selected]

    async def replay_dead_job(self, job_id: str) -> bool:
        """Re-enqueue a dead job as pending with retries reset. Returns True if replayed."""
        job = await self.get_job(job_id)
        if job is None or job.status != JobStatus.dead:
            return False
        job.reset_for_replay()
        job.status = JobStatus.pending
        await self.update_job(job)
        await self.enqueue(job)
        return True

    async def _check_dependencies(self, job: Job) -> None:
        """Set job status to waiting if any dependency is not yet completed.

        Mutates *job* in-place. Call this at the start of enqueue() before
        persisting the job.
        """
        if not job.depends_on:
            return
        if not await self._all_deps_completed(job.depends_on):
            job.status = JobStatus.waiting

    async def _all_deps_completed(self, dep_ids: Collection[str]) -> bool:
        """Return True when every id in *dep_ids* names a completed job.

        Backends with a cheaper bulk query override this.
        """
        for dep_id in dep_ids:
            dep = await self.get_job(dep_id)
            if dep is None or dep.status != JobStatus.completed:
                return False
        return True
