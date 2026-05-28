from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import Job, JobStatus


class BaseBroker(ABC):
    """Abstract broker interface. Implement this to add a new backend."""

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def enqueue(self, job: Job) -> None: ...

    async def enqueue_many(self, jobs: list[Job]) -> None:
        """Enqueue multiple jobs. Default: loop over enqueue()."""
        for job in jobs:
            await self.enqueue(job)

    @abstractmethod
    async def dequeue(self, queues: list[str], timeout: float = 2.0) -> Job | None: ...

    async def ack(self, job: Job) -> None:
        """Mark job as acknowledged by persisting its current state."""
        await self.update_job(job)

    async def nack(self, job: Job, requeue: bool = False) -> None:
        """Negative-acknowledge a job. Requeue it as pending if requeue=True."""
        if requeue:
            job.status = JobStatus.pending
        await self.update_job(job)

    @abstractmethod
    async def update_job(self, job: Job) -> None: ...

    @abstractmethod
    async def get_job(self, job_id: str) -> Job | None: ...

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

    @abstractmethod
    async def register_worker(self, worker_id: str, queues: list[str]) -> None: ...

    @abstractmethod
    async def heartbeat_worker(self, worker_id: str) -> None: ...

    @abstractmethod
    async def deregister_worker(self, worker_id: str) -> None: ...

    @abstractmethod
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending/retrying/waiting job. Returns True if cancelled."""
        ...

    @abstractmethod
    async def retry_job(self, job_id: str) -> bool:
        """Re-enqueue a failed or cancelled job as a fresh pending job. Returns True if retried."""
        ...

    @abstractmethod
    async def list_workers(self) -> list[dict]: ...

    async def list_dead_jobs(self, queue: str | None = None) -> list[Job]:
        """Return all jobs with status=dead, optionally filtered by DLQ queue name."""
        return await self.list_jobs(queue=queue, status=JobStatus.dead)

    async def replay_dead_job(self, job_id: str) -> bool:
        """Re-enqueue a dead job as pending with retries reset. Returns True if replayed."""
        job = await self.get_job(job_id)
        if job is None or job.status != JobStatus.dead:
            return False
        job.status = JobStatus.pending
        job.retries = 0
        job.error = None
        job.started_at = None
        job.completed_at = None
        job.worker_id = None
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
        for dep_id in job.depends_on:
            dep = await self.get_job(dep_id)
            if dep is None or dep.status != JobStatus.completed:
                job.status = JobStatus.waiting
                return

    async def __aenter__(self) -> BaseBroker:
        await self.connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self.disconnect()
