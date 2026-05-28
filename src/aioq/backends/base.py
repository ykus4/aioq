from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator

from ..models import Job, JobStatus


class BaseBroker(ABC):
    """Abstract broker interface. Implement this to add a new backend."""

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def enqueue(self, job: Job) -> None: ...

    @abstractmethod
    async def dequeue(self, queues: list[str], timeout: float = 2.0) -> Job | None: ...

    @abstractmethod
    async def ack(self, job: Job) -> None: ...

    @abstractmethod
    async def nack(self, job: Job, requeue: bool = False) -> None: ...

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
        """Cancel a pending/retrying job. Returns True if cancelled."""
        ...

    @abstractmethod
    async def retry_job(self, job_id: str) -> bool:
        """Re-enqueue a failed or cancelled job as a fresh pending job. Returns True if retried."""
        ...

    @abstractmethod
    async def list_workers(self) -> list[dict]: ...

    async def __aenter__(self) -> "BaseBroker":
        await self.connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self.disconnect()
