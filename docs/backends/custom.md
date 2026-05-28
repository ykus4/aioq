# Custom Backend

You can add support for any storage system by subclassing `BaseBroker` and implementing all abstract methods.

## Implementation template

```python
from aioq.backends.base import BaseBroker
from aioq.models import Job, JobStatus


class MyBroker(BaseBroker):

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open connections / pools."""
        ...

    async def disconnect(self) -> None:
        """Close connections / pools."""
        ...

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    async def enqueue(self, job: Job) -> None:
        """Persist and publish a job."""
        ...

    async def dequeue(self, queues: list[str], timeout: float = 2.0) -> Job | None:
        """
        Block until a job is available on any of `queues`, or until
        `timeout` seconds elapse. Return None on timeout.

        Must be safe for concurrent workers (only one worker receives each job).
        """
        ...

    async def ack(self, job: Job) -> None:
        """Acknowledge successful completion."""
        ...

    async def nack(self, job: Job, requeue: bool = False) -> None:
        """Negative-acknowledge a job. Requeue if `requeue=True`."""
        ...

    async def update_job(self, job: Job) -> None:
        """Persist updated job state (status, result, error, etc.)."""
        ...

    async def get_job(self, job_id: str) -> Job | None:
        """Fetch a single job by ID."""
        ...

    async def list_jobs(
        self,
        queue: str | None = None,
        status: JobStatus | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        """Return jobs filtered by queue/status, ordered by enqueued_at desc."""
        ...

    async def queue_stats(self) -> dict[str, dict[str, int]]:
        """
        Return per-queue status counts.
        Example: {"default": {"pending": 3, "running": 1, "completed": 42}}
        """
        ...

    async def cancel_job(self, job_id: str) -> bool:
        """
        Set a pending/retrying job to cancelled.
        Return True if cancelled, False if the job was in a non-cancellable state.
        """
        ...

    async def retry_job(self, job_id: str) -> bool:
        """
        Reset a failed/cancelled job to pending and re-enqueue it.
        Return True if retried, False if the job was in a non-retryable state.
        """
        ...

    # ------------------------------------------------------------------
    # Workers
    # ------------------------------------------------------------------

    async def register_worker(self, worker_id: str, queues: list[str]) -> None:
        """Register a worker on startup."""
        ...

    async def heartbeat_worker(self, worker_id: str) -> None:
        """Update the worker's last-seen timestamp."""
        ...

    async def deregister_worker(self, worker_id: str) -> None:
        """Remove the worker on shutdown."""
        ...

    async def list_workers(self) -> list[dict]:
        """
        Return a list of worker dicts. Each dict must include at minimum:
          worker_id, queues, registered_at, last_heartbeat, alive
        """
        ...
```

## Registration

Pass your broker to `Aarq` like any other:

```python
from aioq import Aarq

broker = MyBroker(...)
app = Aarq(broker=broker)
```

## Context manager support

`BaseBroker` already implements `__aenter__` / `__aexit__` which call `connect()` and `disconnect()`. You get this for free:

```python
async with MyBroker(...) as broker:
    await broker.enqueue(job)
```

## Tips

- **Dequeue atomicity**: the most important invariant is that `dequeue()` returns a job to at most one worker. Use `SELECT ... FOR UPDATE SKIP LOCKED` (SQL), `BRPOP` (Redis), or an equivalent mechanism.
- **Timeout**: `dequeue()` should block for at most `timeout` seconds and return `None` if nothing arrives. This lets the worker check `_running` regularly for graceful shutdown.
- **Worker liveness**: store `last_heartbeat` and expose an `alive` boolean in `list_workers()` (e.g. `alive = last_heartbeat > now - 30s`). The dashboard uses this.
