from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from .constants import DEFAULT_RESULT_TTL, DEFAULT_RETRY_BACKOFF_MAX


class JobStatus(StrEnum):
    pending = "pending"
    waiting = "waiting"
    running = "running"
    completed = "completed"
    failed = "failed"
    retrying = "retrying"
    cancelled = "cancelled"
    dead = "dead"


#: Statuses a job never leaves on its own.
TERMINAL_STATUSES = frozenset(
    {JobStatus.completed, JobStatus.failed, JobStatus.cancelled, JobStatus.dead}
)

#: Statuses from which :meth:`BaseBroker.cancel_job` will cancel a job.
CANCELLABLE_STATUSES = frozenset({JobStatus.pending, JobStatus.retrying, JobStatus.waiting})

#: Statuses from which :meth:`BaseBroker.retry_job` will re-enqueue a job.
RETRIABLE_STATUSES = frozenset({JobStatus.failed, JobStatus.cancelled})


class Job(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_name: str
    queue: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    priority: int = 0
    status: JobStatus = JobStatus.pending
    retries: int = 0
    max_retries: int = 0
    retry_delay: float = 0.0
    retry_backoff: bool = False
    retry_backoff_max: float = DEFAULT_RETRY_BACKOFF_MAX
    timeout: float | None = None
    enqueued_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    run_at: datetime | None = None  # for deferred jobs
    result: Any = None
    result_ttl: int = DEFAULT_RESULT_TTL
    error: str | None = None
    worker_id: str | None = None
    save_result: bool = False
    dead_letter_queue: str | None = None
    depends_on: list[str] = Field(default_factory=list)

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @property
    def duration(self) -> float | None:
        """Wall-clock seconds spent running, or ``None`` if not finished."""
        if self.started_at is None or self.completed_at is None:
            return None
        return (self.completed_at - self.started_at).total_seconds()

    def model_dump_json_safe(self) -> dict[str, Any]:
        """Dump to a dict containing only JSON-native types.

        Unlike a plain ``model_dump()`` this converts datetimes (and anything
        else non-native that ended up in ``result``) so the output can be
        handed straight to ``json.dumps``.
        """
        return self.model_dump(mode="json", warnings=False)

    def reset_for_replay(self) -> None:
        """Clear per-attempt state so this job can run again from scratch.

        Used by ``retry_job`` / ``replay_dead_job``. Does not change ``status``
        — the caller decides that.
        """
        self.retries = 0
        self.error = None
        self.result = None
        self.started_at = None
        self.completed_at = None
        self.worker_id = None
        self.run_at = None
