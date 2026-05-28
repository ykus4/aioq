from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class JobStatus(StrEnum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"
    retrying = "retrying"
    cancelled = "cancelled"
    dead = "dead"


class Job(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_name: str
    queue: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    status: JobStatus = JobStatus.pending
    retries: int = 0
    max_retries: int = 0
    retry_delay: float = 0.0
    enqueued_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    run_at: datetime | None = None  # for deferred jobs
    result: Any = None
    error: str | None = None
    worker_id: str | None = None
    save_result: bool = False
    dead_letter_queue: str | None = None

    def model_dump_json_safe(self) -> dict[str, Any]:
        d = self.model_dump()
        for k in ("enqueued_at", "started_at", "completed_at", "run_at"):
            if d[k] is not None:
                d[k] = d[k].isoformat()
        return d
