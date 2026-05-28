# Job & JobStatus

## JobStatus

`aioq.models.JobStatus` is a `StrEnum` representing the lifecycle state of a job.

| Value | Description |
|---|---|
| `pending` | Waiting to be picked up by a worker |
| `waiting` | Blocked on one or more dependencies |
| `running` | Currently executing |
| `completed` | Finished successfully |
| `failed` | Raised an exception, no retries remaining |
| `retrying` | Failed, waiting to be re-enqueued |
| `cancelled` | Cancelled before execution |
| `dead` | Exhausted retries and moved to dead letter queue |

### State transitions

```
pending ──► running ──► completed
                    └──► failed ──► (retry) ──► pending
                                └──► dead (DLQ) ──► (replay) ──► pending
                                └──► (retry from UI) ──► pending
pending ──► cancelled ──► (retry from UI) ──► pending
waiting ──► pending (when all dependencies complete) ──► running
```

## Job

`aioq.models.Job` is a Pydantic model representing a job record.

```python
class Job(BaseModel):
    id: str                       # UUID (auto-generated)
    task_name: str                # Dotted task name
    queue: str                    # Queue name
    args: list[Any]               # Positional arguments
    kwargs: dict[str, Any]        # Keyword arguments
    priority: int                 # Priority tier: 0, 5, or 10
    status: JobStatus             # Current status
    retries: int                  # Current retry count
    max_retries: int              # Max retry attempts
    retry_delay: float            # Seconds between retries
    enqueued_at: datetime         # When the job was created
    started_at: datetime | None
    completed_at: datetime | None
    run_at: datetime | None       # Scheduled time (deferred jobs)
    result: Any                   # Return value (if save_result=True)
    error: str | None             # Exception message
    worker_id: str | None         # Worker that executed the job
    save_result: bool             # Whether to persist result
    dead_letter_queue: str | None # DLQ queue name (if configured)
    depends_on: list[str]         # Job IDs this job depends on
```

### `model_dump_json_safe()`

Returns a dict suitable for JSON serialisation — datetime fields are converted to ISO 8601 strings.

```python
job = Job(task_name="tasks.add", queue="default")
d = job.model_dump_json_safe()
# d["enqueued_at"] == "2026-01-01T00:00:00"
```
