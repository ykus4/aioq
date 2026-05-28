# Enqueueing Jobs

Once a task is defined, call `.enqueue()` on the decorated function to push a job onto the queue.

## Basic enqueue

```python
job = await my_task.enqueue(arg1, arg2, kwarg=value)
print(job.id)      # UUID string
print(job.status)  # "pending"
```

`.enqueue()` returns a `Job` object immediately after writing to the broker.

## Deferred jobs

Run a job after a fixed delay:

```python
# Run after 60 seconds
job = await send_email.enqueue(to="user@example.com", defer_by=60)
```

Run a job at a specific time:

```python
from datetime import datetime

job = await send_email.enqueue(
    to="user@example.com",
    defer_until=datetime(2026, 1, 1, 9, 0),
)
```

!!! note
    Deferred jobs are stored in a separate sorted set (Redis) or filtered by `run_at` (PostgreSQL). The worker promotes them to the active queue once their scheduled time passes.

## Enqueue from within a task

```python
@app.task(queue="pipeline")
async def step_one(ctx, data: dict):
    result = process(data)
    await step_two.enqueue(result)
```

The broker is available as `ctx["broker"]` if you need it directly.

## Checking job status

```python
async with broker:
    job = await my_task.enqueue(value=42)

    # Poll for completion
    import asyncio
    while True:
        job = await broker.get_job(job.id)
        if job.status in ("completed", "failed", "cancelled"):
            break
        await asyncio.sleep(1)

    print(job.status, job.result, job.error)
```

## Cancelling a pending job

```python
cancelled = await broker.cancel_job(job.id)
# True if the job was pending/retrying and is now cancelled
# False if the job was already running/completed
```

## Retrying a failed job

```python
retried = await broker.retry_job(job.id)
# True if the job was failed/cancelled and has been re-enqueued as pending
```

## Batch enqueue

Enqueue multiple calls to the same task in a single round-trip:

```python
jobs = await process_record.enqueue_many([
    {"record_id": 1},
    {"record_id": 2},
    {"record_id": 3},
])
print(len(jobs))  # 3
```

Items can be dicts (kwargs) or tuples (positional args):

```python
jobs = await send_email.enqueue_many([
    ("user1@example.com", "Hello"),
    ("user2@example.com", "Hello"),
])
```

Supports `defer_by` and `priority` overrides:

```python
jobs = await process_record.enqueue_many(items, defer_by=30, priority=10)
```

## Priority

Jobs are assigned a priority of `0` (default), `5` (medium), or `10` (high). Workers dequeue higher-priority jobs first.

Set priority at the task level:

```python
@app.task(queue="default", priority=10)
async def urgent_task(ctx, data):
    ...
```

Or per-enqueue:

```python
job = await my_task.enqueue(data, priority=5)
```

## Dead letter queue

When a job exhausts all retries it normally moves to `failed`. With a DLQ configured it moves to `dead` in the DLQ queue instead:

```python
@app.task(queue="default", retries=3, dead_letter_queue="dlq")
async def risky_task(ctx, data):
    ...
```

Inspect and replay dead jobs:

```python
dead_jobs = await broker.list_dead_jobs()
replayed = await broker.replay_dead_job(job.id)  # re-enqueues as pending
```

## Job dependencies

Use `depends_on` to ensure a job runs only after other jobs complete:

```python
job_a = await step_a.enqueue(data)
job_b = await step_b.enqueue(data, depends_on=[job_a.id])
# job_b starts in `waiting` state and is promoted to `pending` once job_a completes
```

Chaining multiple dependencies:

```python
jobs = await asyncio.gather(
    fetch_data.enqueue(source="api"),
    fetch_data.enqueue(source="db"),
)
job_ids = [j.id for j in jobs]
result_job = await merge_results.enqueue(depends_on=job_ids)
```

## Job status lifecycle

```
pending ──► running ──► completed
                    └──► failed ──► (retry) ──► pending
                                └──► dead (DLQ) ──► (replay) ──► pending
                                └──► (retry from UI/code) ──► pending
pending ──► cancelled ──► (retry from UI/code) ──► pending
waiting ──► pending (when all dependencies complete) ──► running
```
