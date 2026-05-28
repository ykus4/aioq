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

## Job status lifecycle

```
pending ──► running ──► completed
                    └──► failed ──► (retry) ──► pending
                                └──► (retry from UI/code) ──► pending
pending ──► cancelled ──► (retry from UI/code) ──► pending
```
