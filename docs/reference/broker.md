# BaseBroker

`aioq.backends.base.BaseBroker` is the abstract interface that all backends implement.

## Context manager

```python
async with broker:
    await broker.enqueue(job)
```

Calls `connect()` on enter and `disconnect()` on exit.

## Methods

### `await broker.connect()`

Open connections or connection pools. Called automatically by the context manager and by `Worker.run()`.

### `await broker.disconnect()`

Close connections. Called automatically on context manager exit and after graceful worker shutdown.

### `await broker.enqueue(job)`

Persist and publish a `Job`. The job is immediately available to workers unless it has a future `run_at`.

```python
job = Job(task_name="tasks.add", queue="default", kwargs={"a": 1, "b": 2})
await broker.enqueue(job)
```

### `await broker.dequeue(queues, timeout=2.0)`

Block until a job is available on any of `queues`, returning it — or return `None` after `timeout` seconds.

```python
job = await broker.dequeue(["default", "email"], timeout=5.0)
```

### `await broker.update_job(job)`

Persist the current state of a `Job` (status, result, error, etc.).

### `await broker.get_job(job_id)`

Fetch a single job by ID. Returns `Job | None`.

```python
job = await broker.get_job("550e8400-e29b-41d4-a716-446655440000")
```

### `await broker.list_jobs(queue, status, limit, offset)`

Return a list of jobs filtered by queue and/or status, ordered by `enqueued_at` descending.

```python
failed_jobs = await broker.list_jobs(
    queue="default",
    status=JobStatus.failed,
    limit=50,
    offset=0,
)
```

### `await broker.queue_stats()`

Return per-queue status counts as a nested dict.

```python
stats = await broker.queue_stats()
# {"default": {"pending": 3, "running": 1}, "email": {"completed": 42}}
```

### `await broker.cancel_job(job_id)`

Cancel a `pending` or `retrying` job. Returns `True` if cancelled, `False` otherwise.

```python
ok = await broker.cancel_job(job_id)
```

### `await broker.retry_job(job_id)`

Reset a `failed` or `cancelled` job to `pending` and re-enqueue it. Returns `True` if retried.

```python
ok = await broker.retry_job(job_id)
```

### `await broker.register_worker(worker_id, queues)`

Register a worker. Called by `Worker.run()` on startup.

### `await broker.heartbeat_worker(worker_id)`

Update the worker's last-seen timestamp.

### `await broker.deregister_worker(worker_id)`

Remove the worker. Called by `Worker.run()` on shutdown.

### `await broker.list_workers()`

Return a list of worker info dicts. Each dict includes:

| Key | Type | Description |
|---|---|---|
| `worker_id` | `str` | Worker UUID |
| `queues` | `list[str]` | Queues this worker consumes |
| `registered_at` | `str` | ISO 8601 timestamp |
| `last_heartbeat` | `str` | ISO 8601 timestamp |
| `alive` | `bool` | True if heartbeat is recent |
