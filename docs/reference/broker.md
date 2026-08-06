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

### `await broker.enqueue_many(jobs)`

Persist and publish several jobs. Backends that can batch do so in one
round-trip; the default implementation loops over `enqueue()`.

### `await broker.cancel_job(job_id)`

Cancel a `pending`, `waiting` or `retrying` job. Returns `True` if cancelled,
`False` otherwise — a running job cannot be cancelled.

```python
ok = await broker.cancel_job(job_id)
```

### `await broker.retry_job(job_id)`

Reset a `failed` or `cancelled` job to `pending` and re-enqueue it. Returns `True` if retried.

```python
ok = await broker.retry_job(job_id)
```

### `await broker.list_dead_jobs(queue=None)`

Return every job with status `dead`, optionally restricted to one DLQ.

```python
dead = await broker.list_dead_jobs(queue="emails-dlq")
```

### `await broker.replay_dead_job(job_id)`

Reset a `dead` job to `pending` with its retry count cleared and re-enqueue it.
Returns `True` if replayed.

### `await broker.purge(older_than, statuses=None)`

Delete finished job records older than `older_than` seconds, comparing against
`completed_at` (falling back to `enqueued_at`). Returns how many were removed.

`statuses` defaults to every terminal status, so a purge can never remove queued
work.

```python
removed = await broker.purge(older_than=7 * 86400)
removed = await broker.purge(older_than=3600, statuses=[JobStatus.failed])
```

### `await broker.acquire_cron_lock(key, ttl=60)`

Claim one cron occurrence for the whole fleet. Returns `True` for exactly one
caller per `key`; everyone else gets `False` and skips that occurrence.

Every bundled broker implements this. A custom broker that does not override it
inherits a default that always returns `True` — correct with a single worker
only, and it logs a warning.

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

## Implementing a backend

`SQLBroker` (`aioq.backends.sql`) sits between `BaseBroker` and the two SQL
backends and carries the dialect-independent parts: `build_filters()`,
`row_to_job()`, `worker_row_to_dict()`, the JSON-column parsers and the
dependency-resolution algorithm. Subclass it rather than `BaseBroker` if your
backend is a relational table, and implement the three dialect hooks:

| Hook | Purpose |
|---|---|
| `_fetch_waiting_dependents(job_id)` | Waiting jobs that may depend on `job_id` |
| `_count_completed(dep_ids)` | How many of those ids are completed |
| `_mark_waiting_as_pending(job_id)` | Flip a waiting job to pending |

See [Custom Backend](../backends/custom.md) for a full walkthrough.
