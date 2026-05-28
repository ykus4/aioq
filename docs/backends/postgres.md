# PostgreSQL Backend

`PostgresBroker` uses PostgreSQL as a durable job store, backed by `asyncpg` with a connection pool.

## Setup

```bash
pip install "aioq[postgres]"
```

```python
from aioq.backends.postgres import PostgresBroker

broker = PostgresBroker(
    dsn="postgresql://user:password@localhost/mydb",
    min_size=2,
    max_size=10,
)
```

### Constructor options

| Parameter | Default | Description |
|---|---|---|
| `dsn` | — | PostgreSQL DSN string |
| `min_size` | `2` | Minimum connection pool size |
| `max_size` | `10` | Maximum connection pool size |

## Auto-created tables

Tables are created automatically on first `connect()` if they don't exist:

### `aioq_jobs`

| Column | Type | Description |
|---|---|---|
| `id` | `TEXT` | Job UUID (primary key) |
| `task_name` | `TEXT` | Dotted task name |
| `queue` | `TEXT` | Queue name |
| `status` | `TEXT` | Current status |
| `args` | `JSONB` | Positional arguments |
| `kwargs` | `JSONB` | Keyword arguments |
| `retries` | `INT` | Current retry count |
| `max_retries` | `INT` | Max retry attempts |
| `retry_delay` | `FLOAT` | Seconds between retries |
| `enqueued_at` | `TIMESTAMPTZ` | Enqueue time |
| `started_at` | `TIMESTAMPTZ` | Execution start time |
| `completed_at` | `TIMESTAMPTZ` | Execution end time |
| `run_at` | `TIMESTAMPTZ` | Scheduled run time (deferred jobs) |
| `result` | `JSONB` | Return value (if `save_result=True`) |
| `error` | `TEXT` | Exception message |
| `worker_id` | `TEXT` | Worker UUID |
| `save_result` | `BOOLEAN` | Whether to persist result |

Indexes: `(queue, status)`, `(run_at) WHERE run_at IS NOT NULL`

### `aioq_workers`

| Column | Type | Description |
|---|---|---|
| `worker_id` | `TEXT` | Worker UUID (primary key) |
| `queues` | `JSONB` | List of queues this worker consumes |
| `registered_at` | `TIMESTAMPTZ` | Registration time |
| `last_heartbeat` | `TIMESTAMPTZ` | Last heartbeat time |

## Dequeue with SKIP LOCKED

`PostgresBroker.dequeue()` uses `SELECT ... FOR UPDATE SKIP LOCKED` to ensure that concurrent workers never pick the same job:

```sql
UPDATE aioq_jobs
SET status = 'running', started_at = now()
WHERE id = (
    SELECT id FROM aioq_jobs
    WHERE queue = ANY($1)
      AND status = 'pending'
      AND (run_at IS NULL OR run_at <= now())
    ORDER BY enqueued_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *
```

This approach is deadlock-free and requires no external locking mechanism.

## Deferred jobs

Deferred jobs are stored with a `run_at` timestamp. The `SKIP LOCKED` query filters them out until `now() >= run_at`, so no separate promotion step is needed.

## Durability

PostgreSQL provides full ACID guarantees. Jobs are never lost, even if the worker or database restarts mid-execution. A job that was set to `running` when the worker crashed will remain in `running` state — you can detect stale running jobs by checking `started_at` age.

## Migrations

If you update aioq and new columns are added, the `CREATE TABLE IF NOT EXISTS` DDL will not add them automatically. Re-run the init SQL or write a migration:

```sql
-- Example: add a new column
ALTER TABLE aioq_jobs ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 0;
```
