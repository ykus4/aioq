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
| `retry_backoff` | `BOOLEAN` | Grow the retry delay exponentially |
| `retry_backoff_max` | `FLOAT` | Cap on the exponential delay |
| `timeout` | `FLOAT` | Execution timeout in seconds |
| `enqueued_at` | `TIMESTAMPTZ` | Enqueue time |
| `started_at` | `TIMESTAMPTZ` | Execution start time |
| `completed_at` | `TIMESTAMPTZ` | Execution end time |
| `run_at` | `TIMESTAMPTZ` | Scheduled run time (deferred jobs) |
| `result` | `JSONB` | Return value (if `save_result=True`) |
| `result_ttl` | `INT` | Seconds to keep a saved result |
| `error` | `TEXT` | Exception message |
| `worker_id` | `TEXT` | Worker UUID |
| `priority` | `INT` | Dequeue priority |
| `save_result` | `BOOLEAN` | Whether to persist result |
| `dead_letter_queue` | `TEXT` | DLQ for exhausted retries |
| `depends_on` | `JSONB` | Job IDs this job waits on |

Indexes: `(queue, status)`, `(run_at) WHERE run_at IS NOT NULL`,
`(queue, priority DESC, enqueued_at) WHERE status IN ('pending','retrying')`,
and a GIN index on `depends_on`.

### `aioq_workers`

| Column | Type | Description |
|---|---|---|
| `worker_id` | `TEXT` | Worker UUID (primary key) |
| `queues` | `JSONB` | List of queues this worker consumes |
| `registered_at` | `TIMESTAMPTZ` | Registration time |
| `last_heartbeat` | `TIMESTAMPTZ` | Last heartbeat time |

### `aioq_cron_locks`

Holds one row per claimed cron occurrence, which is what stops two workers
running the same occurrence.

| Column | Type | Description |
|---|---|---|
| `lock_key` | `TEXT` | `{cron name}@{occurrence timestamp}` (primary key) |
| `acquired_at` | `TIMESTAMPTZ` | When the lock was taken |
| `expires_at` | `TIMESTAMPTZ` | After this, another worker may steal it |

Expired rows are cleaned up by `broker.purge()` / `aioq purge`.

## Dequeue with SKIP LOCKED

`PostgresBroker.dequeue()` uses `SELECT ... FOR UPDATE SKIP LOCKED` to ensure that concurrent workers never pick the same job:

```sql
UPDATE aioq_jobs
SET status = 'running', started_at = now()
WHERE id = (
    SELECT id FROM aioq_jobs
    WHERE queue = ANY($1::text[])
      AND status = ANY($2::text[])          -- 'pending' or 'retrying'
      AND (run_at IS NULL OR run_at <= now())
    ORDER BY priority DESC, enqueued_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *
```

`retrying` is claimable because a retry is the same row rescheduled via `run_at`,
not a new job.

This approach is deadlock-free and requires no external locking mechanism.

## Deferred jobs

Deferred jobs are stored with a `run_at` timestamp. The `SKIP LOCKED` query filters them out until `now() >= run_at`, so no separate promotion step is needed.

## Durability

PostgreSQL provides full ACID guarantees. Jobs are never lost, even if the worker or database restarts mid-execution. A job that was set to `running` when the worker crashed will remain in `running` state — you can detect stale running jobs by checking `started_at` age.

## Retention

Nothing is deleted automatically — finished rows accumulate until you remove
them. Run a purge on a schedule:

```bash
aioq purge myapp.tasks:app --older-than 604800 --yes
```

```python
@app.cron("0 4 * * *")
async def nightly_purge(ctx):
    await ctx["broker"].purge(older_than=7 * 86400)
```

## Migrations

`connect()` applies its own schema upgrades: it runs `CREATE TABLE IF NOT
EXISTS`, then `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for every column added
in a later aioq release. Upgrading the package is enough — no manual migration
step.

!!! note
    That means the connecting role needs `ALTER` on `aioq_jobs` the first time it
    runs after an upgrade. If your production role cannot alter tables, apply the
    DDL from `aioq/backends/postgres.py` (`_INIT_SQL`) yourself. Reading a table
    whose migrations have not run still works — missing columns fall back to the
    model defaults.
