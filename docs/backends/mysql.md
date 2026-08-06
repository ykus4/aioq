# MySQL Backend

`MySQLBroker` uses `aiomysql` with `SKIP LOCKED` for concurrent job dequeuing.

## Setup

```bash
pip install "aioq[mysql]"
```

```python
from aioq.backends import MySQLBroker

broker = MySQLBroker(
    host="localhost",
    port=3306,
    user="myuser",
    password="mypassword",
    db="mydb",
)
```

### Constructor options

| Parameter | Default | Description |
|---|---|---|
| `host` | `"localhost"` | MySQL host |
| `port` | `3306` | MySQL port |
| `user` | `"root"` | Database user |
| `password` | `""` | Database password |
| `db` | `"aioq"` | Database name |
| `min_size` | `2` | Minimum pool connections |
| `max_size` | `10` | Maximum pool connections |

## Table schema

`MySQLBroker` auto-creates tables on `connect()`:

```sql
CREATE TABLE IF NOT EXISTS aioq_jobs (
    id                VARCHAR(64) PRIMARY KEY,
    task_name         VARCHAR(255) NOT NULL,
    queue             VARCHAR(191) NOT NULL DEFAULT 'default',
    status            VARCHAR(32) NOT NULL DEFAULT 'pending',
    args              JSON NOT NULL,
    kwargs            JSON NOT NULL,
    retries           INT NOT NULL DEFAULT 0,
    max_retries       INT NOT NULL DEFAULT 0,
    retry_delay       FLOAT NOT NULL DEFAULT 0,
    retry_backoff     TINYINT(1) NOT NULL DEFAULT 0,
    retry_backoff_max FLOAT NOT NULL DEFAULT 600,
    timeout           FLOAT NULL,
    enqueued_at       DATETIME(6) NOT NULL,
    started_at        DATETIME(6) NULL,
    completed_at      DATETIME(6) NULL,
    run_at            DATETIME(6) NULL,
    result            JSON NULL,
    result_ttl        INT NOT NULL DEFAULT 3600,
    error             TEXT NULL,
    worker_id         VARCHAR(64) NULL,
    priority          INT NOT NULL DEFAULT 0,
    save_result       TINYINT(1) NOT NULL DEFAULT 0,
    dead_letter_queue VARCHAR(191) NULL,
    depends_on        JSON NULL
);
```

Plus `aioq_workers` and `aioq_cron_locks`, the latter being what keeps a cron
occurrence from firing on more than one worker.

## Dequeuing

`dequeue()` uses a `SELECT ... FOR UPDATE SKIP LOCKED` inside a transaction to
guarantee each job is picked up by exactly one worker, even under high
concurrency. It claims both `pending` and `retrying` jobs, because a retry is the
same row rescheduled via `run_at` rather than a new job.

## Time zones

All timestamps are stored as naive UTC, and every comparison in the generated SQL
uses `UTC_TIMESTAMP()` rather than `NOW()`. `NOW()` follows the connection's
`time_zone`, which would offset every deferred job and heartbeat by the session
offset.

## Migrations

MySQL — unlike MariaDB — has no `ADD COLUMN IF NOT EXISTS` or `CREATE INDEX IF
NOT EXISTS`. `connect()` therefore queries `information_schema` and only issues
the `ALTER TABLE` / `CREATE INDEX` it actually needs, so upgrading the package is
enough.

The connecting user needs `ALTER` on `aioq_jobs` for the first connection after
an upgrade, plus `SELECT` on `information_schema`. Reading a table whose
migrations have not run still works — missing columns fall back to the model
defaults.

## Retention

Nothing is deleted automatically. Run `aioq purge` (or `broker.purge()`) on a
schedule to drop old finished rows and expired cron locks.

## Requirements

- MySQL 8.0.13+ (`SKIP LOCKED`, plus expression defaults for JSON columns)
- `aiomysql` package (`pip install "aioq[mysql]"`)
