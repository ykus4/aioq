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
| `minsize` | `2` | Minimum pool connections |
| `maxsize` | `10` | Maximum pool connections |

## Table schema

`MySQLBroker` auto-creates tables on `connect()`:

```sql
CREATE TABLE IF NOT EXISTS aioq_jobs (
    id           VARCHAR(36) PRIMARY KEY,
    task_name    TEXT NOT NULL,
    queue        VARCHAR(255) NOT NULL DEFAULT 'default',
    status       VARCHAR(32) NOT NULL DEFAULT 'pending',
    args         JSON NOT NULL,
    kwargs       JSON NOT NULL,
    retries      INT NOT NULL DEFAULT 0,
    max_retries  INT NOT NULL DEFAULT 0,
    retry_delay  FLOAT NOT NULL DEFAULT 0,
    enqueued_at  DATETIME(6) NOT NULL,
    started_at   DATETIME(6),
    completed_at DATETIME(6),
    run_at       DATETIME(6),
    result       JSON,
    error        TEXT,
    worker_id    VARCHAR(36),
    priority     INT NOT NULL DEFAULT 0,
    save_result  TINYINT(1) NOT NULL DEFAULT 0,
    depends_on   JSON NOT NULL
);
```

## Dequeuing

`dequeue()` uses a `SELECT ... FOR UPDATE SKIP LOCKED` inside a transaction to guarantee each job is picked up by exactly one worker, even under high concurrency.

## Requirements

- MySQL 8.0+ (for `SKIP LOCKED` support)
- `aiomysql` package (`pip install "aioq[mysql]"`)
