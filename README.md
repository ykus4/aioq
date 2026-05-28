# aioq

Async job queue for Python with a built-in real-time dashboard.
Inspired by [arq](https://github.com/python-arq/arq), built for multiple backends and better observability.

## Features

- **Decorator-based API** — `@app.task(...)` and `@app.cron(...)`
- **Multiple backends** — Redis now, PostgreSQL (SKIP LOCKED), MySQL coming
- **Built-in dashboard** — real-time queue stats, job browser, retry/cancel from UI
- **Deferred jobs** — `defer_by=60` or `defer_until=datetime(...)`
- **Retry with delay** — configurable `retries` and `retry_delay`
- **Cron scheduling** — standard cron expressions via `croniter`
- **Job cancellation & retry** — from code or dashboard
- **Result storage** — optional per-task with configurable TTL
- **Graceful shutdown** — drains in-flight jobs on SIGTERM/SIGINT

## Installation

```bash
pip install aioq                    # Redis only
pip install "aioq[postgres]"        # + PostgreSQL
pip install "aioq[cron]"            # + cron scheduling
pip install "aioq[all]"             # everything
```

**Requirements:** Python 3.11+

## Quick start

### 1. Define tasks

```python
# tasks.py
from aioq import Aarq
from aioq.backends import RedisBroker

broker = RedisBroker(url="redis://localhost:6379")
app = Aarq(broker=broker)


@app.task(queue="default", retries=3, retry_delay=10.0)
async def send_email(ctx, to: str, subject: str) -> dict:
    # ctx contains: worker_id, job_id, broker
    print(f"Sending email to {to}")
    return {"status": "sent"}


@app.task(queue="default", save_result=True, result_ttl=3600)
async def heavy_computation(ctx, n: int) -> int:
    return n * n
```

### 2. Enqueue jobs

```python
import asyncio
from tasks import app, broker

async def main():
    async with broker:
        # Basic enqueue
        job = await send_email.enqueue(to="user@example.com", subject="Hello")
        print(f"Enqueued: {job.id}")

        # Deferred — runs after 60 seconds
        await send_email.enqueue(to="user@example.com", defer_by=60)

        # Deferred — runs at a specific time
        from datetime import datetime
        await send_email.enqueue(
            to="user@example.com",
            defer_until=datetime(2026, 1, 1, 9, 0),
        )

        # With result storage
        job = await heavy_computation.enqueue(n=42)
        # fetch result later: await broker.get_job(job.id)

asyncio.run(main())
```

### 3. Run a worker

```bash
aioq worker tasks:app
```

Options:

| Flag | Default | Description |
|---|---|---|
| `-q` / `--queue` | `default` | Queue(s) to consume (repeatable) |
| `-c` / `--concurrency` | `10` | Max concurrent jobs |
| `--log-level` | `info` | Log level |

```bash
# Multiple queues, higher concurrency
aioq worker tasks:app -q default -q email -q heavy --concurrency 20
```

### 4. Run the dashboard

```bash
aioq dashboard tasks:app --port 8080
```

Then open [http://localhost:8080](http://localhost:8080).

## Cron scheduling

```python
from aioq.backends import RedisBroker
from aioq import Aarq

app = Aarq(broker=RedisBroker())


@app.cron("*/5 * * * *")          # every 5 minutes
async def cleanup_old_jobs(ctx):
    ...


@app.cron("0 9 * * 1-5", queue="reports")   # weekdays at 9am
async def send_daily_report(ctx):
    ...
```

Cron tasks are fired automatically by the worker — no separate scheduler needed.

## PostgreSQL backend

```python
from aioq import Aarq
from aioq.backends.postgres import PostgresBroker

broker = PostgresBroker(
    dsn="postgresql://user:pass@localhost/mydb",
    min_size=2,
    max_size=10,
)
app = Aarq(broker=broker)
```

Tables (`aioq_jobs`, `aioq_workers`) are created automatically on first connect.
Uses `SELECT ... FOR UPDATE SKIP LOCKED` for safe concurrent dequeue.

## Dashboard

The dashboard is a FastAPI app you can mount into an existing application or run standalone.

```python
# Mount into an existing FastAPI app
from fastapi import FastAPI
from aioq.dashboard import create_dashboard

main_app = FastAPI()
dashboard = create_dashboard(app)
main_app.mount("/aioq", dashboard)
```

### Overview page

- Per-queue job counts broken down by status (pending / running / completed / failed / …)
- Worker list with liveness indicator and last heartbeat
- Updates every 2 seconds via **Server-Sent Events**

### Jobs page

- Filter by queue and status
- Paginated job list with inline error preview for failed jobs
- Failed rows are highlighted in red
- Auto-refreshes every 5 seconds

### Job detail page

- Full job metadata: ID, task name, queue, worker, timing, retry count
- Arguments and result / error displayed as formatted JSON
- **Retry button** — re-enqueues a failed or cancelled job from scratch
- **Cancel button** — cancels a pending or retrying job
- Auto-refreshes every 3 seconds while the job is active

## API reference

### `@app.task(...)`

```python
@app.task(
    queue="default",      # queue name
    retries=0,            # max retry attempts
    retry_delay=5.0,      # seconds between retries
    save_result=False,    # persist return value
    result_ttl=3600,      # seconds to keep result (Redis only)
)
async def my_task(ctx, arg1, arg2):
    ...
```

The decorated function becomes a `TaskDef` with an `.enqueue()` method:

```python
job = await my_task.enqueue(arg1, arg2)
job = await my_task.enqueue(arg1, defer_by=30)          # 30s delay
job = await my_task.enqueue(arg1, defer_until=datetime(...))
```

### `@app.cron(...)`

```python
@app.cron(
    "*/10 * * * *",       # standard cron expression
    queue="default",      # queue to run on
)
async def my_cron(ctx):
    ...
```

### `ctx` object

The first argument of every task/cron function receives a context dict:

| Key | Type | Description |
|---|---|---|
| `worker_id` | `str` | UUID of the executing worker |
| `job_id` | `str` | UUID of the current job (tasks only) |
| `broker` | `BaseBroker` | broker instance for advanced use |

### Job status lifecycle

```
pending → running → completed
                 ↘ failed → (retry) → pending
                          → (retry from UI) → pending
pending → cancelled → (retry from UI) → pending
```

### Broker API

All brokers implement `BaseBroker`:

```python
await broker.enqueue(job)
await broker.get_job(job_id)
await broker.list_jobs(queue=None, status=None, limit=100, offset=0)
await broker.queue_stats()          # {queue: {status: count}}
await broker.cancel_job(job_id)     # True if cancelled
await broker.retry_job(job_id)      # True if re-enqueued
await broker.list_workers()
```

## Extending with a new backend

Subclass `BaseBroker` and implement all abstract methods:

```python
from aioq.backends.base import BaseBroker

class MyBroker(BaseBroker):
    async def connect(self): ...
    async def disconnect(self): ...
    async def enqueue(self, job): ...
    async def dequeue(self, queues, timeout=2.0): ...
    async def ack(self, job): ...
    async def nack(self, job, requeue=False): ...
    async def update_job(self, job): ...
    async def get_job(self, job_id): ...
    async def list_jobs(self, queue=None, status=None, limit=100, offset=0): ...
    async def queue_stats(self): ...
    async def cancel_job(self, job_id): ...
    async def retry_job(self, job_id): ...
    async def register_worker(self, worker_id, queues): ...
    async def heartbeat_worker(self, worker_id): ...
    async def deregister_worker(self, worker_id): ...
    async def list_workers(self): ...
```

## Development

```bash
git clone https://github.com/yourname/aioq
cd aioq
uv venv .venv && source .venv/bin/activate
uv pip install -e ".[dev]"
pytest tests/ -v
```

## Roadmap

- [ ] MySQL backend
- [ ] Job dependencies (run B after A)
- [ ] Dead letter queue
- [ ] Priority queues
- [ ] Prometheus metrics endpoint
- [ ] Batch enqueue
