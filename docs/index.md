# aioq

**aioq** is an async job queue for Python with multiple backend support and a built-in real-time dashboard.

Inspired by [arq](https://github.com/python-arq/arq), designed for production use with better observability and pluggable backends.

## Features

- **Decorator-based API** — `@app.task(...)` and `@app.cron(...)`
- **Multiple backends** — Redis (built-in), PostgreSQL (`SKIP LOCKED`), MySQL (`SKIP LOCKED`), extensible via `BaseBroker`
- **Built-in dashboard** — real-time queue stats, job browser, retry/cancel from UI
- **Prometheus metrics** — `/metrics` endpoint for Grafana/Alertmanager integration
- **Priority queues** — per-job priority (0/5/10) processed highest-first
- **Batch enqueue** — `task.enqueue_many(items)` for efficient bulk submission
- **Dead letter queue** — configurable DLQ per task; failed jobs move to `dead` status
- **Job dependencies** — `depends_on=[job_id, ...]` to chain jobs
- **Deferred jobs** — `defer_by=60` or `defer_until=datetime(...)`
- **Retry with delay** — configurable `retries` and `retry_delay`
- **Cron scheduling** — standard cron expressions via `croniter`
- **Job cancellation & retry** — from code or dashboard
- **Result storage** — optional per-task with configurable TTL
- **Graceful shutdown** — drains in-flight jobs on SIGTERM/SIGINT

## Quick Example

```python
from aioq import Aarq
from aioq.backends import RedisBroker

broker = RedisBroker(url="redis://localhost:6379")
app = Aarq(broker=broker)


@app.task(queue="default", retries=3, retry_delay=10.0)
async def send_email(ctx, to: str, subject: str) -> dict:
    print(f"Sending email to {to}")
    return {"status": "sent"}


@app.cron("0 9 * * 1-5", queue="reports")
async def daily_report(ctx):
    print("Sending daily report...")
```

```bash
# Enqueue a job
python -c "
import asyncio
from tasks import app, broker, send_email

async def main():
    async with broker:
        job = await send_email.enqueue(to='user@example.com', subject='Hello')
        print(f'Enqueued: {job.id}')

asyncio.run(main())
"

# Run a worker
aioq worker tasks:app

# Open the dashboard
aioq dashboard tasks:app --port 8080
```

## Installation

```bash
pip install aioq                     # Redis only
pip install "aioq[postgres]"         # + PostgreSQL
pip install "aioq[mysql]"            # + MySQL
pip install "aioq[prometheus]"       # + Prometheus metrics
pip install "aioq[cron]"             # + cron scheduling
pip install "aioq[all]"              # everything
```

**Requirements:** Python 3.11+

## Navigation

- **[Getting Started](getting-started/installation.md)** — install and run your first job in 5 minutes
- **[User Guide](guide/tasks.md)** — in-depth coverage of tasks, workers, cron, and the dashboard
- **[Backends](backends/redis.md)** — backend-specific configuration and internals
- **[API Reference](reference/aarq.md)** — full API documentation
