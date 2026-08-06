# aioq

**aioq** is an async job queue for Python with multiple backend support and a built-in real-time dashboard.

Inspired by [arq](https://github.com/python-arq/arq), designed for production use with better observability and pluggable backends.

## Features

- **Decorator-based API** — `@app.task(...)` and `@app.cron(...)`
- **Multiple backends** — Redis, PostgreSQL and MySQL (`SKIP LOCKED`), in-memory for tests, extensible via `BaseBroker`
- **Built-in dashboard** — real-time queue stats, job browser, retry/cancel/replay from UI
- **Prometheus metrics** — queue gauges from the dashboard, execution counters and duration histograms from each worker
- **Job timeouts** — `timeout=30` cancels a hung job instead of letting it hold a slot forever
- **Retries with backoff** — flat or exponential-with-jitter; a waiting retry never occupies worker capacity
- **Dead letter queue** — configurable DLQ per task, with replay
- **Priority queues** — per-job priority (0/5/10) processed highest-first
- **Job dependencies** — `depends_on=[job_id, ...]` to chain jobs
- **Deferred jobs** — `defer_by=60` or `defer_until=datetime(...)`
- **Batch enqueue** — `task.enqueue_many(items)` for efficient bulk submission
- **Cron scheduling** — standard cron expressions, fired exactly once across the whole worker fleet
- **Lifecycle hooks** — `@app.on_job_success` / `on_job_failure` / … for logging, tracing, error reporting
- **CLI** — `aioq status`, `jobs`, `show`, `retry`, `cancel`, `purge`
- **Result storage** — optional per-task with configurable TTL
- **Graceful shutdown** — drains in-flight jobs on SIGTERM/SIGINT

## Quick Example

```python
from aioq import Aioq
from aioq.backends import RedisBroker

broker = RedisBroker(url="redis://localhost:6379")
app = Aioq(broker=broker)


@app.task(queue="default", retries=3, retry_delay=10.0, retry_backoff=True, timeout=30)
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

# Or check in from the terminal
aioq status tasks:app
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
- **[API Reference](reference/aioq.md)** — full API documentation
