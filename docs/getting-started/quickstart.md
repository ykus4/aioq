# Quick Start

This guide walks you through defining tasks, enqueueing jobs, running a worker, and opening the dashboard — all in under 5 minutes.

## 1. Start Redis

```bash
docker run -d -p 6379:6379 redis:7
```

!!! tip "No Redis handy?"
    Swap `RedisBroker` for `MemoryBroker` below and skip this step. Everything
    works the same, but the queue lives inside one process — so run the worker
    [embedded](../backends/memory.md#running-everything-in-one-process) rather
    than via `aioq worker`.

## 2. Define tasks

Create `tasks.py`:

```python
from aioq import Aioq
from aioq.backends import RedisBroker

broker = RedisBroker(url="redis://localhost:6379")
app = Aioq(broker=broker)


@app.task(queue="default", retries=3, retry_delay=5.0, timeout=30)
async def add(ctx, a: int, b: int) -> int:
    print(f"[add] {a} + {b} = {a + b}")
    return a + b


@app.task(queue="email", save_result=True)
async def send_email(ctx, to: str, subject: str) -> dict:
    print(f"[send_email] to={to}")
    return {"status": "sent", "to": to}
```

## 3. Enqueue jobs

```python
# enqueue.py
import asyncio
from tasks import broker, add, send_email


async def main():
    async with broker:
        job = await add.enqueue(1, 2)
        print(f"Enqueued add: {job.id}")

        job = await send_email.enqueue(to="user@example.com", subject="Hello!")
        print(f"Enqueued send_email: {job.id}")

        # Deferred — runs after 30 seconds
        job = await add.enqueue(10, 20, defer_by=30)
        print(f"Enqueued deferred add: {job.id}")


asyncio.run(main())
```

```bash
python enqueue.py
```

## 4. Run a worker

```bash
aioq worker tasks:app -q default -q email
```

The worker prints results as jobs complete. Name every queue you want consumed —
without `-q` it only picks up `default`, so the `send_email` job would sit there
untouched.

```bash
aioq worker tasks:app -q default -q email --concurrency 20
```

## 5. Check in from the terminal

```bash
aioq status tasks:app        # queue depth per status, worker health
aioq jobs tasks:app          # the 20 most recent jobs
```

## 6. Open the dashboard

```bash
aioq dashboard tasks:app --port 8080
```

Navigate to [http://localhost:8080](http://localhost:8080) to see:

- Live queue stats updated every 2 seconds
- A list of all jobs with their status
- Individual job detail pages with retry/cancel buttons

## What's next?

- [Defining Tasks](../guide/tasks.md) — timeouts, retries, backoff, DLQs
- [Cron Scheduling](../guide/cron.md) — recurring tasks
- [Lifecycle Hooks](../guide/hooks.md) — logging and error reporting
- [Monitoring](../guide/monitoring.md) — Prometheus metrics
- [CLI](../guide/cli.md) — inspecting and fixing jobs from the terminal
- [Dashboard](../guide/dashboard.md) — mounting into an existing app
- [PostgreSQL backend](../backends/postgres.md) — swap out Redis
- [In-memory backend](../backends/memory.md) — for tests
