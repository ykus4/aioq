# aioq

Async job queue for Python with Redis, PostgreSQL, and MySQL backends, priority queues, job dependencies, dead letter queues, and a built-in real-time dashboard.

**[Documentation](https://ykus4.github.io/aioq/)** · [PyPI](https://pypi.org/project/aioq/)

## Features

- **Multiple backends** — Redis, PostgreSQL, MySQL
- **Priority queues** — high/medium/default priority processed in order
- **Batch enqueue** — submit many jobs in a single round-trip
- **Dead letter queue** — route exhausted jobs to a configurable DLQ
- **Job dependencies** — chain jobs with `depends_on=[job_id, ...]`
- **Deferred jobs** — `defer_by=60` or `defer_until=datetime(...)`
- **Cron scheduling** — standard cron expressions
- **Prometheus metrics** — `/metrics` endpoint for Grafana integration
- **Built-in dashboard** — real-time queue stats, job browser, retry/cancel/replay

## Install

```bash
pip install aioq                     # Redis only
pip install "aioq[postgres]"         # + PostgreSQL
pip install "aioq[mysql]"            # + MySQL
pip install "aioq[prometheus]"       # + Prometheus metrics
pip install "aioq[all]"              # everything
```

## Quick start

**1. Define tasks**

```python
# tasks.py
from aioq import Aarq
from aioq.backends import RedisBroker

broker = RedisBroker(url="redis://localhost:6379")
app = Aarq(broker=broker)

@app.task(queue="default", retries=3, priority=5, dead_letter_queue="dlq")
async def send_email(ctx, to: str, subject: str) -> dict:
    ...
```

**2. Enqueue jobs**

```python
# Enqueue a job
job = await send_email.enqueue(to="user@example.com", subject="Hello")

# With dependencies — job_b runs only after job_a completes
job_a = await send_email.enqueue(to="a@example.com", subject="Hi")
job_b = await send_email.enqueue(to="b@example.com", subject="Hi", depends_on=[job_a.id])

# Deferred
await send_email.enqueue(to="x@example.com", subject="Later", defer_by=60)
```

**3. Run a worker**

```bash
aioq worker tasks:app
```

**4. Open the dashboard**

```bash
aioq dashboard tasks:app --port 8080
```

The dashboard at `http://localhost:8080` shows real-time queue stats, worker liveness, job browser with filtering, and per-job detail with **Retry**, **Cancel**, and **Replay** (dead jobs) actions.

See the **[docs](https://ykus4.github.io/aioq/)** for full usage, backend configuration, and API reference.

## License

MIT
