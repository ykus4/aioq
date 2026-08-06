# Cron Scheduling

aioq supports recurring tasks via standard cron expressions. Cron tasks are fired automatically by the worker — no separate scheduler process is needed.

## Installation

Cron requires the `croniter` package:

```bash
pip install "aioq[cron]"
```

## Defining cron tasks

```python
from aioq import Aioq
from aioq.backends import RedisBroker

app = Aioq(broker=RedisBroker())


@app.cron("*/5 * * * *")             # every 5 minutes
async def cleanup_old_jobs(ctx):
    broker = ctx["broker"]
    # ... cleanup logic


@app.cron("0 9 * * 1-5", queue="reports")  # weekdays at 9 AM
async def send_daily_report(ctx):
    # ... report logic


@app.cron("0 0 1 * *")               # first day of every month
async def monthly_summary(ctx):
    # ... summary logic
```

## Cron expression syntax

Cron expressions follow the standard 5-field format:

```
┌─────── minute      (0–59)
│ ┌───── hour        (0–23)
│ │ ┌─── day of month (1–31)
│ │ │ ┌─ month       (1–12)
│ │ │ │ ┌ day of week (0–7, 0 and 7 = Sunday)
│ │ │ │ │
* * * * *
```

Common examples:

| Expression | Meaning |
|---|---|
| `* * * * *` | Every minute |
| `*/5 * * * *` | Every 5 minutes |
| `0 * * * *` | Every hour |
| `0 9 * * *` | Every day at 9 AM |
| `0 9 * * 1-5` | Weekdays at 9 AM |
| `0 0 * * 0` | Every Sunday at midnight |
| `0 0 1 * *` | First day of every month |

## `ctx` in cron tasks

Cron tasks receive a context dict with `worker_id` and `broker`, but **not** `job_id` (cron tasks don't create `Job` records):

```python
async def my_cron(ctx):
    worker_id = ctx["worker_id"]
    broker = ctx["broker"]
```

## How it works

The worker runs a background loop that checks the schedule every second. When a cron task is due:

1. The worker tries to claim that occurrence with a broker-held lock
2. If it wins, the task is called in the worker's event loop as an `asyncio.Task`
3. If it loses, another worker is already running it, and this worker skips it
4. The next run time is computed immediately after firing

Every worker derives the same occurrence timestamps from the same cron
expression, so they all compete for the same lock key — which is what makes an
occurrence run **exactly once across the whole fleet**, however many workers are
up. The lock is held for 60 seconds by default, comfortably longer than the
one-second scheduler tick.

```python
# Safe to scale out: this sends one report, not one per worker.
@app.cron("0 9 * * 1-5", queue="reports")
async def send_daily_report(ctx): ...
```

!!! note "Custom brokers"
    All bundled brokers implement `acquire_cron_lock()`. A custom `BaseBroker`
    subclass that does not override it falls back to always granting the lock —
    correct with a single worker, but it logs a warning, because with several
    workers each one would fire the cron.

Cron tasks execute directly rather than creating `Job` records, so they do not
appear in the dashboard's job list. Enqueue a regular task from inside the cron
function if you want that visibility — and the retries and DLQ that come with it:

```python
@app.cron("0 9 * * 1-5")
async def schedule_report(ctx):
    await build_report.enqueue()  # a normal task, fully tracked
```

## Error handling

Cron task failures are logged but do not affect the worker's main loop:

```
2026-01-01 09:00:01 ERROR aioq.worker: Cron task myapp.tasks.send_daily_report failed: connection refused
```

The next scheduled run will still fire normally.
