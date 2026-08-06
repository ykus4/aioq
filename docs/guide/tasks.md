# Defining Tasks

Tasks are async functions decorated with `@app.task(...)`. The decorator registers the function in the app and returns a `TaskDef` object that exposes an `.enqueue()` method.

## Basic task

```python
from aioq import Aioq
from aioq.backends import RedisBroker

app = Aioq(broker=RedisBroker())


@app.task(queue="default")
async def my_task(ctx, message: str) -> str:
    print(f"Running: {message}")
    return message.upper()
```

## Task options

```python
@app.task(
    queue="default",           # (str) queue to publish to
    retries=0,                 # (int) max retry attempts on failure
    retry_delay=5.0,           # (float) seconds to wait before each retry
    retry_backoff=False,       # (bool) grow the delay exponentially
    retry_backoff_max=600.0,   # (float) cap for the exponential delay
    timeout=None,              # (float) cancel and fail the job after N seconds
    save_result=False,         # (bool) persist return value in the broker
    result_ttl=3600,           # (int) seconds to keep the result
    priority=0,                # (int) higher runs first
    dead_letter_queue=None,    # (str) where to park permanently failed jobs
)
async def my_task(ctx, ...):
    ...
```

| Option | Type | Default | Description |
|---|---|---|---|
| `queue` | `str` | `"default"` | Queue name |
| `retries` | `int` | `0` | Max retry attempts |
| `retry_delay` | `float` | `5.0` | Seconds between retries |
| `retry_backoff` | `bool` | `False` | Exponential backoff with jitter |
| `retry_backoff_max` | `float` | `600.0` | Upper bound on the backoff delay |
| `timeout` | `float \| None` | `None` | Per-job execution timeout in seconds |
| `save_result` | `bool` | `False` | Persist return value |
| `result_ttl` | `int` | `3600` | Result TTL in seconds |
| `priority` | `int` | `0` | Dequeue priority |
| `dead_letter_queue` | `str \| None` | `None` | DLQ for exhausted retries |

Task names must be unique within an app — registering two tasks with the same
module and qualified name raises `ValueError`, because workers resolve jobs by
name.

## The `ctx` argument

Every task receives a context dict as its first argument:

```python
async def my_task(ctx, arg1, arg2):
    worker_id = ctx["worker_id"]  # str — UUID of the executing worker
    job_id    = ctx["job_id"]     # str — UUID of the current job
    job       = ctx["job"]        # Job  — the full job record
    broker    = ctx["broker"]     # BaseBroker — broker instance
```

You can use `ctx["broker"]` to enqueue follow-up jobs from within a task:

```python
@app.task(queue="default")
async def parent_task(ctx, n: int):
    for i in range(n):
        await child_task.enqueue(i)
```

## Timeouts

Without a `timeout`, a task that hangs — a socket that never returns, an
infinite loop — occupies one of the worker's concurrency slots forever. Set one
and the worker cancels the coroutine and treats it as a failure:

```python
@app.task(timeout=30, retries=2)
async def call_flaky_api(ctx, url: str):
    async with httpx.AsyncClient() as client:
        return (await client.get(url)).json()
```

A timed-out job raises `aioq.JobTimeoutError` internally, so it retries and
lands in the DLQ exactly like any other error. `JobTimeoutError` subclasses
`TimeoutError`, so you can catch it inside your task if you want to handle the
deadline yourself:

```python
from aioq import JobTimeoutError
```

!!! note
    Cancellation is cooperative. A task blocked in synchronous CPU-bound code
    cannot be interrupted — run that in a thread or process pool.

## Retry behaviour

When a task raises and `retries > 0`, the worker:

1. Increments `job.retries`
2. Sets `job.status = "retrying"`
3. Schedules the job to run again after the retry delay
4. Releases its concurrency slot immediately

The job is *rescheduled*, not slept on, so a long backoff does not tie up worker
capacity. Once `job.retries == job.max_retries` and the task fails again, the
job is marked `failed` — or moved to its `dead_letter_queue` if it has one.

```python
@app.task(queue="default", retries=5, retry_delay=30.0)
async def flaky_api_call(ctx, url: str):
    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
```

### Exponential backoff

A flat `retry_delay` retries every failure at the same cadence, which hammers a
struggling dependency and makes every job that failed together retry together.
`retry_backoff=True` doubles the delay per attempt and applies full jitter:

```python
@app.task(retries=5, retry_delay=2.0, retry_backoff=True, retry_backoff_max=300)
async def call_rate_limited_api(ctx, url: str):
    ...
```

With `retry_delay=2.0`, the delay is drawn uniformly from `[0, 2]`, `[0, 4]`,
`[0, 8]`, `[0, 16]`, `[0, 32]` — capped at `retry_backoff_max`.

## Dead letter queues

Give a task a `dead_letter_queue` and jobs that exhaust their retries are moved
there with status `dead` rather than simply marked `failed`:

```python
@app.task(retries=3, dead_letter_queue="emails-dlq")
async def send_email(ctx, to: str):
    ...
```

Inspect and replay them:

```python
dead = await broker.list_dead_jobs(queue="emails-dlq")
await broker.replay_dead_job(dead[0].id)   # back to pending, retries reset
```

The dashboard exposes the same thing via a **Replay** button, and the CLI via
`aioq retry <app> <job-id>`.

## Result storage

Enable `save_result=True` to persist the return value. Retrieve it via the broker:

```python
@app.task(queue="default", save_result=True, result_ttl=7200)
async def compute(ctx, n: int) -> int:
    return n * n


# After the job completes:
job = await broker.get_job(job_id)
print(job.result)  # 25 (if n=5)
```

`result_ttl` sets how long a saved result is kept. `RedisBroker` expires the
record automatically and cleans up its index entries; the SQL backends keep rows
until you run `broker.purge(...)` or `aioq purge`.

Set `result_ttl=0` to keep results forever.

## Task name

The task name is derived automatically as `{module}.{qualname}`:

```python
# In module "myapp.tasks"
@app.task()
async def send_email(ctx, to: str): ...

# task name: "myapp.tasks.send_email"
print(send_email.name)
```

The worker uses this name to look up the function at execution time, so `tasks.py` must be importable from the worker process.
