# Defining Tasks

Tasks are async functions decorated with `@app.task(...)`. The decorator registers the function in the app and returns a `TaskDef` object that exposes an `.enqueue()` method.

## Basic task

```python
from aioq import Aarq
from aioq.backends import RedisBroker

app = Aarq(broker=RedisBroker())


@app.task(queue="default")
async def my_task(ctx, message: str) -> str:
    print(f"Running: {message}")
    return message.upper()
```

## Task options

```python
@app.task(
    queue="default",      # (str) queue to publish to
    retries=0,            # (int) max retry attempts on failure
    retry_delay=5.0,      # (float) seconds to wait before each retry
    save_result=False,    # (bool) persist return value in the broker
    result_ttl=3600,      # (int) seconds to keep the result (Redis only)
)
async def my_task(ctx, ...):
    ...
```

| Option | Type | Default | Description |
|---|---|---|---|
| `queue` | `str` | `"default"` | Queue name |
| `retries` | `int` | `0` | Max retry attempts |
| `retry_delay` | `float` | `5.0` | Seconds between retries |
| `save_result` | `bool` | `False` | Persist return value |
| `result_ttl` | `int` | `3600` | Result TTL in seconds (Redis) |

## The `ctx` argument

Every task receives a context dict as its first argument:

```python
async def my_task(ctx, arg1, arg2):
    worker_id = ctx["worker_id"]  # str — UUID of the executing worker
    job_id    = ctx["job_id"]     # str — UUID of the current job
    broker    = ctx["broker"]     # BaseBroker — broker instance
```

You can use `ctx["broker"]` to enqueue follow-up jobs from within a task:

```python
@app.task(queue="default")
async def parent_task(ctx, n: int):
    for i in range(n):
        await child_task.enqueue(i)
```

## Retry behaviour

When a task raises an exception and `retries > 0`, the worker:

1. Increments `job.retries`
2. Sets `job.status = "retrying"`
3. Waits `retry_delay` seconds
4. Re-enqueues the job as `pending`

Once `job.retries == job.max_retries` and the task fails again, the job is marked `failed` permanently.

```python
@app.task(queue="default", retries=5, retry_delay=30.0)
async def flaky_api_call(ctx, url: str):
    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
```

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

!!! note
    `result_ttl` is only honoured by `RedisBroker`. `PostgresBroker` stores results indefinitely until the row is deleted.

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
