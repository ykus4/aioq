# TaskDef

`aioq.task.TaskDef` wraps an async function registered via `@app.task(...)`. It is the object you interact with to enqueue jobs.

## Attributes

| Attribute | Type | Description |
|---|---|---|
| `name` | `str` | Dotted task name (`module.qualname`) |
| `queue` | `str` | Default queue |
| `retries` | `int` | Max retry attempts |
| `retry_delay` | `float` | Seconds between retries |
| `retry_backoff` | `bool` | Grow the retry delay exponentially |
| `retry_backoff_max` | `float` | Cap on the exponential delay |
| `timeout` | `float \| None` | Execution timeout in seconds |
| `save_result` | `bool` | Whether to persist result |
| `result_ttl` | `int` | Result TTL in seconds |
| `priority` | `int` | Default dequeue priority |
| `dead_letter_queue` | `str \| None` | DLQ for exhausted retries |
| `fn` | `Callable` | The underlying async function |
| `app` | `Aioq` | Parent application |

## `await task.enqueue(...)`

Push a job onto the queue and return the `Job`.

```python
job = await my_task.enqueue(arg1, arg2, kwarg=value)
```

### Enqueue options

These keyword arguments are consumed by `enqueue()` and **not** passed to the task function:

| Argument | Type | Description |
|---|---|---|
| `defer_by` | `float \| None` | Delay in seconds before the job runs |
| `defer_until` | `datetime \| None` | Absolute time to run the job |
| `priority` | `int \| None` | Override the task's default priority |
| `depends_on` | `list[str] \| None` | Job IDs that must complete first |

Passing both `defer_by` and `defer_until` raises `ValueError`.

All other positional and keyword arguments are forwarded to the task function.

```python
# Run now
job = await send_email.enqueue(to="a@b.com", subject="Hi")

# Run after 5 minutes
job = await send_email.enqueue(to="a@b.com", defer_by=300)

# Run at a specific time
from datetime import datetime
job = await send_email.enqueue(
    to="a@b.com",
    defer_until=datetime(2026, 6, 1, 9, 0),
)
```

## `await task.enqueue_many(items, ...)`

Enqueue many calls in one broker round-trip. Each item is either a kwargs dict or
a positional-args tuple; the two can be mixed. Returns the list of `Job` objects.

```python
jobs = await send_email.enqueue_many([
    {"to": "a@b.com"},
    {"to": "c@d.com"},
])

jobs = await add.enqueue_many([(1, 2), (3, 4)])
```

The keyword options `defer_by`, `defer_until`, `priority` and `depends_on` apply
to every job in the batch.

## `await task(ctx, ...)`

Directly call the underlying function. Used internally by the worker.

```python
result = await my_task(ctx, arg1, arg2)
```
