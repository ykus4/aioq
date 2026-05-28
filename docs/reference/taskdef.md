# TaskDef

`aioq.task.TaskDef` wraps an async function registered via `@app.task(...)`. It is the object you interact with to enqueue jobs.

## Attributes

| Attribute | Type | Description |
|---|---|---|
| `name` | `str` | Dotted task name (`module.qualname`) |
| `queue` | `str` | Default queue |
| `retries` | `int` | Max retry attempts |
| `retry_delay` | `float` | Seconds between retries |
| `save_result` | `bool` | Whether to persist result |
| `result_ttl` | `int` | Result TTL in seconds |
| `fn` | `Callable` | The underlying async function |
| `app` | `Aarq` | Parent application |

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

## `await task(ctx, ...)`

Directly call the underlying function. Used internally by the worker.

```python
result = await my_task(ctx, arg1, arg2)
```
