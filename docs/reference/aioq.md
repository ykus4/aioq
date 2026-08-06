# Aioq

`aioq.Aioq` is the central application object. It holds the broker, task registry, and cron list.

## Constructor

```python
from aioq import Aioq
from aioq.backends import RedisBroker

app = Aioq(broker=RedisBroker())
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `broker` | `BaseBroker` | — | Broker instance to use for all operations |
| `dashboard_enabled` | `bool` | `True` | Set to `False` to disable the dashboard |

```python
# Disable the dashboard (e.g. in production)
app = Aioq(broker=RedisBroker(), dashboard_enabled=False)
```

When `dashboard_enabled=False`:
- `aioq dashboard tasks:app` exits with an error
- `create_dashboard(app)` raises `RuntimeError`

## `@app.task(...)`

Register an async function as a task.

```python
@app.task(
    queue: str = "default",
    retries: int = 0,
    retry_delay: float = 5.0,
    retry_backoff: bool = False,
    retry_backoff_max: float = 600.0,
    timeout: float | None = None,
    save_result: bool = False,
    result_ttl: int = 3600,
    priority: int = 0,
    dead_letter_queue: str | None = None,
)
async def my_task(ctx, ...): ...
```

Returns a [`TaskDef`](taskdef.md) instance. Raises `ValueError` if a task with
the same name is already registered.

## `@app.cron(...)`

Register an async function as a recurring cron task.

```python
@app.cron(
    expression: str,       # Standard cron expression
    queue: str = "default",
    name: str | None = None,
)
async def my_cron(ctx): ...
```

Requires `pip install "aioq[cron]"`.

## Hook decorators

Register callbacks that observe every job this app runs. See
[Lifecycle Hooks](../guide/hooks.md).

| Decorator | Callback signature |
|---|---|
| `@app.on_job_start` | `(job)` |
| `@app.on_job_success` | `(job, result)` |
| `@app.on_job_retry` | `(job, exc)` |
| `@app.on_job_failure` | `(job, exc)` |
| `@app.on_job_dead` | `(job, exc)` |

Each returns the function unchanged, so it stays callable. Sync callbacks are
accepted; exceptions raised inside a hook are logged and swallowed.

## `app.get_task(name)`

Look up a registered task by its dotted name.

```python
task_def = app.get_task("myapp.tasks.send_email")
```

Returns `TaskDef | None`.

## `app.task_names`

Property returning a list of all registered task names.

```python
print(app.task_names)
# ['myapp.tasks.send_email', 'myapp.tasks.add']
```

## `app.tasks` / `app.crons`

Copies of the task registry and cron list.

```python
app.tasks  # {"myapp.tasks.send_email": TaskDef, ...}
app.crons  # [CronDef, ...]
```

## `app.broker`

Direct access to the broker instance.

```python
stats = await app.broker.queue_stats()
```

## `Aarq`

`aioq.Aarq` is an alias of `Aioq`, kept so code written against 0.4 keeps
working. New code should use `Aioq`.

```python
from aioq import Aarq, Aioq

assert Aarq is Aioq
```
