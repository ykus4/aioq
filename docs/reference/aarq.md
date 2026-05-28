# Aarq

`aioq.Aarq` is the central application object. It holds the broker, task registry, and cron list.

## Constructor

```python
from aioq import Aarq
from aioq.backends import RedisBroker

app = Aarq(broker=RedisBroker())
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `broker` | `BaseBroker` | — | Broker instance to use for all operations |
| `dashboard_enabled` | `bool` | `True` | Set to `False` to disable the dashboard |

```python
# Disable the dashboard (e.g. in production)
app = Aarq(broker=RedisBroker(), dashboard_enabled=False)
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
    save_result: bool = False,
    result_ttl: int = 3600,
    priority: int = 0,
    dead_letter_queue: str | None = None,
)
async def my_task(ctx, ...): ...
```

Returns a [`TaskDef`](taskdef.md) instance.

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

## `app.broker`

Direct access to the broker instance.

```python
stats = await app.broker.queue_stats()
```
