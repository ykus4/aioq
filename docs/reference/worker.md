# Worker

`aioq.worker.Worker` dequeues and executes jobs from one or more queues.

## Constructor

```python
from aioq.worker import Worker

worker = Worker(
    app=app,
    queues=["default", "email"],
    concurrency=10,
    heartbeat_interval=10.0,
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `app` | `Aarq` | — | Application instance |
| `queues` | `list[str] \| None` | `["default"]` | Queues to consume |
| `concurrency` | `int` | `10` | Max concurrent jobs |
| `heartbeat_interval` | `float` | `10.0` | Seconds between heartbeats |

## `await worker.run()`

Start the worker. This method blocks until the worker stops.

```python
import asyncio

asyncio.run(worker.run())
```

**Startup sequence:**

1. `broker.connect()`
2. `broker.register_worker(worker_id, queues)`
3. Install SIGTERM / SIGINT handlers
4. Start heartbeat loop
5. Start cron loop (if any cron tasks are registered)
6. Enter the main dequeue loop

**Shutdown sequence (on SIGTERM/SIGINT):**

1. Stop the dequeue loop
2. Cancel heartbeat and cron tasks
3. Wait for all in-flight jobs to finish (`asyncio.gather`)
4. `broker.deregister_worker(worker_id)`
5. `broker.disconnect()`

## `worker.worker_id`

A UUID string assigned on construction. Unique per process.

```python
print(worker.worker_id)  # "3d6f4455-..."
```

## Internal loops

### Heartbeat loop

Runs every `heartbeat_interval` seconds, calling `broker.heartbeat_worker()`. Failures are silently suppressed so a temporary broker outage doesn't crash the worker.

### Cron loop

Runs every second, checking whether any cron task is due. When a task fires, it runs as an `asyncio.Task` and its next run time is scheduled immediately.
