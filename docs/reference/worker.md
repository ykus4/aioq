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
    metrics=None,
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `app` | `Aioq` | — | Application instance |
| `queues` | `list[str] \| None` | `["default"]` | Queues to consume |
| `concurrency` | `int` | `10` | Max concurrent jobs |
| `heartbeat_interval` | `float` | `10.0` | Seconds between heartbeats |
| `metrics` | `WorkerMetrics \| None` | `None` | Records per-job counters — see [Monitoring](../guide/monitoring.md) |

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

The worker takes a concurrency slot *before* claiming a job, so it never holds a
claimed job while waiting for capacity. If `dequeue()` raises — a broker outage —
the error is logged, the slot released, and the loop retries after a short
backoff rather than crashing the process.

**Shutdown sequence (on SIGTERM/SIGINT):**

1. Stop the dequeue loop
2. Cancel heartbeat and cron tasks
3. Wait for all in-flight jobs to finish (`asyncio.gather`)
4. `broker.deregister_worker(worker_id)`
5. `broker.disconnect()`

## `worker.stop()`

Ask the worker to finish its in-flight jobs and shut down — the programmatic
equivalent of SIGTERM. Safe to call before `run()`, and safe to call twice.

```python
runner = asyncio.create_task(worker.run())
...
worker.stop()
await runner  # returns once in-flight jobs have drained
```

Signal handlers are installed on a best-effort basis: where
`loop.add_signal_handler` is unavailable (Windows, or a non-main thread) the
worker logs a debug message and relies on you calling `stop()`.

## `worker.is_running`

`False` once a stop has been requested.

## `worker.worker_id`

A UUID string assigned on construction. Unique per process.

```python
print(worker.worker_id)  # "3d6f4455-..."
```

## Internal loops

### Heartbeat loop

Runs every `heartbeat_interval` seconds, calling `broker.heartbeat_worker()`. Failures are silently suppressed so a temporary broker outage doesn't crash the worker.

### Cron loop

Runs every second, checking whether any cron task is due. Before firing, the
worker must win `broker.acquire_cron_lock()` for that occurrence, so an
occurrence executes exactly once no matter how many workers are running. A cron
task that raises is logged; the next occurrence still fires.

## Retries

A failed attempt with retries left is *rescheduled*, not slept on: the worker
sets `run_at` to the next attempt time, re-enqueues the job and frees its
concurrency slot immediately. A long backoff therefore costs no worker capacity.
See [Defining Tasks](../guide/tasks.md#retry-behaviour).

## Timeouts

A job with a `timeout` is run under `asyncio.wait_for`. On expiry the coroutine
is cancelled and the job fails with `JobTimeoutError`, which then follows the
normal retry and DLQ path.
