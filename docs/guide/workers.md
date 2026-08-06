# Running Workers

Workers dequeue jobs and execute them concurrently. Each worker runs as a single process.

## CLI

```bash
aioq worker <app_path>
```

`app_path` is a `module:attribute` path to your `Aioq` instance:

```bash
aioq worker myapp.tasks:app
```

### Options

| Flag | Default | Description |
|---|---|---|
| `-q` / `--queue` | `default` | Queue(s) to consume. Repeatable. |
| `-c` / `--concurrency` | `10` | Max concurrent jobs |
| `--log-level` | `info` | Logging level (`debug`, `info`, `warning`, `error`) |
| `--metrics-port` | — | Serve this worker's Prometheus metrics on that port |

```bash
# Consume two queues with higher concurrency
aioq worker myapp.tasks:app -q default -q email --concurrency 20

# Verbose logging for debugging
aioq worker myapp.tasks:app --log-level debug

# Expose per-worker metrics for Prometheus
aioq worker myapp.tasks:app --metrics-port 9100
```

See [Monitoring](monitoring.md) for what those metrics contain, and
[CLI](cli.md) for the other commands.

## Programmatic start

```python
import asyncio
from aioq.worker import Worker
from myapp.tasks import app

worker = Worker(app, queues=["default", "email"], concurrency=20)
asyncio.run(worker.run())
```

## Concurrency model

The worker uses `asyncio.Semaphore(concurrency)` to cap the number of jobs
running at the same time. It takes the slot *before* claiming a job, so it never
holds a claimed job hostage while waiting for capacity, and a retry waiting out
its backoff holds no slot at all. All jobs run as `asyncio.Task` objects in the same event loop — this means CPU-bound work will block other jobs. Use `asyncio.to_thread()` or a `ProcessPoolExecutor` for CPU-intensive tasks.

```python
import asyncio


@app.task(queue="cpu")
async def heavy_compute(ctx, n: int) -> int:
    # Offload to a thread to avoid blocking the event loop
    return await asyncio.to_thread(expensive_function, n)
```

## Heartbeat

Workers send a heartbeat to the broker every 10 seconds. The dashboard uses this to show a liveness indicator. A worker is considered dead if its last heartbeat is older than 30 seconds.

## Graceful shutdown

When the worker receives `SIGTERM` or `SIGINT` (Ctrl-C):

1. Stops pulling new jobs from the queue
2. Waits for all in-flight jobs to finish
3. Deregisters from the broker
4. Exits cleanly

This means a `docker stop` or `kubectl rollout restart` will not drop in-flight jobs.

## Running multiple workers

Run multiple worker processes against the same broker for horizontal scaling:

```bash
# Terminal 1
aioq worker myapp.tasks:app -q default

# Terminal 2
aioq worker myapp.tasks:app -q email --concurrency 5
```

Each worker gets its own UUID and registers independently.

## Supervisor / systemd example

```ini
[Unit]
Description=aioq worker
After=network.target

[Service]
ExecStart=/usr/local/bin/aioq worker myapp.tasks:app --concurrency 20
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```
