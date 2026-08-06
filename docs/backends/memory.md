# MemoryBroker

An in-process broker. No server to install, nothing to clean up — everything
lives in Python objects.

```python
from aioq import Aioq
from aioq.backends import MemoryBroker

app = Aioq(broker=MemoryBroker())
```

It ships with aioq itself; there is no extra to install.

## What it is for

- **Tests.** Exercise the real worker, real retries and real dependency
  resolution without a Redis container or database fixture.
- **Local development.** Run tasks, the worker loop and the dashboard in one
  process while you are still shaping your tasks.
- **Examples.** Anything that should run with `python script.py` and no setup.

## What it is not for

**MemoryBroker is per-process.** A worker started with `aioq worker` builds its
own `MemoryBroker` and sees an empty queue — it cannot see jobs your web process
enqueued. Use Redis or a SQL backend as soon as more than one process is
involved.

It also does not persist: restart the process and the queue is gone.

## Feature parity

It implements the full `BaseBroker` interface — priorities, deferred jobs, job
dependencies, cancellation, dead letter queues, worker registration, cron locks
and purging all behave the same as with the other backends. That is the point:
what passes against MemoryBroker should pass against Redis.

## Testing with it

```python
import pytest
from aioq import Aioq
from aioq.backends import MemoryBroker
from aioq.worker import Worker


@pytest.fixture
async def app():
    broker = MemoryBroker()
    async with broker:
        yield Aioq(broker=broker)


async def test_my_task(app):
    @app.task(save_result=True)
    async def add(ctx, a, b):
        return a + b

    job = await add.enqueue(2, 3)

    # Claim and run exactly one job, like a worker tick.
    worker = Worker(app)
    claimed = await app.broker.dequeue(["default"], timeout=0.2)
    await worker._process(claimed)

    assert (await app.broker.get_job(job.id)).result == 5
```

Driving one job at a time keeps tests deterministic. If you want the real loop,
start it and stop it explicitly:

```python
worker = Worker(app, concurrency=4)
runner = asyncio.create_task(worker.run())
...
worker.stop()
await runner  # drains in-flight jobs, then returns
```

### Resetting between tests

```python
await broker.clear()  # drops jobs, workers and cron locks
```

## Running everything in one process

`MemoryBroker` is the one backend where the dashboard can serve an embedded
worker's queue, because they share the object:

```python
import asyncio
import uvicorn

from aioq.dashboard import create_dashboard
from aioq.worker import Worker
from myapp.tasks import app


async def main():
    worker = Worker(app, concurrency=4)
    server = uvicorn.Server(uvicorn.Config(create_dashboard(app), port=8080))
    await asyncio.gather(worker.run(), server.serve())


asyncio.run(main())
```
