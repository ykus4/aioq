# aioq

Async job queue for Python with Redis, PostgreSQL and MySQL backends, job timeouts, retries with exponential backoff, priority queues, job dependencies, dead letter queues, cron scheduling, and a built-in real-time dashboard.

**[Documentation](https://ykus4.github.io/aioq/)** · [PyPI](https://pypi.org/project/aioq/)

## Install

```bash
pip install aioq                # Redis only
pip install "aioq[postgres]"    # + PostgreSQL
pip install "aioq[mysql]"       # + MySQL
pip install "aioq[all]"         # everything
```

## Quick start

```python
# tasks.py
from aioq import Aioq
from aioq.backends import RedisBroker

app = Aioq(broker=RedisBroker(url="redis://localhost:6379"))

@app.task(queue="default", retries=3, retry_backoff=True, timeout=30)
async def my_task(ctx, value: int):
    ...
```

```bash
aioq worker tasks:app       # run a worker
aioq dashboard tasks:app    # dashboard at :8080
aioq status tasks:app       # queue depth and worker health
```

Prefer no external service while developing or testing? Swap in the in-process
broker — same API, nothing to install:

```python
from aioq.backends import MemoryBroker

app = Aioq(broker=MemoryBroker())
```

See the **[docs](https://ykus4.github.io/aioq/)** for full usage.

## License

MIT
