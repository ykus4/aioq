# aioq

Async job queue for Python with Redis, PostgreSQL, and MySQL backends, priority queues, job dependencies, dead letter queues, and a built-in real-time dashboard.

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
from aioq import Aarq
from aioq.backends import RedisBroker

app = Aarq(broker=RedisBroker(url="redis://localhost:6379"))

@app.task(queue="default", retries=3)
async def my_task(ctx, value: int):
    ...
```

```bash
aioq worker tasks:app       # run a worker
aioq dashboard tasks:app    # dashboard at :8080
```

See the **[docs](https://ykus4.github.io/aioq/)** for full usage.

## License

MIT
