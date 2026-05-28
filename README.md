# aioq

Async job queue for Python with Redis/PostgreSQL backends and a built-in real-time dashboard.

**[Documentation](https://ykus4.github.io/aioq/)** · [PyPI](https://pypi.org/project/aioq/)

## Install

```bash
pip install aioq           # Redis
pip install "aioq[all]"    # Redis + PostgreSQL + cron
```

## Quick start

```python
from aioq import Aarq
from aioq.backends import RedisBroker

app = Aarq(broker=RedisBroker())

@app.task(queue="default", retries=3)
async def send_email(ctx, to: str, subject: str) -> dict:
    ...
```

```bash
aioq worker tasks:app       # run a worker
aioq dashboard tasks:app    # open the dashboard at :8080
```

See the **[docs](https://ykus4.github.io/aioq/)** for full usage, backend configuration, and API reference.

## License

MIT
