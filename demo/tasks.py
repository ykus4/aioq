"""Demo task definitions.

Set ``AIOQ_DEMO_BROKER=memory`` to run the whole demo without Redis — handy for
a quick look at the dashboard. Note that MemoryBroker is per-process, so with it
the worker and the dashboard each see their own queue; use Redis to see them
talk to each other.
"""

import os

from aioq import Aioq
from aioq.backends import MemoryBroker, RedisBroker

if os.getenv("AIOQ_DEMO_BROKER") == "memory":
    broker = MemoryBroker()
else:
    broker = RedisBroker(url=os.getenv("AIOQ_DEMO_REDIS", "redis://localhost:6379"))

app = Aioq(broker=broker)


@app.task(queue="default", retries=2, retry_delay=1.0, dead_letter_queue="dlq")
async def flaky_task(ctx, x: int):
    """Fails for even numbers, succeeds for odd numbers."""
    if x % 2 == 0:
        raise ValueError(f"Even number failed: {x}")
    return x * 10


@app.task(queue="default", save_result=True)
async def add(ctx, a: int, b: int) -> int:
    return a + b


@app.task(queue="high", priority=10, save_result=True)
async def urgent(ctx, msg: str) -> str:
    return f"[URGENT] {msg}"


@app.task(queue="default", timeout=2.0, retries=1, retry_delay=0.5)
async def slow_task(ctx, seconds: float):
    """Times out — and therefore retries — if it runs past `timeout`."""
    import asyncio

    await asyncio.sleep(seconds)
    return "finished in time"


@app.task(
    queue="default",
    retries=4,
    retry_delay=1.0,
    retry_backoff=True,
    retry_backoff_max=30.0,
    dead_letter_queue="dlq",
)
async def always_fails(ctx):
    """Retries with exponential backoff, then lands in the DLQ."""
    raise RuntimeError("this task never succeeds")


@app.on_job_success
async def log_success(job, result):
    print(f"  ✓ {job.task_name.rsplit('.', 1)[-1]} → {result!r}")


@app.on_job_retry
async def log_retry(job, exc):
    print(f"  ↻ {job.task_name.rsplit('.', 1)[-1]} retry {job.retries}/{job.max_retries}: {exc}")


@app.on_job_dead
async def log_dead(job, exc):
    print(f"  ☠ {job.task_name.rsplit('.', 1)[-1]} moved to {job.queue}: {exc}")
