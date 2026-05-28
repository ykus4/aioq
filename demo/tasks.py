from aioq import Aarq
from aioq.backends import RedisBroker

broker = RedisBroker(url="redis://localhost:6379")
app = Aarq(broker=broker)


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
