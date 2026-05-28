"""Quick-start example.

Run worker:
    aioq worker example:app

Run dashboard:
    aioq dashboard example:app --port 8080

Enqueue jobs:
    python example.py enqueue
"""
from __future__ import annotations

import asyncio
import sys

from src.aioq import Aarq
from src.aioq.backends import RedisBroker

broker = RedisBroker(url="redis://localhost:6379")
app = Aarq(broker=broker)


@app.task(queue="default", retries=2, retry_delay=5.0)
async def add(ctx, a: int, b: int) -> int:
    print(f"[add] {a} + {b}")
    return a + b


@app.task(queue="email", retries=3, save_result=True)
async def send_email(ctx, to: str, subject: str) -> dict:
    print(f"[send_email] to={to} subject={subject}")
    return {"status": "sent", "to": to}


async def main():
    async with broker:
        j1 = await add.enqueue(1, 2)
        print(f"Enqueued add job: {j1.id}")

        j2 = await send_email.enqueue(to="user@example.com", subject="Hello from aioq!")
        print(f"Enqueued email job: {j2.id}")

        j3 = await add.enqueue(10, 20, defer_by=30)
        print(f"Enqueued deferred add job (runs in 30s): {j3.id}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "enqueue":
        asyncio.run(main())
    else:
        print(__doc__)
