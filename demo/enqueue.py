"""Enqueue demo jobs.

Usage:
    uv run python demo/enqueue.py
"""
import asyncio

from demo.tasks import add, broker, flaky_task, urgent


async def main():
    async with broker:
        # 1. add jobs (will succeed)
        for a, b in [(1, 2), (10, 20), (100, 200)]:
            job = await add.enqueue(a, b)
            print(f"[add]    enqueued {a}+{b}  → {job.id[:8]}")

        # 2. flaky jobs: odd succeed, even → retry → DLQ
        for x in range(6):
            job = await flaky_task.enqueue(x)
            print(f"[flaky]  enqueued x={x}   → {job.id[:8]}")

        # 3. high-priority urgent jobs
        for msg in ["deploy now", "alert fired"]:
            job = await urgent.enqueue(msg)
            print(f"[urgent] enqueued '{msg}' → {job.id[:8]}")

        # 4. dependent jobs
        j1 = await add.enqueue(1, 1)
        j2 = await add.enqueue(2, 2)
        j3 = await add.enqueue(3, 3, depends_on=[j1.id, j2.id])
        print(f"[deps]   j1={j1.id[:8]} j2={j2.id[:8]} → j3={j3.id[:8]} (waiting)")

    print("\nDone. Check the dashboard at http://localhost:8080")


asyncio.run(main())
