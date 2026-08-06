"""Enqueue demo jobs.

Usage:
    uv run python demo/enqueue.py
"""

import asyncio

from demo.tasks import add, always_fails, broker, flaky_task, slow_task, urgent


async def main():
    async with broker:
        # 1. add jobs (will succeed)
        for a, b in [(1, 2), (10, 20), (100, 200)]:
            job = await add.enqueue(a, b)
            print(f"[add]      enqueued {a}+{b}  → {job.id[:8]}")

        # 2. flaky jobs: odd succeed, even → retry → DLQ
        for x in range(6):
            job = await flaky_task.enqueue(x)
            print(f"[flaky]    enqueued x={x}   → {job.id[:8]}")

        # 3. high-priority urgent jobs
        for msg in ["deploy now", "alert fired"]:
            job = await urgent.enqueue(msg)
            print(f"[urgent]   enqueued '{msg}' → {job.id[:8]}")

        # 4. dependent jobs — j3 stays "waiting" until j1 and j2 complete
        j1 = await add.enqueue(1, 1)
        j2 = await add.enqueue(2, 2)
        j3 = await add.enqueue(3, 3, depends_on=[j1.id, j2.id])
        print(f"[deps]     j1={j1.id[:8]} j2={j2.id[:8]} → j3={j3.id[:8]} (waiting)")

        # 5. timeouts: the 5s job blows through its 2s timeout and retries
        quick = await slow_task.enqueue(0.1)
        hangs = await slow_task.enqueue(5.0)
        print(f"[timeout]  ok={quick.id[:8]}  times-out={hangs.id[:8]}")

        # 6. exponential backoff, ending in the dead letter queue
        doomed = await always_fails.enqueue()
        print(f"[backoff]  enqueued → {doomed.id[:8]} (4 retries, then DLQ)")

        # 7. a batch, deferred by 5 seconds
        batch = await add.enqueue_many([(i, i) for i in range(5)], defer_by=5)
        print(f"[batch]    enqueued {len(batch)} deferred jobs (run in 5s)")

    print("\nDone. Check the dashboard at http://localhost:8080")
    print("Or from the terminal:  aioq status demo.tasks:app")


asyncio.run(main())
