"""Worker startup, shutdown and background loops."""

from __future__ import annotations

import asyncio
import inspect

import pytest

from aioq import Aioq
from aioq.backends.memory import MemoryBroker
from aioq.constants import DEFAULT_QUEUE
from aioq.models import JobStatus
from aioq.worker import Worker


@pytest.fixture
def app():
    return Aioq(broker=MemoryBroker())


async def run_until(worker: Worker, condition, timeout: float = 5.0) -> None:
    """Start *worker*, wait for *condition*, then shut it down cleanly.

    *condition* may be sync or async and is polled until it returns true.
    """
    runner = asyncio.create_task(worker.run())
    satisfied = False
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            result = condition()
            if inspect.isawaitable(result):
                result = await result
            if result:
                satisfied = True
                break
            await asyncio.sleep(0.02)
    finally:
        worker.stop()
        await asyncio.wait_for(runner, timeout=timeout)

    if not satisfied:
        pytest.fail("condition never became true")


# ----------------------------------------------------------------------
# Defaults
# ----------------------------------------------------------------------


def test_worker_defaults_to_the_default_queue(app):
    assert Worker(app).queues == [DEFAULT_QUEUE]


def test_worker_ids_are_unique(app):
    assert Worker(app).worker_id != Worker(app).worker_id


def test_explicit_queues_are_kept(app):
    assert Worker(app, queues=["a", "b"]).queues == ["a", "b"]


# ----------------------------------------------------------------------
# run() lifecycle
# ----------------------------------------------------------------------


async def test_run_registers_then_deregisters_the_worker(app):
    worker = Worker(app)

    await run_until(worker, lambda: _registered(app, worker.worker_id))

    assert await app.broker.list_workers() == []


async def _registered(app: Aioq, worker_id: str) -> bool:
    return any(w["worker_id"] == worker_id for w in await app.broker.list_workers())


async def test_run_processes_queued_jobs(app):
    done = asyncio.Event()

    @app.task()
    async def work(ctx):
        done.set()

    await app.broker.connect()
    job = await work.enqueue()

    await run_until(Worker(app), lambda: done.is_set())

    assert (await app.broker.get_job(job.id)).status == JobStatus.completed


async def test_run_waits_for_in_flight_jobs_before_stopping(app):
    started = asyncio.Event()
    completed = []

    @app.task()
    async def slow(ctx):
        started.set()
        await asyncio.sleep(0.2)
        completed.append(1)

    await app.broker.connect()
    job = await slow.enqueue()

    worker = Worker(app)
    runner = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), timeout=5)
    worker.stop()
    await asyncio.wait_for(runner, timeout=5)

    # Shutdown drained the job rather than abandoning it.
    assert completed == [1]
    assert (await app.broker.get_job(job.id)).status == JobStatus.completed


async def test_stop_is_idempotent(app):
    worker = Worker(app)
    worker.stop()
    worker.stop()
    assert worker.is_running is False


async def test_stop_before_run_is_honoured(app):
    """A stop requested before run() is scheduled must not be overwritten."""
    ran = []

    @app.task()
    async def work(ctx):
        ran.append(1)

    await app.broker.connect()
    await work.enqueue()

    worker = Worker(app)
    worker.stop()
    await asyncio.wait_for(worker.run(), timeout=5)

    assert ran == []
    assert worker.is_running is False


# ----------------------------------------------------------------------
# Resilience
# ----------------------------------------------------------------------


async def test_a_failing_dequeue_does_not_kill_the_worker(app, monkeypatch):
    """A broker outage must be retried, not crash the process."""
    calls = []
    real_dequeue = app.broker.dequeue

    async def flaky(queues, timeout=2.0):
        calls.append(1)
        if len(calls) <= 2:
            raise ConnectionError("broker is down")
        return await real_dequeue(queues, timeout=timeout)

    monkeypatch.setattr(app.broker, "dequeue", flaky)
    monkeypatch.setattr("aioq.worker._BROKER_ERROR_BACKOFF", 0.01)

    worker = Worker(app)
    await run_until(worker, lambda: _at_least(calls, 3))


def _at_least(calls: list, n: int) -> bool:
    return len(calls) >= n


async def test_dequeue_failure_releases_the_concurrency_slot(app, monkeypatch):
    """Leaking the semaphore on error would wedge the worker after N failures."""
    attempts = []

    async def failing(queues, timeout=2.0):
        attempts.append(1)
        raise ConnectionError("down")

    monkeypatch.setattr(app.broker, "dequeue", failing)
    monkeypatch.setattr("aioq.worker._BROKER_ERROR_BACKOFF", 0.01)

    worker = Worker(app, concurrency=2)
    await run_until(worker, lambda: _at_least(attempts, 5))

    # All slots are free again after the failures.
    assert worker._semaphore._value == 2


async def test_an_instantly_empty_dequeue_does_not_starve_the_loop(app, monkeypatch):
    """A broker that returns None without blocking must not wedge shutdown."""
    calls = []

    async def instantly_empty(queues, timeout=2.0):
        calls.append(1)
        return None

    monkeypatch.setattr(app.broker, "dequeue", instantly_empty)

    worker = Worker(app)
    await run_until(worker, lambda: _at_least(calls, 10), timeout=5)


async def test_heartbeat_loop_refreshes_the_registration(app):
    beats = []
    real_heartbeat = app.broker.heartbeat_worker

    async def counting(worker_id):
        beats.append(worker_id)
        await real_heartbeat(worker_id)

    app.broker.heartbeat_worker = counting  # type: ignore[method-assign]

    worker = Worker(app, heartbeat_interval=0.02)
    await run_until(worker, lambda: _at_least(beats, 2))


async def test_heartbeat_errors_are_swallowed(app):
    async def boom(worker_id):
        raise ConnectionError("down")

    app.broker.heartbeat_worker = boom  # type: ignore[method-assign]

    worker = Worker(app, heartbeat_interval=0.01)
    # The worker must still start, run and stop normally.
    await run_until(worker, lambda: _registered(app, worker.worker_id))


# ----------------------------------------------------------------------
# Cron loop
# ----------------------------------------------------------------------


async def test_cron_loop_exits_immediately_with_no_crons(app):
    worker = Worker(app)
    await app.broker.connect()
    # Returns rather than ticking forever.
    await asyncio.wait_for(worker._cron_loop(), timeout=1)


async def test_cron_loop_fires_a_due_occurrence(app, monkeypatch):
    fired = asyncio.Event()

    @app.cron("* * * * *")
    async def tick(ctx):
        fired.set()

    # Pretend the next occurrence is already due.
    monkeypatch.setattr(app.crons[0], "next_run", lambda after=None: 0.0)
    monkeypatch.setattr("aioq.worker.CRON_TICK_INTERVAL", 0.01)

    worker = Worker(app)
    await run_until(worker, fired.is_set)


async def _resolve(value: bool) -> bool:
    """Wrap a plain value in a coroutine, for monkeypatching async methods."""
    return value


async def test_a_broken_cron_lock_skips_the_occurrence(app, monkeypatch):
    ran = []

    @app.cron("* * * * *")
    async def tick(ctx):
        ran.append(1)

    async def boom(key, ttl=60):
        raise ConnectionError("broker down")

    await app.broker.connect()
    monkeypatch.setattr(app.broker, "acquire_cron_lock", boom)

    await Worker(app)._maybe_fire_cron(app.crons[0], 0.0)

    assert ran == []


async def test_a_lost_cron_lock_skips_the_occurrence(app, monkeypatch):
    ran = []

    @app.cron("* * * * *")
    async def tick(ctx):
        ran.append(1)

    await app.broker.connect()
    monkeypatch.setattr(app.broker, "acquire_cron_lock", lambda key, ttl=60: _resolve(False))

    await Worker(app)._maybe_fire_cron(app.crons[0], 0.0)

    assert ran == []


async def test_cron_context_names_the_cron(app):
    seen = {}

    @app.cron("* * * * *")
    async def tick(ctx):
        seen.update(ctx)

    await app.broker.connect()
    worker = Worker(app)
    await worker._run_cron(app.crons[0])

    assert seen["cron"] == app.crons[0].name
    assert seen["worker_id"] == worker.worker_id
    assert seen["broker"] is app.broker
