import asyncio
import time

import pytest

from aioq import Aioq
from aioq.backends.memory import MemoryBroker
from aioq.cron import CronDef
from aioq.worker import Worker


@pytest.fixture
def app():
    return Aioq(broker=MemoryBroker())


def test_cron_registration(app):
    @app.cron("*/5 * * * *")
    async def my_job(ctx):
        pass

    assert len(app.crons) == 1
    assert isinstance(app.crons[0], CronDef)
    assert app.crons[0].expression == "*/5 * * * *"


def test_invalid_expression_is_rejected_at_registration(app):
    # croniter's errors subclass ValueError.
    with pytest.raises(ValueError, match="columns has to be specified"):
        app.cron("not a cron expression")(lambda ctx: None)


def test_cron_next_run_is_in_the_future(app):
    @app.cron("* * * * *")
    async def tick(ctx):
        pass

    assert app.crons[0].next_run() > time.time()


def test_cron_next_run_is_stateless(app):
    """Called twice with no argument it returns the same occurrence.

    This is what lets every worker agree on the lock key for an occurrence.
    """

    @app.cron("0 * * * *")
    async def hourly(ctx):
        pass

    cron = app.crons[0]
    assert cron.next_run() == cron.next_run()


def test_cron_next_run_advances_from_a_base(app):
    @app.cron("0 * * * *")
    async def hourly(ctx):
        pass

    cron = app.crons[0]
    first = cron.next_run()
    second = cron.next_run(after=first)
    assert second == pytest.approx(first + 3600)


def test_lock_key_is_unique_per_occurrence(app):
    @app.cron("* * * * *")
    async def tick(ctx):
        pass

    cron = app.crons[0]
    assert cron.lock_key(100.0) != cron.lock_key(160.0)
    assert cron.lock_key(100.4) == cron.lock_key(100.9)  # sub-second noise ignored


async def test_only_one_worker_fires_an_occurrence():
    """Two workers sharing a broker must not double-run the same occurrence."""
    broker = MemoryBroker()
    app = Aioq(broker=broker)
    fired = []

    @app.cron("* * * * *")
    async def tick(ctx):
        fired.append(ctx["worker_id"])

    cron = app.crons[0]
    occurrence = cron.next_run()

    workers = [Worker(app), Worker(app)]
    async with broker:
        for worker in workers:
            await worker._maybe_fire_cron(cron, occurrence)
        await asyncio.gather(*(t for w in workers for t in w._cron_tasks))

    assert len(fired) == 1


async def test_cron_failure_is_contained():
    broker = MemoryBroker()
    app = Aioq(broker=broker)

    @app.cron("* * * * *")
    async def broken(ctx):
        raise RuntimeError("cron blew up")

    worker = Worker(app)
    async with broker:
        # Must not propagate — a failing cron cannot take the worker down.
        await worker._run_cron(app.crons[0])
