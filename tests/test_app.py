"""Application object: registration, naming, hooks, back-compat."""

from __future__ import annotations

import pytest

import aioq
from aioq import Aarq, Aioq
from aioq.backends.memory import MemoryBroker
from aioq.constants import DEFAULT_QUEUE, DEFAULT_RESULT_TTL


@pytest.fixture
def app():
    return Aioq(broker=MemoryBroker())


# ----------------------------------------------------------------------
# Naming / back-compat
# ----------------------------------------------------------------------


def test_aarq_is_an_alias_for_aioq():
    """The old name must keep working for code written against 0.4."""
    assert Aarq is Aioq
    assert aioq.Aarq is aioq.Aioq


def test_alias_instances_are_aioq_instances():
    assert isinstance(Aarq(broker=MemoryBroker()), Aioq)


# ----------------------------------------------------------------------
# Task registration
# ----------------------------------------------------------------------


def test_task_defaults(app):
    @app.task()
    async def noop(ctx):
        pass

    assert noop.queue == DEFAULT_QUEUE
    assert noop.retries == 0
    assert noop.timeout is None
    assert noop.retry_backoff is False
    assert noop.result_ttl == DEFAULT_RESULT_TTL


def test_task_options_are_stored(app):
    @app.task(
        queue="emails",
        retries=5,
        retry_delay=2.5,
        retry_backoff=True,
        retry_backoff_max=99.0,
        timeout=30,
        save_result=True,
        result_ttl=60,
        priority=10,
        dead_letter_queue="emails-dlq",
    )
    async def send(ctx):
        pass

    assert (send.queue, send.retries, send.retry_delay) == ("emails", 5, 2.5)
    assert (send.retry_backoff, send.retry_backoff_max) == (True, 99.0)
    assert (send.timeout, send.save_result, send.result_ttl) == (30, True, 60)
    assert (send.priority, send.dead_letter_queue) == (10, "emails-dlq")


def test_task_is_registered_under_its_qualified_name(app):
    @app.task()
    async def my_task(ctx):
        pass

    assert my_task.name in app.task_names
    assert app.get_task(my_task.name) is my_task
    assert "my_task" in my_task.name


def test_task_preserves_function_metadata(app):
    @app.task()
    async def documented(ctx):
        """Some docs."""

    assert documented.__name__ == "documented"
    assert documented.__doc__ == "Some docs."


def test_duplicate_task_name_is_rejected(app):
    def register():
        @app.task()
        async def dupe(ctx):
            pass

        return dupe

    register()
    with pytest.raises(ValueError, match="already registered"):
        register()


def test_get_unknown_task_returns_none(app):
    assert app.get_task("nope") is None


def test_tasks_property_is_a_copy(app):
    @app.task()
    async def one(ctx):
        pass

    tasks = app.tasks
    tasks.clear()
    assert len(app.tasks) == 1


def test_crons_property_is_a_copy(app):
    @app.cron("* * * * *")
    async def tick(ctx):
        pass

    crons = app.crons
    crons.clear()
    assert len(app.crons) == 1


# ----------------------------------------------------------------------
# Hooks
# ----------------------------------------------------------------------


def test_hook_decorators_return_the_function(app):
    @app.on_job_start
    async def started(job):
        pass

    assert callable(started)
    assert app.hooks.on_start == [started]


def test_every_hook_type_registers(app):
    async def hook(*args):
        pass

    app.on_job_start(hook)
    app.on_job_success(hook)
    app.on_job_failure(hook)
    app.on_job_retry(hook)
    app.on_job_dead(hook)

    assert app.hooks.on_start == [hook]
    assert app.hooks.on_success == [hook]
    assert app.hooks.on_failure == [hook]
    assert app.hooks.on_retry == [hook]
    assert app.hooks.on_dead == [hook]


async def test_hooks_run_in_registration_order(app):
    order = []
    app.on_job_start(lambda job: order.append("first"))
    app.on_job_start(lambda job: order.append("second"))

    await app.hooks.emit("on_start", None)

    assert order == ["first", "second"]


async def test_one_broken_hook_does_not_stop_the_rest(app):
    order = []

    @app.on_job_start
    def broken(job):
        raise RuntimeError("boom")

    app.on_job_start(lambda job: order.append("still ran"))

    await app.hooks.emit("on_start", None)

    assert order == ["still ran"]


async def test_emit_ignores_unregistered_events(app):
    # No hooks registered — emitting must be a silent no-op.
    await app.hooks.emit("on_success", None, None)


def test_hooks_are_per_app():
    a, b = Aioq(broker=MemoryBroker()), Aioq(broker=MemoryBroker())

    @a.on_job_start
    async def hook(job):
        pass

    assert a.hooks.on_start == [hook]
    assert b.hooks.on_start == []


# ----------------------------------------------------------------------
# Dashboard flag
# ----------------------------------------------------------------------


def test_dashboard_enabled_by_default(app):
    assert app.dashboard_enabled is True


def test_dashboard_can_be_disabled():
    assert Aioq(broker=MemoryBroker(), dashboard_enabled=False).dashboard_enabled is False
