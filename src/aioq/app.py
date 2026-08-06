from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar

from .backends.base import BaseBroker
from .constants import DEFAULT_QUEUE, DEFAULT_RESULT_TTL, DEFAULT_RETRY_BACKOFF_MAX
from .hooks import FailureHook, Hooks, StartHook, SuccessHook
from .task import TaskDef

if TYPE_CHECKING:
    from .cron import CronDef

_S = TypeVar("_S", bound=StartHook)
_U = TypeVar("_U", bound=SuccessHook)
_F = TypeVar("_F", bound=FailureHook)


class Aioq:
    """Main application object. Holds the broker, task registry and hooks."""

    def __init__(self, broker: BaseBroker, *, dashboard_enabled: bool = True):
        self.broker = broker
        self.dashboard_enabled = dashboard_enabled
        self.hooks = Hooks()
        self._tasks: dict[str, TaskDef] = {}
        self._crons: list[CronDef] = []

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def task(
        self,
        *,
        queue: str = DEFAULT_QUEUE,
        retries: int = 0,
        retry_delay: float = 5.0,
        retry_backoff: bool = False,
        retry_backoff_max: float = DEFAULT_RETRY_BACKOFF_MAX,
        timeout: float | None = None,
        save_result: bool = False,
        result_ttl: int = DEFAULT_RESULT_TTL,
        priority: int = 0,
        dead_letter_queue: str | None = None,
    ) -> Callable[[Callable], TaskDef]:
        """Decorator to register an async function as a task.

        *timeout* cancels the job after that many seconds and treats it as a
        failure (so it retries / lands in the DLQ like any other error).

        *retry_backoff* switches the delay between retries from a flat
        *retry_delay* to exponential-with-jitter, capped at *retry_backoff_max*.

        Usage::

            @app.task(queue="email", retries=3, timeout=30, retry_backoff=True)
            async def send_email(ctx, user_id: int):
                ...

            job = await send_email.enqueue(user_id=42)
        """

        def decorator(fn: Callable) -> TaskDef:
            task_def = TaskDef(
                fn=fn,
                app=self,
                queue=queue,
                retries=retries,
                retry_delay=retry_delay,
                retry_backoff=retry_backoff,
                retry_backoff_max=retry_backoff_max,
                timeout=timeout,
                save_result=save_result,
                result_ttl=result_ttl,
                priority=priority,
                dead_letter_queue=dead_letter_queue,
            )
            if task_def.name in self._tasks:
                raise ValueError(
                    f"A task named {task_def.name!r} is already registered. "
                    "Task names must be unique — workers resolve jobs by name."
                )
            self._tasks[task_def.name] = task_def
            return task_def

        return decorator

    def cron(
        self,
        expression: str,
        *,
        queue: str = DEFAULT_QUEUE,
        name: str | None = None,
    ) -> Callable[[Callable], CronDef]:
        """Decorator to register a recurring cron task.

        Each occurrence fires exactly once across the whole worker fleet: a
        worker must win a broker-held lock for that occurrence before running
        it, so scaling out workers does not duplicate cron runs.

        Usage::

            @app.cron("*/5 * * * *", queue="default")
            async def cleanup(ctx):
                ...
        """

        def decorator(fn: Callable) -> CronDef:
            from .cron import CronDef

            cron_def = CronDef(fn=fn, app=self, expression=expression, queue=queue, name=name)
            self._crons.append(cron_def)
            return cron_def

        return decorator

    # ------------------------------------------------------------------
    # Lifecycle hooks
    # ------------------------------------------------------------------

    def on_job_start(self, fn: _S) -> _S:
        """Register ``async def hook(job)``, called just before a job runs."""
        self.hooks.on_start.append(fn)
        return fn

    def on_job_success(self, fn: _U) -> _U:
        """Register ``async def hook(job, result)``, called after a job succeeds."""
        self.hooks.on_success.append(fn)
        return fn

    def on_job_failure(self, fn: _F) -> _F:
        """Register ``async def hook(job, exc)``, called on a final failure.

        Not called for attempts that will be retried — use
        :meth:`on_job_retry` for those.
        """
        self.hooks.on_failure.append(fn)
        return fn

    def on_job_retry(self, fn: _F) -> _F:
        """Register ``async def hook(job, exc)``, called when a job is re-enqueued."""
        self.hooks.on_retry.append(fn)
        return fn

    def on_job_dead(self, fn: _F) -> _F:
        """Register ``async def hook(job, exc)``, called when a job enters its DLQ."""
        self.hooks.on_dead.append(fn)
        return fn

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def get_task(self, name: str) -> TaskDef | None:
        return self._tasks.get(name)

    @property
    def tasks(self) -> dict[str, TaskDef]:
        return dict(self._tasks)

    @property
    def crons(self) -> list[CronDef]:
        return list(self._crons)

    @property
    def task_names(self) -> list[str]:
        return list(self._tasks)


#: Backwards-compatible alias. ``Aioq`` is the canonical name.
Aarq = Aioq
