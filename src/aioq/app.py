from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from .backends.base import BaseBroker
from .task import TaskDef

if TYPE_CHECKING:
    from .cron import CronDef


class Aarq:
    """Main application object. Holds the broker and task registry."""

    def __init__(self, broker: BaseBroker):
        self.broker = broker
        self._tasks: dict[str, TaskDef] = {}
        self._crons: list[CronDef] = []

    def task(
        self,
        *,
        queue: str = "default",
        retries: int = 0,
        retry_delay: float = 5.0,
        save_result: bool = False,
        result_ttl: int = 3600,
        priority: int = 0,
        dead_letter_queue: str | None = None,
    ) -> Callable:
        """Decorator to register an async function as a task.

        Usage::

            @app.task(queue="email", retries=3)
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
                save_result=save_result,
                result_ttl=result_ttl,
                priority=priority,
                dead_letter_queue=dead_letter_queue,
            )
            self._tasks[task_def.name] = task_def
            return task_def

        return decorator

    def cron(
        self,
        expression: str,
        *,
        queue: str = "default",
        name: str | None = None,
    ) -> Callable:
        """Decorator to register a recurring cron task.

        Usage::

            @app.cron("*/5 * * * *", queue="default")
            async def cleanup(ctx):
                ...
        """

        def decorator(fn: Callable) -> Callable:
            from .cron import CronDef

            cron_def = CronDef(fn=fn, app=self, expression=expression, queue=queue, name=name)
            self._crons.append(cron_def)
            return cron_def

        return decorator

    def get_task(self, name: str) -> TaskDef | None:
        return self._tasks.get(name)

    @property
    def task_names(self) -> list[str]:
        return list(self._tasks.keys())
