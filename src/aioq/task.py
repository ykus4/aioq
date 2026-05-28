from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from .models import Job

if TYPE_CHECKING:
    from .app import Aarq


class TaskDef:
    """A registered task definition. Created by @app.task(...)."""

    def __init__(
        self,
        fn: Callable,
        app: Aarq,
        queue: str,
        retries: int,
        retry_delay: float,
        save_result: bool,
        result_ttl: int,
    ):
        self.fn = fn
        self.app = app
        self.queue = queue
        self.retries = retries
        self.retry_delay = retry_delay
        self.save_result = save_result
        self.result_ttl = result_ttl
        self.name = f"{fn.__module__}.{fn.__qualname__}"
        self.__doc__ = fn.__doc__
        self.__name__ = fn.__name__

    async def enqueue(
        self,
        *args: Any,
        defer_by: float | None = None,
        defer_until: datetime | None = None,
        **kwargs: Any,
    ) -> Job:
        """Enqueue this task and return the Job."""
        run_at: datetime | None = None
        if defer_by is not None:
            run_at = datetime.now(UTC) + timedelta(seconds=defer_by)
        elif defer_until is not None:
            run_at = defer_until

        job = Job(
            task_name=self.name,
            queue=self.queue,
            args=list(args),
            kwargs=kwargs,
            max_retries=self.retries,
            retry_delay=self.retry_delay,
            save_result=self.save_result,
            run_at=run_at,
        )
        await self.app.broker.enqueue(job)
        return job

    async def __call__(self, ctx: dict, *args: Any, **kwargs: Any) -> Any:
        """Directly call the underlying function (used by worker)."""
        return await self.fn(ctx, *args, **kwargs)
