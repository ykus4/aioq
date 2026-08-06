from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from .constants import DEFAULT_RESULT_TTL, DEFAULT_RETRY_BACKOFF_MAX
from .models import Job

if TYPE_CHECKING:
    from .app import Aioq


class TaskDef:
    """A registered task definition. Created by ``@app.task(...)``."""

    def __init__(
        self,
        fn: Callable,
        app: Aioq,
        queue: str,
        retries: int,
        retry_delay: float,
        save_result: bool,
        result_ttl: int = DEFAULT_RESULT_TTL,
        priority: int = 0,
        dead_letter_queue: str | None = None,
        retry_backoff: bool = False,
        retry_backoff_max: float = DEFAULT_RETRY_BACKOFF_MAX,
        timeout: float | None = None,
    ):
        self.fn = fn
        self.app = app
        self.queue = queue
        self.retries = retries
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff
        self.retry_backoff_max = retry_backoff_max
        self.timeout = timeout
        self.save_result = save_result
        self.result_ttl = result_ttl
        self.priority = priority
        self.dead_letter_queue = dead_letter_queue
        self.name = f"{fn.__module__}.{fn.__qualname__}"
        self.__doc__ = fn.__doc__
        self.__name__ = fn.__name__

    # ------------------------------------------------------------------
    # Enqueueing
    # ------------------------------------------------------------------

    async def enqueue(
        self,
        *args: Any,
        defer_by: float | None = None,
        defer_until: datetime | None = None,
        priority: int | None = None,
        depends_on: list[str] | None = None,
        **kwargs: Any,
    ) -> Job:
        """Enqueue this task and return the Job."""
        job = self._build_job(
            args=list(args),
            kwargs=kwargs,
            run_at=_resolve_run_at(defer_by, defer_until),
            priority=priority,
            depends_on=depends_on,
        )
        await self.app.broker.enqueue(job)
        return job

    async def enqueue_many(
        self,
        items: Sequence[tuple | dict],
        *,
        defer_by: float | None = None,
        defer_until: datetime | None = None,
        priority: int | None = None,
        depends_on: list[str] | None = None,
    ) -> list[Job]:
        """Enqueue multiple calls to this task in one broker round-trip.

        *items* is a list of kwargs dicts or positional-args tuples.
        Returns the list of created :class:`~aioq.models.Job` objects.
        """
        run_at = _resolve_run_at(defer_by, defer_until)
        jobs = [
            self._build_job(
                args=[] if isinstance(item, dict) else list(item),
                kwargs=item if isinstance(item, dict) else {},
                run_at=run_at,
                priority=priority,
                depends_on=depends_on,
            )
            for item in items
        ]
        await self.app.broker.enqueue_many(jobs)
        return jobs

    def _build_job(
        self,
        *,
        args: list[Any],
        kwargs: dict[str, Any],
        run_at: datetime | None,
        priority: int | None,
        depends_on: list[str] | None,
    ) -> Job:
        return Job(
            task_name=self.name,
            queue=self.queue,
            args=args,
            kwargs=kwargs,
            max_retries=self.retries,
            retry_delay=self.retry_delay,
            retry_backoff=self.retry_backoff,
            retry_backoff_max=self.retry_backoff_max,
            timeout=self.timeout,
            save_result=self.save_result,
            result_ttl=self.result_ttl,
            run_at=run_at,
            priority=self.priority if priority is None else priority,
            dead_letter_queue=self.dead_letter_queue,
            depends_on=list(depends_on or []),
        )

    async def __call__(self, ctx: dict, *args: Any, **kwargs: Any) -> Any:
        """Directly call the underlying function (used by the worker)."""
        return await self.fn(ctx, *args, **kwargs)


def _resolve_run_at(defer_by: float | None, defer_until: datetime | None) -> datetime | None:
    """Turn the two mutually-exclusive defer options into an absolute time."""
    if defer_by is not None and defer_until is not None:
        raise ValueError("Pass either defer_by or defer_until, not both.")
    if defer_by is not None:
        return datetime.now(UTC) + timedelta(seconds=defer_by)
    return defer_until
