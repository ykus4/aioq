from __future__ import annotations

from typing import TYPE_CHECKING, Callable

try:
    from croniter import croniter
except ImportError as e:
    raise ImportError("croniter is required for cron support: pip install croniter") from e

if TYPE_CHECKING:
    from .app import Aarq


class CronDef:
    """A registered cron task. Created by @app.cron(...)."""

    def __init__(
        self,
        fn: Callable,
        app: "Aarq",
        expression: str,
        queue: str,
        name: str | None = None,
    ):
        self.fn = fn
        self.app = app
        self.expression = expression
        self.queue = queue
        self.name = name or f"{fn.__module__}.{fn.__qualname__}"
        self.__name__ = fn.__name__
        self.__doc__ = fn.__doc__
        self._iter = croniter(expression)

    def next_run(self) -> float:
        """Return unix timestamp of the next scheduled run."""
        return self._iter.get_next(float)

    async def __call__(self, ctx: dict) -> None:
        await self.fn(ctx)
