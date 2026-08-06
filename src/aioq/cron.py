from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING

try:
    from croniter import croniter
except ImportError as e:  # pragma: no cover - depends on install extras
    raise ImportError("croniter is required for cron support: pip install croniter") from e

if TYPE_CHECKING:
    from .app import Aioq


class CronDef:
    """A registered cron task. Created by ``@app.cron(...)``."""

    def __init__(
        self,
        fn: Callable,
        app: Aioq,
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
        # Validate eagerly so a bad expression fails at import, not at 3am.
        croniter(expression)

    def next_run(self, after: float | None = None) -> float:
        """Return the unix timestamp of the first occurrence after *after*.

        Stateless on purpose: every worker computes the same occurrence
        timestamps from the same expression, which is what lets them agree on a
        lock key per occurrence (see :meth:`lock_key`).
        """
        base = time.time() if after is None else after
        return croniter(self.expression, base).get_next(float)

    def lock_key(self, occurrence: float) -> str:
        """Fleet-wide identifier for one scheduled occurrence of this cron."""
        return f"{self.name}@{int(occurrence)}"

    async def __call__(self, ctx: dict) -> None:
        await self.fn(ctx)
