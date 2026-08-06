"""Lifecycle hooks.

Hooks let you observe job execution without wrapping every task — logging,
error reporting, tracing, metrics. They run in the worker process, in
registration order, and a hook that raises is logged and otherwise ignored so a
broken hook can never fail an otherwise-healthy job.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, TypeAlias

from .models import Job

logger = logging.getLogger("aioq.hooks")

#: ``async def hook(job: Job) -> None`` (sync callables are also accepted).
StartHook: TypeAlias = Callable[[Job], Awaitable[None] | None]
#: ``async def hook(job: Job, result: Any) -> None``
SuccessHook: TypeAlias = Callable[[Job, Any], Awaitable[None] | None]
#: ``async def hook(job: Job, exc: BaseException) -> None``
FailureHook: TypeAlias = Callable[[Job, BaseException], Awaitable[None] | None]


@dataclass
class Hooks:
    """Registry of lifecycle callbacks held by an :class:`~aioq.app.Aioq`."""

    on_start: list[StartHook] = field(default_factory=list)
    on_success: list[SuccessHook] = field(default_factory=list)
    on_failure: list[FailureHook] = field(default_factory=list)
    on_retry: list[FailureHook] = field(default_factory=list)
    on_dead: list[FailureHook] = field(default_factory=list)

    async def emit(self, event: str, *args: Any) -> None:
        """Invoke every hook registered for *event*, swallowing their errors."""
        for hook in getattr(self, event):
            try:
                result = hook(*args)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception("Hook %r for %s raised", getattr(hook, "__name__", hook), event)
