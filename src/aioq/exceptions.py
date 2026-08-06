"""Exception types raised by aioq itself."""

from __future__ import annotations


class AioqError(Exception):
    """Base class for every aioq-specific error."""


class UnknownTaskError(AioqError):
    """A job names a task this worker has not registered.

    Usually means the worker was started against a module that does not import
    the task, or a task was renamed while jobs for the old name were queued.
    """


class JobTimeoutError(AioqError, TimeoutError):
    """A job exceeded the ``timeout`` configured on its task.

    Subclasses :class:`TimeoutError` so existing ``except TimeoutError``
    handlers keep working.
    """
