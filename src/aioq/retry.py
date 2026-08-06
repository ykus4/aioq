"""Retry delay computation."""

from __future__ import annotations

import random

from .constants import DEFAULT_RETRY_BACKOFF_MAX


def compute_retry_delay(
    base_delay: float,
    attempt: int,
    *,
    backoff: bool = False,
    backoff_max: float = DEFAULT_RETRY_BACKOFF_MAX,
    jitter: bool = True,
) -> float:
    """Return how long to wait before retry *attempt*.

    *attempt* is 1-based: the delay before the first retry is ``attempt=1``.

    With ``backoff=False`` the delay is always *base_delay* (the historical
    behaviour). With ``backoff=True`` it grows exponentially — ``base_delay *
    2 ** (attempt - 1)`` — capped at *backoff_max*.

    *jitter* applies "full jitter" (a uniform pick between 0 and the computed
    delay) which spreads out retries of jobs that failed together, e.g. every
    job that hit the same downstream outage.
    """
    if base_delay <= 0:
        return 0.0
    if attempt < 1:
        attempt = 1

    delay = min(base_delay * (2 ** (attempt - 1)), backoff_max) if backoff else base_delay

    if jitter and backoff:
        delay = random.uniform(0, delay)  # noqa: S311 — not used for security

    return delay
