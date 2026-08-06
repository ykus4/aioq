"""Shared defaults used across the app, worker and every broker backend.

Keeping these in one place stops the three broker implementations from drifting
apart (they each used to define their own ``_WORKER_TTL``).
"""

from __future__ import annotations

#: Queue used when a task does not name one.
DEFAULT_QUEUE = "default"

#: A worker is considered dead if it has not sent a heartbeat within this many
#: seconds. Workers must heartbeat more frequently than this.
WORKER_TTL = 30.0

#: How often a worker sends its heartbeat.
WORKER_HEARTBEAT_INTERVAL = 10.0

#: How long ``dequeue()`` waits for a job before returning ``None``.
DEQUEUE_TIMEOUT = 2.0

#: Polling interval for brokers that cannot block on a queue (SQL backends).
SQL_POLL_INTERVAL = 0.5

#: How long a cron fire-lock is held. Must exceed the cron scheduler tick so
#: that two workers cannot fire the same occurrence.
CRON_LOCK_TTL = 60

#: Cron scheduler tick interval.
CRON_TICK_INTERVAL = 1.0

#: Default TTL (seconds) for a saved job result.
DEFAULT_RESULT_TTL = 3600

#: Upper bound applied to exponential retry backoff.
DEFAULT_RETRY_BACKOFF_MAX = 600.0
