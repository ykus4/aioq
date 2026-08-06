# Changelog

## 0.5.0

Existing code keeps working: `Aarq` is still exported, no public method changed
signature, and the SQL backends migrate their own tables on `connect()`.

### Added

- **Job timeouts.** `@app.task(timeout=30)` cancels a job that overruns and fails
  it with `JobTimeoutError`, which then retries and reaches the DLQ like any
  other error. Previously a hung task held a concurrency slot forever.
- **Exponential backoff.** `retry_backoff=True` grows the retry delay per attempt
  with full jitter, capped at `retry_backoff_max`.
- **`MemoryBroker`.** A complete in-process backend for tests and local
  development — no Redis, no database, full feature parity.
- **Lifecycle hooks.** `@app.on_job_start`, `on_job_success`, `on_job_retry`,
  `on_job_failure`, `on_job_dead`. Sync or async; a hook that raises is logged and
  cannot fail the job.
- **CLI commands.** `aioq status`, `jobs`, `show`, `retry`, `cancel`, `purge`,
  plus `--version` and clearer errors when an app path cannot be loaded.
- **Worker metrics.** `aioq worker --metrics-port 9100` exposes per-job counters
  and a duration histogram. The dashboard gains `aioq_workers_alive`, `/healthz`
  and `GET /api/dead`.
- **`broker.purge(older_than, statuses)`** for deleting old finished job records,
  wired to `aioq purge`.
- **`broker.acquire_cron_lock()`** on every bundled backend.
- `Job.duration`, `Job.is_terminal`, and the `TERMINAL_STATUSES` /
  `CANCELLABLE_STATUSES` / `RETRIABLE_STATUSES` sets.
- `Aioq` is now the canonical application class name. `Aarq` remains an alias.

### Fixed

- **`dead_letter_queue` was never persisted by the SQL backends** — the column did
  not exist, so DLQ routing silently did nothing on PostgreSQL and MySQL.
- **Retries never ran on the SQL backends.** A retry was stored with status
  `retrying`, but `dequeue()` only claimed `pending`, so the job was dropped.
  Both backends now claim `pending` and `retrying`.
- **MySQL `connect()` failed outright on MySQL 8**: `CREATE INDEX IF NOT EXISTS`
  is MariaDB-only syntax. Schema upgrades now check `information_schema` first.
- **MySQL compared naive-UTC timestamps against `NOW()`**, which follows the
  session time zone — skewing every deferred job and heartbeat by the session
  offset. All comparisons now use `UTC_TIMESTAMP()`.
- **Deferred Redis jobs lost their priority**, always being promoted into the
  priority-0 list. Deferred jobs are now kept per priority tier.
- **Redis dequeued queue-major rather than priority-major**, so a low-priority job
  in the first queue beat an urgent one in a later queue.
- **`result_ttl` did nothing.** Saved results now expire on Redis, and their index
  entries are reaped so the expired jobs stop showing up in queue counts.
- **`register_worker` put a TTL on the entire Redis workers hash**, so every
  worker could disappear from the dashboard at once. Stale entries are now pruned
  individually.
- **A re-queued job could be counted under two statuses at once** in
  `queue_stats()`, because `enqueue()` did not clear the old status index.
- **`queue_stats()` truncated queue names containing a colon.**
- **A dependency completing between the readiness check and the write left a job
  waiting forever.** Both the Redis and SQL paths now close that race.
- **A saved result of `0`, `False` or `""` came back as `None`** from the SQL
  backends.
- `stop()` called before `run()` was scheduled was silently discarded and the
  worker started anyway.
- A worker whose `dequeue()` raised crashed the process; it now logs, backs off
  and retries.
- A broker returning `None` from `dequeue()` without blocking starved the event
  loop, wedging shutdown, heartbeats and cron.
- `Worker.run()` no longer dies if `loop.add_signal_handler` is unavailable
  (Windows, non-main thread).
- The dashboard returned 500 on an unknown `?status=` value (now 400) and rendered
  a broken page for a missing job (now 404).
- Registering two tasks with the same name silently shadowed the first; it now
  raises `ValueError`.

### Changed

- **Retries are rescheduled, not slept on.** The worker sets `run_at` and releases
  its concurrency slot immediately instead of blocking it for the whole
  `retry_delay`. A job in `retrying` is therefore "waiting for its next attempt",
  and long backoffs cost no worker capacity.
- **Cron occurrences fire exactly once across the fleet.** Workers compete for a
  broker-held lock per occurrence; previously every worker ran every cron.
- `enqueue_many()` accepts `defer_until` and `depends_on`.
- `enqueue()` raises `ValueError` if given both `defer_by` and `defer_until`.
- Worker context (`ctx`) now includes the full `job` object.
- The SQL backends share a `SQLBroker` base class holding the row mapping, filter
  building and dependency resolution they had each duplicated.
