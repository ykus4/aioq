# CLI

Every command takes an `APP_PATH` — a `module:attribute` path to your `Aioq`
instance, e.g. `myapp.tasks:app`. The module must be importable from the current
directory.

```bash
aioq --help
aioq --version
```

## Running processes

### `aioq worker`

```bash
aioq worker myapp.tasks:app -q default -q emails -c 20
```

| Option | Default | Description |
|---|---|---|
| `-q, --queue` | `default` | Queue to consume. Repeat for several. |
| `-c, --concurrency` | `10` | Max jobs in flight at once. |
| `--log-level` | `info` | Root log level. |
| `--metrics-port` | — | Serve this worker's Prometheus metrics on that port. |

### `aioq dashboard`

```bash
aioq dashboard myapp.tasks:app --host 0.0.0.0 --port 8080
```

## Inspection

### `aioq status`

Queue depth per status, plus worker health.

```console
$ aioq status myapp.tasks:app
QUEUE     completed  failed  pending  running
default   1043       2       17       4
emails    88         0       0        1

Workers: 2 alive / 2 registered
  ● 7f3c1a9e-...  queues=default,emails
  ● b2d90f41-...  queues=default
```

### `aioq jobs`

Most recently enqueued first.

```bash
aioq jobs myapp.tasks:app                      # 20 most recent
aioq jobs myapp.tasks:app -s failed -n 50      # 50 most recent failures
aioq jobs myapp.tasks:app -q emails            # one queue
```

### `aioq show`

Every field of one job.

```bash
aioq show myapp.tasks:app 7f3c1a9e-4b21-4c8e-9f10-2a5d6e8b1c03
```

## Mutations

### `aioq retry`

Re-enqueues a `failed` or `cancelled` job, or replays a `dead` one — it picks the
right operation for the job's current status.

```bash
aioq retry myapp.tasks:app <job-id>
```

### `aioq cancel`

Cancels a job that has not started yet (`pending`, `waiting` or `retrying`). A
job already running cannot be cancelled.

```bash
aioq cancel myapp.tasks:app <job-id>
```

### `aioq purge`

Deletes finished job records. Prompts unless you pass `--yes`.

```bash
aioq purge myapp.tasks:app                              # finished > 1 day ago
aioq purge myapp.tasks:app --older-than 604800 --yes    # finished > 1 week ago
aioq purge myapp.tasks:app -s failed -s cancelled --yes # only these statuses
```

`--older-than` is in seconds and compares against `completed_at` (falling back to
`enqueued_at`). Only terminal statuses — `completed`, `failed`, `cancelled`,
`dead` — are ever eligible, so a purge can never delete queued work.

Worth running on a schedule: the SQL backends keep every row until told
otherwise, and Redis only expires jobs that saved a result.

```python
# Or from inside your app, e.g. as a cron task:
@app.cron("0 4 * * *")
async def nightly_purge(ctx):
    removed = await ctx["broker"].purge(older_than=7 * 86400)
    logger.info("purged %d job records", removed)
```
