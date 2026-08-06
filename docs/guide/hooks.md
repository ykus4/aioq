# Lifecycle Hooks

Hooks let you observe job execution across every task without wrapping each one
— logging, error reporting, tracing, custom metrics. They run in the worker
process, in registration order.

```python
from aioq import Aioq
from aioq.backends import RedisBroker

app = Aioq(broker=RedisBroker())


@app.on_job_start
async def log_start(job):
    logger.info("starting %s (%s)", job.task_name, job.id)


@app.on_job_success
async def log_success(job, result):
    logger.info("%s finished in %.2fs", job.task_name, job.duration)


@app.on_job_failure
async def report_failure(job, exc):
    sentry_sdk.capture_exception(exc)
```

## Available hooks

| Decorator | Signature | Fires when |
|---|---|---|
| `@app.on_job_start` | `(job)` | Just before the task function is called |
| `@app.on_job_success` | `(job, result)` | The task returned without raising |
| `@app.on_job_retry` | `(job, exc)` | The attempt failed and will be retried |
| `@app.on_job_failure` | `(job, exc)` | The job failed for good, with no DLQ |
| `@app.on_job_dead` | `(job, exc)` | The job exhausted its retries and moved to its DLQ |

`on_failure` and `on_dead` are mutually exclusive: a job with a
`dead_letter_queue` ends in `on_dead`, one without ends in `on_failure`. Neither
fires for an attempt that will be retried — use `on_job_retry` for those.

At the point a hook runs, `job` already carries its new status, so
`job.status`, `job.retries` and `job.duration` are all up to date.

## Sync hooks work too

A plain function is fine; only awaitables are awaited.

```python
@app.on_job_success
def count(job, result):
    STATSD.incr(f"jobs.{job.task_name}.ok")
```

## Errors in hooks are contained

A hook that raises is logged and otherwise ignored, so instrumentation can never
fail an otherwise-healthy job:

```python
@app.on_job_start
async def broken(job):
    raise RuntimeError("my metrics backend is down")


# The job still runs and still completes.
```

## Multiple hooks

Register as many as you like per event; they run in the order registered.

```python
@app.on_job_failure
async def notify_slack(job, exc): ...


@app.on_job_failure
async def record_metric(job, exc): ...
```

## Hooks vs. middleware

Hooks observe; they cannot change a job's outcome, swap its arguments, or
suppress an exception. To wrap execution itself, wrap the task function:

```python
def traced(fn):
    @functools.wraps(fn)
    async def wrapper(ctx, *args, **kwargs):
        with tracer.start_span(fn.__name__):
            return await fn(ctx, *args, **kwargs)

    return wrapper


@app.task()
@traced
async def my_task(ctx): ...
```

!!! note
    Put `@app.task()` outermost. It reads `__module__`/`__qualname__` off what it
    decorates to build the task name, and `functools.wraps` keeps those pointing
    at your original function.
