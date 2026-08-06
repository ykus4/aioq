# Monitoring

Install the extra:

```bash
pip install "aioq[prometheus]"
```

Metrics come from two places, because they answer different questions and live in
different processes.

| Where | Scrape from | Answers |
|---|---|---|
| Dashboard `/metrics` | any dashboard process | How much work is queued right now? |
| Worker `--metrics-port` | each worker | How fast is work being processed, and does it fail? |

## Queue gauges (dashboard)

The dashboard reads state out of the broker, so one dashboard reports for the
whole cluster.

```bash
aioq dashboard myapp.tasks:app --port 8080
curl localhost:8080/metrics
```

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `aioq_jobs_total` | gauge | `queue`, `status` | Jobs currently in that state |
| `aioq_workers_total` | gauge | — | Registered workers |
| `aioq_workers_alive` | gauge | — | Workers that heartbeated recently |

```promql
# Backlog per queue
sum by (queue) (aioq_jobs_total{status=~"pending|waiting|retrying"})

# Alert if workers stop heartbeating
aioq_workers_alive == 0
```

## Execution counters (worker)

Counters and durations only exist where jobs actually run, so each worker serves
its own endpoint:

```bash
aioq worker myapp.tasks:app --metrics-port 9100
```

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `aioq_job_starts_total` | counter | `queue`, `task` | Jobs this worker began |
| `aioq_job_finishes_total` | counter | `queue`, `task`, `status` | Jobs finished, by outcome |
| `aioq_job_retries_total` | counter | `queue`, `task` | Retries this worker scheduled |
| `aioq_job_duration_seconds` | histogram | `queue`, `task` | Execution time |

```promql
# Failure rate per task
sum by (task) (rate(aioq_job_finishes_total{status="failed"}[5m]))
  / sum by (task) (rate(aioq_job_finishes_total[5m]))

# p95 duration
histogram_quantile(0.95, sum by (le, task) (rate(aioq_job_duration_seconds_bucket[5m])))
```

Point Prometheus at every worker — the counters are per-process, so aggregate with
`sum by (...)` rather than expecting one worker to report the fleet.

## Embedding the collectors

```python
from aioq.metrics import AioqCollector, WorkerMetrics, make_registry

# Queue gauges, refreshed on demand from any async context:
collector = AioqCollector()
registry = make_registry(collector)
await collector.update(app.broker)

# Execution counters, passed to the worker you construct yourself:
metrics = WorkerMetrics()
worker = Worker(app, metrics=metrics)
metrics.serve(9100)
```

`WorkerMetrics` uses its own registry rather than the global default, so several
workers can coexist in one process without colliding on metric names.

## Health checks

The dashboard exposes a liveness probe that confirms it can reach the broker:

```bash
curl localhost:8080/healthz    # {"status":"ok"}, or 503 with the error
```

## Custom metrics via hooks

For anything the built-ins do not cover, use [lifecycle hooks](hooks.md):

```python
@app.on_job_success
async def record(job, result):
    MY_HISTOGRAM.labels(job.task_name).observe(job.duration)
```
