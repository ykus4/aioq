# Dashboard

aioq ships with a FastAPI-based dashboard that provides real-time visibility into your queues, workers, and jobs.

## Enabling and disabling

The dashboard is enabled by default. Set `dashboard_enabled=False` on the `Aioq` instance to disable it:

```python
app = Aioq(broker=broker, dashboard_enabled=False)
```

When disabled, `aioq dashboard tasks:app` exits with an error and `create_dashboard(app)` raises `RuntimeError`. This is useful for production deployments where you want to expose the dashboard selectively.

## Standalone mode

```bash
aioq dashboard myapp.tasks:app --port 8080
```

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | Bind address |
| `--port` | `8080` | Port |
| `--reload` | `False` | Enable auto-reload (development) |

## Mount into an existing FastAPI app

```python
from fastapi import FastAPI
from aioq.dashboard import create_dashboard
from myapp.tasks import app as aioq_app

main_app = FastAPI()
dashboard = create_dashboard(aioq_app)
main_app.mount("/aioq", dashboard)
```

The dashboard is then available at `/aioq`.

## Pages

### Overview (`/`)

- Per-queue job counts broken down by status: `pending`, `running`, `completed`, `failed`, `retrying`, `cancelled`
- Worker list with a liveness indicator (green = heartbeat within 30 s) and last heartbeat time
- Updates automatically every 2 seconds via **Server-Sent Events**

### Jobs (`/jobs`)

- Full job list with inline filtering by queue and status
- Paginated (20 per page by default)
- Failed job rows highlighted in red with an inline error preview
- Auto-refreshes every 5 seconds

### Job detail (`/jobs/{job_id}`)

Full metadata for a single job:

| Field | Description |
|---|---|
| ID | UUID |
| Task name | Dotted module path |
| Queue | Queue name |
| Priority | Numeric priority (0 / 5 / 10) |
| Worker | Worker UUID that executed the job |
| DLQ | Dead letter queue name (if configured) |
| Depends on | Links to dependency jobs (if any) |
| Status | Current status |
| Enqueued at | When the job was created |
| Started at | When execution began |
| Completed at | When execution finished |
| Duration | Wall-clock execution time |
| Timeout | Configured execution timeout (if any) |
| Scheduled | `run_at` for deferred jobs |
| Retries | Current retry count / max retries (dot indicator) |
| Arguments | JSON-formatted `args` and `kwargs` |
| Result | JSON-formatted return value (if `save_result=True`) |
| Error | Exception message for failed / dead jobs |

**Cancel button** — visible for `pending`, `retrying`, and `waiting` jobs. Prevents the job from executing.

**Retry button** — visible for `failed` and `cancelled` jobs. Re-enqueues as a fresh `pending` job.

**Replay button** — visible for `dead` jobs (exhausted DLQ). Re-enqueues as a fresh `pending` job.

Auto-refreshes every 3 seconds while the job is in `pending`, `running`, `retrying`, or `waiting` state.

## REST API

The dashboard exposes a REST API for programmatic access or HTMX partial updates:

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/stats` | Queue stats + worker list |
| `GET` | `/api/jobs` | Paginated job list (`queue`, `status`, `limit`, `offset`) |
| `GET` | `/api/jobs/{id}` | Single job detail |
| `GET` | `/api/dead` | Every dead job, optionally filtered by `queue` |
| `POST` | `/api/jobs/{id}/cancel` | Cancel a pending/retrying/waiting job |
| `POST` | `/api/jobs/{id}/retry` | Retry a failed/cancelled job |
| `POST` | `/api/jobs/{id}/replay` | Replay a dead (DLQ-exhausted) job |
| `GET` | `/healthz` | Liveness probe — 200 if the broker is reachable, else 503 |

An unknown `?status=` value returns `400`; a mutating endpoint returns `409` when
the job is not in a state that allows it.

### SSE endpoint

`GET /sse/stats` streams stats and worker info as Server-Sent Events every 2 seconds:

```json
data: {"stats": {"default": {"pending": 3, "running": 1}}, "workers": [...]}
```

## Prometheus metrics

If `prometheus_client` is installed (`pip install "aioq[prometheus]"`), the dashboard exposes a `/metrics` endpoint compatible with Prometheus scraping:

```
GET /metrics
```

Metrics exposed:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `aioq_jobs_total` | Gauge | `queue`, `status` | Job count per queue and status |
| `aioq_workers_total` | Gauge | — | Total registered worker count |
| `aioq_workers_alive` | Gauge | — | Workers that heartbeated recently |

Configure Prometheus to scrape the dashboard:

```yaml
scrape_configs:
  - job_name: aioq
    static_configs:
      - targets: ["localhost:8080"]
```

These are queue-level gauges read from the broker, so one dashboard covers the
whole cluster. Per-job counters and duration histograms live in the workers —
see [Monitoring](monitoring.md).
