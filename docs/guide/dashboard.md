# Dashboard

aioq ships with a FastAPI-based dashboard that provides real-time visibility into your queues, workers, and jobs.

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
| Worker | Worker UUID that executed the job |
| Status | Current status |
| Enqueued at | When the job was created |
| Started at | When execution began |
| Completed at | When execution finished |
| Retries | Current retry count / max retries |
| Arguments | JSON-formatted `args` and `kwargs` |
| Result | JSON-formatted return value (if `save_result=True`) |
| Error | Exception message for failed jobs |

**Retry button** — visible for `failed` and `cancelled` jobs. Re-enqueues as a fresh `pending` job.

**Cancel button** — visible for `pending` and `retrying` jobs. Prevents the job from executing.

Auto-refreshes every 3 seconds while the job is in `pending` or `running` state.

## REST API

The dashboard exposes a REST API for programmatic access or HTMX partial updates:

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/stats` | Queue stats + worker list |
| `GET` | `/api/jobs` | Paginated job list (`queue`, `status`, `limit`, `offset`) |
| `GET` | `/api/jobs/{id}` | Single job detail |
| `POST` | `/api/jobs/{id}/cancel` | Cancel a pending/retrying job |
| `POST` | `/api/jobs/{id}/retry` | Retry a failed/cancelled job |

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

Configure Prometheus to scrape the dashboard:

```yaml
scrape_configs:
  - job_name: aioq
    static_configs:
      - targets: ["localhost:8080"]
```
