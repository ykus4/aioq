from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ..app import Aarq
from ..models import JobStatus

try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    from ..metrics import AioqCollector, make_registry

    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

_TEMPLATES_DIR = Path(__file__).parent / "templates"
_STATIC_DIR = Path(__file__).parent / "static"


def create_dashboard(app: Aarq) -> FastAPI:
    if not app.dashboard_enabled:
        raise RuntimeError(
            "Dashboard is disabled for this Aarq instance (dashboard_enabled=False)."
        )
    """Create and return a FastAPI app serving the aioq dashboard."""

    dashboard = FastAPI(title="aioq dashboard")
    templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

    if _PROMETHEUS_AVAILABLE:
        _collector = AioqCollector()
        _registry = make_registry(_collector)

    if _STATIC_DIR.exists():
        dashboard.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    # ------------------------------------------------------------------
    # Pages
    # ------------------------------------------------------------------

    @dashboard.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        broker = app.broker
        stats = await broker.queue_stats()
        workers = await broker.list_workers()
        return templates.TemplateResponse(
            "index.html",
            {"request": request, "stats": stats, "workers": workers},
        )

    @dashboard.get("/jobs", response_class=HTMLResponse)
    async def jobs_page(
        request: Request,
        queue: str | None = None,
        status: str | None = None,
        page: int = 1,
        per_page: int = 20,
    ):
        broker = app.broker
        status_enum = JobStatus(status) if status else None
        jobs = await broker.list_jobs(
            queue=queue,
            status=status_enum,
            limit=per_page,
            offset=(page - 1) * per_page,
        )
        stats = await broker.queue_stats()
        return templates.TemplateResponse(
            "jobs.html",
            {
                "request": request,
                "jobs": jobs,
                "stats": stats,
                "current_queue": queue,
                "current_status": status,
                "page": page,
                "per_page": per_page,
                "statuses": [s.value for s in JobStatus],
            },
        )

    @dashboard.get("/jobs/{job_id}", response_class=HTMLResponse)
    async def job_detail(request: Request, job_id: str):
        job = await app.broker.get_job(job_id)
        return templates.TemplateResponse(
            "job_detail.html",
            {"request": request, "job": job},
        )

    # ------------------------------------------------------------------
    # SSE — real-time stats push
    # ------------------------------------------------------------------

    @dashboard.get("/sse/stats")
    async def sse_stats(request: Request) -> StreamingResponse:
        async def event_generator() -> AsyncGenerator[str, None]:
            while True:
                if await request.is_disconnected():
                    break
                stats = await app.broker.queue_stats()
                workers = await app.broker.list_workers()
                payload = json.dumps({"stats": stats, "workers": workers})
                yield f"data: {payload}\n\n"
                await asyncio.sleep(2)

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # ------------------------------------------------------------------
    # REST API (for HTMX partial updates)
    # ------------------------------------------------------------------

    @dashboard.get("/api/stats")
    async def api_stats():
        stats = await app.broker.queue_stats()
        workers = await app.broker.list_workers()
        return {"stats": stats, "workers": workers}

    @dashboard.get("/api/jobs")
    async def api_jobs(
        queue: str | None = None,
        status: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ):
        status_enum = JobStatus(status) if status else None
        jobs = await app.broker.list_jobs(
            queue=queue, status=status_enum, limit=limit, offset=offset
        )
        return [j.model_dump_json_safe() for j in jobs]

    @dashboard.get("/api/jobs/{job_id}")
    async def api_job(job_id: str):
        job = await app.broker.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return job.model_dump_json_safe()

    @dashboard.post("/api/jobs/{job_id}/cancel")
    async def api_cancel_job(job_id: str):
        cancelled = await app.broker.cancel_job(job_id)
        if not cancelled:
            raise HTTPException(
                status_code=409,
                detail="Job cannot be cancelled (not in pending/retrying state or not found)",
            )
        return {"cancelled": True, "job_id": job_id}

    @dashboard.post("/api/jobs/{job_id}/retry")
    async def api_retry_job(job_id: str):
        retried = await app.broker.retry_job(job_id)
        if not retried:
            raise HTTPException(
                status_code=409,
                detail="Job cannot be retried (not in failed/cancelled state or not found)",
            )
        return {"retried": True, "job_id": job_id}

    # ------------------------------------------------------------------
    # Prometheus metrics
    # ------------------------------------------------------------------

    @dashboard.get("/metrics")
    async def metrics_endpoint():
        if not _PROMETHEUS_AVAILABLE:
            raise HTTPException(status_code=501, detail="prometheus-client not installed")

        await _collector.update(app.broker)
        output = generate_latest(_registry)
        return Response(content=output, media_type=CONTENT_TYPE_LATEST)

    return dashboard
