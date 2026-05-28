from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
from fastapi import FastAPI

from aioq.app import Aarq
from aioq.dashboard.app import create_dashboard

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app(queue_stats: dict, workers: list[dict]) -> FastAPI:
    """Build a dashboard FastAPI app backed by a fully-mocked broker."""
    broker = AsyncMock()
    broker.queue_stats = AsyncMock(return_value=queue_stats)
    broker.list_workers = AsyncMock(return_value=workers)

    aarq = Aarq(broker=broker)
    return create_dashboard(aarq)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_metrics_returns_200():
    app = _make_app(
        queue_stats={"default": {"pending": 3, "running": 1, "completed": 10}},
        workers=[{"worker_id": "w1"}, {"worker_id": "w2"}],
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/metrics")

    assert response.status_code == 200


async def test_metrics_content_type():
    app = _make_app(queue_stats={}, workers=[])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/metrics")

    assert "text/plain" in response.headers["content-type"]


async def test_metrics_jobs_total_per_queue_and_status():
    app = _make_app(
        queue_stats={
            "default": {"pending": 5, "running": 2},
            "email": {"completed": 7},
        },
        workers=[],
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/metrics")

    body = response.text
    assert 'aioq_jobs_total{queue="default",status="pending"} 5.0' in body
    assert 'aioq_jobs_total{queue="default",status="running"} 2.0' in body
    assert 'aioq_jobs_total{queue="email",status="completed"} 7.0' in body


async def test_metrics_workers_total():
    workers = [{"worker_id": "w1"}, {"worker_id": "w2"}, {"worker_id": "w3"}]
    app = _make_app(queue_stats={}, workers=workers)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/metrics")

    body = response.text
    assert "aioq_workers_total 3.0" in body


async def test_metrics_empty_queue_stats():
    app = _make_app(queue_stats={}, workers=[])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/metrics")

    assert response.status_code == 200
    body = response.text
    # No job metric lines when there are no queues
    assert "aioq_jobs_total{" not in body
    # Workers gauge should still appear
    assert "aioq_workers_total 0.0" in body


async def test_metrics_broker_called_on_each_scrape():
    broker = AsyncMock()
    broker.queue_stats = AsyncMock(return_value={})
    broker.list_workers = AsyncMock(return_value=[])

    aarq = Aarq(broker=broker)
    app = create_dashboard(aarq)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.get("/metrics")
        await client.get("/metrics")

    assert broker.queue_stats.call_count == 2
    assert broker.list_workers.call_count == 2
