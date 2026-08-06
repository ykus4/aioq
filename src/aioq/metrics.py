"""Prometheus integration.

Two separate concerns live here:

* :class:`AioqCollector` — queue-level *gauges* scraped from the dashboard.
  It reads state out of the broker, so any process can serve it.
* :class:`WorkerMetrics` — per-execution *counters and histograms* recorded by a
  worker as it runs jobs. These only exist in the worker process, which is why
  ``aioq worker --metrics-port`` exposes its own scrape endpoint.
"""

from __future__ import annotations

import logging

from prometheus_client import CollectorRegistry, Counter, Histogram, start_http_server
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

from .backends.base import BaseBroker
from .models import Job

logger = logging.getLogger("aioq.metrics")

#: Buckets tuned for job durations, which spread far wider than HTTP latencies.
_DURATION_BUCKETS = (0.005, 0.05, 0.25, 1.0, 5.0, 15.0, 60.0, 300.0, 900.0, float("inf"))


class AioqCollector(Collector):
    """Prometheus collector that exposes aioq job queue statistics.

    Because prometheus_client's ``collect()`` is synchronous, this collector
    caches the last known stats in memory.  Call ``await update(broker)`` from
    an async context (e.g. the /metrics endpoint handler) before generating
    output to refresh the cache.
    """

    def __init__(self) -> None:
        # {queue: {status: count}}
        self._queue_stats: dict[str, dict[str, int]] = {}
        # list of worker dicts
        self._workers: list[dict] = []

    async def update(self, broker: BaseBroker) -> None:
        """Fetch fresh stats from the broker and store them in the cache."""
        self._queue_stats = await broker.queue_stats()
        self._workers = await broker.list_workers()

    # ------------------------------------------------------------------
    # Collector protocol
    # ------------------------------------------------------------------

    def collect(self):
        jobs_gauge = GaugeMetricFamily(
            "aioq_jobs_total",
            "Number of jobs per queue and status",
            labels=["queue", "status"],
        )
        for queue, statuses in self._queue_stats.items():
            for status, count in statuses.items():
                jobs_gauge.add_metric([queue, status], count)
        yield jobs_gauge

        workers_gauge = GaugeMetricFamily(
            "aioq_workers_total",
            "Number of registered workers",
        )
        workers_gauge.add_metric([], len(self._workers))
        yield workers_gauge

        alive_gauge = GaugeMetricFamily(
            "aioq_workers_alive",
            "Number of workers that have heartbeated recently",
        )
        alive_gauge.add_metric([], sum(1 for w in self._workers if w.get("alive")))
        yield alive_gauge


def make_registry(collector: Collector) -> CollectorRegistry:
    """Create an isolated CollectorRegistry pre-registered with *collector*."""
    registry = CollectorRegistry(auto_describe=True)
    registry.register(collector)
    return registry


class WorkerMetrics:
    """Counters and histograms recorded by a worker as it executes jobs.

    Uses its own registry rather than the global default so that several
    workers in one process (tests, embedded use) do not collide on metric
    names.
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry(auto_describe=True)

        self.started = Counter(
            "aioq_job_starts_total",
            "Jobs this worker began executing",
            ["queue", "task"],
            registry=self.registry,
        )
        self.finished = Counter(
            "aioq_job_finishes_total",
            "Jobs this worker finished, by outcome",
            ["queue", "task", "status"],
            registry=self.registry,
        )
        self.duration = Histogram(
            "aioq_job_duration_seconds",
            "Wall-clock execution time per job",
            ["queue", "task"],
            buckets=_DURATION_BUCKETS,
            registry=self.registry,
        )
        self.retries = Counter(
            "aioq_job_retries_total",
            "Retry attempts scheduled by this worker",
            ["queue", "task"],
            registry=self.registry,
        )

    def record_start(self, job: Job) -> None:
        self.started.labels(job.queue, job.task_name).inc()

    def record_finish(self, job: Job, duration: float) -> None:
        self.finished.labels(job.queue, job.task_name, job.status.value).inc()
        self.duration.labels(job.queue, job.task_name).observe(duration)

    def record_retry(self, job: Job) -> None:
        self.retries.labels(job.queue, job.task_name).inc()

    def serve(self, port: int, addr: str = "0.0.0.0") -> None:  # noqa: S104
        """Start a background HTTP server exposing these metrics."""
        start_http_server(port, addr=addr, registry=self.registry)
        logger.info("Worker metrics available at http://%s:%d/metrics", addr, port)
