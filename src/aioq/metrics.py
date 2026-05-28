from __future__ import annotations

from prometheus_client import CollectorRegistry
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

from .backends.base import BaseBroker


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

    def collect(self):  # type: ignore[override]
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
            "Number of alive workers",
        )
        workers_gauge.add_metric([], len(self._workers))
        yield workers_gauge


def make_registry(collector: AioqCollector) -> CollectorRegistry:
    """Create an isolated CollectorRegistry pre-registered with *collector*."""
    registry = CollectorRegistry(auto_describe=True)
    registry.register(collector)
    return registry
