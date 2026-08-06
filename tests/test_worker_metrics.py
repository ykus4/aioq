"""WorkerMetrics: the per-execution counters a worker records."""

from __future__ import annotations

import pytest
from prometheus_client import generate_latest

from aioq import Aioq
from aioq.backends.memory import MemoryBroker
from aioq.metrics import AioqCollector, WorkerMetrics, make_registry
from aioq.models import Job, JobStatus
from aioq.worker import Worker


def sample(status: JobStatus = JobStatus.completed) -> Job:
    return Job(task_name="tasks.add", queue="default", status=status)


def value(metrics: WorkerMetrics, name: str, **labels) -> float | None:
    return metrics.registry.get_sample_value(name, labels)


@pytest.fixture
def metrics():
    return WorkerMetrics()


# ----------------------------------------------------------------------
# Recording
# ----------------------------------------------------------------------


def test_record_start_increments_the_start_counter(metrics):
    metrics.record_start(sample())
    metrics.record_start(sample())

    assert value(metrics, "aioq_job_starts_total", queue="default", task="tasks.add") == 2


def test_record_finish_labels_by_outcome(metrics):
    metrics.record_finish(sample(JobStatus.completed), 0.5)
    metrics.record_finish(sample(JobStatus.failed), 0.1)

    labels = {"queue": "default", "task": "tasks.add"}
    assert value(metrics, "aioq_job_finishes_total", **labels, status="completed") == 1
    assert value(metrics, "aioq_job_finishes_total", **labels, status="failed") == 1


def test_record_finish_observes_duration(metrics):
    metrics.record_finish(sample(), 0.25)
    metrics.record_finish(sample(), 0.75)

    labels = {"queue": "default", "task": "tasks.add"}
    assert value(metrics, "aioq_job_duration_seconds_count", **labels) == 2
    assert value(metrics, "aioq_job_duration_seconds_sum", **labels) == pytest.approx(1.0)


def test_record_retry_increments_the_retry_counter(metrics):
    metrics.record_retry(sample())
    assert value(metrics, "aioq_job_retries_total", queue="default", task="tasks.add") == 1


def test_metrics_are_labelled_per_queue_and_task(metrics):
    metrics.record_start(Job(task_name="a", queue="q1"))
    metrics.record_start(Job(task_name="b", queue="q2"))

    assert value(metrics, "aioq_job_starts_total", queue="q1", task="a") == 1
    assert value(metrics, "aioq_job_starts_total", queue="q2", task="b") == 1


def test_registries_are_isolated():
    """Two WorkerMetrics in one process must not collide on metric names."""
    a, b = WorkerMetrics(), WorkerMetrics()
    a.record_start(sample())

    assert value(a, "aioq_job_starts_total", queue="default", task="tasks.add") == 1
    assert value(b, "aioq_job_starts_total", queue="default", task="tasks.add") is None


def test_exposition_output_contains_the_metric_names(metrics):
    metrics.record_start(sample())
    metrics.record_finish(sample(), 0.1)

    output = generate_latest(metrics.registry).decode()

    assert "aioq_job_starts_total" in output
    assert "aioq_job_finishes_total" in output
    assert "aioq_job_duration_seconds" in output


# ----------------------------------------------------------------------
# Wiring into the worker
# ----------------------------------------------------------------------


async def test_worker_records_a_successful_job(metrics):
    broker = MemoryBroker()
    app = Aioq(broker=broker)

    @app.task()
    async def ok(ctx):
        return 1

    async with broker:
        await ok.enqueue()
        worker = Worker(app, metrics=metrics)
        job = await broker.dequeue(["default"], timeout=0.2)
        await worker._process(job)

    labels = {"queue": "default", "task": ok.name}
    assert value(metrics, "aioq_job_starts_total", **labels) == 1
    assert value(metrics, "aioq_job_finishes_total", **labels, status="completed") == 1
    assert value(metrics, "aioq_job_duration_seconds_count", **labels) == 1


async def test_worker_records_a_retry_and_its_outcome(metrics):
    broker = MemoryBroker()
    app = Aioq(broker=broker)

    @app.task(retries=1, retry_delay=0)
    async def boom(ctx):
        raise ValueError("nope")

    async with broker:
        await boom.enqueue()
        worker = Worker(app, metrics=metrics)
        for _ in range(2):
            job = await broker.dequeue(["default"], timeout=0.2)
            await worker._process(job)

    labels = {"queue": "default", "task": boom.name}
    assert value(metrics, "aioq_job_retries_total", **labels) == 1
    assert value(metrics, "aioq_job_finishes_total", **labels, status="failed") == 1


async def test_worker_without_metrics_still_runs():
    broker = MemoryBroker()
    app = Aioq(broker=broker)

    @app.task()
    async def ok(ctx):
        return 1

    async with broker:
        job = await ok.enqueue()
        claimed = await broker.dequeue(["default"], timeout=0.2)
        await Worker(app, metrics=None)._process(claimed)
        assert (await broker.get_job(job.id)).status == JobStatus.completed


# ----------------------------------------------------------------------
# AioqCollector gauges
# ----------------------------------------------------------------------


async def test_collector_reports_queue_gauges():
    broker = MemoryBroker()
    async with broker:
        await broker.enqueue(Job(task_name="a", queue="default"))
        await broker.register_worker("w1", ["default"])

        collector = AioqCollector()
        await collector.update(broker)

    registry = make_registry(collector)
    output = generate_latest(registry).decode()

    assert registry.get_sample_value("aioq_jobs_total", {"queue": "default", "status": "pending"})
    assert registry.get_sample_value("aioq_workers_total", {}) == 1
    assert registry.get_sample_value("aioq_workers_alive", {}) == 1
    assert "aioq_jobs_total" in output


async def test_collector_counts_dead_workers_separately():
    broker = MemoryBroker()
    async with broker:
        await broker.register_worker("w1", ["default"])
        broker._workers["w1"]["last_heartbeat"] = "2020-01-01T00:00:00+00:00"

        collector = AioqCollector()
        await collector.update(broker)

    registry = make_registry(collector)
    assert registry.get_sample_value("aioq_workers_total", {}) == 1
    assert registry.get_sample_value("aioq_workers_alive", {}) == 0


def test_collector_with_no_data_emits_empty_gauges():
    registry = make_registry(AioqCollector())
    output = generate_latest(registry).decode()
    assert "aioq_workers_total 0.0" in output
