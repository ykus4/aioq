from __future__ import annotations

import asyncio
import contextlib
import functools
import importlib
import logging
from collections.abc import Callable, Coroutine
from typing import Any

import click
import uvicorn

from .app import Aioq
from .backends.base import BaseBroker
from .models import TERMINAL_STATUSES, Job, JobStatus
from .worker import Worker


def _load_app(path: str) -> Aioq:
    """Load an Aioq instance from a dotted path like ``mymodule:app``."""
    if ":" not in path:
        raise click.BadParameter(
            f"{path!r} must be given as 'module:attribute', e.g. 'myapp.tasks:app'"
        )
    module_path, attr = path.rsplit(":", 1)
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise click.ClickException(f"Could not import {module_path!r}: {exc}") from exc
    try:
        app = getattr(module, attr)
    except AttributeError as exc:
        raise click.ClickException(f"{module_path!r} has no attribute {attr!r}") from exc
    if not isinstance(app, Aioq):
        raise click.ClickException(f"{path} is a {type(app).__name__}, expected an Aioq instance")
    return app


def with_broker(fn: Callable[..., Coroutine[Any, Any, None]]) -> Callable[..., None]:
    """Wrap an async command body so it runs with a connected broker.

    The wrapped function receives ``(app, broker, **params)``.
    """

    @functools.wraps(fn)
    def wrapper(app_path: str, **params: Any) -> None:
        app = _load_app(app_path)

        async def main() -> None:
            async with app.broker as broker:
                await fn(app, broker, **params)

        asyncio.run(main())

    return wrapper


@click.group()
@click.version_option(package_name="aioq")
def cli():
    """aioq — async job queue CLI"""


# ----------------------------------------------------------------------
# Long-running processes
# ----------------------------------------------------------------------


@cli.command()
@click.argument("app_path")
@click.option("--queue", "-q", multiple=True, default=["default"], show_default=True)
@click.option("--concurrency", "-c", default=10, show_default=True)
@click.option("--log-level", default="info", show_default=True)
@click.option(
    "--metrics-port",
    type=int,
    default=None,
    help="Serve this worker's Prometheus metrics on the given port.",
)
def worker(
    app_path: str,
    queue: tuple[str, ...],
    concurrency: int,
    log_level: str,
    metrics_port: int | None,
):
    """Start a worker.

    APP_PATH: dotted path to an Aioq instance, e.g. myapp.tasks:app
    """
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app = _load_app(app_path)
    metrics = None
    if metrics_port is not None:
        try:
            from .metrics import WorkerMetrics
        except ImportError as exc:  # pragma: no cover - depends on install extras
            raise click.ClickException(
                "--metrics-port needs prometheus-client: pip install 'aioq[prometheus]'"
            ) from exc
        metrics = WorkerMetrics()
        metrics.serve(metrics_port)

    asyncio.run(Worker(app, queues=list(queue), concurrency=concurrency, metrics=metrics).run())


@cli.command()
@click.argument("app_path")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8080, show_default=True)
@click.option("--reload", is_flag=True, default=False)
def dashboard(app_path: str, host: str, port: int, reload: bool):
    """Start the aioq dashboard.

    APP_PATH: dotted path to an Aioq instance, e.g. myapp.tasks:app
    """
    from .dashboard import create_dashboard

    app = _load_app(app_path)

    if not app.dashboard_enabled:
        raise click.ClickException(
            "Dashboard is disabled for this Aioq instance (dashboard_enabled=False)."
        )

    @contextlib.asynccontextmanager
    async def lifespan(fastapi_app):
        await app.broker.connect()
        yield
        await app.broker.disconnect()

    dash = create_dashboard(app)
    dash.router.lifespan_context = lifespan

    uvicorn.run(dash, host=host, port=port, reload=reload)


# ----------------------------------------------------------------------
# Inspection
# ----------------------------------------------------------------------


@cli.command()
@click.argument("app_path")
@with_broker
async def status(app: Aioq, broker: BaseBroker) -> None:
    """Show queue depth and worker health."""
    stats = await broker.queue_stats()
    workers = await broker.list_workers()

    if not stats:
        click.echo("No queues have any jobs yet.")
    else:
        statuses = sorted({s for per_queue in stats.values() for s in per_queue})
        width = max(len(q) for q in stats) + 2
        click.echo(click.style("QUEUE".ljust(width), bold=True) + "  ".join(statuses))
        for queue in sorted(stats):
            counts = "  ".join(str(stats[queue].get(s, 0)).ljust(len(s)) for s in statuses)
            click.echo(queue.ljust(width) + counts)

    alive = sum(1 for w in workers if w.get("alive"))
    click.echo(f"\nWorkers: {alive} alive / {len(workers)} registered")
    for info in workers:
        marker = click.style("●", fg="green") if info.get("alive") else click.style("○", fg="red")
        click.echo(f"  {marker} {info['worker_id']}  queues={','.join(info['queues'])}")


@cli.command(name="jobs")
@click.argument("app_path")
@click.option("--queue", "-q", default=None, help="Filter by queue.")
@click.option(
    "--status",
    "-s",
    "status_filter",
    type=click.Choice([s.value for s in JobStatus]),
    default=None,
    help="Filter by status.",
)
@click.option("--limit", "-n", default=20, show_default=True)
@with_broker
async def jobs_cmd(
    app: Aioq,
    broker: BaseBroker,
    queue: str | None,
    status_filter: str | None,
    limit: int,
) -> None:
    """List jobs, most recently enqueued first."""
    found = await broker.list_jobs(
        queue=queue,
        status=JobStatus(status_filter) if status_filter else None,
        limit=limit,
    )
    if not found:
        click.echo("No matching jobs.")
        return
    for job in found:
        click.echo(_format_job(job))


@cli.command()
@click.argument("app_path")
@click.argument("job_id")
@with_broker
async def show(app: Aioq, broker: BaseBroker, job_id: str) -> None:
    """Show one job in full."""
    job = await broker.get_job(job_id)
    if job is None:
        raise click.ClickException(f"No job with id {job_id}")
    for key, value in job.model_dump_json_safe().items():
        click.echo(f"{key + ':':<20}{value}")


# ----------------------------------------------------------------------
# Mutations
# ----------------------------------------------------------------------


@cli.command()
@click.argument("app_path")
@click.argument("job_id")
@with_broker
async def retry(app: Aioq, broker: BaseBroker, job_id: str) -> None:
    """Re-enqueue a failed or cancelled job."""
    if await broker.retry_job(job_id):
        click.echo(f"Re-enqueued {job_id}")
        return
    # A dead job needs replay_dead_job rather than retry_job.
    if await broker.replay_dead_job(job_id):
        click.echo(f"Replayed dead job {job_id}")
        return
    raise click.ClickException(f"{job_id} is not in a failed, cancelled or dead state.")


@cli.command()
@click.argument("app_path")
@click.argument("job_id")
@with_broker
async def cancel(app: Aioq, broker: BaseBroker, job_id: str) -> None:
    """Cancel a job that has not started yet."""
    if not await broker.cancel_job(job_id):
        raise click.ClickException(f"{job_id} is not pending, waiting or retrying.")
    click.echo(f"Cancelled {job_id}")


@cli.command()
@click.argument("app_path")
@click.option(
    "--older-than",
    default=86400.0,
    show_default=True,
    help="Only delete jobs that finished at least this many seconds ago.",
)
@click.option(
    "--status",
    "-s",
    "status_filters",
    multiple=True,
    type=click.Choice(sorted(s.value for s in TERMINAL_STATUSES)),
    help="Restrict to these statuses. Defaults to every finished status.",
)
@click.option("--yes", is_flag=True, help="Skip the confirmation prompt.")
@with_broker
async def purge(
    app: Aioq,
    broker: BaseBroker,
    older_than: float,
    status_filters: tuple[str, ...],
    yes: bool,
) -> None:
    """Delete finished job records."""
    statuses = [JobStatus(s) for s in status_filters] or None
    label = ", ".join(status_filters) if status_filters else "all finished statuses"
    if not yes:
        click.confirm(
            f"Delete jobs ({label}) older than {older_than:.0f}s? This cannot be undone.",
            abort=True,
        )
    removed = await broker.purge(older_than, statuses)
    click.echo(f"Purged {removed} job(s).")


def _format_job(job: Job) -> str:
    colors = {
        JobStatus.completed: "green",
        JobStatus.failed: "red",
        JobStatus.dead: "red",
        JobStatus.running: "cyan",
        JobStatus.cancelled: "yellow",
        JobStatus.retrying: "yellow",
    }
    status = click.style(job.status.value.ljust(9), fg=colors.get(job.status))
    when = job.enqueued_at.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{job.id[:8]}  {status}  {when}  {job.queue}/{job.task_name}"
    if job.error:
        line += click.style(f"  — {job.error.splitlines()[0][:60]}", fg="red")
    return line
