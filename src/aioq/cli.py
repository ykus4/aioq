from __future__ import annotations

import asyncio
import importlib
import logging

import click
import uvicorn

from .app import Aarq
from .worker import Worker


def _load(path: str) -> object:
    """Load an object from a dotted path like `mymodule:app`."""
    module_path, attr = path.rsplit(":", 1)
    mod = importlib.import_module(module_path)
    return getattr(mod, attr)


@click.group()
def cli():
    """aioq — async job queue CLI"""


@cli.command()
@click.argument("app_path")
@click.option("--queue", "-q", multiple=True, default=["default"], show_default=True)
@click.option("--concurrency", "-c", default=10, show_default=True)
@click.option("--log-level", default="info", show_default=True)
def worker(app_path: str, queue: tuple, concurrency: int, log_level: str):
    """Start a worker.

    APP_PATH: dotted path to Aarq instance, e.g. myapp.tasks:app
    """
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app: Aarq = _load(app_path)  # type: ignore[assignment]
    w = Worker(app, queues=list(queue), concurrency=concurrency)

    asyncio.run(w.run())


@cli.command()
@click.argument("app_path")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8080, show_default=True)
@click.option("--reload", is_flag=True, default=False)
def dashboard(app_path: str, host: str, port: int, reload: bool):
    """Start the aioq dashboard.

    APP_PATH: dotted path to Aarq instance, e.g. myapp.tasks:app
    """
    from .dashboard import create_dashboard

    app: Aarq = _load(app_path)  # type: ignore[assignment]

    if not app.dashboard_enabled:
        raise click.ClickException(
            "Dashboard is disabled for this Aarq instance (dashboard_enabled=False)."
        )

    async def lifespan(fastapi_app):
        await app.broker.connect()
        yield
        await app.broker.disconnect()

    dash = create_dashboard(app)
    # Inject lifespan
    dash.router.lifespan_context = lifespan

    uvicorn.run(dash, host=host, port=port, reload=reload)
