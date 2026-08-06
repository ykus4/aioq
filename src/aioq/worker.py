from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from .app import Aioq
from .constants import (
    CRON_TICK_INTERVAL,
    DEFAULT_QUEUE,
    DEQUEUE_TIMEOUT,
    WORKER_HEARTBEAT_INTERVAL,
)
from .exceptions import JobTimeoutError, UnknownTaskError
from .models import Job, JobStatus
from .retry import compute_retry_delay

if TYPE_CHECKING:
    from .cron import CronDef
    from .metrics import WorkerMetrics

logger = logging.getLogger("aioq.worker")

# How long to back off when the broker itself is failing, so a worker does not
# spin against a dead Redis at full speed.
_BROKER_ERROR_BACKOFF = 1.0


class Worker:
    """Pulls jobs off the broker and runs them."""

    def __init__(
        self,
        app: Aioq,
        queues: list[str] | None = None,
        concurrency: int = 10,
        heartbeat_interval: float = WORKER_HEARTBEAT_INTERVAL,
        metrics: WorkerMetrics | None = None,
    ):
        self.app = app
        self.queues = queues or [DEFAULT_QUEUE]
        self.concurrency = concurrency
        self.heartbeat_interval = heartbeat_interval
        self.metrics = metrics
        self.worker_id = str(uuid.uuid4())
        self._semaphore = asyncio.Semaphore(concurrency)
        # Set by stop(), never cleared: a stop requested before run() is
        # scheduled must still be honoured rather than silently lost.
        self._stop_requested = False
        self._jobs: set[asyncio.Task] = set()
        self._cron_tasks: set[asyncio.Task] = set()

    @property
    def is_running(self) -> bool:
        return not self._stop_requested

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> None:
        broker = self.app.broker

        await broker.connect()
        await broker.register_worker(self.worker_id, self.queues)
        logger.info(
            "Worker %s started. queues=%s concurrency=%d",
            self.worker_id,
            self.queues,
            self.concurrency,
        )

        self._install_signal_handlers()
        background = [
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._cron_loop()),
        ]

        try:
            await self._main_loop()
        finally:
            for task in background:
                task.cancel()
            await asyncio.gather(*background, return_exceptions=True)
            await self._drain()
            with contextlib.suppress(Exception):
                await broker.deregister_worker(self.worker_id)
            await broker.disconnect()
            logger.info("Worker %s stopped.", self.worker_id)

    async def _main_loop(self) -> None:
        broker = self.app.broker
        while not self._stop_requested:
            # Take the concurrency slot *before* claiming a job: claiming first
            # would hold a job hostage while we wait for a slot, and with the
            # Redis backend that job is already off the queue.
            await self._semaphore.acquire()
            try:
                job = await broker.dequeue(self.queues, timeout=DEQUEUE_TIMEOUT)
            except asyncio.CancelledError:
                self._semaphore.release()
                raise
            except Exception:
                self._semaphore.release()
                logger.exception("Failed to dequeue; retrying in %.1fs", _BROKER_ERROR_BACKOFF)
                await asyncio.sleep(_BROKER_ERROR_BACKOFF)
                continue

            if job is None:
                self._semaphore.release()
                # Yield a scheduler turn. Bundled brokers block for `timeout`,
                # but a custom one may return instantly, and without this the
                # loop would spin without ever letting the shutdown check, the
                # heartbeat or the cron loop run.
                await asyncio.sleep(0)
                continue

            task = asyncio.create_task(self._process(job))
            self._jobs.add(task)
            task.add_done_callback(self._on_job_done)

    def _on_job_done(self, task: asyncio.Task) -> None:
        self._jobs.discard(task)
        self._semaphore.release()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._request_stop)
            except (NotImplementedError, RuntimeError):
                # Windows, or not running in the main thread. The embedding
                # application is responsible for calling stop() itself.
                logger.debug("Cannot install handler for %s in this environment", sig)

    def _request_stop(self) -> None:
        logger.info("Shutdown signal received. Finishing in-flight jobs…")
        self._stop_requested = True

    def stop(self) -> None:
        """Ask the worker to finish in-flight jobs and shut down.

        Safe to call before :meth:`run` — the worker will then start up, drain
        nothing and exit.
        """
        self._request_stop()

    async def _drain(self) -> None:
        """Let in-flight jobs finish, then cancel leftover cron tasks."""
        if self._jobs:
            logger.info("Waiting for %d in-flight jobs to finish…", len(self._jobs))
            await asyncio.gather(*self._jobs, return_exceptions=True)
        if self._cron_tasks:
            for task in list(self._cron_tasks):
                task.cancel()
            await asyncio.gather(*self._cron_tasks, return_exceptions=True)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            with contextlib.suppress(Exception):
                await self.app.broker.heartbeat_worker(self.worker_id)

    # ------------------------------------------------------------------
    # Cron
    # ------------------------------------------------------------------

    async def _cron_loop(self) -> None:
        """Fire cron tasks at their scheduled times.

        Every worker computes the same occurrence timestamps, then competes for
        a broker-held lock on each one, so an occurrence runs exactly once no
        matter how many workers are up.
        """
        crons = self.app.crons
        if not crons:
            return

        schedule: dict[str, tuple[CronDef, float]] = {
            cron.name: (cron, cron.next_run()) for cron in crons
        }

        while True:
            now = time.time()
            for name, (cron_def, occurrence) in list(schedule.items()):
                if now < occurrence:
                    continue
                schedule[name] = (cron_def, cron_def.next_run(after=occurrence))
                await self._maybe_fire_cron(cron_def, occurrence)
            await asyncio.sleep(CRON_TICK_INTERVAL)

    async def _maybe_fire_cron(self, cron_def: CronDef, occurrence: float) -> None:
        try:
            won = await self.app.broker.acquire_cron_lock(cron_def.lock_key(occurrence))
        except Exception:
            logger.exception("Could not acquire cron lock for %s; skipping", cron_def.name)
            return

        if not won:
            logger.debug("Cron %s already claimed by another worker", cron_def.name)
            return

        logger.info("Firing cron task: %s", cron_def.name)
        task = asyncio.create_task(self._run_cron(cron_def))
        self._cron_tasks.add(task)
        task.add_done_callback(self._cron_tasks.discard)

    async def _run_cron(self, cron_def: CronDef) -> None:
        ctx: dict[str, Any] = {
            "worker_id": self.worker_id,
            "broker": self.app.broker,
            "cron": cron_def.name,
        }
        try:
            await cron_def(ctx)
        except Exception:
            logger.exception("Cron task %s failed", cron_def.name)

    # ------------------------------------------------------------------
    # Job execution
    # ------------------------------------------------------------------

    async def _process(self, job: Job) -> None:
        task_def = self.app.get_task(job.task_name)
        if task_def is None:
            logger.error("Unknown task: %s — marking job %s as failed", job.task_name, job.id)
            await self._fail(job, UnknownTaskError(f"Unknown task: {job.task_name}"))
            return

        # A job can be cancelled after it was claimed but before it starts.
        fresh = await self.app.broker.get_job(job.id)
        if fresh is not None and fresh.status == JobStatus.cancelled:
            logger.info("Job %s was cancelled before execution, skipping", job.id)
            return

        job.status = JobStatus.running
        job.started_at = datetime.now(UTC)
        job.worker_id = self.worker_id
        job.error = None
        await self.app.broker.update_job(job)
        await self.app.hooks.emit("on_start", job)
        if self.metrics:
            self.metrics.record_start(job)

        started = time.monotonic()
        try:
            result = await self._invoke(task_def, job)
        except Exception as exc:
            await self._handle_failure(job, exc)
        else:
            await self._handle_success(job, result)
        finally:
            if self.metrics:
                self.metrics.record_finish(job, time.monotonic() - started)

    async def _invoke(self, task_def: Any, job: Job) -> Any:
        """Run the task function, enforcing its timeout if it has one."""
        ctx: dict[str, Any] = {
            "worker_id": self.worker_id,
            "job_id": job.id,
            "job": job,
            "broker": self.app.broker,
        }
        coro = task_def(ctx, *job.args, **job.kwargs)
        if job.timeout is None:
            return await coro
        try:
            return await asyncio.wait_for(coro, job.timeout)
        except TimeoutError as exc:
            raise JobTimeoutError(f"Job exceeded its {job.timeout}s timeout") from exc

    async def _handle_success(self, job: Job, result: Any) -> None:
        job.status = JobStatus.completed
        job.completed_at = datetime.now(UTC)
        if job.save_result:
            job.result = result
        await self.app.broker.update_job(job)
        logger.info("Job %s (%s) completed", job.id, job.task_name)
        await self.app.hooks.emit("on_success", job, result)

    async def _handle_failure(self, job: Job, exc: Exception) -> None:
        logger.exception("Job %s (%s) failed: %s", job.id, job.task_name, exc)
        if job.retries < job.max_retries:
            await self._schedule_retry(job, exc)
        elif job.dead_letter_queue:
            await self._move_to_dlq(job, exc)
        else:
            await self._fail(job, exc)

    async def _schedule_retry(self, job: Job, exc: Exception) -> None:
        """Re-queue the job as deferred instead of sleeping on it.

        Sleeping would pin a concurrency slot for the whole backoff, which with
        exponential backoff can be minutes of idle capacity.
        """
        job.retries += 1
        job.status = JobStatus.retrying
        job.error = str(exc)
        delay = compute_retry_delay(
            job.retry_delay,
            job.retries,
            backoff=job.retry_backoff,
            backoff_max=job.retry_backoff_max,
        )
        job.run_at = datetime.now(UTC) + timedelta(seconds=delay) if delay > 0 else None
        job.started_at = None
        job.worker_id = None

        await self.app.broker.enqueue(job)
        logger.info(
            "Job %s re-enqueued in %.1fs (attempt %d/%d)",
            job.id,
            delay,
            job.retries,
            job.max_retries,
        )
        if self.metrics:
            self.metrics.record_retry(job)
        await self.app.hooks.emit("on_retry", job, exc)

    async def _move_to_dlq(self, job: Job, exc: Exception) -> None:
        job.status = JobStatus.dead
        job.error = str(exc)
        job.completed_at = datetime.now(UTC)
        job.queue = job.dead_letter_queue or job.queue
        await self.app.broker.update_job(job)
        logger.info(
            "Job %s (%s) moved to DLQ %s after exhausting retries",
            job.id,
            job.task_name,
            job.queue,
        )
        await self.app.hooks.emit("on_dead", job, exc)

    async def _fail(self, job: Job, exc: BaseException) -> None:
        job.status = JobStatus.failed
        job.error = str(exc)
        job.completed_at = datetime.now(UTC)
        await self.app.broker.update_job(job)
        await self.app.hooks.emit("on_failure", job, exc)
