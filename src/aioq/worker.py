from __future__ import annotations

import asyncio
import logging
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from .app import Aarq
from .models import Job, JobStatus

logger = logging.getLogger("aioq.worker")


class Worker:
    def __init__(
        self,
        app: Aarq,
        queues: list[str] | None = None,
        concurrency: int = 10,
        heartbeat_interval: float = 10.0,
    ):
        self.app = app
        self.queues = queues or ["default"]
        self.concurrency = concurrency
        self.heartbeat_interval = heartbeat_interval
        self.worker_id = str(uuid.uuid4())
        self._semaphore: asyncio.Semaphore | None = None
        self._running = False
        self._tasks: set[asyncio.Task] = set()

    async def run(self) -> None:
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._running = True

        broker = self.app.broker
        await broker.connect()
        await broker.register_worker(self.worker_id, self.queues)
        logger.info(
            "Worker %s started. queues=%s concurrency=%d",
            self.worker_id,
            self.queues,
            self.concurrency,
        )

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._request_stop)

        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        cron_task = asyncio.create_task(self._cron_loop())

        try:
            while self._running:
                async with self._semaphore:
                    job = await broker.dequeue(self.queues, timeout=2.0)
                if job is None:
                    continue
                t = asyncio.create_task(self._process(job))
                self._tasks.add(t)
                t.add_done_callback(self._tasks.discard)
        finally:
            heartbeat_task.cancel()
            cron_task.cancel()
            if self._tasks:
                logger.info("Waiting for %d in-flight jobs to finish…", len(self._tasks))
                await asyncio.gather(*self._tasks, return_exceptions=True)
            await broker.deregister_worker(self.worker_id)
            await broker.disconnect()
            logger.info("Worker %s stopped.", self.worker_id)

    def _request_stop(self) -> None:
        logger.info("Shutdown signal received. Finishing in-flight jobs…")
        self._running = False

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            try:
                await self.app.broker.heartbeat_worker(self.worker_id)
            except Exception:
                pass

    async def _cron_loop(self) -> None:
        """Fire cron tasks at their scheduled times."""
        if not self.app._crons:
            return

        # Build next-run schedule: {cron_def: next_unix_timestamp}
        schedule: dict[Any, float] = {c: c.next_run() for c in self.app._crons}

        while True:
            now = time.time()
            for cron_def, next_ts in list(schedule.items()):
                if now >= next_ts:
                    logger.info("Firing cron task: %s", cron_def.name)
                    ctx: dict[str, Any] = {
                        "worker_id": self.worker_id,
                        "broker": self.app.broker,
                    }
                    t = asyncio.create_task(self._run_cron(cron_def, ctx))
                    self._tasks.add(t)
                    t.add_done_callback(self._tasks.discard)
                    schedule[cron_def] = cron_def.next_run()
            await asyncio.sleep(1)

    async def _run_cron(self, cron_def: Any, ctx: dict) -> None:
        try:
            await cron_def(ctx)
        except Exception as exc:
            logger.exception("Cron task %s failed: %s", cron_def.name, exc)

    async def _process(self, job: Job) -> None:
        task_def = self.app.get_task(job.task_name)
        if task_def is None:
            logger.error("Unknown task: %s — marking as failed", job.task_name)
            job.status = JobStatus.failed
            job.error = f"Unknown task: {job.task_name}"
            job.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            await self.app.broker.update_job(job)
            return

        job.status = JobStatus.running
        job.started_at = datetime.now(timezone.utc).replace(tzinfo=None)
        job.worker_id = self.worker_id
        await self.app.broker.update_job(job)

        ctx: dict[str, Any] = {
            "worker_id": self.worker_id,
            "job_id": job.id,
            "broker": self.app.broker,
        }

        # Re-check if job was cancelled while waiting in queue
        fresh = await self.app.broker.get_job(job.id)
        if fresh and fresh.status == JobStatus.cancelled:
            logger.info("Job %s was cancelled before execution, skipping", job.id)
            return

        try:
            result = await task_def(ctx, *job.args, **job.kwargs)
            job.status = JobStatus.completed
            job.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            if job.save_result:
                job.result = result
            await self.app.broker.update_job(job)
            logger.info("Job %s (%s) completed", job.id, job.task_name)

        except Exception as exc:
            logger.exception("Job %s (%s) failed: %s", job.id, job.task_name, exc)
            if job.retries < job.max_retries:
                job.retries += 1
                job.status = JobStatus.retrying
                job.error = str(exc)
                await self.app.broker.update_job(job)
                if job.retry_delay > 0:
                    await asyncio.sleep(job.retry_delay)
                await self.app.broker.enqueue(job)
                logger.info(
                    "Job %s re-enqueued (attempt %d/%d)",
                    job.id,
                    job.retries,
                    job.max_retries,
                )
            else:
                job.status = JobStatus.failed
                job.error = str(exc)
                job.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
                await self.app.broker.update_job(job)
