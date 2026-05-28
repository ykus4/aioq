from __future__ import annotations

import json
import time
from datetime import UTC, datetime

import redis.asyncio as aioredis

from ..models import Job, JobStatus
from .base import BaseBroker

# Redis key schema:
#   aioq:queue:{queue}:p{0|5|10}:pending  - LIST  (LPUSH / BRPOP), priority tiers
#   aioq:queue:{queue}:deferred            - Sorted Set (deferred job IDs, score=timestamp)
#   aioq:job:{id}                          - String (job data as JSON)
#   aioq:jobs:all                          - SET   (all job ids)
#   aioq:jobs:status:{status}              - SET   (job ids by status)
#   aioq:jobs:queue:{queue}                - SET   (job ids by queue)
#   aioq:job:{id}:dependents               - SET   (job ids waiting on this job)
#   aioq:workers                           - HASH  (worker_id -> JSON info)

_PREFIX = "aioq"

# Supported priority tiers (highest first for dequeue ordering)
_PRIORITY_TIERS = [10, 5, 0]


def _clamp_priority(priority: int) -> int:
    """Snap a priority value to the nearest supported tier (0, 5, 10)."""
    if priority <= 0:
        return 0
    if priority <= 5:
        return 5
    return 10


# Lua script: atomically promote due deferred jobs to the pending list.
# KEYS[1] = deferred sorted set, KEYS[2] = pending list
# ARGV[1] = current unix timestamp (float as string)
_PROMOTE_DEFERRED_LUA = """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if #ids == 0 then return 0 end
redis.call('ZREM', KEYS[1], unpack(ids))
for _, id in ipairs(ids) do
    redis.call('LPUSH', KEYS[2], id)
end
return #ids
"""


def _queue_key(queue: str, priority: int = 0) -> str:
    p = _clamp_priority(priority)
    return f"{_PREFIX}:queue:{queue}:p{p}:pending"


def _job_key(job_id: str) -> str:
    return f"{_PREFIX}:job:{job_id}"


def _status_set(status: JobStatus) -> str:
    return f"{_PREFIX}:jobs:status:{status.value}"


def _queue_set(queue: str) -> str:
    return f"{_PREFIX}:jobs:queue:{queue}"


def _dependents_key(job_id: str) -> str:
    return f"{_PREFIX}:job:{job_id}:dependents"


_WORKERS_KEY = f"{_PREFIX}:workers"
_WORKER_TTL = 30  # seconds; heartbeat must be sent more frequently


class RedisBroker(BaseBroker):
    def __init__(self, url: str = "redis://localhost:6379"):
        self.url = url
        self._redis: aioredis.Redis | None = None

    @property
    def redis(self) -> aioredis.Redis:
        if self._redis is None:
            raise RuntimeError("Broker not connected. Call connect() first.")
        return self._redis

    async def connect(self) -> None:
        self._redis = aioredis.from_url(self.url, decode_responses=True)

    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.aclose()  # type: ignore[attr-defined]
            self._redis = None

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    async def enqueue(self, job: Job) -> None:
        # Check dependencies: if any dep is not yet completed, store as waiting
        if job.depends_on:
            all_completed = True
            for dep_id in job.depends_on:
                dep = await self.get_job(dep_id)
                if dep is None or dep.status != JobStatus.completed:
                    all_completed = False
                    break
            if not all_completed:
                job.status = JobStatus.waiting

        pipe = self.redis.pipeline()
        data = json.dumps(job.model_dump_json_safe())
        pipe.set(_job_key(job.id), data)
        pipe.sadd(f"{_PREFIX}:jobs:all", job.id)
        pipe.sadd(_status_set(job.status), job.id)
        pipe.sadd(_queue_set(job.queue), job.id)

        # Register this job as a dependent of each dep it's waiting on
        for dep_id in job.depends_on:
            pipe.sadd(_dependents_key(dep_id), job.id)

        if job.status != JobStatus.waiting:
            if job.run_at and job.run_at > datetime.now(UTC):
                score = job.run_at.timestamp()
                pipe.zadd(f"{_PREFIX}:queue:{job.queue}:deferred", {job.id: score})
            else:
                pipe.lpush(_queue_key(job.queue, job.priority), job.id)

        await pipe.execute()

    async def _promote_deferred(self, queue: str, now: float) -> None:
        """Move due deferred jobs into the pending list using a Lua script for atomicity."""
        deferred_key = f"{_PREFIX}:queue:{queue}:deferred"
        pending_key = _queue_key(queue, 0)
        await self.redis.eval(  # type: ignore[attr-defined]
            _PROMOTE_DEFERRED_LUA,
            2,
            deferred_key,
            pending_key,
            now,
        )

    async def dequeue(self, queues: list[str], timeout: float = 2.0) -> Job | None:
        # Promote any deferred jobs whose run_at has passed (atomic via Lua)
        now = time.time()
        for queue in queues:
            await self._promote_deferred(queue, now)

        # Build keys in priority order: p10, p5, p0 for each queue so that
        # BRPOP naturally picks the highest-priority non-empty list first.
        priority_keys = []
        for q in queues:
            for p in _PRIORITY_TIERS:
                priority_keys.append(_queue_key(q, p))
        result = await self.redis.brpop(priority_keys, timeout=timeout)
        if result is None:
            return None

        _, job_id = result
        raw = await self.redis.get(_job_key(job_id))
        if raw is None:
            return None

        return self._deserialize(raw)

    async def ack(self, job: Job) -> None:
        await self.update_job(job)

    async def nack(self, job: Job, requeue: bool = False) -> None:
        if requeue:
            await self.redis.lpush(_queue_key(job.queue, job.priority), job.id)
        await self.update_job(job)

    async def update_job(self, job: Job) -> None:
        old_raw = await self.redis.get(_job_key(job.id))
        pipe = self.redis.pipeline()

        if old_raw:
            old = self._deserialize(old_raw)
            if old.status != job.status:
                pipe.srem(_status_set(old.status), job.id)
                pipe.sadd(_status_set(job.status), job.id)
            # Track queue membership change (e.g. when job moves to DLQ)
            if old.queue != job.queue:
                pipe.srem(_queue_set(old.queue), job.id)
                pipe.sadd(_queue_set(job.queue), job.id)

        data = json.dumps(job.model_dump_json_safe())
        pipe.set(_job_key(job.id), data)
        await pipe.execute()

        # When a job completes, check if any dependent jobs are now ready
        if job.status == JobStatus.completed:
            dep_ids = await self.redis.smembers(_dependents_key(job.id))
            for dep_id in dep_ids:
                await self._check_and_enqueue_if_ready(dep_id)

    async def _check_and_enqueue_if_ready(self, job_id: str) -> None:
        """Promote a waiting job to pending if all its dependencies are completed."""
        raw = await self.redis.get(_job_key(job_id))
        if raw is None:
            return
        job = self._deserialize(raw)
        if job.status != JobStatus.waiting:
            return

        for dep_id in job.depends_on:
            dep = await self.get_job(dep_id)
            if dep is None or dep.status != JobStatus.completed:
                return  # still waiting

        # All deps completed — promote to pending
        pipe = self.redis.pipeline()
        pipe.srem(_status_set(JobStatus.waiting), job_id)
        pipe.sadd(_status_set(JobStatus.pending), job_id)
        job.status = JobStatus.pending
        data = json.dumps(job.model_dump_json_safe())
        pipe.set(_job_key(job_id), data)
        pipe.lpush(_queue_key(job.queue, job.priority), job_id)
        await pipe.execute()

    async def retry_job(self, job_id: str) -> bool:
        raw = await self.redis.get(_job_key(job_id))
        if not raw:
            return False
        job = self._deserialize(raw)
        if job.status not in (JobStatus.failed, JobStatus.cancelled):
            return False
        job.status = JobStatus.pending
        job.retries = 0
        job.error = None
        job.result = None
        job.started_at = None
        job.completed_at = None
        job.worker_id = None
        await self.update_job(job)
        await self.redis.lpush(_queue_key(job.queue, job.priority), job.id)
        return True

    async def cancel_job(self, job_id: str) -> bool:
        raw = await self.redis.get(_job_key(job_id))
        if not raw:
            return False
        job = self._deserialize(raw)
        if job.status not in (JobStatus.pending, JobStatus.retrying, JobStatus.waiting):
            return False
        job.status = JobStatus.cancelled
        await self.update_job(job)
        return True

    async def get_job(self, job_id: str) -> Job | None:
        raw = await self.redis.get(_job_key(job_id))
        if raw is None:
            return None
        return self._deserialize(raw)

    async def list_jobs(
        self,
        queue: str | None = None,
        status: JobStatus | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        if queue and status:
            ids = await self.redis.sinter(_queue_set(queue), _status_set(status))
        elif queue:
            ids = await self.redis.smembers(_queue_set(queue))
        elif status:
            ids = await self.redis.smembers(_status_set(status))
        else:
            ids = await self.redis.smembers(f"{_PREFIX}:jobs:all")

        job_ids: list[str] = [str(i) for i in ids]
        job_ids = job_ids[offset : offset + limit]

        if not job_ids:
            return []

        # Fetch all job data in a single pipeline round-trip
        pipe = self.redis.pipeline(transaction=False)
        for job_id in job_ids:
            pipe.get(_job_key(job_id))
        raws = await pipe.execute()

        jobs = [self._deserialize(raw) for raw in raws if raw]
        jobs.sort(key=lambda j: j.enqueued_at, reverse=True)
        return jobs

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    async def queue_stats(self) -> dict[str, dict[str, int]]:
        queue_keys = await self.redis.keys(f"{_PREFIX}:jobs:queue:*")
        if not queue_keys:
            return {}

        queues = [key.split(":")[-1] for key in queue_keys]
        statuses = list(JobStatus)

        pipe = self.redis.pipeline(transaction=False)
        for queue in queues:
            for status in statuses:
                pipe.sintercard(2, [_queue_set(queue), _status_set(status)])
        results = await pipe.execute()

        stats: dict[str, dict[str, int]] = {}
        idx = 0
        for queue in queues:
            stats[queue] = {}
            for status in statuses:
                count = results[idx]
                if count > 0:
                    stats[queue][status.value] = count
                idx += 1

        return stats

    # ------------------------------------------------------------------
    # Workers
    # ------------------------------------------------------------------

    async def register_worker(self, worker_id: str, queues: list[str]) -> None:
        info = {
            "worker_id": worker_id,
            "queues": queues,
            "registered_at": datetime.now(UTC).isoformat(),
            "last_heartbeat": datetime.now(UTC).isoformat(),
        }
        await self.redis.hset(_WORKERS_KEY, worker_id, json.dumps(info))
        await self.redis.expire(_WORKERS_KEY, _WORKER_TTL * 10)

    async def heartbeat_worker(self, worker_id: str) -> None:
        raw = await self.redis.hget(_WORKERS_KEY, worker_id)
        if raw:
            info = json.loads(raw)
            info["last_heartbeat"] = datetime.now(UTC).isoformat()
            await self.redis.hset(_WORKERS_KEY, worker_id, json.dumps(info))

    async def deregister_worker(self, worker_id: str) -> None:
        await self.redis.hdel(_WORKERS_KEY, worker_id)

    async def list_workers(self) -> list[dict]:
        raw_map = await self.redis.hgetall(_WORKERS_KEY)
        workers = []
        cutoff = time.time() - _WORKER_TTL
        for _worker_id, raw in raw_map.items():
            info = json.loads(raw)
            hb = datetime.fromisoformat(info["last_heartbeat"]).timestamp()
            info["alive"] = hb > cutoff
            workers.append(info)
        return workers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _deserialize(raw: str) -> Job:
        data = json.loads(raw)
        for k in ("enqueued_at", "started_at", "completed_at", "run_at"):
            if data.get(k):
                data[k] = datetime.fromisoformat(data[k])
        return Job(**data)
