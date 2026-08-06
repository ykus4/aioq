from __future__ import annotations

import json
import logging
import time
from collections.abc import Collection
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as aioredis

from ..constants import CRON_LOCK_TTL, DEQUEUE_TIMEOUT, WORKER_TTL
from ..models import CANCELLABLE_STATUSES, RETRIABLE_STATUSES, Job, JobStatus
from .base import BaseBroker

logger = logging.getLogger("aioq.broker.redis")

# Redis key schema:
#   aioq:queue:{queue}:p{0|5|10}:pending   - LIST      (LPUSH / BRPOP), priority tiers
#   aioq:queue:{queue}:p{0|5|10}:deferred  - ZSET      (job ids, score=run_at timestamp)
#   aioq:job:{id}                          - STRING    (job data as JSON)
#   aioq:jobs:all                          - SET       (all job ids)
#   aioq:jobs:status:{status}              - SET       (job ids by status)
#   aioq:jobs:queue:{queue}                - SET       (job ids by queue)
#   aioq:jobs:expiring                     - ZSET      ("{id}|{queue}", score=result expiry)
#   aioq:job:{id}:dependents               - SET       (job ids waiting on this job)
#   aioq:workers                           - HASH      (worker_id -> JSON info)
#   aioq:cron:lock:{key}                   - STRING    (SET NX EX, one per occurrence)

_PREFIX = "aioq"

# Supported priority tiers (highest first for dequeue ordering).
_PRIORITY_TIERS = (10, 5, 0)

_ALL_JOBS_KEY = f"{_PREFIX}:jobs:all"
_EXPIRING_KEY = f"{_PREFIX}:jobs:expiring"
_WORKERS_KEY = f"{_PREFIX}:workers"

# Workers that stopped heartbeating this long ago are dropped from the hash
# entirely, so a crashed worker does not linger in the dashboard forever.
_WORKER_REAP_AFTER = WORKER_TTL * 10

# Atomically promote due deferred jobs to their pending list.
# KEYS come in (deferred, pending) pairs; ARGV[1] = current unix timestamp.
# ZREM/LPUSH are chunked because `unpack` on a large table overflows Lua's stack.
_PROMOTE_DEFERRED_LUA = """
local moved = 0
local chunk = 500
for i = 1, #KEYS, 2 do
    local ids = redis.call('ZRANGEBYSCORE', KEYS[i], '-inf', ARGV[1])
    local n = #ids
    if n > 0 then
        for j = 1, n, chunk do
            local last = math.min(j + chunk - 1, n)
            redis.call('ZREM', KEYS[i], unpack(ids, j, last))
            redis.call('LPUSH', KEYS[i + 1], unpack(ids, j, last))
        end
        moved = moved + n
    end
end
return moved
"""


def _clamp_priority(priority: int) -> int:
    """Snap a priority value to the nearest supported tier (0, 5, 10)."""
    if priority <= 0:
        return 0
    if priority <= 5:
        return 5
    return 10


def _pending_key(queue: str, priority: int = 0) -> str:
    return f"{_PREFIX}:queue:{queue}:p{_clamp_priority(priority)}:pending"


def _deferred_key(queue: str, priority: int = 0) -> str:
    return f"{_PREFIX}:queue:{queue}:p{_clamp_priority(priority)}:deferred"


def _job_key(job_id: str) -> str:
    return f"{_PREFIX}:job:{job_id}"


def _status_set(status: JobStatus) -> str:
    return f"{_PREFIX}:jobs:status:{status.value}"


_QUEUE_SET_PREFIX = f"{_PREFIX}:jobs:queue:"


def _queue_set(queue: str) -> str:
    return f"{_QUEUE_SET_PREFIX}{queue}"


def _dependents_key(job_id: str) -> str:
    return f"{_PREFIX}:job:{job_id}:dependents"


def _cron_lock_key(key: str) -> str:
    return f"{_PREFIX}:cron:lock:{key}"


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
            await self._redis.aclose()
            self._redis = None

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    async def enqueue(self, job: Job) -> None:
        # Register the job as a dependent *before* inspecting dependency state.
        # Doing it the other way round loses the wake-up if a dependency
        # completes in between, leaving the job waiting forever.
        if job.depends_on:
            pipe = self.redis.pipeline()
            for dep_id in job.depends_on:
                pipe.sadd(_dependents_key(dep_id), job.id)
            await pipe.execute()

        await self._check_dependencies(job)
        await self._write_job(job, push=True)

        if job.status == JobStatus.waiting:
            # Close the remaining race: a dependency may have completed while
            # we were writing the job, in which case nobody will wake us.
            await self._promote_if_ready(job.id)

    async def _write_job(self, job: Job, *, push: bool) -> None:
        """Persist *job*, fix up its index entries, and optionally queue it.

        Both ``enqueue()`` and ``update_job()`` go through here so that a job
        being re-queued (a retry, a DLQ replay) never ends up listed under two
        statuses at once, which would double-count it in ``queue_stats()``.
        """
        old_raw = await self.redis.get(_job_key(job.id))
        pipe = self.redis.pipeline()

        if old_raw:
            old = _deserialize(old_raw)
            if old.status != job.status:
                pipe.srem(_status_set(old.status), job.id)
            # Track queue membership change (e.g. when a job moves to its DLQ)
            if old.queue != job.queue:
                pipe.srem(_queue_set(old.queue), job.id)

        pipe.set(_job_key(job.id), _serialize(job))
        pipe.sadd(_ALL_JOBS_KEY, job.id)
        pipe.sadd(_status_set(job.status), job.id)
        pipe.sadd(_queue_set(job.queue), job.id)

        if push and job.status != JobStatus.waiting:
            if job.run_at and job.run_at > datetime.now(UTC):
                pipe.zadd(
                    _deferred_key(job.queue, job.priority),
                    {job.id: job.run_at.timestamp()},
                )
            else:
                pipe.lpush(_pending_key(job.queue, job.priority), job.id)

        # Saved results are not kept forever: expire the record and remember to
        # clean its index entries up once it is gone.
        if job.status == JobStatus.completed and job.save_result and job.result_ttl > 0:
            pipe.expire(_job_key(job.id), job.result_ttl)
            pipe.zadd(_EXPIRING_KEY, {f"{job.id}|{job.queue}": time.time() + job.result_ttl})

        await pipe.execute()

    async def _promote_deferred(self, queues: list[str], now: float) -> None:
        """Move every due deferred job into the pending list of its own tier."""
        keys: list[str] = []
        for queue in queues:
            for tier in _PRIORITY_TIERS:
                keys.append(_deferred_key(queue, tier))
                keys.append(_pending_key(queue, tier))
        await self.redis.eval(_PROMOTE_DEFERRED_LUA, len(keys), *keys, now)

    async def dequeue(self, queues: list[str], timeout: float = DEQUEUE_TIMEOUT) -> Job | None:
        await self._promote_deferred(queues, time.time())

        # Priority-major ordering: BRPOP scans keys left to right, so every
        # queue's p10 list must come before any queue's p5 list — otherwise a
        # low-priority job in the first queue would beat an urgent one in the
        # second.
        priority_keys = [_pending_key(q, p) for p in _PRIORITY_TIERS for q in queues]
        result = await self.redis.brpop(priority_keys, timeout=timeout)
        if result is None:
            return None

        job_id = _text(result[1])
        raw = await self.redis.get(_job_key(job_id))
        if raw is None:
            logger.warning("Dequeued job %s but its data is gone; dropping it", job_id)
            await self._forget(job_id)
            return None

        return _deserialize(raw)

    async def nack(self, job: Job, requeue: bool = False) -> None:
        if requeue:
            await self.redis.lpush(_pending_key(job.queue, job.priority), job.id)
        await self.update_job(job)

    async def update_job(self, job: Job) -> None:
        await self._write_job(job, push=False)

        # A completed job may have unblocked jobs waiting on it.
        if job.status == JobStatus.completed:
            for dep_id in await self.redis.smembers(_dependents_key(job.id)):
                await self._promote_if_ready(_text(dep_id))

    async def _promote_if_ready(self, job_id: str) -> None:
        """Promote a waiting job to pending once all its dependencies completed."""
        raw = await self.redis.get(_job_key(job_id))
        if raw is None:
            return
        job = _deserialize(raw)
        if job.status != JobStatus.waiting:
            return
        if not await self._all_deps_completed(job.depends_on):
            return

        job.status = JobStatus.pending
        pipe = self.redis.pipeline()
        pipe.srem(_status_set(JobStatus.waiting), job_id)
        pipe.sadd(_status_set(JobStatus.pending), job_id)
        pipe.set(_job_key(job_id), _serialize(job))
        pipe.lpush(_pending_key(job.queue, job.priority), job_id)
        await pipe.execute()

    async def retry_job(self, job_id: str) -> bool:
        job = await self.get_job(job_id)
        if job is None or job.status not in RETRIABLE_STATUSES:
            return False
        job.reset_for_replay()
        job.status = JobStatus.pending
        job.run_at = None
        await self._write_job(job, push=True)
        return True

    async def cancel_job(self, job_id: str) -> bool:
        job = await self.get_job(job_id)
        if job is None or job.status not in CANCELLABLE_STATUSES:
            return False
        job.status = JobStatus.cancelled
        await self.update_job(job)
        return True

    async def get_job(self, job_id: str) -> Job | None:
        raw = await self.redis.get(_job_key(job_id))
        return _deserialize(raw) if raw is not None else None

    async def list_jobs(
        self,
        queue: str | None = None,
        status: JobStatus | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        await self._reap_expired()

        if queue and status:
            ids = await self.redis.sinter([_queue_set(queue), _status_set(status)])
        elif queue:
            ids = await self.redis.smembers(_queue_set(queue))
        elif status:
            ids = await self.redis.smembers(_status_set(status))
        else:
            ids = await self.redis.smembers(_ALL_JOBS_KEY)

        job_ids = [str(i) for i in ids][offset : offset + limit]
        if not job_ids:
            return []

        jobs = [j for j in await self._get_many(job_ids) if j is not None]
        jobs.sort(key=lambda j: j.enqueued_at, reverse=True)
        return jobs

    async def _get_many(self, job_ids: list[str]) -> list[Job | None]:
        """Fetch several jobs in a single pipeline round-trip."""
        pipe = self.redis.pipeline(transaction=False)
        for job_id in job_ids:
            pipe.get(_job_key(job_id))
        return [_deserialize(raw) if raw else None for raw in await pipe.execute()]

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    async def queue_stats(self) -> dict[str, dict[str, int]]:
        await self._reap_expired()

        queue_keys = await self.redis.keys(f"{_QUEUE_SET_PREFIX}*")
        if not queue_keys:
            return {}

        # Slice by prefix length rather than splitting on ":" — queue names are
        # allowed to contain colons.
        queues = sorted(_text(key)[len(_QUEUE_SET_PREFIX) :] for key in queue_keys)
        statuses = list(JobStatus)

        pipe = self.redis.pipeline(transaction=False)
        for queue in queues:
            for status in statuses:
                pipe.sintercard(2, [_queue_set(queue), _status_set(status)])
        counts = await pipe.execute()

        stats: dict[str, dict[str, int]] = {}
        for i, queue in enumerate(queues):
            per_status = {
                status.value: count
                for status, count in zip(
                    statuses, counts[i * len(statuses) : (i + 1) * len(statuses)], strict=True
                )
                if count > 0
            }
            stats[queue] = per_status
        return stats

    # ------------------------------------------------------------------
    # Workers
    # ------------------------------------------------------------------

    async def register_worker(self, worker_id: str, queues: list[str]) -> None:
        now = datetime.now(UTC).isoformat()
        info = {
            "worker_id": worker_id,
            "queues": queues,
            "registered_at": now,
            "last_heartbeat": now,
        }
        await self.redis.hset(_WORKERS_KEY, worker_id, json.dumps(info))

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
        now = time.time()
        workers: list[dict] = []
        stale: list[str] = []

        for worker_id, raw in raw_map.items():
            info = json.loads(raw)
            hb = datetime.fromisoformat(info["last_heartbeat"]).timestamp()
            if now - hb > _WORKER_REAP_AFTER:
                stale.append(_text(worker_id))
                continue
            info["alive"] = hb > now - WORKER_TTL
            workers.append(info)

        if stale:
            await self.redis.hdel(_WORKERS_KEY, *stale)
        return workers

    # ------------------------------------------------------------------
    # Cron coordination
    # ------------------------------------------------------------------

    async def acquire_cron_lock(self, key: str, ttl: float = CRON_LOCK_TTL) -> bool:
        won = await self.redis.set(_cron_lock_key(key), "1", nx=True, ex=int(ttl))
        return bool(won)

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    async def purge(
        self,
        older_than: float,
        statuses: Collection[JobStatus] | None = None,
    ) -> int:
        await self._reap_expired()
        cutoff = self._purge_cutoff(older_than)
        removed = 0

        for status_value in self._purge_statuses(statuses):
            status = JobStatus(status_value)
            ids = [str(i) for i in await self.redis.smembers(_status_set(status))]
            for job in await self._get_many(ids):
                if job is None:
                    continue
                finished_at = job.completed_at or job.enqueued_at
                if finished_at <= cutoff:
                    await self._forget(job.id, queue=job.queue, status=job.status)
                    removed += 1

        return removed

    async def _forget(
        self,
        job_id: str,
        *,
        queue: str | None = None,
        status: JobStatus | None = None,
    ) -> None:
        """Delete a job and every index entry pointing at it."""
        pipe = self.redis.pipeline()
        pipe.delete(_job_key(job_id))
        pipe.delete(_dependents_key(job_id))
        pipe.srem(_ALL_JOBS_KEY, job_id)
        if queue is not None:
            pipe.srem(_queue_set(queue), job_id)
        if status is not None:
            pipe.srem(_status_set(status), job_id)
        else:
            # Status unknown (the record is already gone) — clear all of them.
            for candidate in JobStatus:
                pipe.srem(_status_set(candidate), job_id)
        await pipe.execute()

    async def _reap_expired(self, now: float | None = None) -> None:
        """Drop index entries for job records whose result TTL has elapsed."""
        due = await self.redis.zrangebyscore(
            _EXPIRING_KEY, "-inf", time.time() if now is None else now
        )
        if not due:
            return
        for member in due:
            job_id, _, queue = str(member).partition("|")
            await self._forget(job_id, queue=queue, status=JobStatus.completed)
        await self.redis.zrem(_EXPIRING_KEY, *due)


# ----------------------------------------------------------------------
# Serialization
# ----------------------------------------------------------------------


def _text(value: Any) -> str:
    """Coerce a Redis reply to ``str``.

    We connect with ``decode_responses=True``, but redis-py types its replies
    as ``bytes | str`` and a caller could hand us a client configured either
    way, so ids and keys are normalised at the boundary.
    """
    return value.decode() if isinstance(value, bytes | bytearray) else str(value)


def _serialize(job: Job) -> str:
    return json.dumps(job.model_dump_json_safe())


def _deserialize(raw: Any) -> Job:
    return Job(**json.loads(raw))
