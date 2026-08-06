# Redis Backend

`RedisBroker` is the default backend, backed by `redis-py` with asyncio support.

## Setup

```bash
pip install aioq  # redis is included
```

```python
from aioq.backends import RedisBroker

broker = RedisBroker(url="redis://localhost:6379")
```

### Constructor options

| Parameter | Default | Description |
|---|---|---|
| `url` | `"redis://localhost:6379"` | Redis connection URL |

Connection URLs support all standard formats:

```python
RedisBroker(url="redis://localhost:6379")
RedisBroker(url="redis://:password@localhost:6379/0")
RedisBroker(url="rediss://localhost:6380")  # TLS
RedisBroker(url="redis://localhost:6379/2")  # database 2
```

## Key schema

All keys are prefixed with `aioq:`:

| Key | Type | Description |
|---|---|---|
| `aioq:queue:{queue}:p{0\|5\|10}:pending` | List | Runnable job IDs per priority tier (LPUSH / BRPOP) |
| `aioq:queue:{queue}:p{0\|5\|10}:deferred` | Sorted Set | Deferred job IDs per tier (score = `run_at`) |
| `aioq:job:{id}` | String | JSON-serialised job data |
| `aioq:jobs:all` | Set | All known job IDs |
| `aioq:jobs:status:{status}` | Set | Job IDs by status |
| `aioq:jobs:queue:{queue}` | Set | Job IDs by queue |
| `aioq:jobs:expiring` | Sorted Set | Jobs whose saved result is due to expire |
| `aioq:job:{id}:dependents` | Set | IDs of jobs waiting on this job to complete |
| `aioq:workers` | Hash | Worker info keyed by worker ID |
| `aioq:cron:lock:{key}` | String | One claimed cron occurrence (`SET NX EX`) |

## Priority queues

Each queue uses three separate lists for priority tiers: `p10` (high), `p5`
(medium), `p0` (default). `RedisBroker` builds the `BRPOP` key list
priority-major — every queue's `p10` list, then every queue's `p5`, then every
`p0` — so an urgent job wins even when it sits in a queue named later in
`--queue`.

Priorities are snapped to the nearest tier: anything `<= 0` is `p0`, `1–5` is
`p5`, above `5` is `p10`.

## Deferred jobs

A deferred job goes into the sorted set for **its own priority tier**, scored by
`run_at`. On every `dequeue()` call the worker promotes due jobs with a Lua
script, moving each one to the pending list of the same tier, so a deferred
high-priority job stays high-priority when it becomes runnable.

The script is atomic and chunks its `ZREM`/`LPUSH` calls, so promoting a large
batch cannot overflow Lua's stack.

Promotion is lazy: a deferred job becomes runnable the next time some worker
calls `dequeue()` on that queue.

## Result expiry

When a job with `save_result=True` completes, its record is given a TTL of
`result_ttl` seconds and recorded in `aioq:jobs:expiring`. Once the record is
gone, the next `list_jobs()`, `queue_stats()` or `purge()` call reaps its index
entries, so expired jobs do not linger in the counts. Set `result_ttl=0` to keep
records indefinitely.

Jobs that do not save a result are never expired automatically — remove them with
`broker.purge()` or `aioq purge`.

## Worker TTL

Workers are registered in a Redis Hash and considered dead if their last
heartbeat is older than 30 seconds; workers heartbeat every 10 seconds by
default. Entries that have been silent for much longer (5 minutes) are dropped
from the hash entirely, so a crashed worker does not sit in the dashboard
forever.

## Cron locks

Each cron occurrence is claimed with `SET aioq:cron:lock:{name}@{timestamp} NX
EX 60`. Exactly one worker gets the key, so an occurrence runs once across the
fleet. The keys expire on their own.

## Persistence and eviction

By default Redis is an in-memory store. Configure `appendonly yes` in `redis.conf` for durability, or use a managed Redis service (ElastiCache, Redis Cloud, Upstash) with persistence enabled.

!!! warning
    If Redis is restarted without persistence enabled, all pending and completed jobs will be lost.

## Scaling

Multiple workers against the same Redis instance are safe — `BRPOP` is atomic and each job ID is only delivered to one worker. For high throughput, consider:

- Redis Cluster or a managed sharded instance
- Separate Redis instances per queue
- Increasing worker concurrency before adding more worker processes
