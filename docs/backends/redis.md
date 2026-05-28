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
| `aioq:queue:{queue}:pending` | List | Active pending job IDs (LPUSH / BRPOP) |
| `aioq:queue:{queue}:deferred` | Sorted Set | Deferred job IDs (score = unix timestamp) |
| `aioq:job:{id}` | String | JSON-serialised job data |
| `aioq:jobs:all` | Set | All known job IDs |
| `aioq:jobs:status:{status}` | Set | Job IDs by status |
| `aioq:jobs:queue:{queue}` | Set | Job IDs by queue |
| `aioq:workers` | Hash | Worker info keyed by worker ID |

## Deferred jobs

Deferred jobs are stored in `aioq:queue:{queue}:deferred` with a Unix timestamp as the sort score. On every `dequeue()` call the worker runs:

```
ZRANGEBYSCORE aioq:queue:{queue}:deferred -inf <now>
```

and moves due jobs to the pending list. This means deferred jobs are promoted lazily — they become runnable the next time a worker calls `dequeue()`.

## Worker TTL

Workers are registered in a Redis Hash with a 30-second TTL check. The dashboard marks a worker as dead if its last heartbeat is older than 30 seconds. Workers send heartbeats every 10 seconds by default.

## Persistence and eviction

By default Redis is an in-memory store. Configure `appendonly yes` in `redis.conf` for durability, or use a managed Redis service (ElastiCache, Redis Cloud, Upstash) with persistence enabled.

!!! warning
    If Redis is restarted without persistence enabled, all pending and completed jobs will be lost.

## Scaling

Multiple workers against the same Redis instance are safe — `BRPOP` is atomic and each job ID is only delivered to one worker. For high throughput, consider:

- Redis Cluster or a managed sharded instance
- Separate Redis instances per queue
- Increasing worker concurrency before adding more worker processes
