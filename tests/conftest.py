"""Shared fixtures.

``fakeredis[lua]`` is a dev dependency, so the Lua scripts the Redis broker
relies on (deferred-job promotion) run for real here rather than being stubbed
out.
"""

from __future__ import annotations

import fakeredis.aioredis
import pytest
import redis.asyncio as aioredis

from aioq import Aioq
from aioq.backends.memory import MemoryBroker
from aioq.backends.redis import RedisBroker


@pytest.fixture
async def broker(monkeypatch):
    """A connected RedisBroker backed by fakeredis."""
    fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(aioredis, "from_url", lambda *a, **kw: fake)

    b = RedisBroker()
    await b.connect()
    yield b
    await b.disconnect()


@pytest.fixture
async def memory_broker():
    """A connected MemoryBroker."""
    async with MemoryBroker() as b:
        yield b


@pytest.fixture
def memory_app(memory_broker):
    """An Aioq app wired to a MemoryBroker."""
    return Aioq(broker=memory_broker)
