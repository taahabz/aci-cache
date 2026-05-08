"""Tests for AsyncAdaptiveCache."""

import asyncio

import pytest
import fakeredis.aioredis

from aci_cache import AsyncAdaptiveCache, CacheStats


@pytest.fixture
async def cache():
    """Create an AsyncAdaptiveCache with fakeredis (controller disabled)."""
    server = fakeredis.FakeServer()
    client = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    pubsub = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    c = AsyncAdaptiveCache(
        redis_client=client,
        pubsub_client=pubsub,
        is_controller=False,
    )
    yield c
    await c.stop()


@pytest.mark.asyncio
class TestAsyncCoreAPI:
    """Basic get/set/delete operations."""

    async def test_set_and_get(self, cache):
        await cache.set("key1", "value1")
        assert await cache.get("key1") == "value1"

    async def test_get_nonexistent_returns_none(self, cache):
        assert await cache.get("missing") is None

    async def test_delete_removes_key(self, cache):
        await cache.set("key1", "value1")
        await cache.delete("key1")
        assert await cache.get("key1") is None

    async def test_flush_removes_all(self, cache):
        await cache.set("a", "1")
        await cache.set("b", "2")
        await cache.flush()
        assert await cache.get("a") is None
        assert await cache.get("b") is None


@pytest.mark.asyncio
class TestAsyncObservability:
    """Strategy, stats properties."""

    async def test_initial_strategy_is_ttl(self, cache):
        assert cache.strategy == "ttl"

    async def test_stats_track_reads(self, cache):
        await cache.get("missing")
        stats = cache.stats
        assert isinstance(stats, CacheStats)
        assert stats.total_reads == 1
        assert stats.cache_misses == 1

    async def test_stats_track_writes(self, cache):
        await cache.set("k", "v")
        stats = cache.stats
        assert stats.total_writes == 1

    async def test_stats_track_hits(self, cache):
        await cache.set("k", "v")
        await cache.get("k")
        assert cache.stats.cache_hits == 1

    async def test_instance_id(self, cache):
        assert isinstance(cache.instance_id, str)
        assert len(cache.instance_id) == 12

    async def test_on_switch_callback(self, cache):
        received = []
        cache.on_switch(lambda f, t: received.append((f, t)))
        cache._apply_switch("ttl", "eager")
        assert received == [("ttl", "eager")]


@pytest.mark.asyncio
class TestAsyncContextManager:
    """Async context manager support."""

    async def test_async_with(self):
        server = fakeredis.FakeServer()
        client = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
        pubsub = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
        async with AsyncAdaptiveCache(client, pubsub, is_controller=False) as cache:
            await cache.set("k", "v")
            assert await cache.get("k") == "v"


@pytest.mark.asyncio
class TestAsyncNamespacing:
    """Namespace support."""

    async def test_namespaced_set_and_get(self):
        server = fakeredis.FakeServer()
        client = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
        pubsub = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
        cache = AsyncAdaptiveCache(
            client, pubsub, namespace="myapp", is_controller=False
        )
        await cache.set("user:1", "alice")
        raw = await client.get("aci:myapp:user:1")
        assert raw == "alice"
        assert await cache.get("user:1") == "alice"
        await cache.stop()
