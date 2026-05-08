"""Tests for the AdaptiveCache public API."""

import time
from unittest.mock import MagicMock, patch

import pytest
import fakeredis

from aci_cache import AdaptiveCache, CacheConfig, CacheStats


@pytest.fixture
def cache():
    """Create an AdaptiveCache with fakeredis (controller disabled to avoid
    background thread interference in unit tests)."""
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server, decode_responses=True)
    pubsub = fakeredis.FakeRedis(server=server, decode_responses=True)
    c = AdaptiveCache(
        redis_client=client,
        pubsub_client=pubsub,
        is_controller=False,
    )
    yield c
    c.stop()


@pytest.fixture
def cache_with_controller():
    """Create an AdaptiveCache with controller enabled."""
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server, decode_responses=True)
    pubsub = fakeredis.FakeRedis(server=server, decode_responses=True)
    c = AdaptiveCache(
        redis_client=client,
        pubsub_client=pubsub,
        is_controller=True,
        controller_interval=0.1,
        sliding_window=1.0,
    )
    yield c
    c.stop()


class TestCoreAPI:
    """Basic get/set/delete/flush operations."""

    def test_set_and_get(self, cache):
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_get_nonexistent_returns_none(self, cache):
        assert cache.get("missing") is None

    def test_delete_removes_key(self, cache):
        cache.set("key1", "value1")
        cache.delete("key1")
        assert cache.get("key1") is None

    def test_flush_removes_all_keys(self, cache):
        cache.set("a", "1")
        cache.set("b", "2")
        cache.flush()
        assert cache.get("a") is None
        assert cache.get("b") is None

    def test_set_with_custom_ttl(self, cache):
        cache.set("key", "val", ttl=1)
        assert cache.get("key") == "val"
        # fakeredis respects TTL, but we can't easily time.sleep in unit tests
        # Just verify it doesn't raise


class TestObservability:
    """strategy, stats, on_switch properties."""

    def test_initial_strategy_is_ttl(self, cache):
        assert cache.strategy == "ttl"

    def test_stats_returns_cache_stats(self, cache):
        stats = cache.stats
        assert isinstance(stats, CacheStats)
        assert stats.total_reads == 0
        assert stats.total_writes == 0

    def test_stats_track_reads(self, cache):
        cache.get("missing")
        stats = cache.stats
        assert stats.total_reads == 1
        assert stats.cache_misses == 1
        assert stats.cache_hits == 0

    def test_stats_track_writes(self, cache):
        cache.set("k", "v")
        stats = cache.stats
        assert stats.total_writes == 1

    def test_stats_track_hits(self, cache):
        cache.set("k", "v")
        cache.get("k")
        stats = cache.stats
        assert stats.cache_hits == 1

    def test_on_switch_callback(self, cache):
        received = []
        cache.on_switch(lambda f, t: received.append((f, t)))
        cache._apply_switch("ttl", "eager")
        assert received == [("ttl", "eager")]

    def test_instance_id_is_string(self, cache):
        assert isinstance(cache.instance_id, str)
        assert len(cache.instance_id) == 12


class TestStrategySwitching:
    """Internal _apply_switch method."""

    def test_switch_to_eager(self, cache):
        cache._apply_switch("ttl", "eager")
        assert cache.strategy == "eager"

    def test_switch_to_batched(self, cache):
        cache._apply_switch("ttl", "batched")
        assert cache.strategy == "batched"

    def test_switch_back_to_ttl(self, cache):
        cache._apply_switch("ttl", "eager")
        cache._apply_switch("eager", "ttl")
        assert cache.strategy == "ttl"

    def test_switch_records_in_stats(self, cache):
        cache._apply_switch("ttl", "eager")
        stats = cache.stats
        assert len(stats.strategy_switches) == 1
        assert stats.strategy_switches[0][1] == "ttl"
        assert stats.strategy_switches[0][2] == "eager"

    def test_same_strategy_is_noop(self, cache):
        cache._apply_switch("ttl", "ttl")
        assert len(cache.stats.strategy_switches) == 0

    def test_invalid_strategy_ignored(self, cache):
        cache._apply_switch("ttl", "invalid_strategy")
        assert cache.strategy == "ttl"


class TestContextManager:
    """Context manager support."""

    def test_context_manager(self):
        server = fakeredis.FakeServer()
        client = fakeredis.FakeRedis(server=server, decode_responses=True)
        pubsub = fakeredis.FakeRedis(server=server, decode_responses=True)
        with AdaptiveCache(client, pubsub, is_controller=False) as cache:
            cache.set("k", "v")
            assert cache.get("k") == "v"
        # After exiting, the cache should be stopped (no assertion needed,
        # just verify no exception)


class TestNamespacing:
    """Keys should be namespaced when namespace is configured."""

    def test_namespaced_set_and_get(self):
        server = fakeredis.FakeServer()
        client = fakeredis.FakeRedis(server=server, decode_responses=True)
        pubsub = fakeredis.FakeRedis(server=server, decode_responses=True)
        cache = AdaptiveCache(
            client, pubsub, namespace="myapp", is_controller=False
        )
        cache.set("user:1", "alice")
        # Should be stored under the namespaced key
        raw = client.get("aci:myapp:user:1")
        assert raw == "alice"
        # get() should also use the namespace
        assert cache.get("user:1") == "alice"
        cache.stop()
