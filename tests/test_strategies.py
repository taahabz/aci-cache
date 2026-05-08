"""Tests for the three invalidation strategies."""

import json
from unittest.mock import MagicMock, patch

import pytest

from aci_cache.strategies.ttl import TTLStrategy
from aci_cache.strategies.eager import EagerStrategy
from aci_cache.strategies.batched import BatchedStrategy


# =========================================================================
# TTL Strategy
# =========================================================================


class TestTTLStrategy:
    """TTL strategy: no-op on write, no pub/sub."""

    def test_name(self):
        s = TTLStrategy()
        assert s.name == "ttl"

    def test_on_write_is_noop(self):
        s = TTLStrategy()
        # Should not raise or do anything
        s.on_write("user:123")

    def test_on_activate_does_not_raise(self):
        s = TTLStrategy()
        s.on_activate()

    def test_on_deactivate_does_not_raise(self):
        s = TTLStrategy()
        s.on_deactivate()


# =========================================================================
# Eager Strategy
# =========================================================================


class TestEagerStrategy:
    """Eager strategy: publishes invalidation on every write."""

    @pytest.fixture
    def mock_redis(self):
        return MagicMock()

    @pytest.fixture
    def strategy(self, mock_redis):
        return EagerStrategy(
            pubsub_client=mock_redis,
            channel="aci:invalidation",
            instance_id="test_inst",
        )

    def test_name(self, strategy):
        assert strategy.name == "eager"

    def test_on_write_publishes(self, strategy, mock_redis):
        strategy.on_write("user:123")
        mock_redis.publish.assert_called_once()

        # Verify channel
        call_args = mock_redis.publish.call_args
        assert call_args[0][0] == "aci:invalidation"

        # Verify payload
        payload = json.loads(call_args[0][1])
        assert payload["action"] == "invalidate"
        assert payload["keys"] == ["user:123"]
        assert payload["strategy"] == "eager"
        assert payload["source"] == "test_inst"
        assert "timestamp" in payload

    def test_on_write_handles_connection_error(self, mock_redis):
        import redis

        mock_redis.publish.side_effect = redis.ConnectionError("gone")
        strategy = EagerStrategy(
            pubsub_client=mock_redis,
            channel="aci:invalidation",
            instance_id="test",
        )
        # Should not raise
        strategy.on_write("key")


# =========================================================================
# Batched Strategy
# =========================================================================


class TestBatchedStrategy:
    """Batched strategy: buffers keys, flushes periodically."""

    @pytest.fixture
    def mock_redis(self):
        return MagicMock()

    @pytest.fixture
    def strategy(self, mock_redis):
        return BatchedStrategy(
            pubsub_client=mock_redis,
            channel="aci:invalidation",
            instance_id="test_inst",
        )

    def test_name(self, strategy):
        assert strategy.name == "batched"

    def test_on_write_buffers_key(self, strategy, mock_redis):
        strategy.on_write("key1")
        strategy.on_write("key2")
        # Should NOT publish yet
        mock_redis.publish.assert_not_called()

    def test_flush_publishes_buffered_keys(self, strategy, mock_redis):
        strategy.on_write("key1")
        strategy.on_write("key2")
        strategy.flush()

        mock_redis.publish.assert_called_once()
        payload = json.loads(mock_redis.publish.call_args[0][1])
        assert set(payload["keys"]) == {"key1", "key2"}
        assert payload["strategy"] == "batched"

    def test_flush_deduplicates(self, strategy, mock_redis):
        strategy.on_write("key1")
        strategy.on_write("key1")
        strategy.on_write("key1")
        strategy.flush()

        payload = json.loads(mock_redis.publish.call_args[0][1])
        assert payload["keys"] == ["key1"]

    def test_flush_empty_buffer_is_noop(self, strategy, mock_redis):
        strategy.flush()
        mock_redis.publish.assert_not_called()

    def test_flush_clears_buffer(self, strategy, mock_redis):
        strategy.on_write("key1")
        strategy.flush()
        mock_redis.publish.reset_mock()

        # Second flush should be a no-op
        strategy.flush()
        mock_redis.publish.assert_not_called()

    def test_on_deactivate_flushes_buffer(self, strategy, mock_redis):
        strategy.on_write("leftover_key")
        strategy.on_deactivate()

        mock_redis.publish.assert_called_once()
        payload = json.loads(mock_redis.publish.call_args[0][1])
        assert payload["keys"] == ["leftover_key"]

    def test_flush_handles_connection_error(self, mock_redis):
        import redis as redis_lib

        mock_redis.publish.side_effect = redis_lib.ConnectionError("gone")
        strategy = BatchedStrategy(
            pubsub_client=mock_redis,
            channel="aci:invalidation",
            instance_id="test",
        )
        strategy.on_write("key")
        # Should not raise
        strategy.flush()
