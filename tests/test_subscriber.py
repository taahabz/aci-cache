"""Tests for the Pub/Sub Subscriber."""

import json
from unittest.mock import MagicMock, patch

import pytest

from aci_cache.subscriber import Subscriber
from aci_cache.config import CacheConfig


class TestSubscriberInit:
    """Subscriber construction and lifecycle."""

    def test_start_creates_thread(self):
        sub = Subscriber(
            redis_client=MagicMock(),
            pubsub_client=MagicMock(),
            config=CacheConfig(),
            on_invalidate=MagicMock(),
            on_strategy_update=MagicMock(),
            instance_id="test",
        )
        sub.start()
        assert sub._thread is not None
        assert sub._thread.is_alive()
        sub.stop()

    def test_double_start_is_idempotent(self):
        sub = Subscriber(
            redis_client=MagicMock(),
            pubsub_client=MagicMock(),
            config=CacheConfig(),
            on_invalidate=MagicMock(),
            on_strategy_update=MagicMock(),
            instance_id="test",
        )
        sub.start()
        t1 = sub._thread
        sub.start()
        assert sub._thread is t1
        sub.stop()


class TestMessageHandling:
    """Test the internal message handler methods directly."""

    @pytest.fixture
    def mock_redis(self):
        return MagicMock()

    @pytest.fixture
    def on_invalidate(self):
        return MagicMock()

    @pytest.fixture
    def on_strategy_update(self):
        return MagicMock()

    @pytest.fixture
    def subscriber(self, mock_redis, on_invalidate, on_strategy_update):
        return Subscriber(
            redis_client=mock_redis,
            pubsub_client=MagicMock(),
            config=CacheConfig(),
            on_invalidate=on_invalidate,
            on_strategy_update=on_strategy_update,
            instance_id="my_instance",
        )

    def test_handle_invalidation(self, subscriber, mock_redis, on_invalidate):
        payload = {
            "action": "invalidate",
            "keys": ["user:1", "user:2"],
            "source": "other_instance",
        }
        subscriber._handle_invalidation(payload)

        # Should delete keys from Redis
        mock_redis.delete.assert_called_once_with("user:1", "user:2")
        # Should call the callback
        on_invalidate.assert_called_once_with(["user:1", "user:2"])

    def test_handle_invalidation_empty_keys(self, subscriber, mock_redis, on_invalidate):
        subscriber._handle_invalidation({"keys": []})
        mock_redis.delete.assert_not_called()
        on_invalidate.assert_not_called()

    def test_handle_strategy_update(self, subscriber, on_strategy_update):
        payload = {
            "action": "strategy_update",
            "strategy": "eager",
            "write_rate": 65.4,
            "source": "controller_instance",
        }
        subscriber._handle_strategy_update(payload)
        on_strategy_update.assert_called_once_with("eager", 65.4)

    def test_handle_strategy_update_no_write_rate(self, subscriber, on_strategy_update):
        payload = {"strategy": "ttl"}
        subscriber._handle_strategy_update(payload)
        on_strategy_update.assert_called_once_with("ttl", None)

    def test_handle_strategy_update_empty_strategy_ignored(self, subscriber, on_strategy_update):
        payload = {"strategy": ""}
        subscriber._handle_strategy_update(payload)
        on_strategy_update.assert_not_called()
