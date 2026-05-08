"""Tests for CacheConfig validation and derived helpers."""

import pytest
from aci_cache.config import CacheConfig


class TestDefaults:
    """Default configuration should match CLAUDE.md values."""

    def test_default_thresholds(self):
        cfg = CacheConfig()
        assert cfg.high_threshold == 50.0
        assert cfg.low_threshold == 10.0

    def test_default_ttl(self):
        cfg = CacheConfig()
        assert cfg.default_ttl == 10

    def test_default_intervals(self):
        cfg = CacheConfig()
        assert cfg.batch_interval == 2.5
        assert cfg.controller_interval == 3.0
        assert cfg.sliding_window == 5.0

    def test_default_channels(self):
        cfg = CacheConfig()
        assert cfg.invalidation_channel == "aci:invalidation"
        assert cfg.strategy_channel == "aci:strategy"

    def test_default_namespace_is_none(self):
        cfg = CacheConfig()
        assert cfg.namespace is None

    def test_default_is_controller(self):
        cfg = CacheConfig()
        assert cfg.is_controller is True


class TestCustomOverrides:
    """Custom values are accepted when valid."""

    def test_custom_thresholds(self):
        cfg = CacheConfig(low_threshold=5, high_threshold=100)
        assert cfg.low_threshold == 5
        assert cfg.high_threshold == 100

    def test_custom_ttl(self):
        cfg = CacheConfig(default_ttl=60)
        assert cfg.default_ttl == 60

    def test_is_controller_false(self):
        cfg = CacheConfig(is_controller=False)
        assert cfg.is_controller is False


class TestValidation:
    """Invalid configurations must raise ValueError at construction."""

    def test_low_gte_high_raises(self):
        with pytest.raises(ValueError, match="low_threshold.*high_threshold"):
            CacheConfig(low_threshold=50, high_threshold=50)

    def test_low_gt_high_raises(self):
        with pytest.raises(ValueError, match="low_threshold.*high_threshold"):
            CacheConfig(low_threshold=60, high_threshold=50)

    def test_negative_ttl_raises(self):
        with pytest.raises(ValueError, match="default_ttl"):
            CacheConfig(default_ttl=0)

    def test_negative_batch_interval_raises(self):
        with pytest.raises(ValueError, match="batch_interval"):
            CacheConfig(batch_interval=-1)

    def test_negative_controller_interval_raises(self):
        with pytest.raises(ValueError, match="controller_interval"):
            CacheConfig(controller_interval=0)

    def test_negative_sliding_window_raises(self):
        with pytest.raises(ValueError, match="sliding_window"):
            CacheConfig(sliding_window=0)

    def test_negative_low_threshold_raises(self):
        with pytest.raises(ValueError, match="low_threshold"):
            CacheConfig(low_threshold=-5)


class TestNamespacing:
    """Namespace affects channel names and key prefixing."""

    def test_namespaced_channels(self):
        cfg = CacheConfig(namespace="user-svc")
        assert cfg.invalidation_channel == "aci:user-svc:invalidation"
        assert cfg.strategy_channel == "aci:user-svc:strategy"

    def test_namespaced_key(self):
        cfg = CacheConfig(namespace="user-svc")
        assert cfg.make_key("user:123") == "aci:user-svc:user:123"

    def test_no_namespace_key_passthrough(self):
        cfg = CacheConfig()
        assert cfg.make_key("user:123") == "user:123"


class TestImmutability:
    """Config should be frozen (immutable after construction)."""

    def test_frozen(self):
        cfg = CacheConfig()
        with pytest.raises(AttributeError):
            cfg.high_threshold = 999  # type: ignore[misc]
