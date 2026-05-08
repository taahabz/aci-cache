"""Tests for the adaptive Controller logic."""

import pytest
from unittest.mock import MagicMock

from aci_cache.controller import Controller
from aci_cache.config import CacheConfig
from aci_cache.stats import StatsCollector
from aci_cache.tracker import WriteRateTracker


class TestStrategySelection:
    """Controller.select_strategy must pick the right strategy at boundaries."""

    @pytest.mark.parametrize(
        "write_rate, expected",
        [
            (0, "ttl"),
            (5, "ttl"),
            (9, "ttl"),         # just below low_threshold
            (9.99, "ttl"),
            (10, "batched"),    # exactly at low_threshold  (not < 10)
            (11, "batched"),
            (25, "batched"),
            (49, "batched"),
            (50, "batched"),    # exactly at high_threshold (not > 50)
            (50.01, "eager"),   # just above high_threshold
            (51, "eager"),
            (100, "eager"),
            (1000, "eager"),
        ],
    )
    def test_boundary_values(self, write_rate, expected):
        result = Controller.select_strategy(
            write_rate=write_rate,
            high_threshold=50,
            low_threshold=10,
        )
        assert result == expected

    def test_custom_thresholds(self):
        assert Controller.select_strategy(5, high_threshold=20, low_threshold=3) == "batched"
        assert Controller.select_strategy(2, high_threshold=20, low_threshold=3) == "ttl"
        assert Controller.select_strategy(25, high_threshold=20, low_threshold=3) == "eager"


class TestControllerLifecycle:
    """Controller start/stop."""

    def test_start_creates_thread(self):
        ctrl = Controller(
            tracker=WriteRateTracker(),
            config=CacheConfig(),
            pubsub_client=MagicMock(),
            stats=StatsCollector(),
            on_switch=MagicMock(),
            instance_id="test",
        )
        ctrl.start()
        assert ctrl._thread is not None
        assert ctrl._thread.is_alive()
        ctrl.stop()

    def test_stop_signals_thread(self):
        ctrl = Controller(
            tracker=WriteRateTracker(),
            config=CacheConfig(controller_interval=0.05),
            pubsub_client=MagicMock(),
            stats=StatsCollector(),
            on_switch=MagicMock(),
            instance_id="test",
        )
        ctrl.start()
        ctrl.stop()
        assert ctrl._running is False

    def test_double_start_is_idempotent(self):
        ctrl = Controller(
            tracker=WriteRateTracker(),
            config=CacheConfig(),
            pubsub_client=MagicMock(),
            stats=StatsCollector(),
            on_switch=MagicMock(),
            instance_id="test",
        )
        ctrl.start()
        thread1 = ctrl._thread
        ctrl.start()  # second call should be no-op
        assert ctrl._thread is thread1
        ctrl.stop()
