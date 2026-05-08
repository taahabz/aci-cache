"""Adaptive strategy controller for aci-cache.

Runs as a daemon thread (Python) that periodically computes write rate
and switches the active invalidation strategy:

- rate > high_threshold  → eager
- rate < low_threshold   → ttl
- otherwise              → batched

Only ONE instance in a multi-instance deployment should run the controller
(configured via ``is_controller=True`` on one instance).
"""

from __future__ import annotations

import json
import logging
import threading
import time

import redis

from .config import CacheConfig
from .stats import StatsCollector
from .tracker import WriteRateTracker
from .types import VALID_STRATEGIES

logger = logging.getLogger(__name__)


class Controller:
    """Background write-rate monitor and strategy selector.

    Parameters
    ----------
    tracker : WriteRateTracker
        Shared tracker that the main thread appends to.
    config : CacheConfig
        Immutable configuration for thresholds and intervals.
    pubsub_client : redis.Redis
        Redis connection used to publish strategy update messages.
    stats : StatsCollector
        Shared stats collector for recording write rate + strategy switches.
    on_switch : callable
        ``(from_strategy: str, to_strategy: str) -> None`` callback invoked
        when the controller decides to switch strategy.
    instance_id : str
        Unique identifier for this application instance.
    """

    def __init__(
        self,
        tracker: WriteRateTracker,
        config: CacheConfig,
        pubsub_client: redis.Redis,
        stats: StatsCollector,
        on_switch: ...,
        instance_id: str,
    ) -> None:
        self._tracker = tracker
        self._config = config
        self._pubsub_client = pubsub_client
        self._stats = stats
        self._on_switch = on_switch
        self._instance_id = instance_id

        self._current_strategy: str = "ttl"
        self._lock = threading.Lock()
        self._running = False
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Strategy selection logic
    # ------------------------------------------------------------------

    @staticmethod
    def select_strategy(
        write_rate: float,
        high_threshold: float,
        low_threshold: float,
    ) -> str:
        """Determine the appropriate strategy for the given write rate."""
        if write_rate > high_threshold:
            return "eager"
        if write_rate < low_threshold:
            return "ttl"
        return "batched"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the controller daemon thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop,
            name="aci-controller",
            daemon=True,
        )
        self._thread.start()
        logger.debug("[CONTROLLER] Started (interval=%.1fs)", self._config.controller_interval)

    def stop(self) -> None:
        """Signal the controller to stop.  The daemon thread will exit on
        the next iteration."""
        self._running = False

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    def _loop(self) -> None:
        while self._running:
            try:
                write_rate = self._tracker.get_rate()
                self._stats.set_write_rate(write_rate)

                desired = self.select_strategy(
                    write_rate,
                    self._config.high_threshold,
                    self._config.low_threshold,
                )

                with self._lock:
                    current = self._current_strategy

                if desired != current:
                    logger.info(
                        "[CONTROLLER] write_rate=%.2f w/s, switching %s → %s",
                        write_rate,
                        current,
                        desired,
                    )
                    with self._lock:
                        self._current_strategy = desired

                    self._on_switch(current, desired)
                    self._publish_strategy_update(desired, write_rate)

            except Exception:
                logger.exception("[CONTROLLER] Error in control loop")

            time.sleep(self._config.controller_interval)

    def _publish_strategy_update(self, strategy: str, write_rate: float) -> None:
        payload = {
            "action": "strategy_update",
            "strategy": strategy,
            "write_rate": write_rate,
            "timestamp": time.time(),
            "source": self._instance_id,
        }
        try:
            self._pubsub_client.publish(
                self._config.strategy_channel,
                json.dumps(payload),
            )
        except redis.ConnectionError as exc:
            logger.warning("[CONTROLLER] Failed to publish strategy update: %s", exc)
