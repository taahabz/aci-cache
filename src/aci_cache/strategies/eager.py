"""Eager Strategy — per-write invalidation via Pub/Sub.

Every ``set()`` call immediately publishes an invalidation message so that
all other subscribed instances can delete the stale key.
"""

from __future__ import annotations

import json
import logging
import time

import redis

from .base import Strategy

logger = logging.getLogger(__name__)


class EagerStrategy(Strategy):
    """Publishes an invalidation message on every write for minimum staleness."""

    def __init__(
        self,
        pubsub_client: redis.Redis,
        channel: str,
        instance_id: str,
    ) -> None:
        self._pubsub_client = pubsub_client
        self._channel = channel
        self._instance_id = instance_id

    @property
    def name(self) -> str:
        return "eager"

    def on_write(self, key: str) -> None:
        payload = {
            "action": "invalidate",
            "keys": [key],
            "strategy": "eager",
            "timestamp": time.time(),
            "source": self._instance_id,
        }
        try:
            self._pubsub_client.publish(self._channel, json.dumps(payload))
        except redis.ConnectionError as exc:
            logger.warning("[EAGER] Publish failed (Redis unavailable): %s", exc)

    def on_activate(self) -> None:
        logger.debug("[EAGER] Strategy activated")

    def on_deactivate(self) -> None:
        logger.debug("[EAGER] Strategy deactivated")
