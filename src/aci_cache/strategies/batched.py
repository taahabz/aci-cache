"""Batched Strategy — buffered invalidation with periodic flush.

Written keys are accumulated in an in-memory buffer.  A background flusher
(managed by ``AdaptiveCache``) periodically calls ``flush()`` to drain the
buffer and publish a single invalidation message for all buffered keys.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import List

import redis

from .base import Strategy

logger = logging.getLogger(__name__)


class BatchedStrategy(Strategy):
    """Buffers written keys and flushes invalidation periodically."""

    def __init__(
        self,
        pubsub_client: redis.Redis,
        channel: str,
        instance_id: str,
    ) -> None:
        self._pubsub_client = pubsub_client
        self._channel = channel
        self._instance_id = instance_id
        self._buffer: List[str] = []
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        return "batched"

    def on_write(self, key: str) -> None:
        """Append *key* to the buffer (thread-safe, O(1))."""
        with self._lock:
            self._buffer.append(key)

    def flush(self) -> None:
        """Drain the buffer and publish an invalidation message.

        The lock is released **before** the Redis ``PUBLISH`` call to avoid
        holding a lock during network I/O (FR-3.11).
        """
        with self._lock:
            if not self._buffer:
                return
            # Deduplicate while preserving first-seen order
            keys = list(dict.fromkeys(self._buffer))
            self._buffer.clear()

        # --- lock is released here — safe to do network I/O ---
        payload = {
            "action": "invalidate",
            "keys": keys,
            "strategy": "batched",
            "timestamp": time.time(),
            "source": self._instance_id,
        }
        try:
            self._pubsub_client.publish(self._channel, json.dumps(payload))
            logger.debug("[BATCHED] Published invalidation for %d key(s)", len(keys))
        except redis.ConnectionError as exc:
            logger.warning("[BATCHED] Publish failed (Redis unavailable): %s", exc)

    def on_activate(self) -> None:
        logger.debug("[BATCHED] Strategy activated")

    def on_deactivate(self) -> None:
        # Flush remaining buffer when leaving batched mode (FR-3.12)
        self.flush()
        logger.debug("[BATCHED] Strategy deactivated — buffer flushed")
