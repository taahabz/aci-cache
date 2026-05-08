"""Pub/Sub subscriber for cache invalidation and strategy updates.

Runs as a daemon thread that listens on two Redis Pub/Sub channels:

- ``aci:invalidation`` — key deletion messages from other instances
- ``aci:strategy``     — strategy switch messages from the controller

Handles disconnection gracefully with exponential back-off (FR-4.6) and
ignores messages published by the local instance (FR-4.3).
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Callable, List, Optional

import redis

from .config import CacheConfig

logger = logging.getLogger(__name__)

# Maximum back-off delay (seconds) for reconnection attempts.
_MAX_BACKOFF = 30.0


class Subscriber:
    """Background Pub/Sub listener and invalidation handler.

    Parameters
    ----------
    redis_client : redis.Redis
        Redis connection used to delete invalidated keys.
    pubsub_client : redis.Redis
        Redis connection dedicated to Pub/Sub (blocking ``listen()``).
    config : CacheConfig
        Immutable configuration (channel names, etc.).
    on_invalidate : callable
        ``(keys: list[str]) -> None`` called when an invalidation message
        arrives from another instance.
    on_strategy_update : callable
        ``(strategy: str, write_rate: float | None) -> None`` called when
        a strategy update message arrives.
    instance_id : str
        Unique identifier for this application instance — used to ignore
        self-published messages.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        pubsub_client: redis.Redis,
        config: CacheConfig,
        on_invalidate: Callable[[List[str]], None],
        on_strategy_update: Callable[[str, Optional[float]], None],
        instance_id: str,
    ) -> None:
        self._redis_client = redis_client
        self._pubsub_client = pubsub_client
        self._config = config
        self._on_invalidate = on_invalidate
        self._on_strategy_update = on_strategy_update
        self._instance_id = instance_id

        self._running = False
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the subscriber daemon thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._listen_loop,
            name="aci-subscriber",
            daemon=True,
        )
        self._thread.start()
        logger.debug("[SUBSCRIBER] Started")

    def stop(self) -> None:
        """Signal the subscriber to stop."""
        self._running = False

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    def _listen_loop(self) -> None:
        backoff = 1.0

        while self._running:
            try:
                pubsub = self._pubsub_client.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(
                    self._config.invalidation_channel,
                    self._config.strategy_channel,
                )
                logger.debug(
                    "[SUBSCRIBER] Subscribed to %s, %s",
                    self._config.invalidation_channel,
                    self._config.strategy_channel,
                )

                # Reset back-off on successful connection
                backoff = 1.0

                for message in pubsub.listen():
                    if not self._running:
                        break
                    if not message:
                        continue

                    channel = message.get("channel")
                    raw_data = message.get("data")
                    if not raw_data or not isinstance(raw_data, str):
                        continue

                    try:
                        payload = json.loads(raw_data)
                    except (json.JSONDecodeError, TypeError):
                        logger.warning("[SUBSCRIBER] Invalid Pub/Sub payload: %s", raw_data)
                        continue

                    # Ignore our own messages (FR-4.3)
                    if payload.get("source") == self._instance_id:
                        continue

                    if channel == self._config.invalidation_channel:
                        self._handle_invalidation(payload)
                    elif channel == self._config.strategy_channel:
                        self._handle_strategy_update(payload)

            except redis.ConnectionError as exc:
                if self._running:
                    logger.warning(
                        "[SUBSCRIBER] Pub/Sub disconnected: %s. Retrying in %.1fs…",
                        exc,
                        backoff,
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2, _MAX_BACKOFF)
            except Exception:
                if self._running:
                    logger.exception("[SUBSCRIBER] Unexpected error. Retrying in %.1fs…", backoff)
                    time.sleep(backoff)
                    backoff = min(backoff * 2, _MAX_BACKOFF)

    # ------------------------------------------------------------------
    # Message handlers
    # ------------------------------------------------------------------

    def _handle_invalidation(self, payload: dict) -> None:
        keys = payload.get("keys", [])
        if not isinstance(keys, list) or not keys:
            return

        # Delete keys from local Redis
        str_keys = [str(k) for k in keys]
        try:
            self._redis_client.delete(*str_keys)
            logger.debug("[SUBSCRIBER] Invalidated %d key(s): %s", len(str_keys), str_keys)
        except redis.ConnectionError as exc:
            logger.warning("[SUBSCRIBER] Failed to delete invalidated keys: %s", exc)

        self._on_invalidate(str_keys)

    def _handle_strategy_update(self, payload: dict) -> None:
        strategy = str(payload.get("strategy", "")).lower().strip()
        if not strategy:
            return

        write_rate = payload.get("write_rate")
        rate_val = float(write_rate) if write_rate is not None else None

        self._on_strategy_update(strategy, rate_val)
        logger.debug("[SUBSCRIBER] Strategy update received: %s", strategy)
