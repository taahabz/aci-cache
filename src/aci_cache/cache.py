"""AdaptiveCache — public API for aci-cache.

This is the main user-facing class.  A developer wraps their existing
Redis client and gets adaptive cache invalidation with zero extra effort:

    >>> import redis
    >>> from aci_cache import AdaptiveCache
    >>> r = redis.Redis()
    >>> cache = AdaptiveCache(r)
    >>> cache.set("user:123", "alice")
    >>> cache.get("user:123")
    'alice'
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Any, Callable, List, Optional

import redis

from .config import CacheConfig
from .controller import Controller
from .stats import CacheStats, StatsCollector
from .strategies import BatchedStrategy, EagerStrategy, TTLStrategy
from .strategies.base import Strategy
from .subscriber import Subscriber
from .tracker import WriteRateTracker
from .types import VALID_STRATEGIES

logger = logging.getLogger(__name__)


class AdaptiveCache:
    """Drop-in Redis wrapper with adaptive cache invalidation.

    Parameters
    ----------
    redis_client : redis.Redis
        The developer's existing Redis connection, used for all
        ``get``/``set``/``delete`` operations.
    pubsub_client : redis.Redis | None
        Optional *separate* Redis connection dedicated to Pub/Sub.
        If not provided, a duplicate of *redis_client* is created
        internally (recommended for production to avoid blocking).
    **kwargs
        Any field from :class:`CacheConfig` can be passed as a keyword
        argument to override the default.

    Example
    -------
    ::

        cache = AdaptiveCache(
            redis.Redis(host="localhost"),
            high_threshold=100,
            low_threshold=20,
            default_ttl=30,
        )

        with cache:
            cache.set("key", "value")
            print(cache.get("key"))
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        pubsub_client: Optional[redis.Redis] = None,
        **kwargs: Any,
    ) -> None:
        # --- config ---
        self._config = CacheConfig(**kwargs)

        # --- instance identity ---
        self._instance_id: str = uuid.uuid4().hex[:12]

        # --- redis connections ---
        self._redis = redis_client
        if pubsub_client is not None:
            self._pubsub_redis = pubsub_client
        else:
            # Create a duplicate connection for Pub/Sub to avoid blocking
            # the main client's connection with the subscriber's listen loop.
            conn_kwargs = redis_client.connection_pool.connection_kwargs.copy()
            self._pubsub_redis = redis.Redis(**conn_kwargs)

        # --- shared state ---
        self._tracker = WriteRateTracker(window=self._config.sliding_window)
        self._stats = StatsCollector()

        # --- strategies ---
        self._ttl_strategy = TTLStrategy()
        self._eager_strategy = EagerStrategy(
            pubsub_client=self._pubsub_redis,
            channel=self._config.invalidation_channel,
            instance_id=self._instance_id,
        )
        self._batched_strategy = BatchedStrategy(
            pubsub_client=self._pubsub_redis,
            channel=self._config.invalidation_channel,
            instance_id=self._instance_id,
        )
        self._strategy_map = {
            "ttl": self._ttl_strategy,
            "eager": self._eager_strategy,
            "batched": self._batched_strategy,
        }

        self._current_strategy: Strategy = self._ttl_strategy
        self._strategy_lock = threading.Lock()

        # --- on-switch callbacks ---
        self._switch_callbacks: List[Callable[[str, str], None]] = []

        # --- background workers ---
        self._controller: Optional[Controller] = None
        self._subscriber: Optional[Subscriber] = None
        self._flusher_running = False
        self._flusher_thread: Optional[threading.Thread] = None

        # Auto-start
        self.start()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start all background threads (controller, subscriber, flusher).

        Called automatically by the constructor.
        """
        # Subscriber (all instances)
        self._subscriber = Subscriber(
            redis_client=self._redis,
            pubsub_client=self._pubsub_redis,
            config=self._config,
            on_invalidate=self._on_invalidate,
            on_strategy_update=self._on_strategy_update,
            instance_id=self._instance_id,
        )
        self._subscriber.start()

        # Controller (only on designated instance)
        if self._config.is_controller:
            self._controller = Controller(
                tracker=self._tracker,
                config=self._config,
                pubsub_client=self._pubsub_redis,
                stats=self._stats,
                on_switch=self._apply_switch,
                instance_id=self._instance_id,
            )
            self._controller.start()

        logger.info(
            "[ACI] AdaptiveCache started (instance=%s, controller=%s)",
            self._instance_id,
            self._config.is_controller,
        )

    def stop(self) -> None:
        """Gracefully shut down all background threads and flush pending data."""
        # Stop flusher
        self._flusher_running = False

        # Flush any pending batched keys
        self._batched_strategy.flush()

        # Stop controller
        if self._controller is not None:
            self._controller.stop()

        # Stop subscriber
        if self._subscriber is not None:
            self._subscriber.stop()

        logger.info("[ACI] AdaptiveCache stopped (instance=%s)", self._instance_id)

    def __enter__(self) -> "AdaptiveCache":
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()

    # ------------------------------------------------------------------
    # Public API — Core
    # ------------------------------------------------------------------

    def get(self, key: str) -> Optional[str]:
        """Read a cached value from Redis.

        Returns ``None`` on cache miss.  aci-cache does **not** auto-fill
        the cache on miss — that is the developer's responsibility.
        """
        full_key = self._config.make_key(key)
        try:
            value = self._redis.get(full_key)
            self._stats.record_read(hit=value is not None)
            return value
        except redis.ConnectionError:
            self._stats.record_read(hit=False)
            return None

    def set(self, key: str, value: str, ttl: Optional[int] = None) -> None:
        """Write a value to Redis with TTL, then apply the active strategy.

        Parameters
        ----------
        key : str
            Cache key.
        value : str
            Value to store.
        ttl : int | None
            Override TTL in seconds.  Defaults to ``config.default_ttl``.
        """
        effective_ttl = ttl if ttl is not None else self._config.default_ttl
        full_key = self._config.make_key(key)

        # 1. Write to Redis with TTL (safety net even in eager/batched mode)
        self._redis.setex(full_key, effective_ttl, value)

        # 2. Record write for rate tracking
        self._tracker.record()
        self._stats.record_write()

        # 3. Apply current strategy's on_write hook
        with self._strategy_lock:
            strategy = self._current_strategy
        strategy.on_write(full_key)

    def delete(self, key: str) -> None:
        """Delete a key from local Redis and broadcast invalidation."""
        import json

        full_key = self._config.make_key(key)
        self._redis.delete(full_key)

        # Publish invalidation so other instances also delete
        payload = {
            "action": "invalidate",
            "keys": [full_key],
            "strategy": self.strategy,
            "timestamp": time.time(),
            "source": self._instance_id,
        }
        try:
            self._pubsub_redis.publish(
                self._config.invalidation_channel,
                json.dumps(payload),
            )
        except redis.ConnectionError as exc:
            logger.warning("[ACI] Failed to publish delete invalidation: %s", exc)

    def flush(self) -> None:
        """Flush all keys from local Redis (development / testing helper)."""
        self._redis.flushdb()

    # ------------------------------------------------------------------
    # Public API — Observability
    # ------------------------------------------------------------------

    @property
    def strategy(self) -> str:
        """Return the name of the currently active strategy."""
        with self._strategy_lock:
            return self._current_strategy.name

    @property
    def stats(self) -> CacheStats:
        """Return an immutable snapshot of cache statistics."""
        return self._stats.snapshot()

    def on_switch(self, callback: Callable[[str, str], None]) -> None:
        """Register a callback to be invoked on strategy switches.

        The callback receives ``(from_strategy, to_strategy)``.
        """
        self._switch_callbacks.append(callback)

    @property
    def instance_id(self) -> str:
        """Unique identifier for this cache instance."""
        return self._instance_id

    # ------------------------------------------------------------------
    # Internal — strategy switching
    # ------------------------------------------------------------------

    def _apply_switch(self, from_strategy: str, to_strategy: str) -> None:
        """Switch the active strategy (called by controller or subscriber)."""
        if to_strategy not in VALID_STRATEGIES:
            logger.warning("[ACI] Ignoring invalid strategy: %s", to_strategy)
            return

        with self._strategy_lock:
            old = self._current_strategy
            new = self._strategy_map[to_strategy]

            if old.name == new.name:
                return

            # Deactivate old
            old.on_deactivate()

            # Activate new
            self._current_strategy = new
            new.on_activate()

        # Manage flusher thread for batched strategy
        if to_strategy == "batched":
            self._start_flusher()
        else:
            self._flusher_running = False

        # Record in stats
        self._stats.record_strategy_switch(from_strategy, to_strategy)

        # Notify registered callbacks
        for cb in self._switch_callbacks:
            try:
                cb(from_strategy, to_strategy)
            except Exception:
                logger.exception("[ACI] on_switch callback error")

    # ------------------------------------------------------------------
    # Internal — pub/sub callbacks
    # ------------------------------------------------------------------

    def _on_invalidate(self, keys: List[str]) -> None:
        """Called by the subscriber when invalidation messages arrive."""
        # Keys have already been deleted from Redis by the subscriber.
        pass

    def _on_strategy_update(self, strategy: str, write_rate: Optional[float]) -> None:
        """Called by the subscriber when a strategy update message arrives."""
        with self._strategy_lock:
            current = self._current_strategy.name
        if strategy != current:
            self._apply_switch(current, strategy)

    # ------------------------------------------------------------------
    # Internal — batch flusher
    # ------------------------------------------------------------------

    def _start_flusher(self) -> None:
        """Start the background batch-flush thread."""
        if self._flusher_running:
            return
        self._flusher_running = True
        self._flusher_thread = threading.Thread(
            target=self._flusher_loop,
            name="aci-batch-flusher",
            daemon=True,
        )
        self._flusher_thread.start()
        logger.debug("[ACI] Batch flusher started (interval=%.2fs)", self._config.batch_interval)

    def _flusher_loop(self) -> None:
        while self._flusher_running:
            time.sleep(self._config.batch_interval)
            try:
                self._batched_strategy.flush()
            except Exception:
                logger.exception("[ACI] Batch flusher error")
