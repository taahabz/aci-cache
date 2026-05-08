"""AsyncAdaptiveCache — asyncio variant of the aci-cache public API.

Same adaptive invalidation logic, but fully ``await``-able using
``redis.asyncio`` instead of blocking ``redis.Redis``.

Usage::

    import redis.asyncio as aioredis
    from aci_cache import AsyncAdaptiveCache

    async def main():
        r = aioredis.Redis()
        async with AsyncAdaptiveCache(r) as cache:
            await cache.set("user:123", "alice")
            user = await cache.get("user:123")
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Callable, List, Optional

import redis.asyncio as aioredis

from .config import CacheConfig
from .stats import CacheStats, StatsCollector
from .tracker import WriteRateTracker
from .types import VALID_STRATEGIES

logger = logging.getLogger(__name__)


class AsyncAdaptiveCache:
    """Async drop-in Redis wrapper with adaptive cache invalidation.

    Parameters
    ----------
    redis_client : redis.asyncio.Redis
        The developer's existing async Redis connection.
    pubsub_client : redis.asyncio.Redis | None
        Optional separate async Redis connection for Pub/Sub.
    **kwargs
        Any field from :class:`CacheConfig`.
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        pubsub_client: Optional[aioredis.Redis] = None,
        **kwargs: Any,
    ) -> None:
        self._config = CacheConfig(**kwargs)
        self._instance_id: str = uuid.uuid4().hex[:12]

        self._redis = redis_client
        self._pubsub_redis = pubsub_client if pubsub_client is not None else redis_client

        # Shared state
        self._tracker = WriteRateTracker(window=self._config.sliding_window)
        self._stats = StatsCollector()

        # Strategy state (simple string-based, no thread locks needed)
        self._current_strategy: str = "ttl"

        # Batch buffer
        self._batch_buffer: List[str] = []

        # Switch callbacks
        self._switch_callbacks: List[Callable[[str, str], None]] = []

        # Background tasks
        self._controller_task: Optional[asyncio.Task] = None
        self._subscriber_task: Optional[asyncio.Task] = None
        self._flusher_task: Optional[asyncio.Task] = None
        self._pubsub: Optional[aioredis.client.PubSub] = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start background asyncio tasks (controller, subscriber)."""
        self._running = True

        # Subscriber (all instances)
        self._subscriber_task = asyncio.create_task(self._subscriber_loop())

        # Controller (designated instance only)
        if self._config.is_controller:
            self._controller_task = asyncio.create_task(self._controller_loop())

        logger.info(
            "[ACI-ASYNC] Started (instance=%s, controller=%s)",
            self._instance_id,
            self._config.is_controller,
        )

    async def stop(self) -> None:
        """Gracefully shut down all background tasks."""
        self._running = False

        # Flush pending batch buffer
        await self._flush_batch()

        # Cancel tasks
        for task in [self._controller_task, self._subscriber_task, self._flusher_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Clean up pubsub
        if self._pubsub:
            try:
                await self._pubsub.unsubscribe()
                await self._pubsub.close()
            except Exception:
                pass

        logger.info("[ACI-ASYNC] Stopped (instance=%s)", self._instance_id)

    async def __aenter__(self) -> "AsyncAdaptiveCache":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    # ------------------------------------------------------------------
    # Public API — Core
    # ------------------------------------------------------------------

    async def get(self, key: str) -> Optional[str]:
        """Read a cached value. Returns None on miss."""
        full_key = self._config.make_key(key)
        try:
            value = await self._redis.get(full_key)
            self._stats.record_read(hit=value is not None)
            return value
        except Exception:
            self._stats.record_read(hit=False)
            return None

    async def set(self, key: str, value: str, ttl: Optional[int] = None) -> None:
        """Write a value with TTL, then apply the active strategy."""
        effective_ttl = ttl if ttl is not None else self._config.default_ttl
        full_key = self._config.make_key(key)

        # 1. Write to Redis with TTL
        await self._redis.setex(full_key, effective_ttl, value)

        # 2. Record write
        self._tracker.record()
        self._stats.record_write()

        # 3. Strategy on_write
        if self._current_strategy == "eager":
            await self._publish_invalidation([full_key])
        elif self._current_strategy == "batched":
            self._batch_buffer.append(full_key)

    async def delete(self, key: str) -> None:
        """Delete a key and broadcast invalidation."""
        full_key = self._config.make_key(key)
        await self._redis.delete(full_key)
        await self._publish_invalidation([full_key])

    async def flush(self) -> None:
        """Flush all keys from local Redis."""
        await self._redis.flushdb()

    # ------------------------------------------------------------------
    # Public API — Observability
    # ------------------------------------------------------------------

    @property
    def strategy(self) -> str:
        """Name of the currently active strategy."""
        return self._current_strategy

    @property
    def stats(self) -> CacheStats:
        """Immutable snapshot of cache statistics."""
        return self._stats.snapshot()

    def on_switch(self, callback: Callable[[str, str], None]) -> None:
        """Register a strategy-switch callback."""
        self._switch_callbacks.append(callback)

    @property
    def instance_id(self) -> str:
        return self._instance_id

    # ------------------------------------------------------------------
    # Internal — helpers
    # ------------------------------------------------------------------

    async def _publish_invalidation(self, keys: List[str]) -> None:
        payload = {
            "action": "invalidate",
            "keys": keys,
            "strategy": self._current_strategy,
            "timestamp": time.time(),
            "source": self._instance_id,
        }
        try:
            await self._pubsub_redis.publish(
                self._config.invalidation_channel, json.dumps(payload)
            )
        except Exception:
            pass

    async def _flush_batch(self) -> None:
        if not self._batch_buffer:
            return
        keys = list(dict.fromkeys(self._batch_buffer))
        self._batch_buffer.clear()
        await self._publish_invalidation(keys)

    def _apply_switch(self, from_strategy: str, to_strategy: str) -> None:
        if to_strategy not in VALID_STRATEGIES:
            return
        if from_strategy == to_strategy:
            return

        # If leaving batched, schedule a flush
        if from_strategy == "batched" and self._batch_buffer:
            asyncio.ensure_future(self._flush_batch())

        self._current_strategy = to_strategy

        # Start/stop flusher
        if to_strategy == "batched":
            self._start_flusher()
        elif self._flusher_task and not self._flusher_task.done():
            self._flusher_task.cancel()
            self._flusher_task = None

        # Stats
        self._stats.record_strategy_switch(from_strategy, to_strategy)

        # Callbacks
        for cb in self._switch_callbacks:
            try:
                cb(from_strategy, to_strategy)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Internal — controller
    # ------------------------------------------------------------------

    async def _controller_loop(self) -> None:
        try:
            while self._running:
                write_rate = self._tracker.get_rate()
                self._stats.set_write_rate(write_rate)

                if write_rate > self._config.high_threshold:
                    desired = "eager"
                elif write_rate < self._config.low_threshold:
                    desired = "ttl"
                else:
                    desired = "batched"

                current = self._current_strategy
                if desired != current:
                    self._apply_switch(current, desired)
                    # Publish strategy update
                    payload = {
                        "action": "strategy_update",
                        "strategy": desired,
                        "write_rate": write_rate,
                        "timestamp": time.time(),
                        "source": self._instance_id,
                    }
                    try:
                        await self._pubsub_redis.publish(
                            self._config.strategy_channel, json.dumps(payload)
                        )
                    except Exception:
                        pass

                await asyncio.sleep(self._config.controller_interval)
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # Internal — subscriber
    # ------------------------------------------------------------------

    async def _subscriber_loop(self) -> None:
        backoff = 1.0
        try:
            while self._running:
                try:
                    self._pubsub = self._pubsub_redis.pubsub()
                    await self._pubsub.subscribe(
                        self._config.invalidation_channel,
                        self._config.strategy_channel,
                    )
                    backoff = 1.0

                    async for message in self._pubsub.listen():
                        if not self._running:
                            break
                        if message["type"] != "message":
                            continue

                        channel = message["channel"]
                        if isinstance(channel, bytes):
                            channel = channel.decode()
                        raw = message["data"]
                        if isinstance(raw, bytes):
                            raw = raw.decode()

                        try:
                            payload = json.loads(raw)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        # Ignore self
                        if payload.get("source") == self._instance_id:
                            continue

                        if channel == self._config.invalidation_channel:
                            keys = payload.get("keys", [])
                            if isinstance(keys, list) and keys:
                                try:
                                    await self._redis.delete(*[str(k) for k in keys])
                                except Exception:
                                    pass

                        elif channel == self._config.strategy_channel:
                            strategy = str(payload.get("strategy", "")).lower().strip()
                            if strategy and strategy != self._current_strategy:
                                self._apply_switch(self._current_strategy, strategy)

                except Exception:
                    if self._running:
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 30.0)
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # Internal — flusher
    # ------------------------------------------------------------------

    def _start_flusher(self) -> None:
        if self._flusher_task and not self._flusher_task.done():
            return
        self._flusher_task = asyncio.create_task(self._flusher_loop())

    async def _flusher_loop(self) -> None:
        try:
            while self._running and self._current_strategy == "batched":
                await asyncio.sleep(self._config.batch_interval)
                await self._flush_batch()
        except asyncio.CancelledError:
            pass
