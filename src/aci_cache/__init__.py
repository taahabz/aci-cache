"""aci-cache — Adaptive Cache Invalidation for Redis.

Usage::

    import redis
    from aci_cache import AdaptiveCache

    cache = AdaptiveCache(redis.Redis())
    cache.set("user:123", "alice")
    print(cache.get("user:123"))
"""

from .cache import AdaptiveCache
from .async_cache import AsyncAdaptiveCache
from .config import CacheConfig
from .stats import CacheStats

__all__ = [
    "AdaptiveCache",
    "AsyncAdaptiveCache",
    "CacheConfig",
    "CacheStats",
]

__version__ = "0.1.0"
