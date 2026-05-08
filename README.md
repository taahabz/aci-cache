# aci-cache

> **Adaptive cache invalidation for Redis. Drop-in wrapper, zero config.**

`aci-cache` wraps your existing Redis client and adds an intelligent invalidation
layer that automatically adjusts its strategy based on real-time write patterns:

- **Low write rate** → TTL-based expiry (minimal overhead)
- **Medium write rate** → Batched invalidation (balanced efficiency)
- **High write rate** → Eager per-write invalidation (maximum consistency)

## Quick Start

```bash
pip install aci-cache
```

```python
import redis
from aci_cache import AdaptiveCache

# Wrap your existing Redis client — that's it
r = redis.Redis(host="localhost")
cache = AdaptiveCache(r)

# Use it just like Redis
cache.set("user:123", "alice")
user = cache.get("user:123")

# Check what strategy is active
print(cache.strategy)  # "ttl", "batched", or "eager"
```

## How It Works

The library monitors your write rate in real time using a sliding window.
Based on configurable thresholds, it automatically switches between three
invalidation strategies:

| Strategy | When Active | Behavior |
|----------|------------|----------|
| **TTL** | < 10 writes/sec | Standard Redis TTL. Zero overhead. |
| **Batched** | 10–50 writes/sec | Buffers keys, flushes every 2.5s via Pub/Sub. |
| **Eager** | > 50 writes/sec | Publishes invalidation per write via Pub/Sub. |

## Multi-Instance Support

When running multiple app instances behind a load balancer, `aci-cache`
coordinates cache invalidation across all instances via Redis Pub/Sub:

```python
# Instance 1 (controller)
cache = AdaptiveCache(redis.Redis(), is_controller=True)

# Instance 2-N (followers)
cache = AdaptiveCache(redis.Redis(), is_controller=False)
```

## Configuration

All settings have sensible defaults. Override via constructor:

```python
cache = AdaptiveCache(
    redis.Redis(),
    high_threshold=100,    # writes/sec → eager
    low_threshold=20,      # writes/sec → ttl
    default_ttl=30,        # seconds
    batch_interval=5.0,    # seconds between batch flushes
    namespace="my-service", # key namespace for multi-service Redis
)
```

## License

MIT
