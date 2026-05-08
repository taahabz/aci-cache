# aci-cache

[![CI](https://github.com/taahabz/aci-cache/actions/workflows/ci.yml/badge.svg)](https://github.com/taahabz/aci-cache/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/aci-cache)](https://pypi.org/project/aci-cache/)
[![npm](https://img.shields.io/npm/v/aci-cache)](https://www.npmjs.com/package/aci-cache)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Drop-in Redis cache wrapper that automatically adapts its invalidation strategy to your write traffic.**

Most caching libraries make you choose a strategy upfront and stick with it. `aci-cache` monitors your write rate in real time and switches between strategies automatically — so you get low overhead at low traffic and strong consistency at high traffic, with no manual tuning.

---

## How it works

The library tracks writes per second in a sliding window. Based on configurable thresholds, it picks one of three strategies:

| Write Rate | Strategy | Behaviour |
|---|---|---|
| < 10 writes/sec | **TTL** | Rely on Redis key expiry. Zero overhead. |
| 10–50 writes/sec | **Batched** | Buffer keys, flush a single Pub/Sub message every 2.5s. |
| > 50 writes/sec | **Eager** | Publish an invalidation message on every write. |

In a multi-instance deployment, one instance runs as the **controller** (monitors rate, decides strategy) and broadcasts decisions to all others via a Redis Pub/Sub channel. All instances subscribe and apply the strategy change locally — keeping caches consistent across your entire fleet.

---

## Installation

**Python**
```bash
pip install aci-cache
```

**Node.js**
```bash
npm install aci-cache
```

---

## Quick start

### Python

```python
import redis
from aci_cache import AdaptiveCache

r = redis.Redis(host="localhost", port=6379)
cache = AdaptiveCache(r)

cache.set("user:42", '{"name": "Alice"}')
data = cache.get("user:42")  # '{"name": "Alice"}'

print(cache.strategy)        # "ttl" | "batched" | "eager"
print(cache.stats.total_writes)
```

**Async (asyncio)**

```python
import redis.asyncio as aioredis
from aci_cache import AsyncAdaptiveCache

async def main():
    r = aioredis.Redis()
    async with AsyncAdaptiveCache(r) as cache:
        await cache.set("session:99", "active")
        print(await cache.get("session:99"))
```

### Node.js (TypeScript)

```ts
import Redis from "ioredis";
import { AdaptiveCache } from "aci-cache";

const cache = new AdaptiveCache({ redis: new Redis() });
await cache.start();

await cache.set("user:42", JSON.stringify({ name: "Alice" }));
const data = await cache.get("user:42");

console.log(cache.strategy);           // "ttl" | "batched" | "eager"
console.log(cache.statsSnapshot.totalWrites);

await cache.stop();
```

---

## Multi-instance setup

Run one instance as the controller. All others follow.

**Python**
```python
# Instance 1 — controller (monitors write rate, publishes decisions)
controller_cache = AdaptiveCache(redis.Redis(), is_controller=True)

# Instance 2, 3, … — followers
follower_cache = AdaptiveCache(redis.Redis(), is_controller=False)
```

**Node.js**
```ts
// Instance 1
const cache = new AdaptiveCache({ redis, isController: true });

// Instances 2+
const cache = new AdaptiveCache({ redis, isController: false });
```

---

## Configuration

All parameters have defaults. Override what you need.

| Parameter | Default | Description |
|---|---|---|
| `high_threshold` / `highThreshold` | `50` | Writes/sec that triggers eager mode |
| `low_threshold` / `lowThreshold` | `10` | Writes/sec that drops back to TTL mode |
| `default_ttl` / `defaultTtl` | `10` | Key TTL in seconds |
| `batch_interval` / `batchInterval` | `2.5` | Seconds between batch flushes |
| `controller_interval` / `controllerInterval` | `3` | Seconds between controller rate checks |
| `sliding_window` / `slidingWindow` | `5` | Write rate measurement window in seconds |
| `namespace` | `None` / `undefined` | Key/channel prefix for multi-service Redis |
| `is_controller` / `isController` | `True` / `true` | Whether this instance runs the controller |

**Python example**
```python
cache = AdaptiveCache(
    redis.Redis(),
    high_threshold=100,
    low_threshold=20,
    default_ttl=30,
    namespace="user-service",
    is_controller=True,
)
```

**Node.js example**
```ts
const cache = new AdaptiveCache({
    redis,
    highThreshold: 100,
    lowThreshold: 20,
    defaultTtl: 30,
    namespace: "user-service",
    isController: true,
});
```

---

## Observability

```python
stats = cache.stats
print(stats.total_reads)        # int
print(stats.cache_hits)         # int
print(stats.cache_misses)       # int
print(stats.total_writes)       # int
print(stats.current_strategy)   # "ttl" | "batched" | "eager"
print(stats.write_rate)         # float (writes/sec)

# React to strategy switches
cache.on_switch(lambda from_s, to_s: print(f"{from_s} → {to_s}"))
```

---

## Repository layout

```
aci-cache/
├── src/aci_cache/          # Python package source
│   ├── cache.py            # AdaptiveCache (sync)
│   ├── async_cache.py      # AsyncAdaptiveCache (asyncio)
│   ├── config.py           # CacheConfig
│   ├── controller.py       # Background rate monitor
│   ├── subscriber.py       # Pub/Sub listener
│   ├── tracker.py          # Sliding-window write rate tracker
│   ├── stats.py            # Stats collector
│   └── strategies/         # TTL, Eager, Batched strategy classes
├── tests/                  # Python test suite (99 tests)
├── node/                   # Node.js/TypeScript package
│   ├── src/                # TypeScript source (mirrors Python)
│   └── tests/              # TypeScript test suite (63 tests)
├── pyproject.toml          # Python package config
└── .github/workflows/      # CI (test) + CD (publish on tag)
```

---

## Development

**Python**
```bash
python -m venv venv && source venv/bin/activate
pip install -e ".[dev]" pytest-asyncio
pytest tests/ -v
```

**Node.js**
```bash
cd node
npm install
npm test
npm run build
```

---

## Releasing a new version

1. Bump `version` in `pyproject.toml` and `node/package.json`
2. Commit and tag:
```bash
git commit -am "feat: v0.2.0"
git tag v0.2.0
git push origin main --tags
```
GitHub Actions will run all tests, publish to PyPI and npm, and create a GitHub Release automatically.

---

## License

MIT — see [LICENSE](LICENSE).
