/**
 * aci-cache — Adaptive Cache Invalidation for Redis.
 *
 * ```ts
 * import Redis from "ioredis";
 * import { AdaptiveCache } from "aci-cache";
 *
 * const cache = new AdaptiveCache({ redis: new Redis() });
 * await cache.start();
 * await cache.set("user:123", "alice");
 * const user = await cache.get("user:123");
 * ```
 */

export { AdaptiveCache } from "./cache";
export type { AdaptiveCacheOptions } from "./cache";
export { CacheConfig } from "./config";
export type { CacheConfigOptions } from "./config";
export type { CacheStatsSnapshot } from "./stats";
export type { Strategy } from "./strategies/base";
