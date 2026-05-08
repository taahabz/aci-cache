import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { AdaptiveCache } from "../src/cache";

// Minimal ioredis mock for unit testing
function createMockRedis() {
  const data = new Map<string, string>();

  return {
    get: vi.fn(async (key: string) => data.get(key) ?? null),
    setex: vi.fn(async (_key: string, _ttl: number, value: string) => {
      data.set(_key, value);
      return "OK";
    }),
    del: vi.fn(async (...keys: string[]) => {
      let count = 0;
      for (const k of keys) {
        if (data.delete(k)) count++;
      }
      return count;
    }),
    flushdb: vi.fn(async () => {
      data.clear();
      return "OK";
    }),
    publish: vi.fn().mockResolvedValue(1),
    subscribe: vi.fn().mockResolvedValue(undefined),
    unsubscribe: vi.fn().mockResolvedValue(undefined),
    on: vi.fn(),
    removeAllListeners: vi.fn(),
    duplicate: vi.fn(),
    _data: data,
  } as any;
}

describe("AdaptiveCache", () => {
  let cache: AdaptiveCache;
  let mockRedis: any;
  let mockPubsub: any;

  beforeEach(async () => {
    mockRedis = createMockRedis();
    mockPubsub = createMockRedis();

    cache = new AdaptiveCache({
      redis: mockRedis,
      pubsubRedis: mockPubsub,
      isController: false,
    });
    await cache.start();
  });

  afterEach(async () => {
    await cache.stop();
  });

  describe("core API", () => {
    it("should set and get a value", async () => {
      await cache.set("key1", "value1");
      const result = await cache.get("key1");
      expect(result).toBe("value1");
    });

    it("should return null for nonexistent key", async () => {
      const result = await cache.get("missing");
      expect(result).toBeNull();
    });

    it("should delete a key", async () => {
      await cache.set("key1", "value1");
      await cache.delete("key1");
      const result = await cache.get("key1");
      expect(result).toBeNull();
    });

    it("should flush all keys", async () => {
      await cache.set("a", "1");
      await cache.set("b", "2");
      await cache.flush();
      expect(await cache.get("a")).toBeNull();
      expect(await cache.get("b")).toBeNull();
    });
  });

  describe("observability", () => {
    it("should start with TTL strategy", () => {
      expect(cache.strategy).toBe("ttl");
    });

    it("should return stats snapshot", () => {
      const stats = cache.statsSnapshot;
      expect(stats.totalReads).toBe(0);
      expect(stats.totalWrites).toBe(0);
    });

    it("should track reads", async () => {
      await cache.get("missing");
      const stats = cache.statsSnapshot;
      expect(stats.totalReads).toBe(1);
      expect(stats.cacheMisses).toBe(1);
    });

    it("should track writes", async () => {
      await cache.set("k", "v");
      const stats = cache.statsSnapshot;
      expect(stats.totalWrites).toBe(1);
    });

    it("should track hits", async () => {
      await cache.set("k", "v");
      await cache.get("k");
      const stats = cache.statsSnapshot;
      expect(stats.cacheHits).toBe(1);
    });

    it("should have a 12-char instance id", () => {
      expect(cache.id).toHaveLength(12);
    });
  });

  describe("namespacing", () => {
    it("should namespace keys when configured", async () => {
      const nsMock = createMockRedis();
      const nsPub = createMockRedis();
      const nsCache = new AdaptiveCache({
        redis: nsMock,
        pubsubRedis: nsPub,
        namespace: "myapp",
        isController: false,
      });
      await nsCache.start();

      await nsCache.set("user:1", "alice");

      // Should call setex with namespaced key
      expect(nsMock.setex).toHaveBeenCalledWith(
        "aci:myapp:user:1",
        10,
        "alice"
      );

      // get should also use namespace
      const result = await nsCache.get("user:1");
      expect(result).toBe("alice");

      await nsCache.stop();
    });
  });
});
