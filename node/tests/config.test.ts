import { describe, it, expect } from "vitest";
import { CacheConfig } from "../src/config";

describe("CacheConfig", () => {
  describe("defaults", () => {
    it("should have correct default thresholds", () => {
      const cfg = new CacheConfig();
      expect(cfg.highThreshold).toBe(50);
      expect(cfg.lowThreshold).toBe(10);
    });

    it("should have correct default TTL", () => {
      const cfg = new CacheConfig();
      expect(cfg.defaultTtl).toBe(10);
    });

    it("should have correct default intervals", () => {
      const cfg = new CacheConfig();
      expect(cfg.batchInterval).toBe(2.5);
      expect(cfg.controllerInterval).toBe(3);
      expect(cfg.slidingWindow).toBe(5);
    });

    it("should have correct default channels", () => {
      const cfg = new CacheConfig();
      expect(cfg.invalidationChannel).toBe("aci:invalidation");
      expect(cfg.strategyChannel).toBe("aci:strategy");
    });

    it("should default namespace to undefined", () => {
      const cfg = new CacheConfig();
      expect(cfg.namespace).toBeUndefined();
    });

    it("should default isController to true", () => {
      const cfg = new CacheConfig();
      expect(cfg.isController).toBe(true);
    });
  });

  describe("custom overrides", () => {
    it("should accept custom thresholds", () => {
      const cfg = new CacheConfig({ lowThreshold: 5, highThreshold: 100 });
      expect(cfg.lowThreshold).toBe(5);
      expect(cfg.highThreshold).toBe(100);
    });

    it("should accept custom TTL", () => {
      const cfg = new CacheConfig({ defaultTtl: 60 });
      expect(cfg.defaultTtl).toBe(60);
    });

    it("should accept isController false", () => {
      const cfg = new CacheConfig({ isController: false });
      expect(cfg.isController).toBe(false);
    });
  });

  describe("validation", () => {
    it("should throw when lowThreshold >= highThreshold", () => {
      expect(() => new CacheConfig({ lowThreshold: 50, highThreshold: 50 }))
        .toThrow("lowThreshold");
    });

    it("should throw when lowThreshold > highThreshold", () => {
      expect(() => new CacheConfig({ lowThreshold: 60, highThreshold: 50 }))
        .toThrow("lowThreshold");
    });

    it("should throw on non-positive TTL", () => {
      expect(() => new CacheConfig({ defaultTtl: 0 })).toThrow("defaultTtl");
    });

    it("should throw on non-positive batchInterval", () => {
      expect(() => new CacheConfig({ batchInterval: -1 })).toThrow("batchInterval");
    });

    it("should throw on non-positive controllerInterval", () => {
      expect(() => new CacheConfig({ controllerInterval: 0 })).toThrow("controllerInterval");
    });

    it("should throw on non-positive slidingWindow", () => {
      expect(() => new CacheConfig({ slidingWindow: 0 })).toThrow("slidingWindow");
    });

    it("should throw on negative lowThreshold", () => {
      expect(() => new CacheConfig({ lowThreshold: -5 })).toThrow("lowThreshold");
    });
  });

  describe("namespacing", () => {
    it("should namespace channels", () => {
      const cfg = new CacheConfig({ namespace: "user-svc" });
      expect(cfg.invalidationChannel).toBe("aci:user-svc:invalidation");
      expect(cfg.strategyChannel).toBe("aci:user-svc:strategy");
    });

    it("should namespace keys", () => {
      const cfg = new CacheConfig({ namespace: "user-svc" });
      expect(cfg.makeKey("user:123")).toBe("aci:user-svc:user:123");
    });

    it("should pass through keys without namespace", () => {
      const cfg = new CacheConfig();
      expect(cfg.makeKey("user:123")).toBe("user:123");
    });
  });
});
