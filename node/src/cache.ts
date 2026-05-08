/**
 * AdaptiveCache — public API for aci-cache (Node.js).
 *
 * Usage:
 *
 * ```ts
 * import Redis from "ioredis";
 * import { AdaptiveCache } from "aci-cache";
 *
 * const redis = new Redis();
 * const cache = new AdaptiveCache({ redis });
 * await cache.set("user:123", "alice");
 * const user = await cache.get("user:123");
 * ```
 */

import type Redis from "ioredis";
import { CacheConfig, type CacheConfigOptions } from "./config";
import { Controller } from "./controller";
import { StatsCollector, type CacheStatsSnapshot } from "./stats";
import {
  TTLStrategy,
  EagerStrategy,
  BatchedStrategy,
  type Strategy,
} from "./strategies";
import { Subscriber } from "./subscriber";
import { WriteRateTracker } from "./tracker";
import { VALID_STRATEGIES, type OnSwitchCallback } from "./types";
import { randomBytes } from "crypto";

export interface AdaptiveCacheOptions extends CacheConfigOptions {
  /** Required: the developer's existing ioredis client. */
  redis: Redis;
  /** Optional: separate ioredis client for Pub/Sub. */
  pubsubRedis?: Redis;
}

export class AdaptiveCache {
  private readonly redis: Redis;
  private readonly pubsubRedis: Redis;
  private readonly config: CacheConfig;
  private readonly instanceId: string;

  private readonly tracker: WriteRateTracker;
  private readonly stats: StatsCollector;

  // Strategies
  private readonly ttlStrategy: TTLStrategy;
  private readonly eagerStrategy: EagerStrategy;
  private readonly batchedStrategy: BatchedStrategy;
  private readonly strategyMap: Record<string, Strategy>;
  private currentStrategy: Strategy;

  // Background workers
  private controller: Controller | null = null;
  private subscriber: Subscriber | null = null;
  private flusherTimer: ReturnType<typeof setInterval> | null = null;

  // Callbacks
  private switchCallbacks: OnSwitchCallback[] = [];

  constructor(options: AdaptiveCacheOptions) {
    const { redis, pubsubRedis, ...configOpts } = options;

    this.redis = redis;
    this.pubsubRedis = pubsubRedis ?? redis.duplicate();
    this.config = new CacheConfig(configOpts);
    this.instanceId = randomBytes(6).toString("hex");

    // Shared state
    this.tracker = new WriteRateTracker(this.config.slidingWindow);
    this.stats = new StatsCollector();

    // Strategies
    this.ttlStrategy = new TTLStrategy();
    this.eagerStrategy = new EagerStrategy(
      this.pubsubRedis,
      this.config.invalidationChannel,
      this.instanceId
    );
    this.batchedStrategy = new BatchedStrategy(
      this.pubsubRedis,
      this.config.invalidationChannel,
      this.instanceId
    );
    this.strategyMap = {
      ttl: this.ttlStrategy,
      eager: this.eagerStrategy,
      batched: this.batchedStrategy,
    };
    this.currentStrategy = this.ttlStrategy;
  }

  // ------------------------------------------------------------------
  // Lifecycle
  // ------------------------------------------------------------------

  /** Start all background workers. Must be called before use. */
  async start(): Promise<void> {
    // Subscriber (all instances)
    this.subscriber = new Subscriber(
      this.redis,
      this.pubsubRedis,
      this.config,
      (keys) => this.onInvalidate(keys),
      (strategy, writeRate) => this.onStrategyUpdate(strategy, writeRate)
,
      this.instanceId
    );
    await this.subscriber.start();

    // Controller (designated instance only)
    if (this.config.isController) {
      this.controller = new Controller(
        this.tracker,
        this.config,
        this.pubsubRedis,
        this.stats,
        (from, to) => this.applySwitch(from, to),
        this.instanceId
      );
      this.controller.start();
    }
  }

  /** Gracefully shut down all background workers. */
  async stop(): Promise<void> {
    // Stop flusher
    if (this.flusherTimer) {
      clearInterval(this.flusherTimer);
      this.flusherTimer = null;
    }

    // Flush remaining batch buffer
    await this.batchedStrategy.flush();

    // Stop controller
    if (this.controller) {
      this.controller.stop();
    }

    // Stop subscriber
    if (this.subscriber) {
      await this.subscriber.stop();
    }
  }

  // ------------------------------------------------------------------
  // Public API — Core
  // ------------------------------------------------------------------

  /** Read a cached value. Returns null on miss. */
  async get(key: string): Promise<string | null> {
    const fullKey = this.config.makeKey(key);
    try {
      const value = await this.redis.get(fullKey);
      this.stats.recordRead(value !== null);
      return value;
    } catch {
      this.stats.recordRead(false);
      return null;
    }
  }

  /** Write a value with TTL, then apply the active strategy. */
  async set(key: string, value: string, ttl?: number): Promise<void> {
    const effectiveTtl = ttl ?? this.config.defaultTtl;
    const fullKey = this.config.makeKey(key);

    // 1. Write to Redis with TTL (safety net)
    await this.redis.setex(fullKey, effectiveTtl, value);

    // 2. Record write for rate tracking
    this.tracker.record();
    this.stats.recordWrite();

    // 3. Apply current strategy's on_write hook
    await this.currentStrategy.onWrite(fullKey);
  }

  /** Delete a key locally and broadcast invalidation. */
  async delete(key: string): Promise<void> {
    const fullKey = this.config.makeKey(key);
    await this.redis.del(fullKey);

    // Publish invalidation
    const payload = {
      action: "invalidate",
      keys: [fullKey],
      strategy: this.strategy,
      timestamp: Date.now() / 1000,
      source: this.instanceId,
    };

    try {
      await this.pubsubRedis.publish(
        this.config.invalidationChannel,
        JSON.stringify(payload)
      );
    } catch {
      // Swallow connection errors
    }
  }

  /** Flush all keys from local Redis. */
  async flush(): Promise<void> {
    await this.redis.flushdb();
  }

  // ------------------------------------------------------------------
  // Public API — Observability
  // ------------------------------------------------------------------

  /** Current active strategy name. */
  get strategy(): string {
    return this.currentStrategy.name;
  }

  /** Snapshot of cache statistics. */
  get statsSnapshot(): CacheStatsSnapshot {
    return this.stats.snapshot();
  }

  /** Register a callback for strategy switches. */
  onSwitch(callback: OnSwitchCallback): void {
    this.switchCallbacks.push(callback);
  }

  /** Unique instance identifier. */
  get id(): string {
    return this.instanceId;
  }

  // ------------------------------------------------------------------
  // Internal — strategy switching
  // ------------------------------------------------------------------

  private async applySwitch(from: string, to: string): Promise<void> {
    if (!VALID_STRATEGIES.has(to)) return;

    const oldStrategy = this.currentStrategy;
    const newStrategy = this.strategyMap[to];
    if (!newStrategy || oldStrategy.name === newStrategy.name) return;

    // Deactivate old
    await oldStrategy.onDeactivate();

    // Activate new
    this.currentStrategy = newStrategy;
    newStrategy.onActivate();

    // Manage flusher interval
    if (to === "batched") {
      this.startFlusher();
    } else if (this.flusherTimer) {
      clearInterval(this.flusherTimer);
      this.flusherTimer = null;
    }

    // Record in stats
    this.stats.recordStrategySwitch(from, to);

    // Notify callbacks
    for (const cb of this.switchCallbacks) {
      try {
        cb(from, to);
      } catch {
        // Swallow callback errors
      }
    }
  }

  // ------------------------------------------------------------------
  // Internal — Pub/Sub handlers
  // ------------------------------------------------------------------

  private onInvalidate(_keys: string[]): void {
    // Keys already deleted by subscriber
  }

  private onStrategyUpdate(
    strategy: string,
    _writeRate: number | null
  ): void {
    const current = this.currentStrategy.name;
    if (strategy !== current) {
      this.applySwitch(current, strategy);
    }
  }

  // ------------------------------------------------------------------
  // Internal — batch flusher
  // ------------------------------------------------------------------

  private startFlusher(): void {
    if (this.flusherTimer) return;

    this.flusherTimer = setInterval(() => {
      this.batchedStrategy.flush().catch(() => {
        // Swallow errors
      });
    }, this.config.batchInterval * 1000);

    if (
      this.flusherTimer &&
      typeof this.flusherTimer === "object" &&
      "unref" in this.flusherTimer
    ) {
      this.flusherTimer.unref();
    }
  }
}
