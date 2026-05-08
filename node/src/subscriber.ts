/**
 * Pub/Sub subscriber for cache invalidation and strategy updates.
 *
 * Uses ioredis event-driven subscriber — no threads needed.
 * Handles disconnection gracefully with ioredis built-in reconnection.
 * Ignores messages published by the local instance.
 */

import type Redis from "ioredis";
import { CacheConfig } from "./config";
import type { OnInvalidateCallback, OnStrategyUpdateCallback } from "./types";

export class Subscriber {
  private readonly redisClient: Redis;
  private readonly subClient: Redis;
  private readonly config: CacheConfig;
  private readonly onInvalidate: OnInvalidateCallback;
  private readonly onStrategyUpdate: OnStrategyUpdateCallback;
  private readonly instanceId: string;
  private running = false;

  constructor(
    redisClient: Redis,
    subClient: Redis,
    config: CacheConfig,
    onInvalidate: OnInvalidateCallback,
    onStrategyUpdate: OnStrategyUpdateCallback,
    instanceId: string
  ) {
    this.redisClient = redisClient;
    this.subClient = subClient;
    this.config = config;
    this.onInvalidate = onInvalidate;
    this.onStrategyUpdate = onStrategyUpdate;
    this.instanceId = instanceId;
  }

  /** Start listening on Pub/Sub channels. */
  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;

    this.subClient.on("message", (channel: string, message: string) => {
      this.handleMessage(channel, message);
    });

    await this.subClient.subscribe(
      this.config.invalidationChannel,
      this.config.strategyChannel
    );
  }

  /** Stop listening. */
  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;

    try {
      await this.subClient.unsubscribe(
        this.config.invalidationChannel,
        this.config.strategyChannel
      );
    } catch {
      // Ignore errors during shutdown
    }

    this.subClient.removeAllListeners("message");
  }

  private handleMessage(channel: string, raw: string): void {
    let payload: any;
    try {
      payload = JSON.parse(raw);
    } catch {
      return; // Invalid JSON — skip
    }

    // Ignore our own messages (FR-4.3)
    if (payload.source === this.instanceId) return;

    if (channel === this.config.invalidationChannel) {
      this.handleInvalidation(payload);
    } else if (channel === this.config.strategyChannel) {
      this.handleStrategyUpdate(payload);
    }
  }

  private handleInvalidation(payload: any): void {
    const keys = payload.keys;
    if (!Array.isArray(keys) || keys.length === 0) return;

    const strKeys: string[] = keys.map(String);

    // Delete keys from local Redis
    this.redisClient.del(...strKeys).catch(() => {
      // Swallow connection errors
    });

    this.onInvalidate(strKeys);
  }

  private handleStrategyUpdate(payload: any): void {
    const strategy = String(payload.strategy ?? "").toLowerCase().trim();
    if (!strategy) return;

    const writeRate =
      payload.write_rate != null ? Number(payload.write_rate) : null;

    this.onStrategyUpdate(strategy, writeRate);
  }
}
