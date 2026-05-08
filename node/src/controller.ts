/**
 * Adaptive strategy controller for aci-cache.
 *
 * Uses setInterval to periodically compute write rate and switch the
 * active invalidation strategy. Only one instance should run the
 * controller in a multi-instance deployment.
 */

import type Redis from "ioredis";
import { CacheConfig } from "./config";
import { StatsCollector } from "./stats";
import { WriteRateTracker } from "./tracker";
import type { StrategyUpdateMessage } from "./types";

export class Controller {
  private readonly tracker: WriteRateTracker;
  private readonly config: CacheConfig;
  private readonly pubsubClient: Redis;
  private readonly stats: StatsCollector;
  private readonly onSwitch: (from: string, to: string) => void;
  private readonly instanceId: string;

  private currentStrategy = "ttl";
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(
    tracker: WriteRateTracker,
    config: CacheConfig,
    pubsubClient: Redis,
    stats: StatsCollector,
    onSwitch: (from: string, to: string) => void,
    instanceId: string
  ) {
    this.tracker = tracker;
    this.config = config;
    this.pubsubClient = pubsubClient;
    this.stats = stats;
    this.onSwitch = onSwitch;
    this.instanceId = instanceId;
  }

  /**
   * Determine the appropriate strategy for the given write rate.
   */
  static selectStrategy(
    writeRate: number,
    highThreshold: number,
    lowThreshold: number
  ): string {
    if (writeRate > highThreshold) return "eager";
    if (writeRate < lowThreshold) return "ttl";
    return "batched";
  }

  /** Start the controller interval. */
  start(): void {
    if (this.timer) return;

    this.timer = setInterval(() => {
      this.tick();
    }, this.config.controllerInterval * 1000);

    // Prevent the timer from keeping the process alive
    if (this.timer && typeof this.timer === "object" && "unref" in this.timer) {
      this.timer.unref();
    }
  }

  /** Stop the controller interval. */
  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private tick(): void {
    try {
      const writeRate = this.tracker.getRate();
      this.stats.setWriteRate(writeRate);

      const desired = Controller.selectStrategy(
        writeRate,
        this.config.highThreshold,
        this.config.lowThreshold
      );

      if (desired !== this.currentStrategy) {
        const previous = this.currentStrategy;
        this.currentStrategy = desired;
        this.onSwitch(previous, desired);
        this.publishStrategyUpdate(desired, writeRate);
      }
    } catch {
      // Swallow errors to keep the interval alive
    }
  }

  private publishStrategyUpdate(
    strategy: string,
    writeRate: number
  ): void {
    const payload: StrategyUpdateMessage = {
      action: "strategy_update",
      strategy: strategy as any,
      write_rate: writeRate,
      timestamp: Date.now() / 1000,
      source: this.instanceId,
    };

    this.pubsubClient
      .publish(this.config.strategyChannel, JSON.stringify(payload))
      .catch(() => {
        // Swallow connection errors
      });
  }
}
