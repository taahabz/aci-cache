/**
 * Configuration for an AdaptiveCache instance.
 *
 * All parameters have sensible defaults — zero configuration is required.
 * Invalid combinations throw at construction time, never at runtime.
 */
export interface CacheConfigOptions {
  /** Writes/sec above which the controller switches to eager. Default: 50 */
  highThreshold?: number;
  /** Writes/sec below which the controller switches to TTL. Default: 10 */
  lowThreshold?: number;
  /** Default Redis key TTL in seconds. Default: 10 */
  defaultTtl?: number;
  /** Seconds between batch flushes. Default: 2.5 */
  batchInterval?: number;
  /** Seconds between controller rate-check cycles. Default: 3 */
  controllerInterval?: number;
  /** Sliding window length in seconds for write-rate calculation. Default: 5 */
  slidingWindow?: number;
  /** Prefix for Pub/Sub channel names. Default: "aci" */
  channelPrefix?: string;
  /** Optional key namespace for multi-service Redis. Default: undefined */
  namespace?: string;
  /** Whether this instance runs the adaptive controller. Default: true */
  isController?: boolean;
}

export class CacheConfig {
  readonly highThreshold: number;
  readonly lowThreshold: number;
  readonly defaultTtl: number;
  readonly batchInterval: number;
  readonly controllerInterval: number;
  readonly slidingWindow: number;
  readonly channelPrefix: string;
  readonly namespace: string | undefined;
  readonly isController: boolean;

  constructor(options: CacheConfigOptions = {}) {
    this.highThreshold = options.highThreshold ?? 50;
    this.lowThreshold = options.lowThreshold ?? 10;
    this.defaultTtl = options.defaultTtl ?? 10;
    this.batchInterval = options.batchInterval ?? 2.5;
    this.controllerInterval = options.controllerInterval ?? 3;
    this.slidingWindow = options.slidingWindow ?? 5;
    this.channelPrefix = options.channelPrefix ?? "aci";
    this.namespace = options.namespace;
    this.isController = options.isController ?? true;

    this.validate();
  }

  private validate(): void {
    if (this.lowThreshold >= this.highThreshold) {
      throw new Error(
        `lowThreshold (${this.lowThreshold}) must be strictly less than highThreshold (${this.highThreshold})`
      );
    }
    if (this.defaultTtl <= 0) {
      throw new Error(`defaultTtl must be positive, got ${this.defaultTtl}`);
    }
    if (this.batchInterval <= 0) {
      throw new Error(
        `batchInterval must be positive, got ${this.batchInterval}`
      );
    }
    if (this.controllerInterval <= 0) {
      throw new Error(
        `controllerInterval must be positive, got ${this.controllerInterval}`
      );
    }
    if (this.slidingWindow <= 0) {
      throw new Error(
        `slidingWindow must be positive, got ${this.slidingWindow}`
      );
    }
    if (this.lowThreshold < 0) {
      throw new Error(
        `lowThreshold must be non-negative, got ${this.lowThreshold}`
      );
    }
    if (this.highThreshold < 0) {
      throw new Error(
        `highThreshold must be non-negative, got ${this.highThreshold}`
      );
    }
  }

  /** Full Pub/Sub channel name for key invalidation messages. */
  get invalidationChannel(): string {
    if (this.namespace) {
      return `${this.channelPrefix}:${this.namespace}:invalidation`;
    }
    return `${this.channelPrefix}:invalidation`;
  }

  /** Full Pub/Sub channel name for strategy update messages. */
  get strategyChannel(): string {
    if (this.namespace) {
      return `${this.channelPrefix}:${this.namespace}:strategy`;
    }
    return `${this.channelPrefix}:strategy`;
  }

  /** Prefix key with the namespace (if configured). */
  makeKey(key: string): string {
    if (this.namespace) {
      return `${this.channelPrefix}:${this.namespace}:${key}`;
    }
    return key;
  }
}
