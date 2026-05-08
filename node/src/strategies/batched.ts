/**
 * Batched Strategy — buffered invalidation with periodic flush.
 *
 * Written keys are accumulated in an in-memory buffer. A background
 * setInterval (managed by AdaptiveCache) periodically calls flush()
 * to drain the buffer and publish a single invalidation message.
 */

import type Redis from "ioredis";
import { Strategy } from "./base";
import type { InvalidationMessage } from "../types";

export class BatchedStrategy implements Strategy {
  readonly name = "batched";

  private readonly pubsubClient: Redis;
  private readonly channel: string;
  private readonly instanceId: string;
  private buffer: string[] = [];

  constructor(pubsubClient: Redis, channel: string, instanceId: string) {
    this.pubsubClient = pubsubClient;
    this.channel = channel;
    this.instanceId = instanceId;
  }

  async onWrite(key: string): Promise<void> {
    this.buffer.push(key);
  }

  /**
   * Drain the buffer and publish an invalidation message.
   * Deduplicates keys within each batch.
   */
  async flush(): Promise<void> {
    if (this.buffer.length === 0) return;

    // Deduplicate while preserving first-seen order
    const keys = [...new Set(this.buffer)];
    this.buffer = [];

    const payload: InvalidationMessage = {
      action: "invalidate",
      keys,
      strategy: "batched",
      timestamp: Date.now() / 1000,
      source: this.instanceId,
    };

    try {
      await this.pubsubClient.publish(this.channel, JSON.stringify(payload));
    } catch {
      // Swallow connection errors — TTL safety net handles staleness
    }
  }

  onActivate(): void {
    // Nothing to do — flusher interval is managed by AdaptiveCache
  }

  async onDeactivate(): Promise<void> {
    // Flush remaining buffer when leaving batched mode
    await this.flush();
  }
}
