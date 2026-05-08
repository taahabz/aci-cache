/**
 * Eager Strategy — per-write invalidation via Pub/Sub.
 *
 * Every set() call immediately publishes an invalidation message so that
 * all other subscribed instances can delete the stale key.
 */

import type Redis from "ioredis";
import { Strategy } from "./base";
import type { InvalidationMessage } from "../types";

export class EagerStrategy implements Strategy {
  readonly name = "eager";

  private readonly pubsubClient: Redis;
  private readonly channel: string;
  private readonly instanceId: string;

  constructor(pubsubClient: Redis, channel: string, instanceId: string) {
    this.pubsubClient = pubsubClient;
    this.channel = channel;
    this.instanceId = instanceId;
  }

  async onWrite(key: string): Promise<void> {
    const payload: InvalidationMessage = {
      action: "invalidate",
      keys: [key],
      strategy: "eager",
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
    // Nothing to do
  }

  async onDeactivate(): Promise<void> {
    // Nothing to do
  }
}
