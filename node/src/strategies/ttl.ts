/**
 * TTL Strategy — passive cache expiration (baseline).
 *
 * On write: no-op. Relies entirely on Redis key TTL for expiration.
 */

import { Strategy } from "./base";

export class TTLStrategy implements Strategy {
  readonly name = "ttl";

  async onWrite(_key: string): Promise<void> {
    // No-op: let Redis TTL handle expiration
  }

  onActivate(): void {
    // Nothing to do
  }

  async onDeactivate(): Promise<void> {
    // Nothing to do
  }
}
