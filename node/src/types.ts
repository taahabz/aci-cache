/**
 * Type definitions for aci-cache.
 */

/** Valid strategy names. */
export type StrategyName = "ttl" | "eager" | "batched";

export const VALID_STRATEGIES: ReadonlySet<string> = new Set([
  "ttl",
  "eager",
  "batched",
]);

/** Pub/Sub invalidation message schema. */
export interface InvalidationMessage {
  action: "invalidate";
  keys: string[];
  strategy: StrategyName;
  timestamp: number;
  source: string;
}

/** Pub/Sub strategy update message schema. */
export interface StrategyUpdateMessage {
  action: "strategy_update";
  strategy: StrategyName;
  write_rate: number;
  timestamp: number;
  source: string;
}

/** Callback invoked on strategy switch. */
export type OnSwitchCallback = (from: string, to: string) => void;

/** Callback invoked on key invalidation from another instance. */
export type OnInvalidateCallback = (keys: string[]) => void;

/** Callback invoked on strategy update from the controller. */
export type OnStrategyUpdateCallback = (
  strategy: string,
  writeRate: number | null
) => void;
