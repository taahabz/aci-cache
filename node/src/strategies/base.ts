/**
 * Strategy interface for aci-cache invalidation strategies.
 */
export interface Strategy {
  /** Canonical strategy name. */
  readonly name: string;

  /** Called after every set() call. */
  onWrite(key: string): Promise<void>;

  /** Hook called when this strategy becomes active. */
  onActivate(): void;

  /** Hook called when switching away from this strategy. */
  onDeactivate(): Promise<void>;
}
