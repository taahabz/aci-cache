/**
 * Sliding-window write rate tracker for aci-cache.
 *
 * No locking needed — Node.js is single-threaded.
 */
export class WriteRateTracker {
  private readonly window: number;
  private timestamps: number[] = [];

  constructor(window = 5) {
    if (window <= 0) {
      throw new Error(`window must be positive, got ${window}`);
    }
    this.window = window;
  }

  /** Record a write at the current time. */
  record(): void {
    this.timestamps.push(Date.now() / 1000);
  }

  /** Return the current write rate (writes per second). */
  getRate(): number {
    this.trim();
    return this.timestamps.length / this.window;
  }

  /** Return the number of writes currently in the window. */
  getCount(): number {
    this.trim();
    return this.timestamps.length;
  }

  /** Remove timestamps older than the sliding window. */
  private trim(): void {
    const cutoff = Date.now() / 1000 - this.window;
    while (this.timestamps.length > 0 && this.timestamps[0] < cutoff) {
      this.timestamps.shift();
    }
  }
}
