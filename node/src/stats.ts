/**
 * Cache statistics collection for aci-cache.
 *
 * Node.js is single-threaded so no locking is needed.
 */

export interface CacheStatsSnapshot {
  totalReads: number;
  totalWrites: number;
  cacheHits: number;
  cacheMisses: number;
  currentStrategy: string;
  writeRate: number;
  strategySwitches: Array<{ timestamp: number; from: string; to: string }>;
}

export class StatsCollector {
  private _totalReads = 0;
  private _totalWrites = 0;
  private _cacheHits = 0;
  private _cacheMisses = 0;
  private _currentStrategy = "ttl";
  private _writeRate = 0;
  private _strategySwitches: Array<{
    timestamp: number;
    from: string;
    to: string;
  }> = [];

  recordRead(hit: boolean): void {
    this._totalReads++;
    if (hit) {
      this._cacheHits++;
    } else {
      this._cacheMisses++;
    }
  }

  recordWrite(): void {
    this._totalWrites++;
  }

  recordStrategySwitch(from: string, to: string): void {
    this._currentStrategy = to;
    this._strategySwitches.push({ timestamp: Date.now() / 1000, from, to });
  }

  setWriteRate(rate: number): void {
    this._writeRate = rate;
  }

  setCurrentStrategy(strategy: string): void {
    this._currentStrategy = strategy;
  }

  snapshot(): CacheStatsSnapshot {
    return {
      totalReads: this._totalReads,
      totalWrites: this._totalWrites,
      cacheHits: this._cacheHits,
      cacheMisses: this._cacheMisses,
      currentStrategy: this._currentStrategy,
      writeRate: this._writeRate,
      strategySwitches: [...this._strategySwitches],
    };
  }
}
