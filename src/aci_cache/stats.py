"""Thread-safe cache statistics collection for aci-cache."""

from __future__ import annotations

import dataclasses
import threading
import time
from typing import List, Tuple


@dataclasses.dataclass
class CacheStats:
    """Snapshot of current cache metrics.

    Returned by ``AdaptiveCache.stats`` — each call produces a fresh snapshot.
    """

    total_reads: int = 0
    total_writes: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    current_strategy: str = "ttl"
    write_rate: float = 0.0
    strategy_switches: List[Tuple[float, str, str]] = dataclasses.field(default_factory=list)
    """List of ``(timestamp, from_strategy, to_strategy)`` transitions."""


class StatsCollector:
    """Thread-safe mutable statistics collector.

    The ``AdaptiveCache`` mutates this via ``record_*`` helpers and exposes
    an immutable ``CacheStats`` snapshot via ``.snapshot()``.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._total_reads: int = 0
        self._total_writes: int = 0
        self._cache_hits: int = 0
        self._cache_misses: int = 0
        self._current_strategy: str = "ttl"
        self._write_rate: float = 0.0
        self._strategy_switches: List[Tuple[float, str, str]] = []

    # ---- mutation methods (called on the hot path) ----

    def record_read(self, hit: bool) -> None:
        with self._lock:
            self._total_reads += 1
            if hit:
                self._cache_hits += 1
            else:
                self._cache_misses += 1

    def record_write(self) -> None:
        with self._lock:
            self._total_writes += 1

    def record_strategy_switch(self, from_strategy: str, to_strategy: str) -> None:
        with self._lock:
            self._current_strategy = to_strategy
            self._strategy_switches.append((time.time(), from_strategy, to_strategy))

    def set_write_rate(self, rate: float) -> None:
        with self._lock:
            self._write_rate = rate

    def set_current_strategy(self, strategy: str) -> None:
        with self._lock:
            self._current_strategy = strategy

    # ---- snapshot ----

    def snapshot(self) -> CacheStats:
        """Return an immutable snapshot of the current stats."""
        with self._lock:
            return CacheStats(
                total_reads=self._total_reads,
                total_writes=self._total_writes,
                cache_hits=self._cache_hits,
                cache_misses=self._cache_misses,
                current_strategy=self._current_strategy,
                write_rate=self._write_rate,
                strategy_switches=list(self._strategy_switches),
            )
