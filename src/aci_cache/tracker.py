"""Sliding-window write rate tracker for aci-cache."""

from __future__ import annotations

import threading
import time
from typing import List


class WriteRateTracker:
    """Tracks write timestamps in a sliding window to calculate writes/sec.

    Thread-safe: the main thread appends via ``record()`` and the controller
    thread reads via ``get_rate()``.
    """

    def __init__(self, window: float = 5.0) -> None:
        if window <= 0:
            raise ValueError(f"window must be positive, got {window}")
        self._window = window
        self._timestamps: List[float] = []
        self._lock = threading.Lock()

    def record(self) -> None:
        """Record a write at the current time."""
        now = time.time()
        with self._lock:
            self._timestamps.append(now)

    def get_rate(self) -> float:
        """Return the current write rate (writes per second).

        Trims timestamps older than the sliding window before computing.
        """
        now = time.time()
        cutoff = now - self._window
        with self._lock:
            # Trim expired entries from the front
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.pop(0)
            count = len(self._timestamps)
        return count / self._window

    def get_count(self) -> int:
        """Return the number of writes currently in the window."""
        now = time.time()
        cutoff = now - self._window
        with self._lock:
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.pop(0)
            return len(self._timestamps)
