"""TTL Strategy — passive cache expiration (baseline).

On write: no-op.  Relies entirely on Redis key TTL for cache expiration.
This is the lowest-overhead strategy with zero Pub/Sub traffic.
"""

from __future__ import annotations

import logging

from .base import Strategy

logger = logging.getLogger(__name__)


class TTLStrategy(Strategy):
    """Passive TTL-only invalidation — no Pub/Sub messages on write."""

    @property
    def name(self) -> str:
        return "ttl"

    def on_write(self, key: str) -> None:
        # No-op: let Redis TTL handle expiration
        pass

    def on_activate(self) -> None:
        logger.debug("[TTL] Strategy activated — using passive TTL expiration")

    def on_deactivate(self) -> None:
        logger.debug("[TTL] Strategy deactivated")
