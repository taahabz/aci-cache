"""Configuration dataclass with validation for aci-cache."""

from __future__ import annotations

import dataclasses
from typing import Optional


@dataclasses.dataclass(frozen=True)
class CacheConfig:
    """Immutable configuration for an AdaptiveCache instance.

    All parameters have sensible defaults — zero configuration is required.
    Invalid combinations raise ``ValueError`` at construction time, never
    at runtime (FR-5.4).
    """

    high_threshold: float = 50.0
    """Writes/sec above which the controller switches to the *eager* strategy."""

    low_threshold: float = 10.0
    """Writes/sec below which the controller switches to the *ttl* strategy."""

    default_ttl: int = 10
    """Default Redis key TTL in seconds."""

    batch_interval: float = 2.5
    """Seconds between batch flushes (batched strategy only)."""

    controller_interval: float = 3.0
    """Seconds between controller rate-check cycles."""

    sliding_window: float = 5.0
    """Length (seconds) of the sliding window used for write-rate calculation."""

    channel_prefix: str = "aci"
    """Prefix for Redis Pub/Sub channel names."""

    namespace: Optional[str] = None
    """Optional key namespace.  When set, keys are stored as
    ``aci:{namespace}:{key}`` and Pub/Sub channels are namespaced accordingly."""

    is_controller: bool = True
    """Whether this instance runs the adaptive controller.  Set ``False`` on
    follower instances in multi-instance deployments."""

    def __post_init__(self) -> None:
        # --- threshold ordering ---
        if self.low_threshold >= self.high_threshold:
            raise ValueError(
                f"low_threshold ({self.low_threshold}) must be strictly less "
                f"than high_threshold ({self.high_threshold})"
            )

        # --- positive numbers ---
        if self.default_ttl <= 0:
            raise ValueError(f"default_ttl must be positive, got {self.default_ttl}")
        if self.batch_interval <= 0:
            raise ValueError(f"batch_interval must be positive, got {self.batch_interval}")
        if self.controller_interval <= 0:
            raise ValueError(
                f"controller_interval must be positive, got {self.controller_interval}"
            )
        if self.sliding_window <= 0:
            raise ValueError(f"sliding_window must be positive, got {self.sliding_window}")

        # --- threshold non-negative ---
        if self.low_threshold < 0:
            raise ValueError(f"low_threshold must be non-negative, got {self.low_threshold}")
        if self.high_threshold < 0:
            raise ValueError(f"high_threshold must be non-negative, got {self.high_threshold}")

    # --- derived helpers ---

    @property
    def invalidation_channel(self) -> str:
        """Full Pub/Sub channel name for key invalidation messages."""
        if self.namespace:
            return f"{self.channel_prefix}:{self.namespace}:invalidation"
        return f"{self.channel_prefix}:invalidation"

    @property
    def strategy_channel(self) -> str:
        """Full Pub/Sub channel name for strategy update messages."""
        if self.namespace:
            return f"{self.channel_prefix}:{self.namespace}:strategy"
        return f"{self.channel_prefix}:strategy"

    def make_key(self, key: str) -> str:
        """Prefix *key* with the namespace (if configured), otherwise pass-through."""
        if self.namespace:
            return f"{self.channel_prefix}:{self.namespace}:{key}"
        return key
