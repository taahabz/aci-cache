"""Type aliases and message schemas for aci-cache."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Union

# ---------------------------------------------------------------------------
# Strategy name literal (Python 3.9 compat — can't use typing.Literal easily
# at runtime, so we define as a plain str alias + a set of valid values).
# ---------------------------------------------------------------------------
StrategyName = str
VALID_STRATEGIES = frozenset({"ttl", "eager", "batched"})

# ---------------------------------------------------------------------------
# Pub/Sub message schemas (dict-based for JSON serialisation)
# ---------------------------------------------------------------------------
InvalidationMessage = Dict[str, Any]
"""
Expected shape:
{
    "action": "invalidate",
    "keys": ["key1", "key2"],
    "strategy": "eager" | "batched",
    "timestamp": 1717500000.123,
    "source": "instance_abc123",
}
"""

StrategyUpdateMessage = Dict[str, Any]
"""
Expected shape:
{
    "action": "strategy_update",
    "strategy": "eager" | "batched" | "ttl",
    "write_rate": 65.4,
    "timestamp": 1717500000.456,
    "source": "instance_abc123",
}
"""

# ---------------------------------------------------------------------------
# Callback types
# ---------------------------------------------------------------------------
OnSwitchCallback = Callable[[str, str], None]
"""Called when strategy switches: (from_strategy, to_strategy)."""

OnInvalidateCallback = Callable[[List[str]], None]
"""Called when an invalidation message arrives: (keys)."""

OnStrategyUpdateCallback = Callable[[str, Optional[float]], None]
"""Called when a strategy update message arrives: (strategy, write_rate)."""
