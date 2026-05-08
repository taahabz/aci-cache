"""Strategy abstract base class for aci-cache invalidation strategies."""

from __future__ import annotations

import abc


class Strategy(abc.ABC):
    """Base class that all invalidation strategies must implement.

    Each strategy is instantiated once per ``AdaptiveCache`` and receives
    its dependencies via the constructor — no module-level globals.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Return the canonical strategy name (``"ttl"``, ``"eager"``, ``"batched"``)."""

    @abc.abstractmethod
    def on_write(self, key: str) -> None:
        """Called after every ``AdaptiveCache.set()`` call.

        Depending on the strategy this may publish a Pub/Sub invalidation
        message, buffer the key, or do nothing.
        """

    def on_activate(self) -> None:
        """Hook called when this strategy becomes the active one."""

    def on_deactivate(self) -> None:
        """Hook called when switching away from this strategy."""
