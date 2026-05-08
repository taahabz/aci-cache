"""Shared test fixtures for aci-cache tests."""

from __future__ import annotations

import pytest
import fakeredis


@pytest.fixture
def fake_redis():
    """Return a fakeredis client for isolated testing."""
    server = fakeredis.FakeServer()
    return fakeredis.FakeRedis(server=server, decode_responses=True)


@pytest.fixture
def fake_redis_pair():
    """Return two fakeredis clients sharing the same server.

    Useful for testing pub/sub where you need a separate client for
    the subscriber's blocking ``listen()`` call.
    """
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server, decode_responses=True)
    pubsub_client = fakeredis.FakeRedis(server=server, decode_responses=True)
    return client, pubsub_client
