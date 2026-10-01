"""Pytest fixtures: fresh event store per test, ASGI transport client.

The existing test suite runs against the ephemeral in-memory backend (set
before importing the app, so the durable default never creates a database
during tests). Persistence-specific tests construct their own SQLite store.
"""

import os

# Must be set before ``app.config`` / ``app.main`` are imported anywhere.
os.environ.setdefault("CAL_STORAGE", "memory")

import pytest
from fastapi.testclient import TestClient

from app import main as main_module


@pytest.fixture()
def client() -> TestClient:
    main_module.store.reset()
    with TestClient(main_module.app) as c:
        yield c
    main_module.store.reset()
