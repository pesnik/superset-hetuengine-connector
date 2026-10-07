"""
Pytest configuration for HetuEngine connector tests.

Superset's models and engine specs need an initialized Flask app at import
time (security manager, encrypted field factory, ...). Faking those internals
breaks between Superset releases, so build a real, minimal app instead with
``superset.app.create_app`` and an in-memory SQLite metadata DB. This works on
Superset 5.x and 6.x alike.
"""

import os
from collections.abc import Iterator

import pytest
from flask import Flask

os.environ.setdefault(
    "SUPERSET_CONFIG_PATH",
    os.path.join(os.path.dirname(__file__), "superset_test_config.py"),
)

from superset.app import create_app  # noqa: E402  (needs SUPERSET_CONFIG_PATH)

_test_app = create_app()
# Pushed at import time: test modules import superset_hetuengine (and thus
# Superset models) during collection, before any fixture runs.
_app_context = _test_app.app_context()
_app_context.push()


@pytest.fixture(scope="session")
def app() -> Flask:
    """The minimal Superset Flask app used by all tests."""
    return _test_app


@pytest.fixture
def client(app: Flask):
    """Flask test client."""
    with app.test_client() as client:
        yield client


@pytest.fixture(autouse=True)
def app_context(app: Flask) -> Iterator[None]:
    """Run every test inside an application context."""
    with app.app_context():
        yield
