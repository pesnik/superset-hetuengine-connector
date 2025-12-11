"""
Pytest configuration for HetuEngine connector tests.

This module sets up the necessary minimal Flask application context required by
Superset's models and engine specifications during test execution.

Based on Apache Superset's unit test configuration patterns:
https://github.com/apache/superset/blob/master/tests/unit_tests/conftest.py

Note: We create a minimal Flask app with Superset's configuration WITHOUT
full initialization to avoid complex dependencies and marshmallow compatibility issues.
"""

import os
import pytest
from collections.abc import Iterator
from flask import Flask


# NOTE: We must set up the Superset extensions BEFORE importing any Superset models
# because they use them at module-import time. We need to initialize them early.

# Initialize event logger IMMEDIATELY (before any Superset models are imported)
import superset.extensions
from superset.utils.log import AbstractEventLogger

class DummyEventLogger(AbstractEventLogger):
    """Dummy event logger for testing that doesn't do anything."""
    def log(self, *args, **kwargs):
        """No-op log method."""
        pass

    def log_this(self, f):
        """No-op decorator."""
        return f

# Replace the None event_logger with our dummy before any models import
superset.extensions.event_logger = DummyEventLogger()

# Also need to initialize security_manager as a dummy
from unittest.mock import MagicMock
superset.extensions.security_manager = MagicMock()


# Create and configure Flask application at module import time
# This is necessary because Superset modules require app context during import
def _create_test_app() -> Flask:
    """
    Create a minimal Flask application with Superset configuration for testing.

    This loads Superset's default configuration but skips the full app initialization
    to avoid complex dependencies that aren't needed for connector testing.
    """
    app = Flask(__name__)

    # Load default Superset configuration
    # This provides all the config keys that Superset modules expect at import time
    app.config.from_object("superset.config")

    # Override with test-specific settings
    app.config["SQLALCHEMY_DATABASE_URI"] = (
        os.environ.get("SUPERSET__SQLALCHEMY_DATABASE_URI") or "sqlite://"
    )
    app.config["WTF_CSRF_ENABLED"] = False
    app.config["PREVENT_UNSAFE_DB_CONNECTIONS"] = False
    app.config["TESTING"] = True
    app.config["RATELIMIT_ENABLED"] = False
    app.config["CACHE_CONFIG"] = {}
    app.config["DATA_CACHE_CONFIG"] = {}

    # Initialize Flask-Babel (required by Superset modules at import time)
    from flask_babel import Babel
    Babel(app)

    # Initialize encrypted_field_factory (required by Superset models at import time)
    from superset.extensions import encrypted_field_factory
    encrypted_field_factory.init_app(app)

    return app


# Create the app IMMEDIATELY at module import time, before anything else
# This allows the encrypted_field_factory to be initialized before Superset models import
_test_app = _create_test_app()

# Push the app context BEFORE any test files are imported
# This is crucial because test files will import our HetuEngine modules,
# which in turn import Superset modules that need the app context
_app_context = _test_app.app_context()
_app_context.push()


@pytest.fixture(scope="session")
def app() -> Flask:
    """
    Provide the Flask application instance for tests.

    This fixture provides access to the Flask app that was created
    and configured at module import time.

    Returns:
        Flask: The test Flask application instance
    """
    return _test_app


@pytest.fixture
def client(app: Flask):
    """
    Provide a test client for the Flask application.

    Args:
        app: The Flask application instance

    Yields:
        FlaskClient: A test client for making HTTP requests
    """
    with app.test_client() as client:
        yield client


@pytest.fixture(autouse=True)
def app_context(app: Flask) -> Iterator[None]:
    """
    Provide application context for tests.

    This fixture is automatically used for all tests and ensures
    each test has access to the application context.

    Args:
        app: The Flask application instance

    Yields:
        None: Yields control while context is active
    """
    with app.app_context():
        yield
