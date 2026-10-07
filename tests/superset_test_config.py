"""Minimal Superset config for the connector test suite (no external services)."""

SECRET_KEY = "hetuengine-connector-tests-only"  # noqa: S105 - test-only value
SQLALCHEMY_DATABASE_URI = "sqlite://"
FEATURE_FLAGS: dict = {}
WTF_CSRF_ENABLED = False
TESTING = True
