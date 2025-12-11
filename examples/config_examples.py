"""
Example Superset configuration for HetuEngine connector.

Copy this file to your Superset pythonpath directory (e.g., superset_config.py)
and customize as needed.
"""

import os

# ---------------------------------------------------------
# Superset Configuration
# ---------------------------------------------------------

# Secret key for session management (change this!)
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-this-secret-key")

# ---------------------------------------------------------
# HetuEngine Configuration
# ---------------------------------------------------------

# JDBC driver path
HETUENGINE_JDBC_JAR = os.environ.get(
    "HETUENGINE_JDBC_JAR", "/opt/hetuengine-jdbc.jar"
)

# Default HetuEngine connection parameters
HETUENGINE_DEFAULT_HOST = os.environ.get("HETUENGINE_HOST", "hetuengine-server")
HETUENGINE_DEFAULT_PORT = int(os.environ.get("HETUENGINE_PORT", "29860"))
HETUENGINE_DEFAULT_CATALOG = os.environ.get("HETUENGINE_CATALOG", "hive")
HETUENGINE_DEFAULT_SCHEMA = os.environ.get("HETUENGINE_SCHEMA", "default")
HETUENGINE_DEFAULT_TENANT = os.environ.get("HETUENGINE_TENANT", "default")

# ---------------------------------------------------------
# Database Configuration
# ---------------------------------------------------------

# Example: PostgreSQL metadata database
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URL",
    "postgresql://superset:superset@postgres:5432/superset"
)

# ---------------------------------------------------------
# Cache Configuration
# ---------------------------------------------------------

# Redis cache configuration
CACHE_CONFIG = {
    "CACHE_TYPE": "redis",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": os.environ.get("REDIS_HOST", "redis"),
    "CACHE_REDIS_PORT": int(os.environ.get("REDIS_PORT", "6379")),
    "CACHE_REDIS_DB": 1,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "redis",
    "CACHE_DEFAULT_TIMEOUT": 3600,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_HOST": os.environ.get("REDIS_HOST", "redis"),
    "CACHE_REDIS_PORT": int(os.environ.get("REDIS_PORT", "6379")),
    "CACHE_REDIS_DB": 2,
}

# ---------------------------------------------------------
# Feature Flags
# ---------------------------------------------------------

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_FILTERS_EXPERIMENTAL": True,
}

# ---------------------------------------------------------
# Row Limits
# ---------------------------------------------------------

ROW_LIMIT = 50000
SQL_MAX_ROW = 100000

# ---------------------------------------------------------
# Timeout Configuration
# ---------------------------------------------------------

SUPERSET_WEBSERVER_TIMEOUT = 300

# SQL Lab query timeout (in seconds)
SQLLAB_TIMEOUT = 300

# ---------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------

import logging

# Enable debug logging for HetuEngine connector
LOG_LEVEL = logging.INFO

# Configure loggers
LOGGERS = {
    "superset_hetuengine": {
        "level": "DEBUG",
    },
    "jaydebeapi": {
        "level": "INFO",
    },
}

# ---------------------------------------------------------
# Example: Pre-configured HetuEngine Database
# ---------------------------------------------------------

# You can programmatically add databases using the Superset CLI or API
# This is just a reference of the configuration

HETUENGINE_DATABASE_CONFIG = {
    "database_name": "HetuEngine Production",
    "sqlalchemy_uri": (
        f"hetuengine://admin:password@{HETUENGINE_DEFAULT_HOST}:"
        f"{HETUENGINE_DEFAULT_PORT}/{HETUENGINE_DEFAULT_CATALOG}/"
        f"{HETUENGINE_DEFAULT_SCHEMA}"
    ),
    "extra": {
        "connect_args": {
            "jar_path": HETUENGINE_JDBC_JAR,
            "service_discovery_mode": "hsbroker",
            "tenant": HETUENGINE_DEFAULT_TENANT,
            "ssl": False,
            "ssl_verification": True,
        },
        "engine_params": {
            "pool_size": 10,
            "max_overflow": 20,
            "pool_timeout": 30,
            "pool_recycle": 3600,
        },
    },
}

# ---------------------------------------------------------
# Security Configuration
# ---------------------------------------------------------

# Enable CSRF protection
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

# Session cookie settings
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False  # Set to True if using HTTPS
SESSION_COOKIE_SAMESITE = "Lax"

# ---------------------------------------------------------
# Email Configuration (Optional)
# ---------------------------------------------------------

# SMTP server configuration for alerts
# SMTP_HOST = "smtp.gmail.com"
# SMTP_STARTTLS = True
# SMTP_SSL = False
# SMTP_USER = "your-email@example.com"
# SMTP_PORT = 587
# SMTP_PASSWORD = "your-password"
# SMTP_MAIL_FROM = "superset@example.com"

# ---------------------------------------------------------
# Async Query Configuration (Optional)
# ---------------------------------------------------------

# Enable async queries for long-running queries
# FEATURE_FLAGS["GLOBAL_ASYNC_QUERIES"] = True

# Celery configuration for async queries
# class CeleryConfig:
#     broker_url = "redis://redis:6379/0"
#     result_backend = "redis://redis:6379/0"
#     task_track_started = True
#     task_serializer = "json"
#     result_serializer = "json"
#     accept_content = ["json"]
#     timezone = "UTC"
#     beat_schedule = {}

# CELERY_CONFIG = CeleryConfig

# ---------------------------------------------------------
# Custom Configuration
# ---------------------------------------------------------

# Allow additional database engines
# PREVENT_UNSAFE_DB_CONNECTIONS = False

# Enable SQL Lab
SUPERSET_WEBSERVER_TIMEOUT = 300

# Custom OAuth configuration (if needed)
# AUTH_TYPE = AUTH_OAUTH
# ...

print("HetuEngine Superset Configuration Loaded")
print(f"JDBC Driver: {HETUENGINE_JDBC_JAR}")
print(f"Default Host: {HETUENGINE_DEFAULT_HOST}:{HETUENGINE_DEFAULT_PORT}")
