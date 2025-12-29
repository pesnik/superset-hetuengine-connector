"""
Apache Superset Database Connector for Huawei HetuEngine.

This package provides a database connector for Apache Superset to connect
to Huawei HetuEngine (Trino-based data warehouse) using JDBC bridge.
"""

from superset_hetuengine.db_engine_spec import HetuEngineSpec
from superset_hetuengine.sqlalchemy_dialect import HetuEngineDialect

__version__ = "0.1.13"
__all__ = ["HetuEngineSpec", "HetuEngineDialect"]
