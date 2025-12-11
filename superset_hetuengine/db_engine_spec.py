"""
HetuEngine Database Engine Specification for Apache Superset.

This module provides the database engine specification for Huawei HetuEngine,
a Trino-based data warehouse that requires JDBC connectivity.
"""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Type

from sqlalchemy.engine.url import URL
from sqlalchemy.engine import Inspector
from superset.db_engine_specs.base import BaseEngineSpec
from superset.db_engine_specs.presto import PrestoEngineSpec
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType
from superset.models.core import Database
from superset.superset_typing import ResultSetColumnType
from superset.sql_parse import Table

logger = logging.getLogger(__name__)


class HetuEngineSpec(PrestoEngineSpec):
    """
    Database engine specification for Huawei HetuEngine.

    HetuEngine is based on Trino/Presto but requires specific JDBC parameters
    like serviceDiscoveryMode and tenant for proper connectivity.
    """

    engine = "hetuengine"
    engine_name = "HetuEngine"

    # Use PrestoEngineSpec as base since HetuEngine is Trino-based
    _time_grain_expressions = PrestoEngineSpec._time_grain_expressions

    default_driver = "hetuengine"
    sqlalchemy_uri_placeholder = (
        "hetuengine://user:password@host:port/catalog/schema"
    )

    # Encryption parameters
    encryption_parameters = {"ssl": "true"}

    # Custom parameters specific to HetuEngine
    custom_params = {
        "service_discovery_mode": "hsbroker",
        "tenant": "default",
    }

    # Disable async query execution - JayDeBeApi doesn't support cursor.poll()
    run_async = False

    @classmethod
    def get_dbapi_exception_mapping(cls) -> Dict[Type[Exception], Type[Exception]]:
        """
        Map JDBC exceptions to SQLAlchemy exceptions.

        Returns:
            Dictionary mapping JDBC exceptions to appropriate SQLAlchemy exceptions
        """
        # Import here to avoid circular dependencies
        from sqlalchemy import exc

        return {
            Exception: exc.DatabaseError,
        }

    @classmethod
    def epoch_to_dttm(cls) -> str:
        """
        Convert epoch timestamp to datetime.

        Returns:
            SQL expression to convert epoch to datetime
        """
        return "from_unixtime({col})"

    @classmethod
    def convert_dttm(
        cls, target_type: str, dttm: datetime, db_extra: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Convert Python datetime to database-specific datetime literal.

        Args:
            target_type: Target SQL type
            dttm: Python datetime object
            db_extra: Optional extra database parameters

        Returns:
            SQL datetime literal string
        """
        sqla_type = target_type.upper()
        if sqla_type in ("TIMESTAMP", "DATETIME"):
            return f"TIMESTAMP '{dttm.strftime('%Y-%m-%d %H:%M:%S')}'"
        if sqla_type == "DATE":
            return f"DATE '{dttm.strftime('%Y-%m-%d')}'"
        if sqla_type == "TIME":
            return f"TIME '{dttm.strftime('%H:%M:%S')}'"
        return None

    @staticmethod
    def get_extra_params(database) -> Dict[str, Any]:
        """
        Extract HetuEngine-specific parameters from database configuration.

        Args:
            database: Superset database object

        Returns:
            Dictionary of extra parameters for connection
        """
        import json

        extra_params = PrestoEngineSpec.get_extra_params(database)

        # Extract HetuEngine-specific parameters from encrypted_extra or extra
        # These might be JSON strings that need to be parsed
        encrypted_extra = database.encrypted_extra or {}
        if isinstance(encrypted_extra, str):
            try:
                encrypted_extra = json.loads(encrypted_extra)
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"Failed to parse encrypted_extra as JSON: {encrypted_extra}")
                encrypted_extra = {}

        extra = database.extra or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"Failed to parse extra as JSON: {extra}")
                extra = {}

        # Merge custom parameters
        connect_args = extra_params.get("connect_args", {})

        # Check if connect_args are nested in extra or encrypted_extra
        # This handles the case where users configure:
        # {"connect_args": {"jar_path": "...", "service_discovery_mode": "..."}}
        encrypted_extra_connect_args = {}
        if isinstance(encrypted_extra, dict) and "connect_args" in encrypted_extra:
            encrypted_extra_connect_args = encrypted_extra.get("connect_args", {})
            if isinstance(encrypted_extra_connect_args, str):
                try:
                    encrypted_extra_connect_args = json.loads(encrypted_extra_connect_args)
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Failed to parse connect_args from encrypted_extra: {encrypted_extra_connect_args}")
                    encrypted_extra_connect_args = {}

        extra_connect_args = {}
        if isinstance(extra, dict) and "connect_args" in extra:
            extra_connect_args = extra.get("connect_args", {})
            if isinstance(extra_connect_args, str):
                try:
                    extra_connect_args = json.loads(extra_connect_args)
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Failed to parse connect_args from extra: {extra_connect_args}")
                    extra_connect_args = {}

        # Add JDBC-specific parameters
        # Priority: encrypted_extra.connect_args > extra.connect_args > encrypted_extra > extra
        if "jar_path" in encrypted_extra_connect_args:
            connect_args["jar_path"] = encrypted_extra_connect_args["jar_path"]
        elif "jar_path" in extra_connect_args:
            connect_args["jar_path"] = extra_connect_args["jar_path"]
        elif "jar_path" in encrypted_extra:
            connect_args["jar_path"] = encrypted_extra["jar_path"]
        elif "jar_path" in extra:
            connect_args["jar_path"] = extra["jar_path"]

        # Add HetuEngine-specific parameters
        if "service_discovery_mode" in encrypted_extra_connect_args:
            connect_args["service_discovery_mode"] = encrypted_extra_connect_args["service_discovery_mode"]
        elif "service_discovery_mode" in extra_connect_args:
            connect_args["service_discovery_mode"] = extra_connect_args["service_discovery_mode"]
        elif "service_discovery_mode" in encrypted_extra:
            connect_args["service_discovery_mode"] = encrypted_extra["service_discovery_mode"]
        elif "service_discovery_mode" in extra:
            connect_args["service_discovery_mode"] = extra["service_discovery_mode"]
        else:
            connect_args["service_discovery_mode"] = "hsbroker"

        if "tenant" in encrypted_extra_connect_args:
            connect_args["tenant"] = encrypted_extra_connect_args["tenant"]
        elif "tenant" in extra_connect_args:
            connect_args["tenant"] = extra_connect_args["tenant"]
        elif "tenant" in encrypted_extra:
            connect_args["tenant"] = encrypted_extra["tenant"]
        elif "tenant" in extra:
            connect_args["tenant"] = extra["tenant"]
        else:
            connect_args["tenant"] = "default"

        # SSL parameters
        if "ssl" in encrypted_extra_connect_args:
            connect_args["ssl"] = encrypted_extra_connect_args["ssl"]
        elif "ssl" in extra_connect_args:
            connect_args["ssl"] = extra_connect_args["ssl"]
        elif "ssl" in encrypted_extra:
            connect_args["ssl"] = encrypted_extra["ssl"]
        elif "ssl" in extra:
            connect_args["ssl"] = extra["ssl"]

        if "ssl_verification" in encrypted_extra_connect_args:
            connect_args["ssl_verification"] = encrypted_extra_connect_args["ssl_verification"]
        elif "ssl_verification" in extra_connect_args:
            connect_args["ssl_verification"] = extra_connect_args["ssl_verification"]
        elif "ssl_verification" in encrypted_extra:
            connect_args["ssl_verification"] = encrypted_extra["ssl_verification"]
        elif "ssl_verification" in extra:
            connect_args["ssl_verification"] = extra["ssl_verification"]

        extra_params["connect_args"] = connect_args

        return extra_params

    @classmethod
    def build_sqlalchemy_uri(
        cls,
        parameters: Dict[str, Any],
        encrypted_extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Build SQLAlchemy URI from connection parameters.

        Args:
            parameters: Connection parameters (host, port, username, password, etc.)
            encrypted_extra: Encrypted extra parameters

        Returns:
            SQLAlchemy URI string
        """
        query_params = parameters.get("query", {})

        # Build base URI
        uri = (
            f"{cls.engine}://"
            f"{parameters.get('username', '')}:"
            f"{parameters.get('password', '')}@"
            f"{parameters.get('host', 'localhost')}:"
            f"{parameters.get('port', 29860)}/"
            f"{parameters.get('catalog', 'hive')}/"
            f"{parameters.get('schema', 'default')}"
        )

        return uri

    @classmethod
    def get_schema_names(cls, inspector) -> set[str]:
        """
        Get list of schema names from database.

        Args:
            inspector: SQLAlchemy inspector

        Returns:
            Set of schema names
        """
        try:
            return set(inspector.get_schema_names())
        except Exception as e:
            logger.error(f"Error getting schema names: {e}")
            return set()

    @classmethod
    def get_table_names(
        cls, database, inspector, schema: Optional[str]
    ) -> set[str]:
        """
        Get list of table names from schema.

        Args:
            database: Superset database object
            inspector: SQLAlchemy inspector
            schema: Schema name

        Returns:
            Set of table names
        """
        try:
            return set(inspector.get_table_names(schema))
        except Exception as e:
            logger.error(f"Error getting table names: {e}")
            return set()

    @classmethod
    def get_view_names(
        cls, database, inspector, schema: Optional[str]
    ) -> set[str]:
        """
        Get list of view names from schema.

        Args:
            database: Superset database object
            inspector: SQLAlchemy inspector
            schema: Schema name

        Returns:
            Set of view names
        """
        try:
            return set(inspector.get_view_names(schema))
        except Exception as e:
            logger.error(f"Error getting view names: {e}")
            return set()

    @classmethod
    def get_columns(
        cls,
        inspector: Inspector,
        table: Table,
        options: Optional[Dict[str, Any]] = None,
    ) -> List[ResultSetColumnType]:
        """
        Get column information for a table.

        Args:
            inspector: SQLAlchemy inspector
            table: Table instance
            options: Optional parameters

        Returns:
            List of column dictionaries
        """
        try:
            return inspector.get_columns(table.table, table.schema)
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []

    @classmethod
    def extract_error_message(cls, ex: Exception) -> str:
        """
        Extract user-friendly error message from exception.

        Args:
            ex: Exception object

        Returns:
            User-friendly error message
        """
        error_str = str(ex)

        # Check for common JDBC/Java errors
        if "java.lang.ClassNotFoundException" in error_str:
            return (
                "JDBC driver not found. Please ensure the HetuEngine JDBC driver "
                "JAR file is properly configured in the jar_path parameter."
            )

        if "java.sql.SQLException" in error_str:
            # Extract SQL exception message
            match = re.search(r"java\.sql\.SQLException:\s*(.+?)(?:\n|$)", error_str)
            if match:
                return f"Database error: {match.group(1)}"

        if "JVMNotFoundException" in error_str:
            return (
                "Java Virtual Machine not found. Please ensure JAVA_HOME is set "
                "and Java is properly installed."
            )

        if "Connection refused" in error_str:
            return (
                "Unable to connect to HetuEngine server. Please check the host, "
                "port, and network connectivity."
            )

        if "serviceDiscoveryMode" in error_str or "404" in error_str:
            return (
                "Connection failed. Please ensure serviceDiscoveryMode=hsbroker "
                "and tenant parameters are properly configured."
            )

        # Return original message if no specific pattern matched
        return super().extract_error_message(ex)

    @classmethod
    def validate_parameters(
        cls, parameters: Dict[str, Any]
    ) -> List[SupersetError]:
        """
        Validate connection parameters before attempting connection.

        Args:
            parameters: Connection parameters

        Returns:
            List of validation errors
        """
        errors: List[SupersetError] = []

        # Validate required parameters
        required = ["host", "port", "username"]
        for param in required:
            if not parameters.get(param):
                errors.append(
                    SupersetError(
                        message=f"Missing required parameter: {param}",
                        error_type=SupersetErrorType.CONNECTION_MISSING_PARAMETERS_ERROR,
                        level=ErrorLevel.ERROR,
                        extra={"missing": [param]},
                    )
                )

        # Validate port is numeric
        port = parameters.get("port")
        if port and not str(port).isdigit():
            errors.append(
                SupersetError(
                    message="Port must be a valid number",
                    error_type=SupersetErrorType.CONNECTION_INVALID_PORT_ERROR,
                    level=ErrorLevel.ERROR,
                    extra={"port": port},
                )
            )

        return errors

    @classmethod
    def get_default_catalog(cls, database) -> Optional[str]:
        """
        Get default catalog name.

        Args:
            database: Superset database object

        Returns:
            Default catalog name (typically 'hive')
        """
        return "hive"

    @classmethod
    def get_default_schema(
        cls, database: Database, catalog: Optional[str] = None
    ) -> Optional[str]:
        """
        Get default schema name.

        Args:
            database: Superset database object
            catalog: Catalog name

        Returns:
            Default schema name (typically 'default')
        """
        return "default"

    @classmethod
    def get_create_view(
        cls, database: Database, schema: Optional[str], table: str
    ) -> Optional[str]:
        """
        Get CREATE VIEW statement for a view.

        Override PrestoEngineSpec implementation to avoid pyhive dependency.
        HetuEngine via JDBC doesn't support retrieving view definitions easily.

        Args:
            database: Superset database object
            schema: Schema name
            table: Table/view name

        Returns:
            None (view creation not supported via JDBC)
        """
        # Return None instead of trying to fetch CREATE VIEW statement
        # PrestoEngineSpec tries to use pyhive which we don't have
        return None

    @classmethod
    def get_extra_table_metadata(
        cls, database: Database, table: Table
    ) -> Dict[str, Any]:
        """
        Get extra metadata for a table.

        Override PrestoEngineSpec implementation to avoid pyhive dependency.

        Args:
            database: Superset database object
            table: Table object

        Returns:
            Dictionary with empty metadata
        """
        # Return minimal metadata without calling parent's get_create_view
        # which requires pyhive
        return {
            "partitions": {"cols": [], "latest": {}},
            "metadata": {},
        }
