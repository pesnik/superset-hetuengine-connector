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
from superset.db_engine_specs.base import BaseEngineSpec
from superset.db_engine_specs.presto import PrestoEngineSpec
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType

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

    @classmethod
    def get_extra_params(cls, database) -> Dict[str, Any]:
        """
        Extract HetuEngine-specific parameters from database configuration.

        Args:
            database: Superset database object

        Returns:
            Dictionary of extra parameters for connection
        """
        extra_params = super().get_extra_params(database)

        # Extract HetuEngine-specific parameters from encrypted_extra or extra
        encrypted_extra = database.encrypted_extra or {}
        extra = database.extra or {}

        # Merge custom parameters
        connect_args = extra_params.get("connect_args", {})

        # Add JDBC-specific parameters
        if "jar_path" in encrypted_extra:
            connect_args["jar_path"] = encrypted_extra["jar_path"]
        elif "jar_path" in extra:
            connect_args["jar_path"] = extra["jar_path"]

        # Add HetuEngine-specific parameters
        if "service_discovery_mode" in encrypted_extra:
            connect_args["service_discovery_mode"] = encrypted_extra["service_discovery_mode"]
        elif "service_discovery_mode" in extra:
            connect_args["service_discovery_mode"] = extra["service_discovery_mode"]
        else:
            connect_args["service_discovery_mode"] = "hsbroker"

        if "tenant" in encrypted_extra:
            connect_args["tenant"] = encrypted_extra["tenant"]
        elif "tenant" in extra:
            connect_args["tenant"] = extra["tenant"]
        else:
            connect_args["tenant"] = "default"

        # SSL parameters
        if "ssl" in encrypted_extra:
            connect_args["ssl"] = encrypted_extra["ssl"]
        elif "ssl" in extra:
            connect_args["ssl"] = extra["ssl"]

        if "ssl_verification" in encrypted_extra:
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
    def get_schema_names(cls, inspector) -> List[str]:
        """
        Get list of schema names from database.

        Args:
            inspector: SQLAlchemy inspector

        Returns:
            List of schema names
        """
        try:
            return inspector.get_schema_names()
        except Exception as e:
            logger.error(f"Error getting schema names: {e}")
            return []

    @classmethod
    def get_table_names(
        cls, database, inspector, schema: Optional[str]
    ) -> List[str]:
        """
        Get list of table names from schema.

        Args:
            database: Superset database object
            inspector: SQLAlchemy inspector
            schema: Schema name

        Returns:
            List of table names
        """
        try:
            return inspector.get_table_names(schema)
        except Exception as e:
            logger.error(f"Error getting table names: {e}")
            return []

    @classmethod
    def get_view_names(
        cls, database, inspector, schema: Optional[str]
    ) -> List[str]:
        """
        Get list of view names from schema.

        Args:
            database: Superset database object
            inspector: SQLAlchemy inspector
            schema: Schema name

        Returns:
            List of view names
        """
        try:
            return inspector.get_view_names(schema)
        except Exception as e:
            logger.error(f"Error getting view names: {e}")
            return []

    @classmethod
    def get_columns(
        cls, inspector, table_name: str, schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get column information for a table.

        Args:
            inspector: SQLAlchemy inspector
            table_name: Table name
            schema: Schema name

        Returns:
            List of column dictionaries
        """
        try:
            return inspector.get_columns(table_name, schema)
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
    def get_default_schema(cls, database) -> Optional[str]:
        """
        Get default schema name.

        Args:
            database: Superset database object

        Returns:
            Default schema name (typically 'default')
        """
        return "default"
