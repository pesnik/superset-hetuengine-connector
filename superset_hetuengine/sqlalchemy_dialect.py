"""
SQLAlchemy Dialect for HetuEngine with JDBC Bridge Support.

This module provides a custom SQLAlchemy dialect that uses JayDeBeAPI
to connect to HetuEngine via JDBC.
"""

import logging
import os
from typing import Any, Dict, List, Optional

import jaydebeapi
from sqlalchemy import pool, types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler

logger = logging.getLogger(__name__)


class HetuEngineCursorWrapper:
    """
    Wrapper for JayDeBeAPI cursor to provide missing methods.
    """
    
    def __init__(self, cursor):
        self._cursor = cursor
        
    def __getattr__(self, name):
        # Delegate all other attributes to the original cursor
        return getattr(self._cursor, name)
    
    def poll(self):
        """
        Implement poll() method for compatibility.
        
        Returns:
            True if query is still running, False otherwise
        """
        # For JayDeBeAPI, we assume query is complete immediately
        # since JayDeBeAPI is synchronous
        return False


class HetuEngineIdentifierPreparer(compiler.IdentifierPreparer):
    """Custom identifier preparer for HetuEngine."""

    reserved_words = compiler.RESERVED_WORDS.copy()


class HetuEngineCompiler(compiler.SQLCompiler):
    """Custom SQL compiler for HetuEngine."""

    pass


class HetuEngineTypeCompiler(compiler.GenericTypeCompiler):
    """Custom type compiler for HetuEngine."""

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"

    def visit_VARCHAR(self, type_, **kw):
        return "VARCHAR"

    def visit_TEXT(self, type_, **kw):
        return "VARCHAR"

    def visit_INTEGER(self, type_, **kw):
        return "INTEGER"

    def visit_BIGINT(self, type_, **kw):
        return "BIGINT"

    def visit_FLOAT(self, type_, **kw):  # type: ignore[override]
        return "DOUBLE"

    def visit_DECIMAL(self, type_, **kw):
        return f"DECIMAL({type_.precision or 10}, {type_.scale or 0})"

    def visit_TIMESTAMP(self, type_, **kw):
        return "TIMESTAMP"

    def visit_DATE(self, type_, **kw):
        return "DATE"


class HetuEngineDialect(default.DefaultDialect):
    """
    SQLAlchemy dialect for HetuEngine using JDBC bridge.

    This dialect uses JayDeBeAPI to connect to HetuEngine via the Trino JDBC driver.
    """

    name = "hetuengine"
    driver = "hetuengine"

    supports_alter = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_native_decimal = True
    supports_native_boolean = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False

    # Transaction support configuration
    # HetuEngine/Trino connections via JDBC are in auto-commit mode
    # and don't support traditional transaction management
    supports_transactions = False
    supports_statement_cache = False

    # JayDeBeApi doesn't support async cursor operations
    supports_server_side_cursors = False

    preparer = HetuEngineIdentifierPreparer
    statement_compiler = HetuEngineCompiler
    type_compiler = HetuEngineTypeCompiler

    # Default port for HetuEngine
    default_port = 29860

    # JDBC driver configuration
    jdbc_driver_class = "io.trino.jdbc.TrinoDriver"

    @classmethod
    def dbapi(cls):
        """
        Return the DBAPI module (JayDeBeAPI).

        Returns:
            jaydebeapi module
        """
        return jaydebeapi

    def create_connect_args(self, url):  # type: ignore[override]
        """
        Build connection arguments for JDBC connection.

        Args:
            url: SQLAlchemy URL object

        Returns:
            Tuple of (args, kwargs) for connection
        """
        # Extract connection parameters
        username = url.username or ""
        password = url.password or ""
        host = url.host or "localhost"
        port = url.port or self.default_port
        database = url.database or "hive/default"

        # Parse catalog and schema from database
        parts = database.split("/")
        catalog = parts[0] if len(parts) > 0 else "hive"
        schema = parts[1] if len(parts) > 1 else "default"

        # Get connect_args from URL query
        connect_args = url.query.copy() if hasattr(url.query, 'copy') else dict(url.query)

        # Extract JDBC jar path
        jar_path = connect_args.pop("jar_path", None)
        if not jar_path:
            jar_path = os.environ.get("HETUENGINE_JDBC_JAR")

        if not jar_path:
            raise ValueError(
                "JDBC driver JAR path not specified. Please provide jar_path in "
                "connection parameters or set HETUENGINE_JDBC_JAR environment variable."
            )

        if not os.path.exists(jar_path):
            raise FileNotFoundError(
                f"JDBC driver JAR file not found at: {jar_path}"
            )

        # Build JDBC URL with HetuEngine-specific parameters
        jdbc_url = self._build_jdbc_url(
            host=host,
            port=port,
            catalog=catalog,
            schema=schema,
            connect_args=connect_args,
        )

        # Build connection properties
        connection_properties = {
            "user": username,
            "password": password,
        }

        # Add SSL properties if specified
        ssl = connect_args.get("ssl", "").lower()
        if ssl in ("true", "1", "yes"):
            connection_properties["SSL"] = "true"

            ssl_verification = connect_args.get("ssl_verification", "").upper()
            if ssl_verification == "NONE" or ssl_verification == "FALSE":
                connection_properties["SSLVerification"] = "NONE"

        logger.info(f"Connecting to HetuEngine at: {jdbc_url}")

        # Return arguments for jaydebeapi.connect()
        return (
            [
                self.jdbc_driver_class,
                jdbc_url,
                connection_properties,
                jar_path,
            ],
            {},
        )

    def _build_jdbc_url(
        self,
        host: str,
        port: int,
        catalog: str,
        schema: str,
        connect_args: Dict[str, Any],
    ) -> str:
        """
        Build JDBC URL for HetuEngine connection.

        Args:
            host: Database host (can be comma-separated for multiple hosts)
            port: Database port
            catalog: Catalog name
            schema: Schema name
            connect_args: Additional connection arguments

        Returns:
            JDBC URL string
        """
        # Handle multiple hosts
        if "," in host:
            # Multiple hosts for load balancing
            hosts_with_ports = ",".join(
                [f"{h.strip()}:{port}" for h in host.split(",")]
            )
        else:
            hosts_with_ports = f"{host}:{port}"

        # Build base JDBC URL
        jdbc_url = f"jdbc:trino://{hosts_with_ports}/{catalog}/{schema}"

        # Add HetuEngine-specific parameters
        params = []

        # Service discovery mode (required for HetuEngine)
        service_discovery_mode = connect_args.get(
            "service_discovery_mode", "hsbroker"
        )
        params.append(f"serviceDiscoveryMode={service_discovery_mode}")

        # Tenant (default tenant)
        tenant = connect_args.get("tenant", "default")
        params.append(f"tenant={tenant}")

        # SSL
        ssl = connect_args.get("ssl", "").lower()
        if ssl in ("true", "1", "yes"):
            params.append("SSL=true")

        # Add query parameters to URL
        if params:
            jdbc_url += "?" + "&".join(params)

        return jdbc_url

    def connect(self, *args, **kwargs):
        """
        Create a new connection.
        
        Returns:
            Connection object with wrapped cursor
        """
        # Create the connection using parent implementation
        conn = super().connect(*args, **kwargs)
        
        # Create a wrapper for the connection's cursor method
        original_cursor = conn.cursor
        
        def wrapped_cursor():
            cursor = original_cursor()
            return HetuEngineCursorWrapper(cursor)
        
        conn.cursor = wrapped_cursor
        return conn

    def _dbapi_connection(self, connection):
        """
        Return the DBAPI connection from SQLAlchemy connection.
        
        Args:
            connection: SQLAlchemy connection object
            
        Returns:
            DBAPI connection
        """
        return connection.connection

    def _cursor(self, connection):
        """
        Return a cursor from the connection.
        
        Args:
            connection: SQLAlchemy connection object
            
        Returns:
            DBAPI cursor
        """
        cursor = connection.connection.cursor()
        return HetuEngineCursorWrapper(cursor)

    def do_execute(self, cursor, statement, parameters, context=None):
        """
        Execute a statement.
        
        This overrides the default implementation to ensure compatibility
        with JayDeBeAPI cursor.
        
        Args:
            cursor: DBAPI cursor
            statement: SQL statement
            parameters: Parameters for the statement
            context: Execution context
        """
        if parameters:
            cursor.execute(statement, parameters)
        else:
            cursor.execute(statement)

    def do_executemany(self, cursor, statement, parameters, context=None):
        """
        Execute a statement with multiple parameter sets.
        
        Args:
            cursor: DBAPI cursor
            statement: SQL statement
            parameters: List of parameter sets
            context: Execution context
        """
        for params in parameters:
            if params:
                cursor.execute(statement, params)
            else:
                cursor.execute(statement)

    def get_default_isolation_level(self, dbapi_conn):
        """
        Get default isolation level.
        
        HetuEngine connections are auto-commit, so we return None.
        
        Args:
            dbapi_conn: DBAPI connection
            
        Returns:
            None (auto-commit mode)
        """
        return None

    @property
    def supports_isolation_level(self):
        """
        Whether the dialect supports isolation level setting.
        
        Returns:
            False (HetuEngine doesn't support explicit isolation levels)
        """
        return False

    def get_isolation_level(self, dbapi_connection):
        """
        Get current isolation level.
        
        Args:
            dbapi_connection: DBAPI connection
            
        Returns:
            None (auto-commit mode)
        """
        return None

    def set_isolation_level(self, dbapi_connection, level):
        """
        Set isolation level (no-op for HetuEngine).
        
        Args:
            dbapi_connection: DBAPI connection
            level: Isolation level (ignored)
        """
        # Intentionally empty - HetuEngine doesn't support explicit isolation levels
        pass

    def get_schema_names(self, connection, **kw):
        """
        Get list of schema names.

        Args:
            connection: Database connection

        Returns:
            List of schema names
        """
        query = "SHOW SCHEMAS"
        result = connection.execute(query)
        return [row[0] for row in result]

    def get_table_names(self, connection, schema=None, **kw):
        """
        Get list of table names in schema.

        Args:
            connection: Database connection
            schema: Schema name

        Returns:
            List of table names
        """
        if schema:
            query = f"SHOW TABLES FROM {schema}"
        else:
            query = "SHOW TABLES"

        result = connection.execute(query)
        return [row[0] for row in result]

    def get_view_names(self, connection, schema=None, **kw):
        """
        Get list of view names in schema.

        Args:
            connection: Database connection
            schema: Schema name

        Returns:
            List of view names
        """
        # HetuEngine/Trino doesn't have a separate SHOW VIEWS command
        # Views are included in SHOW TABLES
        # We need to filter by querying information_schema
        if schema:
            query = f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                AND table_type = 'VIEW'
            """
        else:
            query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_type = 'VIEW'
            """

        result = connection.execute(query)
        return [row[0] for row in result]

    def get_columns(self, connection, table_name, schema=None, **kw):
        """
        Get column information for a table.

        Args:
            connection: Database connection
            table_name: Table name
            schema: Schema name

        Returns:
            List of column dictionaries
        """
        if schema:
            query = f"DESCRIBE {schema}.{table_name}"
        else:
            query = f"DESCRIBE {table_name}"

        result = connection.execute(query)

        columns = []
        for row in result:
            column_name = row[0]
            column_type = row[1]

            # Include both 'name' and 'column_name' for compatibility with different Superset versions
            columns.append({
                "name": column_name,
                "column_name": column_name,  # Some Superset versions expect this key
                "type": self._resolve_type(column_type),
                "nullable": True,
                "default": None,
            })

        return columns

    def _resolve_type(self, type_string: str):
        """
        Resolve HetuEngine type string to SQLAlchemy type.

        Args:
            type_string: Type string from database

        Returns:
            SQLAlchemy type object
        """
        type_lower = type_string.lower()

        if "varchar" in type_lower or "char" in type_lower:
            return types.VARCHAR()
        elif type_lower == "boolean":
            return types.BOOLEAN()
        elif type_lower == "integer" or type_lower == "int":
            return types.INTEGER()
        elif type_lower == "bigint":
            return types.BIGINT()
        elif type_lower == "double" or type_lower == "float":
            return types.FLOAT()
        elif "decimal" in type_lower:
            return types.DECIMAL()
        elif type_lower == "date":
            return types.DATE()
        elif type_lower == "timestamp":
            return types.TIMESTAMP()
        else:
            return types.VARCHAR()

    def has_table(self, connection, table_name, schema=None, **kw):
        """
        Check if table exists.

        Args:
            connection: Database connection
            table_name: Table name
            schema: Schema name

        Returns:
            True if table exists, False otherwise
        """
        try:
            tables = self.get_table_names(connection, schema=schema)
            return table_name in tables
        except Exception:
            return False

    def do_ping(self, dbapi_connection):
        """
        Ping the database connection.

        Args:
            dbapi_connection: DBAPI connection

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            return False

    def do_rollback(self, dbapi_connection):  # type: ignore[override]
        """
        No-op rollback for auto-commit connections.

        HetuEngine connections via JDBC are in auto-commit mode and don't
        support explicit transaction control. This method is a no-op to
        prevent errors when SQLAlchemy tries to rollback.

        Args:
            dbapi_connection: DBAPI connection (unused)
        """
        # Intentionally empty - auto-commit mode doesn't support rollback
        pass

    def do_commit(self, dbapi_connection):  # type: ignore[override]
        """
        No-op commit for auto-commit connections.

        HetuEngine connections via JDBC are in auto-commit mode and don't
        support explicit transaction control. This method is a no-op to
        prevent errors when SQLAlchemy tries to commit.

        Args:
            dbapi_connection: DBAPI connection (unused)
        """
        # Intentionally empty - auto-commit mode doesn't need explicit commit
        pass

    def do_begin(self, dbapi_connection):  # type: ignore[override]
        """
        No-op begin for auto-commit connections.

        HetuEngine connections via JDBC are in auto-commit mode and don't
        support explicit transaction control. This method is a no-op to
        prevent errors when SQLAlchemy tries to begin a transaction.

        Args:
            dbapi_connection: DBAPI connection (unused)
        """
        # Intentionally empty - auto-commit mode doesn't support transactions
        pass

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """
        Get primary key constraint for a table.

        HetuEngine/Trino doesn't enforce primary key constraints, so this
        returns an empty constraint.

        Args:
            connection: Database connection
            table_name: Table name
            schema: Schema name
            **kw: Additional keyword arguments

        Returns:
            Dictionary with empty constraint info
        """
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Get foreign key constraints for a table.

        HetuEngine/Trino doesn't enforce foreign key constraints, so this
        returns an empty list.

        Args:
            connection: Database connection
            table_name: Table name
            schema: Schema name
            **kw: Additional keyword arguments

        Returns:
            Empty list
        """
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """
        Get indexes for a table.

        HetuEngine/Trino doesn't support traditional indexes, so this
        returns an empty list.

        Args:
            connection: Database connection
            table_name: Table name
            schema: Schema name
            **kw: Additional keyword arguments

        Returns:
            Empty list
        """
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        """
        Get unique constraints for a table.

        HetuEngine/Trino doesn't enforce unique constraints, so this
        returns an empty list.

        Args:
            connection: Database connection
            table_name: Table name
            schema: Schema name
            **kw: Additional keyword arguments

        Returns:
            Empty list
        """
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        """
        Get check constraints for a table.

        HetuEngine/Trino doesn't enforce check constraints, so this
        returns an empty list.

        Args:
            connection: Database connection
            table_name: Table name
            schema: Schema name
            **kw: Additional keyword arguments

        Returns:
            Empty list
        """
        return []
