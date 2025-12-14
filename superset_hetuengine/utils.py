"""
Utility functions for HetuEngine connector.

This module provides helper functions for connection validation,
error handling, and configuration management.
"""

import logging
import os
import subprocess
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def check_java_installation() -> Tuple[bool, Optional[str]]:
    """
    Check if Java is installed and accessible.

    Returns:
        Tuple of (is_installed, java_version)
    """
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        # Java outputs version to stderr
        version_output = result.stderr or result.stdout
        return True, version_output.split("\n")[0] if version_output else None
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
        logger.warning(f"Java check failed: {e}")
        return False, None


def validate_jdbc_jar(jar_path: str) -> Tuple[bool, Optional[str]]:
    """
    Validate that JDBC JAR file exists and is accessible.

    Args:
        jar_path: Path to JDBC JAR file

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not jar_path:
        return False, "JAR path not provided"

    if not os.path.exists(jar_path):
        return False, f"JAR file not found at: {jar_path}"

    if not os.path.isfile(jar_path):
        return False, f"Path is not a file: {jar_path}"

    if not jar_path.endswith(".jar"):
        return False, f"File does not appear to be a JAR file: {jar_path}"

    if not os.access(jar_path, os.R_OK):
        return False, f"JAR file is not readable: {jar_path}"

    return True, None


def parse_connection_string(connection_string: str) -> Dict[str, Any]:
    """
    Parse HetuEngine connection string into components.

    Args:
        connection_string: Connection string (e.g., hetuengine://user:pass@host:port/catalog/schema)

    Returns:
        Dictionary of connection parameters
    """
    import re
    from urllib.parse import unquote

    pattern = r"hetuengine://(?:([^:]+):([^@]+)@)?([^:/]+)(?::(\d+))?(?:/([^/]+))?(?:/([^?]+))?"
    match = re.match(pattern, connection_string)

    if not match:
        raise ValueError(f"Invalid connection string format: {connection_string}")

    username, password, host, port, catalog, schema = match.groups()

    return {
        "username": unquote(username) if username else "",
        "password": unquote(password) if password else "",
        "host": host,
        "port": int(port) if port else 29860,
        "catalog": catalog or "hive",
        "schema": schema or "default",
    }


def build_jdbc_url(
    host: str,
    port: int = 29860,
    catalog: str = "hive",
    schema: str = "default",
    service_discovery_mode: str = "hsbroker",
    tenant: str = "default",
    ssl: bool = False,
    ssl_verification: bool = True,
) -> str:
    """
    Build JDBC URL for HetuEngine connection.

    Args:
        host: Database host (can include multiple comma-separated hosts)
        port: Database port (default: 29860)
        catalog: Catalog name (default: hive)
        schema: Schema name (default: default)
        service_discovery_mode: Service discovery mode (default: hsbroker)
        tenant: Tenant name (default: default)
        ssl: Enable SSL (default: False)
        ssl_verification: Enable SSL verification (default: True)

    Returns:
        JDBC URL string
    """
    # Handle multiple hosts
    if "," in host:
        hosts_with_ports = ",".join([f"{h.strip()}:{port}" for h in host.split(",")])
    else:
        hosts_with_ports = f"{host}:{port}"

    # Build base URL
    jdbc_url = f"jdbc:trino://{hosts_with_ports}/{catalog}/{schema}"

    # Build query parameters
    params = []
    params.append(f"serviceDiscoveryMode={service_discovery_mode}")
    params.append(f"tenant={tenant}")

    if ssl:
        params.append("SSL=true")
        if not ssl_verification:
            params.append("SSLVerification=NONE")

    if params:
        jdbc_url += "?" + "&".join(params)

    return jdbc_url


def validate_connection_params(params: Dict[str, str]) -> List[str]:
    """
    Validate connection parameters.

    Args:
        params: Dictionary of connection parameters

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []

    required_fields = ["host", "username"]
    for field in required_fields:
        if not params.get(field):
            errors.append(f"Missing required parameter: {field}")

    port = params.get("port")
    if port:
        try:
            port_int = int(port)
            if port_int < 1 or port_int > 65535:
                errors.append("Port must be between 1 and 65535")
        except (ValueError, TypeError):
            errors.append("Port must be a valid integer")

    return errors


def get_environment_config() -> Dict[str, Optional[str]]:
    """
    Get HetuEngine configuration from environment variables.

    Returns:
        Dictionary of configuration values from environment
    """
    return {
        "jdbc_jar": os.environ.get("HETUENGINE_JDBC_JAR"),
        "java_home": os.environ.get("JAVA_HOME"),
        "default_host": os.environ.get("HETUENGINE_HOST"),
        "default_port": os.environ.get("HETUENGINE_PORT"),
        "default_catalog": os.environ.get("HETUENGINE_CATALOG", "hive"),
        "default_schema": os.environ.get("HETUENGINE_SCHEMA", "default"),
        "default_tenant": os.environ.get("HETUENGINE_TENANT", "default"),
    }


def format_error_message(error: Exception) -> str:
    """
    Format exception into user-friendly error message.

    Args:
        error: Exception object

    Returns:
        Formatted error message
    """
    error_str = str(error)

    # Common error patterns and their user-friendly messages
    error_mapping = {
        "java.lang.ClassNotFoundException": (
            "JDBC driver not found. Please ensure:\n"
            "1. The HetuEngine JDBC JAR file is properly specified\n"
            "2. The JAR file exists at the specified path\n"
            "3. The JAR file contains io.trino.jdbc.TrinoDriver"
        ),
        "JVMNotFoundException": (
            "Java Virtual Machine not found. Please ensure:\n"
            "1. Java is installed (JDK or JRE)\n"
            "2. JAVA_HOME environment variable is set\n"
            "3. Java is accessible in your PATH"
        ),
        "Connection refused": (
            "Unable to connect to HetuEngine server. Please check:\n"
            "1. Server host and port are correct\n"
            "2. Network connectivity to the server\n"
            "3. HetuEngine service is running\n"
            "4. Firewall rules allow connection"
        ),
        "serviceDiscoveryMode": (
            "Connection failed with service discovery error. Please ensure:\n"
            "1. serviceDiscoveryMode parameter is set to 'hsbroker'\n"
            "2. tenant parameter is properly configured\n"
            "3. You are using HetuEngine JDBC driver (not standard Trino driver)"
        ),
        "Authentication failed": (
            "Authentication failed. Please check:\n"
            "1. Username is correct\n"
            "2. Password is correct\n"
            "3. User has permissions to access the database"
        ),
        "SSL": (
            "SSL/TLS connection error. Please check:\n"
            "1. SSL parameter is correctly set (true/false)\n"
            "2. If using self-signed certificates, set SSLVerification=NONE\n"
            "3. Certificate chain is valid"
        ),
    }

    # Find matching error pattern
    for pattern, message in error_mapping.items():
        if pattern in error_str:
            return message

    # Return original error if no pattern matched
    return f"Connection error: {error_str}"


def test_jdbc_connection(
    jar_path: str,
    host: str,
    port: int,
    username: str,
    password: str,
    catalog: str = "hive",
    schema: str = "default",
    **kwargs,
) -> Tuple[bool, Optional[str]]:
    """
    Test JDBC connection to HetuEngine.

    Args:
        jar_path: Path to JDBC JAR file
        host: Database host
        port: Database port
        username: Username
        password: Password
        catalog: Catalog name
        schema: Schema name
        **kwargs: Additional connection parameters

    Returns:
        Tuple of (success, error_message)
    """
    try:
        import jaydebeapi

        # Validate JAR file
        is_valid, error = validate_jdbc_jar(jar_path)
        if not is_valid:
            return False, error

        # Build JDBC URL
        jdbc_url = build_jdbc_url(
            host=host,
            port=port,
            catalog=catalog,
            schema=schema,
            service_discovery_mode=kwargs.get("service_discovery_mode", "hsbroker"),
            tenant=kwargs.get("tenant", "default"),
            ssl=kwargs.get("ssl", False),
            ssl_verification=kwargs.get("ssl_verification", True),
        )

        # Attempt connection
        connection = jaydebeapi.connect(
            "io.trino.jdbc.TrinoDriver",
            jdbc_url,
            {"user": username, "password": password},
            jar_path,
        )

        # Test query
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        connection.close()

        if result and result[0] == 1:
            return True, None
        else:
            return False, "Connection test query failed"

    except Exception as e:
        logger.error(f"Connection test failed: {e}", exc_info=True)
        return False, format_error_message(e)
