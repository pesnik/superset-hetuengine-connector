"""Tests for HetuEngine SQLAlchemy dialect."""

import os
import unittest
from unittest.mock import MagicMock, patch

from sqlalchemy.engine.url import make_url

from superset_hetuengine.sqlalchemy_dialect import HetuEngineDialect


class TestHetuEngineDialect(unittest.TestCase):
    """Test cases for HetuEngineDialect."""

    def setUp(self):
        """Set up test fixtures."""
        self.dialect = HetuEngineDialect()

    def test_dialect_name(self):
        """Test that dialect name is correctly set."""
        self.assertEqual(self.dialect.name, "hetuengine")
        self.assertEqual(self.dialect.driver, "hetuengine")

    def test_default_port(self):
        """Test default port is 29860."""
        self.assertEqual(self.dialect.default_port, 29860)

    def test_jdbc_driver_class(self):
        """Test JDBC driver class name."""
        self.assertEqual(
            self.dialect.jdbc_driver_class, "io.trino.jdbc.TrinoDriver"
        )

    def test_dbapi(self):
        """Test that dbapi returns jaydebeapi module."""
        import jaydebeapi
        self.assertEqual(self.dialect.dbapi(), jaydebeapi)

    @patch.dict(os.environ, {"HETUENGINE_JDBC_JAR": "/opt/test.jar"})
    @patch("os.path.exists", return_value=True)
    def test_create_connect_args_basic(self, mock_exists):
        """Test creating basic connection arguments."""
        url = make_url(
            "hetuengine://testuser:testpass@localhost:29860/hive/default"
        )

        args, kwargs = self.dialect.create_connect_args(url)

        self.assertEqual(len(args), 4)
        self.assertEqual(args[0], "io.trino.jdbc.TrinoDriver")
        self.assertIn("jdbc:trino://localhost:29860/hive/default", args[1])
        self.assertEqual(args[2]["user"], "testuser")
        self.assertEqual(args[2]["password"], "testpass")
        self.assertEqual(args[3], "/opt/test.jar")

    @patch("os.path.exists", return_value=True)
    def test_create_connect_args_with_jar_path_in_url(self, mock_exists):
        """Test creating connection args with jar_path in URL."""
        url = make_url(
            "hetuengine://testuser:testpass@localhost:29860/hive/default"
            "?jar_path=/custom/path.jar"
        )

        args, kwargs = self.dialect.create_connect_args(url)

        self.assertEqual(args[3], "/custom/path.jar")

    def test_create_connect_args_missing_jar_path(self):
        """Test that missing jar_path raises ValueError."""
        url = make_url(
            "hetuengine://testuser:testpass@localhost:29860/hive/default"
        )

        with self.assertRaises(ValueError) as context:
            self.dialect.create_connect_args(url)

        self.assertIn("JDBC driver JAR path not specified", str(context.exception))

    @patch("os.path.exists", return_value=False)
    def test_create_connect_args_jar_not_found(self, mock_exists):
        """Test that non-existent JAR file raises FileNotFoundError."""
        url = make_url(
            "hetuengine://testuser:testpass@localhost:29860/hive/default"
            "?jar_path=/nonexistent.jar"
        )

        with self.assertRaises(FileNotFoundError) as context:
            self.dialect.create_connect_args(url)

        self.assertIn("JAR file not found", str(context.exception))

    def test_build_jdbc_url_basic(self):
        """Test building basic JDBC URL."""
        jdbc_url = self.dialect._build_jdbc_url(
            host="localhost",
            port=29860,
            catalog="hive",
            schema="default",
            connect_args={},
        )

        expected = (
            "jdbc:trino://localhost:29860/hive/default"
            "?serviceDiscoveryMode=hsbroker&tenant=default"
        )
        self.assertEqual(jdbc_url, expected)

    def test_build_jdbc_url_with_multiple_hosts(self):
        """Test building JDBC URL with multiple hosts."""
        jdbc_url = self.dialect._build_jdbc_url(
            host="host1,host2,host3",
            port=29860,
            catalog="hive",
            schema="default",
            connect_args={},
        )

        self.assertIn("host1:29860,host2:29860,host3:29860", jdbc_url)

    def test_build_jdbc_url_with_custom_service_discovery(self):
        """Test building JDBC URL with custom service discovery mode."""
        jdbc_url = self.dialect._build_jdbc_url(
            host="localhost",
            port=29860,
            catalog="hive",
            schema="default",
            connect_args={"service_discovery_mode": "custom"},
        )

        self.assertIn("serviceDiscoveryMode=custom", jdbc_url)

    def test_build_jdbc_url_with_custom_tenant(self):
        """Test building JDBC URL with custom tenant."""
        jdbc_url = self.dialect._build_jdbc_url(
            host="localhost",
            port=29860,
            catalog="hive",
            schema="default",
            connect_args={"tenant": "custom_tenant"},
        )

        self.assertIn("tenant=custom_tenant", jdbc_url)

    def test_build_jdbc_url_with_ssl(self):
        """Test building JDBC URL with SSL enabled."""
        jdbc_url = self.dialect._build_jdbc_url(
            host="localhost",
            port=29860,
            catalog="hive",
            schema="default",
            connect_args={"ssl": "true"},
        )

        self.assertIn("SSL=true", jdbc_url)

    def test_build_jdbc_url_with_ssl_no_verification(self):
        """Test building JDBC URL with SSL but no verification."""
        jdbc_url = self.dialect._build_jdbc_url(
            host="localhost",
            port=29860,
            catalog="hive",
            schema="default",
            connect_args={"ssl": "true", "ssl_verification": "false"},
        )

        self.assertIn("SSL=true", jdbc_url)
        # Note: SSLVerification is added to connection properties, not URL

    def test_get_schema_names(self):
        """Test getting schema names."""
        mock_connection = MagicMock()
        mock_result = [("schema1",), ("schema2",), ("schema3",)]
        mock_connection.execute.return_value = mock_result

        schemas = self.dialect.get_schema_names(mock_connection)

        self.assertEqual(schemas, ["schema1", "schema2", "schema3"])
        mock_connection.execute.assert_called_once_with("SHOW SCHEMAS")

    def test_get_table_names_with_schema(self):
        """Test getting table names with schema specified."""
        mock_connection = MagicMock()
        mock_result = [("table1",), ("table2",)]
        mock_connection.execute.return_value = mock_result

        tables = self.dialect.get_table_names(
            mock_connection, schema="test_schema"
        )

        self.assertEqual(tables, ["table1", "table2"])
        mock_connection.execute.assert_called_once_with(
            "SHOW TABLES FROM test_schema"
        )

    def test_get_table_names_without_schema(self):
        """Test getting table names without schema specified."""
        mock_connection = MagicMock()
        mock_result = [("table1",), ("table2",)]
        mock_connection.execute.return_value = mock_result

        tables = self.dialect.get_table_names(mock_connection)

        self.assertEqual(tables, ["table1", "table2"])
        mock_connection.execute.assert_called_once_with("SHOW TABLES")

    def test_get_view_names_with_schema(self):
        """Test getting view names with schema specified."""
        mock_connection = MagicMock()
        mock_result = [("view1",), ("view2",)]
        mock_connection.execute.return_value = mock_result

        views = self.dialect.get_view_names(mock_connection, schema="test_schema")

        self.assertEqual(views, ["view1", "view2"])
        # Verify query includes schema filter
        call_args = mock_connection.execute.call_args[0][0]
        self.assertIn("test_schema", call_args)
        self.assertIn("table_type = 'VIEW'", call_args)

    def test_get_columns_with_schema(self):
        """Test getting columns with schema specified."""
        mock_connection = MagicMock()
        mock_result = [
            ("id", "integer"),
            ("name", "varchar"),
            ("created_at", "timestamp"),
        ]
        mock_connection.execute.return_value = mock_result

        columns = self.dialect.get_columns(
            mock_connection, "test_table", schema="test_schema"
        )

        self.assertEqual(len(columns), 3)
        self.assertEqual(columns[0]["name"], "id")
        self.assertEqual(columns[1]["name"], "name")
        self.assertEqual(columns[2]["name"], "created_at")

        mock_connection.execute.assert_called_once_with(
            "DESCRIBE test_schema.test_table"
        )

    def test_get_columns_without_schema(self):
        """Test getting columns without schema specified."""
        mock_connection = MagicMock()
        mock_result = [("id", "integer")]
        mock_connection.execute.return_value = mock_result

        columns = self.dialect.get_columns(mock_connection, "test_table")

        mock_connection.execute.assert_called_once_with("DESCRIBE test_table")

    def test_resolve_type_varchar(self):
        """Test resolving VARCHAR type."""
        from sqlalchemy import types

        result = self.dialect._resolve_type("varchar(255)")
        self.assertIsInstance(result, types.VARCHAR)

    def test_resolve_type_integer(self):
        """Test resolving INTEGER type."""
        from sqlalchemy import types

        result = self.dialect._resolve_type("integer")
        self.assertIsInstance(result, types.INTEGER)

    def test_resolve_type_bigint(self):
        """Test resolving BIGINT type."""
        from sqlalchemy import types

        result = self.dialect._resolve_type("bigint")
        self.assertIsInstance(result, types.BIGINT)

    def test_resolve_type_boolean(self):
        """Test resolving BOOLEAN type."""
        from sqlalchemy import types

        result = self.dialect._resolve_type("boolean")
        self.assertIsInstance(result, types.BOOLEAN)

    def test_resolve_type_double(self):
        """Test resolving DOUBLE type."""
        from sqlalchemy import types

        result = self.dialect._resolve_type("double")
        self.assertIsInstance(result, types.FLOAT)

    def test_resolve_type_decimal(self):
        """Test resolving DECIMAL type."""
        from sqlalchemy import types

        result = self.dialect._resolve_type("decimal(10,2)")
        self.assertIsInstance(result, types.DECIMAL)

    def test_resolve_type_date(self):
        """Test resolving DATE type."""
        from sqlalchemy import types

        result = self.dialect._resolve_type("date")
        self.assertIsInstance(result, types.DATE)

    def test_resolve_type_timestamp(self):
        """Test resolving TIMESTAMP type."""
        from sqlalchemy import types

        result = self.dialect._resolve_type("timestamp")
        self.assertIsInstance(result, types.TIMESTAMP)

    def test_resolve_type_unknown(self):
        """Test resolving unknown type defaults to VARCHAR."""
        from sqlalchemy import types

        result = self.dialect._resolve_type("unknown_type")
        self.assertIsInstance(result, types.VARCHAR)

    def test_has_table_exists(self):
        """Test checking if table exists (positive case)."""
        mock_connection = MagicMock()
        mock_connection.execute.return_value = [("test_table",), ("other_table",)]

        result = self.dialect.has_table(
            mock_connection, "test_table", schema="test_schema"
        )

        self.assertTrue(result)

    def test_has_table_not_exists(self):
        """Test checking if table exists (negative case)."""
        mock_connection = MagicMock()
        mock_connection.execute.return_value = [("other_table",)]

        result = self.dialect.has_table(
            mock_connection, "test_table", schema="test_schema"
        )

        self.assertFalse(result)

    def test_has_table_error(self):
        """Test has_table returns False on error."""
        mock_connection = MagicMock()
        mock_connection.execute.side_effect = Exception("Error")

        result = self.dialect.has_table(
            mock_connection, "test_table", schema="test_schema"
        )

        self.assertFalse(result)

    def test_do_ping_success(self):
        """Test successful database ping."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        result = self.dialect.do_ping(mock_connection)

        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("SELECT 1")
        mock_cursor.close.assert_called_once()

    def test_do_ping_failure(self):
        """Test failed database ping."""
        mock_connection = MagicMock()
        mock_connection.cursor.side_effect = Exception("Connection lost")

        result = self.dialect.do_ping(mock_connection)

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
