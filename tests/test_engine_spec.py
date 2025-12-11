"""Tests for HetuEngine database engine specification."""

import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from superset_hetuengine.db_engine_spec import HetuEngineSpec


class TestHetuEngineSpec(unittest.TestCase):
    """Test cases for HetuEngineSpec."""

    def test_engine_name(self):
        """Test that engine name is correctly set."""
        self.assertEqual(HetuEngineSpec.engine, "hetuengine")
        self.assertEqual(HetuEngineSpec.engine_name, "HetuEngine")

    def test_sqlalchemy_uri_placeholder(self):
        """Test SQLAlchemy URI placeholder format."""
        expected = "hetuengine://user:password@host:port/catalog/schema"
        self.assertEqual(HetuEngineSpec.sqlalchemy_uri_placeholder, expected)

    def test_convert_dttm_timestamp(self):
        """Test converting Python datetime to TIMESTAMP."""
        dttm = datetime(2024, 1, 15, 10, 30, 45)
        result = HetuEngineSpec.convert_dttm("TIMESTAMP", dttm)
        self.assertEqual(result, "TIMESTAMP '2024-01-15 10:30:45'")

    def test_convert_dttm_date(self):
        """Test converting Python datetime to DATE."""
        dttm = datetime(2024, 1, 15, 10, 30, 45)
        result = HetuEngineSpec.convert_dttm("DATE", dttm)
        self.assertEqual(result, "DATE '2024-01-15'")

    def test_convert_dttm_time(self):
        """Test converting Python datetime to TIME."""
        dttm = datetime(2024, 1, 15, 10, 30, 45)
        result = HetuEngineSpec.convert_dttm("TIME", dttm)
        self.assertEqual(result, "TIME '10:30:45'")

    def test_convert_dttm_unsupported(self):
        """Test converting datetime to unsupported type returns None."""
        dttm = datetime(2024, 1, 15, 10, 30, 45)
        result = HetuEngineSpec.convert_dttm("UNSUPPORTED", dttm)
        self.assertIsNone(result)

    def test_epoch_to_dttm(self):
        """Test epoch to datetime conversion expression."""
        result = HetuEngineSpec.epoch_to_dttm()
        self.assertEqual(result, "from_unixtime({col})")

    def test_get_extra_params_with_jar_path(self):
        """Test extracting extra params including jar_path."""
        mock_database = MagicMock()
        mock_database.encrypted_extra = {"jar_path": "/opt/driver.jar"}
        mock_database.extra = {}

        with patch.object(HetuEngineSpec.__bases__[0], 'get_extra_params', return_value={}):
            params = HetuEngineSpec.get_extra_params(mock_database)

        self.assertIn("connect_args", params)
        self.assertEqual(params["connect_args"]["jar_path"], "/opt/driver.jar")

    def test_get_extra_params_with_service_discovery_mode(self):
        """Test extracting service discovery mode parameter."""
        mock_database = MagicMock()
        mock_database.encrypted_extra = {"service_discovery_mode": "hsbroker"}
        mock_database.extra = {}

        with patch.object(HetuEngineSpec.__bases__[0], 'get_extra_params', return_value={}):
            params = HetuEngineSpec.get_extra_params(mock_database)

        self.assertEqual(
            params["connect_args"]["service_discovery_mode"], "hsbroker"
        )

    def test_get_extra_params_with_tenant(self):
        """Test extracting tenant parameter."""
        mock_database = MagicMock()
        mock_database.encrypted_extra = {"tenant": "custom_tenant"}
        mock_database.extra = {}

        with patch.object(HetuEngineSpec.__bases__[0], 'get_extra_params', return_value={}):
            params = HetuEngineSpec.get_extra_params(mock_database)

        self.assertEqual(params["connect_args"]["tenant"], "custom_tenant")

    def test_get_extra_params_defaults(self):
        """Test default values for extra params."""
        mock_database = MagicMock()
        mock_database.encrypted_extra = {}
        mock_database.extra = {}

        with patch.object(HetuEngineSpec.__bases__[0], 'get_extra_params', return_value={}):
            params = HetuEngineSpec.get_extra_params(mock_database)

        # Check defaults
        self.assertEqual(
            params["connect_args"]["service_discovery_mode"], "hsbroker"
        )
        self.assertEqual(params["connect_args"]["tenant"], "default")

    def test_get_extra_params_with_ssl(self):
        """Test extracting SSL parameters."""
        mock_database = MagicMock()
        mock_database.encrypted_extra = {
            "ssl": True,
            "ssl_verification": False,
        }
        mock_database.extra = {}

        with patch.object(HetuEngineSpec.__bases__[0], 'get_extra_params', return_value={}):
            params = HetuEngineSpec.get_extra_params(mock_database)

        self.assertTrue(params["connect_args"]["ssl"])
        self.assertFalse(params["connect_args"]["ssl_verification"])

    def test_build_sqlalchemy_uri_basic(self):
        """Test building basic SQLAlchemy URI."""
        parameters = {
            "username": "testuser",
            "password": "testpass",
            "host": "localhost",
            "port": 29860,
            "catalog": "hive",
            "schema": "default",
            "query": {},
        }

        uri = HetuEngineSpec.build_sqlalchemy_uri(parameters)
        expected = "hetuengine://testuser:testpass@localhost:29860/hive/default"
        self.assertEqual(uri, expected)

    def test_build_sqlalchemy_uri_with_defaults(self):
        """Test building URI with default values."""
        parameters = {
            "username": "testuser",
            "password": "testpass",
            "query": {},
        }

        uri = HetuEngineSpec.build_sqlalchemy_uri(parameters)
        self.assertIn("hetuengine://testuser:testpass@", uri)
        self.assertIn(":29860/", uri)
        self.assertIn("/hive/default", uri)

    def test_validate_parameters_success(self):
        """Test parameter validation with valid parameters."""
        parameters = {
            "host": "localhost",
            "port": 29860,
            "username": "testuser",
        }

        errors = HetuEngineSpec.validate_parameters(parameters)
        self.assertEqual(len(errors), 0)

    def test_validate_parameters_missing_required(self):
        """Test parameter validation with missing required fields."""
        parameters = {
            "host": "localhost",
        }

        errors = HetuEngineSpec.validate_parameters(parameters)
        self.assertGreater(len(errors), 0)

    def test_validate_parameters_invalid_port(self):
        """Test parameter validation with invalid port."""
        parameters = {
            "host": "localhost",
            "port": "invalid",
            "username": "testuser",
        }

        errors = HetuEngineSpec.validate_parameters(parameters)
        self.assertGreater(len(errors), 0)

    def test_extract_error_message_jdbc_driver_not_found(self):
        """Test extracting error message for JDBC driver not found."""
        ex = Exception("java.lang.ClassNotFoundException: io.trino.jdbc.TrinoDriver")
        message = HetuEngineSpec.extract_error_message(ex)

        self.assertIn("JDBC driver not found", message)
        self.assertIn("jar_path", message)

    def test_extract_error_message_jvm_not_found(self):
        """Test extracting error message for JVM not found."""
        ex = Exception("JVMNotFoundException: Java not found")
        message = HetuEngineSpec.extract_error_message(ex)

        self.assertIn("Java Virtual Machine not found", message)
        self.assertIn("JAVA_HOME", message)

    def test_extract_error_message_connection_refused(self):
        """Test extracting error message for connection refused."""
        ex = Exception("Connection refused by server")
        message = HetuEngineSpec.extract_error_message(ex)

        self.assertIn("Unable to connect", message)

    def test_extract_error_message_service_discovery(self):
        """Test extracting error message for service discovery error."""
        ex = Exception("Error 404: serviceDiscoveryMode not found")
        message = HetuEngineSpec.extract_error_message(ex)

        self.assertIn("serviceDiscoveryMode=hsbroker", message)
        self.assertIn("tenant", message)

    def test_get_default_catalog(self):
        """Test getting default catalog name."""
        mock_database = MagicMock()
        catalog = HetuEngineSpec.get_default_catalog(mock_database)
        self.assertEqual(catalog, "hive")

    def test_get_default_schema(self):
        """Test getting default schema name."""
        mock_database = MagicMock()
        schema = HetuEngineSpec.get_default_schema(mock_database)
        self.assertEqual(schema, "default")

    def test_get_schema_names_success(self):
        """Test getting schema names successfully."""
        mock_inspector = MagicMock()
        mock_inspector.get_schema_names.return_value = ["schema1", "schema2"]

        schemas = HetuEngineSpec.get_schema_names(mock_inspector)
        self.assertEqual(schemas, ["schema1", "schema2"])

    def test_get_schema_names_error(self):
        """Test getting schema names with error returns empty list."""
        mock_inspector = MagicMock()
        mock_inspector.get_schema_names.side_effect = Exception("Error")

        schemas = HetuEngineSpec.get_schema_names(mock_inspector)
        self.assertEqual(schemas, [])

    def test_get_table_names_success(self):
        """Test getting table names successfully."""
        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["table1", "table2"]
        mock_database = MagicMock()

        tables = HetuEngineSpec.get_table_names(
            mock_database, mock_inspector, "test_schema"
        )
        self.assertEqual(tables, ["table1", "table2"])

    def test_get_table_names_error(self):
        """Test getting table names with error returns empty list."""
        mock_inspector = MagicMock()
        mock_inspector.get_table_names.side_effect = Exception("Error")
        mock_database = MagicMock()

        tables = HetuEngineSpec.get_table_names(
            mock_database, mock_inspector, "test_schema"
        )
        self.assertEqual(tables, [])

    def test_get_view_names_success(self):
        """Test getting view names successfully."""
        mock_inspector = MagicMock()
        mock_inspector.get_view_names.return_value = ["view1", "view2"]
        mock_database = MagicMock()

        views = HetuEngineSpec.get_view_names(
            mock_database, mock_inspector, "test_schema"
        )
        self.assertEqual(views, ["view1", "view2"])

    def test_get_columns_success(self):
        """Test getting columns successfully."""
        mock_inspector = MagicMock()
        mock_inspector.get_columns.return_value = [
            {"name": "col1", "type": "INTEGER"},
            {"name": "col2", "type": "VARCHAR"},
        ]

        columns = HetuEngineSpec.get_columns(
            mock_inspector, "test_table", "test_schema"
        )
        self.assertEqual(len(columns), 2)
        self.assertEqual(columns[0]["name"], "col1")


if __name__ == "__main__":
    unittest.main()
