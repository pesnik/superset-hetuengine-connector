"""Regression tests for Superset 6.x compatibility (connector 0.1.14).

They run against whichever Superset is installed: 5.x branches are skipped where
the 6.x API does not exist, so the same suite guards both major versions.
"""

import importlib.util
import unittest
from unittest.mock import MagicMock, patch

from superset_hetuengine.db_engine_spec import HetuEngineSpec, Table

SUPERSET_6 = importlib.util.find_spec("superset.sql.parse") is not None


class TestSuperset6Compat(unittest.TestCase):
    def test_table_import_resolves(self):
        """superset.sql_parse was removed in 6.0; Table must still import."""
        t = Table("t", "s", "c")
        self.assertEqual((t.table, t.schema, t.catalog), ("t", "s", "c"))

    def test_get_extra_params_accepts_source(self):
        """6.0 calls get_extra_params(database, source)."""
        db = MagicMock(encrypted_extra="{}", extra="{}")
        base = HetuEngineSpec.__bases__[0]
        with patch.object(base, "get_extra_params", return_value={}) as m:
            HetuEngineSpec.get_extra_params(db, source="sql_lab")
            m.assert_called_once_with(db, "sql_lab")
        with patch.object(base, "get_extra_params", return_value={}) as m:
            HetuEngineSpec.get_extra_params(db)  # 5.x call style
            m.assert_called_once_with(db)

    def test_extract_error_message_java_exception(self):
        """JDBC errors carry a java.sql.SQLException, not a Presto error dict."""

        class FakeJavaSQLException:
            def __str__(self):
                return "java.sql.SQLException: Query failed: line 1:8: Unknown type: TEXT"

        class DatabaseError(Exception):
            pass

        msg = HetuEngineSpec._extract_error_message(DatabaseError(FakeJavaSQLException()))
        self.assertIn("Unknown type: TEXT", msg)

    def test_extract_error_message_presto_dict_still_supported(self):
        class DatabaseError(Exception):
            pass

        msg = HetuEngineSpec._extract_error_message(DatabaseError({"message": "boom"}))
        self.assertIn("boom", msg)

    @unittest.skipUnless(SUPERSET_6, "sqlglot dialect mapping exists only in Superset >= 6.0")
    def test_sqlglot_dialect_is_trino(self):
        from sqlglot.dialects.dialect import Dialects
        from superset.sql.parse import SQLGLOT_DIALECTS

        self.assertEqual(SQLGLOT_DIALECTS.get("hetuengine"), Dialects.TRINO)

    @unittest.skipUnless(SUPERSET_6, "SQLScript exists only in Superset >= 6.0")
    def test_varchar_not_rewritten_as_text(self):
        """Without the mapping, the generic dialect turns VARCHAR into TEXT."""
        from superset.sql.parse import SQLScript

        out = SQLScript("SELECT CAST(x AS VARCHAR) AS y FROM t", "hetuengine").format()
        self.assertIn("VARCHAR", out.upper())
        self.assertNotIn(" TEXT", out.upper())


if __name__ == "__main__":
    unittest.main()
