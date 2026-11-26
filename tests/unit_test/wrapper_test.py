import json
import unittest
from unittest.mock import MagicMock

from dbt.adapters.odps.setting_parser import SettingParser


class TestSettingParserInjectSchema(unittest.TestCase):
    def _mock_schema(self, name: str):
        """Create a mock connection with a schema object."""
        mock_schema = MagicMock()
        mock_schema.name = name
        mock_odps = MagicMock()
        mock_odps.get_schema.return_value = mock_schema
        mock_conn = MagicMock()
        mock_conn.odps = mock_odps
        return mock_conn

    def _mock_no_schema(self):
        """Create a mock connection where get_schema returns None."""
        mock_odps = MagicMock()
        mock_odps.get_schema.return_value = None
        mock_conn = MagicMock()
        mock_conn.odps = mock_odps
        return mock_conn

    # === inject schema tests ===

    def test_inject_schema_into_simple_json(self):
        """Schema is injected into a single-line JSON comment."""
        conn = self._mock_schema("my_schema")
        op = '/* {"app": "dbt", "dbt_version": "1.9.8"} */\nSELECT 1;'
        result = SettingParser.parse(op, connection=conn)
        json_part = result.remaining_query.split("*/", 1)[0].lstrip("/*").strip()
        data = json.loads(json_part)
        self.assertEqual(data["app"], "dbt")
        self.assertEqual(data["schema"], "my_schema")

    def test_inject_schema_multiline_json(self):
        """Schema is injected into a multiline JSON comment."""
        conn = self._mock_schema("dev_schema")
        op = """/*
{
  "app": "dbt",
  "dbt_version": "1.9.8"
}
*/
SELECT 1;"""
        result = SettingParser.parse(op, connection=conn)
        start = result.remaining_query.index("/*") + 2
        end = result.remaining_query.index("*/", start)
        data = json.loads(result.remaining_query[start:end].strip())
        self.assertEqual(data["schema"], "dev_schema")

    def test_preserves_rest_of_query(self):
        """Only the leading comment is modified; the rest of the SQL is unchanged."""
        conn = self._mock_schema("test_schema")
        op = '/* {"app": "dbt"} */\nALTER VIEW my_view RENAME TO `other`;'
        result = SettingParser.parse(op, connection=conn)
        self.assertIn("ALTER VIEW my_view RENAME TO `other`;", result.remaining_query)

    def test_invalid_json_unchanged(self):
        """Leading comment that is not valid JSON → query unchanged."""
        conn = self._mock_schema("schema")
        op = "/* not valid json */\nSELECT 1;"
        result = SettingParser.parse(op, connection=conn)
        self.assertEqual(result.remaining_query, op)
        self.assertNotIn("schema", result.remaining_query)

    def test_leading_whitespace_preserved(self):
        """Whitespace before /* is preserved."""
        conn = self._mock_schema("s")
        op = '  \n  /* {"app": "dbt"} */\nSELECT 1;'
        result = SettingParser.parse(op, connection=conn)
        self.assertTrue(result.remaining_query.startswith("  \n  "))

    def test_no_connection(self):
        """Without connection → query unchanged."""
        op = '/* {"app": "dbt"} */\nSELECT 1;'
        result = SettingParser.parse(op, connection=None)
        self.assertEqual(result.remaining_query, op)
        self.assertNotIn("schema", result.remaining_query)

    def test_connection_no_schema(self):
        """Connection where get_schema returns None → JSON is still reformatted compact."""
        conn = self._mock_no_schema()
        op = '/* {"app": "dbt"} */\nSELECT 1;'
        result = SettingParser.parse(op, connection=conn)
        # json.dumps always uses compact separators, so spaces after ":" are stripped
        self.assertNotIn('"app": "dbt"', result.remaining_query)
        self.assertIn('"app":"dbt"', result.remaining_query)
        self.assertIn("SELECT 1;", result.remaining_query)

    def test_get_schema_failure_is_not_fatal(self):
        """get_schema() 失败时降级为不注入 schema，不影响 SQL 本身。"""
        mock_odps = MagicMock()
        mock_odps.get_schema.side_effect = RuntimeError("network down")
        mock_conn = MagicMock()
        mock_conn.odps = mock_odps
        op = '/* {"app": "dbt"} */\nSELECT 1;'
        result = SettingParser.parse(op, connection=mock_conn)
        self.assertNotIn("schema", result.remaining_query)
        self.assertIn("SELECT 1;", result.remaining_query)

    def test_schema_key_overwritten(self):
        """If schema key already exists in JSON, it is replaced."""
        conn = self._mock_schema("new_schema")
        op = '/* {"app": "dbt", "schema": "old_schema"} */\nSELECT 1;'
        result = SettingParser.parse(op, connection=conn)
        json_part = result.remaining_query.split("*/", 1)[0].lstrip("/*").strip()
        data = json.loads(json_part)
        self.assertEqual(data["schema"], "new_schema")

    def test_parse_result_contains_injected_comment(self):
        """注入后的 JSON 注释保留在 remaining_query 中。"""
        conn = self._mock_schema("s")
        op = '/* {"app": "dbt"} */\nSELECT 1;'
        result = SettingParser.parse(op, connection=conn)
        self.assertIn('"schema":', result.remaining_query)
        self.assertIn("SELECT 1;", result.remaining_query)

    def test_original_behavior_unchanged(self):
        """Without connection, parse() behaves exactly as before."""
        op = """
        SET a=1;
        SET b=2;
        SELECT * FROM table;
        """
        result = SettingParser.parse(op)
        self.assertEqual(result.settings, {"a": "1", "b": "2"})
        self.assertIn("SELECT * FROM table;", result.remaining_query)
        self.assertEqual(result.errors, [])

    def test_set_and_schema_together(self):
        """SET statements and schema injection work together."""
        conn = self._mock_schema("prod")
        op = '/* {"app": "dbt"} */\nSET odps.sql.allow.fullscan=true;\nSELECT 1;'
        result = SettingParser.parse(op, connection=conn)
        # schema injected
        json_part = result.remaining_query.split("*/", 1)[0].lstrip("/*").strip()
        data = json.loads(json_part)
        self.assertEqual(data["schema"], "prod")
        # SET captured
        self.assertEqual(result.settings, {"odps.sql.allow.fullscan": "true"})
        self.assertIn("SELECT 1;", result.remaining_query)


if __name__ == "__main__":
    unittest.main()
