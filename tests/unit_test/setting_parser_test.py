import unittest

from dbt.adapters.odps.setting_parser import SettingParser


class TestSettingParser(unittest.TestCase):
    def test_basic_parsing(self):
        query = """
        SET a=1;
        SET b=2;
        SELECT * FROM table;
        """
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"a": "1", "b": "2"})
        self.assertIn("SELECT * FROM table;", result.remaining_query)
        self.assertEqual(result.errors, [])

    def test_escaped_semicolon(self):
        query = "SET c=value\\;withsemicolon;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings["c"], "value;withsemicolon")

    def test_missing_semicolon(self):
        query = "SET d=3"
        result = SettingParser.parse(query)
        self.assertIn("missing semicolon", result.errors[0])
        self.assertNotIn("d", result.settings)
        self.assertEqual(result.remaining_query.strip(), query.strip())

    def test_invalid_key_value_pairs(self):
        query = "SET e; SET f=4; SET =5; SET 'g'=6;"
        result = SettingParser.parse(query)
        self.assertEqual(len(result.errors), 2)
        self.assertIn("missing '='", result.errors[0])
        self.assertIn("empty key", result.errors[1])
        self.assertEqual(result.settings, {"'g'": "6", "f": "4"})

    def test_comments_handling(self):
        query = """
        -- SET commented=1;
        /* SET commented=2; */
        SET valid=3;
        """
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"valid": "3"})
        self.assertNotIn("commented", result.settings)

    def test_mixed_content(self):
        query = """
        -- Comment
        SET a=1; /* Another comment */
        SET b=2;
        Invalid statement;
        SET c=3;
        """
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"a": "1", "b": "2"})

    def test_whitespace_handling(self):
        query = "   SET   d  =  value  ;   "
        result = SettingParser.parse(query)
        self.assertEqual(result.settings["d"], "value")

    def test_multiple_errors(self):
        query = "SET =missing; SET k=; SET l=4;"
        result = SettingParser.parse(query)
        self.assertEqual(len(result.errors), 1)
        self.assertIn("empty key", result.errors[0])
        self.assertEqual(result.settings, {"k": "", "l": "4"})

    def test_empty_value(self):
        query = "SET empty=;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings["empty"], "")

    def test_case_insensitivity(self):
        query = "sEt CaSeInSeNsItIvE=VALUE;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"CaSeInSeNsItIvE": "VALUE"})

    def test_multiple_statements_per_line(self):
        query = "SET a=1;SET b=2;SET c=3;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"a": "1", "b": "2", "c": "3"})

    # === Edge cases: string literals containing 'set' keyword ===
    # NOTE: SettingParser is a simple character-by-character state machine that does NOT
    # handle SQL string literals. Content inside quotes is parsed as-is, so '--' inside
    # a string starts a comment and 'set' inside a string stops parsing.
    # 该脆弱点对 SettingParser 的修改很关键：改动前请先补充边界用例测试。

    def test_string_literal_with_set_keyword(self):
        """Parser does not understand SQL string literals; '--' inside strings starts a comment."""
        query = "SELECT 'set key=value' AS col; SET a=1; SELECT 1;"
        result = SettingParser.parse(query)
        # Parser sees '--' inside string and starts a comment, stopping SET parsing
        self.assertNotIn("a", result.settings)

    def test_double_quoted_string_with_set(self):
        """Parser does not distinguish single/double quotes; content is parsed literally."""
        query = 'SELECT "set a=1" FROM t; SET b=2;'
        result = SettingParser.parse(query)
        # Parser sees 'set' inside string and stops at that point
        self.assertNotIn("b", result.settings)

    def test_string_with_semicolon_and_set(self):
        """Semicolon inside a string literal is NOT treated as statement separator."""
        query = "SELECT 'value; -- set'; SET a=1;"
        result = SettingParser.parse(query)
        # '--' in string starts a single-line comment, stops parsing
        self.assertNotIn("a", result.settings)

    # === Edge cases: nested / malformed comments ===

    def test_multiline_comment_containing_set(self):
        """SET inside a multi-line comment should be ignored."""
        query = "/* SET ignored=1; */ SET valid=2;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"valid": "2"})
        self.assertNotIn("ignored", result.settings)

    def test_single_line_comment_with_semicolon(self):
        """Semicolon inside a single-line comment should not end SET parsing."""
        query = "-- SET ignored=1;\nSET a=2;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"a": "2"})
        self.assertNotIn("ignored", result.settings)

    def test_comment_directly_before_set(self):
        """Comment directly before SET should not affect parsing."""
        query = "/* comment */SET a=1;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"a": "1"})

    def test_double_dash_in_string(self):
        """Parser does not handle SQL strings; '--' inside string is a comment start."""
        query = "SELECT '--' AS d; SET a=1;"
        result = SettingParser.parse(query)
        # '--' inside string starts a single-line comment, stopping parsing
        self.assertNotIn("a", result.settings)

    def test_multiline_comment_at_end_without_closing(self):
        """Unclosed multi-line comment stops parsing; content remains in remaining_query."""
        query = "SET a=1; /* unclosed comment"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"a": "1"})
        # Unclosed comment is left in remaining_query (parser stops at unclosed comment)
        self.assertIn("unclosed", result.remaining_query)

    # === Edge cases: whitespace and formatting ===

    def test_set_at_end_of_query(self):
        """SET without semicolon at the end is never reached if SELECT precedes it."""
        query = "SELECT 1; SET a=1"
        result = SettingParser.parse(query)
        # Parser stops at SELECT before reaching SET, so no settings and no errors
        self.assertEqual(result.settings, {})
        self.assertEqual(result.errors, [])

    def test_only_whitespace_and_set(self):
        query = "   \n  SET   key   =   value   ;   \n  "
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"key": "value"})

    def test_tab_and_newline_whitespace(self):
        query = "SET\ta=\tb;\nSET c=d;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"a": "b", "c": "d"})

    def test_empty_query(self):
        query = ""
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {})
        self.assertEqual(result.remaining_query, "")

    def test_only_comments(self):
        """With no SET statements, remaining_query preserves all comment text."""
        query = "-- comment\n/* block comment */\n-- another"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {})
        # Parser never enters STOP state, so comments remain in remaining_query
        self.assertIn("-- comment", result.remaining_query)

    def test_set_with_no_space_around_equals(self):
        query = "SET a=1;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"a": "1"})

    # === Edge cases: remaining_query format ===

    def test_remaining_query_preserves_order(self):
        """Parser stops at first non-set content, so only leading SETs are captured."""
        query = "SET a=1; SELECT 1; SET b=2; SELECT 2;"
        result = SettingParser.parse(query)
        # Parser sees SELECT after first SET and stops; only first SET is captured
        self.assertEqual(result.settings, {"a": "1"})
        self.assertIn("SELECT 1;", result.remaining_query)

    def test_remaining_query_strips_set_statements(self):
        query = "SET a=1;\nSET b=2;\nSELECT * FROM t;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"a": "1", "b": "2"})
        self.assertEqual(result.remaining_query.strip(), "SELECT * FROM t;")

    def test_remaining_query_preserves_leading_newlines(self):
        query = "\n\nSET a=1;\n\nSELECT 1;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"a": "1"})
        self.assertTrue(result.remaining_query.startswith("\n\n"))
        self.assertIn("SELECT 1;", result.remaining_query)

    # === Edge cases: ODPS-specific hints ===

    def test_odps_hints_format(self):
        """ODPS uses hints like odps.sql.*, odps.select.*, etc."""
        query = "SET odps.sql.allow.fullscan=true; SET odps.sql.timezone=Asia/Shanghai; SELECT 1;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {
            "odps.sql.allow.fullscan": "true",
            "odps.sql.timezone": "Asia/Shanghai",
        })

    def test_hint_value_with_colon(self):
        query = "SET odps.sql.timezone=Asia/Shanghai; SELECT 1;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings["odps.sql.timezone"], "Asia/Shanghai")

    def test_hint_value_with_special_chars(self):
        query = "SET key=value_with_underscore; SET key2=a-b-c; SET key3=123.456;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {
            "key": "value_with_underscore",
            "key2": "a-b-c",
            "key3": "123.456",
        })

    def test_escaped_backslash(self):
        """Backslash escape: \\; remains as \\; in stored value."""
        query = r"SET k=val\\;ue;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings["k"], r"val\;ue")

    def test_multiple_escaped_semicolons(self):
        query = r"SET k=a\;b\;c;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings["k"], r"a;b;c")

    # === Edge cases: SET keyword variations ===

    def test_set_keyword_in_cte(self):
        """SET appearing after CTE content (non-set) stops parsing."""
        query = "WITH cte AS (SELECT 1 AS col) SET a=1; SELECT * FROM cte;"
        result = SettingParser.parse(query)
        # Parser sees non-whitespace non-set content, stops
        self.assertEqual(result.settings, {})

    def test_set_after_select(self):
        """SET appearing after a SELECT statement is not parsed."""
        query = "SELECT 1; SET a=1;"
        result = SettingParser.parse(query)
        # Parser stops at first non-set character
        self.assertEqual(result.settings, {})

    def test_real_world_query(self):
        """A realistic ODPS query with settings, hints, and comments."""
        query = """
        -- enable full scan
        SET odps.sql.allow.fullscan=true;
        SET odps.sql.timezone=Asia/Shanghai;
        /* this is a multi-line
           comment */
        -- now the actual query
        SELECT
            t.id,
            t.name
        FROM my_table t
        WHERE t.status = 'active';
        """
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {
            "odps.sql.allow.fullscan": "true",
            "odps.sql.timezone": "Asia/Shanghai",
        })
        self.assertIn("SELECT", result.remaining_query)
        self.assertIn("FROM my_table t", result.remaining_query)
        # NOTE: Multi-line comments remain in remaining_query (parser consumes but doesn't strip)
        self.assertIn("comment", result.remaining_query)

    def test_multiple_empty_lines_between_statements(self):
        query = "SET a=1;\n\n\n\nSET b=2;\n\nSELECT 1;"
        result = SettingParser.parse(query)
        self.assertEqual(result.settings, {"a": "1", "b": "2"})


if __name__ == "__main__":
    unittest.main()
