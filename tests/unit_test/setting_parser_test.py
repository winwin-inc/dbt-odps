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


if __name__ == "__main__":
    unittest.main()
