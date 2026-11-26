import unittest
from unittest.mock import MagicMock

from dbt.adapters.odps.wrapper import ConnectionWrapper, CursorWrapper


class TestPyodpsCompatibility(unittest.TestCase):
    """适配器依赖的 pyodps 内部属性，在 0.12.5 之前并不存在。

    pyproject 声明的下限是 pyodps>=0.12.2.2，所以这些用例在两个版本下都应通过：
      - Connection._quota_name：0.12.5+ 才有
      - Cursor.instance：0.12.5+ 才有（更早只有私有的 _instance）
    """

    def _connection(self, hints=None):
        return ConnectionWrapper(odps=MagicMock(), hints=hints or {})

    def test_connection_always_has_quota_name(self):
        conn = self._connection()
        self.assertIsNone(conn._quota_name)

    def test_connection_keeps_given_quota_name(self):
        conn = ConnectionWrapper(odps=MagicMock(), hints={}, quota_name="quota_x")
        self.assertEqual(conn._quota_name, "quota_x")

    def test_cursor_has_quota_name_attribute(self):
        conn = self._connection()
        cursor = conn.cursor()
        self.assertIsInstance(cursor, CursorWrapper)
        self.assertIsNone(cursor._quota_name)

    def test_cursor_instance_returns_the_running_instance(self):
        conn = self._connection()
        cursor = conn.cursor()
        sentinel = object()
        cursor._instance = sentinel
        self.assertIs(cursor.instance, sentinel)

    def test_cursor_hints_are_copied_from_connection(self):
        # 旧版 pyodps 的 Cursor.execute 会直接 update 传进来的 dict，
        # 不复制的话单条 SQL 的 hints 会泄漏到同一连接的后续 SQL
        conn = self._connection(hints={"a": "1"})
        cursor = conn.cursor()
        self.assertEqual(cursor._hints, {"a": "1"})
        self.assertIsNot(cursor._hints, conn._hints)

    def test_two_cursors_do_not_share_hints(self):
        conn = self._connection(hints={"a": "1"})
        first, second = conn.cursor(), conn.cursor()
        self.assertIsNot(first._hints, second._hints)
        first._hints["b"] = "2"
        self.assertEqual(second._hints, {"a": "1"})
        self.assertEqual(conn._hints, {"a": "1"})


if __name__ == "__main__":
    unittest.main()
