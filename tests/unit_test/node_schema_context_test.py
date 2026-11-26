import threading
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest import mock
from unittest.mock import MagicMock

from dbt.adapters.odps.context import get_current_node_schema, set_current_node_schema
from dbt.adapters.odps.impl import ODPSAdapter
from dbt.adapters.odps.setting_parser import SettingParser
from dbt.adapters.odps.wrapper import CursorWrapper
from dbt.adapters.sql import SQLAdapter


class TestNodeSchemaThreadLocal(unittest.TestCase):
    def setUp(self):
        set_current_node_schema(None)

    def tearDown(self):
        set_current_node_schema(None)

    def test_default_is_none(self):
        self.assertIsNone(get_current_node_schema())

    def test_set_and_get(self):
        set_current_node_schema("my_schema")
        self.assertEqual(get_current_node_schema(), "my_schema")

    def test_threads_are_isolated(self):
        set_current_node_schema("main_schema")
        seen = {}

        def worker():
            seen["value"] = get_current_node_schema()

        t = threading.Thread(target=worker)
        t.start()
        t.join()
        # dbt 每个 node 一个线程，别的线程不应看到当前线程的 schema
        self.assertIsNone(seen["value"])
        self.assertEqual(get_current_node_schema(), "main_schema")


class TestConnectionNamedSchemaContext(unittest.TestCase):
    """ODPSAdapter.connection_named 负责把当前 node 的 schema 暴露给执行层。"""

    def setUp(self):
        set_current_node_schema(None)

    def tearDown(self):
        set_current_node_schema(None)

    def _adapter(self):
        # 跳过 __init__：本用例只关心 connection_named 的上下文行为，
        # 不需要真实的 profile / manifest。
        adapter = object.__new__(ODPSAdapter)
        adapter.connections = SimpleNamespace(query_header=None)
        return adapter

    def _record_connection_lifecycle(self, calls):
        """保留真实的父类 connection_named，只拦截「取连接 / 释放连接」两步。"""

        def fake_acquire(self, name=None):
            calls.append(("acquire", name, get_current_node_schema()))
            return "fake-connection"

        def fake_release(self):
            calls.append(("release", None, get_current_node_schema()))

        stack = ExitStack()
        stack.enter_context(
            mock.patch.object(SQLAdapter, "acquire_connection", fake_acquire)
        )
        stack.enter_context(
            mock.patch.object(SQLAdapter, "release_connection", fake_release)
        )
        return stack

    def test_node_schema_set_before_connection_is_acquired(self):
        calls = []
        node = SimpleNamespace(unique_id="model.proj.my_model", schema="custom_schema")
        adapter = self._adapter()
        with self._record_connection_lifecycle(calls):
            with adapter.connection_named(node.unique_id, node):
                self.assertEqual(get_current_node_schema(), "custom_schema")
        # 父类取连接之前 schema 就已就位；退出后清空，不污染同一线程的后续 SQL
        self.assertEqual(
            calls,
            [
                ("acquire", "model.proj.my_model", "custom_schema"),
                ("release", None, "custom_schema"),
            ],
        )
        self.assertIsNone(get_current_node_schema())

    def test_non_node_context_has_no_schema(self):
        calls = []
        adapter = self._adapter()
        with self._record_connection_lifecycle(calls):
            with adapter.connection_named("master"):
                self.assertIsNone(get_current_node_schema())
        self.assertEqual(calls[0], ("acquire", "master", None))

    def test_context_without_schema_attribute(self):
        adapter = self._adapter()
        with self._record_connection_lifecycle([]):
            with adapter.connection_named("show", "show"):
                self.assertIsNone(get_current_node_schema())

    def test_schema_restored_after_exception(self):
        calls = []
        node = SimpleNamespace(unique_id="model.proj.my_model", schema="custom_schema")
        adapter = self._adapter()
        with self._record_connection_lifecycle(calls):
            with self.assertRaises(RuntimeError):
                with adapter.connection_named(node.unique_id, node):
                    raise RuntimeError("boom")
        self.assertIsNone(get_current_node_schema())
        self.assertEqual(calls[-1][0], "release")

    def test_nested_context_restores_outer_schema(self):
        outer = SimpleNamespace(unique_id="model.proj.outer", schema="outer_schema")
        inner = SimpleNamespace(unique_id="model.proj.inner", schema="inner_schema")
        adapter = self._adapter()
        with self._record_connection_lifecycle([]):
            with adapter.connection_named(outer.unique_id, outer):
                with adapter.connection_named(inner.unique_id, inner):
                    self.assertEqual(get_current_node_schema(), "inner_schema")
                self.assertEqual(get_current_node_schema(), "outer_schema")
        self.assertIsNone(get_current_node_schema())


class TestCursorWrapperSchemaInjection(unittest.TestCase):
    """CursorWrapper 必须把当前 node 的 schema 传给 SettingParser。"""

    def setUp(self):
        set_current_node_schema(None)

    def tearDown(self):
        set_current_node_schema(None)

    def _execute_and_capture_schema_arg(self):
        cursor = CursorWrapper(MagicMock())
        # 让 parse 直接抛错，只需检查它收到的参数，不必真正访问 MaxCompute
        with mock.patch.object(SettingParser, "parse") as parse:
            parse.side_effect = RuntimeError("stop here")
            with self.assertRaises(RuntimeError):
                cursor.execute("SELECT 1")
        return parse.call_args.kwargs["schema"]

    def test_execute_passes_node_schema_to_parser(self):
        set_current_node_schema("node_schema")
        self.assertEqual(self._execute_and_capture_schema_arg(), "node_schema")

    def test_execute_passes_none_outside_node_context(self):
        self.assertIsNone(self._execute_and_capture_schema_arg())


if __name__ == "__main__":
    unittest.main()
