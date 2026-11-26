#!/usr/bin/env python
"""运行本 dbt 项目，校验注入到 query comment 里的 schema。

dbt 会在每条 SQL 最前面加一段 JSON 注释（query comment），本分支的适配器会把
schema 写进去供血缘采集使用。本脚本拦截真正交给 pyodps（即发往 MaxCompute）的 SQL，
做三方核对：

  1. comment schema   —— 适配器实际注入的值
  2. manifest schema  —— dbt 认为该模型所在的 schema
  3. MaxCompute       —— 表实际所在的 schema（仅在线模式）

三者一致才算通过。预期：

  models/*.sql           （默认 schema） → 注入 profile 的 target schema
  models/custom/*.sql    （+schema）      → 注入自定义 schema，而不是 target schema
                                           （dbt 默认的 generate_schema_name 会把
                                             自定义 schema 生成为 <target>_<custom>）

用法（在仓库根目录执行）。用仓库的 .venv（dbt 1.9.4 + pyodps 0.12.2.2）或生产同款的
dbt 运行环境（dbt 1.9.8 + pyodps 0.12.5.1）都可以。脚本自带 import 钩子，
会让所用环境加载本仓库 src/ 下的适配器代码和宏，不需要改动环境本身。

    DBT_PY=~/.local/share/uv/envs/dbt/bin/python

    # 离线验证（不连 MaxCompute），应当 PASS
    $DBT_PY tests/dbt_projects/schema_comment/verify.py --offline

    # 对照组：绕过「按 node 注入 schema」的修复，应当 FAIL
    $DBT_PY tests/dbt_projects/schema_comment/verify.py --offline --without-node-schema

    # 在线验证：连真实 MaxCompute，多核对「表实际所在 schema」这一项
    $DBT_PY tests/dbt_projects/schema_comment/verify.py
    $DBT_PY tests/dbt_projects/schema_comment/verify.py --cleanup   # 顺带清理测试对象

环境变量来自仓库根目录的 test.env；自定义 schema 名可用 ODPS_TEST_CUSTOM_SCHEMA 覆盖。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

PROJECT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_DIR.parents[2]  # tests/dbt_projects/schema_comment -> 仓库根目录
TEST_ENV = REPO_ROOT / "test.env"
MANIFEST = PROJECT_DIR / "target" / "manifest.json"

# 与适配器 SettingParser._LEADING_COMMENT_RE 保持一致
COMMENT_RE = re.compile(r"^\s*/\*\s*(.*?)\s*\*/", re.DOTALL)
SET_A_EQ_B_RE = re.compile(r"\bset\s+a\s*=\s*b\s*;", re.IGNORECASE)

RULE = "=" * 104


@dataclass
class SentSql:
    """一条交给 pyodps 的 SQL。"""

    node_id: Optional[str]  # comment 里的 node_id；非 node SQL 为 None
    label: str  # 非 node SQL 的来源（connection_name）
    schema: Optional[str] = None  # comment 里注入的 schema
    comment: Optional[dict] = field(default=None, repr=False)  # 解析后的前置 JSON 注释
    raw: str = field(default="", repr=False)  # 原始 SQL（含前置注释）
    body: str = field(default="", repr=False)  # 去掉前置注释后的 SQL 正文
    set_leaked: bool = False  # sql_header 里的 SET 是否未被剥离


def use_local_adapter() -> str:
    """确保 dbt.adapters.odps / dbt.include.odps 加载的是本仓库的代码。

    这样可以直接用生产的 dbt 运行环境来验证本分支，而不必改动那个环境里
    已安装的发布版适配器——适配器代码与宏目录都会指向仓库的 src/。
    """
    import importlib.abc
    import importlib.util
    import sys

    src = str(REPO_ROOT / "src")
    prefixes = ("dbt.adapters.odps", "dbt.include.odps")

    def is_local(name: str) -> bool:
        return any(name == p or name.startswith(p + ".") for p in prefixes)

    class _LocalAdapterFinder(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if not is_local(fullname):
                return None
            rel = fullname.replace(".", "/")
            for candidate in (f"{src}/{rel}/__init__.py", f"{src}/{rel}.py"):
                if os.path.exists(candidate):
                    return importlib.util.spec_from_file_location(fullname, candidate)
            return None

    for name in [m for m in list(sys.modules) if is_local(m)]:
        del sys.modules[name]
    sys.meta_path.insert(0, _LocalAdapterFinder())

    import dbt.adapters.odps.wrapper as wrapper_mod

    return wrapper_mod.__file__


def load_env(path: Path) -> None:
    """把 test.env 的变量载入 os.environ，以 test.env 为准（覆盖已有值）。

    必须覆盖：shell 里可能已经导出了另一套 ODPS_ACCESS_ID（比如来自 profile），
    如果只补缺失的变量，就会拿 shell 的 AccessId 配 test.env 的 Secret，
    得到一对不匹配的凭据，MaxCompute 会报 SignatureNotMatch。
    """
    if not path.exists():
        sys.exit(f"找不到环境变量文件: {path}")
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ[key.strip()] = value.strip()
    # pyodps 在没有显式传 key 时会读这个变量，这里确保不会混进别的凭据
    os.environ.pop("ODPS_SECRET_ACCESS_KEY", None)


def odps_client():
    from odps import ODPS

    return ODPS(
        os.environ["ODPS_ACCESS_ID"],
        os.environ["ODPS_ACCESS_KEY"],
        os.environ["ODPS_PROJECT"],
        endpoint=os.environ["ODPS_ENDPOINT"],
        schema=os.environ.get("ODPS_SCHEMA", "default"),
    )


def target_schema() -> str:
    return os.environ.get("ODPS_SCHEMA", "default")


@contextmanager
def capture_outgoing_sql():
    """在线模式：拦截 odps.dbapi.Cursor.execute，记录真正发往 MaxCompute 的 SQL。"""
    import odps.dbapi

    captured: list[str] = []
    original = odps.dbapi.Cursor.execute

    def spy(self, operation, *args, **kwargs):
        captured.append(operation)
        return original(self, operation, *args, **kwargs)

    odps.dbapi.Cursor.execute = spy
    try:
        yield captured
    finally:
        odps.dbapi.Cursor.execute = original


@contextmanager
def fake_maxcompute():
    """离线模式：不连接 MaxCompute。

    只替换两层最底层的 pyodps 依赖：
      - ODPSCredentials.odps() → 假的 ODPS 客户端（没有表、没有 schema）
      - Cursor.execute         → 只记录 SQL，不提交作业
    dbt-core 与适配器（connection_named → CursorWrapper → SettingParser）都是真实代码。
    """
    import time as time_mod
    from unittest.mock import MagicMock

    import odps.dbapi
    from dbt.adapters.odps import impl as impl_mod
    from dbt.adapters.odps import wrapper as wrapper_mod
    from dbt.adapters.odps.credentials import ODPSCredentials
    from odps.errors import NoSuchObject

    captured: list[str] = []
    default_project = os.environ["ODPS_PROJECT"]

    def fake_get_table(name, project=None, schema=None):
        # 与 pyodps 的惰性 Table 对象一致：reload 时才报「表不存在」，
        # 适配器正是靠 NoSuchObject 判断表不存在的
        table = MagicMock(name=f"Table({name})")
        table.name = name
        table.project.name = project or default_project
        fake_schema = MagicMock(name="Schema")
        fake_schema.name = schema or target_schema()
        table.get_schema.return_value = fake_schema
        table.reload.side_effect = NoSuchObject("table not found (offline)")
        return table

    fake_odps = MagicMock(name="ODPS")
    fake_odps.list_tables.return_value = []
    fake_odps.list_schemas.return_value = []
    fake_odps.get_table.side_effect = fake_get_table
    default_schema = MagicMock(name="Schema")
    default_schema.name = target_schema()
    fake_odps.get_schema.return_value = default_schema

    instance = MagicMock(name="Instance")
    instance.id = "offline-instance"
    instance.get_logview_address.return_value = ""
    instance.get_task_detail.return_value = {}

    original_execute = odps.dbapi.Cursor.execute
    original_odps = ODPSCredentials.odps

    def fake_execute(self, operation, *args, **kwargs):
        captured.append(operation)
        # dbt-odps 通过 cursor.description 取模型字段及类型，这里返回固定的两列
        self._description = (("id", "bigint"), ("model_name", "string"))
        self._instance = instance

    # 假表永远「不存在」，适配器会按「新建表后短暂查不到」的逻辑重试并 sleep(10)。
    # 离线模式下去掉这些等待，否则整个 run 要 3 分多钟。
    no_wait = SimpleNamespace(sleep=lambda seconds: None, time=time_mod.time)
    original_time = {"impl": impl_mod.time, "wrapper": wrapper_mod.time}

    ODPSCredentials.odps = lambda self: fake_odps
    odps.dbapi.Cursor.execute = fake_execute
    impl_mod.time = no_wait
    wrapper_mod.time = no_wait
    try:
        yield captured
    finally:
        ODPSCredentials.odps = original_odps
        odps.dbapi.Cursor.execute = original_execute
        impl_mod.time = original_time["impl"]
        wrapper_mod.time = original_time["wrapper"]


@contextmanager
def without_node_schema():
    """对照组：把 connection_named 换回父类实现，即去掉「按 node 注入 schema」的修复。"""
    from dbt.adapters.odps.impl import ODPSAdapter
    from dbt.adapters.sql import SQLAdapter

    original = ODPSAdapter.connection_named
    ODPSAdapter.connection_named = SQLAdapter.connection_named
    try:
        yield
    finally:
        ODPSAdapter.connection_named = original


def run_dbt() -> bool:
    from dbt.cli.main import dbtRunner

    os.chdir(PROJECT_DIR)
    result = dbtRunner().invoke(
        ["run", "--project-dir", str(PROJECT_DIR), "--profiles-dir", str(PROJECT_DIR)]
    )
    return bool(result.success)


def parse_captured(captured: list[str]) -> list[SentSql]:
    """拆出每条 SQL 的前置 JSON 注释，得到 node_id 与注入的 schema。"""
    parsed: list[SentSql] = []
    for sql in captured:
        m = COMMENT_RE.match(sql)
        comment = None
        body = sql
        if m:
            body = sql[m.end() :]
            try:
                comment = json.loads(m.group(1))
            except json.JSONDecodeError:
                comment = None
        if comment is None:
            parsed.append(
                SentSql(node_id=None, label="<无 JSON 注释>", raw=sql, body=body)
            )
            continue
        parsed.append(
            SentSql(
                node_id=comment.get("node_id"),
                label=comment.get("connection_name", "<未知>"),
                schema=comment.get("schema"),
                comment=comment,
                raw=sql,
                body=body,
                set_leaked=bool(SET_A_EQ_B_RE.search(body)),
            )
        )
    return parsed


def manifest_models() -> dict[str, dict]:
    """本次运行生成的 manifest 里的模型节点。"""
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    return {
        unique_id: node
        for unique_id, node in manifest["nodes"].items()
        if node["resource_type"] == "model"
    }


def actual_schema(odps, node: dict) -> Optional[str]:
    """表在 MaxCompute 上实际所在的 schema；表不存在时返回 None。"""
    try:
        table = odps.get_table(node["alias"], node["database"], node["schema"])
        table.reload()
    except Exception:
        return None
    schema = table.get_schema()
    return schema.name if schema else "default"


def verify(parsed: list[SentSql], offline: bool) -> bool:
    models = manifest_models()
    odps = None if offline else odps_client()

    by_node: dict[str, list[SentSql]] = {}
    for item in parsed:
        if item.node_id:
            by_node.setdefault(item.node_id, []).append(item)

    print()
    print(RULE)
    print("核对结果（comment = 注入值，manifest = dbt 意图，MaxCompute = 表实际位置）")
    print(RULE)
    print(f"{'模型':<24}{'manifest':<36}{'comment':<36}{'MaxCompute':<10}结果")
    print("-" * 104)

    all_ok = True
    for unique_id, node in models.items():
        expected = node["schema"]
        seen = sorted({str(s.schema) for s in by_node.get(unique_id, [])})
        if offline:
            actual, actual_text = expected, "跳过"
        else:
            actual = actual_schema(odps, node)
            actual_text = actual or "<不存在>"
        ok = seen == [expected] and actual == expected
        all_ok = all_ok and ok
        print(
            f"{node['alias']:<24}{expected:<36}{','.join(seen) or '<未捕获>':<36}"
            f"{actual_text:<10}{'OK' if ok else 'FAIL'}"
        )

    leaked = [item for item in parsed if item.set_leaked]
    print("-" * 104)
    print(f"捕获 SQL 总数: {len(parsed)}，其中 sql_header 的 SET 未被剥离: {len(leaked)} 条")
    all_ok = all_ok and not leaked

    non_node = [item for item in parsed if not item.node_id]
    if non_node:
        print()
        print("非 node 上下文的 SQL（无 node schema，应回退到连接默认 schema）:")
        for label, schema in sorted({(i.label, str(i.schema)) for i in non_node}):
            print(f"  connection_name={label!r:<16} schema={schema}")

    print()
    print("每个 schema 各取一条实际发出的 SQL，其 query comment 如下:")
    shown: set[str] = set()
    for item in parsed:
        if not item.schema or not item.comment or item.schema in shown:
            continue
        shown.add(item.schema)
        print(f"  {json.dumps(item.comment, ensure_ascii=False)}")

    print(RULE)
    print("结论: " + ("全部通过 ✅" if all_ok else "存在不一致 ❌"))
    return all_ok


def existing_schemas(odps) -> set[str]:
    try:
        return {s.name for s in odps.list_schemas(os.environ["ODPS_PROJECT"])}
    except Exception:
        return set()


def cleanup(odps, preexisting_schemas: set[str]) -> None:
    """删除本次测试建的表，以及本次运行新建的 schema（不动运行前就存在的 schema）。"""
    project = os.environ["ODPS_PROJECT"]
    models = manifest_models() if MANIFEST.exists() else {}
    for node in models.values():
        try:
            odps.delete_table(
                node["alias"],
                project=node["database"],
                schema=node["schema"],
                if_exists=True,
            )
            print(f"已删除表 {node['database']}.{node['schema']}.{node['alias']}")
        except Exception as e:
            print(f"删除表 {node['alias']} 失败: {e}")
    used = {node["schema"] for node in models.values()}
    for schema in sorted(used - {target_schema()} - preexisting_schemas):
        try:
            if odps.exist_schema(schema, project):
                odps.delete_schema(schema, project)
                print(f"已删除 schema {project}.{schema}")
        except Exception as e:
            print(f"删除 schema {schema} 失败: {e}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--offline", action="store_true", help="不连 MaxCompute，只把 pyodps 换成假实现")
    parser.add_argument(
        "--without-node-schema",
        action="store_true",
        help="对照组：绕过「按 node 注入 schema」的修复，预期 FAIL",
    )
    parser.add_argument(
        "--cleanup", action="store_true", help="在线模式：校验后删除测试建的表和 schema"
    )
    args = parser.parse_args()

    load_env(TEST_ENV)

    adapter_file = use_local_adapter()
    from dbt.adapters.odps.__version__ import version

    print(f"适配器代码: {adapter_file}")
    print(f"适配器版本: {version}")
    if not adapter_file.startswith(str(REPO_ROOT / "src")):
        sys.exit("适配器代码不在本仓库 src/ 下：实际验证的不是本分支代码")
    print(f"项目: {os.environ['ODPS_PROJECT']}  target schema: {target_schema()}")
    print(f"自定义 schema 配置值: {os.environ.get('ODPS_TEST_CUSTOM_SCHEMA', 'dbt_schema_comment_custom')}")
    print(f"模式: {'离线（MaxCompute 为假实现）' if args.offline else '在线（连真实 MaxCompute）'}")

    odps = None
    preexisting_schemas: set[str] = set()
    if not args.offline:
        odps = odps_client()
        preexisting_schemas = existing_schemas(odps)
        try:
            odps.get_project().reload()
        except Exception as e:
            sys.exit(
                f"连不上 MaxCompute（{type(e).__name__}）——可先跑 --offline 验证。"
                "若是 SignatureNotMatch，先确认凭据没有被 shell 里已有的 ODPS_* "
                "环境变量污染（load_env 已做覆盖），再怀疑 AccessKey 本身"
            )

    with ExitStack() as stack:
        if args.without_node_schema:
            stack.enter_context(without_node_schema())
        captured = stack.enter_context(fake_maxcompute() if args.offline else capture_outgoing_sql())
        run_ok = run_dbt()
    print(f"\ndbt run: {'成功' if run_ok else '失败'}，捕获到 {len(captured)} 条交给 pyodps 的 SQL")

    if not MANIFEST.exists():
        print("未生成 manifest.json，无法核对")
        return 1

    verify_ok = verify(parse_captured(captured), offline=args.offline)

    if args.cleanup and odps is not None:
        print()
        cleanup(odps, preexisting_schemas)

    return 0 if (run_ok and verify_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
