# schema comment 注入验证项目

验证 dbt-odps 适配器（本分支）是否把**正确的 schema** 写进了 dbt 的 query comment
——即每条 SQL 最前面那段 `/* {json} */` 注释，供 MaxCompute 血缘采集使用。

## 项目结构

```
dbt_project.yml          # custom/ 目录配置 +schema
profiles.yml             # 凭据全部走 env_var，文件本身不含密钥
models/
  default_schema_model.sql      # 默认 schema（profile 的 target schema）
  default_view_model.sql        # 默认 schema，视图
  custom/
    custom_schema_model.sql     # +schema → 自定义 schema
    with_sql_header.sql         # +schema + sql_header 里的 SET 语句
verify.py                # 运行并校验
```

| 模型 | 所在 schema（manifest） | 期望注入到 comment 的 schema |
|---|---|---|
| `default_schema_model` | `default` | `default` |
| `default_view_model` | `default` | `default` |
| `custom_schema_model` | `default_dbt_schema_comment_custom` | `default_dbt_schema_comment_custom` |
| `with_sql_header` | `default_dbt_schema_comment_custom` | `default_dbt_schema_comment_custom` |

> dbt 默认的 `generate_schema_name` 会把 `+schema` 生成为 `<target schema>_<自定义值>`，
> 所以 `dbt_schema_comment_custom` 实际落在 `default_dbt_schema_comment_custom`。

修复前，适配器注入的是**连接默认 schema**，后两个模型会被错误地标成 `default`。
修复后注入的是 **dbt node 的 schema**（`ODPSAdapter.connection_named` 从 node 取值，
经线程局部变量传到 `CursorWrapper.execute`）。

## 怎么跑

在仓库根目录执行。建议用生产同款的 dbt 运行环境（脚本自带 import 钩子，
会让它加载本仓库 `src/` 下的适配器代码和宏，**不需要改动那个环境**）：

```bash
DBT_PY=~/.local/share/uv/envs/dbt/bin/python

# 离线验证：不连 MaxCompute，应当 PASS
$DBT_PY tests/dbt_projects/schema_comment/verify.py --offline

# 对照组：绕过「按 node 注入 schema」的修复，应当 FAIL
$DBT_PY tests/dbt_projects/schema_comment/verify.py --offline --without-node-schema

# 在线验证：连真实 MaxCompute，额外核对「表实际所在 schema」
$DBT_PY tests/dbt_projects/schema_comment/verify.py
$DBT_PY tests/dbt_projects/schema_comment/verify.py --cleanup   # 顺带清理测试对象

# 原样执行任意 dbt 命令（参数透传给 dbt），例如带 --debug 能看到每个作业的 LogView
$DBT_PY tests/dbt_projects/schema_comment/dbt_local.py run --select custom_schema_model --debug
```

凭据读自仓库根目录的 `test.env`（`ODPS_ACCESS_ID` / `ODPS_ACCESS_KEY` / `ODPS_PROJECT` /
`ODPS_ENDPOINT` / `ODPS_SCHEMA`），**以 `test.env` 为准，会覆盖 shell 里已有的同名变量**。
自定义 schema 名可用 `ODPS_TEST_CUSTOM_SCHEMA` 覆盖。

> 为什么必须覆盖：shell 里可能已经导出了另一套 `ODPS_ACCESS_ID`（比如来自 profile，
> pyodps 自己也会读这个变量）。如果只补缺失的变量，就会拿 shell 的 AccessId 配
> `test.env` 的 Secret，拼成一对不匹配的凭据，MaxCompute 报 `SignatureNotMatch` ——
> 看起来很像 AccessKey 失效，其实不是。

在线模式会在 MaxCompute 上建出 `default.default_schema_model`、`default.default_view_model`
两个对象，以及 `default_dbt_schema_comment_custom` schema 下的两张表。不想要残留就加 `--cleanup`。

## 校验的是什么

脚本拦截真正交给 pyodps 的 SQL，对每个模型做三方核对：

1. **comment schema** —— 前置 JSON 注释里实际的 `schema` 值
2. **manifest schema** —— dbt 认为该模型所在的 schema
3. **MaxCompute**（仅在线） —— 表在 MaxCompute 上实际所在的 schema

另外检查：带 `sql_header` 的模型，其 `set a=b;` 是否被剥离成 hints（不应留在 SQL 正文里）。

### 离线模式

只把两层最底层的 pyodps 依赖换成假实现——`ODPSCredentials.odps()`（假的 ODPS 客户端）
和 `Cursor.execute`（只记录 SQL，不提交作业）。dbt-core 的解析、调度、多线程执行，
以及适配器的 `connection_named → CursorWrapper → SettingParser` 全链路都是真实代码，
`threads: 4` 也会真实并发。所以它验证的是「dbt 实际会发出去的 SQL」，
只是不落到 MaxCompute 上。不需要凭据，可以在任何机器上跑。

### 预期输出

```
模型                      manifest                            comment                             MaxCompute结果
--------------------------------------------------------------------------------------------------------
default_schema_model    default                             default                             跳过        OK
default_view_model      default                             default                             跳过        OK
with_sql_header         default_dbt_schema_comment_custom   default_dbt_schema_comment_custom   跳过        OK
custom_schema_model     default_dbt_schema_comment_custom   default_dbt_schema_comment_custom   跳过        OK
...
结论: 全部通过 ✅
```

对照组（`--without-node-schema`）里后两个模型会变成 `FAIL`：comment 是 `default`，
而 manifest 是 `default_dbt_schema_comment_custom`。

## 已知环境问题

- **仓库 `.venv` 跑不了这个适配器**：`uv.lock` 锁的是 pyodps 0.12.2.2，而适配器用到了
  pyodps 0.12.5+ 才有的内部属性（`Cursor.instance`、`Connection._quota_name` 等）。
  `pyproject.toml` 声明的下限是 `pyodps>=0.12.2.2`，与实际不符。请用 dbt 运行环境。
- **连 MaxCompute 报 `SignatureNotMatch`**：先怀疑凭据被 shell 里已有的 `ODPS_*`
  环境变量污染（见上面的说明），而不是 AccessKey 失效。本项目的 `load_env` 已做覆盖。

## 最近一次真实 MaxCompute 验证

2026-11-26，项目 `zhidou_hz_dev`，dbt 1.9.8 + pyodps 0.12.5.1，适配器为本仓库代码：

```
default_schema_model    default                           default                           default                           OK
default_view_model      default                           default                           default                           OK
with_sql_header         default_dbt_schema_comment_custom default_dbt_schema_comment_custom default_dbt_schema_comment_custom OK
custom_schema_model     default_dbt_schema_comment_custom default_dbt_schema_comment_custom default_dbt_schema_comment_custom OK
```

21 条 SQL，三方一致；表也确实建在了对应的 schema 里。
