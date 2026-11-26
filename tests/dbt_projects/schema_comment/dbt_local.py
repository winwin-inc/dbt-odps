#!/usr/bin/env python
"""用本仓库的适配器代码 + test.env 的凭据，运行真实的 dbt 命令（参数原样透传给 dbt）。

    ~/.local/share/uv/envs/dbt/bin/python tests/dbt_projects/schema_comment/dbt_local.py <dbt 子命令及参数>

例如：

    ... dbt_local.py ls
    ... dbt_local.py run
    ... dbt_local.py run --select custom_schema_model
    ... dbt_local.py docs generate

与 verify.py 的区别：verify.py 跑完后会拦截发出的 SQL 并核对 schema；
dbt_local.py 只是原样执行 dbt 命令，方便用别的子命令查看结果。
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import verify  # noqa: E402


def main() -> int:
    verify.load_env(verify.TEST_ENV)
    adapter_file = verify.use_local_adapter()
    print(f"[dbt_local] 适配器代码: {adapter_file}", file=sys.stderr)
    print(f"[dbt_local] MaxCompute 项目: {verify.os.environ['ODPS_PROJECT']}", file=sys.stderr)

    from dbt.cli.main import cli

    return cli(prog_name="dbt")


if __name__ == "__main__":
    sys.exit(main())
