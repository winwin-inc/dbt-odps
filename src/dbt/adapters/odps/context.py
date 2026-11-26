import threading
from typing import Optional

from .utils import dbt_odps_version

GLOBAL_SQL_HINTS = {
    "dbt.odps.version": dbt_odps_version(),
    "odps.sql.type.system.odps2": "true",
    "odps.sql.decimal.odps2": "true",
    "odps.sql.hive.compatible": "true",
    "odps.sql.allow.fullscan": "true",
    "odps.sql.select.output.format": "csv",
    "odps.sql.submit.mode": "script",
    "odps.sql.allow.cartesian": "true",
    "odps.sql.timezone": "Asia/Shanghai",
    "odps.sql.allow.schema.evolution": "true",
    "odps.sql.python.version": "cp37",
}


# 当前线程正在执行的 dbt node 的 schema。
# dbt 为每个 node 分配独立线程执行，线程局部变量天然就是 per-node 通道：
# 由 ODPSAdapter.connection_named 在进入 node 上下文时写入，
# 由 CursorWrapper.execute 在拼装 query comment 时读取。
_node_schema = threading.local()


def set_current_node_schema(schema: Optional[str]) -> None:
    """记录当前 node 的 schema；传 None 表示清空。"""
    _node_schema.schema = schema


def get_current_node_schema() -> Optional[str]:
    """读取当前 node 的 schema；不在 node 上下文中时返回 None。"""
    return getattr(_node_schema, "schema", None)
