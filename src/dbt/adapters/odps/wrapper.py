import time

from dbt.adapters.events.logging import AdapterLogger

from odps.dbapi import Connection, Cursor
from odps.errors import ODPSError

from .context import get_current_node_schema
from .setting_parser import SettingParser


class ConnectionWrapper(Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # pyodps 0.12.5 之前的 Connection 不保存 quota_name，这里补齐
        if not hasattr(self, "_quota_name"):
            self._quota_name = kwargs.get("quota_name")

    def cursor(self, *args, **kwargs):
        return CursorWrapper(
            self,
            *args,
            sqa_type=self._sqa_type,
            fallback_policy=self._fallback_policy,
            # 复制一份：pyodps 0.12.5 之前的 Cursor.execute 会直接 update 传进来的
            # dict，不复制的话单条 SQL 的 hints 会泄漏到同一连接的后续 SQL
            hints=dict(self._hints) if self._hints is not None else None,
            quota_name=self._quota_name,
            **kwargs,
        )

    def cancel(self):
        self.close()


logger = AdapterLogger("ODPS")


class CursorWrapper(Cursor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # pyodps 0.12.5 之前的 Cursor 不保存 quota_name（参数被 **kwargs 吞掉），这里补齐
        if not hasattr(self, "_quota_name"):
            self._quota_name = kwargs.get("quota_name")

    @property
    def instance(self):
        """当前作业实例。

        pyodps 0.12.5+ 自带该属性，更早的版本只有私有的 _instance，
        这里兼容 pyproject 里声明的下限（pyodps>=0.12.2.2）。
        """
        return getattr(super(), "instance", self._instance)

    def execute(self, operation, parameters=None, **kwargs):
        retry_times = 10
        for i in range(retry_times):
            try:
                # 剥离 SET 语句（转成 hints），并把当前 schema 注入到 dbt 前置 JSON 注释中
                result = SettingParser.parse(
                    operation,
                    connection=self._connection,
                    schema=get_current_node_schema(),
                )
                super().execute(
                    result.remaining_query, hints=result.settings, async_=True
                )

                logger.debug(
                    f"ODPS Instance ID: {self.instance.id},Log view: {self.instance.get_logview_address()}"
                )

                self.instance.wait_for_success()
                # print task summary
                task_detail = self.instance.get_task_detail()
                task_summary = task_detail.get("Instance", {}).get("Summary", "")
                if task_summary:
                    logger.debug(task_summary)
                return
            except ODPSError as e:
                # 0130201: view not found, 0110061, 0130131: table not found
                if (
                    e.code == "ODPS-0130201"
                    or e.code == "ODPS-0130211"  # Table or view already exists
                    or e.code == "ODPS-0110061"
                    or e.code == "ODPS-0130131"
                    or e.code == "ODPS-0420111"
                ):
                    if i == retry_times - 1:
                        raise e
                    logger.warning(f"Retry because of {e}, retry times {i + 1}")
                    time.sleep(15)
                    continue
                else:
                    o = self.connection.odps
                    if e.instance_id:
                        instance = o.get_instance(e.instance_id)
                        logger.error(instance.get_logview_address())
                    raise e
