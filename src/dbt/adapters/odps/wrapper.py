import copy
import re
import time

from dbt.adapters.events.logging import AdapterLogger
from odps.dbapi import Connection, Cursor
from odps.errors import ODPSError

from .setting_parser import SettingParser


class ConnectionWrapper(Connection):
    def cursor(self, *args, **kwargs):
        return CursorWrapper(
            self,
            *args,
            **kwargs,
        )

    def cancel(self):
        self.close()


logger = AdapterLogger("ODPS")


class CursorWrapper(Cursor):
    def execute(self, operation, parameters=None, **kwargs):
        # retry ten times, each time wait for 15 seconds
        result = SettingParser.parse(operation)
        retry_times = 10
        for i in range(retry_times):
            try:
                super().execute(result.remaining_query, hints=result.settings,async_=True)
                logger.debug(f"ODPS instance logview: {self.instance.get_logview_address()}")
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
