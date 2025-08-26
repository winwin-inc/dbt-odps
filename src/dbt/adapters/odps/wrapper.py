import copy
import re
import time

from dbt.adapters.events.logging import AdapterLogger
from odps.compat import six
from odps.dbapi import Connection, Cursor
from odps.errors import ODPSError
from odps.utils import to_str

from .credentials import ODPSCredentials
from .setting_parser import SettingParser


class ConnectionWrapper(Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._credentials = kwargs.get("credentials", None)

    def cursor(self, *args, **kwargs):
        return CursorWrapper(
            self,
            *args,
            hints=copy.deepcopy(self._hints),
            credentials=self._credentials,
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
                super().execute(result.remaining_query, hints=result.settings)
                self._instance.wait_for_success()
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
