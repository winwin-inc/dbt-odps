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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._credentials: ODPSCredentials = kwargs.get("credentials", None)

    def execute(self, operation, parameters=None, **kwargs):
        # retry ten times, each time wait for 15 seconds
        result = SettingParser.parse(operation)
        sql = result.remaining_query
        if parameters:
            for origin, replacement in parameters.items():
                if isinstance(replacement, six.string_types):
                    replacement = self.escape_string(replacement)

                pattern_str = ":%s([,)])?" % re.escape(to_str(origin))
                replacement_str = "%s\\1" % to_str(replacement)
                sql = re.sub(pattern_str, replacement_str, to_str(sql))

        retry_times = 3
        for i in range(retry_times):
            try:
                self._reset_state()
                odps = self._connection.odps
                self._instance = odps.run_sql(
                    sql, hints=self._hints, priority=self._credentials.priority
                )
                logger.debug(
                    f"ODPS instance logview: {self._instance.get_logview_address()}"
                )
                self._instance.wait_for_success()

                # print task summary
                task_detail = self._instance.get_task_detail()
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
