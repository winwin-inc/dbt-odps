import re

from odps.compat import six
from odps.dbapi import Connection, Cursor
from odps.errors import ODPSError
from odps.utils import to_str

from dbt.adapters.odps.utils import print_method_call, logger, parse_hints, remove_comments


class ODPSCursor(Cursor):
    @print_method_call
    def execute(self, operation, parameters=None, **kwargs):
        # prepare statement
        sql = remove_comments(operation)
        if parameters:
            for origin, replacement in parameters.items():
                if isinstance(replacement, six.string_types):
                    replacement = self.escape_string(replacement)

                pattern_str = ":%s([,)])?" % re.escape(to_str(origin))
                replacement_str = "%s\\1" % to_str(replacement)
                sql = re.sub(pattern_str, replacement_str, to_str(sql))

        self._reset_state()
        odps = self._connection.odps
        run_sql = odps.run_sql
        if self._use_sqa:
            run_sql = self._run_sqa_with_fallback
        logger.debug(f"ODPSCursor.execute  sql: {sql}")
        hints = {'odps.sql.submit.mode':  'script'}

        try:
            self._instance = run_sql(sql, hints= hints )
            logger.debug(f"""instance log url: <a href="{self._instance.get_logview_address()}" /> """)
            self._instance.wait_for_success()
        except ODPSError as e:
            logger.error(f"An ODPS error occurred: {e}")
            raise e
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            raise e


class ODPSConnection(Connection):

    def cursor(self, *args, **kwargs):
        self._cursor = ODPSCursor(
            self,
            *args,
            use_sqa=self._use_sqa,
            fallback_policy=self._fallback_policy,
            hints=self._hints,
            **kwargs,
        )
        return self._cursor

    def cancel(self):
        if self._cursor is not None:
            self._cursor.cancel()
