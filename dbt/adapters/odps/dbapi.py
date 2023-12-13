import re

from odps.compat import six
from odps.dbapi import Connection, Cursor
from odps.errors import ODPSError
from odps.utils import to_str

from dbt.adapters.odps.utils import print_method_call, logger, parse_hints, remove_comments


class ODPSCursor(Cursor):
    @print_method_call
    def execute(self, operation, parameters=None, **kwargs):
        for k in ["async", "async_"]:
            if k in kwargs:
                async_ = kwargs[k]
                break
        else:
            async_ = False

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
        run_sql = odps.execute_sql
        if self._use_sqa:
            run_sql = self._run_sqa_with_fallback
        if async_:
            run_sql = odps.run_sql
        hints, sql = parse_hints(sql)
        logger.error(f"ODPSCursor.execute {sql}")
        try:
            self._instance = run_sql(sql, hints=hints | (self._hints or {}))
        except ODPSError as e:
            logger.error(f"An ODPS error occurred: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")


class ODPSConnection(Connection):
    def cursor(self, *args, **kwargs):
        return ODPSCursor(
            self,
            *args,
            use_sqa=self._use_sqa,
            fallback_policy=self._fallback_policy,
            hints=self._hints,
            **kwargs,
        )
