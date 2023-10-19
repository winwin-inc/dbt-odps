from odps.dbapi import Connection, Cursor
from dbt.adapters.odps.utils import print_method_call,logger


class ODPSCursor(Cursor):
    @print_method_call
    def execute(self, operation, parameters=None, **kwargs):
        for k in ["async", "async_"]:
            if k in kwargs:
                async_ = kwargs[k]
                break
        else:
            async_ = False

        # format parameters
        if parameters is None:
            sql = operation
        else:
            raise NotImplementedError("Parameters are not supported yet")

        self._reset_state()
        odps = self._connection.odps
        run_sql = odps.execute_sql
        if self._use_sqa:
            run_sql = self._run_sqa_with_fallback
        if async_:
            run_sql = odps.run_sql

        self._instance = run_sql(sql, hints=self._hints)


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
