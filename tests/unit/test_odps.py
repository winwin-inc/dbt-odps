import os

import odps
from odps import ODPS
from odps.errors import ODPSError
from odps.models import Table


class TestOdps:
    def test_get_tables(self):
        odps.options.verbose = True
        o = ODPS(
            os.getenv('ODPS_ACCESS_ID'),
            os.getenv('ODPS_ACCESS_KEY'),
            os.getenv('ODPS_PROJECT'),
            os.getenv('ODPS_ENDPOINT')
        )
        tables = o.list_tables(type='external_table')
        table: Table = next(tables)
        print(table.name)

    def test_get_project(self):
        odps.options.verbose = True
        o = ODPS(
            os.getenv('ODPS_ACCESS_ID'),
            os.getenv('ODPS_ACCESS_KEY'),
            os.getenv('ODPS_PROJECT'),
            os.getenv('ODPS_ENDPOINT')
        )
        tables = o.list_schemas('msy_customer_dev')
        try:
            print(next(tables))
        except ODPSError as e:
            if e.code == "ODPS-0110061" or str(e).endswith('is not 3-tier model project.'):
                tables = ["default"]
            else:
                raise e
        print(tables)
