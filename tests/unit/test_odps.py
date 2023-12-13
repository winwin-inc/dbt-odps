import os
import unittest

import odps
from odps import ODPS
from odps.models import Table


class TestOdps(unittest.TestCase):
    def test_get_project(self):
        odps.options.verbose = True
        o = ODPS(
            os.getenv('ODPS_ACCESS_ID'),
            os.getenv('ODPS_ACCESS_KEY'),
            os.getenv('ODPS_PROJECT'),
            os.getenv('ODPS_ENDPOINT')
        )
        tables = o.list_tables()
        table: Table = next(tables)
        print(table.name)