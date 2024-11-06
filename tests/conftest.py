import pytest
from dbt.flags import set_from_args
from argparse import Namespace
import os

set_from_args(Namespace(), None)


pytest_plugins = ["dbt.tests.fixtures.project"]


@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    return "default"


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():

    profile_yml =  {
        'type': 'odps',
        "threads": 1,
        'access_id': os.getenv('ODPS_ACCESS_ID'),
        'secret_access_key': os.getenv('ODPS_ACCESS_KEY'),
        'database': os.getenv('ODPS_PROJECT'),
        'endpoint': os.getenv('ODPS_ENDPOINT'),
        'schema': 'default',
        'threads': 1,
        'priority': 4
    }

    return profile_yml
