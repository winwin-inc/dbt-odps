import os
import pytest

# import os
import yaml

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    config = {
        "type": "odps",
        "threads": 1,
        "access_id": os.getenv("ODPS_ACCESS_ID"),
        "secret_access_key": os.getenv("ODPS_ACCESS_KEY"),
        "database": os.getenv("ODPS_PROJECT"),
        "endpoint": os.getenv("ODPS_ENDPOINT"),
        "schema": os.getenv("ODPS_SCHEMA"),
        "priority": 4,
    }
    return config
