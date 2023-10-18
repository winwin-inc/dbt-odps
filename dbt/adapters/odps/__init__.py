from dbt.adapters.odps.connections import ODPSConnectionManager  # noqa
from dbt.adapters.odps.connections import ODPSCredentials
from dbt.adapters.odps.impl import ODPSAdapter
from dbt.include import odps

from dbt.adapters.base import AdapterPlugin


Plugin = AdapterPlugin(
    adapter=ODPSAdapter,
    credentials=ODPSCredentials,
    include_path=odps.PACKAGE_PATH,
)
