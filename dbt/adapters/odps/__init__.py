import dbt.context.base

from dbt.adapters.odps.connections import ODPSConnectionManager  # noqa
from dbt.adapters.odps.connections import ODPSCredentials
from dbt.adapters.odps.impl import ODPSAdapter
from dbt.include import odps

from dbt.adapters.base import AdapterPlugin

from dbt.context.base import get_context_modules
from . import date


Plugin = AdapterPlugin(
    adapter=ODPSAdapter,
    credentials=ODPSCredentials,
    include_path=odps.PACKAGE_PATH,
)


def new_context_modules():
    return get_context_modules() | {
        "date": date
    }


dbt.context.base.get_context_modules = new_context_modules
