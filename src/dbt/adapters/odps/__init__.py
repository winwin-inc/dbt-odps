# ruff: noqa: F401
import dbt.context.base
from dbt.adapters.base import AdapterPlugin
from dbt.context.base import get_context_modules
from dbt.include import odps

from . import date
from .connections import ODPSConnectionManager
from .credentials import ODPSCredentials
from .impl import ODPSAdapter

Plugin = AdapterPlugin(
    adapter=ODPSAdapter,
    credentials=ODPSCredentials,
    include_path=odps.PACKAGE_PATH,
)


def new_context_modules():
    return get_context_modules() | {"date": date}


dbt.context.base.get_context_modules = new_context_modules
