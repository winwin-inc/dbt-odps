import time
import functools

from odps.errors import ODPSError, NoSuchObject

from pathlib import Path

# used for this adapter's version and in determining the compatible dbt-core version
VERSION = Path(__file__).parent / "__version__.py"


def dbt_odps_version() -> str:
    """
    Pull the package version from the main package version file
    """
    attributes: dict[str, str] = {}
    exec(VERSION.read_text(), attributes)
    return attributes["version"]


def quote_string(value: str) -> str:
    value = value.replace("'", "\\'")
    return f"'{value}'"


def quote_ref(value: str) -> str:
    value = value.replace("`", "``")
    return f"`{value}`"


def is_schema_not_found(e: ODPSError) -> bool:
    if isinstance(e, NoSuchObject):
        return True
    if "ODPS-0110061" in str(e):
        return True
    if "ODPS-0422155" in str(e):
        return True
    if "ODPS-0420111" in str(e):
        return True
    return False


