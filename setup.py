#!/usr/bin/env python
from setuptools import find_namespace_packages, setup
from pathlib import Path
import os
import re

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md"), encoding="utf8") as f:
    long_description = f.read()
 
package_version = "1.4.0"

def _get_plugin_version_dict():
    _version_path = os.path.join(this_directory, "dbt", "adapters", "odps", "__version__.py")
    _semver = r"""(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"""
    _pre = r"""((?P<prekind>a|b|rc)(?P<pre>\d+))?"""
    _version_pattern = rf"""version\s*=\s*["']{_semver}{_pre}["']"""
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f"invalid version at {_version_path}")
        return match.groupdict()
    
# require a compatible minor version (~=), prerelease if this is a prerelease
def _get_dbt_core_version():
    parts = _get_plugin_version_dict()
    minor = "{major}.{minor}.0".format(**parts)
    pre = parts["prekind"] + "1" if parts["prekind"] else ""
    return f"{minor}{pre}"

package_name = "winwin-odps-dbt"
description = """The ODPS (MaxCompute)  adapter plugin for dbt"""
dbt_core_version = _get_dbt_core_version()

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="leezhongshan",
    author_email="leezhongshan0316@gmail.com",
    url="https://github.com/ai-excelsior/F2AI",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
          f"dbt-core~={dbt_core_version}",
          f"pyodps~=0.11.4.1",
    ],
)
