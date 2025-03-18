#!/usr/bin/env python
import os
import re
import io
import sys
from shutil import rmtree

from setuptools import find_namespace_packages, setup,Command

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md"), encoding="utf8") as f:
    long_description = f.read()

package_version = "1.0.20a8"
here = os.path.abspath(os.path.dirname(__file__))


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


package_name = "dbt-odps-winwin"
description = """The ODPS (MaxCompute)  adapter plugin for dbt"""
dbt_core_version = _get_dbt_core_version()
 

class UploadCommand(Command):
    """Support setup.py upload."""

    description = 'Build and publish the package.'
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print('\033[1m{0}\033[0m'.format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status('Removing previous builds…')
            rmtree(os.path.join(here, 'dist'))
        except OSError:
            pass

        self.status('Building Source and Wheel (universal) distribution…')
        os.system('{0} setup.py sdist bdist_wheel --universal'.format(sys.executable))

        self.status('Uploading the package to PyPI via Twine…')
        os.system('twine upload dist/*')

        self.status('Pushing git tags…')
        os.system('git tag v{0}'.format(package_version))
        os.system('git push --tags')

        sys.exit()


setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="leezhongshan",
    author_email="leezhongshan0316@gmail.com",
    url="https://github.com/winwin-inc/dbt-odps",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        f"dbt-core~={dbt_core_version}",
        f"pyodps==0.12.1.1",
    ],
     # $ setup.py upload support.
    cmdclass={
        'upload': UploadCommand,
    },
)
