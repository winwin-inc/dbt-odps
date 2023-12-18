import os
import pytest

from dbt.config import PartialProject

from dbt.adapters.odps import ODPSAdapter
from dbt.adapters.odps.impl import OdpsConfig


class Obj:
    which = "blah"
    profile = "default"
    project = "X"
    target = "odps"
    single_threaded = False


def profile_from_dict(profile, profile_name, cli_vars="{}"):
    from dbt.config import Profile
    from dbt.config.renderer import ProfileRenderer
    from dbt.config.utils import parse_cli_vars

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = ProfileRenderer(cli_vars)

    # in order to call dbt's internal profile rendering, we need to set the
    # flags global. This is a bit of a hack, but it's the best way to do it.
    from dbt.flags import set_from_args
    from argparse import Namespace

    set_from_args(Namespace(), None)
    return Profile.from_raw_profile_info(
        profile,
        profile_name,
        renderer,
    )


def project_from_dict(project, profile, packages=None, selectors=None, cli_vars="{}"):
    from dbt.config.renderer import DbtProjectYamlRenderer
    from dbt.config.utils import parse_cli_vars

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = DbtProjectYamlRenderer(profile, cli_vars)

    project_root = project.pop("project-root", os.getcwd())

    partial = PartialProject.from_dicts(
        project_root=project_root,
        project_dict=project,
        packages_dict=packages,
        selectors_dict=selectors,
    )
    return partial.render(renderer)


def config_from_dicts(project, profile, packages=None, selectors=None, cli_vars="{}"):
    from dbt.config import Project, Profile, RuntimeConfig
    from copy import deepcopy
    from dbt.config.utils import parse_cli_vars

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    if isinstance(project, Project):
        profile_name = project.profile_name
    else:
        profile_name = project.get("profile")

    if not isinstance(profile, Profile):
        profile = profile_from_dict(
            deepcopy(profile),
            profile_name,
            cli_vars,
        )

    if not isinstance(project, Project):
        project = project_from_dict(
            deepcopy(project),
            profile,
            packages,
            selectors,
            cli_vars,
        )

    args = Obj()
    args.vars = cli_vars
    args.profile_dir = "/dev/null"
    return RuntimeConfig.from_parts(project=project, profile=profile, args=args)


def get_adapter():
    profile = {
        "outputs": {
            "odps": {
                'type': 'odps',
                "threads": 1,
                'access_id': os.getenv('ODPS_ACCESS_ID'),
                'secret_access_key': os.getenv('ODPS_ACCESS_KEY'),
                'database': os.getenv('ODPS_PROJECT'),
                'endpoint': os.getenv('ODPS_ENDPOINT'),
                'schema': 'default',
            }
        },
        "target": "odps"
    }
    project = {
        "name": "X",
        "version": "0.1",
        "project-root": "/tmp/dbt/does-not-exist",
        "profile": "default",
        "config-version": 2,
    }
    return ODPSAdapter(config_from_dicts(project, profile))


class TestOdpsAdapter:

    def test_quote(self):
        assert  "`test`" ==  get_adapter().quote("test")

    def test_relation(self):
        adapter = get_adapter()
        r = adapter.get_relation("zhidou_test", "default", 'tmp_dbt_test_model')
        print(r)
