import pytest
from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt
from tests.functional.sources.fixtures import (
    models_simple_sql,
    models_simple_schema_yml,
    macros_sql
)


@pytest.mark.mutidb
class TestMutiDatabase:
    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_model_table",
            "models": {"+materialized": "table", "schema":"example"}
            #"models": {"+materialized": "table", "schema":"default"}
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models_simple.sql": models_simple_sql,
            "models_simple_schema.yml": models_simple_schema_yml 

        }
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macros.sql": macros_sql}
    
    # The actual sequence of dbt commands and assertions
    # pytest will take care of all "setup" + "teardown"
    def test_run(self, project):
        """
        Seed, then run, then test. We expect one of the tests to fail
        An alternative pattern is to use pytest "xfail" (see below)
        """
        # seed seeds
        results = run_dbt(["run", "-d"])
        assert len(results) == 1
        # validate that the results include one pass and one failure
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == [RunStatus.Success]