import pytest
from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt


@pytest.mark.model
class TestModelTable:
    """
    Methods in this class will be of two types:
    1. Fixtures defining the dbt "project" for this test case.
       These are scoped to the class, and reused for all tests in the class.
    2. Actual tests, whose names begin with 'test_'.
       These define sequences of dbt commands and 'assert' statements.
    """

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_model_table",
            "models": {"+materialized": "table"}
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tmp_dbt_test_model.sql": "SELECT getdate() as biz_date",
        }

    # The actual sequence of dbt commands and assertions
    # pytest will take care of all "setup" + "teardown"
    def test_run(self, project):
        """
        Seed, then run, then test. We expect one of the tests to fail
        An alternative pattern is to use pytest "xfail" (see below)
        """
        # seed seeds
        results = run_dbt(["run"])
        assert len(results) == 1
        # validate that the results include one pass and one failure
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == [RunStatus.Success]


class TestModelTableContractEnforced:
    """
    Methods in this class will be of two types:
    1. Fixtures defining the dbt "project" for this test case.
       These are scoped to the class, and reused for all tests in the class.
    2. Actual tests, whose names begin with 'test_'.
       These define sequences of dbt commands and 'assert' statements.
    """

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_model_table",
            "models": {"+materialized": "table"}
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tmp_dbt_test_model.sql": "SELECT getdate() as biz_date",
            "schema.yml": '''
version: 2
models:
  - name: tmp_dbt_test_model
    config:
      contract:
        enforced: true
    columns:
      - name: biz_date
        data_type: datetime  
'''
        }

    # The actual sequence of dbt commands and assertions
    # pytest will take care of all "setup" + "teardown"
    def test_run(self, project):
        """
        Seed, then run, then test. We expect one of the tests to fail
        An alternative pattern is to use pytest "xfail" (see below)
        """
        # seed seeds
        results = run_dbt(["run"])
        assert len(results) == 1
        # validate that the results include one pass and one failure
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == [RunStatus.Success]


class TestIncrementalOverwriteInsert:
    """
    Methods in this class will be of two types:
    1. Fixtures defining the dbt "project" for this test case.
       These are scoped to the class, and reused for all tests in the class.
    2. Actual tests, whose names begin with 'test_'.
       These define sequences of dbt commands and 'assert' statements.
    """

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_model_table",
            "models": {"+materialized": "table"}
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tmp_dbt_test_model.sql": "SELECT getdate() as biz_date, '20231101' ds",
            "schema.yml": '''
version: 2
models:
  - name: tmp_dbt_test_model
    config:
      materialized: incremental
      incremental_strategy: insert_overwrite
      on_schema_change: fail
      partition_by:
        - field: ds
          data_type: string
          comment: the date partition  
      contract:
        enforced: true
    columns:
      - name: biz_date
        data_type: datetime
'''
        }

    # The actual sequence of dbt commands and assertions
    # pytest will take care of all "setup" + "teardown"
    def test_run(self, project):
        """
        Seed, then run, then test. We expect one of the tests to fail
        An alternative pattern is to use pytest "xfail" (see below)
        """
        # seed seeds
        results = run_dbt(["run"])
        assert len(results) == 1
        # validate that the results include one pass and one failure
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == [RunStatus.Success]

class TestIncrementalOverwriteInsertNotContractEnforced:
    """
    Methods in this class will be of two types:
    1. Fixtures defining the dbt "project" for this test case.
       These are scoped to the class, and reused for all tests in the class.
    2. Actual tests, whose names begin with 'test_'.
       These define sequences of dbt commands and 'assert' statements.
    """

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_model_table",
            "models": {"+materialized": "table"}
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tmp_dbt_test_model.sql": "SELECT getdate() as biz_date, '20231101' ds",
            "schema.yml": '''
version: 2
models:
  - name: tmp_dbt_test_model
    config:
      materialized: incremental
      incremental_strategy: insert_overwrite
      on_schema_change: fail
      partition_by:
        - field: ds
          data_type: string
          comment: the date partition  
      pre-hook: drop table if exists tmp_dbt_test_model
'''
        }

    # The actual sequence of dbt commands and assertions
    # pytest will take care of all "setup" + "teardown"
    def test_run(self, project):
        """
        Seed, then run, then test. We expect one of the tests to fail
        An alternative pattern is to use pytest "xfail" (see below)
        """
        # seed seeds
        results = run_dbt(["run"])
        assert len(results) == 1
        # validate that the results include one pass and one failure
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == [RunStatus.Success]


class TestIncrementalOverwriteInsertNotContractEnforcedExists:
    """
    Methods in this class will be of two types:
    1. Fixtures defining the dbt "project" for this test case.
       These are scoped to the class, and reused for all tests in the class.
    2. Actual tests, whose names begin with 'test_'.
       These define sequences of dbt commands and 'assert' statements.
    """

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_model_table",
            "models": {"+materialized": "table"}
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tmp_dbt_test_model.sql": "SELECT getdate() as biz_date, '20231101' ds",
            "schema.yml": '''
version: 2
models:
  - name: tmp_dbt_test_model
    config:
      materialized: incremental
      incremental_strategy: insert_overwrite
      on_schema_change: fail
      partition_by:
        - field: ds
          data_type: string
          comment: the date partition  
'''
        }

    # The actual sequence of dbt commands and assertions
    # pytest will take care of all "setup" + "teardown"
    def test_run(self, project):
        """
        Seed, then run, then test. We expect one of the tests to fail
        An alternative pattern is to use pytest "xfail" (see below)
        """
        # seed seeds
        results = run_dbt(["run"])
        assert len(results) == 1
        # validate that the results include one pass and one failure
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == [RunStatus.Success]
