import pytest
from dbt.tests.util import (
    run_dbt,
)

seeds_base_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
5,Hannah,1982-06-23T05:41:26
6,Eleanor,1991-08-10T23:12:21
7,Lily,1971-03-29T14:58:02
8,Jonathan,1988-02-26T02:55:24
9,Adrian,1994-02-09T13:14:23
10,Nora,1976-03-01T16:51:39
""".lstrip()

models__sql = """
{{ config(
    materialized='table',
    partition_by={"fields": "name,some_date", "data_types": "string,string"}
) }}
select * from {{ source('raw', 'seed') }}

""".lstrip()

models_2__sql = """
{{ config(
    materialized='table',
    partition_by={"fields": "some_date", "data_types": "timestamp"}
) }}
select * from {{ source('raw', 'seed') }}

""".lstrip()

models_3__sql = """
{{ config(
    materialized='table',
    partition_by={"fields": "some_date", "data_types": "timestamp", "generate_column_name": "pt1", "granularity": "month"}
) }}
select * from {{ source('raw', 'seed') }}

""".lstrip()

schema_base_yml = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'base') }}"
"""


class BaseTestPartitionTable:

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models__sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
        }

    def test_base(self, project):
        # seed command
        results = run_dbt(["seed"])
        # seed result length
        print(results)

        # run command
        results = run_dbt()
        # run result length
        print(results)


class TestPartitionTable(BaseTestPartitionTable):
    pass


class BaseTestAutoPartitionTable:

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": models_2__sql,
            "model2.sql": models_3__sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
        }

    def test_base(self, project):
        # seed command
        results = run_dbt(["seed"])
        # seed result length
        print(results)

        # run command
        results = run_dbt()
        # run result length
        print(results)


class TestAutoPartitionTable(BaseTestAutoPartitionTable):
    pass
