import os
import re

from odps import ODPS
import yaml


class TestGetTable(object):
    def get_odps_client(self) -> ODPS:
        project = os.getenv("ODPS_PROJECT")
        schema = os.getenv("ODPS_SCHEMA")
        endpoint = os.getenv("ODPS_ENDPOINT")
        access_id = os.getenv("ODPS_ACCESS_ID")
        access_key = os.getenv("ODPS_ACCESS_KEY")

        o = ODPS(
            access_id,
            access_key,
            project=project,
            endpoint=endpoint,
        )
        o.schema = schema

        return o

    def get_relations_by_pattern(
        self,
        schema_pattern: str,
        table_pattern: str,
        exclude: str,
        database: str,
    ):
        o = self.get_odps_client()
        results = []

        # 转换模式为正则表达式
        schema_regex = self.sql_like_to_regex(schema_pattern)
        table_regex = self.sql_like_to_regex(table_pattern)
        exclude_regex = self.sql_like_to_regex(exclude)

        # 获取 schemas
        schemas = []
        for schema in o.list_schemas(database):
            print(f"regex: {schema_regex}, schema: {schema.name}")
            if re.fullmatch(schema_regex, schema.name):
                print("match")
                schemas.append(schema)

        # 获取 tables
        for schema in schemas:
            for table in o.list_tables(project=database, schema=schema.name):
                print(f"regex: {table_regex}, table: {table.name}")
                if re.fullmatch(table_regex, table.name):
                    if exclude and re.fullmatch(exclude_regex, table.name):
                        continue
                    results.append(self.get_relation(database, schema.name, table.name))
        return results

    def get_relations_by_prefix(self, schema: str, prefix: str, exclude: str, database: str):
        o = self.get_odps_client()
        exclude_regex = self.sql_like_to_regex(exclude) if exclude else None
        results = []
        for table in o.list_tables(project=database, schema=schema, prefix=prefix):
            if exclude and re.fullmatch(exclude_regex, table.name):
                continue
            results.append(self.get_relation(database, schema, table.name))
        return results

    def sql_like_to_regex(self, pattern: str) -> str:
        if not pattern:
            return "^$"
        regex = re.escape(pattern)
        regex = regex.replace("%", ".*").replace("_", ".")
        return f"^{regex}$"

    def get_relation(self, database: str, schema: str, identifier: str):
        print(f"database: {database}, schema: {schema}, identifier: {identifier}")

    def test_main(self):
        # 测试代码
        self.get_relations_by_pattern("dbt%", "schema%", "", "dingxin")
