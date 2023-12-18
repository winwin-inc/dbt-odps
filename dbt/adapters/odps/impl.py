from dataclasses import dataclass
from typing import List, Optional, Dict, Iterable, Any

import agate
import dbt.exceptions
from dbt.adapters.base import AdapterConfig
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.sql import SQLAdapter
from dbt.clients import agate_helper
from dbt.contracts.relation import RelationType
from odps import ODPS
from odps.errors import ODPSError
from odps.models import Table

import dbt
from dbt.adapters.odps.utils import print_method_call, logger
from .colums import OdpsColumn
from .connections import ODPSConnectionManager, ODPSCredentials
from .relation import OdpsRelation

LIST_RELATIONS_MACRO_NAME = "list_relations_without_caching"
SHOW_CREATE_TABLE_MACRO_NAME = "show_create_table"
RENAME_RELATION_MACRO_NAME = "rename_relation"


@dataclass
class OdpsConfig(AdapterConfig):
    partitioned_by: Optional[List[Dict[str, str]]] = None
    properties: Optional[Dict[str, str]] = None


class ODPSAdapter(SQLAdapter):
    """
    Controls actual implementation of adapter, and ability to override certain methods.
    """

    ConnectionManager = ODPSConnectionManager
    Relation = OdpsRelation
    Column = OdpsColumn
    AdapterSpecificConfigs = OdpsConfig

    @property
    def odps(self) -> ODPS:
        return self.connections.get_thread_connection().handle.odps

    @property
    def credentials(self) -> ODPSCredentials:
        return self.config.credentials

    @classmethod
    def date_function(cls) -> str:
        return "CURRENT_TIMESTAMP()"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        # TODO CT-211
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))  # type: ignore[attr-defined]
        return "double" if decimals else "bigint"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "datetime"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        raise NotImplementedError("ODPS does not support a native time type. Use a timestamp instead.")

    @classmethod
    def convert_text_type(cls, agate_table, col_idx: int) -> str:
        return "string"

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        """the interval could be one of [dd, mm, yyyy, mi, ss, year, month, mon, day, hour, hh]'"""
        # return f"{add_to} + interval '{number} {interval}'"
        return f"dateadd({add_to}, {number}, '{interval}')"

    @print_method_call
    def create_schema(self, relation: BaseRelation) -> None:
        """ODPS does not support schemas, so this is a no-op"""
        try:
            self.odps.create_schema(relation.identifier, relation.database)
        except ODPSError as e:
            if e.code == "ODPS-0110061":
                return
            else:
                raise e

    def drop_schema(self, relation: OdpsRelation) -> None:
        """ODPS does not support schemas, so this is a no-op"""
        try:
            self.odps.delete_schema(relation.identifier, relation.database)
        except ODPSError as e:
            if e.code == "ODPS-0110061":
                return
            else:
                raise e

    def quote(self, identifier):
        return "`{}`".format(identifier)

    @print_method_call
    def check_schema_exists(self, database: str, schema: str) -> bool:
        """always return true, as ODPS does not have schemas."""
        return True

    @print_method_call
    def list_schemas(self, database: str) -> List[str]:
        try:
            return [schema.name for schema in self.odps.list_schemas(database)]
        except ODPSError as e:
            if e.code == "ODPS-0110061":
                return ["default"]
            else:
                raise e

    @print_method_call
    def list_relations_without_caching(
            self,
            schema_relation: OdpsRelation = None,
    ) -> List[OdpsRelation]:

        """Get a list of Relation(table or view) by SQL directly
        Use different SQL statement for view/table
        """
        kwargs = {"schema": schema_relation}
        result_views: agate.Table = self.execute_macro("odps__list_views_without_caching", kwargs=kwargs)
        views = set()
        relations = []
        for row in result_views.rows:
            relations.append(
                self.Relation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=row['table_name'],
                    type=RelationType.View,
                )
            )
            views.add(row['table_name'])
        for row in self.odps.list_tables():
            if row.name in views:
                continue
            relations.append(
                self.Relation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=row.name,
                    type=RelationType.Table,
                )
            )
        return relations

    @print_method_call
    def get_odps_table_by_relation(self, relation: OdpsRelation):
        return self.get_odps_table_if_exists(
            relation.identifier,
            project=relation.database,
        )

    @print_method_call
    def get_odps_table_if_exists(self, name, project=None) -> Optional[Table]:
        kwargs = {
            "name": name,
            "project": project,
        }

        if self.odps.exist_table(**kwargs):
            return self.odps.get_table(**kwargs)

        return None

    # override
    @print_method_call
    def get_columns_in_relation(self, relation: OdpsRelation):
        # logger.debug(f"impl.py get_columns_in_relation {relation}")

        odps_table = self.get_odps_table_by_relation(relation)
        return (
            [OdpsColumn.from_odps_column(column) for column in odps_table.table_schema.simple_columns]
            if odps_table
            else []
        )

    @print_method_call
    def get_relation(self, database: str, schema: str, identifier: str) -> Optional[BaseRelation]:
        """Get a Relation for own list"""
        if not self.Relation.get_default_quote_policy().database:
            database = None

        return super().get_relation(database, schema, identifier)

    """ 
    @print_method_call
    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        manifest: Manifest,
    ) -> agate.Table:
        # raise Exception('_get_one_catalog')
        # logger.info(f"_get_one_catalog   schemas {schemas} manifest:{manifest}")
        rows = []
        for schema in schemas:
            for table in self.odps.list_tables(project=information_schema.database, schema=schema):
                table = cast(Table, table)
                table_rows = (
                    information_schema.database,
                    schema,
                    table.name,
                    "VIEW" if table.is_virtual_view else "TABLE",
                    table.comment,
                    table.owner,
                )
                for i, column in enumerate(table.schema.get_columns()):
                    column = cast(TableSchema.TableColumn, column)
                    column_rows = table_rows + (column.name, i, column.type, column.comment)
                    rows.append(column_rows)
        table = agate.Table(
            rows,
            [
                "table_database",
                "table_schema",
                "table_name",
                "table_type",
                "table_comment",
                "table_owner",
                "column_name",
                "column_index",
                "column_type",
                "column_comment",
            ],
        )
        return table
    """

# may require more build out to make more user friendly to confer with team and community.
