import os
import pickle
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Dict, Iterable, Any

import agate
import dbt.exceptions
import odps
from dbt.adapters.base import AdapterConfig
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.sql import SQLAdapter
# from dbt.contracts.relation import RelationType
from dbt.adapters.contracts.relation import RelationType
from odps import ODPS
from odps.errors import ODPSError, NoSuchObject
from odps.models import Table
from packaging import version

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
            database = database.strip('`')
            return [schema.name for schema in self.odps.list_schemas(database)]
        except ODPSError as e:
            if e.code == "ODPS-0110061" or str(e).endswith('is not 3-tier model project.'):
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
        cache_enabled = os.getenv('ODPS_RELATION_CACHE_ENABLE', 'false') == 'true'
        if cache_enabled:
            cache_file = Path(tempfile.gettempdir()) / f"odps_relation_{schema_relation.without_quote()}"
            if cache_file.exists() and time.time() - cache_file.stat().st_ctime < 3600:
                logger.info(f"load relations cache from file {cache_file}")
                with cache_file.open('rb') as f:
                    return pickle.load(f)
        if version.parse(odps.__version__) >= version.parse('0.11.5b2'):
            result_views = set([t.name for t in self.odps.list_tables(
                project=schema_relation.database,
                schema=schema_relation.schema,
                type='virtual_view'
            )])
        else:
            kwargs = {"schema": schema_relation}
            result_views = set([t['table_name'] for t in self.execute_macro("odps__list_views_without_caching", kwargs=kwargs).rows])
        relations = []
        for row in result_views:
            relations.append(
                self.Relation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=row,
                    type=RelationType.View,
                )
            )
        for row in self.odps.list_tables(project=schema_relation.database, schema=schema_relation.schema):
            if row.name in result_views:
                continue
            relations.append(
                self.Relation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=row.name,
                    type=RelationType.Table,
                )
            )
        for row in self.odps.list_tables(project=schema_relation.database, schema=schema_relation.schema, type="external_table"):
            if row.name in result_views:
                continue
            relations.append(
                self.Relation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=row.name,
                    type=RelationType.External,
                )
            )


        if cache_enabled:
            logger.info(f"save relations to cache file {cache_file}")
            with cache_file.open("wb") as f:
                pickle.dump(relations, f)
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
