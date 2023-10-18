import agate
from dataclasses import dataclass
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.base import AdapterConfig
from dbt.adapters.base.relation import BaseRelation, InformationSchema
from dbt.contracts.graph.manifest import Manifest
from typing import List, Set, cast, Optional, Dict
from typing_extensions import TypeAlias
from odps import ODPS
from odps.models import Table, TableSchema
from odps.errors import NoSuchObject, ODPSError
from .relation import OdpsRelation
from .colums import OdpsColumn
from .connections import ODPSConnectionManager, ODPSCredentials
import logging
import sys
logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("app.log"),
            logging.StreamHandler(sys.stdout),
        ],
)

logger = logging.getLogger(__name__)  # 创建适配器专用的日志记录器

LIST_RELATIONS_MACRO_NAME = "list_relations_without_caching"
SHOW_CREATE_TABLE_MACRO_NAME = "show_create_table"
RENAME_RELATION_MACRO_NAME = "rename_relation"


@dataclass
class OdpsConfig(AdapterConfig):
    partitioned_by: Optional[List[Dict[str, str]]] = None
    properties: Optional[Dict[str, str]] = None


class ODPSAdapter(SQLAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = ODPSConnectionManager
    Relation: TypeAlias = OdpsRelation
    Column: TypeAlias = OdpsColumn
    AdapterSpecificConfigs: TypeAlias = OdpsConfig

    @property
    def odps(self) -> ODPS:
        return self.connections.get_thread_connection().handle._odps

    @property
    def credentials(self) -> ODPSCredentials:
        return self.config.credentials

    @classmethod
    def date_function(cls) -> str:
        return "current_timestamp()"

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

    def check_schema_exists(self, database: str, schema: str) -> bool:
        """always return true, as ODPS does not have schemas."""
        return True

    def list_schemas(self, database: str) -> List[str]:
        try:
            return [schema.name for schema in self.odps.list_schemas(database)]
        except ODPSError as e:
            if e.code == "ODPS-0110061":
                return ["default"]
            else:
                raise e

    def list_relations_without_caching(
        self,
        schema_relation: OdpsRelation = None,
    ) -> List[OdpsRelation]:
        logger.error(f"list_relations_without_caching : {schema_relation},{schema_relation.schema}")

        kwargs = {}
        if schema_relation and schema_relation.schema != "default":
            kwargs["schema"] = schema_relation.schema

        
        # print(f"{kwargs}")
        for key, value in kwargs.items():
            logger.error(f"kwargs : {key}: {value}")

            
        relations = []
        for table in self.odps.list_tables(**kwargs):
            try:
                print(f"""{table}""")
                relations.append(OdpsRelation.from_odps_table(table))
            except NoSuchObject:
                pass
        return relations

    def get_odps_table_by_relation(self, relation: OdpsRelation):
        return self.get_odps_table_if_exists(
            relation.identifier,
            project=relation.database,
            schema=relation.schema,
        )

    def get_odps_table_if_exists(self, name, project=None, schema=None) -> Optional[Table]:
        kwargs = {
            "name": name,
            "project": project,
        }
        if schema != "default":
            kwargs["schema"] = schema

        if self.odps.exist_table(**kwargs):
            return self.odps.get_table(**kwargs)

        return None

    def get_columns_in_relation(self, relation: OdpsRelation):
        odps_table = self.get_odps_table_by_relation(relation)
        return (
            [OdpsColumn.from_odps_column(column) for column in odps_table.table_schema.simple_columns]
            if odps_table
            else []
        )

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        manifest: Manifest,
    ) -> agate.Table:
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


# may require more build out to make more user friendly to confer with team and community.
