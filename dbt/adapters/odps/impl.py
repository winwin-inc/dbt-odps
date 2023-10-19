import agate
from dataclasses import dataclass
import dbt
import dbt.exceptions
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
from dbt.adapters.odps.utils import print_method_call,logger
from dbt.clients import agate_helper


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
        try:
            result_tables = self.execute_macro("hive__list_tables_without_caching", kwargs=kwargs)
            result_views = self.execute_macro("hive__list_views_without_caching", kwargs=kwargs)
        except dbt.exceptions.DbtRuntimeError as e:
            errmsg = getattr(e, "msg", "")
            if f"Database '{schema_relation}' not found" in errmsg:
                return []
            else:
                description = "Error while retrieving information about"
                logger.debug(f"{description} {schema_relation}: {e.msg}")
                return []

        # hive2
        # Collect table/view separately
        # Unfortunatly, Hive2 does not distincguish table/view
        # Currently views are also listed in `show tables`
        # https://issues.apache.org/jira/browse/HIVE-14558
        # all_rows = result_tables
        # relations = []
        # for row in all_rows:
        #    relation_type = self.get_relation_type(f"{schema_relation}.{row['tab_name']}")
        #    relations.append(
        #         self.Relation.create(
        #            schema=schema_relation.schema,
        #            identifier=row['tab_name'],
        #            type=relation_type
        #        )
        #    )

        # in Hive 2, result_tables has table + view, result_views only has views
        # so we build a result_tables_without_view that doesnot have views

        result_tables_without_view = []
        for row in result_tables:
            # check if this table is view
            is_view = (
                len(list(filter(lambda x: x["tab_name"] == row["tab_name"], result_views))) == 1
            )
            if not is_view:
                result_tables_without_view.append(row)

        relations = []
        for row in result_tables_without_view:
            relations.append(
                self.Relation.create(
                    schema=schema_relation.schema,
                    identifier=row["tab_name"],
                    type="table",
                )
            )
        for row in result_views:
            relations.append(
                self.Relation.create(
                    schema=schema_relation.schema,
                    identifier=row["tab_name"],
                    type="view",
                )
            )

        return relations
    
    @print_method_call
    def get_odps_table_by_relation(self, relation: OdpsRelation):
        return self.get_odps_table_if_exists(
            relation.identifier,
            project=relation.database,
            schema=relation.schema,
        )
    @print_method_call
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
    
    def _get_one_catalog(
        self,
        information_schema,
        schemas,
        manifest,
    ) -> agate.Table:
        """Get ONE catalog. Used by get_catalog

        manifest is used to run the method in other context's
        threadself.get_columns_in_relation
        """
        if len(schemas) != 1:
            dbt.exceptions.raise_compiler_error(
                f"Expected only one schema in Hive _get_one_catalog, found " f"{schemas}"
            )

        database = information_schema.database
        schema = list(schemas)[0]

        schema_relation = self.Relation.create(
            database=database,
            schema=schema,
            identifier="",
            quote_policy=self.config.quoting,
        ).without_identifier()

        columns: List[Dict[str, Any]] = []
        for relation in self.list_relations(database, schema):
            logger.debug(f"Getting table schema for relation {relation}")
            columns.extend(self._get_columns_for_catalog(relation))

        if len(columns) > 0:
            text_types = agate_helper.build_type_tester(["table_owner", "table_database"])
        else:
            text_types = []

        return agate.Table.from_object(
            columns,
            column_types=text_types,
        )

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
