import os
import pickle
import tempfile
import time
from datetime import datetime
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import List, Optional, Dict, Iterable, Any

import agate
import dbt.exceptions
import odps
from dbt.adapters.base import AdapterConfig
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.capability import Capability, CapabilityDict, CapabilitySupport, Support
from dbt.adapters.base.impl import FreshnessResponse

from dbt.clients import agate_helper
from dbt.contracts.relation import RelationType
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

    _capabilities: CapabilityDict = CapabilityDict(
        {
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Full),
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
        }
    )

    @property
    def odps(self) -> ODPS:
        return self.connections.get_thread_connection().handle.odps
    def get_odps_client(self):
        conn = self.connections.get_thread_connection()
        return conn.handle.odps
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
            if relation.schema:
                self.odps.create_schema(relation.schema, relation.database)
        except ODPSError as e:
            if e.code in ( "ODPS-0110061","ObjectAlreadyExists"):
                return
            else:
                raise e

    def drop_schema(self, relation: OdpsRelation) -> None:
        """ODPS does not support schemas, so this is a no-op"""
        logger.debug(f"drop_schema: '{relation.project}.{relation.schema}'")
        
      

    def quote(self, identifier):
        return "`{}`".format(identifier)

    @lru_cache(maxsize=100)  # Cache results with no limit on size
    def support_namespace_schema(self, project: str):
        return self.get_odps_client().get_project(project).get_property("odps.schema.model.enabled",
                                                                        "false") == "true"

    def standardize_grants_dict(self, grants_table: agate.Table) -> dict:
        """Translate the result of `show grants` (or equivalent) to match the
        grants which a user would configure in their project.
        Ideally, the SQL to show grants should also be filtering:
        filter OUT any grants TO the current user/role (e.g. OWNERSHIP).
        If that's not possible in SQL, it can be done in this method instead.
        :param grants_table: An agate table containing the query result of
            the SQL returned by get_show_grant_sql
        :return: A standardized dictionary matching the `grants` config
        :rtype: dict
        """
        unsupported_privileges = ["INDEX", "READ", "WRITE"]

        grants_dict: Dict[str, List[str]] = {}
        # for row in grants_table:
        #     grantee = row["grantor"]
        #     privilege = row["privilege"]

        #     # skip unsupported privileges
        #     if privilege in unsupported_privileges:
        #         continue

        #     if privilege in grants_dict.keys():
        #         grants_dict[privilege].append(grantee)
        #     else:
        #         grants_dict.update({privilege: [grantee]})
        return grants_dict                                                                        
    
    @print_method_call
    def check_schema_exists(self, database: str, schema: str) -> bool:
        database = database.strip('`')
        if not self.support_namespace_schema(database):
            return False
        schema = schema.strip('`')
        schema_exist = self.odps.exist_schema(schema, database)
        return schema_exist

    @print_method_call
    def list_schemas(self, database: str) -> List[str]:
        database = database.split('.')[0]
        database = database.strip('`')
        if not self.support_namespace_schema(database):
            return ["default"]

        res = [schema.name for schema in self.get_odps_client().list_schemas(database)]

        return res

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
    def get_odps_table_by_relation(self, relation: OdpsRelation, retry_times=3):
        # Sometimes the newly created table will be judged as not existing, so add retry to obtain it.
        for i in range(retry_times):
            table = self.get_odps_client().get_table(
                relation.identifier, relation.project, relation.schema
            )
            try:
                table.reload()
                return table
            except NoSuchObject:
                logger.info(f"Table {relation.render()} does not exist, retrying...")
                time.sleep(10)
                continue
        logger.warning(f"Table {relation.render()} does not exist.")
        return None    
    @print_method_call
    def calculate_freshness_from_metadata(
        self,
        source: BaseRelation,
        manifest: 'Optional[Manifest]' = None,
     ):
        table = self.get_odps_table_by_relation(source)
      
        snapshot = datetime.now()

        freshness = FreshnessResponse(
            max_loaded_at=table.last_data_modified_time,
            snapshotted_at=snapshot,
            age=(snapshot - table.last_data_modified_time).total_seconds(),
        )

        logger.debug(f"calculate_freshness_from_metadata {freshness}")
        return None, freshness
        

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
        # if not self.Relation.get_default_quote_policy().database:
        #     database = None
        odpsTable = self.get_odps_client().get_table(identifier, database, schema)
        try:
            odpsTable.reload()
        except NoSuchObject:
            return None
        return OdpsRelation.from_odps_table(odpsTable)
    
