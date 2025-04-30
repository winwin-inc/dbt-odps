from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from dbt.adapters.contracts.relation import (
    ComponentName,
    RelationConfig,
)

from ..utils import quote_ref, quote_string
from ._base import ODPSBaseRelationConfig
from ._partition import (
    PartitionConfig,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class ODPSMaterializedViewConfig(ODPSBaseRelationConfig):
    name: str
    project: str
    schema: str
    lifecycle: Optional[int] = None
    build_deferred: bool = False
    columns: Optional[List[str]] = None
    column_comment: Optional[Dict[str, str]] = None
    disable_rewrite: bool = False
    table_comment: Optional[str] = None
    partition_by: Optional[PartitionConfig] = None
    tblProperties: Optional[Dict[str, str]] = None

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "ODPSMaterializedViewConfig":
        # required
        kwargs_dict: Dict[str, Any] = {
            "name": cls._render_part(ComponentName.Identifier, config_dict["name"]),
            "schema": cls._render_part(ComponentName.Schema, config_dict["schema"]),
            "project": cls._render_part(ComponentName.Database, config_dict["project"]),
        }
        for key, value in config_dict.items():
            if key in ["name", "schema", "project"]:
                pass
            kwargs_dict[key] = value

        if partition := config_dict.get("partition_by"):
            kwargs_dict.update({"partition_by": PartitionConfig.parse(partition)})

        materialized_view: "ODPSMaterializedViewConfig" = super().from_dict(kwargs_dict)
        return materialized_view

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        config_dict = {
            "name": relation_config.identifier,
            "schema": relation_config.schema,
            "project": relation_config.database,
        }
        items = [
            "lifecycle",
            "build_deferred",
            "columns",
            "column_comment",
            "disable_rewrite",
            "table_comment",
            "partition_by",
            "tblProperties",
        ]

        if relation_config:
            for item in items:
                if item in relation_config.config:
                    config_dict.update({item: relation_config.config[item]})
        return config_dict

    def get_coordinate(self) -> str:
        if self.schema is None:
            return f"{self.name}"
        if self.project is None:
            return f"{self.schema}.{self.name}"
        return f"{self.project}.{self.schema}.{self.name}"

    def create_table_sql(self) -> str:
        sql = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {self.get_coordinate()}\n"
        if self.lifecycle and self.lifecycle > 0:
            sql += f"LIFECYCLE {self.lifecycle}\n"
        if self.build_deferred:
            sql += "BUILD DEFERRED\n"
        if self.columns and len(self.columns) > 0:
            sql += "("
            for column in self.columns:
                if self.column_comment and column in self.column_comment:
                    sql += f"{quote_ref(column)} COMMENT {quote_string(self.column_comment[column])}"
                else:
                    sql += f"{quote_ref(column)}"
                sql += ", "
            sql = sql[:-2]
            sql += ")\n"
        if self.disable_rewrite:
            sql += " DISABLE REWRITE\n"
        if self.table_comment:
            sql += f"COMMENT {quote_string(self.table_comment)}\n"
        if self.partition_by and len(self.partition_by.fields) > 0:
            sql += f"PARTITIONED BY({self.partition_by.render(False)})\n"
        if self.tblProperties and len(self.tblProperties) > 0:
            sql += "TBLPROPERTIES( "
            for k, v in self.tblProperties.items():
                sql += f'"{k}"="{v}", '
            sql = sql[:-2]
            sql += ")\n"
        return sql
