from dataclasses import dataclass, field
from typing import FrozenSet, Optional, TypeVar

from dbt.adapters.base.relation import BaseRelation, InformationSchema
from dbt.adapters.contracts.relation import RelationType, Path, Policy, RelationConfig
from odps.models import Table

from .relation_configs import ODPSMaterializedViewConfig, ODPSIncludePolicy

Self = TypeVar("Self", bound="ODPSRelation")


@dataclass(frozen=True, eq=False, repr=False)
class ODPSRelation(BaseRelation):
    quote_character: str = ""
    # subquery alias name is not required in MaxCompute
    require_alias: bool = False

    def without_quote(self):
        return self.quote(False, False, False)

    include_policy: Policy = field(default_factory=lambda: ODPSIncludePolicy())

    renameable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {
                RelationType.View,
                RelationType.Table,
            }
        )
    )

    replaceable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {
                RelationType.View,
                RelationType.Table,
                RelationType.MaterializedView,
            }
        )
    )

    @property
    def project(self):
        return self.database

    @property
    def is_transactional(self):
        return self.get("transactional", False)

    def information_schema(
        self, identifier: Optional[str] = None
    ) -> "ODPSInformationSchema":
        return ODPSInformationSchema.from_relation(self, identifier)

    @classmethod
    def from_odps_table(cls, table: Table):
        schema = table.get_schema()
        schema = schema.name if schema else "default"

        table_type = RelationType.Table
        if table.is_virtual_view:
            table_type = RelationType.View
        if table.is_materialized_view:
            table_type = RelationType.MaterializedView

        return cls.create(
            database=table.project.name,
            schema=schema,
            identifier=table.name,
            type=table_type,
        )

    @classmethod
    def materialized_view_from_relation_config(
        cls, relation_config: RelationConfig
    ) -> ODPSMaterializedViewConfig:
        return ODPSMaterializedViewConfig.from_relation_config(relation_config)


@dataclass(frozen=True, eq=False, repr=False)
class ODPSInformationSchema(InformationSchema):
    quote_character: str = ""

    @classmethod
    def get_path(
        cls, relation: BaseRelation, information_schema_view: Optional[str]
    ) -> Path:
        return Path(
            database="SYSTEM_CATALOG",
            schema="INFORMATION_SCHEMA",
            identifier=information_schema_view,
        )

    @classmethod
    def get_include_policy(cls, relation, information_schema_view):
        return relation.include_policy.replace(
            database=True, schema=True, identifier=True
        )

    @classmethod
    def get_quote_policy(
        cls,
        relation,
        information_schema_view: Optional[str],
    ) -> Policy:
        return relation.quote_policy.replace(
            database=False, schema=False, identifier=False
        )
