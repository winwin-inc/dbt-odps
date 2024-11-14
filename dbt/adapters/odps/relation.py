from dataclasses import dataclass, field
from dbt.adapters.base.relation import BaseRelation,InformationSchema
from dbt.contracts.relation import Policy, RelationType,ComponentName,Path
from odps.models.table import Table
from .utils import print_method_call, logger
from typing import FrozenSet, Optional, TypeVar, Type
 


@dataclass
class OdpsIncludePolicy(Policy):
    database: bool = True
    schema: bool = True 
    identifier: bool = True
 
@dataclass(frozen=True, eq=False, repr=False)
class OdpsRelation(BaseRelation):
    include_policy: Policy = field(default_factory =  lambda: OdpsIncludePolicy())
    quote_character: str = "`"

    def without_quote(self):
        return self.quote(False, False, False)
     
        
    @classmethod
    @print_method_call
    def create(cls, database=None, schema=None, identifier=None, type=None, **kwargs):
        if schema != "default":
            kwargs.update(
                {
                    "include_policy": OdpsIncludePolicy(schema=True),
                }
            )
        return super().create(database, schema, identifier, type, **kwargs)
 
   
    @property
    def project(self):
        return self.database

    @property
    def schema(self) -> str:
        if self.path.schema == "":
            return "default"
        return self.path.schema
    
    def render(self) -> str:
        render_str = ''

        if self.project:
           render_str = self.project 
        if self.schema:
            render_str = render_str +  "." + self.schema
        if self.table:
            render_str = render_str + "." + self.table
        return render_str 
    def information_schema(
        self, identifier: Optional[str] = None
    ) -> "OdpsInformationSchema":
        return OdpsInformationSchema.from_relation(self, identifier)
@dataclass(frozen=True, eq=False, repr=False)
class OdpsInformationSchema(InformationSchema):
    quote_character: str = "`"

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