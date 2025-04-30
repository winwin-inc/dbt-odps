from dataclasses import dataclass

from dbt.adapters.base.relation import Policy


class ODPSIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class ODPSQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True
