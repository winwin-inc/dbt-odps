from dataclasses import dataclass
from typing import Dict, Optional

from dbt.adapters.contracts.connection import Credentials
from odps import ODPS

@dataclass
class ODPSCredentials(Credentials):
    endpoint: str
    access_id: str
    secret_access_key: str
    priority: Optional[int] = None
    hints: Optional[Dict[str, str]] = None

    _ALIASES = {
        "ak": "access_id",
        "sk": "secret_access_key",
        "accessId": "access_id",
        "accessKey": "secret_access_key",
        "project": "database",
    }

    @property
    def type(self):
        return "odps"

    @property
    def unique_field(self):
        return self.endpoint + "_" + self.database

    def _connection_keys(self):
        return "project", "database", "schema", "endpoint"

    def odps(self):
        o = ODPS(
            endpoint=self.endpoint,
            access_id=self.access_id,
            secret_access_key=self.secret_access_key,
            project=self.database,
            schema=self.schema,
        )
        
            
        return o
