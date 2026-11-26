from dataclasses import dataclass
from typing import Dict, Optional

from dbt.adapters.contracts.connection import Credentials
from dbt_common.exceptions import DbtConfigError
from odps import ODPS
from odps.accounts import StsAccount


def _clean(value: Optional[str]) -> str:
    """去掉首尾空白；没填或只填空白都算没填。"""
    return value.strip() if isinstance(value, str) else ""


@dataclass
class ODPSCredentials(Credentials):
    endpoint: str
    access_id: str
    secret_access_key: str
    sts_token: Optional[str] = None
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
        access_id = _clean(self.access_id)
        secret_access_key = _clean(self.secret_access_key)
        sts_token = _clean(self.sts_token)

        if not access_id:
            raise DbtConfigError(
                "ODPS 凭据不完整：access_id 为空，请在 profiles.yml 里配置 access_id。"
            )
        if not secret_access_key:
            raise DbtConfigError(
                "ODPS 凭据不完整：secret_access_key 为空，"
                "请在 profiles.yml 里配置 secret_access_key。"
            )

        if sts_token:
            # STS：access_id / secret_access_key / sts_token 三者一起用，
            # token 由 pyodps 放进 authorization-sts-token 请求头。
            # expired_hours=None：凭据来自 profile，运行期间固定，
            # 不会在 token 过期后悄悄换成环境变量里的另一套凭据。
            account = StsAccount(
                access_id, secret_access_key, sts_token, expired_hours=None
            )
            return ODPS(
                endpoint=self.endpoint,
                project=self.database,
                schema=self.schema,
                account=account,
            )

        return ODPS(
            endpoint=self.endpoint,
            access_id=access_id,
            secret_access_key=secret_access_key,
            project=self.database,
            schema=self.schema,
        )
