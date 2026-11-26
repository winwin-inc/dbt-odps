import os
import unittest
from unittest import mock

from dbt.adapters.odps.credentials import ODPSCredentials
from dbt_common.exceptions import DbtConfigError
from odps import accounts
from requests import Request

ENDPOINT = "http://service.cn-hangzhou.maxcompute.aliyun.com/api"


def _profile(**kwargs):
    """一份完整的 profile 字段，测试按需覆盖其中几项。"""
    profile = {
        "endpoint": ENDPOINT,
        "access_id": "AK",
        "secret_access_key": "SK",
        "database": "proj",
        "schema": "default",
    }
    profile.update(kwargs)
    return profile


def _credentials(**kwargs):
    return ODPSCredentials(**_profile(**kwargs))


class TestSTSCredentials(unittest.TestCase):
    """STS 三元组（access_id / secret_access_key / sts_token）应该被 pyodps 当成 STS 账号使用。

    长期 AK 的行为必须保持不变，否则现有 profile 全部受影响。
    """

    def test_long_term_access_key_builds_plain_account(self):
        o = _credentials().odps()

        self.assertNotIsInstance(o.account, accounts.StsAccount)
        self.assertEqual(o.account.access_id, "AK")
        self.assertEqual(o.account.secret_access_key, "SK")
        self.assertEqual(o.endpoint, ENDPOINT)
        self.assertEqual(o.project, "proj")
        self.assertEqual(o.schema, "default")

    def test_sts_token_builds_sts_account(self):
        o = _credentials(sts_token="STS.token").odps()

        self.assertIsInstance(o.account, accounts.StsAccount)
        self.assertEqual(o.account.access_id, "AK")
        self.assertEqual(o.account.secret_access_key, "SK")
        self.assertEqual(o.account.sts_token, "STS.token")

    def test_sts_token_is_sent_with_the_request(self):
        o = _credentials(sts_token="STS.token").odps()

        request = Request("GET", f"{ENDPOINT}/projects/proj").prepare()
        o.account.sign_request(request, ENDPOINT)

        self.assertEqual(request.headers.get("authorization-sts-token"), "STS.token")

    def test_blank_sts_token_falls_back_to_long_term_account(self):
        o = _credentials(sts_token="   ").odps()

        self.assertNotIsInstance(o.account, accounts.StsAccount)
        self.assertEqual(o.account.access_id, "AK")

    def test_sts_account_keeps_its_own_credentials_after_expiry(self):
        """STS 凭据来自 profile，运行期间固定：即使过期也不该悄悄换成环境变量里的另一套凭据。"""
        o = _credentials(sts_token="STS.token").odps()
        o.account._expire_time = 0  # 模拟 token 已过期

        env = {
            "ODPS_STS_ACCESS_KEY_ID": "OTHER_AK",
            "ODPS_STS_ACCESS_KEY_SECRET": "OTHER_SK",
            "ODPS_STS_TOKEN": "OTHER_TOKEN",
        }
        with mock.patch.dict(os.environ, env):
            request = Request("GET", f"{ENDPOINT}/projects/proj").prepare()
            o.account.sign_request(request, ENDPOINT)

        self.assertEqual(o.account.access_id, "AK")
        self.assertEqual(request.headers.get("authorization-sts-token"), "STS.token")

    def test_blank_access_key_raises_clear_error(self):
        with self.assertRaises(DbtConfigError) as ctx:
            _credentials(access_id="  ").odps()

        self.assertIn("access_id", str(ctx.exception))

    def test_blank_secret_access_key_raises_clear_error(self):
        with self.assertRaises(DbtConfigError) as ctx:
            _credentials(secret_access_key="").odps()

        self.assertIn("secret_access_key", str(ctx.exception))

    def test_secrets_are_not_exposed_in_connection_info(self):
        info = dict(_credentials(sts_token="STS.token").connection_info())

        self.assertNotIn("access_id", info)
        self.assertNotIn("secret_access_key", info)
        self.assertNotIn("sts_token", info)


class TestCredentialsProfile(unittest.TestCase):
    """profile 里的字段与 dbt 的校验规则。"""

    def test_profile_without_sts_token_is_still_valid(self):
        profile = _profile()

        ODPSCredentials.validate(profile)

        credentials = ODPSCredentials.from_dict(profile)
        self.assertIsNone(credentials.sts_token)

    def test_profile_with_sts_token_is_valid(self):
        profile = _profile(sts_token="STS.token")

        ODPSCredentials.validate(profile)

        credentials = ODPSCredentials.from_dict(profile)
        self.assertEqual(credentials.sts_token, "STS.token")

    def test_aliases_are_still_translated(self):
        translated = ODPSCredentials.translate_aliases(
            {"ak": "AK", "sk": "SK", "project": "proj"}
        )

        self.assertEqual(translated["access_id"], "AK")
        self.assertEqual(translated["secret_access_key"], "SK")
        self.assertEqual(translated["database"], "proj")


if __name__ == "__main__":
    unittest.main()
