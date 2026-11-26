<p align="center">
<img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="500"/>
</p>

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## ODPS

ODPS, called [MaxCompute](https://www.alibabacloud.com/product/maxcompute) before. This adapter is a wrapper bridged PyOdps and DBT together.

MaxCompute Features:

| Feature          | Status |
| ---------------- | ------ |
| Partition Table  | ❎     |
| Cluster Table    | ❎     |
| External Table   | ❎     |
| Table Properties | ❎     |

DBT features:

| Name                                            | Status         |
| ----------------------------------------------- | -------------- |
| Materialization: Table                          | ✅             |
| Materialization: View                           | ✅             |
| Materialization: Incremental - Append           | ✅             |
| Materialization: Incremental - Insert+Overwrite | ✅             |
| Materialization: Incremental - Merge            | ✅             |
| Materialization: Ephemeral                      | ✅             |
| Seeds                                           | ✅             |
| Tests                                           | ✅             |
| Snapshots                                       | ✅<sup>1</sub> |
| Documentation                                   | ✅             |

## Getting Started

### Install DBT-ODPS

```bash
pip install dbt-odps-winwin
```

从源码构建（需要安装 hatch，仓库用 hatchling 打包）：

```bash
make build   # 等价于 hatch clean && hatch build，产物在 dist/
```

### How to Configure?

#### For Fresh Codebase

Run following command after installing dbt-odps:

```bash
dbt init
```

#### For Existing Codebase

Read more in here: https://docs.getdbt.com/docs/core/connection-profiles

Configuration options:

| Property          | Description                                                  | Example                                              |
| ----------------- | ------------------------------------------------------------ | ---------------------------------------------------- |
| Endpoint          | The endpoint of odps, read more in https://help.aliyun.com/document_detail/34951.html | http://service.cn-shanghai.maxcompute.aliyun.com/api |
| database          | The **project name** of odps, which you can find in https://maxcompute.console.aliyun.com/{your area}/project-list | odps-test-project                                    |
| schema            | Using **default** if you don't know what is schema.          | default                                              |
| access_id         | access id                                                    | LTAXXXXXXXXX                                         |
| secret_access_key | secret access key                                            | bZXXXXXXXXXX                                         |
| sts_token         | STS 临时凭证的 security token，填了就按 STS 账号认证（可选）    | STS.Nxxxxxxxxxxxx                                   |
| priority          | pyodps 请求优先级，默认 9                                    | 9                                                    |
| hints             | 追加到每条 SQL 的 odps hints，会覆盖内置默认值               | {"odps.sql.timezone": "Asia/Shanghai"}              |
| threads           | dbt 并发线程数                                               | 8                                                    |
| type              | odps                                                         | odps                                                 |

#### 认证方式

##### 长期 AK（默认）

`access_id` / `secret_access_key` 填长期 AccessKey，行为与之前完全一致。

##### STS 临时凭证

`access_id` / `secret_access_key` / `sts_token` 三个一起填时，适配器用 pyodps 的
`StsAccount` 连 MaxCompute，token 会随每个请求一起发出去（`authorization-sts-token` 请求头）。

profile 示例（仓库里只留模板，密钥一律走环境变量）：

```yaml
data_lab:
  target: dev
  outputs:
    dev:
      type: odps
      endpoint: "{{ env_var('ODPS_ENDPOINT') }}"
      access_id: "{{ env_var('ODPS_STS_ACCESS_KEY_ID') }}"
      secret_access_key: "{{ env_var('ODPS_STS_ACCESS_KEY_SECRET') }}"
      sts_token: "{{ env_var('ODPS_STS_TOKEN') }}"
      database: "{{ env_var('ODPS_PROJECT') }}"
      schema: default
      threads: 8
```

运行前把凭据注入环境：

```bash
export ODPS_ENDPOINT=http://service.cn-hangzhou.maxcompute.aliyun.com/api
export ODPS_PROJECT=<your-project>
export ODPS_STS_ACCESS_KEY_ID=STS.Nxxxxxxxx
export ODPS_STS_ACCESS_KEY_SECRET=xxxxxxxx
export ODPS_STS_TOKEN=xxxxxxxx
# 同名同值再导一份，dbt 会把它的值从日志和报错里抹掉
export DBT_ENV_SECRET_STS_TOKEN="$ODPS_STS_TOKEN"
```

行为与边界：

- **凭据在 dbt 进程启动时固定**：STS token 过期后不会自动续期，正在跑的 dbt 会失败，
  需要重新获取凭证再跑一次。
- **`sts_token` 填空白等同于没填**：这时按长期 AK 认证。
- **`access_id` / `secret_access_key` 填空白直接报错**：不会退化成悄悄用上环境变量里的另一套凭据。
- **密钥不要写进 profile**：用 `env_var` 注入，仓库里只留模板。

STS 三元组（access_id / secret_access_key / security token）由调用方自行获取，
例如通过阿里云 RAM 的 AssumeRole 接口签发；本适配器只负责用给定的凭据连接 MaxCompute。

验证连接：

```bash
dbt debug --profiles-dir <profiles 目录>
```

## NOTES

1. When using merge statement, ODPS required that table is a transactional table. So, we have to create the snapshot table before select. Under the hook, we using the first referred table as source data structure to create table, so this data source must be a table, view is not supported.


## DEVELOPER REF
DBT docs [what-are-adapters](https://docs.getdbt.com/guides/dbt-ecosystem/adapter-development/1-what-are-adapters)

### 本地开发与测试

```bash
# 单元测试
pytest tests/unit_test/ -q

# 端到端验证（仓库根目录要有 test.env，按 test.env.example 建；
# --offline 用假 pyodps，不连 MaxCompute）
python tests/dbt_projects/schema_comment/verify.py --offline
```
