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
python setup.py install --force 
or pip install dbt-odps-winwin
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
| type              | odps                                                         | odps                                                 |

## NOTES

1. When using merge statement, ODPS required that table is a transactional table. So, we have to create the snapshot table before select. Under the hook, we using the first referred table as source data structure to create table, so this data source must be a table, view is not supported.


## DEVELOPER REF
DBT docs [what-are-adapters](https://docs.getdbt.com/guides/dbt-ecosystem/adapter-development/1-what-are-adapters)