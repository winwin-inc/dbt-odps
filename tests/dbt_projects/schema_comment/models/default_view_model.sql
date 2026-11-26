{{ config(materialized='view') }}

-- 视图同样走默认 schema
select * from {{ ref('default_schema_model') }}
