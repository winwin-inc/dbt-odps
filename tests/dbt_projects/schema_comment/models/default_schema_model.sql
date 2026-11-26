-- 落在 profile 的 target schema：注入到 query comment 里的 schema 应等于连接默认 schema
select 1 as id, 'default_schema_model' as model_name
