-- 通过 +schema 落在自定义 schema（default_dbt_schema_comment_custom）：
-- 注入到 query comment 里的 schema 应是这个自定义 schema，
-- 而不是连接默认 schema（default）——这正是本分支修复的问题
select 2 as id, 'custom_schema_model' as model_name
