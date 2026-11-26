{{ config(
    sql_header='set a=b;'
) }}

-- 同时带 SET 语句：验证 SET 被剥离成 hints 的同时，schema 仍被正确注入
select 3 as id, 'custom_with_sql_header' as model_name
