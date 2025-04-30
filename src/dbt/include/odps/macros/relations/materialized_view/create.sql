{% macro odps__get_create_materialized_view_as_sql(relation, sql) %}
    {%- set materialized_view = adapter.Relation.materialized_view_from_relation_config(config.model) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {{ sql_header if sql_header is not none }}
    {{ materialized_view.create_table_sql() }}
    as ({{ sql }});
{% endmacro %}
