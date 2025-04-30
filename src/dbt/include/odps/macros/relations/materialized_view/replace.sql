
{% macro odps__get_replace_materialized_view_sql(relation, sql) %}
    {{ drop_materialized_view(relation) }}
    {{ get_create_materialized_view_as_sql(relation, sql) }}
{% endmacro %}
