{% macro odps__get_rename_materialized_view_sql(relation, new_name) %}
    {{ exceptions.raise_compiler_error(
        "odps materialized view not support rename operation."
    ) }}
{% endmacro %}
