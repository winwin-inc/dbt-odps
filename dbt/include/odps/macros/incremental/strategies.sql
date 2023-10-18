
{% macro get_insert_into_sql(target_relation, temp_relation, dest_columns) %}

    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into table {{ target_relation }}
    select {{dest_cols_csv}} from {{ temp_relation }}

{% endmacro %}
